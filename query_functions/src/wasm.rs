use std::sync::Arc;

use arrow::{
    array::{Float32Builder, Float64Builder, Int32Builder, Int64Builder},
    datatypes::DataType,
};
use datafusion::{
    common::cast::{as_float32_array, as_float64_array, as_int32_array, as_int64_array},
    error::DataFusionError,
    execution::context::SessionState,
    logical_expr::{ScalarFunctionImplementation, Volatility},
    physical_plan::ColumnarValue,
    prelude::create_udf,
    scalar::ScalarValue,
};
use observability_deps::tracing::debug;
use once_cell::sync::Lazy;
use wasmtime::{Engine, Func, Linker, Module, Store, Val, ValType};

static WASM_ENGINE: Lazy<Arc<Engine>> = Lazy::new(|| Arc::new(Engine::default()));

fn wasm_scalar_impl(module: Module, name: String) -> ScalarFunctionImplementation {
    let func = move |args: &[ColumnarValue]| -> Result<ColumnarValue, DataFusionError> {
        let engine = WASM_ENGINE.clone();
        let linker: Linker<()> = Linker::new(engine.as_ref());
        let mut store = Store::new(engine.as_ref(), ());
        let instance = linker.instantiate(&mut store, &module).unwrap();
        if let Some(func) = instance.get_func(&mut store, name.as_str()) {
            let ft = func.ty(&mut store);
            // Its possible we have a combination of scalars and arrays as the parameters to the
            // funciton.
            //
            // When we have any array we need to call the wasm function in an elementwise fashion.
            // We use a matrix to represent all parameters to each invocation.
            // The columns of the matrix represent each parameter and the rows each invocation.
            // If value doesn't exist in the matrix this indicates we have a scalar value and
            // should use the value in the 0th row of the matrix.
            let param_types: Vec<ValType> = ft.params().collect();
            // Determine how many times we will need to call the function
            let num_calls = args
                .iter()
                .map(|arg| match arg {
                    ColumnarValue::Array(arr) => arr.len(),
                    ColumnarValue::Scalar(_) => 1,
                })
                .max()
                .unwrap();
            // Implement a sparse matrix using nested vectors, the outer vector represents the
            // row of the matrix.
            // TODO: Rewrite this to not reallocate the args as Val structs until each invocation
            // of the scalar function.
            let params: Vec<Vec<Val>> = args
                .iter()
                .enumerate()
                .map(|(i, arg)| {
                    match (&param_types[i], arg) {
                        (ValType::I32, ColumnarValue::Scalar(ScalarValue::Int32(v))) => {
                            Ok(vec![Val::I32(v.unwrap())])
                        }
                        (ValType::I64, ColumnarValue::Scalar(ScalarValue::Int64(v))) => {
                            Ok(vec![Val::I64(v.unwrap())])
                        }
                        (ValType::F32, ColumnarValue::Scalar(ScalarValue::Float32(v))) => {
                            Ok(vec![Val::F32(v.unwrap().to_bits())])
                        }
                        (ValType::F64, ColumnarValue::Scalar(ScalarValue::Float64(v))) => {
                            Ok(vec![Val::F64(v.unwrap().to_bits())])
                        }
                        (ValType::I32, ColumnarValue::Array(arr)) => {
                            let arr = as_int32_array(arr)?;
                            Ok(arr.iter().map(|v| Val::I32(v.unwrap())).collect())
                        }
                        (ValType::I64, ColumnarValue::Array(arr)) => {
                            let arr = as_int64_array(arr)?;
                            Ok(arr.iter().map(|v| Val::I64(v.unwrap())).collect())
                        }
                        (ValType::F32, ColumnarValue::Array(arr)) => {
                            let arr = as_float32_array(arr)?;
                            Ok(arr.iter().map(|v| Val::F32(v.unwrap().to_bits())).collect())
                        }
                        (ValType::F64, ColumnarValue::Array(arr)) => {
                            let arr = as_float64_array(arr)?;
                            Ok(arr.iter().map(|v| Val::F64(v.unwrap().to_bits())).collect())
                        }
                        // All other combinations indicate a type mismatch
                        _ => Err(DataFusionError::Internal(format!(
                            "invalid args to wasm scalar udf",
                        ))),
                    }
                })
                .collect::<Result<Vec<Vec<Val>>, DataFusionError>>()?;
            let result_type = ft.results().next().unwrap();
            if num_calls == 1 {
                do_scalar_call(name.as_str(), &params[0], result_type, func, &mut store)
            } else {
                match result_type {
                    ValType::I32 => {
                        let mut builder = Int32Builder::with_capacity(num_calls);
                        do_array_call(
                            name.as_str(),
                            num_calls,
                            &params,
                            func,
                            store,
                            &mut builder,
                        )?;
                        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
                    }
                    ValType::I64 => {
                        let mut builder = Int64Builder::with_capacity(num_calls);
                        do_array_call(
                            name.as_str(),
                            num_calls,
                            &params,
                            func,
                            store,
                            &mut builder,
                        )?;
                        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
                    }
                    ValType::F32 => {
                        let mut builder = Float32Builder::with_capacity(num_calls);
                        do_array_call(
                            name.as_str(),
                            num_calls,
                            &params,
                            func,
                            store,
                            &mut builder,
                        )?;
                        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
                    }
                    ValType::F64 => {
                        let mut builder = Float64Builder::with_capacity(num_calls);
                        do_array_call(
                            name.as_str(),
                            num_calls,
                            &params,
                            func,
                            store,
                            &mut builder,
                        )?;
                        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
                    }
                    _ => todo!(),
                }
            }
        } else {
            Err(DataFusionError::Internal(format!(
                "udf WASM export not found"
            )))
        }
    };
    Arc::new(func)
}

fn do_scalar_call(
    name: &str,
    params: &[Val],
    result_type: ValType,
    func: Func,
    store: &mut Store<()>,
) -> Result<ColumnarValue, DataFusionError> {
    let mut results: Vec<Val> = vec![match result_type {
        ValType::I32 => Val::I32(0),
        ValType::I64 => Val::I64(0),
        ValType::F32 => Val::F32(0),
        ValType::F64 => Val::F64(0),
        ValType::V128 => Val::V128(0),
        ValType::FuncRef => todo!(),
        ValType::ExternRef => todo!(),
    }];
    func.call(store, params, &mut results).unwrap();
    debug!(
        "called wasm function {} parameters: {:?} results: {:?}",
        name, params, results
    );
    match results.pop() {
        Some(Val::I32(v)) => Ok(ColumnarValue::Scalar(ScalarValue::Int32(Some(v)))),
        Some(Val::I64(v)) => Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(v)))),
        Some(Val::F32(v)) => Ok(ColumnarValue::Scalar(ScalarValue::Float32(Some(
            f32::from_bits(v),
        )))),
        Some(Val::F64(v)) => Ok(ColumnarValue::Scalar(ScalarValue::Float64(Some(
            f64::from_bits(v),
        )))),
        // All other combinations indicate a type mismatch
        _ => Err(DataFusionError::Internal(format!(
            "invalid results from wasm scalar udf"
        ))),
    }
}

fn do_array_call<T>(
    name: &str,
    num_calls: usize,
    params: &[Vec<Val>],
    func: Func,
    mut store: Store<()>,
    builder: &mut T,
) -> Result<(), DataFusionError>
where
    T: ValBuilder,
{
    // Params is an uneven slice of vectors.
    // The outer index represents the parameter position.
    // The inner index represents the invocation index.
    // Its possible the function has been called with a combination
    // of scalars and arrays for each of its parameters.
    //
    // When scalars are passed then interior params vector contains a single value that is to be
    // reused for each invocation.
    //
    // When arrays are passed then the interior params vector is contains a value for each
    // invocation of the function.

    let mut results: Vec<Val> = vec![Val::null()];
    let mut call_params: Vec<Val> = Vec::with_capacity(params.len());
    let mut i = 0;
    while i < num_calls {
        for (p, param) in params.iter().enumerate() {
            if i == 0 {
                call_params.push(Val::null())
            }
            if param.len() == 1 {
                call_params[p] = param[0].clone();
            } else {
                call_params[p] = param[i].clone();
            }
        }
        func.call(&mut store, &call_params, &mut results)
            .map_err(|e| {
                DataFusionError::Internal(format!("failed to call wasm function {}: {}", name, e))
            })?;
        debug!(
            "called wasm function {} parameters: {:?} results: {:?}",
            name, call_params, results
        );
        builder.append_value(&results[0]);
        i += 1;
    }
    Ok(())
}

trait ValBuilder {
    fn append_value(&mut self, v: &Val);
}

impl ValBuilder for Int32Builder {
    fn append_value(&mut self, v: &Val) {
        match v {
            Val::I32(v) => self.append_value(*v),
            _ => self.append_null(),
        }
    }
}
impl ValBuilder for Int64Builder {
    fn append_value(&mut self, v: &Val) {
        match v {
            Val::I64(v) => self.append_value(*v),
            _ => self.append_null(),
        }
    }
}
impl ValBuilder for Float32Builder {
    fn append_value(&mut self, v: &Val) {
        match v {
            Val::F32(v) => self.append_value(f32::from_bits(*v)),
            _ => self.append_null(),
        }
    }
}
impl ValBuilder for Float64Builder {
    fn append_value(&mut self, v: &Val) {
        match v {
            Val::F64(v) => self.append_value(f64::from_bits(*v)),
            _ => self.append_null(),
        }
    }
}

/// registers WASM functions so they can be invoked via SQL
pub fn register_wasm_udfs(mut state: SessionState) -> SessionState {
    // TODO: Currently we are compiling the module with each request, we should cache or otherwise
    // optimize this to align with expected usage patterns
    use wasmtime::*;
    let engine = WASM_ENGINE.clone();
    let module = Module::from_file(
        engine.as_ref(),
        // Hardcode for demo location of WASM scalar library
        "./target/wasm32-unknown-unknown/release/wasm_demo.wasm",
    )
    .unwrap();

    for f in module.exports() {
        match f.ty() {
            ExternType::Func(ft) => {
                debug!("found module export {}: {:?}", f.name(), f.ty());
                if f.name().starts_with("_") {
                    // Skip mangled exports
                    continue;
                }
                let params = ft.params().map(|t| convert_type(t)).collect();
                // TODO: Handle multiple return values
                let result = Arc::new(
                    ft.results()
                        .map(|t| convert_type(t))
                        .collect::<Vec<DataType>>()
                        .pop()
                        .unwrap(),
                );
                state.scalar_functions.insert(
                    f.name().to_string(),
                    Arc::new(create_udf(
                        f.name(),
                        params,
                        result,
                        Volatility::Volatile,
                        wasm_scalar_impl(module.clone(), f.name().to_string()),
                    )),
                );
            }
            ExternType::Global(_) => {}
            ExternType::Table(_) => {}
            ExternType::Memory(_) => {}
        }
    }

    state
}

fn convert_type(t: ValType) -> DataType {
    match t {
        ValType::I32 => DataType::Int32,
        ValType::I64 => DataType::Int64,
        ValType::F32 => DataType::Float32,
        ValType::F64 => DataType::Float64,
        ValType::V128 => todo!(),
        ValType::FuncRef => todo!(),
        ValType::ExternRef => todo!(),
    }
}
