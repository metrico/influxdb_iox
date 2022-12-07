use std::{
    ops::DerefMut,
    sync::{Arc, Mutex},
};

use arrow::{
    array::{ArrayRef, Float32Builder, Float64Builder, Int32Builder, Int64Builder},
    compute::kernels::aggregate,
    datatypes::DataType,
};
use datafusion::{
    common::cast::{as_float32_array, as_float64_array, as_int32_array, as_int64_array},
    error::DataFusionError,
    execution::context::SessionState,
    logical_expr::{
        AccumulatorFunctionImplementation, AggregateState, AggregateUDF, ReturnTypeFunction,
        ScalarFunctionImplementation, Signature, StateTypeFunction, Volatility,
    },
    physical_plan::{Accumulator, ColumnarValue},
    prelude::create_udf,
    scalar::ScalarValue,
};
use observability_deps::tracing::debug;
use once_cell::sync::Lazy;
use wasmtime::{Engine, ExternType, Func, Linker, Memory, Module, Store, TypedFunc, Val, ValType};

/// registers WASM functions so they can be invoked via SQL
pub fn register_wasm_udfs(mut state: SessionState) -> SessionState {
    // TODO: Currently we are compiling the module with each request, we should cache or otherwise
    // optimize this to align with expected usage patterns
    let engine = WASM_ENGINE.clone();
    let module = Module::from_file(
        engine.as_ref(),
        // Hardcode for demo location of WASM scalar library
        "./target/wasm32-unknown-unknown/release/wasm_demo.wasm",
        //"./target/wasm32-unknown-unknown/release/wasm_demo.wat",
    )
    .unwrap();

    let names: Vec<&str> = module
        .exports()
        // skip any _internal_ exports
        .filter(|f| !f.name().starts_with("_"))
        .map(|f| f.name())
        .collect();

    let udfs = find_udfs(&names);
    debug!("udfs: {:?}", udfs);

    for udf in udfs {
        match udf {
            UDF::Scalar(name) => {
                let f = module.get_export(name.as_str()).unwrap();
                match f {
                    ExternType::Func(ft) => {
                        debug!("found module function export {}: {:?}", name, ft);
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
                            name.to_string(),
                            Arc::new(create_udf(
                                &name,
                                params,
                                result,
                                Volatility::Volatile,
                                wasm_scalar_impl(module.clone(), name.to_string()),
                            )),
                        );
                    }
                    // Ignore any other kind of export
                    _ => {}
                }
            }
            UDF::Aggregate(name) => {
                debug!("found module aggregate export {}", name);
                state
                    .aggregate_functions
                    .insert(name.clone(), build_wasm_uda(name, module.clone()));
            }
        }
    }

    // Add aggregate UDAs
    state
        .aggregate_functions
        .insert("mean_native".to_string(), build_mean_uda());

    state
}

#[derive(Debug)]
enum UDF {
    Scalar(String),
    Aggregate(String),
}

fn find_udfs(names: &[&str]) -> Vec<UDF> {
    let mut udfs = Vec::with_capacity(names.len());
    let mut all_agg_names = Vec::with_capacity(names.len());
    all_agg_names.push("allocate".to_string());
    all_agg_names.push("deallocate".to_string());
    // Look for aggregate function collections
    for n in names {
        if let Some(agg_prefix) = n.strip_suffix("_new") {
            let agg_prefix = agg_prefix.to_string();
            let mut agg_names = vec![
                agg_prefix.clone() + "_new",
                agg_prefix.clone() + "_drop",
                agg_prefix.clone() + "_state",
                agg_prefix.clone() + "_update",
                agg_prefix.clone() + "_merge",
                agg_prefix.clone() + "_evaluate",
                agg_prefix.clone() + "_size",
            ];
            if agg_names
                .iter()
                .all(|agg_name| names.iter().any(|n| n == agg_name))
            {
                udfs.push(UDF::Aggregate(agg_prefix));
                all_agg_names.extend(agg_names.drain(..));
            }
        }
    }
    // Find any extra functions which are then by definition scalars
    udfs.extend(
        names
            .iter()
            .filter(|n| {
                !all_agg_names
                    .iter()
                    .any(|agg_name| agg_name == n.to_owned())
            })
            .map(|n| UDF::Scalar(n.to_string())),
    );
    udfs
}

fn build_mean_uda() -> Arc<AggregateUDF> {
    let signature = Signature::exact(vec![DataType::Float64], Volatility::Stable);
    let return_type_func: ReturnTypeFunction = Arc::new(|_types| Ok(Arc::new(DataType::Float64)));
    let accumulator: AccumulatorFunctionImplementation =
        Arc::new(|_types| Ok(Box::new(MeanAccumulator::default())));
    let state_type_func: StateTypeFunction =
        Arc::new(|_types| Ok(Arc::new(vec![DataType::Float64])));
    Arc::new(AggregateUDF::new(
        "mean_native",
        &signature,
        &return_type_func,
        &accumulator,
        &state_type_func,
    ))
}

fn build_wasm_uda(name: String, module: Module) -> Arc<AggregateUDF> {
    let signature = Signature::exact(vec![DataType::Float64], Volatility::Stable);
    let return_type_func: ReturnTypeFunction = Arc::new(|_types| Ok(Arc::new(DataType::Float64)));
    let accumulator: AccumulatorFunctionImplementation =
        Arc::new(move |_types| Ok(Box::new(WASMAccumulator::new(name.clone(), module.clone()))));
    let state_type_func: StateTypeFunction =
        Arc::new(|_types| Ok(Arc::new(vec![DataType::Float64])));
    Arc::new(AggregateUDF::new(
        "wasm_mean",
        &signature,
        &return_type_func,
        &accumulator,
        &state_type_func,
    ))
}

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
            // TODO: Redesign this so that we do not have to allocate O(n) memory compared to the
            // input parameters
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
            // TODO: Maybe there is a sparse matrix implentation already? Initial searches show all
            // existing implementation are not generic containers rather specific to linear
            // algebra frameworks.
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

#[derive(Default, Debug)]
struct MeanAccumulator {
    sum: f64,
    count: f64,
}

impl Accumulator for MeanAccumulator {
    fn state(&self) -> datafusion::error::Result<Vec<datafusion::logical_expr::AggregateState>> {
        let mut builder = Float64Builder::with_capacity(2);
        builder.append_value(self.count);
        builder.append_value(self.sum);
        let arr = Arc::new(builder.finish());
        Ok(vec![AggregateState::Array(arr)])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion::error::Result<()> {
        assert_eq!(1, values.len()); // we should only ever have one input array
        let values = values[0].as_ref();
        let data = as_float64_array(values)?;
        self.count += (values.len() - values.null_count()) as f64;
        if let Some(sum) = aggregate::sum(data) {
            self.sum += sum;
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion::error::Result<()> {
        assert_eq!(states.len(), 1);
        for state in states {
            let state = as_float64_array(state)?;
            self.count += state.value(0);
            self.sum += state.value(1);
        }
        Ok(())
    }

    fn evaluate(&self) -> datafusion::error::Result<ScalarValue> {
        if self.count > 0.0 {
            Ok(ScalarValue::Float64(Some(self.sum / self.count)))
        } else {
            Ok(ScalarValue::Float64(None))
        }
    }

    fn size(&self) -> usize {
        // Two f64s
        16
    }
}

type DropFn = TypedFunc<i32, ()>;
type StateFn = TypedFunc<(i32, i32, i32), ()>;
type UpdateFn = TypedFunc<(i32, i32, i32), ()>;
type MergeFn = TypedFunc<(i32, i32, i32), ()>;
type EvaluateFn = TypedFunc<i32, f64>;
type SizeFn = TypedFunc<i32, i32>;
type AllocateFn = TypedFunc<i32, i32>;
type DeallocateFn = TypedFunc<(i32, i32), ()>;

struct WASMAccumulator {
    module: Module,
    store: Mutex<Store<()>>,
    ptr: i32,
    memory: Memory,
    drop_fn: DropFn,
    state_fn: StateFn,
    update_fn: UpdateFn,
    merge_fn: MergeFn,
    evaluate_fn: EvaluateFn,
    size_fn: SizeFn,
    allocate_fn: AllocateFn,
    deallocate_fn: DeallocateFn,
}
impl WASMAccumulator {
    fn new(name: String, module: Module) -> Self {
        let engine = WASM_ENGINE.clone();
        let linker: Linker<()> = Linker::new(engine.as_ref());
        let mut store = Store::new(engine.as_ref(), ());
        let instance = linker.instantiate(&mut store, &module).unwrap();
        let memory = instance.get_memory(&mut store, "memory").unwrap();
        let new = instance
            .get_typed_func::<(), i32, _>(&mut store, &(name.clone() + "_new"))
            .unwrap();
        let ptr = new.call(&mut store, ()).unwrap();
        let drop_fn = instance
            .get_typed_func(&mut store, &(name.clone() + "_drop"))
            .unwrap();
        let state_fn = instance
            .get_typed_func(&mut store, &(name.clone() + "_state"))
            .unwrap();
        let update_fn = instance
            .get_typed_func(&mut store, &(name.clone() + "_update"))
            .unwrap();
        let merge_fn = instance
            .get_typed_func(&mut store, &(name.clone() + "_merge"))
            .unwrap();
        let evaluate_fn = instance
            .get_typed_func(&mut store, &(name.clone() + "_evaluate"))
            .unwrap();
        let size_fn = instance
            .get_typed_func(&mut store, &(name.clone() + "_size"))
            .unwrap();
        let allocate_fn = instance.get_typed_func(&mut store, "allocate").unwrap();
        let deallocate_fn = instance.get_typed_func(&mut store, "deallocate").unwrap();
        let store = Mutex::new(store);
        debug!("wmean_new({})", ptr);
        Self {
            module,
            store,
            ptr,
            memory,
            drop_fn,
            state_fn,
            update_fn,
            merge_fn,
            evaluate_fn,
            size_fn,
            allocate_fn,
            deallocate_fn,
        }
    }
}
impl std::fmt::Debug for WASMAccumulator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "wasm_module {:?}", self.module.name())
    }
}
impl Drop for WASMAccumulator {
    fn drop(&mut self) {
        debug!("wmean_drop({})", self.ptr);
        let mut store = self.store.lock().unwrap();
        self.drop_fn.call(store.deref_mut(), self.ptr).unwrap();
    }
}

impl Accumulator for WASMAccumulator {
    //fn state(&self) -> datafusion::error::Result<Vec<datafusion::logical_expr::AggregateState>> {
    //    let mut store = self.store.lock().unwrap();
    //    let wasm_ptr = self.state_fn.call(store.deref_mut(), self.ptr).unwrap();
    //    let size = self.size_fn.call(store.deref_mut(), self.ptr).unwrap();

    //    let array = unsafe {
    //        let raw_ptr = self.memory.data_ptr(store.deref_mut());
    //        let ptr = NonNull::new_unchecked(raw_ptr.add(wasm_ptr as usize) as *mut u8);
    //        // Defer ownership of the buffer memory since we do not want it to release the memory
    //        let allocation = Arc::new(());
    //        let buffer = Buffer::from_custom_allocation(ptr, size as usize, allocation.clone());
    //        let data = ArrayData::new_unchecked(
    //            DataType::Float64,
    //            (size / 8) as usize,
    //            None,
    //            None,
    //            0,
    //            vec![buffer],
    //            Vec::new(),
    //        );
    //        make_array(data)
    //    };
    //    // TODO we need to free/drop the array later
    //    let arr = Arc::new(array);
    //    println!("state array: {:?}", arr);
    //    Ok(vec![AggregateState::Array(arr)])
    //}
    fn state(&self) -> datafusion::error::Result<Vec<datafusion::logical_expr::AggregateState>> {
        let mut store = self.store.lock().unwrap();

        // TODO Create a mutable arrow buffer from a pointer on the WASM side.
        let len = 16;
        let ptr = self.allocate_fn.call(store.deref_mut(), len).unwrap() as usize;
        self.state_fn
            .call(store.deref_mut(), (self.ptr, ptr as i32, ptr as i32 + 8))
            .unwrap();

        let mut bits: [u8; 8] = [0, 0, 0, 0, 0, 0, 0, 0];
        bits.copy_from_slice(&self.memory.data(store.deref_mut())[ptr..ptr + 8]);
        let count = f64::from_le_bytes(bits);
        bits.copy_from_slice(&self.memory.data(store.deref_mut())[ptr + 8..ptr + 16]);
        let sum = f64::from_le_bytes(bits);
        self.deallocate_fn
            .call(store.deref_mut(), (ptr as i32, len))
            .unwrap();

        let mut builder = Float64Builder::with_capacity(2);
        builder.append_value(count);
        builder.append_value(sum);
        let arr = Arc::new(builder.finish());
        debug!("state({}): {:?}", self.ptr, arr);
        Ok(vec![AggregateState::Array(arr)])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion::error::Result<()> {
        assert_eq!(1, values.len()); // we should only ever have one input array
        let mut store = self.store.lock().unwrap();
        let values = values[0].as_ref();
        debug!("update_batch({}): {:?}", self.ptr, values);
        let array = as_float64_array(values).unwrap();
        // Using the buffer API had a bug, so do this transmute manually
        let (l, bytes, r) = unsafe { array.values().align_to::<u8>() };
        // If these assertions fail the data is not aligned
        assert!(l.is_empty());
        assert!(r.is_empty());
        let len = bytes.len();
        let ptr = self
            .allocate_fn
            .call(store.deref_mut(), len as i32)
            .unwrap() as usize;
        self.memory.data_mut(store.deref_mut())[ptr..ptr + len].copy_from_slice(bytes);
        self.update_fn
            .call(store.deref_mut(), (self.ptr, ptr as i32, len as i32))
            .unwrap();
        self.deallocate_fn
            .call(store.deref_mut(), (ptr as i32, len as i32))
            .unwrap();
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion::error::Result<()> {
        assert_eq!(states.len(), 1);
        let mut store = self.store.lock().unwrap();
        for state in states {
            debug!("merge_batch({}): {:?}", self.ptr, state);
            let array = as_float64_array(state).unwrap();
            // Using the buffer API had a bug, so do this transmute manually
            let (l, bytes, r) = unsafe { array.values().align_to::<u8>() };
            // If these assertions fail the data is not aligned
            assert!(l.is_empty());
            assert!(r.is_empty());
            let len = bytes.len();
            let ptr = self
                .allocate_fn
                .call(store.deref_mut(), len as i32)
                .unwrap() as usize;
            self.memory.data_mut(store.deref_mut())[ptr..ptr + len].copy_from_slice(bytes);
            self.merge_fn
                .call(store.deref_mut(), (self.ptr, ptr as i32, len as i32))
                .unwrap();
            self.deallocate_fn
                .call(store.deref_mut(), (ptr as i32, len as i32))
                .unwrap();
        }
        Ok(())
    }

    fn evaluate(&self) -> datafusion::error::Result<ScalarValue> {
        let mut store = self.store.lock().unwrap();
        let v = self.evaluate_fn.call(store.deref_mut(), self.ptr).unwrap();
        debug!("evaluate({}): {:?}", self.ptr, v);
        Ok(ScalarValue::Float64(Some(v)))
    }

    fn size(&self) -> usize {
        let mut store = self.store.lock().unwrap();
        let size = self.size_fn.call(store.deref_mut(), self.ptr).unwrap() as usize;
        debug!("size({}): {:?}", self.ptr, size);
        size
    }
}
