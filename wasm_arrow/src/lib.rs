use arrow::{array::Float64Array, compute::kernels::aggregate};

#[no_mangle]
pub extern "C" fn wasm_sum(arr: &Float64Array) -> f64 {
    aggregate::sum(&arr).unwrap()
}
