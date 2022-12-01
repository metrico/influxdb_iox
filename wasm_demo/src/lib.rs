#[no_mangle]
pub extern "C" fn wasm_mul(a: f64, b: f64) -> f64 {
    a * b
}
