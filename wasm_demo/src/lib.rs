
//#[wasm_bindgen(module = "host")]
//extern "C" {
//    fn more() -> i32;
//    fn next() -> f32;
//    fn append(v: f32);
//}

#[no_mangle]
pub extern "C" fn wasm_mul(a: f64, b: f64) -> f64 {
    a * b
}
