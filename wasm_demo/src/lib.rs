use wasm_bindgen::prelude::*;

//#[wasm_bindgen(module = "host")]
//extern "C" {
//    fn more() -> i32;
//    fn next() -> f32;
//    fn append(v: f32);
//}

#[wasm_bindgen]
pub fn wasm_mul(a: f64, b: f64) -> f64 {
    a * b
}

#[wasm_bindgen]
pub struct Mean {
    state: Vec<f64>,
}

#[wasm_bindgen]
impl Mean {
    pub fn new() -> Self {
        Self {
            state: vec![0.0, 0.0],
        }
    }
    pub fn state(&self) -> Vec<f64> {
        self.state.clone()
    }
    pub fn update(&mut self, values: Vec<f64>) {
        self.state[0] += values.len() as f64;
        for v in values {
            self.state[1] += v
        }
    }
    pub fn merge(&mut self, state: Vec<f64>) {
        self.state[0] += state[0];
        self.state[1] += state[1];
    }
    pub fn evaluate(&self) -> f64 {
        self.state[1] / self.state[0]
    }
    pub fn size(&self) -> i64 {
        16
    }
}

#[wasm_bindgen]
pub struct Stddev {
    state: Vec<f64>,
}

#[wasm_bindgen]
impl Stddev {
    pub fn new() -> Self {
        Self {
            state: vec![0.0, 0.0, 0.0],
        }
    }
    pub fn state(&self) -> Vec<f64> {
        self.state.clone()
    }
    pub fn update(&mut self, values: Vec<f64>) {
        self.state[0] += values.len() as f64;
        for v in values {
            self.state[1] += v
        }
    }
    pub fn merge(&mut self, state: Vec<f64>) {
        self.state[0] += state[0];
        self.state[1] += state[1];
    }
    pub fn evaluate(&self) -> f64 {
        self.state[1] / self.state[0]
    }
    pub fn size(&self) -> i64 {
        24
    }
}
