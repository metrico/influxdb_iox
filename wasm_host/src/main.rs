use std::{mem, ptr::NonNull};

use arrow::{
    array::{ArrayBuilder, Float64Builder},
    buffer::Buffer,
};
use wasmtime::{Engine, Linker, Module, Store};

fn main() -> anyhow::Result<()> {
    let engine = Engine::default();
    let module = Module::from_file(
        &engine,
        "../target/wasm32-unknown-unknown/release/wasm_arrow.wasm",
    )?;
    let mut store: Store<()> = Store::new(&engine, ());
    let linker = Linker::new(&engine);
    let instance = linker.instantiate(&mut store, &module)?;
    let memory = instance
        .get_memory(&mut store, "memory")
        .ok_or(anyhow::format_err!("failed to get memory"))?;
    unsafe {
        let raw_ptr = memory.data_ptr(&mut store);
        let ptr = NonNull::new_unchecked(raw_ptr);
        let buffer = Buffer::from_raw_parts(ptr, 0, 1024).into_mutable().unwrap();
        let mut array = Float64Builder::new_from_buffer(buffer, None);
        array.append_value(1.2);
        array.append_value(3.4);
        array.append_value(5.6);
        array.append_value(7.8);
        array.append_value(9.0);
        let sum = instance.get_typed_func::<(i32, i32), f64, _>(&mut store, "wasm_sum")?;
        let s = sum
            .call(&mut store, (0 as i32, array.len() as i32))
            .unwrap();
        println!("sum: {}", s);

        //hack forget the builder buffer, this leaks real memory
        mem::forget(array);
        Ok(())
    }
}
