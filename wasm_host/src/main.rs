use std::ptr::NonNull;

use arrow::{
    array::{Array, Float64Array, Float64Builder},
    buffer::Buffer,
    ffi,
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
    let data = memory.data_mut(&mut store);
    println!("memory:\n{}", hex::encode(&data[0..1024]),);
    unsafe {
        let raw_ptr = memory.data_ptr(&mut store);
        let ptr = NonNull::new_unchecked(raw_ptr);
        let buffer = Buffer::from_raw_parts(ptr, 1024, 1024)
            .into_mutable()
            .unwrap();
        let mut array = Float64Builder::new_from_buffer(buffer, None);
        array.append_value(1.2);
        array.append_value(3.4);
        array.append_value(5.6);
        //array.finish();
    };
    let data = memory.data_mut(&mut store);
    println!("memory:\n{}", hex::encode(&data[0..1024]),);
    //let sum = instance.get_typed_func::<(i32), (f64), _>(&mut store, "wasm_sum")?;
    //
    //let array = Float64Array::from(vec![Some(1.0), Some(2.0), Some(3.0), Some(4.0)]);
    //let ffi_array = ffi::ArrowArray::try_new(array.data().clone())?;
    //let (array_ptr, schema_ptr) = ffi::ArrowArray::into_raw(ffi_array);
    //println!("ptr: {:?}", array_ptr);
    //data[0..bytes.len()].clone_from_slice(&bytes);

    Ok(())
}
