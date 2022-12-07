use std::{ptr::NonNull, sync::Arc};

use arrow::{
    array::{as_primitive_array, make_array, ArrayData, Float64Array},
    buffer::Buffer,
    compute::kernels::aggregate,
    datatypes::DataType,
};

#[no_mangle]
pub extern "C" fn wasm_sum(raw_ptr: i32, size: i32) -> f64 {
    unsafe {
        let ptr = NonNull::new_unchecked(raw_ptr as *mut _);
        // Defer ownership of the buffer memory since we do not want it to release the memory
        let allocation = Arc::new(());
        let buffer = Buffer::from_custom_allocation(ptr, (size as usize) * 8, allocation.clone());
        let data = ArrayData::new_unchecked(
            DataType::Float64,
            size as usize,
            None,
            None,
            0,
            vec![buffer],
            Vec::new(),
        );
        let array = make_array(data);
        let float_array: &Float64Array = as_primitive_array(&array);
        aggregate::sum(float_array).unwrap()
    }
}
