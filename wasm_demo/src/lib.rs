use std::{ffi::c_void, mem, ptr::NonNull, sync::Arc};

use arrow::{
    alloc::Allocation,
    array::{as_primitive_array, make_array, Array, ArrayData, ArrayRef, Float64Array},
    buffer::Buffer,
    compute::kernels::aggregate,
    datatypes::DataType,
};

/// Allocate a range of memory in the WASM memory space
#[no_mangle]
pub extern "C" fn allocate(size: usize) -> *mut c_void {
    let mut buffer = Vec::with_capacity(size);
    let pointer = buffer.as_mut_ptr();
    mem::forget(buffer);

    pointer as *mut c_void
}

/// Deallocate a range of memory in the WASM memory space
#[no_mangle]
pub extern "C" fn deallocate(pointer: *mut c_void, capacity: usize) {
    unsafe {
        let _ = Vec::from_raw_parts(pointer, 0, capacity);
    }
}

#[no_mangle]
pub extern "C" fn wasm_mul(a: f64, b: f64) -> f64 {
    a * b
}

#[repr(C)]
pub struct WMean {
    count: f64,
    sum: f64,
}

impl WMean {
    #[no_mangle]
    pub extern "C" fn wmean_new() -> *mut WMean {
        let wmean = Box::new(Self {
            count: 0.0,
            sum: 0.0,
        });
        Box::into_raw(wmean)
    }
    #[no_mangle]
    pub extern "C" fn wmean_drop(wmean: *mut WMean) {
        unsafe {
            let _ = Box::from_raw(wmean);
        }
    }
    #[no_mangle]
    pub extern "C" fn wmean_state(&self, out_count: i32, out_sum: i32) {
        unsafe {
            *(out_count as *mut f64) = self.count;
            *(out_sum as *mut f64) = self.sum;
        }
    }
    #[no_mangle]
    pub extern "C" fn wmean_update(&mut self, values_ptr: i32, values_size: i32) {
        // Defer ownership of the buffer memory since we do not want it to release the memory
        let allocation = Arc::new(());
        let array = to_array(values_ptr, values_size, allocation.clone());
        let values: &Float64Array = as_primitive_array(&array);
        self.count += (values.len() - values.null_count()) as f64;
        if let Some(sum) = aggregate::sum(values) {
            self.sum += sum;
        }
    }
    #[no_mangle]
    pub extern "C" fn wmean_merge(&mut self, ptr: i32, size: i32) {
        // Defer ownership of the buffer memory since we do not want it to release the memory
        let allocation = Arc::new(());
        let array = to_array(ptr, size, allocation.clone());
        let values: &Float64Array = as_primitive_array(&array);
        self.count += values.value(0);
        self.sum += values.value(1);
    }
    #[no_mangle]
    pub extern "C" fn wmean_evaluate(&self) -> f64 {
        self.sum / self.count
    }
    #[no_mangle]
    pub extern "C" fn wmean_size(&self) -> i32 {
        16
    }
}

fn to_array(raw_ptr: i32, size: i32, allocation: Arc<dyn Allocation>) -> ArrayRef {
    unsafe {
        let ptr = NonNull::new_unchecked(raw_ptr as *mut u8);
        let buffer = Buffer::from_custom_allocation(ptr, size as usize, allocation);
        let data = ArrayData::new_unchecked(
            DataType::Float64,
            (size / 8) as usize,
            None,
            None,
            0,
            vec![buffer],
            Vec::new(),
        );
        make_array(data)
    }
}
