//! Codes, indices and counts read from Python into one Rust-owned buffer
//! ([`extract_u64s`]): a `U64Column`'s own buffer, shared; a buffer of u64
//! items or raw bytes, copied through the buffer protocol; any other
//! sequence of ints, one int at a time.

use pyo3::buffer::{ElementType, PyUntypedBuffer};
use pyo3::exceptions::PyValueError;
use pyo3::ffi;
use pyo3::prelude::*;
use vortex_buffer::Buffer;

use crate::kernels::U64Column;

/// The u64 values of a Python object holding codes, indices or counts, as a
/// Rust-owned buffer. A [`U64Column`] shares its own buffer, uncopied. Any
/// other object exporting a buffer is read as one, in one copy, never
/// element by element, and only when its items are u64 values:
/// 8-byte unsigned integers (`memoryview(col).cast("Q")`, `array("Q", ...)`,
/// a `uint64` NumPy array), 8-byte signed integers none of which is negative
/// (an `int64` NumPy array), or raw bytes in multiples of 8 — the view a
/// `U64Column` itself exports — read as native-endian u64s. A buffer of any
/// other items raises `ValueError` naming the u64 view: above all a
/// `cast("I")` view of a column, which would split each code in two. An
/// object without a buffer is a sequence of ints, at one `PyLong` extraction
/// per element; an int that is negative or not below 2**64 raises
/// `OverflowError`. Nothing is narrowed or wrapped.
pub(crate) fn extract_u64s(obj: &Bound<'_, PyAny>) -> PyResult<Buffer<u64>> {
    if let Ok(column) = obj.cast::<U64Column>() {
        return Ok(column.get().codes.clone());
    }
    // SAFETY: `obj` is a live object and the GIL is held.
    if unsafe { ffi::PyObject_CheckBuffer(obj.as_ptr()) } == 1 {
        // An exporter that fills no strides (a ctypes array) is readable
        // through a memoryview, which fills them.
        let buf = PyUntypedBuffer::get(obj).map_err(|e| {
            PyValueError::new_err(format!(
                "this buffer cannot be read as u64 codes ({e}): pass memoryview(obj), or \
                 {U64_FORMS}"
            ))
        })?;
        return buffer_u64s(&buf).map(Buffer::from);
    }
    obj.extract::<Vec<u64>>().map(Buffer::from)
}

/// What a code argument may be, as the refusals of a buffer spell it.
const U64_FORMS: &str = "a U64Column, a buffer of 8-byte integers (memoryview(col).cast(\"Q\"), \
                         array(\"Q\"), a uint64 or non-negative int64 NumPy array) or raw bytes \
                         in multiples of 8";

/// How a code buffer's items read as u64 values.
#[derive(Clone, Copy, PartialEq, Eq)]
enum CodeItems {
    /// 8-byte unsigned integers.
    Unsigned,
    /// 8-byte signed integers, refused if any is negative.
    Signed,
    /// Raw bytes, read as native-endian u64s in groups of 8.
    Raw,
}

/// The u64 values of a buffer's items (see [`extract_u64s`]).
fn buffer_u64s(buf: &PyUntypedBuffer) -> PyResult<Vec<u64>> {
    let format = buf.format();
    let items = match (ElementType::from_format(format), buf.item_size()) {
        (ElementType::UnsignedInteger { bytes: 8 }, 8) => CodeItems::Unsigned,
        (ElementType::SignedInteger { bytes: 8 }, 8) => CodeItems::Signed,
        (ElementType::UnsignedInteger { bytes: 1 }, 1) => CodeItems::Raw,
        _ => {
            return Err(PyValueError::new_err(format!(
                "a buffer of {}-byte items (format '{}') holds no u64 codes: pass {U64_FORMS}",
                buf.item_size(),
                format.to_string_lossy()
            )));
        }
    };
    let len_bytes = buf.len_bytes();
    if !len_bytes.is_multiple_of(8) {
        // Only raw bytes can get here: 8-byte items fill whole u64s.
        return Err(PyValueError::new_err(format!(
            "a byte buffer of {len_bytes} bytes is not a whole number of u64 values"
        )));
    }
    let mut values = vec![0u64; len_bytes / 8];
    // SAFETY: a u64 has no invalid bit patterns and a u8's alignment divides
    // a u64's, so the values are writable as their `len_bytes` bytes.
    let bytes =
        unsafe { std::slice::from_raw_parts_mut(values.as_mut_ptr().cast::<u8>(), len_bytes) };
    copy_items(buf, bytes);
    let swap = items != CodeItems::Raw && foreign_byte_order(format.to_bytes());
    for value in &mut values {
        if swap {
            *value = value.swap_bytes();
        }
        if items == CodeItems::Signed && (*value as i64) < 0 {
            return Err(PyValueError::new_err(format!(
                "a buffer of signed 8-byte items holds {}: codes are non-negative",
                *value as i64
            )));
        }
    }
    Ok(values)
}

/// Whether a struct-module format's byte-order prefix names the byte order
/// this machine does not use (`<` little-endian, `>` and `!` big-endian;
/// `@`, `=` or none native).
fn foreign_byte_order(format: &[u8]) -> bool {
    match format.first() {
        Some(b'<') => cfg!(target_endian = "big"),
        Some(b'>' | b'!') => cfg!(target_endian = "little"),
        _ => false,
    }
}

/// Copies the buffer's items, in C order, into `out` (exactly
/// `buf.len_bytes()` long): one bulk copy when the buffer is C-contiguous,
/// item by item otherwise (a strided or indirect view).
fn copy_items(buf: &PyUntypedBuffer, out: &mut [u8]) {
    debug_assert_eq!(out.len(), buf.len_bytes());
    if buf.is_c_contiguous() {
        // SAFETY: a C-contiguous buffer is `len_bytes` readable bytes at
        // `buf_ptr`, kept alive by `buf`, and cannot overlap `out`, which
        // this function's caller owns.
        unsafe {
            std::ptr::copy_nonoverlapping(buf.buf_ptr().cast::<u8>(), out.as_mut_ptr(), out.len());
        }
        return;
    }
    let shape = buf.shape();
    let item = buf.item_size();
    if out.is_empty() || item == 0 {
        return;
    }
    // Walk every index in C order: the last dimension varies fastest.
    let mut index = vec![0usize; shape.len()];
    for chunk in out.chunks_exact_mut(item) {
        // SAFETY: `index` is in bounds of `shape`, so `get_ptr` addresses a
        // whole item of `item` bytes inside the live buffer.
        unsafe {
            std::ptr::copy_nonoverlapping(
                buf.get_ptr(&index).cast::<u8>(),
                chunk.as_mut_ptr(),
                item,
            );
        }
        for d in (0..index.len()).rev() {
            index[d] += 1;
            if index[d] < shape[d] {
                break;
            }
            index[d] = 0;
        }
    }
}
