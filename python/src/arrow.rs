//! The Arrow PyCapsule interface: matched rows, code columns and the term
//! dictionary handed to any Arrow consumer — pyarrow, polars, DuckDB,
//! DataFusion — through the C Data Interface structs the protocol names,
//! with no pyarrow dependency on either side.
//!
//! A capsule owns the struct it points at. Its destructor drops the boxed
//! struct, which calls the struct's `release` callback unless a consumer
//! already moved the struct out and nulled `release`, as the protocol
//! prescribes. Release callbacks only drop Rust reference counts, so they
//! are safe to run from whatever thread finalizes the capsule.

use std::ffi::{CStr, c_void};
use std::ptr::NonNull;
use std::sync::{Mutex, PoisonError};

use arrow_array::ffi::to_ffi;
use arrow_array::ffi_stream::FFI_ArrowArrayStream;
use arrow_array::{Array, RecordBatch, RecordBatchReader};
use arrow_schema::ffi::FFI_ArrowSchema;
use arrow_schema::{ArrowError, SchemaRef};
use futures::StreamExt;
use pyo3::exceptions::PyValueError;
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyTuple};
use vortex_rdf_core::TermEncoding;
use vortex_rdf_core::arrow::QuadBatches;

use crate::{RUNTIME, VortexRdfError};

const SCHEMA_CAPSULE: &CStr = c"arrow_schema";
const ARRAY_CAPSULE: &CStr = c"arrow_array";
const STREAM_CAPSULE: &CStr = c"arrow_array_stream";

/// Arrow-side failures raise the package's `VortexRdfError`.
fn arrow_err(err: ArrowError) -> PyErr {
    VortexRdfError::new_err(err.to_string())
}

/// The capsule destructor: drop the boxed struct the capsule points at.
unsafe extern "C" fn release_capsule<T>(capsule: *mut ffi::PyObject) {
    // SAFETY: `capsule` is the live capsule being finalized; its name is the
    // one it was created with, so `PyCapsule_GetPointer` hands back the
    // pointer `capsule()` leaked from a `Box<T>`.
    unsafe {
        let name = ffi::PyCapsule_GetName(capsule);
        let pointer = ffi::PyCapsule_GetPointer(capsule, name);
        if !pointer.is_null() {
            drop(Box::from_raw(pointer.cast::<T>()));
        }
    }
}

/// A capsule named `name` whose pointer is the address of a boxed `value`,
/// freed (and its `release` run, if still set) when the capsule dies.
fn capsule<'py, T>(py: Python<'py>, value: T, name: &'static CStr) -> PyResult<Bound<'py, PyCapsule>> {
    let pointer = NonNull::from(Box::leak(Box::new(value))).cast::<c_void>();
    // SAFETY: the pointer is a live leaked box that `release_capsule::<T>`
    // reclaims exactly once, at the capsule's finalization.
    match unsafe {
        PyCapsule::new_with_pointer_and_destructor(py, pointer, name, Some(release_capsule::<T>))
    } {
        Ok(capsule) => Ok(capsule),
        Err(err) => {
            // SAFETY: no capsule took the pointer, so this is its only owner.
            drop(unsafe { Box::from_raw(pointer.as_ptr().cast::<T>()) });
            Err(err)
        }
    }
}

/// `array` as the `(schema, array)` capsule pair `__arrow_c_array__` returns:
/// the C Data Interface export of the array, sharing its buffers.
pub(crate) fn array_capsules<'py>(py: Python<'py>, array: &dyn Array) -> PyResult<Bound<'py, PyTuple>> {
    let (ffi_array, ffi_schema) = to_ffi(&array.to_data()).map_err(arrow_err)?;
    let schema = capsule(py, ffi_schema, SCHEMA_CAPSULE)?;
    let array = capsule(py, ffi_array, ARRAY_CAPSULE)?;
    PyTuple::new(py, [schema.into_any(), array.into_any()])
}

/// A batch reader driving an export on the bindings' runtime: each `next`
/// blocks on the next chunk. It holds no Python state, so it runs wherever
/// the consumer calls it from — with or without the GIL.
struct BlockingReader {
    schema: SchemaRef,
    batches: QuadBatches,
}

impl Iterator for BlockingReader {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        RUNTIME
            .block_on(self.batches.next())
            .map(|batch| batch.map_err(|e| ArrowError::ExternalError(Box::new(e))))
    }
}

impl RecordBatchReader for BlockingReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// The record batches of one `match_arrow` call, handed to an Arrow consumer
/// through the PyCapsule interface: `pyarrow.RecordBatchReader.from_stream`,
/// `polars.DataFrame`, a DuckDB query over the object. The stream is
/// exported once; the schema can be read any number of times.
#[pyclass(frozen, module = "vortex_rdf._native")]
pub struct ArrowQuadStream {
    schema: SchemaRef,
    encoding: TermEncoding,
    /// Taken by the one `__arrow_c_stream__` export.
    batches: Mutex<Option<QuadBatches>>,
}

impl ArrowQuadStream {
    pub(crate) fn new(batches: QuadBatches, encoding: TermEncoding) -> Self {
        Self {
            schema: batches.schema(),
            encoding,
            batches: Mutex::new(Some(batches)),
        }
    }
}

#[pymethods]
impl ArrowQuadStream {
    /// The term encoding of the batches: "codes", "terms" or "strings".
    #[getter]
    fn encoding(&self) -> String {
        self.encoding.to_string()
    }

    /// The batches' schema as an `arrow_schema` capsule (`pyarrow.schema(stream)`).
    fn __arrow_c_schema__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyCapsule>> {
        let ffi_schema = FFI_ArrowSchema::try_from(self.schema.as_ref()).map_err(arrow_err)?;
        capsule(py, ffi_schema, SCHEMA_CAPSULE)
    }

    /// The batches as an `arrow_array_stream` capsule, consumable once; a
    /// second call raises `ValueError`. `requested_schema` is accepted for
    /// protocol conformance and not applied: the batches keep the schema
    /// `__arrow_c_schema__` reports.
    #[pyo3(signature = (requested_schema=None))]
    fn __arrow_c_stream__<'py>(
        &self,
        py: Python<'py>,
        requested_schema: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyCapsule>> {
        let _ = requested_schema;
        let batches = self
            .batches
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
            .ok_or_else(|| {
                PyValueError::new_err(
                    "this ArrowQuadStream was already consumed; call match_arrow again",
                )
            })?;
        let reader = BlockingReader {
            schema: self.schema.clone(),
            batches,
        };
        capsule(py, FFI_ArrowArrayStream::new(Box::new(reader)), STREAM_CAPSULE)
    }

    fn __repr__(&self) -> String {
        let columns: Vec<&str> = self
            .schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect();
        format!(
            "ArrowQuadStream(encoding={:?}, columns={:?})",
            self.encoding.to_string(),
            columns
        )
    }
}
