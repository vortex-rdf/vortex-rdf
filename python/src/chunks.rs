//! The code chunk iterator: a match's term-code columns pulled from core's
//! chunk stream one chunk per `__next__`, with the GIL released.

use std::sync::{Mutex, PoisonError};

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyTuple;
use vortex_rdf_core::{CodeChunkStream, QuadColumn};

use crate::cancel::{Pulled, Token, pull_next};
use crate::codes::U64Column;
use crate::{RUNTIME, settled};

/// A match's term-code columns in chunks of at most `batch_rows` rows, in
/// base row order (`VortexRdfStore.match_code_chunks`). Each `__next__`
/// pulls one chunk on the bindings' runtime, GIL-released and raced with
/// the call's cancel token if it passed one, and returns one zero-copy
/// [`U64Column`] per requested column. The end of the match, an error or a
/// cancel drops the stream, and every later `__next__` stops the
/// iteration.
#[pyclass(frozen, module = "vortex_rdf._native")]
pub struct CodeChunks {
    /// The chunk stream; `None` once it has ended, failed or been
    /// cancelled.
    stream: Mutex<Option<CodeChunkStream>>,
    /// The token every pull races, when the call passed one.
    token: Option<Token>,
    /// The columns of each chunk, in order.
    columns: Vec<QuadColumn>,
    /// The most rows a chunk holds.
    batch_rows: usize,
}

impl CodeChunks {
    pub(crate) fn new(
        stream: CodeChunkStream,
        token: Option<Token>,
        columns: Vec<QuadColumn>,
        batch_rows: usize,
    ) -> Self {
        CodeChunks {
            stream: Mutex::new(Some(stream)),
            token,
            columns,
            batch_rows,
        }
    }
}

#[pymethods]
impl CodeChunks {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyTuple>>> {
        // The lock is taken GIL-released, so a thread waiting on it holds
        // no GIL the pulling thread needs back.
        let pulled = py.detach(|| {
            let mut stream = self.stream.lock().unwrap_or_else(PoisonError::into_inner);
            let pulled = pull_next(&RUNTIME, self.token.as_ref(), &mut stream);
            if !matches!(pulled, Pulled::Done(Some(Ok(_)))) {
                *stream = None;
            }
            pulled
        });
        let chunk = settled(pulled.map(Option::transpose))?;
        chunk
            .map(|chunk| PyTuple::new(py, chunk.into_iter().map(|codes| U64Column { codes })))
            .transpose()
    }

    /// The names of the columns each chunk holds, in order.
    #[getter]
    fn columns<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyTuple>> {
        PyTuple::new(py, self.columns.iter().map(|column| column.name()))
    }

    /// The most rows a chunk holds.
    #[getter]
    fn batch_rows(&self) -> usize {
        self.batch_rows
    }
}

/// The default `columns` of `match_code_chunks`: all four, in
/// `(s, p, o, g)` order.
pub(crate) fn every_column() -> Vec<String> {
    QuadColumn::ALL
        .iter()
        .map(|column| column.name().to_owned())
        .collect()
}

/// The columns `names` name (`"s"`, `"p"`, `"o"`, `"g"`), in order;
/// `ValueError` for no name or an unknown one.
pub(crate) fn chunk_columns(names: &[String]) -> PyResult<Vec<QuadColumn>> {
    if names.is_empty() {
        return Err(PyValueError::new_err(
            "columns names no column: name at least one of \"s\", \"p\", \"o\" or \"g\"",
        ));
    }
    names
        .iter()
        .map(|name| {
            QuadColumn::from_name(name).ok_or_else(|| {
                PyValueError::new_err(format!(
                    "unknown column {name:?}: expected \"s\", \"p\", \"o\" or \"g\""
                ))
            })
        })
        .collect()
}
