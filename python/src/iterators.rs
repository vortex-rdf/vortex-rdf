//! The iterators of the code path, each `__next__` one pull on the bindings'
//! runtime, GIL-released and raced with the call's cancel token if it passed
//! one: a match's term-code columns in chunks from core's chunk stream
//! ([`CodeChunks`]), and a batch's answers in input order ([`ProbeAnswers`]).

use std::sync::{Mutex, PoisonError};

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyTuple;
use vortex_buffer::Buffer;
use vortex_rdf_core::VortexRdfError as CoreError;
use vortex_rdf_core::{CodeChunkStream, QuadColumn, TermCode};

use crate::batch::Batch;
use crate::cancel::{Pulled, Token, pull_next};
use crate::kernels::U64Column;
use crate::{RUNTIME, settled};

/// A match's term-code columns in chunks of at most `batch_rows` rows, in
/// the match's order (`VortexRdfStore.match_code_chunks`). Each `__next__`
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
            .map(|chunk| PyTuple::new(py, chunk.into_iter().map(U64Column::from)))
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

/// `(s, p, o, g)` code columns as returned by [`VortexRdfStore::match_codes`].
pub(crate) type CodeColumns = (U64Column, U64Column, U64Column, U64Column);

pub(crate) fn code_columns(columns: [Buffer<TermCode>; 4]) -> CodeColumns {
    let [s, p, o, g] = columns.map(U64Column::from);
    (s, p, o, g)
}

/// One probe's gathered `(s, p, o, g)` code columns, or `None` where the
/// code path does not apply.
pub(crate) type ProbeColumns = Option<[Buffer<TermCode>; 4]>;

/// The answers of [`VortexRdfStore::match_codes_many`], one per probe in
/// input order: its `(s, p, o, g)` code columns, or `None` where the code
/// path does not apply. A bounded [`Batch`] runs the probes from the call
/// on; each `__next__` waits, GIL-released, for its next answer, raced with
/// the call's cancel token if it passed one. The first failing probe, or a
/// cancelled pull, raises at its position and ends the iteration, and
/// dropping the iterator aborts the probes still in flight.
#[pyclass(frozen, module = "vortex_rdf._native")]
pub struct ProbeAnswers {
    answers: Mutex<Answers>,
    /// The token every pull races, when the call passed one.
    token: Option<Token>,
    /// The number of probes, answered or not.
    len: usize,
}

impl ProbeAnswers {
    pub(crate) fn new(answers: Answers, token: Option<Token>, len: usize) -> Self {
        ProbeAnswers {
            answers: Mutex::new(answers),
            token,
            len,
        }
    }
}

/// What a [`ProbeAnswers`] has still to answer.
pub(crate) enum Answers {
    /// The probes' gathered code columns, from a running batch.
    Batch(Batch<ProbeColumns>),
    /// `None` for each of this many probes: the store's rows have no codes,
    /// so no probe runs.
    Declined(usize),
}

impl Answers {
    /// The next probe's answer, pulled with `token`; `None` once the probes
    /// have all answered, one has failed or a pull was cancelled.
    fn next(&mut self, token: Option<&Token>) -> Pulled<Option<Result<ProbeColumns, CoreError>>> {
        match self {
            Answers::Batch(batch) => batch.next(&RUNTIME, token),
            // No probe runs, so a cancelled token is read here: it ends the
            // answers still due.
            Answers::Declined(left) if *left > 0 && token.is_some_and(Token::is_cancelled) => {
                *left = 0;
                Pulled::Cancelled
            }
            Answers::Declined(left) => Pulled::Done(match left.checked_sub(1) {
                Some(rest) => {
                    *left = rest;
                    Some(Ok(None))
                }
                None => None,
            }),
        }
    }
}

#[pymethods]
impl ProbeAnswers {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<Option<Option<CodeColumns>>> {
        // The lock is taken GIL-released, so a thread waiting on it holds
        // no GIL the answering thread needs back.
        let answer = py.detach(|| {
            self.answers
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .next(self.token.as_ref())
        });
        let answer = settled(answer.map(Option::transpose))?;
        Ok(answer.map(|columns| columns.map(code_columns)))
    }

    fn __len__(&self) -> usize {
        self.len
    }
}
