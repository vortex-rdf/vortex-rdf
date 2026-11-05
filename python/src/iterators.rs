//! The iterators of the code path, each `__next__` one pull on the bindings'
//! runtime, GIL-released and raced with the call's cancel token if it passed
//! one: a match's term-code columns in chunks from core's chunk stream
//! ([`CodeChunks`]), a match's quads as terms, a chunk decoded at a time
//! ([`QuadIterator`]), and a batch's answers in input order
//! ([`ProbeAnswers`]).

use std::sync::{Mutex, PoisonError};

use futures::StreamExt;
use futures::stream::BoxStream;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyString, PyTuple};
use vortex_buffer::Buffer;
use vortex_rdf_core::VortexRdfError as CoreError;
use vortex_rdf_core::{
    CodeChunkStream, DictReader, QuadColumn, SharedQuad, TermCode, VortexRdfStore as CoreStore,
};

use crate::batch::Batch;
use crate::cancel::{Pulled, Token, pull_next};
use crate::kernels::U64Column;
use crate::store::{PyQuad, decoded_columns, shared_columns};
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

/// One chunk of a match, as the stream of a [`QuadIterator`] yields it.
pub(crate) enum QuadChunk {
    /// A Dictionary store's chunk: its four code columns.
    Codes([Buffer<TermCode>; 4]),
    /// Any other store's chunk, its rows decoded to terms.
    Shared(Vec<SharedQuad>),
}

type QuadChunkStream = BoxStream<'static, Result<QuadChunk, CoreError>>;

/// A match's quads as `(s, p, o, g)` strings, in `get_quads`' order and
/// spelling, read a chunk of at most `batch_rows` rows at a time
/// (`VortexRdfStore.iter_quads`). A `__next__` that has used up its chunk
/// pulls the next on the bindings' runtime, GIL-released and raced with the
/// call's cancel token if it passed one. The end of the match, an error or
/// a cancel drops the stream, and every later `__next__` stops the
/// iteration.
#[pyclass(module = "vortex_rdf._native")] // not frozen: `__next__` takes `&mut self`
pub struct QuadIterator {
    /// The chunk stream; `None` once it has ended, failed or been
    /// cancelled. The `Mutex` only makes it `Sync`; it is read with
    /// `get_mut`.
    chunks: Mutex<Option<QuadChunkStream>>,
    /// The current chunk's rows not yet yielded, a column each.
    rows: [std::vec::IntoIter<Py<PyString>>; 4],
    /// The store's dictionary: `Some` exactly when the chunks are codes.
    dict: Option<DictReader>,
    /// The token every pull races, when the call passed one.
    token: Option<Token>,
}

impl QuadIterator {
    /// The quads of `view`, pulled in chunks of at most `batch_rows` (at
    /// least 1): code chunks decoded through `dict` when it is the store's
    /// dictionary, rows decoded to terms otherwise.
    pub(crate) fn open(
        view: CoreStore,
        dict: Option<DictReader>,
        batch_rows: usize,
        token: Option<Token>,
    ) -> Result<Self, CoreError> {
        let chunks: QuadChunkStream = if dict.is_some() {
            view.code_chunks(&QuadColumn::ALL, batch_rows)?
                .map(|columns| {
                    columns.map(|columns| {
                        QuadChunk::Codes(columns.try_into().unwrap_or_else(|columns: Vec<_>| {
                            panic!("{} code columns, not four", columns.len())
                        }))
                    })
                })
                .boxed()
        } else {
            let chunks = view.row_chunks(batch_rows)?;
            // The view decodes its own chunks, so the stream holds it.
            chunks
                .map(move |chunk| view.decode_row_chunk(&chunk?).map(QuadChunk::Shared))
                .boxed()
        };
        Ok(QuadIterator {
            chunks: Mutex::new(Some(chunks)),
            rows: std::array::from_fn(|_| Vec::new().into_iter()),
            dict,
            token,
        })
    }
}

#[pymethods]
impl QuadIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<PyQuad>> {
        loop {
            if let [Some(s), Some(p), Some(o), Some(g)] = self.rows.each_mut().map(Iterator::next) {
                return Ok(Some((s, p, o, g)));
            }
            // A chunk's columns have equal lengths, so a short column is
            // the chunk's end.
            let chunks = self
                .chunks
                .get_mut()
                .unwrap_or_else(PoisonError::into_inner);
            let pulled = py.detach(|| pull_next(&RUNTIME, self.token.as_ref(), chunks));
            if !matches!(pulled, Pulled::Done(Some(Ok(_)))) {
                *chunks = None;
            }
            let Some(chunk) = settled(pulled.map(Option::transpose))? else {
                return Ok(None);
            };
            let columns = match (chunk, &self.dict) {
                (QuadChunk::Codes(codes), Some(dict)) => decoded_columns(py, dict, &codes),
                (QuadChunk::Shared(rows), _) => Ok(shared_columns(py, &rows)),
                (QuadChunk::Codes(_), None) => unreachable!("code chunks come with a dictionary"),
            };
            match columns {
                Ok(columns) => self.rows = columns.map(Vec::into_iter),
                Err(err) => {
                    *self
                        .chunks
                        .get_mut()
                        .unwrap_or_else(PoisonError::into_inner) = None;
                    return Err(err);
                }
            }
        }
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
