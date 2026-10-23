use std::collections::HashMap;
use std::num::NonZero;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock, Mutex, PoisonError};

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyString};
use vortex_buffer::Buffer;
use vortex_rdf_core::{TermCode, VortexRdfError as CoreError, VortexRdfStore as CoreStore};

use crate::batch::Batch;
use crate::cancel::{Pulled, Token};
use crate::chunks::{CodeChunks, chunk_columns, every_column};
use crate::codes::{TermDict, U64Column, decode_slice};
use crate::probes::{narrowed_probe, parse_probes};
use crate::{CancelToken, RUNTIME, VortexRdfError, cancelled, detached, py_err, raced, settled};
use vortex_rdf_core::Probe;

/// `(s, p, o, g)` code columns as returned by [`VortexRdfStore::match_codes`].
type CodeColumns = (U64Column, U64Column, U64Column, U64Column);

fn code_columns([s, p, o, g]: [Buffer<TermCode>; 4]) -> CodeColumns {
    (
        U64Column { codes: s },
        U64Column { codes: p },
        U64Column { codes: o },
        U64Column { codes: g },
    )
}

/// One probe's gathered `(s, p, o, g)` code columns, or `None` where the
/// code path does not apply.
type ProbeColumns = Option<[Buffer<TermCode>; 4]>;

/// The rows `probe` matches as `(s, p, o, g)` term-code columns, gathered,
/// or `None` when the match declines the code path.
async fn probe_codes(store: &CoreStore, probe: &Probe) -> Result<ProbeColumns, CoreError> {
    store.run_probe(probe).await?.code_columns_gathered().await
}

/// A batch's `lookahead` when the call names none: twice the machine's
/// parallelism, and at least 64.
static DEFAULT_LOOKAHEAD: LazyLock<usize> = LazyLock::new(|| {
    std::thread::available_parallelism()
        .map_or(1, NonZero::get)
        .saturating_mul(2)
        .max(64)
});

/// A batch call's `lookahead`, or [`DEFAULT_LOOKAHEAD`] for `None`; zero
/// raises `ValueError`.
fn batch_lookahead(lookahead: Option<usize>) -> PyResult<usize> {
    match lookahead {
        Some(0) => Err(PyValueError::new_err(
            "lookahead must be at least 1: the probes a batch keeps in flight, and the \
             answers it keeps waiting",
        )),
        Some(lookahead) => Ok(lookahead),
        None => Ok(*DEFAULT_LOOKAHEAD),
    }
}

/// The token a `cancel=` argument carries, refused with [`cancelled`] when
/// it is already cancelled, before the call runs or spawns anything.
fn token_of(cancel: Option<&Bound<'_, CancelToken>>) -> PyResult<Option<Token>> {
    let token = cancel.map(|cancel| cancel.get().token.clone());
    if token.as_ref().is_some_and(Token::is_cancelled) {
        return Err(cancelled());
    }
    Ok(token)
}

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

/// What a [`ProbeAnswers`] has still to answer.
enum Answers {
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
        match answer {
            Pulled::Done(None) => Ok(None),
            Pulled::Done(Some(Ok(columns))) => Ok(Some(columns.map(code_columns))),
            Pulled::Done(Some(Err(error))) => Err(py_err(error)),
            Pulled::Cancelled => Err(cancelled()),
        }
    }

    fn __len__(&self) -> usize {
        self.len
    }
}

/// One row of [`VortexRdfStore::get_quads`]: subject, predicate, object, graph.
/// Held as `Py<PyString>` so a term repeated down a column is one Python object
/// shared by every row that uses it.
type PyQuad = (Py<PyString>, Py<PyString>, Py<PyString>, Py<PyString>);

/// `(subjects, predicates, objects, graphs)` as returned by
/// [`VortexRdfStore::match_columns`].
type StringColumns = (
    Vec<Py<PyString>>,
    Vec<Py<PyString>>,
    Vec<Py<PyString>>,
    Vec<Py<PyString>>,
);

/// Unwrap decoded columns, raising `VortexRdfError` on anything that cannot
/// be a valid result.
///
/// A `None` term is a matched row carrying a code the store's dictionary
/// cannot resolve; unequal column lengths are a match that produced ragged
/// columns. Both indicate an inconsistent store, and either would otherwise
/// surface as a silently wrong result set.
fn resolve_columns(columns: [Vec<Option<Py<PyString>>>; 4]) -> PyResult<[Vec<Py<PyString>>; 4]> {
    let rows = columns[0].len();
    if columns.iter().any(|c| c.len() != rows) {
        return Err(VortexRdfError::new_err(format!(
            "matched code columns have unequal lengths: {:?}",
            columns.iter().map(Vec::len).collect::<Vec<_>>()
        )));
    }
    let mut out: [Vec<Py<PyString>>; 4] = std::array::from_fn(|_| Vec::with_capacity(rows));
    for (position, column) in columns.into_iter().enumerate() {
        for (row, term) in column.into_iter().enumerate() {
            match term {
                Some(term) => out[position].push(term),
                None => {
                    return Err(VortexRdfError::new_err(format!(
                        "matched row {row} has a term code outside the store dictionary"
                    )));
                }
            }
        }
    }
    Ok(out)
}

/// A read-only Vortex-RDF store opened from a `.vortex` file or from
/// native-container bytes. A file is memory-mapped: only the footer (and,
/// under the Dictionary layout, the dictionary's window bounds) is read up
/// front, and each query reads the mapped pages it touches. Replace a store's
/// file by renaming a new one over it, never by truncating or rewriting it in
/// place (a reader of the mapping would be killed with SIGBUS); `serialize_rdf`
/// already writes that way (on Windows it fails until the store mapping the
/// file, and every `TermDict` and `U64Column` taken from it, is dropped). The
/// mapping lives as long as the store or any of those. One instance is meant to
/// be kept and queried repeatedly.
///
/// The Python bindings are read-only: stores are built with `serialize_rdf`
/// (file to file), then opened and queried. There is no in-memory build, RDF
/// export, membership test or mutation.
#[pyclass(frozen, module = "vortex_rdf._native")]
pub struct VortexRdfStore {
    store: CoreStore,
    /// `None` for stores opened from bytes.
    path: Option<PathBuf>,
}

impl VortexRdfStore {
    /// The matched rows as four columns of N-Triples strings, in
    /// subject-predicate-object-graph order. The default graph is the empty
    /// string, the spelling `Probe::parse` accepts for it.
    ///
    /// Backs both [`Self::get_quads`] and [`Self::match_columns`], so the two
    /// resolve a pattern the same way.
    fn matched_columns(&self, py: Python<'_>, probe: &Probe) -> PyResult<[Vec<Py<PyString>>; 4]> {
        if let Some(reader) = self.store.dict_reader() {
            // `dict_reader` reports only that the path can apply (a Dictionary
            // layout), whether the dictionary is in memory or
            // left in the mapped file; the match itself still decides, so fall
            // through when it declines.
            if let Some(codes) = detached(py, || probe_codes(&self.store, probe))? {
                let mut decoded = Vec::with_capacity(4);
                for column in &codes {
                    decoded.push(decode_slice(py, &reader, column.as_slice())?);
                }
                let decoded: [_; 4] = decoded
                    .try_into()
                    .unwrap_or_else(|_| unreachable!("four code columns"));
                return resolve_columns(decoded);
            }
        }

        // The matched quads with shared-string terms: each distinct term of a
        // decoded chunk is one `Arc<str>`, handed to every row repeating it.
        let rows = detached(py, || async {
            self.store.run_probe(probe).await?.shared_quads_vec().await
        })?;
        // Intern by `Arc` address: `rows` holds every `Arc` for the whole
        // loop, so an address identifies one term. A strong count of one means
        // no other row shares the term, so it skips the map.
        let mut interned: HashMap<usize, Py<PyString>> = HashMap::new();
        let mut out: [Vec<Py<PyString>>; 4] =
            std::array::from_fn(|_| Vec::with_capacity(rows.len()));
        for row in &rows {
            for (column, term) in out.iter_mut().zip([&row.s, &row.p, &row.o, &row.g]) {
                if Arc::strong_count(term) == 1 {
                    column.push(PyString::new(py, term).unbind());
                    continue;
                }
                let key = Arc::as_ptr(term) as *const u8 as usize;
                let py_term = interned
                    .entry(key)
                    .or_insert_with(|| PyString::new(py, term).unbind())
                    .clone_ref(py);
                column.push(py_term);
            }
        }
        Ok(out)
    }
}

#[pymethods]
impl VortexRdfStore {
    /// Open `path`. A file store is memory-mapped: what stays in RAM is the
    /// operating system's page cache (counted as file-backed RSS) plus at
    /// most 4 MiB of rebuilt dictionary windows per store (anonymous memory),
    /// and the file must not be modified while open — replace its file by
    /// renaming a new one over it, never by truncating or rewriting it in
    /// place (a reader of the mapping would be killed with SIGBUS); `serialize_rdf` already writes that way (on Windows it fails
    /// until the store mapping the file, and every `TermDict` and `U64Column`
    /// taken from it, is dropped). The mapping lives as long as the store or
    /// any of those. `in_memory=True` loads the whole store instead, keeping
    /// its columns in their compressed form wherever matches can bind them
    /// directly; every later match then skips the file.
    #[new]
    #[pyo3(signature = (path, *, in_memory=false))]
    fn new(py: Python<'_>, path: PathBuf, in_memory: bool) -> PyResult<Self> {
        let store = detached(py, || async {
            if in_memory {
                // The whole store, read through the file reader and adopted
                // in memory — nothing reads the file again.
                CoreStore::from_file_in_memory(&path).await
            } else {
                CoreStore::from_file(&path).await
            }
        })?;
        Ok(Self {
            store,
            path: Some(path),
        })
    }

    /// Open a store from native-container bytes: what [`Self::to_bytes`],
    /// the JS bindings' `toBytes`, or reading a `.vortex` file into memory
    /// produces. The whole store lives in memory. `data` should be `bytes`
    /// (or `bytearray`), copied in one memcpy; any other int sequence is
    /// accepted but extracted element by element.
    #[staticmethod]
    fn from_bytes(py: Python<'_>, data: Vec<u8>) -> PyResult<Self> {
        let store = detached(py, || CoreStore::from_bytes_owned(data))?;
        Ok(Self { store, path: None })
    }

    /// Serialize the store to native-container bytes: the exchange format
    /// shared with [`Self::from_bytes`], the JS bindings and the on-disk
    /// `.vortex` file, carrying the quad table plus the dictionary and
    /// index components.
    fn to_bytes<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let bytes = detached(py, || self.store.to_bytes())?;
        Ok(PyBytes::new(py, &bytes))
    }

    /// Column layout detected from the file: "default", "typed-object" or
    /// "dictionary" — core's canonical strategy names.
    fn layout(&self) -> String {
        self.store.layout().to_string()
    }

    /// The secondary indexes the store carries, as core's canonical
    /// kebab-case names ("secondary-by-copy", "secondary-by-reference").
    fn indexes(&self) -> Vec<String> {
        self.store
            .indexes()
            .iter()
            .map(ToString::to_string)
            .collect()
    }

    fn __len__(&self, py: Python<'_>) -> PyResult<usize> {
        detached(py, || self.store.size())
    }

    fn __repr__(&self) -> String {
        match &self.path {
            Some(path) => format!(
                "VortexRdfStore(path={:?}, layout={:?})",
                path.display().to_string(),
                self.layout()
            ),
            None => format!("VortexRdfStore(layout={:?})", self.layout()),
        }
    }

    /// Match a pattern and return the matching quads as
    /// `(subject, predicate, object, graph)` N-Triples strings. `None`
    /// positions are wildcards; the graph of a quad in the default graph is
    /// the empty string, which is also how a pattern selects it.
    ///
    /// Served from the term-code columns when the store supports them
    /// (Dictionary layout), reading terms out of the dictionary — held in
    /// memory, or read from the mapped file — and usually sharing one Python
    /// string across repeats of a code (see `TermDict.decode_many`);
    /// otherwise from the store's shared-term rows, where a term the decoder
    /// handed to several rows is likewise one Python string. Both paths
    /// return the same rows.
    #[pyo3(signature = (s=None, p=None, o=None, g=None))]
    fn get_quads(
        &self,
        py: Python<'_>,
        s: Option<&str>,
        p: Option<&str>,
        o: Option<&str>,
        g: Option<&str>,
    ) -> PyResult<Vec<PyQuad>> {
        let probe = Probe::parse(s, p, o, g).map_err(py_err)?;
        let [subjects, predicates, objects, graphs] = self.matched_columns(py, &probe)?;
        let mut rows = Vec::with_capacity(subjects.len());
        for (((s, p), o), g) in subjects
            .into_iter()
            .zip(predicates)
            .zip(objects)
            .zip(graphs)
        {
            rows.push((s, p, o, g));
        }
        Ok(rows)
    }

    /// Number of quads matching a pattern, counted from the match's row
    /// selection alone -- no term is materialized into Python.
    ///
    /// `keep` narrows the match to rows whose code in a position is in a
    /// set or range (see [`Self::match_codes`]; Dictionary layout only).
    /// `offset` and `limit` window the rows as [`Self::match_codes`] does:
    /// `offset` skips that many matched rows, and `limit` caps the count,
    /// stopping the read as soon as that many rows are known to exist
    /// (`count_quads(..., limit=1)` is an existence test). With a `cancel`
    /// token the count runs as a task raced with it ([`CancelToken`]);
    /// without one it runs on the calling thread.
    // The parameters are the Python signature: a pattern and its narrowing.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        s=None, p=None, o=None, g=None, *, keep=None, limit=None, offset=0, cancel=None
    ))]
    fn count_quads(
        &self,
        py: Python<'_>,
        s: Option<&str>,
        p: Option<&str>,
        o: Option<&str>,
        g: Option<&str>,
        keep: Option<&Bound<'_, PyAny>>,
        limit: Option<usize>,
        offset: usize,
        cancel: Option<&Bound<'_, CancelToken>>,
    ) -> PyResult<usize> {
        let probe = narrowed_probe(s, p, o, g, keep, limit, offset)?;
        match token_of(cancel)? {
            None => detached(py, || self.store.count_probe(&probe)),
            Some(token) => {
                let store = self.store.clone();
                raced(py, &token, async move { store.count_probe(&probe).await })
            }
        }
    }

    /// [`Self::count_quads`] for a batch of probes in one GIL-released call,
    /// answering in input order. A probe is an `(s, p, o, g)` tuple of
    /// optional term strings or a dict with keys `s`, `p`, `o`, `g`,
    /// `keep`, `limit`, `offset`. Every probe is parsed before any is
    /// evaluated, so a malformed one raises `ValueError` first. The probes
    /// run concurrently through the bounded window of
    /// [`Self::match_codes_many`]. The call raises the error
    /// of the first failing probe in input order and aborts the probes after
    /// it. With a `cancel` token the whole batch is one pull raced with it.
    #[pyo3(signature = (probes, *, lookahead=None, cancel=None))]
    fn count_quads_many(
        &self,
        py: Python<'_>,
        probes: &Bound<'_, PyAny>,
        lookahead: Option<usize>,
        cancel: Option<&Bound<'_, CancelToken>>,
    ) -> PyResult<Vec<usize>> {
        let lookahead = batch_lookahead(lookahead)?;
        let probes = parse_probes(probes)?;
        let token = token_of(cancel)?;
        settled(py.detach(|| {
            Batch::spawn(
                &RUNTIME,
                &self.store,
                probes,
                lookahead,
                |store, probe| async move { store.count_probe(&probe).await },
            )
            .collect(&RUNTIME, token.as_ref())
        }))
    }

    /// Match a pattern and return the matching quads as four parallel columns
    /// of N-Triples strings — `(subjects, predicates, objects, graphs)`, each
    /// as long as the result.
    ///
    /// The column-oriented counterpart of [`Self::get_quads`], for callers that
    /// work a position at a time (filtering on objects, collecting distinct
    /// subjects) and would otherwise build a tuple per row to take it apart
    /// again. Unlike [`Self::match_codes`] it is available on every layout,
    /// falling back to the shared-term rows when the code path does not apply.
    #[pyo3(signature = (s=None, p=None, o=None, g=None))]
    fn match_columns(
        &self,
        py: Python<'_>,
        s: Option<&str>,
        p: Option<&str>,
        o: Option<&str>,
        g: Option<&str>,
    ) -> PyResult<StringColumns> {
        let probe = Probe::parse(s, p, o, g).map_err(py_err)?;
        let [subjects, predicates, objects, graphs] = self.matched_columns(py, &probe)?;
        Ok((subjects, predicates, objects, graphs))
    }

    /// The store's term dictionary, or `None` for a non-Dictionary layout. A
    /// file store's dictionary is read from the mapped file on demand
    /// (`TermDict.file_backed`); an in-memory one answers in place. Pair with
    /// [`Self::match_codes`]; decode each distinct code once.
    fn term_dict(&self) -> Option<TermDict> {
        self.store.dict_reader().map(|reader| TermDict { reader })
    }

    /// Match a pattern and return the rows as four zero-copy `u64` term-code
    /// columns (`U64Column`) `(s, p, o, g)` decodable through [`Self::term_dict`], or
    /// `None` when the code path does not apply (see `term_dict`). Callers
    /// fall back to [`Self::get_quads`] or [`Self::match_columns`], which
    /// resolve terms on every layout.
    ///
    /// `keep` narrows the match inside the store, before any row is
    /// gathered: a dict from position (`"s"`, `"p"`, `"o"`, `"g"` or 0-3)
    /// to the codes to keep there — a code set (`U64Column`, a u64 buffer or
    /// a list of ints; what `TermDict.filter_codes` or encoded `VALUES`
    /// yield) or a code range (a `range` with step 1, or a 2-tuple
    /// `(lo, hi)`, the half-open codes `lo <= code < hi`; what
    /// `TermDict.prefix_range` yields). A 2-tuple is never a set. `offset`
    /// and `limit` window the rows: a window that cuts rows takes them in
    /// base order, and a filtered file scan stops at the first block that
    /// fills it, while a `limit` that cannot cut a row (no `offset`, and at
    /// least the match's row count, or a bound on it, known without reading)
    /// returns the match as it is, in its own order (an index-served
    /// match's rows in its index's). A `limit` that a filtered file match's
    /// rows turn out to fall short of returns the match as it is too, read
    /// from the window's evaluation of the filter. With a `cancel` token the
    /// match runs as a task raced with it ([`CancelToken`]); without one it
    /// runs on the calling thread.
    // The parameters are the Python signature: a pattern and its narrowing.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        s=None, p=None, o=None, g=None, *, keep=None, limit=None, offset=0, cancel=None
    ))]
    fn match_codes(
        &self,
        py: Python<'_>,
        s: Option<&str>,
        p: Option<&str>,
        o: Option<&str>,
        g: Option<&str>,
        keep: Option<&Bound<'_, PyAny>>,
        limit: Option<usize>,
        offset: usize,
        cancel: Option<&Bound<'_, CancelToken>>,
    ) -> PyResult<Option<CodeColumns>> {
        let probe = narrowed_probe(s, p, o, g, keep, limit, offset)?;
        let token = token_of(cancel)?;
        if self.store.dict_reader().is_none() {
            return Ok(None);
        }
        let columns = match token {
            None => detached(py, || probe_codes(&self.store, &probe))?,
            Some(token) => {
                let store = self.store.clone();
                raced(py, &token, async move { probe_codes(&store, &probe).await })?
            }
        };
        Ok(columns.map(code_columns))
    }

    /// [`Self::match_codes`] in chunks: an iterator ([`CodeChunks`]) whose
    /// `__next__` pulls the next chunk of at most `batch_rows` rows in base
    /// row order, as one zero-copy `U64Column` per name in `columns`
    /// (`"s"`, `"p"`, `"o"`, `"g"`, in the order given), or `None` when the
    /// code path does not apply. `keep`, `limit` and `offset` narrow the
    /// match as for [`Self::match_codes`]; this call resolves the narrowed
    /// match. In memory each chunk is sliced or gathered when it is pulled;
    /// on file a scan reads a bounded window of splits ahead of the pulls.
    /// With a `cancel` token this call and every pull race it
    /// ([`CancelToken`]). `batch_rows == 0`, no column or an unknown name
    /// raises `ValueError`.
    // The parameters are the Python signature: a pattern, its narrowing and
    // the chunk shape.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        s=None, p=None, o=None, g=None, *, keep=None, limit=None, offset=0,
        columns=every_column(), batch_rows=65536, cancel=None
    ))]
    fn match_code_chunks(
        &self,
        py: Python<'_>,
        s: Option<&str>,
        p: Option<&str>,
        o: Option<&str>,
        g: Option<&str>,
        keep: Option<&Bound<'_, PyAny>>,
        limit: Option<usize>,
        offset: usize,
        columns: Vec<String>,
        batch_rows: usize,
        cancel: Option<&Bound<'_, CancelToken>>,
    ) -> PyResult<Option<CodeChunks>> {
        let probe = narrowed_probe(s, p, o, g, keep, limit, offset)?;
        let columns = chunk_columns(&columns)?;
        if batch_rows == 0 {
            return Err(PyValueError::new_err(
                "batch_rows must be at least 1: the most rows a chunk holds",
            ));
        }
        let token = token_of(cancel)?;
        if self.store.dict_reader().is_none() {
            return Ok(None);
        }
        let store = self.store.clone();
        let wanted = columns.clone();
        let chunks = async move {
            store
                .run_probe(&probe)
                .await?
                .code_chunks(&wanted, batch_rows)
        };
        let stream = match &token {
            None => detached(py, || chunks)?,
            Some(token) => raced(py, token, chunks)?,
        };
        Ok(Some(CodeChunks::new(stream, token, columns, batch_rows)))
    }

    /// [`Self::match_codes`] for a batch of probes, answered in input order
    /// by an iterator ([`ProbeAnswers`]; see [`Self::count_quads_many`] for
    /// the probe forms). Every probe is parsed before any is evaluated. The
    /// probes run concurrently on the bindings' runtime from the call on,
    /// whether or not the iterator is pulled: at most 2 × `lookahead` of
    /// them running or answered but not yet pulled (`lookahead` is by
    /// default twice the machine's parallelism, at least 64). The first
    /// failing probe in input order raises at its position, and the probes
    /// after it are aborted, as are the probes in flight when the iterator
    /// is dropped. With a `cancel` token every pull races it
    /// ([`CancelToken`]): a cancelled pull raises at its position and aborts
    /// the probes in flight. On a store whose rows have no codes every
    /// answer is `None` and no probe runs.
    #[pyo3(signature = (probes, *, lookahead=None, cancel=None))]
    fn match_codes_many(
        &self,
        probes: &Bound<'_, PyAny>,
        lookahead: Option<usize>,
        cancel: Option<&Bound<'_, CancelToken>>,
    ) -> PyResult<ProbeAnswers> {
        let lookahead = batch_lookahead(lookahead)?;
        let probes = parse_probes(probes)?;
        let token = token_of(cancel)?;
        let len = probes.len();
        let answers = if self.store.dict_reader().is_none() {
            Answers::Declined(len)
        } else {
            Answers::Batch(Batch::spawn(
                &RUNTIME,
                &self.store,
                probes,
                lookahead,
                |store, probe| async move { probe_codes(&store, &probe).await },
            ))
        };
        Ok(ProbeAnswers {
            answers: Mutex::new(answers),
            token,
            len,
        })
    }
}
