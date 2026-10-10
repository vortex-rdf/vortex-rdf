use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use pyo3::exceptions::PyFileNotFoundError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyString};
use vortex_buffer::Buffer;
use vortex_rdf_core::common::terms::{Pattern, parse_pattern_checked};
use vortex_rdf_core::{TermCode, VortexRdfError as CoreError, VortexRdfStore as CoreStore};

use crate::codes::{TermDict, U64Column};
use crate::probes::{parse_keeps, parse_probe, pattern_probe};
use crate::{RUNTIME, VortexRdfError, parse_err, store_err};
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

/// A probe from the keyword narrowing of `match_codes`/`count_quads`.
fn narrowed_probe(
    s: Option<&str>,
    p: Option<&str>,
    o: Option<&str>,
    g: Option<&str>,
    keep: Option<&Bound<'_, PyAny>>,
    limit: Option<usize>,
    offset: usize,
) -> PyResult<Probe> {
    let mut probe = pattern_probe(s, p, o, g)?;
    if let Some(keep) = keep.filter(|k| !k.is_none()) {
        probe.keeps = parse_keeps(keep)?;
    }
    probe.limit = limit;
    probe.offset = offset;
    Ok(probe)
}

/// Every probe of a `*_many` call, parsed before any evaluation.
fn parse_probes(probes: &Bound<'_, PyAny>) -> PyResult<Vec<Probe>> {
    probes
        .try_iter()?
        .map(|probe| parse_probe(&probe?))
        .collect()
}

/// Run `task` for every probe on the bindings' runtime, one task per probe
/// so a batch spreads over its workers (an in-memory match is CPU work;
/// a file-backed one overlaps its reads), and collect the answers in input
/// order. Called GIL-released.
///
/// The first probe to fail ends the call with its error and aborts the probes
/// still queued or running; a probe that panics re-raises its panic, as it
/// would from a single call.
fn fan_out<T, F, Fut>(store: &CoreStore, probes: Vec<Probe>, task: F) -> Result<Vec<T>, CoreError>
where
    T: Send + 'static,
    F: Fn(CoreStore, Probe) -> Fut,
    Fut: std::future::Future<Output = Result<T, CoreError>> + Send + 'static,
{
    RUNTIME.block_on(async {
        let mut tasks = tokio::task::JoinSet::new();
        for (index, probe) in probes.into_iter().enumerate() {
            let answer = task(store.clone(), probe);
            tasks.spawn(async move { (index, answer.await) });
        }
        let mut answers: Vec<Option<T>> = (0..tasks.len()).map(|_| None).collect();
        // Returning with probes outstanding drops the set, which aborts them.
        while let Some(joined) = tasks.join_next().await {
            match joined {
                Ok((index, answer)) => answers[index] = Some(answer?),
                Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
                Err(error) => {
                    return Err(CoreError::InvalidOperation(format!(
                        "a batch probe task failed: {error}"
                    )));
                }
            }
        }
        Ok(answers
            .into_iter()
            .map(|answer| answer.expect("every probe answered"))
            .collect())
    })
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
/// file is dropped). One instance is meant to be kept and queried repeatedly.
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
    /// The store view matching `pattern`.
    async fn matched(&self, pattern: &Pattern) -> Result<CoreStore, CoreError> {
        let (s, p, o, g) = pattern;
        self.store
            .match_pattern(s.as_ref(), p.as_ref(), o.as_ref(), g.as_ref())
            .await
    }

    /// The matched rows as `(s, p, o, g)` term-code columns, gathered off the
    /// GIL, or `None` when the match declines the code path.
    fn matched_code_columns(
        &self,
        py: Python<'_>,
        pattern: &Pattern,
    ) -> PyResult<Option<[Buffer<TermCode>; 4]>> {
        py.detach(|| -> Result<_, CoreError> {
            RUNTIME.block_on(async { self.matched(pattern).await?.code_columns_gathered().await })
        })
        .map_err(store_err)
    }

    /// The matched rows as four columns of N-Triples strings, in
    /// subject-predicate-object-graph order. The default graph is the empty
    /// string, the spelling `parse_pattern_checked` accepts for it.
    ///
    /// Backs both [`Self::get_quads`] and [`Self::match_columns`], so the two
    /// resolve a pattern the same way.
    fn matched_columns(
        &self,
        py: Python<'_>,
        pattern: &Pattern,
    ) -> PyResult<[Vec<Py<PyString>>; 4]> {
        if let Some(reader) = self.store.dict_reader() {
            // `dict_reader` reports only that the path can apply (a Dictionary
            // layout), whether the dictionary is in memory or
            // left in the mapped file; the match itself still decides, so fall
            // through when it declines.
            if let Some(codes) = self.matched_code_columns(py, pattern)? {
                let dict = TermDict { reader };
                let mut decoded = Vec::with_capacity(4);
                for column in &codes {
                    decoded.push(dict.decode_slice(py, column.as_slice())?);
                }
                let decoded: [_; 4] = decoded
                    .try_into()
                    .unwrap_or_else(|_| unreachable!("four code columns"));
                return resolve_columns(decoded);
            }
        }

        // The matched quads with shared-string terms: each distinct term of a
        // decoded chunk is one `Arc<str>`, handed to every row repeating it.
        let rows = py
            .detach(|| -> Result<_, CoreError> {
                RUNTIME.block_on(async { self.matched(pattern).await?.shared_quads_vec().await })
            })
            .map_err(store_err)?;
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
    /// operating system's page cache (counted as file-backed RSS, not
    /// anonymous memory), and the file must not be modified while open —
    /// replace its file by renaming a new one over it, never by truncating or
    /// rewriting it in place (a reader of the mapping would be killed with
    /// SIGBUS); `serialize_rdf` already writes that way (on Windows it fails
    /// until the store mapping the file is dropped). A file written by
    /// vortex-rdf 0.11 or earlier is refused, with an error that says to
    /// rebuild it from its RDF source. `in_memory=True` loads the whole
    /// store instead, keeping its columns in their compressed form wherever
    /// matches can bind them directly; every later match then skips the
    /// file.
    #[new]
    #[pyo3(signature = (path, *, in_memory=false))]
    fn new(py: Python<'_>, path: PathBuf, in_memory: bool) -> PyResult<Self> {
        // Core reports a missing path as an I/O error from the open; the
        // `FileNotFoundError` contract is honoured here with a clear message.
        if !path.is_file() {
            return Err(PyFileNotFoundError::new_err(format!(
                "no such Vortex file: {}",
                path.display()
            )));
        }
        let store = py
            .detach(|| {
                RUNTIME.block_on(async {
                    if in_memory {
                        // The whole store, read through the file reader and
                        // adopted in memory — nothing reads the file again.
                        CoreStore::from_file_in_memory(&path).await
                    } else {
                        CoreStore::from_file(&path).await
                    }
                })
            })
            .map_err(store_err)?;
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
        let store = py
            .detach(|| RUNTIME.block_on(CoreStore::from_bytes_owned(data)))
            .map_err(store_err)?;
        Ok(Self { store, path: None })
    }

    /// Serialize the store to native-container bytes: the exchange format
    /// shared with [`Self::from_bytes`], the JS bindings and the on-disk
    /// `.vortex` file, carrying the quad table plus the dictionary and
    /// index components.
    fn to_bytes<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let bytes = py
            .detach(|| RUNTIME.block_on(self.store.to_bytes()))
            .map_err(store_err)?;
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
        py.detach(|| RUNTIME.block_on(self.store.size()))
            .map_err(store_err)
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
    /// string across repeats of a code (see `TermDict.decode_many`); otherwise from the
    /// store's shared-term rows, where a term the decoder handed to several
    /// rows is likewise one Python string. Both paths return the same rows.
    #[pyo3(signature = (s=None, p=None, o=None, g=None))]
    fn get_quads(
        &self,
        py: Python<'_>,
        s: Option<&str>,
        p: Option<&str>,
        o: Option<&str>,
        g: Option<&str>,
    ) -> PyResult<Vec<PyQuad>> {
        let pattern = parse_pattern_checked(s, p, o, g).map_err(parse_err)?;
        let [subjects, predicates, objects, graphs] = self.matched_columns(py, &pattern)?;
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
    /// set or range (see [`Self::match_codes`]; Dictionary layout only), and
    /// `limit` caps the count, stopping the read as soon as that many rows
    /// are known to exist (`count_quads(..., limit=1)` is an existence
    /// test).
    // The parameters are the Python signature: a pattern and its narrowing.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (s=None, p=None, o=None, g=None, *, keep=None, limit=None))]
    fn count_quads(
        &self,
        py: Python<'_>,
        s: Option<&str>,
        p: Option<&str>,
        o: Option<&str>,
        g: Option<&str>,
        keep: Option<&Bound<'_, PyAny>>,
        limit: Option<usize>,
    ) -> PyResult<usize> {
        let probe = narrowed_probe(s, p, o, g, keep, limit, 0)?;
        py.detach(|| -> Result<usize, CoreError> {
            RUNTIME.block_on(async {
                let counts = self.store.count_many(std::slice::from_ref(&probe)).await?;
                Ok(counts[0])
            })
        })
        .map_err(store_err)
    }

    /// [`Self::count_quads`] for a batch of probes in one GIL-released call,
    /// answering in input order. A probe is an `(s, p, o, g)` tuple of
    /// optional term strings or a dict with keys `s`, `p`, `o`, `g`,
    /// `keep`, `limit`, `offset`. Every probe is parsed before any is
    /// evaluated, so a malformed one raises `ValueError` first.
    fn count_quads_many(&self, py: Python<'_>, probes: &Bound<'_, PyAny>) -> PyResult<Vec<usize>> {
        let probes = parse_probes(probes)?;
        py.detach(|| {
            fan_out(&self.store, probes, |store, probe| async move {
                let counts = store.count_many(std::slice::from_ref(&probe)).await?;
                Ok(counts[0])
            })
        })
        .map_err(store_err)
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
        let pattern = parse_pattern_checked(s, p, o, g).map_err(parse_err)?;
        let [subjects, predicates, objects, graphs] = self.matched_columns(py, &pattern)?;
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
    /// to the codes to keep there — a code set (`U64Column`, u64 buffer or
    /// int sequence; what `TermDict.filter_codes` or encoded `VALUES`
    /// yield) or a code range (a `range` with step 1, or `(lo, hi)`; what
    /// `TermDict.prefix_range` yields). `offset` and `limit` window the
    /// rows in base order; a filtered file scan stops at the first block
    /// that fills the window.
    // The parameters are the Python signature: a pattern and its narrowing.
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (s=None, p=None, o=None, g=None, *, keep=None, limit=None, offset=0))]
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
    ) -> PyResult<Option<CodeColumns>> {
        let probe = narrowed_probe(s, p, o, g, keep, limit, offset)?;
        if self.store.dict_reader().is_none() {
            return Ok(None);
        }
        let columns = py
            .detach(|| -> Result<_, CoreError> {
                RUNTIME.block_on(async {
                    self.store
                        .run_probe(&probe)
                        .await?
                        .code_columns_gathered()
                        .await
                })
            })
            .map_err(store_err)?;
        Ok(columns.map(code_columns))
    }

    /// [`Self::match_codes`] for a batch of probes in one GIL-released
    /// call, answering in input order (see [`Self::count_quads_many`] for
    /// the probe forms). The probes run concurrently on the bindings'
    /// runtime; every probe is parsed before any is evaluated.
    fn match_codes_many(
        &self,
        py: Python<'_>,
        probes: &Bound<'_, PyAny>,
    ) -> PyResult<Vec<Option<CodeColumns>>> {
        let probes = parse_probes(probes)?;
        if self.store.dict_reader().is_none() {
            return Ok(probes.iter().map(|_| None).collect());
        }
        let columns = py
            .detach(|| {
                fan_out(&self.store, probes, |store, probe| async move {
                    store.run_probe(&probe).await?.code_columns_gathered().await
                })
            })
            .map_err(store_err)?;
        Ok(columns.into_iter().map(|c| c.map(code_columns)).collect())
    }
}
