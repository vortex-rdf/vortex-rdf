use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use pyo3::exceptions::PyFileNotFoundError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyString};
use vortex_buffer::Buffer;
use vortex_rdf_core::common::terms::{Pattern, parse_pattern_checked};
use vortex_rdf_core::{
    QuadColumn, TermEncoding, VortexRdfError as CoreError, VortexRdfStore as CoreStore,
};

use crate::arrow::ArrowQuadStream;
use crate::codes::{TermDict, U32Column};
use crate::{RUNTIME, VortexRdfError, parse_err, store_err};

/// `(s, p, o, g)` code columns as returned by [`VortexRdfStore::match_codes`].
type CodeColumns = (U32Column, U32Column, U32Column, U32Column);

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
/// A `None` term is a matched row carrying a code the dictionary snapshot
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
/// native-container bytes. A file open reads only the footer up front and,
/// under the Dictionary layout, lifts the term dictionary when it fits the
/// residency budget; each match then scans the file, so one instance is meant
/// to be kept and queried repeatedly.
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
    ) -> PyResult<Option<[Buffer<u32>; 4]>> {
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
        if let Some(snapshot) = self.store.code_read_snapshot() {
            // `code_read_snapshot` reports only that the path can apply; the
            // match itself still decides, so fall through when it declines.
            if let Some(codes) = self.matched_code_columns(py, pattern)? {
                let dict = TermDict { snapshot };
                let decoded = std::array::from_fn(|i| dict.decode_slice(py, codes[i].as_slice()));
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
    /// Open `path`. By default the store stays file-backed and lazy (only the
    /// footer is read up front). `in_memory=True` loads the whole store into
    /// memory instead, keeping its columns in their compressed form wherever
    /// matches can bind them directly and decoding only the remainder —
    /// every subsequent match skips the per-call file-scan pipeline.
    /// `max_resident_bytes` overrides the Dictionary layout's
    /// term-dictionary residency budget (the dictionary child's compressed
    /// size in bytes).
    #[new]
    #[pyo3(signature = (path, max_resident_bytes=None, in_memory=false))]
    fn new(
        py: Python<'_>,
        path: PathBuf,
        max_resident_bytes: Option<u64>,
        in_memory: bool,
    ) -> PyResult<Self> {
        // Core reports a missing path as `VortexRdfError::Vortex`, not `Io`,
        // so the `FileNotFoundError` contract is honoured here.
        if !path.is_file() {
            return Err(PyFileNotFoundError::new_err(format!(
                "no such Vortex file: {}",
                path.display()
            )));
        }
        let store = py
            .detach(|| {
                RUNTIME.block_on(async {
                    let store = match max_resident_bytes {
                        Some(n) => CoreStore::from_file_with_dict_residency(&path, n).await?,
                        None => CoreStore::from_file(&path).await?,
                    };
                    if in_memory {
                        // Round-trip through the serializable parts: rows,
                        // index components, and the dictionary those rows'
                        // codes address, exactly what `from_parts`
                        // reconstructs a store from.
                        let parts = store.to_serializable_parts().await?;
                        CoreStore::from_parts(parts)
                    } else {
                        Ok(store)
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
    /// (Dictionary layout, resident dictionary, no append tail), reading terms
    /// out of the dictionary and sharing one Python string across repeats of a
    /// code; otherwise from the store's shared-term rows, where a term the
    /// decoder handed to several rows is likewise one Python string. Both
    /// paths return the same rows.
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
    #[pyo3(signature = (s=None, p=None, o=None, g=None))]
    fn count_quads(
        &self,
        py: Python<'_>,
        s: Option<&str>,
        p: Option<&str>,
        o: Option<&str>,
        g: Option<&str>,
    ) -> PyResult<usize> {
        let pattern = parse_pattern_checked(s, p, o, g).map_err(parse_err)?;
        py.detach(|| -> Result<usize, CoreError> {
            RUNTIME.block_on(async { self.matched(&pattern).await?.size().await })
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

    /// The store's term dictionary, or `None` when the code path does not
    /// apply: a non-Dictionary layout, a non-resident (file-backed)
    /// dictionary, or an append tail whose quads are not in the cached
    /// dictionary. Pair with [`Self::match_codes`]; decode each distinct
    /// code once, caching on the Python side.
    fn term_dict(&self) -> Option<TermDict> {
        self.store
            .code_read_snapshot()
            .map(|snapshot| TermDict { snapshot })
    }

    /// Match a pattern and return the rows as four zero-copy `u32` term-code
    /// columns `(s, p, o, g)` decodable through [`Self::term_dict`], or
    /// `None` when the code path does not apply (see `term_dict`). Callers
    /// fall back to [`Self::get_quads`] or [`Self::match_columns`], which
    /// resolve terms on every layout.
    #[pyo3(signature = (s=None, p=None, o=None, g=None))]
    fn match_codes(
        &self,
        py: Python<'_>,
        s: Option<&str>,
        p: Option<&str>,
        o: Option<&str>,
        g: Option<&str>,
    ) -> PyResult<Option<CodeColumns>> {
        let pattern = parse_pattern_checked(s, p, o, g).map_err(parse_err)?;
        if self.store.code_read_snapshot().is_none() {
            return Ok(None);
        }
        let columns = self.matched_code_columns(py, &pattern)?;
        Ok(columns.map(|[s, p, o, g]| {
            (
                U32Column { codes: s },
                U32Column { codes: p },
                U32Column { codes: o },
                U32Column { codes: g },
            )
        }))
    }

    /// Match a pattern and hand the rows to any Arrow consumer as a stream of
    /// record batches (the Arrow PyCapsule interface: pass the result to
    /// `pyarrow.RecordBatchReader.from_stream`, `polars.DataFrame`, or a
    /// DuckDB query). One batch per decode chunk, columns `s`, `p`, `o`, `g`
    /// — or the `projection` subset, in the given order.
    ///
    /// `encoding` selects the cell type: `"codes"` (`uint32` term codes,
    /// sharing the store's buffers; decode through [`Self::term_dict`] or
    /// join in code space), `"terms"` (the same codes as dictionary keys
    /// over the whole term dictionary, so consumers see strings and carry
    /// codes), or `"strings"` (`string_view` N-Triples strings — the one
    /// encoding every layout serves). Codes and terms need the Dictionary
    /// layout; the TypedObject layout has no Arrow export. Batches are
    /// produced as the consumer pulls them, off the GIL wherever the
    /// consumer releases it.
    #[pyo3(signature = (s=None, p=None, o=None, g=None, *, encoding="codes", projection=None))]
    // The parameter list is the Python signature: four pattern positions plus
    // the two keyword options.
    #[allow(clippy::too_many_arguments)]
    fn match_arrow(
        &self,
        py: Python<'_>,
        s: Option<&str>,
        p: Option<&str>,
        o: Option<&str>,
        g: Option<&str>,
        encoding: &str,
        projection: Option<Vec<String>>,
    ) -> PyResult<ArrowQuadStream> {
        let pattern = parse_pattern_checked(s, p, o, g).map_err(parse_err)?;
        let encoding: TermEncoding = encoding.parse().map_err(parse_err)?;
        let projection: Option<Vec<QuadColumn>> = projection
            .map(|names| names.iter().map(|name| name.parse()).collect())
            .transpose()
            .map_err(parse_err)?;
        let batches = py
            .detach(|| -> Result<_, CoreError> {
                RUNTIME.block_on(async {
                    self.matched(&pattern)
                        .await?
                        .to_record_batches(encoding, projection.as_deref())
                        .await
                })
            })
            .map_err(store_err)?;
        Ok(ArrowQuadStream::new(batches, encoding))
    }
}
