use pyo3::exceptions::{PyFileNotFoundError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyString};
use vortex_rdf_core::common::terms::{Pattern, parse_pattern_checked};
use vortex_rdf_core::{VortexRdfError, VortexRdfStore as CoreStore};

use crate::codes::{TermDict, U32Column};
use crate::{RUNTIME, parse_err, store_err};

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

/// Unwrap decoded columns, rejecting anything that cannot be a valid result.
///
/// A `None` term is a matched row carrying a code the dictionary snapshot
/// cannot resolve; unequal column lengths are a match that produced ragged
/// columns. Both indicate an inconsistent store, and either would otherwise
/// surface as a silently wrong result set.
fn resolve_columns(columns: [Vec<Option<Py<PyString>>>; 4]) -> PyResult<[Vec<Py<PyString>>; 4]> {
    let rows = columns[0].len();
    if columns.iter().any(|c| c.len() != rows) {
        return Err(PyValueError::new_err(format!(
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
                    return Err(PyValueError::new_err(format!(
                        "matched row {row} has a term code outside the store dictionary"
                    )));
                }
            }
        }
    }
    Ok(out)
}

/// A read-only Vortex-RDF store opened from a `.vortex` file.
///
/// The file is opened lazily: constructing the object reads only the file
/// footer (and, for the Dictionary layout, lifts the term dictionary when it
/// fits the residency budget). Keeping one instance warm across queries is
/// what makes rdflib `triples()` traffic cheap — reopening per call would
/// re-lift the dictionary every time.
#[pyclass(frozen, module = "vortex_rdf._native")]
pub struct VortexRdfStore {
    store: CoreStore,
    /// `None` for stores opened from bytes rather than a file.
    path: Option<String>,
}

impl VortexRdfStore {
    /// Runs the pattern match off the GIL and returns the matched quads.
    async fn matched_quads(&self, pattern: &Pattern) -> Result<Vec<oxrdf::Quad>, VortexRdfError> {
        let (s, p, o, g) = pattern;
        let view = self
            .store
            .match_pattern(s.as_ref(), p.as_ref(), o.as_ref(), g.as_ref())
            .await?;
        view.quads_vec().await
    }

    /// The matched rows as four columns of N-Triples strings, in
    /// subject-predicate-object-graph order. The default graph is the empty
    /// string, matching what `parse_graph_name` accepts for it.
    ///
    /// Backs both [`Self::get_quads`] and [`Self::match_columns`], so the two
    /// resolve a pattern the same way.
    fn matched_columns(
        &self,
        py: Python<'_>,
        pattern: &Pattern,
    ) -> PyResult<[Vec<Py<PyString>>; 4]> {
        if let Some(snapshot) = self.store.code_read_snapshot() {
            let columns = py
                .detach(|| -> Result<_, VortexRdfError> {
                    RUNTIME.block_on(async {
                        let (s, p, o, g) = pattern;
                        self.store
                            .match_pattern(s.as_ref(), p.as_ref(), o.as_ref(), g.as_ref())
                            .await?
                            .code_columns_gathered()
                            .await
                    })
                })
                .map_err(store_err)?;
            // `code_read_snapshot` reports only that the path can apply; the
            // match itself still decides, so fall through when it declines.
            if let Some(codes) = columns {
                let dict = TermDict { snapshot };
                let decoded = std::array::from_fn(|i| dict.decode_owned(py, codes[i].as_slice()));
                return resolve_columns(decoded);
            }
        }

        let quads = py
            .detach(|| RUNTIME.block_on(self.matched_quads(pattern)))
            .map_err(store_err)?;
        let mut out: [Vec<Py<PyString>>; 4] =
            std::array::from_fn(|_| Vec::with_capacity(quads.len()));
        for quad in quads {
            // `GraphName`'s own `Display` spells the default graph as a term;
            // the empty string is what the pattern parser accepts for it, so
            // both paths agree and a returned graph can be fed straight back.
            let graph = match &quad.graph_name {
                oxrdf::GraphName::DefaultGraph => String::new(),
                named => named.to_string(),
            };
            for (column, term) in out.iter_mut().zip([
                quad.subject.to_string(),
                quad.predicate.to_string(),
                quad.object.to_string(),
                graph,
            ]) {
                column.push(PyString::new(py, &term).unbind());
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
        path: String,
        max_resident_bytes: Option<u64>,
        in_memory: bool,
    ) -> PyResult<Self> {
        if !std::path::Path::new(&path).is_file() {
            return Err(PyFileNotFoundError::new_err(format!(
                "no such Vortex file: {path}"
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

    /// Open a store from native-container bytes — what [`Self::to_bytes`]
    /// (or the JS bindings' `toBytes`, or reading a `.vortex` file into
    /// memory) produces. Unlike the path constructor there is no file to
    /// stay lazily backed by: the whole store lives in memory, and the
    /// buffer crosses the Python boundary in one bulk copy.
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

    fn __len__(&self, py: Python<'_>) -> PyResult<usize> {
        py.detach(|| RUNTIME.block_on(self.store.size()))
            .map_err(store_err)
    }

    fn __repr__(&self) -> String {
        match &self.path {
            Some(path) => format!(
                "VortexRdfStore(path={:?}, layout={:?})",
                path,
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
    /// code; otherwise every matched quad is re-serialized through `oxrdf`'s
    /// `Display`. Both paths return the same rows.
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

    /// Match a pattern and return the matching quads as four parallel columns
    /// of N-Triples strings — `(subjects, predicates, objects, graphs)`, each
    /// as long as the result.
    ///
    /// The column-oriented counterpart of [`Self::get_quads`], for callers that
    /// work a position at a time (filtering on objects, collecting distinct
    /// subjects) and would otherwise build a tuple per row to take it apart
    /// again. Unlike [`Self::match_codes`] it is available on every layout,
    /// falling back to re-serialized quads when the code path does not apply.
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
        if self.store.code_read_snapshot().is_none() {
            return Ok(None);
        }
        let pattern = parse_pattern_checked(s, p, o, g).map_err(parse_err)?;
        let columns = py
            .detach(|| -> Result<_, VortexRdfError> {
                RUNTIME.block_on(async {
                    let (s, p, o, g) = &pattern;
                    self.store
                        .match_pattern(s.as_ref(), p.as_ref(), o.as_ref(), g.as_ref())
                        .await?
                        .code_columns_gathered()
                        .await
                })
            })
            .map_err(store_err)?;
        Ok(columns.map(|[s, p, o, g]| {
            (
                U32Column { codes: s },
                U32Column { codes: p },
                U32Column { codes: o },
                U32Column { codes: g },
            )
        }))
    }
}
