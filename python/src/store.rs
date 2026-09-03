use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use futures::future::try_join_all;
use pyo3::exceptions::{PyFileNotFoundError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyRange, PyRangeMethods, PyString};
use vortex_buffer::Buffer;
use vortex_rdf_core::common::terms::{Pattern, parse_pattern_checked};
use vortex_rdf_core::{
    DictForm, Keep, QuadColumn, TermEncoding, VortexRdfError as CoreError,
    VortexRdfStore as CoreStore,
};

use crate::arrow::ArrowQuadStream;
use crate::codes::{TermDict, codes_from_py};
use crate::{RUNTIME, VortexRdfError, parse_err, store_err};

/// A pattern as Python spells it: four optional N-Triples term strings.
type PyPattern = (
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

/// The restrictions a code read applies after its pattern: `keep` constraints
/// in column order, then a row window.
#[derive(Default)]
struct ReadOptions {
    keep: Vec<(QuadColumn, Keep)>,
    limit: Option<usize>,
    offset: usize,
}

impl ReadOptions {
    fn windowed(&self) -> bool {
        self.limit.is_some() || self.offset > 0
    }
}

/// The `encoding`/`projection` options of an Arrow export, parsed by core's
/// own `FromStr` impls so an error reads the same from every frontend.
struct Export {
    encoding: TermEncoding,
    projection: Option<Vec<QuadColumn>>,
}

fn parse_export(encoding: &str, projection: Option<Vec<String>>) -> PyResult<Export> {
    let encoding: TermEncoding = encoding.parse().map_err(parse_err)?;
    let projection: Option<Vec<QuadColumn>> = projection
        .map(|names| names.iter().map(|name| name.parse()).collect())
        .transpose()
        .map_err(parse_err)?;
    Ok(Export {
        encoding,
        projection,
    })
}

/// Every pattern of a batch parsed up front, so a malformed one raises
/// `ValueError` before anything is evaluated.
fn parse_patterns(patterns: &[PyPattern]) -> PyResult<Vec<Pattern>> {
    patterns
        .iter()
        .map(|(s, p, o, g)| {
            parse_pattern_checked(s.as_deref(), p.as_deref(), o.as_deref(), g.as_deref())
                .map_err(parse_err)
        })
        .collect()
}

/// The `keep` argument — a mapping from column name to a `range` (a code
/// range) or to codes in any form `decode_many` accepts (a code set) — as
/// core constraints, in column order.
fn parse_keep(
    py: Python<'_>,
    keep: Option<HashMap<String, Bound<'_, PyAny>>>,
) -> PyResult<Vec<(QuadColumn, Keep)>> {
    let Some(keep) = keep else {
        return Ok(Vec::new());
    };
    let mut constraints = Vec::with_capacity(keep.len());
    for (name, value) in keep {
        let column: QuadColumn = name.parse().map_err(parse_err)?;
        let keep = match value.cast::<PyRange>() {
            Ok(range) => {
                if range.step()? != 1 {
                    return Err(PyValueError::new_err(format!(
                        "keep range for column {name:?} must have step 1"
                    )));
                }
                let bound = |v: isize| {
                    u32::try_from(v).map_err(|_| {
                        PyValueError::new_err(format!(
                            "keep range for column {name:?} is outside the u32 code space"
                        ))
                    })
                };
                Keep::range(bound(range.start()?)?, bound(range.stop()?)?)
            }
            Err(_) => Keep::set(codes_from_py(py, &value)?),
        };
        constraints.push((column, keep));
    }
    constraints.sort_by_key(|(column, _)| column.index());
    Ok(constraints)
}

/// One row of [`VortexRdfStore::get_quads`]: subject, predicate, object, graph.
/// Held as `Py<PyString>` so a term repeated down a column is one Python object
/// shared by every row that uses it.
type PyQuad = (Py<PyString>, Py<PyString>, Py<PyString>, Py<PyString>);

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

    /// The view matching `pattern`, narrowed by `options` — its `keep`
    /// constraints, then its row window.
    async fn matched_with(
        &self,
        pattern: &Pattern,
        options: &ReadOptions,
    ) -> Result<CoreStore, CoreError> {
        let mut view = self.matched(pattern).await?;
        for (column, keep) in &options.keep {
            view = view.keep(*column, keep).await?;
        }
        if options.windowed() {
            view = view
                .window(options.offset, options.limit.unwrap_or(usize::MAX))
                .await?;
        }
        Ok(view)
    }

    /// The matched rows as `(s, p, o, g)` term-code columns, gathered off the
    /// GIL, or `None` when the match declines the code path.
    fn matched_code_columns(
        &self,
        py: Python<'_>,
        pattern: &Pattern,
        options: &ReadOptions,
    ) -> PyResult<Option<[Buffer<u32>; 4]>> {
        py.detach(|| -> Result<_, CoreError> {
            RUNTIME.block_on(async {
                self.matched_with(pattern, options)
                    .await?
                    .code_columns_gathered()
                    .await
            })
        })
        .map_err(store_err)
    }

    /// The matched rows as four columns of N-Triples strings, in
    /// subject-predicate-object-graph order. The default graph is the empty
    /// string, the spelling `parse_pattern_checked` accepts for it.
    ///
    /// Backs [`Self::get_quads`].
    fn matched_columns(
        &self,
        py: Python<'_>,
        pattern: &Pattern,
    ) -> PyResult<[Vec<Py<PyString>>; 4]> {
        if let Some(snapshot) = self.store.code_read_snapshot() {
            // `code_read_snapshot` reports only that the path can apply; the
            // match itself still decides, so fall through when it declines.
            if let Some(codes) = self.matched_code_columns(py, pattern, &ReadOptions::default())? {
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

/// The resident form of an adopted store's term dictionary when the caller
/// names none (see [`parse_dict_form`]).
const DEFAULT_DICT_FORM: DictForm = DictForm::AsWritten;

/// The `dictionary=` argument resolved through core's names: `"as-written"`
/// keeps the file's chunks (FSST, decoded one term per read), `"plaintext"`
/// decodes the column once into one canonical form; `None` is the binding's
/// default.
fn parse_dict_form(name: Option<&str>) -> PyResult<DictForm> {
    match name {
        None => Ok(DEFAULT_DICT_FORM),
        Some(name) => name.parse().map_err(parse_err),
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
    /// size in bytes). `dictionary` picks the resident form of an in-memory
    /// store's term dictionary (see [`parse_dict_form`]) and applies to
    /// `in_memory=True` only.
    #[new]
    #[pyo3(signature = (path, max_resident_bytes=None, in_memory=false, dictionary=None))]
    fn new(
        py: Python<'_>,
        path: PathBuf,
        max_resident_bytes: Option<u64>,
        in_memory: bool,
        dictionary: Option<&str>,
    ) -> PyResult<Self> {
        // Core reports a missing path as `VortexRdfError::Vortex`, not `Io`,
        // so the `FileNotFoundError` contract is honoured here.
        if !path.is_file() {
            return Err(PyFileNotFoundError::new_err(format!(
                "no such Vortex file: {}",
                path.display()
            )));
        }
        if dictionary.is_some() && !in_memory {
            return Err(PyValueError::new_err(
                "dictionary= picks the resident form of an in-memory store; \
                 a file-backed open keeps the file's own form (use in_memory=True)",
            ));
        }
        let form = parse_dict_form(dictionary)?;
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
                        CoreStore::from_parts_as(parts, form)
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
    /// accepted but extracted element by element. `dictionary` picks the
    /// resident form of the term dictionary (see [`parse_dict_form`]).
    #[staticmethod]
    #[pyo3(signature = (data, dictionary=None))]
    fn from_bytes(py: Python<'_>, data: Vec<u8>, dictionary: Option<&str>) -> PyResult<Self> {
        let form = parse_dict_form(dictionary)?;
        let store = py
            .detach(|| RUNTIME.block_on(CoreStore::from_bytes_owned_as(data, form)))
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
    /// selection alone -- no term is materialized into Python. With `limit`
    /// the count stops there: `count_quads(..., limit=1)` is an existence
    /// test that reads no further than its first row.
    #[pyo3(signature = (s=None, p=None, o=None, g=None, *, limit=None))]
    fn count_quads(
        &self,
        py: Python<'_>,
        s: Option<&str>,
        p: Option<&str>,
        o: Option<&str>,
        g: Option<&str>,
        limit: Option<usize>,
    ) -> PyResult<usize> {
        let pattern = parse_pattern_checked(s, p, o, g).map_err(parse_err)?;
        py.detach(|| -> Result<usize, CoreError> {
            RUNTIME.block_on(async {
                let view = self.matched(&pattern).await?;
                match limit {
                    Some(limit) => view.size_capped(limit).await,
                    None => view.size().await,
                }
            })
        })
        .map_err(store_err)
    }

    /// `count_quads` for a batch of `(s, p, o, g)` patterns in one call:
    /// every pattern is parsed first (a malformed one raises `ValueError`
    /// before anything is evaluated), the matches run concurrently under one
    /// GIL release, and the counts come back in input order.
    fn count_quads_many(&self, py: Python<'_>, patterns: Vec<PyPattern>) -> PyResult<Vec<usize>> {
        let patterns = parse_patterns(&patterns)?;
        py.detach(|| -> Result<Vec<usize>, CoreError> {
            RUNTIME.block_on(async {
                let views = self.store.match_pattern_many(&patterns).await?;
                try_join_all(views.iter().map(|view| view.size())).await
            })
        })
        .map_err(store_err)
    }

    /// The store's term dictionary, or `None` when the code path does not
    /// apply: a non-Dictionary layout, a non-resident (file-backed)
    /// dictionary, or an append tail whose quads are not in the cached
    /// dictionary. Pair with [`Self::match_arrow`]; decode each distinct
    /// code once, caching on the Python side.
    fn term_dict(&self) -> Option<TermDict> {
        self.store
            .code_read_snapshot()
            .map(|snapshot| TermDict { snapshot })
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
    /// layout; the TypedObject layout has no Arrow export.
    ///
    /// `keep` restricts positions by term code before any row is gathered:
    /// a mapping from column name (`"s"`, `"p"`, `"o"`, `"g"`) to a `range`
    /// of codes (what `TermDict.prefix_range` yields) or to a set of codes
    /// in any form `TermDict.decode_many` accepts — a `U32Column`, a
    /// `uint32` Arrow array, a buffer, a sequence of ints. `limit`/`offset`
    /// window the rows in match order, after `keep`. Batches are produced as
    /// the consumer pulls them, off the GIL wherever the consumer releases it.
    #[pyo3(signature = (s=None, p=None, o=None, g=None, *, encoding="codes", projection=None, keep=None, limit=None, offset=0))]
    // The parameter list is the Python signature: four pattern positions plus
    // the keyword options.
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
        keep: Option<HashMap<String, Bound<'_, PyAny>>>,
        limit: Option<usize>,
        offset: usize,
    ) -> PyResult<ArrowQuadStream> {
        let pattern = parse_pattern_checked(s, p, o, g).map_err(parse_err)?;
        let export = parse_export(encoding, projection)?;
        let options = ReadOptions {
            keep: parse_keep(py, keep)?,
            limit,
            offset,
        };
        let batches = py
            .detach(|| -> Result<_, CoreError> {
                RUNTIME.block_on(async {
                    self.matched_with(&pattern, &options)
                        .await?
                        .to_record_batches(export.encoding, export.projection.as_deref())
                        .await
                })
            })
            .map_err(store_err)?;
        Ok(ArrowQuadStream::new(batches, export.encoding))
    }

    /// `match_arrow` for a batch of `(s, p, o, g)` patterns in one call —
    /// every pattern parsed first (a malformed one raises `ValueError`
    /// before anything is evaluated), the matches run concurrently under one
    /// GIL release, one stream per pattern in input order, all under the
    /// same `encoding` and `projection`.
    #[pyo3(signature = (patterns, *, encoding="codes", projection=None))]
    fn match_arrow_many(
        &self,
        py: Python<'_>,
        patterns: Vec<PyPattern>,
        encoding: &str,
        projection: Option<Vec<String>>,
    ) -> PyResult<Vec<ArrowQuadStream>> {
        let patterns = parse_patterns(&patterns)?;
        let export = parse_export(encoding, projection)?;
        let streams = py
            .detach(|| -> Result<_, CoreError> {
                RUNTIME.block_on(async {
                    let views = self.store.match_pattern_many(&patterns).await?;
                    try_join_all(views.iter().map(|view| {
                        view.to_record_batches(export.encoding, export.projection.as_deref())
                    }))
                    .await
                })
            })
            .map_err(store_err)?;
        Ok(streams
            .into_iter()
            .map(|batches| ArrowQuadStream::new(batches, export.encoding))
            .collect())
    }
}
