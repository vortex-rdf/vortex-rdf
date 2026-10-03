// The submodules are crate-private; their public items are re-exported here
// or at the crate root.
pub(crate) mod array;
pub(crate) mod builders;
pub(crate) mod indexes;
pub(crate) mod layouts;
pub(crate) mod persist;
pub(crate) mod probes;
pub(crate) mod query;
pub(crate) mod read;
pub(crate) mod scan;
pub(crate) mod schema;
#[cfg(test)]
pub(crate) mod test_hooks;
pub(crate) mod view;
pub(crate) mod write;

pub use builders::{
    BuiltArray, BuiltStream, ChunkStream, SortedInMemoryBuilder, VortexArrayBuilder,
};
// Compiled out on wasm with the out-of-core builder.
/// The column kernels, also reachable as `vortex_rdf_core::columns`.
pub use crate::columns;
pub use crate::common::quad::{RawQuad, SharedQuad};
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub use builders::SortedStreamBuilder;
pub use indexes::{IndexType, Indexes};
pub use layouts::LayoutStrategy;
pub use layouts::dictionary::DictionaryQuadSink;
pub use layouts::dictionary::{
    DictReader, DictSnapshot, Domain, KindRanges, NumOp, TermPredicate, Verdict,
};
pub use persist::export::export_rdf;
pub use query::batch::Probe;
pub use query::narrowing::Keep;
pub use read::chunks::{CodeChunkStream, RowChunkStream};
pub use read::data_source::{DATA_SOURCE_BATCH_ROWS, VortexRdfDataSource};
pub use read::metadata::{
    IndexComponentInfo, RowCountHint, SelectionKind, SortOrder, ViewStatistics,
};
pub use schema::QuadColumn;

pub(crate) use view::{QuadsSource, Tail};

/// The next store generation (see [`VortexRdfStore::generation`]): one
/// process-wide counter.
pub(crate) fn next_generation() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

use indexes::IndexComponent;

use crate::error::{Result, VortexRdfError};
use layouts::dictionary::TermDictionary;
use layouts::{DictAccess, ResolvedLayout};
use view::selection::{RowSelection, ViewSelection};

use std::iter;
use std::sync::Arc;

use futures::Stream;

use vortex_array::arrays::StructArray;
use vortex_array::dtype::FieldNames;
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray};

/// An RDF quad store over a Vortex array or file: a base (in memory or a
/// file) plus a view over it (a row selection, tombstones and an append
/// tail), and the layout and secondary indexes the base was built with.
/// [`match_pattern`](Self::match_pattern) derives narrower views that share
/// the base; mutation is owner-only ([`owned`](Self::owned)).
#[derive(Clone)]
pub struct VortexRdfStore {
    /// The base and the rows of it this view exposes.
    quads: QuadsSource,
    /// The layout resolved against the base; a Dictionary layout carries its
    /// term access.
    layout: ResolvedLayout,
    /// The secondary indexes pattern matching routes through, read off the
    /// component roster at construction. Views keep them: a view narrows a
    /// [`RowSelection`] and never renumbers rows, so the components' `rid`
    /// columns stay valid; [`compact_with_indexes`](Self::compact_with_indexes)
    /// rebuilds them over the gathered rows.
    indexes: Indexes,
    /// Identity of the data behind this view; see
    /// [`generation`](Self::generation).
    generation: u64,
    /// Rows appended since construction ([`add_quads`](Self::add_quads)),
    /// `None` until an append; [`compact_with_indexes`](Self::compact_with_indexes)
    /// folds them into the base.
    tail: Option<Tail>,
}

/// A store's serializable state: the primary quad rows, the index components
/// describing them and, under the Dictionary layout, the term dictionary the
/// rows' codes address. Produced by
/// [`VortexRdfStore::to_serializable_parts`] and adopted back by
/// [`VortexRdfStore::from_parts`].
pub struct StoreParts {
    pub(crate) array: ArrayRef,
    pub(crate) components: Vec<IndexComponent>,
    pub(crate) dict: Option<Arc<TermDictionary>>,
    /// Whether `array`'s rows are in global `(s, p, o, g)` order; written as
    /// the root's `quads_sorted` (see `WireMetadata::quads_sorted`).
    #[cfg_attr(
        not(any(feature = "file-io", target_arch = "wasm32")),
        allow(dead_code)
    )]
    pub(crate) quads_sorted: bool,
}

/// The layout an in-memory construction resolves to: Dictionary-resident
/// with a dictionary beside the rows, else by the array's dtype. A dict-less
/// Dictionary-layout array is rejected.
fn resolved_layout(
    dict: Option<Arc<TermDictionary>>,
    dtype: &vortex_array::dtype::DType,
) -> Result<ResolvedLayout> {
    match dict {
        Some(dict) => Ok(ResolvedLayout::Dictionary(DictAccess::Resident(dict))),
        None => match LayoutStrategy::from_dtype(dtype) {
            LayoutStrategy::TypedObject => Ok(ResolvedLayout::TypedObject),
            LayoutStrategy::Dictionary => Err(VortexRdfError::Deserialization(
                "Dictionary-layout rows carry no dictionary; a bare code array cannot \
                 self-describe — construct through a builder (`from_built`) or open a \
                 serialized form that carries its dictionary component"
                    .to_string(),
            )),
            LayoutStrategy::Default => Ok(ResolvedLayout::Default),
        },
    }
}

/// The compressed-resident form of a built base and its components: integer
/// children re-encoded into probe-supported encodings
/// ([`with_compressed_int_children`](array::with_compressed_int_children)),
/// the base payload-wrapped for the zero-copy code-column path.
fn compress_built_parts(
    base: ArrayRef,
    components: Vec<IndexComponent>,
) -> Result<(ArrayRef, Vec<IndexComponent>)> {
    let base = array::with_compressed_int_children(base, true)?;
    let components = components
        .into_iter()
        .map(IndexComponent::into_compressed)
        .collect::<Result<Vec<_>>>()?;
    Ok((base, components))
}

impl VortexRdfStore {
    // ── constructors ─────────────────────────────────────────────────────────

    /// Build a store from a quad stream: the global `(s, p, o, g)` sort, the
    /// layout's columns, the requested indexes and the store assembly in one
    /// call. The builder is the out-of-core `SortedStreamBuilder` wherever a
    /// filesystem exists and `SortedInMemoryBuilder` on wasm; name one
    /// directly ([`from_built`](Self::from_built)) to override. The finished
    /// store is resident; `io::quads_stream_to_vortex_file` streams a dataset
    /// to a file instead.
    pub async fn from_quads(
        quads: impl Stream<Item = Result<RawQuad>> + Unpin + Send + 'static,
        layout: LayoutStrategy,
        indexes: Indexes,
    ) -> Result<Self> {
        #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
        let built =
            SortedInMemoryBuilder::build_vortex_array(Box::new(quads), layout, indexes).await?;
        #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
        let built =
            SortedStreamBuilder::build_vortex_array(Box::new(quads), layout, indexes).await?;
        Self::from_built(built)
    }

    /// Rebuild a store from [`StoreParts`], the inverse of
    /// [`to_serializable_parts`](Self::to_serializable_parts). Dict-less
    /// Dictionary parts are refused. Integer children stay compressed
    /// wherever an encoded-search probe binds them and are decoded otherwise
    /// (`with_searchable_int_children`), components likewise. An `IsSorted`
    /// stamp on the `s` column is trusted as global `(s, p, o, g)` order; rows
    /// sorted by subject alone must not carry it.
    pub fn from_parts(parts: StoreParts) -> Result<Self> {
        let layout = resolved_layout(parts.dict, parts.array.dtype())?;
        let base = array::with_searchable_int_children(parts.array)?;
        let components = parts
            .components
            .into_iter()
            .map(IndexComponent::into_searchable)
            .collect::<Result<Vec<_>>>()?;
        Self::assemble_resident(base, components, layout)
    }

    /// Build from a builder's output: the primary quad array plus the term
    /// dictionary and index components it carries, adopted as they are.
    pub fn from_built(built: BuiltArray) -> Result<Self> {
        let layout = resolved_layout(built.dict, built.array.dtype())?;
        let (base, components) = compress_built_parts(built.array, built.components)?;
        Self::assemble_resident(base, components, layout)
    }

    /// Assemble a store from a primary base and its components, as given;
    /// the index set follows the component roster, and the encoded-search
    /// probes are resolved now.
    fn assemble_resident(
        base: ArrayRef,
        components: Vec<IndexComponent>,
        layout: ResolvedLayout,
    ) -> Result<Self> {
        let components: Arc<[IndexComponent]> = components.into();
        let indexes = crate::store::indexes::indexes_from_components(&components);
        let store_probes = probes::StructProbes::new();
        store_probes.warm(&base);
        for component in components.iter() {
            component.warm_probes();
        }
        Ok(Self {
            layout,
            indexes,
            generation: next_generation(),
            quads: QuadsSource::InMemory {
                base,
                selection: ViewSelection::all(),
                components,
                deleted: None,
                probes: store_probes,
                serve: None,
            },
            tail: None,
        })
    }

    /// Create an empty in-memory store with Default layout.
    pub fn empty() -> Self {
        // One empty string column serves all four fields.
        let e = array::make_string_array(iter::empty::<&str>());

        let quads = StructArray::try_new(
            FieldNames::from(schema::PRIMARY_COLUMNS),
            vec![e.clone(), e.clone(), e.clone(), e],
            0,
            Validity::NonNullable,
        )
        .expect("empty StructArray")
        .into_array();

        Self {
            layout: ResolvedLayout::Default,
            indexes: vec![],
            generation: next_generation(),
            quads: QuadsSource::InMemory {
                base: quads,
                selection: ViewSelection::all(),
                components: Arc::from(Vec::new()),
                deleted: None,
                probes: probes::StructProbes::new(),
                serve: None,
            },
            tail: None,
        }
    }

    /// A view over this store's layout, indexes and generation with `quads`
    /// and `tail`.
    pub(crate) fn derived(&self, quads: QuadsSource, tail: Option<Tail>) -> Self {
        Self {
            layout: self.layout.clone(),
            indexes: self.indexes.clone(),
            generation: self.generation,
            quads,
            tail,
        }
    }

    // ── ownership & compaction policy ────────────────────────────────────────

    /// Number of physical rows in the append tail, tombstoned ones included;
    /// `0` without a tail. `add_quads` folds the tail into the base once it
    /// crosses the auto-compaction thresholds (rewriting a file-backed
    /// store's source file); [`compact`](Self::compact) folds it on demand.
    pub fn tail_len(&self) -> usize {
        self.tail.as_ref().map_or(0, |tail| tail.rows.len())
    }

    /// This store as an owner that can be mutated: a cheap clone when it
    /// already owns its rows, else an independent compacted copy with its
    /// declared indexes rebuilt
    /// ([`compact_with_indexes`](Self::compact_with_indexes)).
    pub async fn owned(&self) -> Result<Self> {
        if self.is_owner() {
            Ok(self.clone())
        } else {
            self.compact_with_indexes(self.indexes.clone()).await
        }
    }

    /// Whether this store owns its rows: an unrefined base and an unnarrowed
    /// tail. Only an owner may be mutated; a view selecting everything counts
    /// as an owner.
    fn is_owner(&self) -> bool {
        let tail_owned = self
            .tail
            .as_ref()
            .is_none_or(|tail| matches!(tail.selection, RowSelection::All));
        tail_owned && self.quads.is_unrefined()
    }

    /// Err unless [`is_owner`](Self::is_owner).
    fn ensure_owner(&self, operation: &str) -> Result<()> {
        if self.is_owner() {
            return Ok(());
        }
        Err(VortexRdfError::InvalidOperation(format!(
            "{operation} is not supported on a store derived from match_pattern: its rows are a \
             view onto a larger base, so mutating it would either silently drop the rows outside \
             the view or write through to data it does not own. Call owned() for an \
             independent copy to mutate, or call the mutation on the store the view came from."
        )))
    }

    // ── accessors ─────────────────────────────────────────────────────────────

    /// The secondary indexes this store's schema carries.
    pub fn indexes(&self) -> &[IndexType] {
        &self.indexes
    }

    /// The layout strategy this store's rows are stored in.
    pub fn layout(&self) -> LayoutStrategy {
        self.layout.strategy()
    }

    /// The resident term dictionary of a Dictionary-layout store, ungated;
    /// the public accessor is [`code_read_snapshot`](Self::code_read_snapshot).
    pub(crate) fn dictionary_snapshot(&self) -> Option<DictSnapshot> {
        match &self.layout {
            ResolvedLayout::Dictionary(access) => {
                access.resident().map(|dict| DictSnapshot(Arc::clone(dict)))
            }
            _ => None,
        }
    }

    /// An immutable handle on this store's term dictionary ([`DictSnapshot`]),
    /// `Some` only when the codes this view serves
    /// ([`code_columns_gathered`](Self::code_columns_gathered)) decode
    /// against it: the Dictionary layout, an empty append tail (tail rows
    /// hold terms as strings, with no code in the dictionary) and a resident
    /// dictionary. Taking a snapshot is O(1) and retains only the dictionary.
    pub fn code_read_snapshot(&self) -> Option<DictSnapshot> {
        self.is_code_view()
            .then(|| self.dictionary_snapshot())
            .flatten()
    }

    /// A handle on this store's term dictionary under either residency
    /// ([`DictReader`]), gated like
    /// [`code_read_snapshot`](Self::code_read_snapshot) except that a
    /// file-backed dictionary answers too, by reading its child on demand.
    pub fn dict_reader(&self) -> Option<DictReader> {
        match &self.layout {
            ResolvedLayout::Dictionary(access) if self.is_code_view() => Some(access.reader()),
            _ => None,
        }
    }

    /// Whether this view's rows are code-addressable: the Dictionary layout
    /// with an empty append tail.
    pub(crate) fn is_code_view(&self) -> bool {
        self.layout.strategy() == LayoutStrategy::Dictionary && self.tail_len() == 0
    }

    /// Err unless [`is_code_view`](Self::is_code_view).
    pub(crate) fn ensure_code_view(&self, operation: &str) -> Result<()> {
        if self.layout.strategy() != LayoutStrategy::Dictionary {
            return Err(VortexRdfError::InvalidOperation(format!(
                "{operation} needs the Dictionary layout: this store's {:?} layout stores terms \
                 as strings, which have no codes to keep",
                self.layout.strategy()
            )));
        }
        self.ensure_no_dictionary_tail(operation)
    }

    /// Err for a Dictionary-layout view with a non-empty append tail; every
    /// other view passes.
    pub(crate) fn ensure_no_dictionary_tail(&self, operation: &str) -> Result<()> {
        if self.layout.strategy() == LayoutStrategy::Dictionary && self.tail_len() != 0 {
            return Err(VortexRdfError::InvalidOperation(format!(
                "{operation} needs an empty append tail: the {} appended rows hold terms the \
                 dictionary has no codes for; compact() the store first",
                self.tail_len()
            )));
        }
        Ok(())
    }
}
