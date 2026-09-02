// The submodules are crate-private: every public item below is re-exported
// here (or at the crate root), so each has exactly one canonical public path.
pub(crate) mod array;
pub(crate) mod builders;
pub(crate) mod indexes;
pub(crate) mod layouts;
#[cfg(feature = "file-io")]
pub(crate) mod native_file;
pub(crate) mod probes;
pub(crate) mod scan;
pub(crate) mod schema;
pub(crate) mod selection;
pub(crate) mod source;

// [`VortexRdfStore`]'s impl clusters — the struct itself is defined below.
mod batches;
mod compaction;
mod export;
mod matching;
mod mutation;
mod open;
mod pushdown;
mod rows;
mod serialize;
mod streaming;
#[cfg(test)]
pub(crate) mod test_hooks;

pub use builders::{
    BuiltArray, BuiltStream, ChunkStream, SortedInMemoryBuilder, VortexArrayBuilder,
};
pub use export::export_rdf;
// Compiled out on wasm along with the rest of the sorted-stream builder's
// out-of-core merge (see the module gate in `builders`).
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub use builders::SortedStreamBuilder;
pub use indexes::{IndexType, Indexes};
pub use layouts::LayoutStrategy;
pub use layouts::dictionary::{DictSnapshot, NumOp, TermPredicate, Verdict};
pub use pushdown::Keep;
pub use layouts::dictionary::DictionaryQuadSink;
// `RawQuad` lives in `common` (it is pure RDF text — see that module's
// charter); this re-export makes `store::RawQuad` the path builder consumers
// use.
pub use crate::common::quad::{RawQuad, SharedQuad};

pub(crate) use source::{QuadsSource, Tail};

use indexes::IndexComponent;

use crate::error::{Result, VortexRdfError};
use layouts::dictionary::TermDictionary;
use layouts::{DictAccess, ResolvedLayout};
use selection::{RowSelection, ViewSelection};

use std::iter;
use std::sync::Arc;

use futures::Stream;

use vortex_array::arrays::StructArray;
use vortex_array::dtype::FieldNames;
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray};

/// An RDF quad store over a Vortex array or file. A store is a base (in
/// memory or a file) plus a view over it — a row selection, tombstones, and an
/// append tail — and the layout and secondary indexes the base was built
/// with; [`match_pattern`](Self::match_pattern) derives narrower views that
/// share the base, and mutation is owner-only ([`owned`](Self::owned)).
#[derive(Clone)]
pub struct VortexRdfStore {
    /// The store's backing quad data, either an in-memory array or a lazily
    /// scanned Vortex file, together with the row selection, filters, and
    /// tombstones that define the rows visible through this store or view.
    quads: QuadsSource,
    /// The layout resolved against the backing array, carrying any state
    /// intrinsic to it (the Dictionary layout's term dictionary is loaded
    /// once at construction and propagated to derived stores, which may have
    /// lost the payload row through slicing/filtering).
    layout: ResolvedLayout,
    /// The secondary indexes this store can route through, read at
    /// construction off the roster of components it holds (or, file-backed,
    /// its index children) — see `indexes_from_components`. Pattern matching
    /// plans index lookups against this set.
    ///
    /// Views derived through `match_pattern` keep their indexes: a view narrows
    /// a [`RowSelection`] over the base and never rewrites rows, so the
    /// components' `rid` columns still address the base the ids were built
    /// against. Only physically gathering the rows — which renumbers them from
    /// zero, as [`compact_with_indexes`] does — invalidates those ids; it
    /// rebuilds the index set over the new order.
    ///
    /// [`compact_with_indexes`]: Self::compact_with_indexes
    indexes: Indexes,
    /// Rows appended since construction ([`add_quads`]), kept outside the base
    /// so appending never rewrites it — which is what lets the base's indexes
    /// and tombstones survive an append. `None` until something is appended.
    /// Queries run the base's fast paths plus a mask scan over the tail and
    /// union the two; [`compact_with_indexes`] folds the tail back in.
    ///
    /// [`add_quads`]: Self::add_quads
    /// [`compact_with_indexes`]: Self::compact_with_indexes
    tail: Option<Tail>,
}

/// A store's serializable state, split the way the store holds it: the
/// primary quad rows, the in-memory index components describing them, and —
/// under the Dictionary layout — the term dictionary the rows' codes address.
/// Produced by [`VortexRdfStore::to_serializable_parts`] and adopted back by
/// [`VortexRdfStore::from_parts`] (the bindings' in-memory round-trip).
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

/// The layout an in-memory construction (`from_parts`, `from_built`,
/// `from_bytes`, compaction's `from_raw_quads`) resolves to: a dictionary
/// held beside the rows makes it Dictionary-resident, otherwise the array's
/// own dtype decides. A dict-less Dictionary-layout array is rejected: bare
/// code columns carry no way back to their terms.
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

/// The compressed-resident form every in-memory construction produces: the
/// base's u32 code columns and each component's integer children are
/// re-encoded into probe-supported encodings (see
/// [`with_compressed_int_children`](array::with_compressed_int_children)),
/// with the base additionally payload-wrapped so the code-column read path
/// keeps its zero-copy fast path. Shared by the builder adoption
/// (`from_built`) and compaction's rebuild (`from_raw_quads`) — the two
/// places canonical built columns become a store.
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
    /// call. Callers who need neither a particular builder nor its
    /// intermediate [`BuiltArray`] should prefer this over pairing
    /// `VortexArrayBuilder::build_vortex_array` with
    /// [`from_built`](Self::from_built).
    ///
    /// The builder is picked by target: the out-of-core `SortedStreamBuilder`
    /// wherever a filesystem exists, so the sort itself is not bounded by
    /// memory, and `SortedInMemoryBuilder` on wasm, which has none. Both
    /// produce the same globally sorted rows; name one directly to override
    /// the choice.
    ///
    /// The finished store is resident either way. Writing a dataset larger
    /// than memory never has to materialize it — `io::quads_stream_to_vortex_file`
    /// streams the builder's chunks straight into the file writer instead.
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

    /// Rebuild a store from [`StoreParts`] — the inverse of
    /// [`to_serializable_parts`](Self::to_serializable_parts). A
    /// Dictionary-layout quad array must arrive with its dictionary beside
    /// it: dict-less Dictionary parts are refused (bare code columns carry no
    /// way back to their terms).
    ///
    /// This is the bindings' explicit resident adoption: the base's integer
    /// children stay in their compressed form wherever the match fast paths
    /// can bind them through encoded search probes, and only children outside
    /// the probe's supported set are decoded to canonical primitives (see
    /// `with_searchable_int_children`);
    /// every index component is materialized into the same resident form, so
    /// its sorted probes bind directly too.
    ///
    /// The array's statistics are trusted as provenance: an `IsSorted` stamp
    /// on its `s` column asserts the rows are in global `(s, p, o, g)` order —
    /// the order every builder of this crate produces — and the match fast
    /// paths search every bound role on that basis. Rows sorted by subject
    /// alone must not carry it.
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

    /// Build from a builder's output: the primary quad array plus whatever the
    /// builder carries beside it — the Dictionary layout's term dictionary and
    /// the requested indexes' components, both adopted as they are. Nothing is
    /// re-derived or split out of the rows.
    pub fn from_built(built: BuiltArray) -> Result<Self> {
        let layout = resolved_layout(built.dict, built.array.dtype())?;
        let (base, components) = compress_built_parts(built.array, built.components)?;
        Self::assemble_resident(base, components, layout)
    }

    /// Assemble a store from an already-split primary base plus its
    /// components — the shared tail of every in-memory construction path.
    ///
    /// Callers own the resident form of what they pass: construction sites
    /// compress first ([`compress_built_parts`]), the adoption site keeps
    /// wire encodings selectively
    /// ([`with_searchable_int_children`](array::with_searchable_int_children));
    /// this assembler transforms nothing.
    fn assemble_resident(
        base: ArrayRef,
        components: Vec<IndexComponent>,
        layout: ResolvedLayout,
    ) -> Result<Self> {
        let components: Arc<[IndexComponent]> = components.into();
        // The queryable index set follows the component roster, exactly as
        // the file path follows its child roster.
        let indexes = crate::store::indexes::indexes_from_components(&components);
        // Resolve the encoded-search probes at construction, so no query pays
        // the encoding-tree walk.
        let store_probes = probes::StructProbes::new();
        store_probes.warm(&base);
        for component in components.iter() {
            component.warm_probes();
        }
        Ok(Self {
            layout,
            indexes,
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
        // Build one empty string column and reuse it for all four fields —
        // they're all zero-length anyway, so there's nothing to distinguish.
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

    /// An empty view of this store: same base (and tail), selecting no row of
    /// either.
    ///
    /// Scans over it plan no work and `size()` answers 0 without touching the
    /// data. Indexes and components are dropped: chained matches on an empty
    /// view would otherwise run pointless lookups just to intersect with
    /// nothing. No serve plan carries across — a plan is only valid while its
    /// row run is exactly the selection.
    fn empty_view(&self) -> Self {
        let quads = match &self.quads {
            QuadsSource::InMemory {
                base,
                deleted,
                probes,
                ..
            } => QuadsSource::InMemory {
                base: base.clone(),
                selection: ViewSelection::Exact(RowSelection::empty()),
                components: Arc::from(Vec::new()),
                deleted: deleted.clone(),
                probes: Arc::clone(probes),
                serve: None,
            },
            #[cfg(feature = "file-io")]
            QuadsSource::File {
                path,
                dict_max_resident_bytes,
                file,
                filter,
                deleted,
                ..
            } => QuadsSource::File {
                path: path.clone(),
                dict_max_resident_bytes: *dict_max_resident_bytes,
                file: file.clone(),
                filter: filter.clone(),
                selection: ViewSelection::Exact(RowSelection::empty()),
                deleted: deleted.clone(),
                serve: None,
            },
        };
        let tail = self
            .tail
            .as_ref()
            .map(|tail| tail.with_selection(RowSelection::empty()));
        Self {
            layout: self.layout.clone(),
            indexes: vec![],
            quads,
            tail,
        }
    }

    /// The layout the tail's rows are stored in: the store's own, except under
    /// the Dictionary layout, where an appended term has no code in the sorted
    /// dictionary, so the tail holds Default-layout strings instead — patterns
    /// probe the base by code and the tail by string.
    fn tail_layout(&self) -> ResolvedLayout {
        match &self.layout {
            ResolvedLayout::Dictionary(_) => ResolvedLayout::Default,
            other => other.clone(),
        }
    }

    // ── ownership & compaction policy ────────────────────────────────────────

    /// Number of physical rows in the append tail (including any tombstoned
    /// since they were appended); `0` when nothing has been appended or the
    /// tail has been compacted away.
    ///
    /// The tail is the store's only unindexed, unsorted region, so this is the
    /// number to watch when tuning compaction: `add_quads` folds it back into
    /// the base automatically once it crosses the thresholds — rewriting the
    /// source file for a file-backed store — and [`compact`](Self::compact)
    /// folds it on demand.
    pub fn tail_len(&self) -> usize {
        self.tail.as_ref().map_or(0, |tail| tail.rows.len())
    }

    /// This store as one that owns its rows and can be mutated — cheaply when it
    /// already is an owner, otherwise an independent, compacted copy.
    ///
    /// A view derived from `match_pattern` shares a base it does not own, so it
    /// cannot be mutated in place. This turns such a view into an owner by
    /// compacting, rebuilding its declared indexes
    /// ([`compact_with_indexes`]) so mutating a match result yields an
    /// independent store that is still indexed. An owner is returned as a
    /// cheap clone, preserving its tombstones and indexes, so repeated
    /// in-place deletes stay cheap and keep their indexes.
    ///
    /// [`compact_with_indexes`]: Self::compact_with_indexes
    pub async fn owned(&self) -> Result<Self> {
        if self.is_owner() {
            Ok(self.clone())
        } else {
            self.compact_with_indexes(self.indexes.clone()).await
        }
    }

    /// Whether this store owns its rows, as opposed to being a window onto
    /// someone else's.
    ///
    /// Only an owner may be mutated: a narrowed view's rows are a subset of a
    /// base it shares, so mutating it would either silently discard the rows
    /// outside the view or write through to data it doesn't own. A view that
    /// happens to select everything (an unconstrained `match_pattern`) covers
    /// exactly the same rows as the store it came from, so it counts as an
    /// owner — mutating it is indistinguishable from mutating that store.
    fn is_owner(&self) -> bool {
        // A narrowed tail marks a view just as a narrowed base does — a match
        // may cover all base rows yet only some of the tail's.
        let tail_owned = self
            .tail
            .as_ref()
            .is_none_or(|tail| matches!(tail.selection, RowSelection::All));
        tail_owned && self.quads.is_unrefined()
    }

    /// Err unless this store owns its rows — the gate every mutation takes;
    /// see [`is_owner`](Self::is_owner).
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

    /// The layout strategy this store's rows are stored in (the build-time
    /// tag, independent of whether the Dictionary layout's dictionary is
    /// resident or file-backed).
    pub fn layout(&self) -> LayoutStrategy {
        self.layout.strategy()
    }

    /// An immutable handle on this store's term dictionary, or `None` when the
    /// store is not Dictionary-layout or the dictionary is not resident.
    ///
    /// Taking a snapshot is O(1) and retains only the dictionary — not the
    /// store, nor its quad columns. Crate-internal: the public accessor is
    /// [`code_read_snapshot`](Self::code_read_snapshot), which additionally
    /// gates on the codes this store's arrays serve being decodable.
    pub(crate) fn dictionary_snapshot(&self) -> Option<DictSnapshot> {
        match &self.layout {
            ResolvedLayout::Dictionary(access) => {
                access.resident().map(|dict| DictSnapshot(Arc::clone(dict)))
            }
            _ => None,
        }
    }

    /// An immutable handle on this store's term dictionary ([`DictSnapshot`]),
    /// gated on the codes this store's arrays serve being decodable against
    /// it — the one public dictionary accessor, and the one implementation of
    /// the code-read invariant, for every frontend that pairs a code-typed
    /// read
    /// (`code_columns`/[`code_columns_gathered`](Self::code_columns_gathered))
    /// with a decode handle. Taking a snapshot is O(1) and retains only the
    /// dictionary — not the store, nor its quad columns. Hand it to any
    /// consumer that holds term codes: see [`DictSnapshot`] for why the store
    /// itself is the wrong thing to decode against.
    ///
    /// `Some` requires all three of:
    /// - Dictionary layout — no other layout has codes at all;
    /// - an empty append tail — tail rows store terms as *strings* (an
    ///   appended term has no code in the sorted dictionary), so a tailed
    ///   view's rows are not fully code-addressable and gathering them
    ///   re-encodes against a fresh dictionary this handle would not be (see
    ///   `selected_rows`'s contract);
    /// - a resident dictionary — a file-backed one has no in-memory snapshot
    ///   to hand out.
    ///
    /// Decoding codes against anything less than all three yields silently
    /// wrong terms, so bindings take this gate as a whole.
    pub fn code_read_snapshot(&self) -> Option<DictSnapshot> {
        if self.tail_len() != 0 {
            return None;
        }
        self.dictionary_snapshot()
    }
}
