// The submodules are crate-private: every public item below is re-exported
// here (or at the crate root), so each has exactly one canonical public path.
pub(crate) mod array;
pub(crate) mod builders;
mod construct;
pub(crate) mod indexes;
pub(crate) mod layouts;
pub(crate) mod persist;
pub(crate) mod probes;
pub(crate) mod query;
pub(crate) mod read;
pub(crate) mod resident;
pub(crate) mod scan;
pub(crate) mod schema;
#[cfg(test)]
pub(crate) mod test_hooks;
pub(crate) mod view;
pub(crate) mod write;

pub use builders::{BuiltArray, SortedInMemoryBuilder, VortexArrayBuilder};
// Compiled out on wasm along with the rest of the sorted-stream builder's
// out-of-core merge (see the module gate in `builders`).
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub use builders::SortedStreamBuilder;
pub use indexes::{IndexType, Indexes};
pub use layouts::LayoutStrategy;
pub use layouts::dictionary::DictionaryQuadSink;
pub use layouts::dictionary::{
    CaseMap, DictReader, DictSnapshot, KindRanges, NumOp, TermPredicate, TextOptions, Verdict,
};
pub use persist::export::export_rdf;
pub use query::batch::Probe;
pub(crate) use query::matching::IdsNeed;
pub use query::narrowing::Keep;
pub use read::chunks::{CodeChunkStream, RowChunkStream};
pub use read::metadata::{
    IndexComponentInfo, RowCountHint, SelectionKind, SortOrder, ViewStatistics,
};
pub use schema::{QuadColumn, RowId, TermCode};
// `RawQuad` lives in `common` (it is pure RDF text — see that module's
// charter); this re-export makes `store::RawQuad` the path builder consumers
// use.
pub use crate::common::quad::{RawQuad, SharedQuad};

pub(crate) use view::{QuadsSource, Tail};

/// The next store generation (see [`VortexRdfStore::generation`]), from one
/// process-wide counter: two stores share a generation only when one was
/// derived from the other without a change to its data.
pub(crate) fn next_generation() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

use crate::error::{Result, VortexRdfError};
use layouts::ResolvedLayout;
use view::selection::RowSelection;

use std::sync::Arc;

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
    /// The identity of the data behind this view: see
    /// [`generation`](Self::generation).
    generation: u64,
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

/// A store's serializable state, split the way the store holds it: a
/// [`BuiltArray`] of the primary quad rows, the in-memory index components
/// describing them, and — under the Dictionary layout — the term dictionary
/// the rows' codes address, plus the rows' sortedness. Produced by
/// [`VortexRdfStore::to_serializable_parts`] and adopted back by
/// [`VortexRdfStore::from_parts`] (the bindings' in-memory round-trip).
pub struct StoreParts {
    pub(crate) built: BuiltArray,
    /// Whether the rows are in global `(s, p, o, g)` order; written as the
    /// root's `quads_sorted` (see `WireMetadata::quads_sorted`).
    #[cfg_attr(
        not(any(feature = "file-io", target_arch = "wasm32")),
        allow(dead_code)
    )]
    pub(crate) quads_sorted: bool,
}

impl VortexRdfStore {
    /// A view over the same data as this store — its layout, indexes and
    /// [`generation`](Self::generation) — reading `quads` and `tail`: the one
    /// constructor every derived view goes through. A mutation, whose data
    /// differs, takes a fresh generation over it.
    #[inline]
    pub(crate) fn derived(&self, quads: QuadsSource, tail: Option<Tail>) -> Self {
        Self {
            layout: self.layout.clone(),
            indexes: self.indexes.clone(),
            generation: self.generation,
            quads,
            tail,
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
    pub(crate) fn empty_view(&self) -> Self {
        let tail = self
            .tail
            .as_ref()
            .map(|tail| tail.with_selection(RowSelection::empty()));
        Self {
            indexes: vec![],
            ..self.derived(self.quads.emptied(), tail)
        }
    }

    /// Whether this view's rows are code-addressable: the Dictionary layout
    /// with an empty append tail (tail rows hold terms as strings, which have
    /// no codes in the dictionary).
    pub(crate) fn is_code_view(&self) -> bool {
        self.layout.strategy() == LayoutStrategy::Dictionary && self.tail_len() == 0
    }

    /// Err unless this view's rows are code-addressable
    /// ([`is_code_view`](Self::is_code_view)).
    pub(in crate::store) fn ensure_code_view(&self, operation: &str) -> Result<()> {
        if self.layout.strategy() != LayoutStrategy::Dictionary {
            return Err(VortexRdfError::InvalidOperation(format!(
                "{operation} needs the Dictionary layout: this store's {:?} layout stores terms \
                 as strings, which have no codes to keep",
                self.layout.strategy()
            )));
        }
        self.ensure_no_dictionary_tail(operation)
    }

    /// Err for a Dictionary-layout view with a non-empty append tail, whose
    /// rows have no single dtype: the tail holds terms the dictionary has no
    /// codes for. Every other view passes.
    pub(in crate::store) fn ensure_no_dictionary_tail(&self, operation: &str) -> Result<()> {
        if self.layout.strategy() == LayoutStrategy::Dictionary && self.tail_len() != 0 {
            return Err(VortexRdfError::InvalidOperation(format!(
                "{operation} needs an empty append tail: the {} appended rows hold terms the \
                 dictionary has no codes for; compact() the store first",
                self.tail_len()
            )));
        }
        Ok(())
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
        if !self.is_code_view() {
            return None;
        }
        self.dictionary_snapshot()
    }

    /// A handle on this store's term dictionary under either residency
    /// ([`DictReader`]), gated like
    /// [`code_read_snapshot`](Self::code_read_snapshot) — a Dictionary layout
    /// and an empty tail — but not on residency: a file-backed dictionary
    /// answers through the handle by reading its child on demand, where the
    /// snapshot is `None`. The codes [`code_columns_gathered`] serves are
    /// decodable against it either way.
    ///
    /// [`code_columns_gathered`]: Self::code_columns_gathered
    pub fn dict_reader(&self) -> Option<DictReader> {
        match &self.layout {
            ResolvedLayout::Dictionary(access) if self.is_code_view() => Some(access.reader()),
            _ => None,
        }
    }
}
