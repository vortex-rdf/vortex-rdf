// Submodules are crate-private; their public items are re-exported here or
// at the crate root.
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

/// The column kernels, also reachable as `vortex_rdf_core::columns`.
pub use crate::columns;
pub use crate::common::quad::{RawQuad, SharedQuad};
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub use builders::SortedStreamBuilder;
pub use builders::{
    BuiltArray, BuiltStream, ChunkStream, SortedInMemoryBuilder, VortexArrayBuilder,
};
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

/// The next store generation, from one process-wide counter.
pub(crate) fn next_generation() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

use crate::error::{Result, VortexRdfError};
use layouts::ResolvedLayout;

use std::sync::Arc;

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
    /// Index types resolvable on this store, from its component roster;
    /// shared by views (row ids address the base).
    indexes: Indexes,
    /// Identity of the data behind this view.
    generation: u64,
    /// Rows appended since construction, `None` until an append.
    tail: Option<Tail>,
}

/// A store's serializable state: the built rows, components and dictionary,
/// and whether the rows are in global `(s, p, o, g)` order. Produced by
/// [`VortexRdfStore::to_serializable_parts`], adopted back by
/// [`VortexRdfStore::from_parts`].
pub struct StoreParts {
    pub(crate) built: BuiltArray,
    /// Whether the rows are in global `(s, p, o, g)` order; written as the
    /// root's `quads_sorted`.
    #[cfg_attr(
        not(any(feature = "file-io", target_arch = "wasm32")),
        allow(dead_code)
    )]
    pub(crate) quads_sorted: bool,
}

impl VortexRdfStore {
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

    // ── tail ─────────────────────────────────────────────────────────────────

    /// Number of physical rows in the append tail, tombstoned ones included;
    /// `0` without a tail.
    pub fn tail_len(&self) -> usize {
        self.tail.as_ref().map_or(0, |tail| tail.rows.len())
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

    /// An immutable handle on this store's term dictionary, `Some` only when
    /// this view's codes decode against it: the Dictionary layout, an empty
    /// append tail (tail rows hold terms as strings) and a resident
    /// dictionary. O(1); retains only the dictionary.
    pub fn code_read_snapshot(&self) -> Option<DictSnapshot> {
        self.is_code_view()
            .then(|| self.dictionary_snapshot())
            .flatten()
    }

    /// A [`DictReader`] on this store's term dictionary, resident or
    /// file-backed; gated like [`code_read_snapshot`](Self::code_read_snapshot)
    /// except that a file-backed dictionary answers too.
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
