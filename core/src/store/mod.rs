// This module and its submodules are crate-private: the public items below
// are re-exported at the crate root, each one's only public path. `construct`,
// `persist`, `query`, `read` and `write` hold `impl VortexRdfStore` blocks.
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
// The out-of-core builder needs a filesystem to spill to.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub use builders::SortedStreamBuilder;
pub use indexes::{IndexType, Indexes};
pub use layouts::LayoutStrategy;
pub use layouts::dictionary::DictionaryQuadSink;
pub use layouts::dictionary::{
    CaseMap, DictReader, DictSnapshot, KindRanges, NumOp, Number, TermPredicate, TextOptions,
    TextPredicate, Verdict,
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
// The quads are RDF text, so they live in `common`; the crate root re-exports
// them from here with the store's types.
pub use crate::common::quad::{RawQuad, SharedQuad};

#[cfg(feature = "file-io")]
pub(crate) use view::FileBase;
pub(crate) use view::{Backend, MemoryBase, QuadsSource, Tail};

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

/// An RDF quad store over a Vortex array or file. A store is a base (in
/// memory or a file) plus a view over it (a row selection, tombstones and an
/// append tail), with the layout and secondary indexes the base was built
/// with; [`match_pattern`](Self::match_pattern) derives narrower views that
/// share the base, and mutation is owner-only ([`owned`](Self::owned)).
#[derive(Clone)]
pub struct VortexRdfStore {
    /// The base (an in-memory array or a lazily scanned file) with the row
    /// selection and tombstones that define the base rows this view sees.
    quads: QuadsSource,
    /// The layout resolved against the base; a Dictionary layout carries its
    /// term access, shared by every derived view.
    layout: ResolvedLayout,
    /// The secondary indexes this store routes through, read at construction
    /// off its components (file-backed: its index children). A derived view
    /// keeps them, since it narrows a [`RowSelection`] and never renumbers
    /// rows, so the components' `rid`s still address the base; an empty view
    /// drops them. Only gathering the rows renumbers them
    /// ([`compact_with_indexes`]), which rebuilds the set over the new order.
    ///
    /// [`RowSelection`]: view::selection::RowSelection
    /// [`compact_with_indexes`]: Self::compact_with_indexes
    indexes: Indexes,
    /// The identity of the data behind this view: see
    /// [`generation`](Self::generation).
    generation: u64,
    /// Rows appended since construction ([`add_quads`]), `None` until an
    /// append. Kept outside the base, so an append never rewrites it and the
    /// base's indexes and tombstones survive; queries union the base's fast
    /// paths with a mask scan over the tail, and [`compact_with_indexes`]
    /// folds it back in.
    ///
    /// [`add_quads`]: Self::add_quads
    /// [`compact_with_indexes`]: Self::compact_with_indexes
    tail: Option<Tail>,
}

/// A store's serializable state, a [`BuiltArray`]: the primary quad rows, the
/// index components describing them and, under the Dictionary layout, the
/// term dictionary the codes address. The rows' sortedness travels as their
/// `s` column's stamp. Produced by [`VortexRdfStore::to_serializable_parts`]
/// and adopted back by [`VortexRdfStore::from_parts`] (the in-memory open,
/// `from_file_in_memory`).
pub(crate) struct StoreParts {
    pub(crate) built: BuiltArray,
}

impl VortexRdfStore {
    /// A view over the same data as this store (its layout, indexes and
    /// [`generation`](Self::generation)) reading `quads` and `tail`: the
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

    // ── tail ─────────────────────────────────────────────────────────────────

    /// The number of physical rows in the append tail, tombstoned ones
    /// included; `0` without a tail. The tail is the store's only unindexed,
    /// unsorted region: `add_quads` folds it into the base once it crosses the
    /// auto-compaction thresholds (rewriting a file-backed store's source
    /// file), and [`compact`](Self::compact) folds it on demand.
    pub fn tail_len(&self) -> usize {
        self.tail.as_ref().map_or(0, |tail| tail.rows.len())
    }

    // ── accessors ─────────────────────────────────────────────────────────────

    /// The secondary indexes this view routes through: those its store holds
    /// components for, kept by every derived view except an empty one (an
    /// empty match result), which carries none.
    pub fn indexes(&self) -> &[IndexType] {
        &self.indexes
    }

    /// The layout strategy this store's rows are stored in, whether a
    /// Dictionary layout's dictionary is resident or file-backed.
    pub fn layout(&self) -> LayoutStrategy {
        self.layout.strategy()
    }

    /// A [`DictReader`] on this store's term dictionary, resident or
    /// file-backed: what translates the codes
    /// [`code_columns_gathered`](Self::code_columns_gathered) serves. Taking
    /// one is O(1) and retains only the dictionary, not the store or its quad
    /// columns; [`DictSnapshot`] says why a holder of codes decodes against it
    /// rather than the store.
    ///
    /// `Some` requires both:
    /// - the Dictionary layout: no other layout has codes;
    /// - an empty append tail: tail rows hold terms as *strings* (an appended
    ///   term has no code in the sorted dictionary), and serializing a tailed
    ///   view re-encodes it against a fresh dictionary this handle is not.
    ///
    /// A view that fails either has codes this dictionary does not address,
    /// and decoding them against it yields wrong terms without an error, so
    /// bindings take this gate as a whole. A file-backed dictionary answers by
    /// reading its child on demand; [`DictReader::snapshot`] is a resident
    /// one's synchronous [`DictSnapshot`].
    pub fn dict_reader(&self) -> Option<DictReader> {
        match &self.layout {
            ResolvedLayout::Dictionary(access) if self.is_code_view() => Some(access.reader()),
            _ => None,
        }
    }
}
