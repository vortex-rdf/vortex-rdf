//! The builders' hub: the [`VortexArrayBuilder`] contract and its products
//! ([`BuiltArray`], and the crate-private `BuiltStream` the file writer
//! consumes), the primary chunk build ([`build_struct_array`]) and the
//! globally sorted index emission ([`build_components`]). `sorted_in_memory`
//! and `sorted_stream` are the two pipelines; `spill` (the sort runs) and
//! `stream_indexes` (the index children) back the out-of-core one.
//!
//! Which pipeline runs is a property of the target, never a caller's choice:
//! the out-of-core global sort (`SortedStreamBuilder`), whose peak memory does
//! not scale with the dataset, wherever a filesystem exists; the in-memory
//! one ([`SortedInMemoryBuilder`]) on `wasm32-unknown-unknown`, the only one
//! compiled there.
//!
//! A builder emits primary-only quad rows plus one *component* per requested
//! index's persisted child, the one form index data takes: the in-memory sort
//! builds them over the whole dataset ([`build_components`]), the out-of-core
//! one streams each family off its own spill-run merger. Both sort globally,
//! so their components are `sorted`, the provenance a reader binary-searches
//! on. The rows' own subject sortedness travels as the `s` column's
//! `IsSorted` stamp and is re-read rather than assumed: an append loses it.
//!
//! Quads are unique and terms canonical in what a build emits. The input
//! arrives as canonical [`RawQuad`]s, one spelling per RDF term, so interning
//! a spelling interns the term; and both sorts drop repeated quads right after
//! sorting, before any column, row id or index child is derived, so the rows
//! and every child describe the same deduplicated dataset. The rebuilds
//! (compaction, a mutated store's serialization) do the same in one place
//! (`sorted_live_raw_quads`) before they call [`build_parts_from_raws`].

use crate::error::{Result, VortexRdfError};
use crate::store::RawQuad;
use crate::store::array::stamp_is_sorted;
use crate::store::indexes::{
    ALL_INDEX_TYPES, IndexComponent, IndexInput, IndexType, Indexes, check_indexed_rows,
};
use crate::store::layouts::dictionary::TermDictionary;
use crate::store::layouts::{LayoutStrategy, dictionary};
use futures::{Stream, stream};
use std::future::Future;
use std::sync::Arc;
use vortex_array::arrays::StructArray;
use vortex_array::dtype::DType;
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray};

/// Number of quads per StructArray chunk in streaming/chunked builders.
pub(crate) const DEFAULT_CHUNK_ROWS: usize = 100_000;

/// Entries per spill run of the out-of-core build: quads per quad run, and
/// entries per run of each index family. A run's records are term ids and
/// ranks (16 bytes a quad, 8 or 20 an index entry), so a run of ten chunks'
/// worth is buffered in a few megabytes and a build opens a tenth as many
/// run files as runs of a chunk would.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) const DEFAULT_RUN_ROWS: usize = 1 << 20;

/// The out-of-core build's sizes: rows per emitted chunk (quad chunks and
/// index-child chunks alike) and entries per spill run.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
#[derive(Clone, Copy, Debug)]
pub(crate) struct BuildSizes {
    pub(crate) chunk_rows: usize,
    pub(crate) run_rows: usize,
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
impl BuildSizes {
    /// [`DEFAULT_CHUNK_ROWS`] and [`DEFAULT_RUN_ROWS`]: every build's.
    pub(crate) const DEFAULT: Self = Self {
        chunk_rows: DEFAULT_CHUNK_ROWS,
        run_rows: DEFAULT_RUN_ROWS,
    };

    /// One size for both, so a test of a few quads cuts several runs and
    /// several chunks.
    #[cfg(test)]
    pub(crate) const fn uniform(rows: usize) -> Self {
        Self {
            chunk_rows: rows,
            run_rows: rows,
        }
    }
}

/// A stream of StructArray chunks for the Vortex file writer. Items are
/// `VortexResult` because the writer polls the stream directly; builder
/// errors convert through `From<VortexRdfError> for VortexError`.
pub(crate) type ChunkStream = stream::BoxStream<'static, vortex_error::VortexResult<ArrayRef>>;

/// A built dataset: the quad array, the requested indexes' components and,
/// under the Dictionary layout, the term dictionary its code columns address
/// (a serialized file's `dictionary` child). Cloning is shallow, so one build
/// can be handed to
/// [`VortexRdfStore::from_built`](crate::VortexRdfStore::from_built), which
/// consumes it, more than once.
#[derive(Clone)]
pub struct BuiltArray {
    /// The quad rows as one struct array in the layout's column schema.
    pub array: ArrayRef,
    /// The requested indexes' children ([`IndexComponent`]s), the store's
    /// adoption currency; empty exactly when no indexes were requested.
    pub(crate) components: Vec<IndexComponent>,
    /// The Dictionary layout's term dictionary.
    pub(crate) dict: Option<Arc<TermDictionary>>,
}

/// The streaming counterpart of [`BuiltArray`]: the schema dtype, the lazy
/// stream of primary-only quad chunks, the index children as writable
/// components, and the dictionary the serializer writes as the `dictionary`
/// child.
pub(crate) struct BuiltStream {
    /// The schema dtype shared by every chunk.
    pub dtype: DType,
    /// The lazy stream of primary-only quad chunks.
    pub chunks: ChunkStream,
    /// The index children riding beside the rows as writable components.
    pub(crate) components: Vec<crate::io::container::NativeComponentWrite>,
    /// Whether the chunks are in global `(s, p, o, g)` order; written as the
    /// root's `quads_sorted`.
    pub(crate) quads_sorted: bool,
    /// The Dictionary layout's terms: the `dictionary` child, or the
    /// dictionary of the [`BuiltArray`] the stream is materialized into.
    pub(crate) dict: Option<Arc<TermDictionary>>,
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) mod interner;
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) mod runs;
pub(crate) mod sorted_in_memory;
// No filesystem to spill to on wasm32-unknown-unknown; compiling the external
// sort out also keeps rkyv and uuid out of the wasm artifact.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) mod sorted_stream;
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) mod spill;
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) mod stream_indexes;

pub use sorted_in_memory::SortedInMemoryBuilder;
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub use sorted_stream::SortedStreamBuilder;

/// A build pipeline from a quad stream to a materialized dataset, sorted
/// globally by (s, p, o, g), for
/// [`VortexRdfStore::from_built`](crate::VortexRdfStore::from_built). Which
/// implementation exists is decided by the target (see the module doc); the
/// lazy chunk stream for the file writer is `SortedStreamBuilder`'s alone.
///
/// The input quads must be canonical (from [`RawQuad::from_quad`], the parser
/// or [`RawQuad::canonical`]): a builder interns and compares the spelling it
/// is given and does not parse a term again.
pub trait VortexArrayBuilder {
    /// The complete dataset as one (possibly chunked) array, with its index
    /// components and the Dictionary layout's term dictionary.
    fn build_vortex_array(
        quad_stream: Box<dyn Stream<Item = Result<RawQuad>> + Unpin + Send + 'static>,
        layout: LayoutStrategy,
        indexes: Indexes,
    ) -> impl Future<Output = Result<BuiltArray>> + Send;
}

/// One StructArray chunk of primary quad columns for `layout`; indexes are
/// built beside it as components ([`build_components`]). An empty `quads`
/// builds every layout's empty chunk, the Dictionary layout's being
/// [`dictionary::empty_struct`] (unstamped: nothing to sort); a non-empty
/// Dictionary chunk needs the dictionary's codes ([`dictionary::build_code_chunk`]).
///
/// `s_sorted` stamps `IsSorted` on the `s` column, which `match_pattern`
/// trusts as global `(s, p, o, g)` order: it must be `true` only when `quads`
/// is sorted that way, or query results are corrupt.
pub(crate) fn build_struct_array(
    quads: &[RawQuad],
    layout: LayoutStrategy,
    s_sorted: bool,
) -> Result<ArrayRef> {
    if layout == LayoutStrategy::Dictionary && quads.is_empty() {
        return dictionary::empty_struct();
    }
    let field_names = layout.field_names();
    let field_arrays = layout.build_columns(quads)?;

    if s_sorted {
        // `s` is the first column of every layout.
        stamp_is_sorted(&field_arrays[0]);
    }

    StructArray::try_new(
        field_names.into(),
        field_arrays,
        quads.len(),
        Validity::NonNullable,
    )
    .map_err(VortexRdfError::Vortex)
    .map(|a| a.into_array())
}

/// The requested indexes' children over a complete in-memory dataset in final
/// row order: its term strings, or its codes under the Dictionary layout,
/// whose order is the strings', so the children stay binary-searchable. Every
/// in-memory index emission goes through here (the builders and the
/// rebuilds). A dataset with more rows than an index can address is refused
/// ([`check_indexed_rows`]). Each index builds once, however often it is
/// requested, and the children come in [`ALL_INDEX_TYPES`] order, each
/// globally sorted and marked so.
pub(crate) fn build_components(
    indexes: &[IndexType],
    input: IndexInput<'_>,
) -> Result<Vec<IndexComponent>> {
    if !indexes.is_empty() {
        check_indexed_rows(input.rows() as u64)?;
    }
    let mut components = Vec::new();
    for index in ALL_INDEX_TYPES {
        if indexes.contains(&index) {
            components.extend(index.components(input)?);
        }
    }
    Ok(components)
}

/// A store's parts rebuilt from raw quads under `strategy`: the primary rows,
/// the requested indexes' components over them and, under the Dictionary
/// layout, a fresh term dictionary derived from `raws` (empty for no rows,
/// with the components over empty codes, so the index roster and its code
/// dtypes survive). Every compaction and every mutated store's serialization
/// runs it. `raws` must be SPOG-sorted with each quad once (as
/// `sorted_live_raw_quads` hands them over): the `s` column is stamped sorted
/// and the index children are numbered by these rows.
pub(crate) fn build_parts_from_raws(
    raws: &[RawQuad],
    strategy: LayoutStrategy,
    indexes: &[IndexType],
) -> Result<BuiltArray> {
    debug_assert!(
        raws.windows(2).all(|pair| pair[0] < pair[1]),
        "sorted rows must ascend with each quad once"
    );
    match strategy {
        LayoutStrategy::Dictionary => {
            let (dict, code_map) = TermDictionary::from_quads_with_map(raws)?;
            let codes = dictionary::encode_quads(raws, &code_map)?;
            Ok(BuiltArray {
                array: dictionary::build_code_chunk(&codes, 0..raws.len(), true)?,
                components: build_components(indexes, IndexInput::Codes(&codes))?,
                dict: Some(Arc::new(dict)),
            })
        }
        strategy => Ok(BuiltArray {
            array: build_struct_array(raws, strategy, true)?,
            components: build_components(indexes, IndexInput::Quads(raws))?,
            dict: None,
        }),
    }
}
