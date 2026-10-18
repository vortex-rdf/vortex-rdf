//! Emission orchestration: turning a quad stream into the parts a store is
//! built or serialized from.
//!
//! This hub owns everything the two pipelines share — the
//! [`VortexArrayBuilder`] contract and its products ([`BuiltArray`], and the
//! crate-private `BuiltStream` the file writer consumes), the primary chunk
//! build ([`build_struct_array`]), the sortedness stamps a build is
//! allowed to claim, and the globally-sorted index emission
//! ([`build_components`]). A leaf (`sorted_in_memory`, `sorted_stream`)
//! contributes only its memory profile; `spill` backs the out-of-core one.
//!
//! Which pipeline runs is a property of the target, never a caller's choice:
//! where a filesystem exists the rows go through the out-of-core global sort
//! ([`SortedStreamBuilder`]), whose peak memory does not scale with the
//! dataset; `wasm32-unknown-unknown` has no filesystem to spill to, so there
//! the in-memory global sort ([`SortedInMemoryBuilder`]) is the pipeline —
//! and the only one compiled in.
//!
//! Index data has exactly one form: a builder emits primary-only quad rows
//! plus one *component* per requested index's persisted-child identity (see
//! `indexes::components`), which is what a store adopts and what a file
//! writes — nothing intermediate, nothing to split. The two pipelines differ
//! only in where the components come from: the in-memory sort builds all of
//! them at once over the dataset ([`build_components`]), the out-of-core one
//! streams each family off its own spill-run merger.
//!
//! Both sort globally, which is what lets them declare their components
//! `sorted` — a reader binary-searches on that provenance alone. The quad
//! rows' own subject sortedness is not a constant in the same way: it is
//! true of every build here, but a store that has been appended to loses it,
//! so it travels as the `s` column's `IsSorted` stamp and is re-read rather
//! than assumed.
//!
//! Quads are unique and terms are canonical in what a build emits. The input
//! reaches the builders as [`RawQuad`]s the parser (or `RawQuad::from_quad`)
//! rendered, one spelling per RDF term, so interning a spelling interns the
//! term; and both sorts drop the repeated quads right after sorting — before
//! any column, row id or index child is derived from the rows — so the rows
//! and every child describe the same deduplicated dataset. The rebuilds
//! (compaction, a mutated store's serialization) sort and drop repeats the
//! same way, in one place (`sorted_live_raw_quads`), before they call
//! [`build_parts_from_raws`].

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

/// A stream of StructArray chunks ready for consumption by the Vortex file
/// writer. Items use `VortexResult` because the writer polls the stream
/// directly; builder errors are converted through the `From<VortexRdfError> for VortexError` impl.
pub(crate) type ChunkStream = stream::BoxStream<'static, vortex_error::VortexResult<ArrayRef>>;

/// A built dataset: the quad array plus whatever layout state cannot be
/// derived from the array alone — for the Dictionary layout, its term
/// dictionary (the array holds only code columns; the terms travel
/// beside it and reach serialized files as the native container's
/// `dictionary` child).
///
/// Cloning is shallow (Arc'd buffers throughout), so one build can be
/// handed to [`VortexRdfStore::from_built`](crate::VortexRdfStore::from_built)
/// — which consumes it — more than once.
#[derive(Clone)]
pub struct BuiltArray {
    /// The quad rows as one struct array (in the layout's column schema).
    pub array: ArrayRef,
    /// The requested indexes' children, built beside the quad rows — the
    /// store's adoption currency
    /// ([`IndexComponent`](crate::store::indexes::IndexComponent)). Empty
    /// exactly when no indexes were requested.
    pub(crate) components: Vec<IndexComponent>,
    pub(crate) dict: Option<Arc<TermDictionary>>,
}

/// The streaming counterpart of [`BuiltArray`]: the schema dtype, the lazy
/// stream of primary-only quad chunks, the index children riding beside it as
/// writable components, and the dictionary the serializer writes as the
/// `dictionary` child.
pub(crate) struct BuiltStream {
    /// The schema dtype shared by every chunk.
    pub dtype: DType,
    /// The lazy stream of primary-only quad chunks.
    pub chunks: ChunkStream,
    /// The index children riding beside the rows as writable components.
    pub(crate) components: Vec<crate::io::container::NativeComponentWrite>,
    /// Whether the chunks are in global `(s, p, o, g)` order; written as the
    /// root's `quads_sorted` (see `WireMetadata::quads_sorted`).
    // Read by the serializer (`io::write`), which is compiled in under
    // `file-io` and on wasm32.
    #[cfg_attr(
        not(any(feature = "file-io", target_arch = "wasm32")),
        allow(dead_code)
    )]
    pub(crate) quads_sorted: bool,
    /// The Dictionary layout's terms, placed as the `dictionary` child by the
    /// serializer and carried into the materialized [`BuiltArray`].
    #[cfg_attr(
        not(any(feature = "file-io", target_arch = "wasm32")),
        allow(dead_code)
    )]
    pub(crate) dict: Option<Arc<TermDictionary>>,
}

pub(crate) mod sorted_in_memory;
// wasm32-unknown-unknown has no filesystem to spill to; compiling the
// external sort out keeps rkyv and uuid out of the wasm artifact.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) mod sorted_stream;
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) mod spill;
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) mod stream_indexes;

pub use sorted_in_memory::SortedInMemoryBuilder;
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub use sorted_stream::SortedStreamBuilder;

/// A build pipeline from a quad stream to store parts:
/// [`build_vortex_array`](Self::build_vortex_array) materializes the dataset
/// (for [`VortexRdfStore::from_built`](crate::VortexRdfStore::from_built)).
/// Both implementations sort globally by (s, p, o, g); which one exists is
/// decided by the target (see the module doc). The lazy chunk stream for the
/// file writer is [`SortedStreamBuilder`]'s alone.
///
/// The input quads must be canonical (from [`RawQuad::from_quad`], the parser
/// or [`RawQuad::canonical`]): a builder interns and compares the spelling it
/// is given and does not parse a term again.
pub trait VortexArrayBuilder {
    /// Build the complete dataset as a single (possibly chunked) in-memory
    /// array, together with the layout state the array alone cannot carry
    /// (the Dictionary layout's term dictionary).
    fn build_vortex_array(
        quad_stream: Box<dyn Stream<Item = Result<RawQuad>> + Unpin + Send + 'static>,
        layout: LayoutStrategy,
        indexes: Indexes,
    ) -> impl Future<Output = Result<BuiltArray>> + Send;
}

/// Build one StructArray chunk of primary quad columns for the given layout.
/// Secondary indexes never ride here — they are built as components beside
/// the quad rows (see [`build_components`]).
///
/// The layout-specific column logic lives in [`crate::store::layouts`]; this
/// function only orchestrates it. An empty `quads` builds every layout's
/// empty chunk, the Dictionary layout's being [`dictionary::empty_struct`]
/// (unstamped: nothing to sort); a non-empty Dictionary chunk needs the
/// dictionary's codes ([`dictionary::build_chunk`]).
///
/// `s_sorted` must be `true` only when `quads` is sorted by subject: it stamps
/// the `IsSorted` statistic on the `s` column, which enables the binary-search
/// fast path in `match_pattern`. Stamping it on unsorted data would corrupt
/// query results.
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
        // The s column is first in both layouts.
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
/// row order — its term strings, or its codes under the Dictionary layout,
/// whose sort is order-equivalent to the strings', so the children stay
/// binary-searchable (queries translate the pattern terms to codes first).
/// The one entry point every in-memory index emission goes through (the
/// builders' construction paths and the mutation/compaction rebuilds alike).
///
/// Each index in `indexes` builds once, however often it is requested, and
/// the children come in [`ALL_INDEX_TYPES`] order, whatever the request's
/// order; each is globally sorted by construction and says so.
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
/// the requested indexes' components over them, and — under the Dictionary
/// layout — the fresh term dictionary the rows' codes address. The rebuild
/// every compaction and every mutated store's serialization run.
///
/// The Dictionary layout derives its dictionary from `raws`; an empty set
/// yields an empty dictionary and the components over empty codes, so the
/// index roster and its code dtypes survive. `raws` must be SPOG-sorted with
/// each quad once (the rows `sorted_live_raw_quads` hands both rebuilds): the
/// `s` column is stamped sorted, and the index children are numbered by these
/// rows, so the store this builds holds each quad once.
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
