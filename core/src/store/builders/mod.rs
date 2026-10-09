//! Emission orchestration: turning a quad stream into the parts a store is
//! built or serialized from.
//!
//! This hub owns everything the two pipelines share — the
//! [`VortexArrayBuilder`] contract and its two products ([`BuiltArray`],
//! [`BuiltStream`]), primary chunk assembly, the sortedness stamps a build is
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
//! them at once over the dataset (`GlobalIndexes`), the out-of-core one
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
//! same way before they call [`build_parts_from_raws`].

use crate::error::{Result, VortexRdfError};
use crate::store::RawQuad;
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
use crate::store::array::chunked_or_single;
use crate::store::array::stamp_is_sorted;
use crate::store::indexes::{
    IndexComponent, IndexType, Indexes, secondary_by_copy, secondary_by_reference, unique_indexes,
};
use crate::store::layouts::LayoutStrategy;
use crate::store::layouts::dictionary::{QuadCodes, TermDictionary};
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
/// directly; builder errors are converted via `into_vortex_error`.
pub type ChunkStream = stream::BoxStream<'static, vortex_error::VortexResult<ArrayRef>>;

/// Convert a builder error into a `VortexError` for use inside a [`ChunkStream`].
fn into_vortex_error(e: VortexRdfError) -> vortex_error::VortexError {
    match e {
        VortexRdfError::Vortex(v) => v,
        other => vortex_error::vortex_err!("{}", other),
    }
}

/// A built dataset: the quad array plus whatever layout state cannot be
/// derived from the array alone — for the Dictionary layout, its term
/// dictionary (the array holds only u32 code columns; the terms travel
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
pub struct BuiltStream {
    /// The schema dtype shared by every chunk.
    pub dtype: DType,
    /// The lazy stream of primary-only quad chunks.
    pub chunks: ChunkStream,
    /// The index children riding beside the rows as writable components.
    pub(crate) components: Vec<crate::io::container::NativeComponentWrite>,
    /// Whether the chunks are in global `(s, p, o, g)` order; written as the
    /// root's `quads_sorted` (see `WireMetadata::quads_sorted`).
    // Read by the serializer (`io::ser`), which is compiled in under
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

pub use sorted_in_memory::SortedInMemoryBuilder;
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub use sorted_stream::SortedStreamBuilder;

/// A build pipeline from a quad stream to store parts:
/// [`build_vortex_array`](Self::build_vortex_array) materializes the dataset
/// (for [`VortexRdfStore::from_built`](crate::VortexRdfStore::from_built)),
/// [`build_vortex_stream`](Self::build_vortex_stream) emits it lazily for the
/// file writer. Both sort globally by (s, p, o, g); which implementation
/// exists is decided by the target (see the module doc).
pub trait VortexArrayBuilder {
    /// Build the complete dataset as a single (possibly chunked) in-memory
    /// array, together with the layout state the array alone cannot carry
    /// (the Dictionary layout's term dictionary).
    fn build_vortex_array(
        quad_stream: Box<dyn Stream<Item = Result<RawQuad>> + Unpin + Send + 'static>,
        layout: LayoutStrategy,
        indexes: Indexes,
    ) -> impl Future<Output = Result<BuiltArray>> + Send;

    /// Produce the schema dtype and a lazily-evaluated stream of StructArray
    /// chunks, for feeding directly into the Vortex file writer, so that
    /// writing a file needs only O(chunk) memory for the column arrays
    /// instead of O(dataset).
    fn build_vortex_stream(
        quad_stream: Box<dyn Stream<Item = Result<RawQuad>> + Unpin + Send + 'static>,
        layout: LayoutStrategy,
        indexes: Indexes,
    ) -> impl Future<Output = Result<BuiltStream>> + Send;
}

/// Build one StructArray chunk of primary quad columns for the given layout.
/// Secondary indexes never ride here — they are built as components beside
/// the quad rows (see [`build_components`]).
///
/// The layout-specific column logic lives in [`crate::store::layouts`]; this
/// function only orchestrates it.
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

/// Every requested index's columns, sorted once over the complete in-memory
/// dataset, handed on as persisted children by
/// [`into_components`](Self::into_components).
struct GlobalIndexes {
    by_copy: Option<secondary_by_copy::GlobalCopyArrays>,
    by_reference: Option<secondary_by_reference::GlobalReferenceArrays>,
}

impl GlobalIndexes {
    /// Build the requested families, each over the dataset in final row
    /// order; `copy` and `reference` supply a family's arrays and run only
    /// when that family is requested.
    fn build(
        indexes: &[IndexType],
        copy: impl FnOnce() -> secondary_by_copy::GlobalCopyArrays,
        reference: impl FnOnce() -> secondary_by_reference::GlobalReferenceArrays,
    ) -> Self {
        let unique = unique_indexes(indexes);
        Self {
            by_copy: unique.contains(&IndexType::SecondaryByCopy).then(copy),
            by_reference: unique
                .contains(&IndexType::SecondaryByReference)
                .then(reference),
        }
    }

    /// Every built index's persisted children, in index declaration order.
    /// Each is globally sorted by construction and says so.
    fn into_components(self) -> Result<Vec<IndexComponent>> {
        let mut components = Vec::new();
        if let Some(sbc) = self.by_copy {
            components.extend(sbc.into_components()?);
        }
        if let Some(sbr) = self.by_reference {
            components.extend(sbr.into_components()?);
        }
        Ok(components)
    }
}

/// The requested indexes' children over a complete in-memory dataset in final
/// row order — the one entry point every in-memory index emission goes
/// through (the builders' construction paths and the mutation/compaction
/// rebuilds alike).
pub(crate) fn build_components(
    indexes: &[IndexType],
    quads: &[RawQuad],
) -> Result<Vec<IndexComponent>> {
    GlobalIndexes::build(
        indexes,
        || secondary_by_copy::GlobalCopyArrays::from_quads(quads),
        || secondary_by_reference::GlobalReferenceArrays::from_quads(quads),
    )
    .into_components()
}

/// Dictionary-layout counterpart of [`build_components`]: the children are
/// built over the dataset's u32 codes. Sorting codes is order-equivalent to
/// sorting the term strings, so the children stay binary-searchable —
/// queries translate the pattern terms to codes first.
pub(crate) fn build_components_from_codes(
    indexes: &[IndexType],
    codes: &QuadCodes,
) -> Result<Vec<IndexComponent>> {
    GlobalIndexes::build(
        indexes,
        || secondary_by_copy::GlobalCopyArrays::from_codes(codes),
        || secondary_by_reference::GlobalReferenceArrays::from_codes(codes),
    )
    .into_components()
}

/// The parts of a store rebuilt from raw quads under `strategy`: the primary
/// rows, the requested indexes' components over them, and — under the
/// Dictionary layout — the fresh term dictionary the rows' codes address.
/// The rebuild every compaction and every mutated store's serialization run.
///
/// The Dictionary layout derives its dictionary from `raws`; an empty set
/// still yields the components (over empty codes), so the index roster and
/// its code dtypes survive. `sorted` must be `true` only when `raws` is
/// SPOG-sorted: it stamps the `s` column. The components are globally
/// sorted whatever the row order.
///
/// Like every builder this keeps the rows it is given and numbers the index
/// children by them, so a caller that sorts them (`sorted`) drops the
/// repeated quads first, and the store it builds holds each quad once.
pub(crate) fn build_parts_from_raws(
    raws: &[RawQuad],
    strategy: LayoutStrategy,
    indexes: &[IndexType],
    sorted: bool,
) -> Result<(ArrayRef, Vec<IndexComponent>, Option<Arc<TermDictionary>>)> {
    use crate::store::layouts::dictionary;
    debug_assert!(
        !sorted || raws.windows(2).all(|pair| pair[0] < pair[1]),
        "sorted rows must ascend with each quad once"
    );
    match strategy {
        LayoutStrategy::Dictionary if raws.is_empty() => Ok((
            dictionary::empty_struct()?,
            build_components_from_codes(indexes, &QuadCodes::empty())?,
            Some(Arc::new(TermDictionary::empty())),
        )),
        LayoutStrategy::Dictionary => {
            let (dict, code_map) = TermDictionary::from_quads_with_map(raws)?;
            let codes = dictionary::encode_quads(raws, &code_map)?;
            let primary = dictionary::build_code_chunk(&codes, 0..raws.len(), sorted)?;
            let components = build_components_from_codes(indexes, &codes)?;
            Ok((primary, components, Some(Arc::new(dict))))
        }
        strategy => {
            let primary = build_struct_array(raws, strategy, sorted)?;
            let components = build_components(indexes, raws)?;
            Ok((primary, components, None))
        }
    }
}

/// Assemble a builder's per-chunk StructArrays into a single ArrayRef. Every
/// build emits at least one (possibly empty) chunk, so `chunks` carries the
/// schema; an empty list is a caller bug.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) fn assemble_chunks(chunks: Vec<ArrayRef>) -> Result<ArrayRef> {
    let dtype = chunks
        .first()
        .ok_or_else(|| {
            VortexRdfError::InvalidOperation("assemble_chunks: no chunks to assemble".to_string())
        })?
        .dtype()
        .clone();
    chunked_or_single(chunks, dtype)
}

/// An empty StructArray with the given layout's primary schema. Building from
/// an empty quad slice yields every column empty but with the correct dtype,
/// so this is just the regular build path with no rows.
fn make_empty_struct(layout: LayoutStrategy) -> Result<ArrayRef> {
    if layout == LayoutStrategy::Dictionary {
        return crate::store::layouts::dictionary::empty_struct();
    }
    build_struct_array(&[], layout, false)
}
