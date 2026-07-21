use crate::common::utils::stamp_is_sorted;
use crate::error::{Result, VortexRdfError};
use crate::io::VORTEX_LIGHT_SESSION;
use crate::store::RawQuad;
use crate::store::indexes::{GlobalIndexes, IndexType, Indexes, unique_indexes};
use crate::store::layouts::LayoutStrategy;
use futures::{Stream, StreamExt, stream};
use oxrdf::Quad;
use std::future::Future;
use vortex_array::arrays::StructArray;
use vortex_array::arrays::struct_::StructArrayExt;
use vortex_array::dtype::DType;
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute};

use clap::ValueEnum;

/// Number of quads per StructArray chunk in streaming/chunked builders.
pub const DEFAULT_CHUNK_SIZE: usize = 100_000;

/// A stream of StructArray chunks ready for consumption by the Vortex file
/// writer. Items use `VortexResult` because the writer polls the stream
/// directly; builder errors are converted via `into_vortex_error`.
pub type ChunkStream = stream::BoxStream<'static, vortex_error::VortexResult<ArrayRef>>;

/// Convert a builder error into a `VortexError` for use inside a [`ChunkStream`].
pub(crate) fn into_vortex_error(e: VortexRdfError) -> vortex_error::VortexError {
    match e {
        VortexRdfError::Vortex(v) => v,
        other => vortex_error::vortex_err!("{}", other),
    }
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum, Debug)]
pub enum BuilderStrategy {
    /// Natural insertion order, no sorting. Chunks stream directly to the
    /// writer during serialization, bounding memory by the chunk size.
    UnsortedStream,
    /// Global sort of quads by Subject -> Predicate -> Object -> Graph in memory.
    SortedInMemory,
    /// External-memory out-of-core global sort using merge runs.
    SortedStream,
}

pub mod sorted_in_memory;
pub mod sorted_stream;
pub(crate) mod spill;
pub mod unsorted_stream;

pub use sorted_in_memory::SortedInMemoryBuilder;
pub use sorted_stream::SortedStreamBuilder;
pub use unsorted_stream::UnsortedStreamBuilder;

pub trait VortexArrayBuilder {
    /// Build the complete dataset as a single (possibly chunked) in-memory array.
    fn build_vortex_array(
        quad_stream: Box<dyn Stream<Item = Result<Quad>> + Unpin + Send + 'static>,
        layout: LayoutStrategy,
        indexes: Indexes,
    ) -> impl Future<Output = Result<ArrayRef>> + Send;

    /// Produce the schema dtype and a lazily-evaluated stream of StructArray
    /// chunks, for feeding directly into the Vortex file writer.
    ///
    /// The default implementation materializes the full array via
    /// [`Self::build_vortex_array`] and yields it as a single chunk. Builders
    /// that can emit chunks incrementally should override this so that writing
    /// a file needs only O(chunk) memory instead of O(dataset).
    fn build_vortex_stream(
        quad_stream: Box<dyn Stream<Item = Result<Quad>> + Unpin + Send + 'static>,
        layout: LayoutStrategy,
        indexes: Indexes,
    ) -> impl Future<Output = Result<(DType, ChunkStream)>> + Send {
        async move {
            let array = Self::build_vortex_array(quad_stream, layout, indexes).await?;
            let dtype = array.dtype().clone();
            let chunks: ChunkStream = futures::stream::once(async move { Ok(array) }).boxed();
            Ok((dtype, chunks))
        }
    }
}

/// Build a complete StructArray chunk: primary columns for the given layout,
/// followed by the columns of every requested index.
///
/// The layout-specific column logic lives in [`crate::store::layouts`] and the
/// index-specific column logic in [`crate::store::indexes`]; this function only
/// orchestrates them.
///
/// `start_row` is the global row ID of the first quad in `quads`; pass the
/// running row offset when building one chunk of a larger array so index row
/// IDs address the assembled array, or `0` for a single-chunk build.
///
/// `s_sorted` must be `true` only when `quads` is sorted by subject: it stamps
/// the `IsSorted` statistic on the `s` column, which enables the binary-search
/// fast path in `match_pattern`. Stamping it on unsorted data would corrupt
/// query results.
///
/// `whole_dataset` must be `true` only when `quads` is the complete dataset
/// (single-chunk builds): the per-chunk index sort is then the global order,
/// and the index value columns get stamped `IsSorted` so `match_pattern` may
/// binary-search them.
pub(crate) fn build_struct_array(
    quads: &[RawQuad],
    layout: LayoutStrategy,
    indexes: &[IndexType],
    n: usize,
    start_row: u32,
    s_sorted: bool,
    whole_dataset: bool,
) -> Result<ArrayRef> {
    let mut field_names = layout.field_names();
    let mut field_arrays = layout.build_columns(quads)?;

    if s_sorted {
        // The s column is first in both layouts.
        stamp_is_sorted(&field_arrays[0]);
    }

    for idx in unique_indexes(indexes) {
        idx.append_columns(
            &mut field_names,
            &mut field_arrays,
            quads,
            start_row,
            whole_dataset,
        );
    }

    StructArray::try_new(field_names.into(), field_arrays, n, Validity::NonNullable)
        .map_err(VortexRdfError::Vortex)
        .map(|a| a.into_array())
}

/// Build a chunk for rows `range` of an in-memory dataset whose index columns
/// come pre-sorted from a [`GlobalIndexes`] — the sorted in-memory builder's
/// chunked emission path. `quads` is the chunk's slice (`dataset[range]`).
pub(crate) fn build_struct_array_global(
    quads: &[RawQuad],
    layout: LayoutStrategy,
    global_indexes: &GlobalIndexes,
    range: std::ops::Range<usize>,
    s_sorted: bool,
) -> Result<ArrayRef> {
    let mut field_names = layout.field_names();
    let mut field_arrays = layout.build_columns(quads)?;

    if s_sorted {
        stamp_is_sorted(&field_arrays[0]);
    }

    global_indexes.append_slice(&mut field_names, &mut field_arrays, range)?;

    StructArray::try_new(
        field_names.into(),
        field_arrays,
        quads.len(),
        Validity::NonNullable,
    )
    .map_err(VortexRdfError::Vortex)
    .map(|a| a.into_array())
}

/// Canonicalize a sorted builder's (possibly chunked) materialized array and
/// re-stamp the sortedness stats the builder guarantees: the `s` column is
/// globally sorted, and — when the chunks were emitted as windows of a global
/// index order — so are the `_idx_*_val` columns and the copy families' lead
/// columns. Canonicalization loses the per-chunk stats, so without this
/// multi-chunk in-memory stores would fall back to mask scans.
pub(crate) fn canonicalize_sorted(arr: ArrayRef) -> Result<ArrayRef> {
    let mut ctx = VORTEX_LIGHT_SESSION.create_execution_ctx();
    let struct_arr = arr
        .execute::<StructArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;

    for field in [
        "s",
        "_idx_o_val",
        "_idx_p_val",
        "_idx_posg_p",
        "_idx_ospg_o",
    ] {
        if let Ok(col) = struct_arr.unmasked_field_by_name(field) {
            stamp_is_sorted(col);
        }
    }
    Ok(struct_arr.into_array())
}

/// Assemble a list of per-chunk StructArrays into a single ArrayRef.
/// Returns an empty StructArray with the correct schema when `chunks` is empty.
pub fn assemble_chunks(
    mut chunks: Vec<ArrayRef>,
    layout: LayoutStrategy,
    indexes: &Indexes,
) -> Result<ArrayRef> {
    if chunks.is_empty() {
        make_empty_struct(layout, indexes)
    } else if chunks.len() == 1 {
        Ok(chunks.remove(0))
    } else {
        use vortex_array::arrays::ChunkedArray;
        let dtype = chunks[0].dtype().clone();
        let chunked = ChunkedArray::try_new(chunks, dtype)
            .map_err(VortexRdfError::Vortex)?
            .into_array();
        Ok(chunked)
    }
}

/// An empty StructArray with the schema of the given layout and indexes.
/// Building from an empty quad slice yields every column empty but with the
/// correct dtype, so this is just the regular build path with no rows.
pub(crate) fn make_empty_struct(layout: LayoutStrategy, indexes: &Indexes) -> Result<ArrayRef> {
    if layout == LayoutStrategy::Dictionary {
        return crate::store::layouts::dictionary::empty_struct(indexes);
    }
    build_struct_array(&[], layout, indexes, 0, 0, false, true)
}
