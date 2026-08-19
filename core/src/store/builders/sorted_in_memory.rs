//! The [`SortedInMemoryBuilder`] strategy: hold the whole dataset, sort it
//! once by (s, p, o, g), and emit chunks as windows of that single order.
//!
//! Holding everything at once is what earns the global sortedness stamps on
//! `s` and on the index value columns (built through
//! [`GlobalIndexes`] over the sorted dataset), the
//! stamps a reader binary-searches on. The cost is O(dataset) memory; the
//! out-of-core strategy with the same guarantee is
//! [`sorted_stream`](super::sorted_stream). Only this file's ordering
//! discipline lives here — the emission machinery it drives belongs to
//! [`builders`](super).

use super::{
    BuiltArray, BuiltStream, ChunkStream, DEFAULT_CHUNK_SIZE, VortexArrayBuilder,
    build_struct_array, build_struct_array_global, into_vortex_error, make_empty_struct,
};
use crate::error::Result;
use crate::store::RawQuad;
use crate::store::builders::GlobalIndexes;
use crate::store::indexes::Indexes;
use crate::store::layouts::dictionary::{QuadCodes, TermDictionary, ingest_interning};
use crate::store::layouts::{LayoutStrategy, dictionary};

use futures::{Stream, StreamExt, stream};
use std::sync::Arc;
use web_time::Instant;

/// Fully in-memory, globally sorted Vortex RDF Array Builder.
///
/// Sorts all quads in memory by (s, p, o, g) before writing columns.
/// Produces Reference secondary indexes when requested; their columns are
/// emitted in global sorted order (stamped `IsSorted`), so `match_pattern`
/// can binary-search them.
pub struct SortedInMemoryBuilder;

impl VortexArrayBuilder for SortedInMemoryBuilder {
    async fn build_vortex_array(
        quad_stream: Box<dyn Stream<Item = Result<RawQuad>> + Unpin + Send + 'static>,
        layout: LayoutStrategy,
        indexes: Indexes,
    ) -> Result<BuiltArray> {
        let start = Instant::now();

        // Build a single contiguous StructArray: for in-memory stores this
        // keeps index columns global and the s column monotonically sorted.
        //
        // Dictionary layout interns terms as the stream drains, so the sort
        // runs over 16-byte coded rows and no `Vec<RawQuad>` (four owned
        // Strings per quad) ever accumulates.
        let (n, build_start, built);
        if layout == LayoutStrategy::Dictionary {
            let (dict, codes) = ingest_interning(quad_stream).await?.finish()?;
            n = codes.s.len();
            build_start = Instant::now();
            built = BuiltArray {
                array: dictionary::build_array(&codes, &indexes)?,
                components: Vec::new(),
                dict: Some(Arc::new(dict)),
            };
        } else {
            let quads = ingest_and_sort(quad_stream).await?;
            n = quads.len();
            build_start = Instant::now();
            built = BuiltArray {
                array: build_struct_array(&quads, layout, &indexes, 0, true, true)?,
                components: Vec::new(),
                dict: None,
            };
        };
        log::debug!(
            "[SortedInMemoryBuilder] Constructed StructArray in {:?}",
            build_start.elapsed()
        );
        log::debug!(
            "[SortedInMemoryBuilder] Completed serialization of {} quads in {:?}",
            n,
            start.elapsed()
        );

        Ok(built)
    }

    /// Streaming override for file writes: the sort still requires the whole
    /// dataset in memory as `RawQuad`s, but column chunks are built lazily as
    /// the writer polls, so only one chunk's Vortex arrays exist at a time —
    /// peak memory drops from ~2× dataset to ~1× dataset + one chunk.
    async fn build_vortex_stream(
        quad_stream: Box<dyn Stream<Item = Result<RawQuad>> + Unpin + Send + 'static>,
        layout: LayoutStrategy,
        indexes: Indexes,
    ) -> Result<BuiltStream> {
        build_sorted_chunk_stream(quad_stream, layout, indexes, DEFAULT_CHUNK_SIZE).await
    }
}

/// Ingest the full quad stream and sort it globally by (s, p, o, g).
async fn ingest_and_sort(
    mut quads_in: Box<dyn Stream<Item = Result<RawQuad>> + Unpin + Send + 'static>,
) -> Result<Vec<RawQuad>> {
    let mut quads: Vec<RawQuad> = Vec::new();
    while let Some(res) = quads_in.next().await {
        quads.push(res?);
    }
    log::debug!("[SortedInMemoryBuilder] Read {} quads", quads.len());

    let sort_start = Instant::now();
    quads.sort_unstable();
    log::debug!(
        "[SortedInMemoryBuilder] Sorted quads in {:?}",
        sort_start.elapsed()
    );

    Ok(quads)
}

/// Ingest, sort, then emit fixed-size StructArray chunks over slices of the
/// sorted vec. The first chunk is built eagerly so the schema dtype is known
/// up front; subsequent chunks are built only when polled.
///
/// Index columns are precomputed once in global sorted order and sliced per
/// chunk, so their concatenation across chunks stays globally sorted (each
/// slice is stamped `IsSorted`) and row IDs address the assembled array.
pub(crate) async fn build_sorted_chunk_stream(
    quad_stream: Box<dyn Stream<Item = Result<RawQuad>> + Unpin + Send + 'static>,
    layout: LayoutStrategy,
    indexes: Indexes,
    chunk_size: usize,
) -> Result<BuiltStream> {
    if layout == LayoutStrategy::Dictionary {
        let (dict, codes) = ingest_interning(quad_stream).await?.finish()?;
        return emit_dict_chunks(codes, Arc::new(dict), indexes, chunk_size);
    }

    let quads = ingest_and_sort(quad_stream).await?;

    let global_idx = Arc::new(GlobalIndexes::from_quads(&indexes, &quads));

    let n0 = quads.len().min(chunk_size);
    let first = if quads.is_empty() {
        make_empty_struct(layout, &indexes)?
    } else {
        build_struct_array_global(&quads[..n0], layout, &global_idx, 0..n0, true)?
    };
    let dtype = first.dtype().clone();

    let rest = stream::unfold(
        (quads, layout, global_idx, n0),
        move |(quads, layout, global_idx, offset)| async move {
            if offset >= quads.len() {
                return None;
            }
            let end = (offset + chunk_size).min(quads.len());
            let chunk = build_struct_array_global(
                &quads[offset..end],
                layout,
                &global_idx,
                offset..end,
                true,
            )
            .map_err(into_vortex_error);
            Some((chunk, (quads, layout, global_idx, end)))
        },
    );

    let chunks: ChunkStream = stream::once(async move { Ok(first) }).chain(rest).boxed();
    Ok(BuiltStream {
        components: Vec::new(),
        // Chunks slice one global emission: both the quads and the index
        // columns concatenate back to their global sort orders.
        components_sorted: true,
        quads_sorted: true,
        dtype,
        chunks,
        dict: None,
    })
}

/// Dictionary-layout emission over the interned codes: the index order is
/// precomputed globally over the codes, and chunks are cut as ranges of both.
/// The dictionary rides beside the stream for the serializer to place.
fn emit_dict_chunks(
    codes: QuadCodes,
    dict: Arc<TermDictionary>,
    indexes: Indexes,
    chunk_size: usize,
) -> Result<BuiltStream> {
    let global_idx = GlobalIndexes::from_codes(&indexes, &codes);
    let n = codes.s.len();

    let n0 = n.min(chunk_size);
    let first = if n == 0 {
        dictionary::empty_struct(&indexes)?
    } else {
        dictionary::build_chunk_global(&codes, 0..n0, &global_idx, true)?
    };
    let dtype = first.dtype().clone();

    let rest = stream::unfold(
        (codes, global_idx, n0),
        move |(codes, global_idx, offset)| async move {
            if offset >= n {
                return None;
            }
            let end = (offset + chunk_size).min(n);
            let chunk = dictionary::build_chunk_global(&codes, offset..end, &global_idx, true)
                .map_err(into_vortex_error);
            Some((chunk, (codes, global_idx, end)))
        },
    );

    let chunks: ChunkStream = stream::once(async move { Ok(first) }).chain(rest).boxed();
    Ok(BuiltStream {
        components: Vec::new(),
        components_sorted: true,
        quads_sorted: true,
        dtype,
        chunks,
        dict: Some(dict),
    })
}
