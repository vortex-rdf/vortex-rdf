use super::{
    ChunkStream, DEFAULT_CHUNK_SIZE, VortexArrayBuilder, build_struct_array,
    build_struct_array_global, into_vortex_error, make_empty_struct,
};
use crate::error::Result;
use crate::store::RawQuad;
use crate::store::indexes::{GlobalIndexes, Indexes};
use crate::store::layouts::term_dictionary::TermDictionary;
use crate::store::layouts::{LayoutStrategy, dictionary};

use futures::{Stream, StreamExt, stream};
use oxrdf::Quad;
use std::sync::Arc;
use web_time::Instant;

use vortex_array::ArrayRef;
use vortex_array::dtype::DType;

/// Fully in-memory, globally sorted Vortex RDF Array Builder.
///
/// Sorts all quads in memory by (s, p, o, g) before writing columns.
/// Produces Reference secondary indexes when requested; their columns are
/// emitted in global sorted order (stamped `IsSorted`), so `match_pattern`
/// can binary-search them.
pub struct SortedInMemoryBuilder;

impl VortexArrayBuilder for SortedInMemoryBuilder {
    async fn build_vortex_array(
        quad_stream: Box<dyn Stream<Item = Result<Quad>> + Unpin + Send + 'static>,
        layout: LayoutStrategy,
        indexes: Indexes,
    ) -> Result<ArrayRef> {
        let start = Instant::now();

        let quads = ingest_and_sort(quad_stream).await?;

        // Build a single contiguous StructArray: for in-memory stores this
        // keeps index columns global and the s column monotonically sorted.
        let n = quads.len();
        let build_start = Instant::now();
        let struct_array = if layout == LayoutStrategy::Dictionary {
            let dict = TermDictionary::from_quads(&quads)?;
            let id_map = dict.build_id_map();
            dictionary::build_chunk(&quads, &dict, &id_map, &indexes, 0, true, true, true)?
        } else {
            build_struct_array(&quads, layout, &indexes, n, 0, true, true)?
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

        Ok(struct_array)
    }

    /// Streaming override for file writes: the sort still requires the whole
    /// dataset in memory as `RawQuad`s, but column chunks are built lazily as
    /// the writer polls, so only one chunk's Vortex arrays exist at a time —
    /// peak memory drops from ~2× dataset to ~1× dataset + one chunk.
    async fn build_vortex_stream(
        quad_stream: Box<dyn Stream<Item = Result<Quad>> + Unpin + Send + 'static>,
        layout: LayoutStrategy,
        indexes: Indexes,
    ) -> Result<(DType, ChunkStream)> {
        build_sorted_chunk_stream(quad_stream, layout, indexes, DEFAULT_CHUNK_SIZE).await
    }
}

/// Ingest the full quad stream and sort it globally by (s, p, o, g).
async fn ingest_and_sort(
    mut quads_in: Box<dyn Stream<Item = Result<Quad>> + Unpin + Send + 'static>,
) -> Result<Vec<RawQuad>> {
    let mut quads: Vec<RawQuad> = Vec::new();
    while let Some(res) = quads_in.next().await {
        quads.push(RawQuad::from_quad(&res?));
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
    quad_stream: Box<dyn Stream<Item = Result<Quad>> + Unpin + Send + 'static>,
    layout: LayoutStrategy,
    indexes: Indexes,
    chunk_size: usize,
) -> Result<(DType, ChunkStream)> {
    let quads = ingest_and_sort(quad_stream).await?;

    if layout == LayoutStrategy::Dictionary {
        let dict = Arc::new(TermDictionary::from_quads(&quads)?);
        let id_map = Arc::new(dict.build_id_map());
        return emit_dict_chunks(quads, dict, id_map, indexes, chunk_size);
    }

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
    Ok((dtype, chunks))
}

/// Dictionary-layout emission over the sorted vec: the dataset is encoded to
/// u32 codes once, the index order is precomputed globally over those codes,
/// and chunks are cut as ranges of both — with the dictionary payload carried
/// only by the first chunk.
fn emit_dict_chunks(
    quads: Vec<RawQuad>,
    dict: Arc<TermDictionary>,
    id_map: Arc<crate::store::layouts::term_dictionary::TermIdMap>,
    indexes: Indexes,
    chunk_size: usize,
) -> Result<(DType, ChunkStream)> {
    let codes = dictionary::encode_quads(&quads, &dict, &id_map)?;
    drop(quads); // chunks are built from the codes alone
    let global_idx = GlobalIndexes::from_codes(&indexes, &codes);
    let n = codes.s.len();

    let n0 = n.min(chunk_size);
    let first = if n == 0 {
        dictionary::empty_struct(&indexes)?
    } else {
        dictionary::build_chunk_global(&codes, 0..n0, &dict, &global_idx, true, true)?
    };
    let dtype = first.dtype().clone();

    let rest = stream::unfold(
        (codes, dict, global_idx, n0),
        move |(codes, dict, global_idx, offset)| async move {
            if offset >= n {
                return None;
            }
            let end = (offset + chunk_size).min(n);
            let chunk = dictionary::build_chunk_global(
                &codes,
                offset..end,
                &dict,
                &global_idx,
                true,
                false,
            )
            .map_err(into_vortex_error);
            Some((chunk, (codes, dict, global_idx, end)))
        },
    );

    let chunks: ChunkStream = stream::once(async move { Ok(first) }).chain(rest).boxed();
    Ok((dtype, chunks))
}
