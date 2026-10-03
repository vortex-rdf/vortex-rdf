//! The [`SortedInMemoryBuilder`] strategy: hold the whole dataset, sort it
//! once by (s, p, o, g), and emit chunks as windows of that single order.
//!
//! Holding everything at once is what earns the global sortedness this
//! builder claims: the `s` column's `IsSorted` stamp, and the index children
//! it builds through [`build_components`] over the sorted dataset. The cost
//! is O(dataset) memory; the out-of-core strategy with the same guarantee is
//! [`sorted_stream`](super::sorted_stream). Only this file's ordering
//! discipline lives here — the emission machinery it drives belongs to
//! [`builders`](super).

use super::{
    BuiltArray, BuiltStream, ChunkStream, DEFAULT_CHUNK_ROWS, VortexArrayBuilder, build_components,
    build_components_from_codes, build_struct_array, into_vortex_error,
};
use crate::error::Result;
use crate::store::RawQuad;
use crate::store::indexes::Indexes;
use crate::store::layouts::dictionary::ingest::{InterningQuadBuilder, finish_interned};
use crate::store::layouts::dictionary::{QuadCodes, TermDictionary};
use crate::store::layouts::{LayoutStrategy, dictionary};

use crate::debug;
use futures::{Stream, StreamExt, stream};
use std::sync::Arc;

/// Fully in-memory, globally sorted Vortex RDF Array Builder.
///
/// Sorts all quads in memory by (s, p, o, g), builds the primary columns, and
/// builds every requested index child (by copy and by reference) over the
/// same sorted dataset, so each child is globally sorted and
/// binary-searchable.
pub struct SortedInMemoryBuilder;

impl VortexArrayBuilder for SortedInMemoryBuilder {
    async fn build_vortex_array(
        quad_stream: Box<dyn Stream<Item = Result<RawQuad>> + Unpin + Send + 'static>,
        layout: LayoutStrategy,
        indexes: Indexes,
    ) -> Result<BuiltArray> {
        let start = debug::timer();

        // Build a single contiguous StructArray of primary columns, with each
        // requested index's child built beside it over the same sorted
        // dataset: both the `s` column and every child are then globally
        // sorted.
        //
        // Dictionary layout interns terms as the stream drains, so the sort
        // runs over 16-byte coded rows and no `Vec<RawQuad>` (four owned
        // Strings per quad) ever accumulates.
        let (n, build_start, built);
        if layout == LayoutStrategy::Dictionary {
            let interner = InterningQuadBuilder::from_stream(quad_stream).await?;
            build_start = debug::timer();
            built = finish_interned(interner, &indexes)?;
            n = built.array.len();
        } else {
            let quads = ingest_and_sort(quad_stream).await?;
            n = quads.len();
            build_start = debug::timer();
            built = BuiltArray {
                array: build_struct_array(&quads, layout, true)?,
                components: build_components(&indexes, &quads)?,
                dict: None,
            };
        };
        log::debug!(
            "[SortedInMemoryBuilder] Constructed StructArray in {:?}",
            debug::elapsed(build_start)
        );
        log::debug!(
            "[SortedInMemoryBuilder] Completed serialization of {} quads in {:?}",
            n,
            debug::elapsed(start)
        );

        Ok(built)
    }

    /// The sort holds the whole dataset (as `RawQuad`s, or as interned codes
    /// under the Dictionary layout); column chunks are built lazily as the
    /// writer polls, so only one chunk's Vortex arrays exist at a time.
    async fn build_vortex_stream(
        quad_stream: Box<dyn Stream<Item = Result<RawQuad>> + Unpin + Send + 'static>,
        layout: LayoutStrategy,
        indexes: Indexes,
    ) -> Result<BuiltStream> {
        build_chunk_stream(quad_stream, layout, indexes, DEFAULT_CHUNK_ROWS).await
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

    let sort_start = debug::timer();
    quads.sort_unstable();
    log::debug!(
        "[SortedInMemoryBuilder] Sorted quads in {:?}",
        debug::elapsed(sort_start)
    );

    Ok(quads)
}

/// Ingest, sort, then emit fixed-size primary StructArray chunks over slices
/// of the sorted vec. The first chunk is built eagerly so the schema dtype is
/// known up front; subsequent chunks are built only when polled.
///
/// The index children are built once over the whole sorted dataset and ride
/// beside the stream as complete components — their row ids address the
/// assembled array, so they cannot be cut per chunk anyway.
pub(crate) async fn build_chunk_stream(
    quad_stream: Box<dyn Stream<Item = Result<RawQuad>> + Unpin + Send + 'static>,
    layout: LayoutStrategy,
    indexes: Indexes,
    chunk_size: usize,
) -> Result<BuiltStream> {
    if layout == LayoutStrategy::Dictionary {
        let (dict, codes) = InterningQuadBuilder::from_stream(quad_stream)
            .await?
            .finish()?;
        return emit_dict_chunks(codes, Arc::new(dict), indexes, chunk_size);
    }

    let quads = ingest_and_sort(quad_stream).await?;

    let components = component_writes(build_components(&indexes, &quads)?)?;

    let n0 = quads.len().min(chunk_size);
    let first = if quads.is_empty() {
        build_struct_array(&[], layout, false)?
    } else {
        build_struct_array(&quads[..n0], layout, true)?
    };
    let dtype = first.dtype().clone();

    let rest = stream::unfold(
        (quads, layout, n0),
        move |(quads, layout, offset)| async move {
            if offset >= quads.len() {
                return None;
            }
            let end = (offset + chunk_size).min(quads.len());
            let chunk =
                build_struct_array(&quads[offset..end], layout, true).map_err(into_vortex_error);
            Some((chunk, (quads, layout, end)))
        },
    );

    let chunks: ChunkStream = stream::once(async move { Ok(first) }).chain(rest).boxed();
    Ok(BuiltStream {
        components,
        quads_sorted: true,
        dtype,
        chunks,
        dict: None,
    })
}

/// The built children as writable components. They are already materialized
/// (this builder holds the dataset), so each is a replayable single-chunk
/// source. A build with no serializer compiled in has nothing to hand them
/// to, and says so with an empty roster.
fn component_writes(
    components: Vec<crate::store::indexes::IndexComponent>,
) -> Result<Vec<crate::io::container::NativeComponentWrite>> {
    #[cfg(any(feature = "file-io", target_arch = "wasm32"))]
    {
        components.iter().map(|c| c.to_write()).collect()
    }
    #[cfg(not(any(feature = "file-io", target_arch = "wasm32")))]
    {
        drop(components);
        Ok(Vec::new())
    }
}

/// Dictionary-layout emission over the interned codes: primary code chunks
/// cut as ranges of the coded dataset, index children built once over all of
/// it. The dictionary rides beside the stream for the serializer to place.
fn emit_dict_chunks(
    codes: QuadCodes,
    dict: Arc<TermDictionary>,
    indexes: Indexes,
    chunk_size: usize,
) -> Result<BuiltStream> {
    let components = component_writes(build_components_from_codes(&indexes, &codes)?)?;
    let n = codes.s.len();

    let n0 = n.min(chunk_size);
    let first = if n == 0 {
        build_struct_array(&[], LayoutStrategy::Dictionary, false)?
    } else {
        dictionary::build_code_chunk(&codes, 0..n0, true)?
    };
    let dtype = first.dtype().clone();

    let rest = stream::unfold((codes, n0), move |(codes, offset)| async move {
        if offset >= n {
            return None;
        }
        let end = (offset + chunk_size).min(n);
        let chunk =
            dictionary::build_code_chunk(&codes, offset..end, true).map_err(into_vortex_error);
        Some((chunk, (codes, end)))
    });

    let chunks: ChunkStream = stream::once(async move { Ok(first) }).chain(rest).boxed();
    Ok(BuiltStream {
        components,
        quads_sorted: true,
        dtype,
        chunks,
        dict: Some(dict),
    })
}
