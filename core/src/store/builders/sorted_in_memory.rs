//! The [`SortedInMemoryBuilder`] strategy: hold the whole dataset, sort it
//! once by (s, p, o, g), and emit chunks as windows of that single order.

use super::{
    BuiltArray, BuiltStream, DEFAULT_CHUNK_ROWS, VortexArrayBuilder, build_components,
    build_components_from_codes, build_struct_array, chunk_stream,
};
use crate::error::Result;
use crate::store::RawQuad;
use crate::store::indexes::Indexes;
use crate::store::layouts::dictionary::ingest::{InterningQuadBuilder, finish_interned};
use crate::store::layouts::{LayoutStrategy, dictionary};

use crate::debug;
use futures::{Stream, StreamExt};
use std::ops::Range;
use std::sync::Arc;

/// Fully in-memory, globally sorted builder: the quads are sorted by
/// (s, p, o, g) in memory and every requested index child is built over the
/// same sorted dataset, so each child is globally sorted and
/// binary-searchable. Peak memory is O(dataset).
pub struct SortedInMemoryBuilder;

impl VortexArrayBuilder for SortedInMemoryBuilder {
    async fn build_vortex_array(
        quad_stream: Box<dyn Stream<Item = Result<RawQuad>> + Unpin + Send + 'static>,
        layout: LayoutStrategy,
        indexes: Indexes,
    ) -> Result<BuiltArray> {
        let start = debug::timer();
        // The Dictionary layout interns terms as the stream drains and sorts
        // the coded rows; the string layouts sort the raw quads.
        let built = if layout == LayoutStrategy::Dictionary {
            let interner = InterningQuadBuilder::from_stream(quad_stream).await?;
            finish_interned(interner, &indexes)?
        } else {
            let quads = ingest_and_sort(quad_stream).await?;
            BuiltArray {
                array: build_struct_array(&quads, layout, true)?,
                components: build_components(&indexes, &quads)?,
                dict: None,
            }
        };
        log::debug!(
            "[SortedInMemoryBuilder] Built {} quads in {:?}",
            built.array.len(),
            debug::elapsed(start)
        );
        Ok(built)
    }

    async fn build_vortex_stream(
        quad_stream: Box<dyn Stream<Item = Result<RawQuad>> + Unpin + Send + 'static>,
        layout: LayoutStrategy,
        indexes: Indexes,
    ) -> Result<BuiltStream> {
        build_chunk_stream(quad_stream, layout, indexes, DEFAULT_CHUNK_ROWS).await
    }
}

/// The full quad stream, sorted globally by (s, p, o, g).
async fn ingest_and_sort(
    mut quads_in: Box<dyn Stream<Item = Result<RawQuad>> + Unpin + Send + 'static>,
) -> Result<Vec<RawQuad>> {
    let mut quads: Vec<RawQuad> = Vec::new();
    while let Some(res) = quads_in.next().await {
        quads.push(res?);
    }
    quads.sort_unstable();
    Ok(quads)
}

/// The next window of at most `n` rows at `*at` in `len` rows, advancing
/// `at`; `None` past the end.
fn next_window(at: &mut usize, len: usize, n: usize) -> Option<Range<usize>> {
    if *at >= len {
        return None;
    }
    let end = (*at + n).min(len);
    let range = *at..end;
    *at = end;
    Some(range)
}

/// Ingest, sort, then emit primary chunks of `chunk_size` rows as windows of
/// the sorted dataset; the index children are built once over all of it and
/// ride beside the stream as complete components. `chunk_size` is a test
/// parameter; `build_vortex_stream` passes `DEFAULT_CHUNK_ROWS`.
pub(crate) async fn build_chunk_stream(
    quad_stream: Box<dyn Stream<Item = Result<RawQuad>> + Unpin + Send + 'static>,
    layout: LayoutStrategy,
    indexes: Indexes,
    chunk_size: usize,
) -> Result<BuiltStream> {
    let start = debug::timer();
    if layout == LayoutStrategy::Dictionary {
        let (dict, codes) = InterningQuadBuilder::from_stream(quad_stream)
            .await?
            .finish()?;
        let components = component_writes(build_components_from_codes(&indexes, &codes)?)?;
        let len = codes.s.len();
        log::debug!(
            "[SortedInMemoryBuilder] Interned and sorted {} quads in {:?}",
            len,
            debug::elapsed(start)
        );
        let (dtype, chunks) = chunk_stream(
            (codes, 0usize),
            chunk_size,
            move |(codes, at), n| {
                next_window(at, len, n)
                    .map(|range| dictionary::build_code_chunk(codes, range, true))
                    .transpose()
            },
            || build_struct_array(&[], layout, false),
        )?;
        return Ok(BuiltStream::sorted(
            dtype,
            chunks,
            components,
            Some(Arc::new(dict)),
        ));
    }

    let quads = ingest_and_sort(quad_stream).await?;
    let components = component_writes(build_components(&indexes, &quads)?)?;
    let len = quads.len();
    log::debug!(
        "[SortedInMemoryBuilder] Sorted {} quads in {:?}",
        len,
        debug::elapsed(start)
    );
    let (dtype, chunks) = chunk_stream(
        (quads, 0usize),
        chunk_size,
        move |(quads, at), n| {
            next_window(at, len, n)
                .map(|range| build_struct_array(&quads[range], layout, true))
                .transpose()
        },
        || build_struct_array(&[], layout, false),
    )?;
    Ok(BuiltStream::sorted(dtype, chunks, components, None))
}

/// The built children as writable components, each a replayable
/// single-chunk source. A build with no serializer compiled in has nothing
/// to hand them to and returns an empty roster.
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
