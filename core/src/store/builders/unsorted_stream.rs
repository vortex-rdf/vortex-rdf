use super::spill::{RunReader, RunWriter, TempRunsGuard, make_temp_dir};
use super::{
    ChunkStream, DEFAULT_CHUNK_SIZE, VortexArrayBuilder, assemble_chunks, build_struct_array,
    into_vortex_error, make_empty_struct,
};
use crate::error::{Result, VortexRdfError};
use crate::store::RawQuad;
use crate::store::indexes::Indexes;
use crate::store::layouts::default::DirectChunkBuilder;
use crate::store::layouts::term_dictionary::{TermDictionary, TermDictionaryBuilder};
use crate::store::layouts::{LayoutStrategy, dictionary};

use futures::{Stream, StreamExt, TryStreamExt, stream};
use oxrdf::Quad;
use std::sync::Arc;
use web_time::Instant;

use vortex_array::ArrayRef;
use vortex_array::dtype::DType;

/// Unsorted Vortex RDF Array Builder.
///
/// Quads are ingested in natural insertion order and built into fixed-size
/// StructArray chunks:
///
/// - `build_vortex_stream` (used when serializing to a file) produces chunks
///   lazily as the Vortex writer polls for them, so peak memory is bounded by
///   the chunk size rather than the dataset size.
/// - `build_vortex_array` (used for in-memory stores) collects the same chunks
///   into a single (possibly chunked) array.
pub struct UnsortedStreamBuilder;

impl VortexArrayBuilder for UnsortedStreamBuilder {
    async fn build_vortex_array(
        quad_stream: Box<dyn Stream<Item = Result<Quad>> + Unpin + Send + 'static>,
        layout: LayoutStrategy,
        indexes: Indexes,
    ) -> Result<ArrayRef> {
        let start = Instant::now();

        // Dictionary layout: the result is materialized anyway, so buffer the
        // quads in memory (no disk spill) and build one contiguous chunk.
        if layout == LayoutStrategy::Dictionary {
            let mut quads = quad_stream;
            let mut buf: Vec<RawQuad> = Vec::new();
            while let Some(res) = quads.next().await {
                buf.push(RawQuad::from_quad(&res?));
            }
            let dict = TermDictionary::from_quads(&buf)?;
            let id_map = dict.build_id_map();
            // Single contiguous chunk == whole dataset: index columns are
            // globally sorted and stamped for binary-search routing.
            let result =
                dictionary::build_chunk(&buf, &dict, &id_map, &indexes, 0, false, true, true)?;
            log::debug!(
                "[UnsortedStreamBuilder] Materialized {} dictionary-encoded quads in {:?}",
                result.len(),
                start.elapsed()
            );
            return Ok(result);
        }

        let (_dtype, chunks) =
            build_chunk_stream(quad_stream, layout, indexes.clone(), DEFAULT_CHUNK_SIZE).await?;
        let chunks: Vec<ArrayRef> = chunks.try_collect().await.map_err(VortexRdfError::Vortex)?;

        let result = assemble_chunks(chunks, layout, &indexes)?;
        log::debug!(
            "[UnsortedStreamBuilder] Materialized {} quads in {:?}",
            result.len(),
            start.elapsed()
        );
        Ok(result)
    }

    /// True streaming implementation: chunks are built on demand as the file
    /// writer polls the stream.
    async fn build_vortex_stream(
        quad_stream: Box<dyn Stream<Item = Result<Quad>> + Unpin + Send + 'static>,
        layout: LayoutStrategy,
        indexes: Indexes,
    ) -> Result<(DType, ChunkStream)> {
        build_chunk_stream(quad_stream, layout, indexes, DEFAULT_CHUNK_SIZE).await
    }
}

/// Produce the schema dtype and a lazily-evaluated stream of StructArray chunks.
///
/// The first chunk is read eagerly because the Vortex writer needs the schema
/// dtype before the first chunk arrives (and it surfaces input errors early).
/// Subsequent chunks are built only when the consumer polls for them, each
/// carrying global row IDs via `start_row` so index columns stay valid across
/// the assembled file.
pub(crate) async fn build_chunk_stream(
    quads: Box<dyn Stream<Item = Result<Quad>> + Unpin + Send + 'static>,
    layout: LayoutStrategy,
    indexes: Indexes,
    chunk_size: usize,
) -> Result<(DType, ChunkStream)> {
    // Dictionary layout: the global dictionary must be complete before any
    // encoded chunk can be emitted, so this runs a two-pass spill pipeline.
    if layout == LayoutStrategy::Dictionary {
        return build_dict_chunk_stream(quads, indexes, chunk_size).await;
    }
    // Fast path: with the Default layout and no index columns, terms are
    // formatted straight into the column builders — no intermediate RawQuad
    // strings are allocated.
    if layout == LayoutStrategy::Default && indexes.is_empty() {
        return build_direct_chunk_stream(quads, chunk_size).await;
    }
    build_buffered_chunk_stream(quads, layout, indexes, chunk_size).await
}

/// Two-pass Dictionary-layout chunk stream.
///
/// Pass 1 spills quads to a temp file in arrival order while incrementally
/// collecting the unique terms; the dictionary is then sorted and frozen.
/// Pass 2 reads the spill back and lazily emits u32-encoded chunks, the first
/// carrying the dictionary payload. Peak memory: O(unique terms + chunk).
async fn build_dict_chunk_stream(
    mut quads: Box<dyn Stream<Item = Result<Quad>> + Unpin + Send + 'static>,
    indexes: Indexes,
    chunk_size: usize,
) -> Result<(DType, ChunkStream)> {
    // ── Pass 1: spill + incremental dictionary ──
    let temp_dir = make_temp_dir("unsorted_dict")?;
    let guard = TempRunsGuard {
        dir: temp_dir.clone(),
    };
    let spill_path = temp_dir.join("quads.bin");

    let mut writer = RunWriter::create(&spill_path)?;
    let mut dict_builder = TermDictionaryBuilder::new();
    let mut total = 0usize;
    while let Some(res) = quads.next().await {
        let raw = RawQuad::from_quad(&res?);
        dict_builder.insert_quad(&raw);
        writer.push(&raw)?;
        total += 1;
    }
    writer.finish()?;
    let dict = Arc::new(dict_builder.finish()?);
    let id_map = Arc::new(dict.build_id_map());
    log::debug!(
        "[UnsortedStreamBuilder] Spilled {} quads; dictionary of {} terms",
        total,
        dict.len()
    );

    // ── Pass 2: lazily emit encoded chunks from the spill ──
    let mut reader: RunReader<RawQuad> = RunReader::new(&spill_path)?;
    let mut buf: Vec<RawQuad> = Vec::with_capacity(chunk_size.min(4096));
    while buf.len() < chunk_size {
        match reader.next()? {
            Some(q) => buf.push(q),
            None => break,
        }
    }

    let first = if buf.is_empty() {
        dictionary::empty_struct(&indexes)?
    } else {
        // `total` is known from pass 1: a first chunk that covers everything
        // holds globally sorted index columns and gets them stamped.
        dictionary::build_chunk(
            &buf,
            &dict,
            &id_map,
            &indexes,
            0,
            false,
            true,
            total <= chunk_size,
        )?
    };
    let dtype = first.dtype().clone();
    let next_row = buf.len() as u32;
    drop(buf);

    let rest = stream::unfold(
        (reader, dict, id_map, indexes, next_row, guard),
        move |(mut reader, dict, id_map, indexes, row, guard)| async move {
            let mut buf: Vec<RawQuad> = Vec::with_capacity(chunk_size.min(4096));
            while buf.len() < chunk_size {
                match reader.next() {
                    Ok(Some(q)) => buf.push(q),
                    Ok(None) => break,
                    Err(e) => {
                        return Some((
                            Err(into_vortex_error(e)),
                            (reader, dict, id_map, indexes, row, guard),
                        ));
                    }
                }
            }
            if buf.is_empty() {
                return None;
            }
            let n = buf.len() as u32;
            let chunk =
                dictionary::build_chunk(&buf, &dict, &id_map, &indexes, row, false, false, false)
                    .map_err(into_vortex_error);
            Some((chunk, (reader, dict, id_map, indexes, row + n, guard)))
        },
    );

    let chunks: ChunkStream = stream::once(async move { Ok(first) }).chain(rest).boxed();
    Ok((dtype, chunks))
}

/// General chunk-stream path: quads are buffered as `RawQuad`s per chunk, as
/// required by index building (whole-chunk sorts) and the TypedObject layout.
async fn build_buffered_chunk_stream(
    mut quads: Box<dyn Stream<Item = Result<Quad>> + Unpin + Send + 'static>,
    layout: LayoutStrategy,
    indexes: Indexes,
    chunk_size: usize,
) -> Result<(DType, ChunkStream)> {
    let mut buf: Vec<RawQuad> = Vec::with_capacity(chunk_size.min(4096));
    while buf.len() < chunk_size {
        match quads.next().await {
            Some(res) => buf.push(RawQuad::from_quad(&res?)),
            None => break,
        }
    }

    let first = if buf.is_empty() {
        make_empty_struct(layout, &indexes)?
    } else {
        // A first chunk shorter than chunk_size means the stream is exhausted:
        // the chunk is the whole dataset and its index columns are globally
        // sorted (stamped for binary-search routing).
        build_struct_array(
            &buf,
            layout,
            &indexes,
            buf.len(),
            0,
            false,
            buf.len() < chunk_size,
        )?
    };
    let dtype = first.dtype().clone();
    let next_row = buf.len() as u32;
    drop(buf);

    let rest = stream::unfold(
        (quads, layout, indexes, next_row),
        move |(mut quads, layout, indexes, row)| async move {
            let mut buf: Vec<RawQuad> = Vec::with_capacity(chunk_size.min(4096));
            while buf.len() < chunk_size {
                match quads.next().await {
                    Some(Ok(q)) => buf.push(RawQuad::from_quad(&q)),
                    Some(Err(e)) => {
                        return Some((Err(into_vortex_error(e)), (quads, layout, indexes, row)));
                    }
                    None => break,
                }
            }
            if buf.is_empty() {
                return None;
            }
            let n = buf.len();
            let chunk = build_struct_array(&buf, layout, &indexes, n, row, false, false)
                .map_err(into_vortex_error);
            Some((chunk, (quads, layout, indexes, row + n as u32)))
        },
    );

    let chunks: ChunkStream = stream::once(async move { Ok(first) }).chain(rest).boxed();
    Ok((dtype, chunks))
}

/// Fast chunk-stream path for the Default layout without indexes: appends
/// term strings directly into per-column builders, skipping the `RawQuad`
/// intermediate (4 String allocations + frees per quad) entirely.
async fn build_direct_chunk_stream(
    mut quads: Box<dyn Stream<Item = Result<Quad>> + Unpin + Send + 'static>,
    chunk_size: usize,
) -> Result<(DType, ChunkStream)> {
    let mut builder = DirectChunkBuilder::new(chunk_size.min(4096));
    while builder.len() < chunk_size {
        match quads.next().await {
            Some(res) => builder.push(&res?),
            None => break,
        }
    }

    let first = if builder.is_empty() {
        make_empty_struct(LayoutStrategy::Default, &Vec::new())?
    } else {
        builder.finish()?
    };
    let dtype = first.dtype().clone();

    let rest = stream::unfold(quads, move |mut quads| async move {
        let mut builder = DirectChunkBuilder::new(chunk_size.min(4096));
        while builder.len() < chunk_size {
            match quads.next().await {
                Some(Ok(q)) => builder.push(&q),
                Some(Err(e)) => return Some((Err(into_vortex_error(e)), quads)),
                None => break,
            }
        }
        if builder.is_empty() {
            return None;
        }
        Some((builder.finish().map_err(into_vortex_error), quads))
    });

    let chunks: ChunkStream = stream::once(async move { Ok(first) }).chain(rest).boxed();
    Ok((dtype, chunks))
}
