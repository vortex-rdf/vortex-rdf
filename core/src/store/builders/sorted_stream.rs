//! The [`SortedStreamBuilder`] strategy: spill sorted runs to temporary
//! files, then K-way merge them back into one global (s, p, o, g) order.
//!
//! It offers the same sortedness guarantee as
//! [`sorted_in_memory`](super::sorted_in_memory) — globally sorted `s` and
//! index children, both binary-searchable — without holding the dataset,
//! paying for it in temp-file I/O. Requested indexes are merged from their
//! own spilled `(value, row id)` runs and stream straight out as components,
//! never materialized whole. The run file format itself belongs to
//! [`spill`](super::spill), the index families' runs and components to
//! [`stream_indexes`](super::stream_indexes), the chunk build to
//! [`builders`](super); what lives here is the merge and the one chunk
//! stream it feeds.
//!
//! Quads are unique and terms are canonical in what this builder emits. The
//! quad runs are spilled *distinct*: a quad that arrives more than once, in
//! one run or in several, is merged out once, and the dictionary collects
//! the terms of the parsed (so canonical) quads as a set. Row ids are
//! assigned by that merge, after the repeats are gone, so every index
//! family's `(value, row id)` records describe the deduplicated rows — and
//! are themselves unique, since each carries a row id of its own. The
//! families are spilled and merged as they are, never deduplicated.

use super::spill::{RunSpiller, TempRunsGuard};
use super::stream_indexes::merge_feeding_indexes;
use super::{
    BuiltArray, BuiltStream, ChunkStream, DEFAULT_CHUNK_ROWS, VortexArrayBuilder,
    build_struct_array,
};
use crate::error::Result;
use crate::store::RawQuad;
use crate::store::array::{chunked_or_single, with_subject_stamp};
use crate::store::indexes::{IndexComponent, Indexes};
use crate::store::layouts::dictionary::{TermCodeMap, TermDictionary, TermDictionaryBuilder};
use crate::store::layouts::{LayoutStrategy, dictionary};

use crate::debug;
use futures::{Stream, StreamExt, TryStreamExt, stream};
use std::path::Path;
use std::sync::Arc;

use vortex_array::ArrayRef;
use vortex_array::dtype::DType;

/// Out-of-core globally sorted Vortex RDF Array Builder.
///
/// Processes datasets larger than available memory using external merge sort:
/// sorted runs are spilled to disk, then K-way merged into fixed-size chunks.
///
/// With any secondary index requested, the quad merge runs eagerly to a spill
/// (row ids are assigned by the merge) while each index family's
/// `(value, row id)` entries are spilled as sorted runs; each family then
/// streams its child straight off its own merger beside the lazily re-read
/// quad chunks. Without indexes a single lazy merge pass emits chunks as the
/// consumer polls.
pub struct SortedStreamBuilder;

impl VortexArrayBuilder for SortedStreamBuilder {
    async fn build_vortex_array(
        quad_stream: Box<dyn Stream<Item = Result<RawQuad>> + Unpin + Send + 'static>,
        layout: LayoutStrategy,
        indexes: Indexes,
    ) -> Result<BuiltArray> {
        build_array(quad_stream, layout, indexes, DEFAULT_CHUNK_ROWS).await
    }
}

impl SortedStreamBuilder {
    /// The dataset as a lazy chunk stream for the file writer. After the
    /// (blocking) run-sort phase, merged chunks are built on demand as the
    /// writer polls.
    #[cfg(feature = "file-io")]
    pub(crate) async fn build_vortex_stream(
        quad_stream: Box<dyn Stream<Item = Result<RawQuad>> + Unpin + Send + 'static>,
        layout: LayoutStrategy,
        indexes: Indexes,
    ) -> Result<BuiltStream> {
        build_chunk_stream(quad_stream, layout, indexes, DEFAULT_CHUNK_ROWS, None).await
    }
}

/// The chunk stream materialized into one array, its `s` column stamped
/// sorted — every chunk is a window of the global merge, and assembling
/// chunks loses the per-chunk stats `match_pattern` gates its binary searches
/// on — and each streamed index child materialized as an in-memory component
/// ([`IndexComponent::from_write`]), which `from_built` adopts as it is.
pub(crate) async fn build_array(
    quad_stream: Box<dyn Stream<Item = Result<RawQuad>> + Unpin + Send + 'static>,
    layout: LayoutStrategy,
    indexes: Indexes,
    chunk_size: usize,
) -> Result<BuiltArray> {
    let start = debug::timer();
    let BuiltStream {
        dtype,
        chunks,
        components: writes,
        dict,
        ..
    } = build_chunk_stream(quad_stream, layout, indexes, chunk_size, None).await?;
    let chunks: Vec<ArrayRef> = chunks.try_collect().await?;
    let mut components = Vec::with_capacity(writes.len());
    for write in &writes {
        components.push(IndexComponent::from_write(write).await?);
    }
    let array = with_subject_stamp(chunked_or_single(chunks, dtype)?, true)?;
    log::debug!(
        "[SortedStreamBuilder] Materialized {} quads in {:?}",
        array.len(),
        debug::elapsed(start)
    );
    Ok(BuiltArray {
        array,
        components,
        dict,
    })
}

/// How merged quads become primary chunks: a string layout's columns, or
/// codes against the dictionary collected during ingest.
enum Encoding {
    Strings(LayoutStrategy),
    Codes {
        dict: Arc<TermDictionary>,
        code_map: TermCodeMap,
    },
}

impl Encoding {
    fn strategy(&self) -> LayoutStrategy {
        match self {
            Encoding::Strings(layout) => *layout,
            Encoding::Codes { .. } => LayoutStrategy::Dictionary,
        }
    }

    /// `quads`, a non-empty window of the global merge, as one chunk with
    /// `s` stamped sorted.
    fn chunk(&self, quads: &[RawQuad]) -> Result<ArrayRef> {
        match self {
            Encoding::Strings(layout) => build_struct_array(quads, *layout, true),
            Encoding::Codes { code_map, .. } => dictionary::build_chunk(quads, code_map, true),
        }
    }

    /// The dictionary that rides beside the chunks for the serializer.
    fn dict(&self) -> Option<Arc<TermDictionary>> {
        match self {
            Encoding::Strings(_) => None,
            Encoding::Codes { dict, .. } => Some(Arc::clone(dict)),
        }
    }
}

/// External merge sort producing a lazily evaluated stream of sorted chunks,
/// each distinct quad once.
///
/// Ingest to sorted runs on disk runs to completion before this returns —
/// sorted output cannot be emitted until all input has been seen. Without
/// secondary indexes the K-way merge then produces chunks only when the
/// consumer polls, keeping peak memory at heap + one chunk; with them the
/// merge itself runs eagerly (it assigns the row ids the index runs carry,
/// see [`SortedStreamBuilder`]) and only chunk emission stays lazy. Either
/// way the merged quads leave through one emission. Temp run files are
/// removed when the stream is dropped.
///
/// `spill_dir` pins where the run files land (compaction points it at the
/// directory of its temp file, beside the file it replaces, so spills share
/// the output's volume); `None` takes [`TempRunsGuard::create`]'s default
/// resolution.
pub(crate) async fn build_chunk_stream(
    mut quads_in: Box<dyn Stream<Item = Result<RawQuad>> + Unpin + Send + 'static>,
    layout: LayoutStrategy,
    indexes: Indexes,
    chunk_size: usize,
    spill_dir: Option<&Path>,
) -> Result<BuiltStream> {
    let start = debug::timer();
    let guard = Arc::new(TempRunsGuard::create("sorted_stream", spill_dir)?);

    // For the Dictionary layout, the global term dictionary is built
    // incrementally during this same ingestion pass.
    let mut dict_builder = (layout == LayoutStrategy::Dictionary).then(TermDictionaryBuilder::new);

    // Distinct: a repeated quad must be one row, and one row id.
    let mut spiller = RunSpiller::<RawQuad>::distinct(guard.path(), "quads", chunk_size);
    let mut ingested = 0usize;
    while let Some(res) = quads_in.next().await {
        let raw = res?;
        if let Some(b) = dict_builder.as_mut() {
            b.insert_quad(&raw);
        }
        spiller.push(raw)?;
        ingested += 1;
    }
    let merger = spiller.into_merger()?;
    let encoding = match dict_builder {
        Some(builder) => {
            let (dict, code_map) = builder.finish()?;
            Encoding::Codes {
                dict: Arc::new(dict),
                code_map,
            }
        }
        None => Encoding::Strings(layout),
    };
    log::debug!(
        "[SortedStreamBuilder] Ingested {} quads into {} runs in {:?} (dictionary={})",
        ingested,
        merger.run_count(),
        debug::elapsed(start),
        matches!(encoding, Encoding::Codes { .. })
    );

    // Any requested index means a first pass over the merge: the index
    // children are globally sorted, which needs the row ids the merge
    // assigns before their entries can be sorted. Only the families the
    // requested types need are spilled, each once however often requested.
    let (merger, components) = if indexes.is_empty() {
        (merger, Vec::new())
    } else {
        match &encoding {
            Encoding::Codes { code_map, .. } => {
                merge_feeding_indexes(merger, &guard, chunk_size, &indexes, |term| {
                    dictionary::code_of(code_map, term)
                })?
            }
            Encoding::Strings(_) => {
                merge_feeding_indexes(merger, &guard, chunk_size, &indexes, |term| {
                    Ok(term.to_string())
                })?
            }
        }
    };

    let dict = encoding.dict();
    let strategy = encoding.strategy();
    let (dtype, chunks) = chunk_stream(
        (merger, guard),
        chunk_size,
        move |(merger, _guard), n| {
            let batch = merger.next_batch(n)?;
            if batch.is_empty() {
                return Ok(None);
            }
            encoding.chunk(&batch).map(Some)
        },
        || build_struct_array(&[], strategy, false),
    )?;
    Ok(BuiltStream {
        dtype,
        chunks,
        components,
        quads_sorted: true,
        dict,
    })
}

/// The eager-first-chunk-then-unfold emission: `next` yields the next chunk
/// of at most `chunk_size` rows off `source`, `None` once exhausted, and
/// `empty` supplies the schema-carrying chunk of an empty dataset. The first
/// chunk is built before returning so the dtype is known up front; the rest
/// are built as polled.
fn chunk_stream<S: Send + 'static>(
    mut source: S,
    chunk_size: usize,
    mut next: impl FnMut(&mut S, usize) -> Result<Option<ArrayRef>> + Send + 'static,
    empty: impl FnOnce() -> Result<ArrayRef>,
) -> Result<(DType, ChunkStream)> {
    let first = match next(&mut source, chunk_size)? {
        Some(chunk) => chunk,
        None => empty()?,
    };
    let dtype = first.dtype().clone();
    let rest = stream::unfold((source, next), move |(mut source, mut next)| async move {
        match next(&mut source, chunk_size) {
            Ok(None) => None,
            Ok(Some(chunk)) => Some((Ok(chunk), (source, next))),
            Err(e) => Some((Err(e.into()), (source, next))),
        }
    });
    let chunks: ChunkStream = stream::once(async move { Ok(first) }).chain(rest).boxed();
    Ok((dtype, chunks))
}
