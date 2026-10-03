//! The [`SortedStreamBuilder`] strategy: spill sorted runs to temporary
//! files, then K-way merge them back into one global (s, p, o, g) order.
//!
//! It offers the same sortedness guarantee as
//! [`sorted_in_memory`](super::sorted_in_memory) — globally sorted `s` and
//! index children, both binary-searchable — without holding the dataset,
//! paying for it in temp-file I/O. Requested indexes are merged from their
//! own spilled `(value, row id)` runs and stream straight out as components,
//! never materialized whole. The run file format itself belongs to
//! [`spill`](super::spill), the emission machinery to [`builders`](super);
//! what lives here is the merge.

use super::spill::{Run, RunMerger, RunSpiller, TempRunsGuard};
use super::stream_indexes::{IndexMergers, merge_quads_feeding_indexes, merger_components};
use super::{
    BuiltArray, BuiltStream, ChunkStream, DEFAULT_CHUNK_ROWS, VortexArrayBuilder,
    build_struct_array, into_vortex_error,
};
use crate::error::{Result, VortexRdfError};
use crate::store::RawQuad;
use crate::store::array::{chunked_or_single, with_subject_stamp};
use crate::store::indexes::{IndexComponent, IndexType, Indexes, known_component, unique_indexes};
use crate::store::layouts::dictionary::{TermCodeMap, TermDictionary, TermDictionaryBuilder};
use crate::store::layouts::{LayoutStrategy, dictionary};

use crate::debug;
use futures::{Stream, StreamExt, TryStreamExt, stream};
use std::path::Path;
use std::sync::Arc;

use vortex_array::ArrayRef;
use vortex_array::arrays::StructArray;
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

    /// After the (blocking) run-sort phase, merged chunks are built on demand
    /// as the file writer polls.
    async fn build_vortex_stream(
        quad_stream: Box<dyn Stream<Item = Result<RawQuad>> + Unpin + Send + 'static>,
        layout: LayoutStrategy,
        indexes: Indexes,
    ) -> Result<BuiltStream> {
        build_chunk_stream(quad_stream, layout, indexes, DEFAULT_CHUNK_ROWS, None).await
    }
}

/// Materialize the chunk stream into a single in-memory array.
///
/// The quad result is canonicalized and its `s` sortedness stat re-stamped
/// (assembling chunks loses the per-chunk stats that `match_pattern` gates
/// its binary searches on). The streamed index children are materialized
/// directly as the store's in-memory components, which `from_built` adopts
/// as they are.
pub(crate) async fn build_array(
    quad_stream: Box<dyn Stream<Item = Result<RawQuad>> + Unpin + Send + 'static>,
    layout: LayoutStrategy,
    indexes: Indexes,
    chunk_size: usize,
) -> Result<BuiltArray> {
    use vortex_array::VortexSessionExecute as _;

    let start = debug::timer();

    let BuiltStream {
        dtype,
        chunks,
        components: writes,
        dict,
        ..
    } = build_chunk_stream(quad_stream, layout, indexes.clone(), chunk_size, None).await?;
    let chunks: Vec<ArrayRef> = chunks.try_collect().await.map_err(VortexRdfError::Vortex)?;

    // Materialize each streamed component child as one canonical struct in
    // child schema. Sortedness is the descriptor's provenance — the mergers
    // emit each family in its global sort order — not an inspection.
    let mut components: Vec<IndexComponent> = Vec::new();
    let mut ctx = crate::session::VORTEX_SESSION.create_execution_ctx();
    for component in writes {
        let Some(known) = known_component(&component.descriptor.implementation) else {
            continue;
        };
        let arrays: Vec<ArrayRef> = component
            .source
            .open()
            .map_err(VortexRdfError::Vortex)?
            .try_collect()
            .await
            .map_err(VortexRdfError::Vortex)?;
        let part = chunked_or_single(arrays, component.descriptor.dtype.clone())?;
        let array = part
            .execute::<StructArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?;
        components.push(IndexComponent::built(
            known.identity,
            array,
            component.descriptor.sorted,
        ));
    }
    let assembled = chunked_or_single(chunks, dtype)?;
    // Correct by construction for this builder: every emission is a window
    // of the global merge, so the s column is globally sorted — the stamp
    // the store's adoption reads back.
    let result = with_subject_stamp(assembled, true)?;
    log::debug!(
        "[SortedStreamBuilder] Materialized {} quads in {:?}",
        result.len(),
        debug::elapsed(start)
    );
    Ok(BuiltArray {
        array: result,
        components,
        dict,
    })
}

/// External merge sort producing a lazily-evaluated stream of sorted chunks.
///
/// Phase 1 (ingest → sorted runs on disk) runs to completion before this
/// function returns — sorted output cannot be emitted until all input has been
/// seen. Without secondary indexes, the K-way merge then produces chunks only
/// when the consumer polls, keeping peak memory at heap + one chunk; with
/// them, the merge itself also runs eagerly (see [`SortedStreamBuilder`]) and
/// only chunk emission stays lazy. Temp run files are removed when the stream
/// is dropped.
///
/// `spill_dir` pins where the run files land (compaction points it at the
/// store file's own directory so spills share the output's volume); `None`
/// takes [`TempRunsGuard::create`]'s default resolution.
pub(crate) async fn build_chunk_stream(
    mut quads_in: Box<dyn Stream<Item = Result<RawQuad>> + Unpin + Send + 'static>,
    layout: LayoutStrategy,
    indexes: Indexes,
    chunk_size: usize,
    spill_dir: Option<&Path>,
) -> Result<BuiltStream> {
    let build_start = debug::timer();
    // ── Phase 1: Ingest and write sorted runs ──
    let ingest_start = debug::timer();
    let guard = Arc::new(TempRunsGuard::create("sorted_stream", spill_dir)?);

    // For the Dictionary layout, the global term dictionary is built
    // incrementally during this same ingestion pass.
    let mut dict_builder = (layout == LayoutStrategy::Dictionary).then(TermDictionaryBuilder::new);

    let mut spiller = RunSpiller::<RawQuad>::new(guard.path(), "quads", chunk_size);
    let mut total_ingested = 0usize;
    while let Some(res) = quads_in.next().await {
        let raw = res?;
        if let Some(b) = dict_builder.as_mut() {
            b.insert_quad(&raw);
        }
        spiller.push(raw)?;
        total_ingested += 1;
    }
    let merger = spiller.into_merger()?;
    log::debug!(
        "[SortedStreamBuilder] Ingested {} quads into {} runs in {:?} (dictionary collection={})",
        total_ingested,
        merger.run_count(),
        debug::elapsed(ingest_start),
        dict_builder.is_some()
    );
    let dict = dict_builder
        .map(|b| finish_dict(b, build_start))
        .transpose()?;

    // ── Phase 2: chunk emission ──
    // Any requested index means the two-pass pipeline: the index children are
    // globally sorted, which needs the quad merge's row ids (first pass)
    // before the pairs can be sorted and emitted (second pass). Spill only
    // the families the requested types need.
    let unique = unique_indexes(&indexes);
    if !unique.is_empty() {
        let want_ref = unique.contains(&IndexType::SecondaryByReference);
        let want_copy = unique.contains(&IndexType::SecondaryByCopy);
        return match dict {
            Some((dict, code_map)) => {
                let codes = Arc::clone(&code_map);
                let (merged, mergers) = merge_quads_feeding_indexes(
                    merger,
                    guard.path(),
                    chunk_size,
                    want_ref,
                    want_copy,
                    move |term| dictionary::code_of(&codes, term),
                )?;
                emit_merged_run_dict_chunks(merged, mergers, dict, code_map, chunk_size, guard)
            }
            None => {
                let (merged, mergers) = merge_quads_feeding_indexes(
                    merger,
                    guard.path(),
                    chunk_size,
                    want_ref,
                    want_copy,
                    |term| Ok(term.to_string()),
                )?;
                emit_merged_run_chunks(merged, mergers, layout, chunk_size, guard)
            }
        };
    }

    // ── No secondary indexes: lazily emit merged chunks ──
    match dict {
        Some((dict, code_map)) => emit_dict_chunks(merger, dict, code_map, chunk_size, guard),
        None => {
            let (dtype, chunks) = chunk_stream(
                (merger, guard),
                chunk_size,
                |(merger, _guard), n| merger.next_batch(n),
                move |buf| build_struct_array(buf, layout, true),
                || build_struct_array(&[], layout, false),
            )?;
            Ok(BuiltStream {
                dtype,
                chunks,
                components: Vec::new(),
                quads_sorted: true,
                dict: None,
            })
        }
    }
}

/// Freeze the dictionary collected during ingest.
fn finish_dict(
    builder: TermDictionaryBuilder,
    build_start: Option<web_time::Instant>,
) -> Result<(Arc<TermDictionary>, Arc<TermCodeMap>)> {
    let dict_start = debug::timer();
    let (dict, code_map) = builder.finish()?;
    log::debug!(
        "[SortedStreamBuilder] Finalized dictionary of {} terms in {:?} ({:?} since build start)",
        dict.len(),
        debug::elapsed(dict_start),
        debug::elapsed(build_start)
    );
    Ok((Arc::new(dict), Arc::new(code_map)))
}

/// The eager-first-chunk-then-unfold emission every chunk stream here shares:
/// `pull` takes up to `chunk_size` quads off `source` in global order, `build`
/// turns a non-empty batch into one primary chunk, and `empty` supplies the
/// schema-carrying chunk of an empty dataset. The first chunk is built before
/// returning so the dtype is known up front; the rest are built as polled.
fn chunk_stream<S: Send + 'static>(
    mut source: S,
    chunk_size: usize,
    mut pull: impl FnMut(&mut S, usize) -> Result<Vec<RawQuad>> + Send + 'static,
    build: impl Fn(&[RawQuad]) -> Result<ArrayRef> + Send + 'static,
    empty: impl FnOnce() -> Result<ArrayRef>,
) -> Result<(DType, ChunkStream)> {
    let buf = pull(&mut source, chunk_size)?;
    let first = if buf.is_empty() {
        empty()?
    } else {
        build(&buf)?
    };
    let dtype = first.dtype().clone();
    drop(buf);

    let rest = stream::unfold(
        (source, pull, build),
        move |(mut source, mut pull, build)| async move {
            let chunk = (|| {
                let buf = pull(&mut source, chunk_size)?;
                if buf.is_empty() {
                    return Ok(None);
                }
                build(&buf).map(Some)
            })();
            match chunk {
                Ok(None) => None,
                Ok(Some(c)) => Some((Ok(c), (source, pull, build))),
                Err(e) => Some((Err(into_vortex_error(e)), (source, pull, build))),
            }
        },
    );

    let chunks: ChunkStream = stream::once(async move { Ok(first) }).chain(rest).boxed();
    Ok((dtype, chunks))
}

/// Second pass of the indexed pipeline (string layouts): lazily re-read the merged
/// quads in chunk-size batches as primary-only chunks, while each index
/// family's merger streams its own child component beside them.
fn emit_merged_run_chunks(
    merged: Run<RawQuad>,
    mergers: IndexMergers<String>,
    layout: LayoutStrategy,
    chunk_size: usize,
    guard: Arc<TempRunsGuard>,
) -> Result<BuiltStream> {
    let components = merger_components(
        mergers,
        chunk_size,
        &guard,
        false,
        crate::store::indexes::reference::out_of_core::ref_child_chunk,
    )?;
    let (dtype, chunks) = chunk_stream(
        (merged, guard),
        chunk_size,
        |(merged, _guard), n| merged.next_batch(n),
        move |buf| build_struct_array(buf, layout, true),
        || build_struct_array(&[], layout, false),
    )?;
    Ok(BuiltStream {
        dtype,
        chunks,
        components,
        quads_sorted: true,
        dict: None,
    })
}

/// Dictionary-layout variant of [`emit_merged_run_chunks`]: the entries hold
/// u32 codes; the dictionary rides beside the stream for the serializer.
fn emit_merged_run_dict_chunks(
    merged: Run<RawQuad>,
    mergers: IndexMergers<u32>,
    dict: Arc<TermDictionary>,
    code_map: Arc<TermCodeMap>,
    chunk_size: usize,
    guard: Arc<TempRunsGuard>,
) -> Result<BuiltStream> {
    let components = merger_components(
        mergers,
        chunk_size,
        &guard,
        true,
        crate::store::indexes::reference::out_of_core::ref_child_chunk,
    )?;
    let (dtype, chunks) = chunk_stream(
        (merged, guard),
        chunk_size,
        |(merged, _guard), n| merged.next_batch(n),
        move |buf| dictionary::build_chunk(buf, &code_map, true),
        || build_struct_array(&[], LayoutStrategy::Dictionary, false),
    )?;
    Ok(BuiltStream {
        dtype,
        chunks,
        components,
        quads_sorted: true,
        dict: Some(dict),
    })
}

/// Dictionary-layout emission over the K-way merge (no secondary indexes):
/// chunks of u32 codes encoded against the completed global dictionary,
/// which rides beside the stream for the serializer to place.
fn emit_dict_chunks(
    merger: RunMerger<RawQuad>,
    dict: Arc<TermDictionary>,
    code_map: Arc<TermCodeMap>,
    chunk_size: usize,
    guard: Arc<TempRunsGuard>,
) -> Result<BuiltStream> {
    let (dtype, chunks) = chunk_stream(
        (merger, guard),
        chunk_size,
        |(merger, _guard), n| merger.next_batch(n),
        move |buf| dictionary::build_chunk(buf, &code_map, true),
        || build_struct_array(&[], LayoutStrategy::Dictionary, false),
    )?;
    Ok(BuiltStream {
        dtype,
        chunks,
        components: Vec::new(),
        quads_sorted: true,
        dict: Some(dict),
    })
}
