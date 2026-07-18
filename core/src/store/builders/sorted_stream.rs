use crate::common::utils::stamp_is_sorted;
use crate::error::{Result, VortexRdfError};
use crate::store::RawQuad;
use crate::store::layouts::{dictionary, LayoutStrategy};
use crate::store::layouts::term_dictionary::{TermDictionary, TermDictionaryBuilder};
use crate::store::indexes::{indexes_need_global_sorted_emission, unique_indexes, Indexes, IndexType};
use crate::store::indexes::secondary_by_copy::{self, CopyKey};
use crate::store::indexes::secondary_by_reference::append_sorted_string_pairs;
use super::{
    VortexArrayBuilder, ChunkStream,
    assemble_chunks, build_struct_array, canonicalize_sorted, make_empty_struct,
    into_vortex_error, DEFAULT_CHUNK_SIZE,
};
use super::spill::{
    make_temp_dir, write_run, PairMerger, PairRunSpiller, RunReader, RunWriter, TempRunsGuard,
};

use std::path::{Path, PathBuf};
use std::sync::Arc;
use web_time::Instant;
use std::collections::BinaryHeap;
use futures::{stream, Stream, StreamExt, TryStreamExt};
use oxrdf::Quad;
use rkyv::api::high::{HighDeserializer, HighSerializer};
use rkyv::rancor::Error as RkyvError;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};

use vortex_array::ArrayRef;
use vortex_array::arrays::StructArray;
use vortex_array::{IntoArray, dtype::DType};
use vortex_array::validity::Validity;

/// Out-of-core globally sorted Vortex RDF Array Builder.
///
/// Processes datasets larger than available memory using external merge sort:
/// sorted runs are spilled to disk, then K-way merged into fixed-size chunks.
///
/// When Reference secondary indexes are requested, the pipeline runs a second
/// external sort so the index columns come out in *global* sorted order
/// (stamped `IsSorted`, binary-searchable): the quad merge is run eagerly to
/// a spill — row IDs are only known as the merge assigns them — while the
/// `(value, row ID)` pairs are spilled as sorted runs, then chunk emission
/// zips the re-read quads with the pair merges. This roughly doubles disk
/// I/O; without indexes the original lazy single-pass merge is used.
pub struct SortedStreamBuilder;

struct HeapItem {
    quad: RawQuad,
    reader_idx: usize,
}

impl Eq for HeapItem {}
impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool { self.quad == other.quad }
}
impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.quad.cmp(&self.quad) // reversed for min-heap
    }
}
impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> { Some(self.cmp(other)) }
}

impl VortexArrayBuilder for SortedStreamBuilder {
    async fn build_vortex_array(
        quad_stream: Box<dyn Stream<Item = Result<Quad>> + Unpin + Send + 'static>,
        layout: LayoutStrategy,
        indexes: Indexes,
    ) -> Result<ArrayRef> {
        build_sorted_stream_array(quad_stream, layout, indexes, DEFAULT_CHUNK_SIZE).await
    }

    /// True streaming implementation: after the (inherently blocking) run-sort
    /// phase, merged chunks are built on demand as the file writer polls.
    async fn build_vortex_stream(
        quad_stream: Box<dyn Stream<Item = Result<Quad>> + Unpin + Send + 'static>,
        layout: LayoutStrategy,
        indexes: Indexes,
    ) -> Result<(DType, ChunkStream)> {
        build_sorted_stream_chunk_stream(quad_stream, layout, indexes, DEFAULT_CHUNK_SIZE).await
    }
}

/// Materialize the chunk stream into a single in-memory array.
///
/// The result is canonicalized and its sortedness stats re-stamped: the `s`
/// column and any global-order index columns are sorted across the whole
/// array, but assembling chunks loses the per-chunk stats that `match_pattern`
/// gates its binary searches on.
pub(crate) async fn build_sorted_stream_array(
    quad_stream: Box<dyn Stream<Item = Result<Quad>> + Unpin + Send + 'static>,
    layout: LayoutStrategy,
    indexes: Indexes,
    chunk_size: usize,
) -> Result<ArrayRef> {
    let start = Instant::now();

    let (_dtype, chunks) =
        build_sorted_stream_chunk_stream(quad_stream, layout, indexes.clone(), chunk_size).await?;
    let chunks: Vec<ArrayRef> = chunks.try_collect().await.map_err(VortexRdfError::Vortex)?;

    let result = canonicalize_sorted(assemble_chunks(chunks, layout, &indexes)?)?;
    log::debug!(
        "[SortedStreamBuilder] Materialized {} quads in {:?}",
        result.len(),
        start.elapsed()
    );
    Ok(result)
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
pub(crate) async fn build_sorted_stream_chunk_stream(
    mut quads_in: Box<dyn Stream<Item = Result<Quad>> + Unpin + Send + 'static>,
    layout: LayoutStrategy,
    indexes: Indexes,
    chunk_size: usize,
) -> Result<(DType, ChunkStream)> {
    let build_start = Instant::now();
    // ── Phase 1: Ingest and write sorted runs ──
    let ingest_start = Instant::now();
    let temp_dir = make_temp_dir("sorted_stream")?;
    let guard = TempRunsGuard { dir: temp_dir.clone() };

    // For the Dictionary layout, the global term dictionary is built
    // incrementally during this same ingestion pass.
    let mut dict_builder = (layout == LayoutStrategy::Dictionary).then(TermDictionaryBuilder::new);

    let mut buffer: Vec<RawQuad> = Vec::with_capacity(chunk_size.min(4096));
    let mut run_paths = Vec::new();
    let mut total_ingested = 0usize;

    while let Some(res) = quads_in.next().await {
        let raw = RawQuad::from_quad(&res?);
        if let Some(b) = dict_builder.as_mut() {
            b.insert_quad(&raw);
        }
        buffer.push(raw);
        total_ingested += 1;

        if buffer.len() >= chunk_size {
            buffer.sort_unstable();
            let run_path = temp_dir.join(format!("run_{}.bin", run_paths.len()));
            write_run(&run_path, &buffer)?;
            log::debug!("[SortedStreamBuilder] Wrote sorted run {} ({} quads)", run_paths.len(), buffer.len());
            run_paths.push(run_path);
            buffer.clear();
        }
    }

    if !buffer.is_empty() {
        buffer.sort_unstable();
        let run_path = temp_dir.join(format!("run_{}.bin", run_paths.len()));
        write_run(&run_path, &buffer)?;
        log::debug!("[SortedStreamBuilder] Wrote final sorted run {} ({} quads)", run_paths.len(), buffer.len());
        run_paths.push(run_path);
        drop(buffer);
    }
    log::debug!(
        "[SortedStreamBuilder] Ingested {} quads into {} runs in {:?} (dictionary collection={})",
        total_ingested,
        run_paths.len(),
        ingest_start.elapsed(),
        dict_builder.is_some()
    );

    // ── Phase 2: K-way merge setup ──
    let mut readers: Vec<RunReader<RawQuad>> = run_paths.iter()
        .map(|p| RunReader::new(p))
        .collect::<Result<_>>()?;

    let mut heap = BinaryHeap::new();
    for (i, r) in readers.iter_mut().enumerate() {
        if let Some(q) = r.next()? {
            heap.push(HeapItem { quad: q, reader_idx: i });
        }
    }

    // ── Phase 3: chunk emission ──
    let want_global_idx = indexes_need_global_sorted_emission(&indexes);

    if want_global_idx {
        // Two-pass pipeline for globally sorted index columns; spill only the
        // families the requested index types actually need.
        let unique = unique_indexes(&indexes);
        let want_ref = unique.contains(&IndexType::SecondaryByReference);
        let want_copy = unique.contains(&IndexType::SecondaryByCopy);
        if let Some(b) = dict_builder {
            let dict_start = Instant::now();
            let dict = Arc::new(b.finish()?);
            let id_map = Arc::new(dict.build_id_map());
            log::debug!(
                "[SortedStreamBuilder] Finalized dictionary of {} terms in {:?} ({:?} since build start)",
                dict.len(),
                dict_start.elapsed(),
                build_start.elapsed()
            );
            let ids = id_map.clone();
            let (merged_path, spilled) =
                merge_to_spill(readers, heap, &temp_dir, chunk_size, want_ref, want_copy, move |q| {
                    let encode = |term: &str| {
                        ids.get(term).copied().ok_or_else(|| {
                            VortexRdfError::Serialization(format!(
                                "Term missing from dictionary during encoding: {}",
                                term
                            ))
                        })
                    };
                    Ok([encode(&q.s)?, encode(&q.p)?, encode(&q.o)?, encode(&q.g)?])
                })?;
            return emit_presorted_dict_chunks(
                merged_path, spilled, dict, id_map, indexes, chunk_size, guard,
            );
        }
        let (merged_path, spilled) =
            merge_to_spill(readers, heap, &temp_dir, chunk_size, want_ref, want_copy, |q| {
                Ok([q.s.clone(), q.p.clone(), q.o.clone(), q.g.clone()])
            })?;
        return emit_presorted_chunks(merged_path, spilled, layout, indexes, chunk_size, guard);
    }

    // ── No secondary indexes: lazily emit merged chunks ──
    if let Some(b) = dict_builder {
        let dict_start = Instant::now();
        let dict = Arc::new(b.finish()?);
        let id_map = Arc::new(dict.build_id_map());
        log::debug!(
            "[SortedStreamBuilder] Finalized dictionary of {} terms in {:?} ({:?} since build start)",
            dict.len(),
            dict_start.elapsed(),
            build_start.elapsed()
        );
        return emit_dict_chunks(readers, heap, dict, id_map, indexes, chunk_size, guard);
    }

    // The first chunk is built eagerly so the schema dtype is known up front.
    let first_buf = next_sorted_chunk(&mut readers, &mut heap, chunk_size)?;
    let first = if first_buf.is_empty() {
        make_empty_struct(layout, &indexes)?
    } else {
        build_struct_array(&first_buf, layout, &indexes, first_buf.len(), 0, true, false)?
    };
    let dtype = first.dtype().clone();
    let next_row = first_buf.len() as u32;
    drop(first_buf);

    let rest = stream::unfold(
        (readers, heap, layout, indexes, next_row, guard),
        move |(mut readers, mut heap, layout, indexes, row, guard)| async move {
            let buf = match next_sorted_chunk(&mut readers, &mut heap, chunk_size) {
                Ok(b) => b,
                Err(e) => {
                    return Some((Err(into_vortex_error(e)), (readers, heap, layout, indexes, row, guard)));
                }
            };
            if buf.is_empty() {
                return None;
            }
            let n = buf.len();
            let chunk = build_struct_array(&buf, layout, &indexes, n, row, true, false)
                .map_err(into_vortex_error);
            Some((chunk, (readers, heap, layout, indexes, row + n as u32, guard)))
        },
    );

    let chunks: ChunkStream = stream::once(async move { Ok(first) }).chain(rest).boxed();
    Ok((dtype, chunks))
}

/// The two `SecondaryByReference` mergers of a build: (objects, predicates).
type RefMergers<V> = (PairMerger<V>, PairMerger<V>);
/// The two `SecondaryByCopy` mergers of a build: (POSG keys, OSPG keys).
type CopyMergers<V> = (PairMerger<CopyKey<V>>, PairMerger<CopyKey<V>>);
/// One chunk of the two reference index columns: (objects, predicates), each a
/// run of (value, row ID) entries.
type RefBatches<V> = (Vec<(V, u32)>, Vec<(V, u32)>);
/// One chunk of the two copy index columns: (POSG, OSPG), each a run of
/// (sort key, row ID) entries.
type CopyBatches<V> = (Vec<(CopyKey<V>, u32)>, Vec<(CopyKey<V>, u32)>);

/// The external-sort mergers for one build's secondary indexes, present only
/// for the index types the build requested. `V` is the term encoding: strings,
/// or u32 dictionary codes.
struct SpilledIndexes<V> {
    ref_pairs: Option<RefMergers<V>>,
    copy_keys: Option<CopyMergers<V>>,
}

/// One chunk's worth of every present merger's output, pulled in lockstep with
/// the merged-quad reader by [`next_index_batches`].
struct IndexBatches<V> {
    ref_pairs: Option<RefBatches<V>>,
    copy_keys: Option<CopyBatches<V>>,
}

/// Run the K-way quad merge to completion (pass A of the indexed pipeline):
/// merged quads are spilled sequentially to `merged.bin`, while each quad's
/// terms — extracted by `spog_of` as `[s, p, o, g]`, strings or u32 dictionary
/// codes — feed the external-sort spillers of every requested index family:
/// (value, row ID) pairs for the reference index, full [`CopyKey`]s for the
/// copy index. Returns the merged-quads path and the per-family mergers,
/// ready to stream entries in global sort order.
fn merge_to_spill<V>(
    mut readers: Vec<RunReader<RawQuad>>,
    mut heap: BinaryHeap<HeapItem>,
    temp_dir: &Path,
    pair_capacity: usize,
    want_ref: bool,
    want_copy: bool,
    mut spog_of: impl FnMut(&RawQuad) -> Result<[V; 4]>,
) -> Result<(PathBuf, SpilledIndexes<V>)>
where
    V: Clone + Ord + Archive + for<'a> RkyvSerialize<HighSerializer<AlignedVec, ArenaHandle<'a>, RkyvError>>,
    V::Archived: RkyvDeserialize<V, HighDeserializer<RkyvError>>,
    CopyKey<V>: Ord + Archive + for<'a> RkyvSerialize<HighSerializer<AlignedVec, ArenaHandle<'a>, RkyvError>>,
    <CopyKey<V> as Archive>::Archived: RkyvDeserialize<CopyKey<V>, HighDeserializer<RkyvError>>,
{
    let merged_path = temp_dir.join("merged.bin");
    let mut merged: RunWriter<RawQuad> = RunWriter::create(&merged_path)?;
    let mut o_spill = want_ref.then(|| PairRunSpiller::<V>::new(temp_dir, "idx_o", pair_capacity));
    let mut p_spill = want_ref.then(|| PairRunSpiller::<V>::new(temp_dir, "idx_p", pair_capacity));
    let mut posg_spill =
        want_copy.then(|| PairRunSpiller::<CopyKey<V>>::new(temp_dir, "idx_posg", pair_capacity));
    let mut ospg_spill =
        want_copy.then(|| PairRunSpiller::<CopyKey<V>>::new(temp_dir, "idx_ospg", pair_capacity));

    let mut rid: u32 = 0;
    while let Some(item) = heap.pop() {
        let r_idx = item.reader_idx;
        let quad = item.quad;
        let spog = spog_of(&quad)?;
        if let Some(spiller) = posg_spill.as_mut() {
            spiller.push(CopyKey::posg(&spog), rid)?;
        }
        if let Some(spiller) = ospg_spill.as_mut() {
            spiller.push(CopyKey::ospg(&spog), rid)?;
        }
        // Consumed last, so the reference pairs take ownership with no clone.
        let [_, p_val, o_val, _] = spog;
        if let Some(spiller) = o_spill.as_mut() {
            spiller.push(o_val, rid)?;
        }
        if let Some(spiller) = p_spill.as_mut() {
            spiller.push(p_val, rid)?;
        }
        merged.push(&quad)?;
        rid += 1;
        if let Some(next_q) = readers[r_idx].next()? {
            heap.push(HeapItem { quad: next_q, reader_idx: r_idx });
        }
    }
    merged.finish()?;
    log::debug!("[SortedStreamBuilder] Merged {} quads to spill; index pair runs written", rid);

    let ref_pairs = match (o_spill, p_spill) {
        (Some(o), Some(p)) => Some((o.into_merger()?, p.into_merger()?)),
        _ => None,
    };
    let copy_keys = match (posg_spill, ospg_spill) {
        (Some(posg), Some(ospg)) => Some((posg.into_merger()?, ospg.into_merger()?)),
        _ => None,
    };
    Ok((merged_path, SpilledIndexes { ref_pairs, copy_keys }))
}

/// Pull the next `n` entries off every present merger — one chunk's worth of
/// index columns, advancing in lockstep with the merged-quad reader.
fn next_index_batches<V>(spilled: &mut SpilledIndexes<V>, n: usize) -> Result<IndexBatches<V>>
where
    V: Ord + Archive + for<'a> RkyvSerialize<HighSerializer<AlignedVec, ArenaHandle<'a>, RkyvError>>,
    V::Archived: RkyvDeserialize<V, HighDeserializer<RkyvError>>,
    CopyKey<V>: Ord + Archive + for<'a> RkyvSerialize<HighSerializer<AlignedVec, ArenaHandle<'a>, RkyvError>>,
    <CopyKey<V> as Archive>::Archived: RkyvDeserialize<CopyKey<V>, HighDeserializer<RkyvError>>,
{
    Ok(IndexBatches {
        ref_pairs: match spilled.ref_pairs.as_mut() {
            Some((o, p)) => Some((o.next_batch(n)?, p.next_batch(n)?)),
            None => None,
        },
        copy_keys: match spilled.copy_keys.as_mut() {
            Some((posg, ospg)) => Some((posg.next_batch(n)?, ospg.next_batch(n)?)),
            None => None,
        },
    })
}

/// Pull up to `n` quads off the merged-quads spill.
fn read_merged_batch(reader: &mut RunReader<RawQuad>, n: usize) -> Result<Vec<RawQuad>> {
    let mut buf = Vec::with_capacity(n.min(4096));
    while buf.len() < n {
        match reader.next()? {
            Some(q) => buf.push(q),
            None => break,
        }
    }
    Ok(buf)
}

/// Build one chunk from merged quads plus the matching window of every
/// present index family's globally sorted entries.
fn build_presorted_chunk(
    quads: &[RawQuad],
    layout: LayoutStrategy,
    batches: &IndexBatches<String>,
) -> Result<ArrayRef> {
    let mut names = layout.field_names();
    let mut arrays = layout.build_columns(quads)?;
    stamp_is_sorted(&arrays[0]); // merge output is globally s-sorted
    if let Some((posg, ospg)) = &batches.copy_keys {
        secondary_by_copy::append_sorted_string_keys(&mut names, &mut arrays, posg, ospg, true);
    }
    if let Some((o_pairs, p_pairs)) = &batches.ref_pairs {
        append_sorted_string_pairs(&mut names, &mut arrays, o_pairs, p_pairs, true);
    }
    StructArray::try_new(names.into(), arrays, quads.len(), Validity::NonNullable)
        .map_err(VortexRdfError::Vortex)
        .map(|a| a.into_array())
}

/// Pass C of the indexed pipeline (string layouts): lazily re-read the merged
/// quads in chunk-size batches and zip each with the next window of the pair
/// merges. Quad `i` of the merge and pair-window `[i·C, (i+1)·C)` advance in
/// lockstep, so every chunk gets exactly its rows' worth of index entries.
fn emit_presorted_chunks(
    merged_path: PathBuf,
    mut spilled: SpilledIndexes<String>,
    layout: LayoutStrategy,
    indexes: Indexes,
    chunk_size: usize,
    guard: TempRunsGuard,
) -> Result<(DType, ChunkStream)> {
    let mut reader: RunReader<RawQuad> = RunReader::new(&merged_path)?;

    let buf = read_merged_batch(&mut reader, chunk_size)?;
    let first = if buf.is_empty() {
        make_empty_struct(layout, &indexes)?
    } else {
        let batches = next_index_batches(&mut spilled, buf.len())?;
        build_presorted_chunk(&buf, layout, &batches)?
    };
    let dtype = first.dtype().clone();

    let rest = stream::unfold(
        (reader, spilled, layout, guard),
        move |(mut reader, mut spilled, layout, guard)| async move {
            let chunk = (|| {
                let buf = read_merged_batch(&mut reader, chunk_size)?;
                if buf.is_empty() {
                    return Ok(None);
                }
                let batches = next_index_batches(&mut spilled, buf.len())?;
                build_presorted_chunk(&buf, layout, &batches).map(Some)
            })();
            match chunk {
                Ok(None) => None,
                Ok(Some(c)) => Some((Ok(c), (reader, spilled, layout, guard))),
                Err(e) => Some((Err(into_vortex_error(e)), (reader, spilled, layout, guard))),
            }
        },
    );

    let chunks: ChunkStream = stream::once(async move { Ok(first) }).chain(rest).boxed();
    Ok((dtype, chunks))
}

/// Dictionary-layout variant of [`emit_presorted_chunks`]: the entries hold
/// u32 codes, and the dictionary payload is carried only by the first chunk.
fn emit_presorted_dict_chunks(
    merged_path: PathBuf,
    mut spilled: SpilledIndexes<u32>,
    dict: Arc<TermDictionary>,
    id_map: Arc<crate::store::layouts::term_dictionary::TermIdMap>,
    indexes: Indexes,
    chunk_size: usize,
    guard: TempRunsGuard,
) -> Result<(DType, ChunkStream)> {
    let mut reader: RunReader<RawQuad> = RunReader::new(&merged_path)?;

    let buf = read_merged_batch(&mut reader, chunk_size)?;
    let first = if buf.is_empty() {
        dictionary::empty_struct(&indexes)?
    } else {
        let batches = next_index_batches(&mut spilled, buf.len())?;
        build_presorted_dict_chunk(&buf, &dict, &id_map, &batches, true)?
    };
    let dtype = first.dtype().clone();

    let rest = stream::unfold(
        (reader, spilled, dict, id_map, guard),
        move |(mut reader, mut spilled, dict, id_map, guard)| async move {
            let chunk = (|| {
                let buf = read_merged_batch(&mut reader, chunk_size)?;
                if buf.is_empty() {
                    return Ok(None);
                }
                let batches = next_index_batches(&mut spilled, buf.len())?;
                build_presorted_dict_chunk(&buf, &dict, &id_map, &batches, false).map(Some)
            })();
            match chunk {
                Ok(None) => None,
                Ok(Some(c)) => Some((Ok(c), (reader, spilled, dict, id_map, guard))),
                Err(e) => Some((Err(into_vortex_error(e)), (reader, spilled, dict, id_map, guard))),
            }
        },
    );

    let chunks: ChunkStream = stream::once(async move { Ok(first) }).chain(rest).boxed();
    Ok((dtype, chunks))
}

/// Adapt an [`IndexBatches`] window to `dictionary::build_chunk_presorted_indexes`.
fn build_presorted_dict_chunk(
    quads: &[RawQuad],
    dict: &TermDictionary,
    id_map: &crate::store::layouts::term_dictionary::TermIdMap,
    batches: &IndexBatches<u32>,
    carry_dict: bool,
) -> Result<ArrayRef> {
    dictionary::build_chunk_presorted_indexes(
        quads,
        dict,
        id_map,
        batches
            .ref_pairs
            .as_ref()
            .map(|(o, p)| (o.as_slice(), p.as_slice())),
        batches
            .copy_keys
            .as_ref()
            .map(|(posg, ospg)| (posg.as_slice(), ospg.as_slice())),
        true,
        carry_dict,
    )
}

/// Dictionary-layout emission over the K-way merge (no secondary indexes):
/// chunks of u32 codes encoded against the completed global dictionary, with
/// the dictionary payload carried only by the first chunk.
fn emit_dict_chunks(
    mut readers: Vec<RunReader<RawQuad>>,
    mut heap: BinaryHeap<HeapItem>,
    dict: Arc<TermDictionary>,
    id_map: Arc<crate::store::layouts::term_dictionary::TermIdMap>,
    indexes: Indexes,
    chunk_size: usize,
    guard: TempRunsGuard,
) -> Result<(DType, ChunkStream)> {
    let first_buf = next_sorted_chunk(&mut readers, &mut heap, chunk_size)?;
    let first = if first_buf.is_empty() {
        dictionary::empty_struct(&indexes)?
    } else {
        dictionary::build_chunk(&first_buf, &dict, &id_map, &indexes, 0, true, true, false)?
    };
    let dtype = first.dtype().clone();
    let next_row = first_buf.len() as u32;
    drop(first_buf);

    let rest = stream::unfold(
        (readers, heap, dict, id_map, indexes, next_row, guard),
        move |(mut readers, mut heap, dict, id_map, indexes, row, guard)| async move {
            let buf = match next_sorted_chunk(&mut readers, &mut heap, chunk_size) {
                Ok(b) => b,
                Err(e) => {
                    return Some((Err(into_vortex_error(e)), (readers, heap, dict, id_map, indexes, row, guard)));
                }
            };
            if buf.is_empty() {
                return None;
            }
            let n = buf.len() as u32;
            let chunk = dictionary::build_chunk(
                &buf, &dict, &id_map, &indexes, row, true, false, false,
            )
                .map_err(into_vortex_error);
            Some((chunk, (readers, heap, dict, id_map, indexes, row + n, guard)))
        },
    );

    let chunks: ChunkStream = stream::once(async move { Ok(first) }).chain(rest).boxed();
    Ok((dtype, chunks))
}

/// Pull up to `chunk_size` quads off the K-way merge in global sort order.
fn next_sorted_chunk(
    readers: &mut [RunReader<RawQuad>],
    heap: &mut BinaryHeap<HeapItem>,
    chunk_size: usize,
) -> Result<Vec<RawQuad>> {
    let mut buf = Vec::with_capacity(chunk_size.min(4096));
    while buf.len() < chunk_size {
        let Some(item) = heap.pop() else { break };
        let r_idx = item.reader_idx;
        buf.push(item.quad);
        if let Some(next_q) = readers[r_idx].next()? {
            heap.push(HeapItem { quad: next_q, reader_idx: r_idx });
        }
    }
    Ok(buf)
}
