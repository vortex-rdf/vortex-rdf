//! The [`SortedStreamBuilder`] strategy: spill sorted runs to temporary
//! files, then K-way merge them back into one global (s, p, o, g) order.
//!
//! It offers the same sortedness guarantee as
//! [`sorted_in_memory`](super::sorted_in_memory) — globally sorted `s` and
//! index value columns, both binary-searchable — without holding the dataset,
//! paying for it in temp-file I/O. Requested indexes are merged from their
//! own spilled `(value, row id)` runs and handed over as native components
//! rather than riding along as `_idx_*` row-space columns. The run file
//! format itself belongs to [`spill`](super::spill), the emission machinery
//! to [`builders`](super); what lives here is the merge.

use super::spill::{
    PairMerger, PairRunSpiller, Run, RunWriter, TempRunsGuard, make_temp_dir, write_run,
};
use super::{
    BuiltArray, BuiltStream, ChunkStream, DEFAULT_CHUNK_SIZE, VortexArrayBuilder, assemble_chunks,
    build_struct_array, canonicalize_sorted, into_vortex_error, make_empty_struct,
};
use crate::error::{Result, VortexRdfError};
use crate::io::container::NativeComponentWrite;
use crate::store::RawQuad;
use crate::store::array::stamp_is_sorted;
use crate::store::indexes::secondary_by_copy::{self, CopyKey};
use crate::store::indexes::{
    IndexComponent, IndexType, Indexes, indexes_need_global_sorted_emission, known_component,
    unique_indexes,
};
use crate::store::layouts::dictionary::{TermDictionary, TermDictionaryBuilder};
use crate::store::layouts::{LayoutStrategy, dictionary};

use futures::{Stream, StreamExt, TryStreamExt, stream};
use rkyv::api::high::{HighDeserializer, HighSerializer};
use rkyv::rancor::Error as RkyvError;
use rkyv::ser::allocator::ArenaHandle;
use rkyv::util::AlignedVec;
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use std::collections::BinaryHeap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use web_time::Instant;

use vortex_array::ArrayRef;
use vortex_array::arrays::StructArray;
use vortex_array::validity::Validity;
use vortex_array::{IntoArray, dtype::DType};

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
    fn eq(&self, other: &Self) -> bool {
        self.quad == other.quad
    }
}
impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.quad.cmp(&self.quad) // reversed for min-heap
    }
}
impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl VortexArrayBuilder for SortedStreamBuilder {
    async fn build_vortex_array(
        quad_stream: Box<dyn Stream<Item = Result<RawQuad>> + Unpin + Send + 'static>,
        layout: LayoutStrategy,
        indexes: Indexes,
    ) -> Result<BuiltArray> {
        build_sorted_stream_array(quad_stream, layout, indexes, DEFAULT_CHUNK_SIZE).await
    }

    /// True streaming implementation: after the (inherently blocking) run-sort
    /// phase, merged chunks are built on demand as the file writer polls.
    async fn build_vortex_stream(
        quad_stream: Box<dyn Stream<Item = Result<RawQuad>> + Unpin + Send + 'static>,
        layout: LayoutStrategy,
        indexes: Indexes,
    ) -> Result<BuiltStream> {
        build_sorted_stream_chunk_stream(quad_stream, layout, indexes, DEFAULT_CHUNK_SIZE, None)
            .await
    }
}

/// Materialize the chunk stream into a single in-memory array.
///
/// The quad result is canonicalized and its `s` sortedness stat re-stamped
/// (assembling chunks loses the per-chunk stats that `match_pattern` gates
/// its binary searches on). The streamed index children are materialized
/// directly as the store's in-memory components — never re-welded into
/// row-space columns; `from_built` adopts them as they are.
pub(crate) async fn build_sorted_stream_array(
    quad_stream: Box<dyn Stream<Item = Result<RawQuad>> + Unpin + Send + 'static>,
    layout: LayoutStrategy,
    indexes: Indexes,
    chunk_size: usize,
) -> Result<BuiltArray> {
    use vortex_array::VortexSessionExecute as _;

    let start = Instant::now();

    let built =
        build_sorted_stream_chunk_stream(quad_stream, layout, indexes.clone(), chunk_size, None)
            .await?;
    let chunks: Vec<ArrayRef> = built
        .chunks
        .try_collect()
        .await
        .map_err(VortexRdfError::Vortex)?;

    // Materialize each streamed component child as one canonical struct in
    // child schema. Sortedness is the descriptor's provenance — the mergers
    // emit each family in its global sort order — not an inspection.
    let mut components: Vec<IndexComponent> = Vec::new();
    let mut ctx = crate::session::VORTEX_SESSION.create_execution_ctx();
    for component in built.components {
        let Some(known) = known_component(&component.descriptor.implementation) else {
            continue;
        };
        let mut arrays: Vec<ArrayRef> = component
            .source
            .open()
            .map_err(VortexRdfError::Vortex)?
            .try_collect()
            .await
            .map_err(VortexRdfError::Vortex)?;
        let part = match arrays.len() {
            1 => arrays.pop().expect("length checked above"),
            _ => {
                let dtype = component.descriptor.dtype.clone();
                vortex_array::arrays::ChunkedArray::try_new(arrays, dtype)
                    .map_err(VortexRdfError::Vortex)?
                    .into_array()
            }
        };
        let array = part
            .execute::<StructArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?;
        components.push(IndexComponent::built(
            known.role.name,
            known.role.slug,
            array,
            component.descriptor.sorted,
        ));
    }
    let assembled = assemble_chunks(chunks, layout, &indexes)?;
    // Correct by construction for this builder: every emission is a window
    // of the global merge, so the s column is globally sorted — the stamp
    // the store's adoption reads back.
    let result = canonicalize_sorted(assembled)?;
    log::debug!(
        "[SortedStreamBuilder] Materialized {} quads in {:?}",
        result.len(),
        start.elapsed()
    );
    Ok(BuiltArray {
        array: result,
        components,
        dict: built.dict,
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
/// takes [`make_temp_dir`]'s default resolution.
pub(crate) async fn build_sorted_stream_chunk_stream(
    mut quads_in: Box<dyn Stream<Item = Result<RawQuad>> + Unpin + Send + 'static>,
    layout: LayoutStrategy,
    indexes: Indexes,
    chunk_size: usize,
    spill_dir: Option<&Path>,
) -> Result<BuiltStream> {
    let build_start = Instant::now();
    // ── Phase 1: Ingest and write sorted runs ──
    let ingest_start = Instant::now();
    let temp_dir = make_temp_dir("sorted_stream", spill_dir)?;
    let guard = Arc::new(TempRunsGuard {
        dir: temp_dir.clone(),
    });

    // For the Dictionary layout, the global term dictionary is built
    // incrementally during this same ingestion pass.
    let mut dict_builder = (layout == LayoutStrategy::Dictionary).then(TermDictionaryBuilder::new);

    let mut buffer: Vec<RawQuad> = Vec::with_capacity(chunk_size.min(4096));
    let mut run_paths = Vec::new();
    let mut total_ingested = 0usize;

    while let Some(res) = quads_in.next().await {
        let raw = res?;
        if let Some(b) = dict_builder.as_mut() {
            b.insert_quad(&raw);
        }
        // Spill only to make room for a quad that would not fit, never merely on
        // reaching the chunk size — a dataset of exactly `chunk_size` quads then
        // stays one in-memory run (see [`Run`]). Peak buffered quads is
        // unchanged, and so is the run size.
        if buffer.len() == chunk_size {
            buffer.sort_unstable();
            let run_path = temp_dir.join(format!("run_{}.bin", run_paths.len()));
            write_run(&run_path, &buffer)?;
            log::debug!(
                "[SortedStreamBuilder] Wrote sorted run {} ({} quads)",
                run_paths.len(),
                buffer.len()
            );
            run_paths.push(run_path);
            buffer.clear();
        }
        buffer.push(raw);
        total_ingested += 1;
    }

    // Phase 2 input: the spilled runs, plus whatever is still buffered. A lone
    // buffer never touches the filesystem; alongside spilled runs it has to join
    // them on disk so the K-way merge sees uniform runs.
    let mut runs: Vec<Run<RawQuad>> = if run_paths.is_empty() {
        buffer.sort_unstable();
        log::debug!(
            "[SortedStreamBuilder] Kept the single sorted run of {} quads in memory",
            buffer.len()
        );
        vec![Run::memory(buffer)]
    } else {
        if !buffer.is_empty() {
            buffer.sort_unstable();
            let run_path = temp_dir.join(format!("run_{}.bin", run_paths.len()));
            write_run(&run_path, &buffer)?;
            log::debug!(
                "[SortedStreamBuilder] Wrote final sorted run {} ({} quads)",
                run_paths.len(),
                buffer.len()
            );
            run_paths.push(run_path);
        }
        drop(buffer);
        run_paths
            .iter()
            .map(|p| Run::file(p))
            .collect::<Result<_>>()?
    };
    log::debug!(
        "[SortedStreamBuilder] Ingested {} quads into {} runs in {:?} (dictionary collection={})",
        total_ingested,
        runs.len(),
        ingest_start.elapsed(),
        dict_builder.is_some()
    );

    // ── Phase 2: K-way merge setup ──
    let mut heap = BinaryHeap::new();
    for (i, r) in runs.iter_mut().enumerate() {
        if let Some(q) = r.next()? {
            heap.push(HeapItem {
                quad: q,
                reader_idx: i,
            });
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
            let (dict, id_map) = b.finish()?;
            let (dict, id_map) = (Arc::new(dict), Arc::new(id_map));
            log::debug!(
                "[SortedStreamBuilder] Finalized dictionary of {} terms in {:?} ({:?} since build start)",
                dict.len(),
                dict_start.elapsed(),
                build_start.elapsed()
            );
            let ids = id_map.clone();
            let (merged, spilled) = merge_to_spill(
                runs,
                heap,
                &temp_dir,
                chunk_size,
                want_ref,
                want_copy,
                move |term| {
                    ids.get(term).copied().ok_or_else(|| {
                        VortexRdfError::Serialization(format!(
                            "Term missing from dictionary during encoding: {}",
                            term
                        ))
                    })
                },
            )?;
            return emit_presorted_dict_chunks(merged, spilled, dict, id_map, chunk_size, guard);
        }
        let (merged, spilled) = merge_to_spill(
            runs,
            heap,
            &temp_dir,
            chunk_size,
            want_ref,
            want_copy,
            |term| Ok(term.to_string()),
        )?;
        let (dtype, chunks, components) =
            emit_presorted_chunks(merged, spilled, layout, chunk_size, guard)?;
        return Ok(BuiltStream {
            dtype,
            chunks,
            components,
            // Index children stream from the global merge; the row-space
            // chunks are primary-only, so the serializer's tee is a no-op.
            components_sorted: true,
            quads_sorted: true,
            dict: None,
        });
    }

    // ── No secondary indexes: lazily emit merged chunks ──
    if let Some(b) = dict_builder {
        let dict_start = Instant::now();
        let (dict, id_map) = b.finish()?;
        let (dict, id_map) = (Arc::new(dict), Arc::new(id_map));
        log::debug!(
            "[SortedStreamBuilder] Finalized dictionary of {} terms in {:?} ({:?} since build start)",
            dict.len(),
            dict_start.elapsed(),
            build_start.elapsed()
        );
        return emit_dict_chunks(runs, heap, dict, id_map, indexes, chunk_size, guard);
    }

    // The first chunk is built eagerly so the schema dtype is known up front.
    let first_buf = next_sorted_chunk(&mut runs, &mut heap, chunk_size)?;
    let first = if first_buf.is_empty() {
        make_empty_struct(layout, &indexes)?
    } else {
        build_struct_array(&first_buf, layout, &indexes, 0, true, false)?
    };
    let dtype = first.dtype().clone();
    let next_row = first_buf.len() as u32;
    drop(first_buf);

    let rest = stream::unfold(
        (runs, heap, layout, indexes, next_row, guard),
        move |(mut runs, mut heap, layout, indexes, row, guard)| async move {
            let buf = match next_sorted_chunk(&mut runs, &mut heap, chunk_size) {
                Ok(b) => b,
                Err(e) => {
                    return Some((
                        Err(into_vortex_error(e)),
                        (runs, heap, layout, indexes, row, guard),
                    ));
                }
            };
            if buf.is_empty() {
                return None;
            }
            let n = buf.len();
            let chunk = build_struct_array(&buf, layout, &indexes, row, true, false)
                .map_err(into_vortex_error);
            Some((chunk, (runs, heap, layout, indexes, row + n as u32, guard)))
        },
    );

    let chunks: ChunkStream = stream::once(async move { Ok(first) }).chain(rest).boxed();
    Ok(BuiltStream {
        dtype,
        chunks,
        components: Vec::new(),
        // Any welded index columns are per-chunk local sorts.
        components_sorted: false,
        quads_sorted: true,
        dict: None,
    })
}

/// The two `SecondaryByReference` mergers of a build: (objects, predicates).
type RefMergers<V> = (PairMerger<V>, PairMerger<V>);
/// The two `SecondaryByCopy` mergers of a build: (POSG keys, OSPG keys).
type CopyMergers<V> = (PairMerger<CopyKey<V>>, PairMerger<CopyKey<V>>);

/// The external-sort mergers for one build's secondary indexes, present only
/// for the index types the build requested. `V` is the term encoding: strings,
/// or u32 dictionary codes.
struct SpilledIndexes<V> {
    ref_pairs: Option<RefMergers<V>>,
    copy_keys: Option<CopyMergers<V>>,
}

/// Run the K-way quad merge to completion (pass A of the indexed pipeline):
/// merged quads are collected in merge order, while each quad's terms —
/// encoded one at a time by `term_of` into `V`, strings or u32 dictionary
/// codes — feed the external-sort spillers of every requested index family:
/// (value, row ID) pairs for the reference index, full [`CopyKey`]s for the
/// copy index. Only the terms the requested families consume are encoded (a
/// String encoding is an allocation, and this path exists for datasets too
/// large for memory): a reference-only build never touches `s`/`g`, and the
/// OSPG key takes the copy build's `[s, p, o, g]` by value instead of
/// cloning it. Returns the merged quads and the per-family mergers, ready to
/// stream entries in global sort order.
///
/// A single input run means the whole dataset already fit in memory once, so
/// the merged output is kept there too rather than round-tripping through
/// `merged.bin`; with several runs the merge is unbounded by construction and
/// spills as before.
fn merge_to_spill<V>(
    mut runs: Vec<Run<RawQuad>>,
    mut heap: BinaryHeap<HeapItem>,
    temp_dir: &Path,
    pair_capacity: usize,
    want_ref: bool,
    want_copy: bool,
    mut term_of: impl FnMut(&str) -> Result<V>,
) -> Result<(Run<RawQuad>, SpilledIndexes<V>)>
where
    V: Clone
        + Ord
        + Archive
        + for<'a> RkyvSerialize<HighSerializer<AlignedVec, ArenaHandle<'a>, RkyvError>>,
    V::Archived: RkyvDeserialize<V, HighDeserializer<RkyvError>>,
    CopyKey<V>: Ord
        + Archive
        + for<'a> RkyvSerialize<HighSerializer<AlignedVec, ArenaHandle<'a>, RkyvError>>,
    <CopyKey<V> as Archive>::Archived: RkyvDeserialize<CopyKey<V>, HighDeserializer<RkyvError>>,
{
    // One run in, one run out: keep the merged quads in memory (see the doc
    // comment); otherwise stream them into a spill file.
    let mut merged = if runs.len() <= 1 {
        MergedSink::Memory(Vec::new())
    } else {
        let path = temp_dir.join("merged.bin");
        MergedSink::File {
            writer: RunWriter::create(&path)?,
            path,
        }
    };
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
        if want_copy {
            let spog = [
                term_of(&quad.s)?,
                term_of(&quad.p)?,
                term_of(&quad.o)?,
                term_of(&quad.g)?,
            ];
            if let Some(spiller) = posg_spill.as_mut() {
                spiller.push(CopyKey::posg(&spog), rid)?;
            }
            // The reference pairs clone the two terms they share with the
            // copy keys, so the OSPG constructor — consumed last — can take
            // the whole tuple by value.
            if let Some(spiller) = o_spill.as_mut() {
                spiller.push(spog[2].clone(), rid)?;
            }
            if let Some(spiller) = p_spill.as_mut() {
                spiller.push(spog[1].clone(), rid)?;
            }
            if let Some(spiller) = ospg_spill.as_mut() {
                spiller.push(CopyKey::ospg(spog), rid)?;
            }
        } else if want_ref {
            if let Some(spiller) = o_spill.as_mut() {
                spiller.push(term_of(&quad.o)?, rid)?;
            }
            if let Some(spiller) = p_spill.as_mut() {
                spiller.push(term_of(&quad.p)?, rid)?;
            }
        }
        merged.push(quad)?;
        rid += 1;
        if let Some(next_q) = runs[r_idx].next()? {
            heap.push(HeapItem {
                quad: next_q,
                reader_idx: r_idx,
            });
        }
    }
    let merged = merged.finish()?;
    log::debug!(
        "[SortedStreamBuilder] Merged {} quads; index pair runs written",
        rid
    );

    let ref_pairs = match (o_spill, p_spill) {
        (Some(o), Some(p)) => Some((o.into_merger()?, p.into_merger()?)),
        _ => None,
    };
    let copy_keys = match (posg_spill, ospg_spill) {
        (Some(posg), Some(ospg)) => Some((posg.into_merger()?, ospg.into_merger()?)),
        _ => None,
    };
    Ok((
        merged,
        SpilledIndexes {
            ref_pairs,
            copy_keys,
        },
    ))
}

/// Where [`merge_to_spill`] puts the merged quads: straight into memory when the
/// merge had a single input run, otherwise into a spill file.
enum MergedSink {
    Memory(Vec<RawQuad>),
    File {
        writer: RunWriter<RawQuad>,
        path: PathBuf,
    },
}

impl MergedSink {
    fn push(&mut self, quad: RawQuad) -> Result<()> {
        match self {
            MergedSink::Memory(quads) => {
                quads.push(quad);
                Ok(())
            }
            MergedSink::File { writer, .. } => writer.push(&quad),
        }
    }

    /// Close the sink and hand back the merged quads as a readable run.
    fn finish(self) -> Result<Run<RawQuad>> {
        match self {
            MergedSink::Memory(quads) => Ok(Run::memory(quads)),
            MergedSink::File { writer, path } => {
                writer.finish()?;
                Run::file(&path)
            }
        }
    }
}

/// Pull up to `n` quads off the merged-quads run.
fn read_merged_batch(merged: &mut Run<RawQuad>, n: usize) -> Result<Vec<RawQuad>> {
    let mut buf = Vec::with_capacity(n.min(4096));
    while buf.len() < n {
        match merged.next()? {
            Some(q) => buf.push(q),
            None => break,
        }
    }
    Ok(buf)
}

/// Build one primary-columns chunk from merged quads (globally s-sorted).
fn build_presorted_chunk(quads: &[RawQuad], layout: LayoutStrategy) -> Result<ArrayRef> {
    let names = layout.field_names();
    let arrays = layout.build_columns(quads)?;
    stamp_is_sorted(&arrays[0]); // merge output is globally s-sorted
    StructArray::try_new(names.into(), arrays, quads.len(), Validity::NonNullable)
        .map_err(VortexRdfError::Vortex)
        .map(|a| a.into_array())
}

/// A window of one copy family's merged entries, as one child chunk.
type CopyChunkFn<V> = fn(secondary_by_copy::Family, &[(CopyKey<V>, u32)]) -> Result<ArrayRef>;
/// A window of one reference component's merged pairs, as one child chunk.
type RefChunkFn<V> = fn(&[(V, u32)]) -> Result<ArrayRef>;

/// Turn a build's spill-run mergers into native component writes: each family
/// streams its child's chunks straight off its merger — no lockstep zip with
/// the quad stream, no materialization. The temp-run guard is shared with the
/// quad stream so the run files outlive every reader.
fn merger_components<V>(
    spilled: SpilledIndexes<V>,
    chunk_size: usize,
    guard: &Arc<TempRunsGuard>,
    copy_dtype: DType,
    ref_dtype: DType,
    copy_chunk: CopyChunkFn<V>,
    ref_chunk: RefChunkFn<V>,
) -> Result<Vec<NativeComponentWrite>>
where
    V: Send
        + 'static
        + Ord
        + Archive
        + for<'a> RkyvSerialize<HighSerializer<AlignedVec, ArenaHandle<'a>, RkyvError>>,
    V::Archived: RkyvDeserialize<V, HighDeserializer<RkyvError>>,
    CopyKey<V>: Ord
        + Archive
        + for<'a> RkyvSerialize<HighSerializer<AlignedVec, ArenaHandle<'a>, RkyvError>>,
    <CopyKey<V> as Archive>::Archived: RkyvDeserialize<CopyKey<V>, HighDeserializer<RkyvError>>,
{
    use crate::io::container::sources::PullComponentSource;
    use crate::io::container::{
        StoreComponentDescriptor, StoreComponentRole, default_child_strategy,
    };
    use crate::store::indexes::secondary_by_copy::Family;
    use crate::store::indexes::secondary_by_reference::{REF_O_COMPONENT, REF_P_COMPONENT};

    let mut components = Vec::new();
    let mut push = |name: &str,
                    slug: &str,
                    dtype: DType,
                    mut pull: Box<dyn FnMut(usize) -> Result<Option<ArrayRef>> + Send>|
     -> Result<()> {
        let guard = Arc::clone(guard);
        let mut emitted = false;
        let pull_fn: crate::io::container::sources::PullFn = Box::new(move |n| {
            let _hold_runs = &guard;
            match pull(n) {
                Ok(Some(chunk)) => {
                    emitted = true;
                    Ok(Some(chunk))
                }
                // The child strategy needs at least one (possibly empty)
                // chunk to write a schema-complete component.
                Ok(None) if !emitted => {
                    emitted = true;
                    pull(0).map_err(into_vortex_error)
                }
                Ok(None) => Ok(None),
                Err(e) => Err(into_vortex_error(e)),
            }
        });
        components.push(
            NativeComponentWrite::new(
                StoreComponentDescriptor {
                    name: name.into(),
                    role: StoreComponentRole::Index,
                    implementation: slug.into(),
                    version: 1,
                    required: false,
                    // The merger emits each family in its global sort order.
                    sorted: true,
                    dtype: dtype.clone(),
                },
                Arc::new(PullComponentSource::new(dtype, chunk_size, pull_fn)),
                default_child_strategy(),
            )
            .map_err(VortexRdfError::Vortex)?,
        );
        Ok(())
    };

    if let Some((posg, ospg)) = spilled.copy_keys {
        for (family, merger) in [(Family::Posg, posg), (Family::Ospg, ospg)] {
            let mut merger = merger;
            push(
                family.component_name(),
                family.component_slug(),
                copy_dtype.clone(),
                Box::new(move |n| {
                    let batch = merger.next_batch(n)?;
                    if batch.is_empty() && n > 0 {
                        return Ok(None);
                    }
                    copy_chunk(family, &batch).map(Some)
                }),
            )?;
        }
    }
    if let Some((o_pairs, p_pairs)) = spilled.ref_pairs {
        use crate::store::indexes::secondary_by_reference::{O_IMPLEMENTATION, P_IMPLEMENTATION};
        for (name, slug, merger) in [
            (REF_O_COMPONENT, O_IMPLEMENTATION, o_pairs),
            (REF_P_COMPONENT, P_IMPLEMENTATION, p_pairs),
        ] {
            let mut merger = merger;
            push(
                name,
                slug,
                ref_dtype.clone(),
                Box::new(move |n| {
                    let batch = merger.next_batch(n)?;
                    if batch.is_empty() && n > 0 {
                        return Ok(None);
                    }
                    ref_chunk(&batch).map(Some)
                }),
            )?;
        }
    }
    Ok(components)
}

/// Pass C of the indexed pipeline (string layouts): lazily re-read the merged
/// quads in chunk-size batches as primary-only chunks, while each index
/// family's merger streams its own child component beside them.
fn emit_presorted_chunks(
    mut merged: Run<RawQuad>,
    spilled: SpilledIndexes<String>,
    layout: LayoutStrategy,
    chunk_size: usize,
    guard: Arc<TempRunsGuard>,
) -> Result<(DType, ChunkStream, Vec<NativeComponentWrite>)> {
    let components = merger_components(
        spilled,
        chunk_size,
        &guard,
        secondary_by_copy::copy_child_dtype(false),
        crate::store::indexes::secondary_by_reference::ref_child_dtype(false),
        secondary_by_copy::copy_child_chunk_strings,
        crate::store::indexes::secondary_by_reference::ref_child_chunk_strings,
    )?;
    let buf = read_merged_batch(&mut merged, chunk_size)?;
    let first = if buf.is_empty() {
        make_empty_struct(layout, &Vec::new())?
    } else {
        build_presorted_chunk(&buf, layout)?
    };
    let dtype = first.dtype().clone();

    let rest = stream::unfold(
        (merged, layout, guard),
        move |(mut merged, layout, guard)| async move {
            let chunk = (|| {
                let buf = read_merged_batch(&mut merged, chunk_size)?;
                if buf.is_empty() {
                    return Ok(None);
                }
                build_presorted_chunk(&buf, layout).map(Some)
            })();
            match chunk {
                Ok(None) => None,
                Ok(Some(c)) => Some((Ok(c), (merged, layout, guard))),
                Err(e) => Some((Err(into_vortex_error(e)), (merged, layout, guard))),
            }
        },
    );

    let chunks: ChunkStream = stream::once(async move { Ok(first) }).chain(rest).boxed();
    Ok((dtype, chunks, components))
}

/// Dictionary-layout variant of [`emit_presorted_chunks`]: the entries hold
/// u32 codes; the dictionary rides beside the stream for the serializer.
fn emit_presorted_dict_chunks(
    mut merged: Run<RawQuad>,
    spilled: SpilledIndexes<u32>,
    dict: Arc<TermDictionary>,
    id_map: Arc<crate::store::layouts::dictionary::TermIdMap>,
    chunk_size: usize,
    guard: Arc<TempRunsGuard>,
) -> Result<BuiltStream> {
    let components = merger_components(
        spilled,
        chunk_size,
        &guard,
        secondary_by_copy::copy_child_dtype(true),
        crate::store::indexes::secondary_by_reference::ref_child_dtype(true),
        secondary_by_copy::copy_child_chunk_codes,
        crate::store::indexes::secondary_by_reference::ref_child_chunk_codes,
    )?;
    let buf = read_merged_batch(&mut merged, chunk_size)?;
    let first = if buf.is_empty() {
        dictionary::empty_struct(&Vec::new())?
    } else {
        build_presorted_dict_chunk(&buf, &dict, &id_map)?
    };
    let dtype = first.dtype().clone();

    let stream_dict = Arc::clone(&dict);
    let rest = stream::unfold(
        (merged, stream_dict, id_map, guard),
        move |(mut merged, dict, id_map, guard)| async move {
            let chunk = (|| {
                let buf = read_merged_batch(&mut merged, chunk_size)?;
                if buf.is_empty() {
                    return Ok(None);
                }
                build_presorted_dict_chunk(&buf, &dict, &id_map).map(Some)
            })();
            match chunk {
                Ok(None) => None,
                Ok(Some(c)) => Some((Ok(c), (merged, dict, id_map, guard))),
                Err(e) => Some((Err(into_vortex_error(e)), (merged, dict, id_map, guard))),
            }
        },
    );

    let chunks: ChunkStream = stream::once(async move { Ok(first) }).chain(rest).boxed();
    Ok(BuiltStream {
        dtype,
        chunks,
        components,
        components_sorted: true,
        quads_sorted: true,
        dict: Some(dict),
    })
}

/// Build one primary-code-columns chunk against the completed dictionary.
fn build_presorted_dict_chunk(
    quads: &[RawQuad],
    dict: &TermDictionary,
    id_map: &crate::store::layouts::dictionary::TermIdMap,
) -> Result<ArrayRef> {
    dictionary::build_chunk_presorted_indexes(quads, dict, id_map, None, None, true)
}

/// Dictionary-layout emission over the K-way merge (no secondary indexes):
/// chunks of u32 codes encoded against the completed global dictionary,
/// which rides beside the stream for the serializer to place.
fn emit_dict_chunks(
    mut runs: Vec<Run<RawQuad>>,
    mut heap: BinaryHeap<HeapItem>,
    dict: Arc<TermDictionary>,
    id_map: Arc<crate::store::layouts::dictionary::TermIdMap>,
    indexes: Indexes,
    chunk_size: usize,
    guard: Arc<TempRunsGuard>,
) -> Result<BuiltStream> {
    let first_buf = next_sorted_chunk(&mut runs, &mut heap, chunk_size)?;
    let first = if first_buf.is_empty() {
        dictionary::empty_struct(&indexes)?
    } else {
        dictionary::build_chunk(&first_buf, &dict, &id_map, &indexes, 0, true, false)?
    };
    let dtype = first.dtype().clone();
    let next_row = first_buf.len() as u32;
    drop(first_buf);

    let stream_dict = Arc::clone(&dict);
    let rest = stream::unfold(
        (runs, heap, stream_dict, id_map, indexes, next_row, guard),
        move |(mut runs, mut heap, dict, id_map, indexes, row, guard)| async move {
            let buf = match next_sorted_chunk(&mut runs, &mut heap, chunk_size) {
                Ok(b) => b,
                Err(e) => {
                    return Some((
                        Err(into_vortex_error(e)),
                        (runs, heap, dict, id_map, indexes, row, guard),
                    ));
                }
            };
            if buf.is_empty() {
                return None;
            }
            let n = buf.len() as u32;
            let chunk = dictionary::build_chunk(&buf, &dict, &id_map, &indexes, row, true, false)
                .map_err(into_vortex_error);
            Some((chunk, (runs, heap, dict, id_map, indexes, row + n, guard)))
        },
    );

    let chunks: ChunkStream = stream::once(async move { Ok(first) }).chain(rest).boxed();
    Ok(BuiltStream {
        dtype,
        chunks,
        components: Vec::new(),
        components_sorted: false,
        quads_sorted: true,
        dict: Some(dict),
    })
}

/// Pull up to `chunk_size` quads off the K-way merge in global sort order.
fn next_sorted_chunk(
    runs: &mut [Run<RawQuad>],
    heap: &mut BinaryHeap<HeapItem>,
    chunk_size: usize,
) -> Result<Vec<RawQuad>> {
    let mut buf = Vec::with_capacity(chunk_size.min(4096));
    while buf.len() < chunk_size {
        let Some(item) = heap.pop() else { break };
        let r_idx = item.reader_idx;
        buf.push(item.quad);
        if let Some(next_q) = runs[r_idx].next()? {
            heap.push(HeapItem {
                quad: next_q,
                reader_idx: r_idx,
            });
        }
    }
    Ok(buf)
}
