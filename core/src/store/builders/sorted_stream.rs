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
//!
//! Quads are unique and terms are canonical in what this builder emits. The
//! quad runs are spilled *distinct*: a quad that arrives more than once, in
//! one run or in several, is merged out once, and the dictionary collects
//! the terms of the parsed (so canonical) quads as a set. Row ids are
//! assigned by that merge, after the repeats are gone, so every index
//! family's `(value, row id)` records describe the deduplicated rows — and
//! are themselves unique, since each carries a row id of its own. The
//! families are spilled and merged as they are, never deduplicated.

use super::spill::{Run, RunMerger, RunSpiller, RunWriter, Spillable, TempRunsGuard};
use super::{
    BuiltArray, BuiltStream, ChunkStream, DEFAULT_CHUNK_ROWS, VortexArrayBuilder, assemble_chunks,
    build_struct_array, into_vortex_error, make_empty_struct,
};
use crate::error::{Result, VortexRdfError};
use crate::io::container::NativeComponentWrite;
use crate::store::RawQuad;
use crate::store::array::{chunked_or_single, with_subject_stamp};
use crate::store::indexes::secondary_by_copy::{self, out_of_core::CopyKey};
use crate::store::indexes::{IndexComponent, IndexType, Indexes, known_component, unique_indexes};
use crate::store::layouts::dictionary::{TermCodeMap, TermDictionary, TermDictionaryBuilder};
use crate::store::layouts::{LayoutStrategy, dictionary};
use crate::store::schema::TermCode;

use crate::debug;
use futures::{Stream, StreamExt, TryStreamExt, stream};
use std::path::{Path, PathBuf};
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

    let built = build_chunk_stream(quad_stream, layout, indexes.clone(), chunk_size, None).await?;
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
            known.identity.name,
            known.identity.slug,
            array,
            component.descriptor.sorted,
        ));
    }
    let assembled = assemble_chunks(chunks)?;
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
        dict: built.dict,
    })
}

/// External merge sort producing a lazily-evaluated stream of sorted chunks,
/// each distinct quad once.
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
    let build_start = debug::timer();
    // ── Phase 1: Ingest and write sorted runs ──
    let ingest_start = debug::timer();
    let guard = Arc::new(TempRunsGuard::create("sorted_stream", spill_dir)?);

    // For the Dictionary layout, the global term dictionary is built
    // incrementally during this same ingestion pass.
    let mut dict_builder = (layout == LayoutStrategy::Dictionary).then(TermDictionaryBuilder::new);

    // Distinct: a repeated quad must be one row, and one row id.
    let mut spiller = RunSpiller::<RawQuad>::distinct(guard.path(), "quads", chunk_size);
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
                || make_empty_struct(layout),
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

/// The two `SecondaryByReference` mergers of a build: (objects, predicates).
type RefMergers<V> = (RunMerger<(V, u32)>, RunMerger<(V, u32)>);
/// The two `SecondaryByCopy` mergers of a build: (POSG keys, OSPG keys).
type CopyMergers<V> = (RunMerger<(CopyKey<V>, u32)>, RunMerger<(CopyKey<V>, u32)>);

/// The external-sort mergers for one build's secondary indexes, present only
/// for the index types the build requested. `V` is the term encoding: strings,
/// or dictionary codes ([`TermCode`]).
struct IndexMergers<V> {
    ref_pairs: Option<RefMergers<V>>,
    copy_keys: Option<CopyMergers<V>>,
}

/// First pass of the indexed pipeline: run the K-way quad merge to completion,
/// collecting merged quads (each distinct quad once, so `rid` counts the
/// deduplicated rows) — in memory when there is a single input run (the
/// dataset already fit once), else spilled to `merged.bin` — while feeding
/// each requested index family's spiller with that quad's terms encoded by
/// `term_of`: `(value, row id)` pairs for the reference index, full
/// [`CopyKey`]s for the copy index. Only the terms the requested families
/// consume are encoded. Returns the merged quads and the per-family mergers,
/// ready to stream entries in global sort order.
fn merge_quads_feeding_indexes<V>(
    mut merger: RunMerger<RawQuad>,
    temp_dir: &Path,
    pair_capacity: usize,
    want_ref: bool,
    want_copy: bool,
    mut term_of: impl FnMut(&str) -> Result<V>,
) -> Result<(Run<RawQuad>, IndexMergers<V>)>
where
    V: Clone,
    (V, u32): Ord + Spillable,
    (CopyKey<V>, u32): Ord + Spillable,
{
    let mut merged = if merger.run_count() <= 1 {
        MergedSink::Memory(Vec::new())
    } else {
        let path = temp_dir.join("merged.bin");
        MergedSink::File {
            writer: RunWriter::create(&path)?,
            path,
        }
    };
    let mut o_spill =
        want_ref.then(|| RunSpiller::<(V, u32)>::new(temp_dir, "idx_o", pair_capacity));
    let mut p_spill =
        want_ref.then(|| RunSpiller::<(V, u32)>::new(temp_dir, "idx_p", pair_capacity));
    let mut posg_spill = want_copy
        .then(|| RunSpiller::<(CopyKey<V>, u32)>::new(temp_dir, "idx_posg", pair_capacity));
    let mut ospg_spill = want_copy
        .then(|| RunSpiller::<(CopyKey<V>, u32)>::new(temp_dir, "idx_ospg", pair_capacity));

    let mut rid: u32 = 0;
    while let Some(quad) = merger.next()? {
        if want_copy {
            let spog = [
                term_of(&quad.s)?,
                term_of(&quad.p)?,
                term_of(&quad.o)?,
                term_of(&quad.g)?,
            ];
            if let Some(spiller) = posg_spill.as_mut() {
                spiller.push((CopyKey::posg(&spog), rid))?;
            }
            // The reference pairs clone the two terms they share with the
            // copy keys, so the OSPG constructor — consumed last — can take
            // the whole tuple by value.
            if let Some(spiller) = o_spill.as_mut() {
                spiller.push((spog[2].clone(), rid))?;
            }
            if let Some(spiller) = p_spill.as_mut() {
                spiller.push((spog[1].clone(), rid))?;
            }
            if let Some(spiller) = ospg_spill.as_mut() {
                spiller.push((CopyKey::ospg(spog), rid))?;
            }
        } else if want_ref {
            if let Some(spiller) = o_spill.as_mut() {
                spiller.push((term_of(&quad.o)?, rid))?;
            }
            if let Some(spiller) = p_spill.as_mut() {
                spiller.push((term_of(&quad.p)?, rid))?;
            }
        }
        merged.push(quad)?;
        rid += 1;
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
        IndexMergers {
            ref_pairs,
            copy_keys,
        },
    ))
}

/// Where [`merge_quads_feeding_indexes`] puts the merged quads: straight into
/// memory when the merge had a single input run, otherwise into a spill file.
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

/// A window of one reference component's merged pairs, as one child chunk.
type RefChunkFn<V> = fn(&[(V, u32)]) -> Result<ArrayRef>;

/// Turn a build's spill-run mergers into native component writes: each family
/// streams its child's chunks straight off its merger — no lockstep zip with
/// the quad stream, no materialization. The temp-run guard is shared with the
/// quad stream so the run files outlive every reader. `encoded` says whether
/// the entries hold dictionary codes (else term strings), which picks the
/// child dtypes; `ref_chunk` builds a reference child chunk for that encoding.
fn merger_components<V>(
    mergers: IndexMergers<V>,
    chunk_size: usize,
    guard: &Arc<TempRunsGuard>,
    encoded: bool,
    ref_chunk: RefChunkFn<V>,
) -> Result<Vec<NativeComponentWrite>>
where
    V: Send + 'static + secondary_by_copy::TermColumn,
    (V, u32): Ord + Spillable,
    (CopyKey<V>, u32): Ord + Spillable,
{
    use crate::io::container::sources::PullComponentSource;
    use crate::io::container::{StoreComponentDescriptor, StoreComponentRole, child_strategy};
    use crate::store::indexes::secondary_by_copy::CopyFamily;
    use crate::store::indexes::secondary_by_copy::out_of_core::{
        copy_child_chunk, copy_child_dtype,
    };
    use crate::store::indexes::secondary_by_reference::RefFamily;
    use crate::store::indexes::secondary_by_reference::out_of_core::ref_child_dtype;

    let copy_dtype = copy_child_dtype(encoded);
    let ref_dtype = ref_child_dtype(encoded);

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
                Arc::new(PullComponentSource::new(dtype.clone(), chunk_size, pull_fn)),
                child_strategy(&dtype),
            )
            .map_err(VortexRdfError::Vortex)?,
        );
        Ok(())
    };

    if let Some((posg, ospg)) = mergers.copy_keys {
        for (family, merger) in [(CopyFamily::Posg, posg), (CopyFamily::Ospg, ospg)] {
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
                    copy_child_chunk(family, &batch).map(Some)
                }),
            )?;
        }
    }
    if let Some((o_pairs, p_pairs)) = mergers.ref_pairs {
        for (family, merger) in [
            (RefFamily::Object, o_pairs),
            (RefFamily::Predicate, p_pairs),
        ] {
            let mut merger = merger;
            push(
                family.component_name(),
                family.component_slug(),
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
        crate::store::indexes::secondary_by_reference::out_of_core::ref_child_chunk_strings,
    )?;
    let (dtype, chunks) = chunk_stream(
        (merged, guard),
        chunk_size,
        |(merged, _guard), n| merged.next_batch(n),
        move |buf| build_struct_array(buf, layout, true),
        || make_empty_struct(layout),
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
/// codes; the dictionary rides beside the stream for the serializer.
fn emit_merged_run_dict_chunks(
    merged: Run<RawQuad>,
    mergers: IndexMergers<TermCode>,
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
        crate::store::indexes::secondary_by_reference::out_of_core::ref_child_chunk_codes,
    )?;
    let (dtype, chunks) = chunk_stream(
        (merged, guard),
        chunk_size,
        |(merged, _guard), n| merged.next_batch(n),
        move |buf| dictionary::build_chunk(buf, &code_map, true),
        dictionary::empty_struct,
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
/// chunks of codes encoded against the completed global dictionary,
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
        dictionary::empty_struct,
    )?;
    Ok(BuiltStream {
        dtype,
        chunks,
        components: Vec::new(),
        quads_sorted: true,
        dict: Some(dict),
    })
}
