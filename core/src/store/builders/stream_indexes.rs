//! The index side of the out-of-core build: each requested family's
//! `(key, row id)` entries spilled as the quad merge assigns row ids, then
//! streamed off their own merger as native components beside the quad chunks.

use std::path::Path;
use std::sync::Arc;

use vortex_array::ArrayRef;
use vortex_array::dtype::DType;

use super::into_vortex_error;
use super::spill::{MergedSink, Run, RunMerger, RunSpiller, RunWriter, Spillable, TempRunsGuard};
use crate::error::{Result, VortexRdfError};
use crate::io::container::NativeComponentWrite;
use crate::store::RawQuad;
use crate::store::indexes::copy::out_of_core::CopyKey;

/// The two `SecondaryByReference` mergers of a build: (objects, predicates).
type RefMergers<V> = (RunMerger<(V, u32)>, RunMerger<(V, u32)>);
/// The two `SecondaryByCopy` mergers of a build: (POSG keys, OSPG keys).
type CopyMergers<V> = (RunMerger<(CopyKey<V>, u32)>, RunMerger<(CopyKey<V>, u32)>);

/// The external-sort mergers for one build's secondary indexes, present only
/// for the index types the build requested. `V` is the term encoding: strings,
/// or u32 dictionary codes.
pub(super) struct IndexMergers<V> {
    ref_pairs: Option<RefMergers<V>>,
    copy_keys: Option<CopyMergers<V>>,
}

/// First pass of the indexed pipeline: run the K-way quad merge to completion,
/// collecting merged quads — in memory when there is a single input run (the
/// dataset already fit once), else spilled to `merged.bin` — while feeding
/// each requested index family's spiller with that quad's terms encoded by
/// `term_of`: `(value, row id)` pairs for the reference index, full
/// [`CopyKey`]s for the copy index. Only the terms the requested families
/// consume are encoded. Returns the merged quads and the per-family mergers,
/// ready to stream entries in global sort order.
pub(super) fn merge_quads_feeding_indexes<V>(
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

/// A window of one reference component's merged pairs, as one child chunk.
pub(super) type RefChunkFn<V> = fn(&[(V, u32)]) -> Result<ArrayRef>;

/// Turn a build's spill-run mergers into native component writes: each family
/// streams its child's chunks straight off its merger — no lockstep zip with
/// the quad stream, no materialization. The temp-run guard is shared with the
/// quad stream so the run files outlive every reader. `encoded` says whether
/// the entries hold u32 dictionary codes (else term strings), which picks the
/// child dtypes; `ref_chunk` builds a reference child chunk for that encoding.
pub(super) fn merger_components<V>(
    mergers: IndexMergers<V>,
    chunk_size: usize,
    guard: &Arc<TempRunsGuard>,
    encoded: bool,
    ref_chunk: RefChunkFn<V>,
) -> Result<Vec<NativeComponentWrite>>
where
    V: Send + 'static + crate::store::indexes::components::TermColumn,
    (V, u32): Ord + Spillable,
    (CopyKey<V>, u32): Ord + Spillable,
{
    use crate::io::container::sources::PullComponentSource;
    use crate::io::container::{
        StoreComponentDescriptor, StoreComponentRole, default_child_strategy,
    };
    use crate::store::indexes::copy::CopyFamily;
    use crate::store::indexes::copy::out_of_core::{copy_child_chunk, copy_child_dtype};
    use crate::store::indexes::reference::RefFamily;
    use crate::store::indexes::reference::out_of_core::ref_child_dtype;

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
                Arc::new(PullComponentSource::new(dtype, chunk_size, pull_fn)),
                default_child_strategy(),
            )
            .map_err(VortexRdfError::Vortex)?,
        );
        Ok(())
    };

    if let Some((posg, ospg)) = mergers.copy_keys {
        for (family, merger) in [(CopyFamily::Posg, posg), (CopyFamily::Ospg, ospg)] {
            let mut merger = merger;
            push(
                family.identity().name,
                family.identity().slug,
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
                family.identity().name,
                family.identity().slug,
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
