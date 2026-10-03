//! The index side of the out-of-core build: each requested family's
//! `(key, row id)` entries spilled as the quad merge assigns row ids, then
//! streamed off their own merger as native components beside the quad chunks.

use std::path::Path;
use std::sync::Arc;

use vortex_array::ArrayRef;
use vortex_array::dtype::DType;

use super::into_vortex_error;
use super::spill::{MergedSink, RunMerger, RunSpiller, Spillable, TempRunsGuard};
use crate::error::Result;
use crate::io::container::sources::{PullComponentSource, PullFn};
use crate::io::container::{
    NativeComponentWrite, StoreComponentDescriptor, StoreComponentRole, default_child_strategy,
};
use crate::store::RawQuad;
use crate::store::indexes::IndexType;
use crate::store::indexes::components::{ComponentIdentity, TermColumn};
use crate::store::indexes::copy::CopyFamily;
use crate::store::indexes::copy::out_of_core::{CopyKey, copy_child_chunk, copy_child_dtype};
use crate::store::indexes::reference::RefFamily;
use crate::store::indexes::reference::out_of_core::{ref_child_chunk, ref_child_dtype};

/// One of each child of a family.
type Pair<T> = (T, T);
/// A reference-index entry: the value and its primary row id.
type RefEntry<V> = (V, u32);
/// A copy-index entry: the family's sort key and its primary row id.
type CopyEntry<V> = (CopyKey<V>, u32);

/// The spillers of a build's requested index families, fed one quad at a
/// time as the merge assigns row ids. `V` is the term encoding: strings, or
/// u32 dictionary codes.
struct IndexSpillers<V> {
    /// `SecondaryByReference`: (objects, predicates).
    refs: Option<Pair<RunSpiller<RefEntry<V>>>>,
    /// `SecondaryByCopy`: (POSG keys, OSPG keys).
    copies: Option<Pair<RunSpiller<CopyEntry<V>>>>,
}

/// The mergers [`IndexSpillers`] finish into, each streaming its family's
/// entries in global sort order.
pub(super) struct IndexMergers<V> {
    refs: Option<Pair<RunMerger<RefEntry<V>>>>,
    copies: Option<Pair<RunMerger<CopyEntry<V>>>>,
}

impl<V> IndexSpillers<V>
where
    V: Clone,
    RefEntry<V>: Ord + Spillable,
    CopyEntry<V>: Ord + Spillable,
{
    /// Spillers for the families `indexes` need, their runs under `dir` in
    /// windows of `capacity` entries.
    fn new(indexes: &[IndexType], dir: &Path, capacity: usize) -> Self {
        Self {
            refs: indexes.contains(&IndexType::SecondaryByReference).then(|| {
                (
                    RunSpiller::new(dir, "idx_o", capacity),
                    RunSpiller::new(dir, "idx_p", capacity),
                )
            }),
            copies: indexes.contains(&IndexType::SecondaryByCopy).then(|| {
                (
                    RunSpiller::new(dir, "idx_posg", capacity),
                    RunSpiller::new(dir, "idx_ospg", capacity),
                )
            }),
        }
    }

    /// Push `quad` at row `rid`, its terms encoded by `term_of`; only the
    /// terms the requested families consume are encoded.
    fn push(
        &mut self,
        quad: &RawQuad,
        rid: u32,
        term_of: &mut impl FnMut(&str) -> Result<V>,
    ) -> Result<()> {
        if let Some((posg, ospg)) = self.copies.as_mut() {
            let spog = [
                term_of(&quad.s)?,
                term_of(&quad.p)?,
                term_of(&quad.o)?,
                term_of(&quad.g)?,
            ];
            posg.push((CopyKey::posg(&spog), rid))?;
            if let Some((o, p)) = self.refs.as_mut() {
                o.push((spog[2].clone(), rid))?;
                p.push((spog[1].clone(), rid))?;
            }
            ospg.push((CopyKey::ospg(spog), rid))?;
        } else if let Some((o, p)) = self.refs.as_mut() {
            o.push((term_of(&quad.o)?, rid))?;
            p.push((term_of(&quad.p)?, rid))?;
        }
        Ok(())
    }

    fn into_mergers(self) -> Result<IndexMergers<V>> {
        Ok(IndexMergers {
            refs: self.refs.map(finish_pair).transpose()?,
            copies: self.copies.map(finish_pair).transpose()?,
        })
    }
}

/// Both spillers of a family finished into their mergers.
fn finish_pair<T: Ord + Spillable>(pair: Pair<RunSpiller<T>>) -> Result<Pair<RunMerger<T>>> {
    Ok((pair.0.into_merger()?, pair.1.into_merger()?))
}

/// First pass of the indexed pipeline: run the K-way quad merge to
/// completion — into memory when there is a single input run, else into
/// `merged.bin` under `dir` — feeding each requested family's spiller with
/// the quad's terms encoded by `term_of`. Returns the merged quads as a
/// single-run merger and the per-family mergers.
pub(super) fn merge_feeding_indexes<V>(
    mut merger: RunMerger<RawQuad>,
    dir: &Path,
    capacity: usize,
    indexes: &[IndexType],
    mut term_of: impl FnMut(&str) -> Result<V>,
) -> Result<(RunMerger<RawQuad>, IndexMergers<V>)>
where
    V: Clone,
    RefEntry<V>: Ord + Spillable,
    CopyEntry<V>: Ord + Spillable,
{
    let mut merged = MergedSink::create(dir, merger.run_count() <= 1)?;
    let mut spillers = IndexSpillers::new(indexes, dir, capacity);
    let mut rid: u32 = 0;
    while let Some(quad) = merger.next()? {
        spillers.push(&quad, rid, &mut term_of)?;
        merged.push(quad)?;
        rid += 1;
    }
    log::debug!(
        "[SortedStreamBuilder] Merged {} quads; index pair runs written",
        rid
    );
    let merged = RunMerger::new(vec![merged.finish()?])?;
    Ok((merged, spillers.into_mergers()?))
}

/// Each family's child as a native component write streaming its chunks
/// off its merger, in windows of `chunk_size` entries. `encoded` says
/// whether the entries hold u32 codes (else term strings), which picks the
/// child dtypes. Every pull closure holds the run guard, so the run files
/// outlive their readers.
pub(super) fn merger_components<V>(
    mergers: IndexMergers<V>,
    chunk_size: usize,
    guard: &Arc<TempRunsGuard>,
    encoded: bool,
) -> Result<Vec<NativeComponentWrite>>
where
    V: TermColumn + Send + 'static,
    RefEntry<V>: Ord + Spillable,
    CopyEntry<V>: Ord + Spillable,
{
    let mut families: Vec<(&'static ComponentIdentity, DType, PullFn)> = Vec::new();
    if let Some((posg, ospg)) = mergers.copies {
        let dtype = copy_child_dtype(encoded);
        for (family, mut merger) in [(CopyFamily::Posg, posg), (CopyFamily::Ospg, ospg)] {
            let pull = pull_fn(guard, move |n| {
                chunk_of(merger.next_batch(n)?, n, |keys| {
                    copy_child_chunk(family, keys)
                })
            });
            families.push((family.identity(), dtype.clone(), pull));
        }
    }
    if let Some((o, p)) = mergers.refs {
        let dtype = ref_child_dtype(encoded);
        for (family, mut merger) in [(RefFamily::Object, o), (RefFamily::Predicate, p)] {
            let pull = pull_fn(guard, move |n| {
                chunk_of(merger.next_batch(n)?, n, ref_child_chunk)
            });
            families.push((family.identity(), dtype.clone(), pull));
        }
    }
    families
        .into_iter()
        .map(|(identity, dtype, pull)| {
            Ok(NativeComponentWrite::new(
                StoreComponentDescriptor {
                    name: identity.name.into(),
                    role: StoreComponentRole::Index,
                    implementation: identity.slug.into(),
                    version: 1,
                    required: false,
                    // The merger emits the family in its global sort order.
                    sorted: true,
                    dtype: dtype.clone(),
                },
                Arc::new(PullComponentSource::new(dtype, chunk_size, pull)),
                default_child_strategy(),
            )?)
        })
        .collect()
}

/// `batch`, a window of a family's merged entries, as one child chunk;
/// `None` once a non-empty pull came back empty (a zero-row pull builds the
/// empty chunk).
fn chunk_of<T>(
    batch: Vec<T>,
    n: usize,
    chunk: impl FnOnce(&[T]) -> Result<ArrayRef>,
) -> Result<Option<ArrayRef>> {
    if batch.is_empty() && n > 0 {
        return Ok(None);
    }
    chunk(&batch).map(Some)
}

/// `pull` as the writer's [`PullFn`], holding the run guard.
fn pull_fn(
    guard: &Arc<TempRunsGuard>,
    mut pull: impl FnMut(usize) -> Result<Option<ArrayRef>> + Send + 'static,
) -> PullFn {
    let guard = Arc::clone(guard);
    Box::new(move |n| {
        let _hold_runs = &guard;
        pull(n).map_err(into_vortex_error)
    })
}
