//! The index side of the out-of-core build: each requested family's
//! `(term ranks, row id)` entries spilled as the quad merge assigns row ids,
//! then streamed off their own merger as native components beside the quad
//! chunks, each rank spelled as the layout's term (a dictionary code, or the
//! term's string) only as a child chunk is built.

use super::BuildSizes;
use super::runs::{
    CopyEntry, EntryRunReader, EntrySpiller, IndexEntry, QuadRuns, RefEntry, RunMerger,
};
use super::spill::TempRunsGuard;
use crate::error::Result;
use crate::io::container::NativeComponentWrite;
use crate::io::container::sources::{PullComponentSource, PullFn};
use crate::store::indexes::components::{ComponentIdentity, TermColumn, index_child_write};
use crate::store::indexes::copy::CopyFamily;
use crate::store::indexes::copy::out_of_core::{CopyKey, copy_child_chunk, copy_child_dtype};
use crate::store::indexes::reference::RefFamily;
use crate::store::indexes::reference::out_of_core::{ref_child_chunk, ref_child_dtype};
use crate::store::indexes::{IndexType, next_row_id};
use crate::store::schema::RowId;

use std::path::Path;
use std::sync::Arc;

use vortex_array::ArrayRef;
use vortex_array::dtype::DType;

/// One of each child of a family.
type Pair<T> = (T, T);

/// The merger of one family's spilled entries.
type EntryMerger<E> = RunMerger<EntryRunReader<E>>;

/// The spillers of a build's requested index families, fed one quad of ranks
/// at a time as the merge assigns row ids.
struct IndexSpillers {
    /// `SecondaryByReference`: (objects, predicates).
    refs: Option<Pair<EntrySpiller<RefEntry>>>,
    /// `SecondaryByCopy`: (POSG keys, OSPG keys).
    copies: Option<Pair<EntrySpiller<CopyEntry>>>,
}

/// The mergers [`IndexSpillers`] finish into, each streaming its family's
/// entries in global sort order.
struct IndexMergers {
    refs: Option<Pair<EntryMerger<RefEntry>>>,
    copies: Option<Pair<EntryMerger<CopyEntry>>>,
}

/// Both spillers of a family named `names`, runs of `capacity` entries.
fn spiller_pair<E: IndexEntry>(
    dir: &Path,
    names: Pair<&'static str>,
    capacity: usize,
) -> Result<Pair<EntrySpiller<E>>> {
    Ok((
        EntrySpiller::new(dir, names.0, capacity)?,
        EntrySpiller::new(dir, names.1, capacity)?,
    ))
}

impl IndexSpillers {
    /// Spillers for the families `indexes` need, their runs under `dir` in
    /// windows of `capacity` entries.
    fn new(indexes: &[IndexType], dir: &Path, capacity: usize) -> Result<Self> {
        Ok(Self {
            refs: indexes
                .contains(&IndexType::SecondaryByReference)
                .then(|| spiller_pair(dir, ("idx_o", "idx_p"), capacity))
                .transpose()?,
            copies: indexes
                .contains(&IndexType::SecondaryByCopy)
                .then(|| spiller_pair(dir, ("idx_posg", "idx_ospg"), capacity))
                .transpose()?,
        })
    }

    /// Push the quad of ranks `[s, p, o, g]` at row `rid`: the ranks each
    /// requested family keys on, in its key order.
    fn push(&mut self, ranks: [u32; 4], rid: RowId) -> Result<()> {
        if let Some((o, p)) = self.refs.as_mut() {
            o.push((ranks[2], rid))?;
            p.push((ranks[1], rid))?;
        }
        if let Some((posg, ospg)) = self.copies.as_mut() {
            posg.push((CopyKey::new(CopyFamily::Posg, ranks).0, rid))?;
            ospg.push((CopyKey::new(CopyFamily::Ospg, ranks).0, rid))?;
        }
        Ok(())
    }

    fn into_mergers(self) -> Result<IndexMergers> {
        Ok(IndexMergers {
            refs: self.refs.map(finish_pair).transpose()?,
            copies: self.copies.map(finish_pair).transpose()?,
        })
    }
}

/// Both spillers of a family finished into their mergers.
fn finish_pair<E: IndexEntry>(pair: Pair<EntrySpiller<E>>) -> Result<Pair<EntryMerger<E>>> {
    Ok((pair.0.into_merger()?, pair.1.into_merger()?))
}

/// The indexed pipeline's first pass and its index children: run a merge of
/// `runs` (their ids ranked through `rank_of`) to the end while the spillers
/// of the families `indexes` requests take each quad's ranks, then stream
/// each family's child off its merger ([`merger_components`]), every rank
/// spelled as `V` by `decode` (a dictionary code, or the term's string). The
/// merge yields each distinct quad once, so its row ids number the
/// deduplicated rows. `sizes` gives the run capacity and the child chunk
/// size; `runs` stays readable for the quad chunks' own merge.
pub(super) fn merge_feeding_indexes<V>(
    runs: &QuadRuns,
    rank_of: &Arc<[u32]>,
    guard: &Arc<TempRunsGuard>,
    sizes: BuildSizes,
    indexes: &[IndexType],
    decode: impl Fn(u32) -> Result<V> + Clone + Send + 'static,
) -> Result<Vec<NativeComponentWrite>>
where
    V: TermColumn + Send + 'static,
{
    let mut merger = runs.merger(rank_of)?;
    let mut spillers = IndexSpillers::new(indexes, guard.path(), sizes.run_rows)?;
    // Rows are numbered as the merge emits them, so the count is unknown up
    // front: each id is a checked increment, refusing the store at the first
    // row past the last row id rather than wrapping.
    let mut rows: u64 = 0;
    while let Some(ranks) = merger.next()? {
        spillers.push(ranks, next_row_id(&mut rows)?)?;
    }
    log::debug!(
        "[SortedStreamBuilder] Merged {} quads; index entry runs written",
        rows
    );
    merger_components(spillers.into_mergers()?, sizes.chunk_rows, guard, decode)
}

/// A copy key's four ranks spelled as `V`.
fn decode_key<V>(decode: &impl Fn(u32) -> Result<V>, key: &[u32; 4]) -> Result<[V; 4]> {
    Ok([
        decode(key[0])?,
        decode(key[1])?,
        decode(key[2])?,
        decode(key[3])?,
    ])
}

/// Each family's child as a native component write ([`index_child_write`])
/// streaming its chunks off its merger in windows of `chunk_size` entries,
/// never materialized and not in lockstep with the quad stream; `decode`
/// spells each window's ranks as `V`, which the child dtypes follow. Every
/// pull holds the run guard, so the run files outlive their readers.
fn merger_components<V>(
    mergers: IndexMergers,
    chunk_size: usize,
    guard: &Arc<TempRunsGuard>,
    decode: impl Fn(u32) -> Result<V> + Clone + Send + 'static,
) -> Result<Vec<NativeComponentWrite>>
where
    V: TermColumn + Send + 'static,
{
    let mut families: Vec<(&'static ComponentIdentity, DType, PullFn)> = Vec::new();
    if let Some((posg, ospg)) = mergers.copies {
        let dtype = copy_child_dtype::<V>();
        for (family, mut merger) in [(CopyFamily::Posg, posg), (CopyFamily::Ospg, ospg)] {
            let decode = decode.clone();
            let pull = pull_fn(guard, move |n| {
                chunk_of(merger.next_batch(n)?, n, |entries| {
                    let keys = entries
                        .iter()
                        .map(|(key, rid)| Ok((CopyKey(decode_key(&decode, key)?), *rid)))
                        .collect::<Result<Vec<_>>>()?;
                    copy_child_chunk(family, &keys)
                })
            });
            families.push((family.identity(), dtype.clone(), pull));
        }
    }
    if let Some((o, p)) = mergers.refs {
        let dtype = ref_child_dtype::<V>();
        for (family, mut merger) in [(RefFamily::Object, o), (RefFamily::Predicate, p)] {
            let decode = decode.clone();
            let pull = pull_fn(guard, move |n| {
                chunk_of(merger.next_batch(n)?, n, |entries| {
                    let pairs = entries
                        .iter()
                        .map(|&(rank, rid)| Ok((decode(rank)?, rid)))
                        .collect::<Result<Vec<_>>>()?;
                    ref_child_chunk(&pairs)
                })
            });
            families.push((family.identity(), dtype.clone(), pull));
        }
    }
    families
        .into_iter()
        .map(|(identity, dtype, pull)| {
            // The merger emits the family in its global sort order.
            index_child_write(
                identity,
                true,
                Arc::new(PullComponentSource::new(dtype, chunk_size, pull)),
            )
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
        Ok(pull(n)?)
    })
}
