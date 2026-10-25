//! The index side of the out-of-core build: each requested family's
//! `(term ranks, row id)` entries spilled as the quad merge assigns row ids,
//! then streamed off their own merger as native components beside the quad
//! chunks, each rank spelled as the layout's term (a dictionary code, or the
//! term's string) only as a child chunk is built.

use super::BuildSizes;
use super::runs::{
    CopyEntry, EntryRunReader, EntrySpiller, IndexEntry, QuadRuns, RankTable, RefEntry, RunMerger,
};
use super::spill::TempRunsGuard;
use crate::error::Result;
use crate::io::container::sources::NativeComponentSource;
use crate::store::indexes::components::{ChildStream, ComponentIdentity, TermColumn};
use crate::store::indexes::copy::CopyFamily;
use crate::store::indexes::copy::out_of_core::{CopyKey, copy_child_chunk};
use crate::store::indexes::reference::RefFamily;
use crate::store::indexes::reference::out_of_core::ref_child_chunk;
use crate::store::indexes::{IndexType, next_row_id};
use crate::store::schema::RowId;

use std::path::Path;
use std::sync::{Arc, Mutex};

use vortex_array::ArrayRef;
use vortex_array::dtype::DType;
use vortex_array::stream::{ArrayStreamAdapter, ArrayStreamExt, SendableArrayStream};
use vortex_error::VortexResult;

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
    rank_of: &RankTable,
    guard: &Arc<TempRunsGuard>,
    sizes: BuildSizes,
    indexes: &[IndexType],
    decode: impl Fn(u32) -> Result<V> + Clone + Send + 'static,
) -> Result<Vec<ChildStream>>
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

/// Each family's child as a [`ChildStream`] whose source streams its chunks
/// off its merger in windows of `chunk_size` entries,
/// never materialized and not in lockstep with the quad stream; `decode`
/// spells each window's ranks as `V`. Each child's empty chunk, built by the
/// same chunk builder over no entries, gives the child its dtype. Every pull
/// holds the run guard, so the run files outlive their readers.
fn merger_components<V>(
    mergers: IndexMergers,
    chunk_size: usize,
    guard: &Arc<TempRunsGuard>,
    decode: impl Fn(u32) -> Result<V> + Clone + Send + 'static,
) -> Result<Vec<ChildStream>>
where
    V: TermColumn + Send + 'static,
{
    let mut families: Vec<(&'static ComponentIdentity, ArrayRef, PullFn)> = Vec::new();
    if let Some((posg, ospg)) = mergers.copies {
        for (family, mut merger) in [(CopyFamily::Posg, posg), (CopyFamily::Ospg, ospg)] {
            let empty = copy_child_chunk::<V>(family, &[])?;
            let decode = decode.clone();
            let pull = pull_fn(guard, move |n| {
                chunk_of(merger.next_batch(n)?, |entries| {
                    let keys = entries
                        .iter()
                        .map(|(key, rid)| Ok((CopyKey(decode_key(&decode, key)?), *rid)))
                        .collect::<Result<Vec<_>>>()?;
                    copy_child_chunk(family, &keys)
                })
            });
            families.push((family.identity(), empty, pull));
        }
    }
    if let Some((o, p)) = mergers.refs {
        for (family, mut merger) in [(RefFamily::Object, o), (RefFamily::Predicate, p)] {
            let empty = ref_child_chunk::<V>(&[])?;
            let decode = decode.clone();
            let pull = pull_fn(guard, move |n| {
                chunk_of(merger.next_batch(n)?, |entries| {
                    let pairs = entries
                        .iter()
                        .map(|&(rank, rid)| Ok((decode(rank)?, rid)))
                        .collect::<Result<Vec<_>>>()?;
                    ref_child_chunk(&pairs)
                })
            });
            families.push((family.identity(), empty, pull));
        }
    }
    Ok(families
        .into_iter()
        .map(|(identity, empty, pull)| ChildStream {
            identity,
            // The merger emits the family in its global sort order.
            sorted: true,
            source: Arc::new(PullComponentSource::new(empty, chunk_size, pull)),
        })
        .collect())
}

/// `batch`, a window of a family's merged entries, as one child chunk;
/// `None` once the merger is exhausted and the window comes back empty.
fn chunk_of<T>(
    batch: Vec<T>,
    chunk: impl FnOnce(&[T]) -> Result<ArrayRef>,
) -> Result<Option<ArrayRef>> {
    if batch.is_empty() {
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

/// A pull closure yielding one component chunk of at most the given rows per
/// call, `Ok(None)` once exhausted.
type PullFn = Box<dyn FnMut(usize) -> VortexResult<Option<ArrayRef>> + Send>;

/// A single-shot component source over a pull closure: each call reads the
/// next window off a spill-run merger's run files, so the component is never
/// materialized. `empty` is the child's empty chunk: its dtype is the
/// source's, and it is the one chunk a child whose first pull is exhausted
/// writes.
struct PullComponentSource {
    empty: ArrayRef,
    batch_rows: usize,
    pull: Mutex<Option<PullFn>>,
}

impl PullComponentSource {
    fn new(empty: ArrayRef, batch_rows: usize, pull: PullFn) -> Self {
        Self {
            empty,
            batch_rows,
            pull: Mutex::new(Some(pull)),
        }
    }
}

impl NativeComponentSource for PullComponentSource {
    fn dtype(&self) -> &DType {
        self.empty.dtype()
    }

    fn open(&self) -> VortexResult<SendableArrayStream> {
        let pull = self
            .pull
            .lock()
            .expect("pull source lock")
            .take()
            .ok_or_else(|| {
                vortex_error::vortex_err!("a pull-backed component source replays only once")
            })?;
        let batch_rows = self.batch_rows;
        // At least one chunk is emitted: the child strategy needs one,
        // possibly empty, to write a schema-complete component, so an empty
        // component contributes the empty chunk the source holds.
        let empty = Some(self.empty.clone());
        let stream =
            futures::stream::unfold((Some(pull), empty), move |(state, empty)| async move {
                let mut pull = state?;
                match pull(batch_rows) {
                    Ok(Some(chunk)) => Some((Ok(chunk), (Some(pull), None))),
                    Ok(None) => empty.map(|empty| (Ok(empty), (None, None))),
                    Err(e) => Some((Err(e), (None, None))),
                }
            });
        Ok(ArrayStreamExt::boxed(ArrayStreamAdapter::new(
            self.empty.dtype().clone(),
            stream,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A pull-backed source hands out its stream once; a second open is an
    /// error.
    #[test]
    fn pull_source_replays_once() {
        let empty = ref_child_chunk::<String>(&[]).unwrap();
        let source = PullComponentSource::new(empty, 1, Box::new(|_| Ok(None)));
        assert!(source.open().is_ok());
        assert!(source.open().is_err());
    }
}
