//! A filter evaluated over a file split by split, and statistics-only pruning
//! envelopes: functions of a [`NativeStoreFile`] and a filter expression, with
//! no store state.

use std::borrow::Borrow;
use std::ops::{BitAnd, Range};
use std::sync::{Arc, Mutex, PoisonError};

use futures::future::BoxFuture;
use futures::{FutureExt as _, StreamExt, future, stream};
use vortex_array::expr::forms::conjuncts;
use vortex_array::expr::{BoundExpression, Expression, get_item, root};
use vortex_array::{ArrayRef, ExecutionCtx, MaskFuture, VortexSessionExecute as _};
use vortex_buffer::Buffer;
use vortex_layout::LayoutReader;
use vortex_mask::{AllOr, Mask};
use vortex_scan::selection::Selection;

use crate::error::{Result, VortexRdfError};
use crate::io::read::{available_parallelism, spawned_splits};
use crate::observe::observe;
#[cfg(test)]
use crate::store::persist::native_file::FileCounter;
use crate::store::persist::native_file::NativeStoreFile;
use crate::store::scan::file_exprs::{PushedFilter, QUAD_SCOPE};
use crate::store::scan::file_scans::{ScanDriver, split_window, strict_ids};
use crate::store::scan::pieces::{FIRST_PIECE_ROWS, growing_pieces};
use crate::store::schema::{COL_G, COL_O, COL_O_VALUE, COL_P, COL_S, RowId};
use crate::store::view::selection::RowSelection;

/// Split a file view's [`RowSelection`] into the two knobs the per-split filter
/// loop understands: a [`Selection`] narrowing the mask (an id list, e.g. from a
/// secondary index) and the row-id `bounds` it iterates. A `Range` narrows the
/// bounds; an `Ids` list narrows the mask; `All` narrows neither.
fn split_bounds(selection: &RowSelection, row_count: u64) -> (Selection, Range<u64>) {
    match selection {
        RowSelection::All => (Selection::All, 0..row_count),
        RowSelection::Range(range) => (Selection::All, range.clone()),
        RowSelection::Ids(ids) => (Selection::IncludeByIndex(strict_ids(ids)), 0..row_count),
    }
}

/// The starting mask for one file split: the rows `selection` covers within
/// `range`, minus any that `deleted` has tombstoned, split-relative (one bit
/// per row of `range`).
fn split_start_mask(
    mask_selection: &Selection,
    deleted: Option<&Mask>,
    range: &Range<u64>,
) -> Mask {
    let mask = mask_selection.row_mask(range).mask().clone();
    match deleted {
        None => mask,
        Some(deleted) => {
            let live = !&deleted.slice(range.start as usize..range.end as usize);
            mask.bitand(&live)
        }
    }
}

/// `filter`'s top-level ANDed conditions (the struct layout prunes one
/// single-field expression at a time), each bound against `reader`'s dtype
/// through the file's memo as the iterator reaches it. One bound identity per
/// shape, shared by every split task and every later call, keeps vortex's
/// identity-keyed reader caches hitting (see `BoundExprMemo`).
fn bound_conjuncts<'a>(
    file: &'a NativeStoreFile,
    reader: &'a dyn LayoutReader,
    filter: &'a Expression,
) -> impl Iterator<Item = Result<BoundExpression>> + 'a {
    conjuncts(filter).into_iter().map(move |conjunct| {
        Ok(file
            .bound_exprs()
            .bind(QUAD_SCOPE, &conjunct, reader.dtype())?)
    })
}

/// `mask` over `range` narrowed by each conjunct's statistics-only pruning
/// (zone maps and footer stats, no row data read), stopping once nothing
/// survives. Pruning only proves that rows cannot match, so a surviving row
/// may still fail the filter. A conjunct is taken from `conjuncts` only when
/// the loop reaches it, so an iterator that binds as it yields binds none
/// past the one that left nothing. The pruning phase of both
/// [`evaluate_filter_split`] and [`row_range_from_pruning`].
async fn prune_mask<B: Borrow<BoundExpression>>(
    reader: &Arc<dyn LayoutReader>,
    conjuncts: impl IntoIterator<Item = Result<B>>,
    range: &Range<u64>,
    mut mask: Mask,
) -> Result<Mask> {
    let mut conjuncts = conjuncts.into_iter();
    while !mask.all_false() {
        let Some(conjunct) = conjuncts.next() else {
            break;
        };
        let pruned = reader
            .pruning_evaluation(range, conjunct?.borrow(), mask.clone())?
            .await?;
        mask = mask.bitand(&pruned);
    }
    Ok(mask)
}

/// The rank of a filter column in the prior order of [`ConjunctOrder`]: an
/// equality on the subject or object column is expected to keep fewer rows
/// than one on the predicate or graph column, whose distinct values are few.
fn column_rank(column: &str) -> u8 {
    match column {
        COL_S => 0,
        COL_O | COL_O_VALUE => 1,
        COL_P => 2,
        COL_G => 3,
        _ => 4,
    }
}

/// The prior order of `filter`'s `n` top-level conjuncts: by column rank
/// when every conjunct is a code equality, ties in the filter's order; the
/// filter's order otherwise.
fn prior_order(filter: &PushedFilter, n: usize) -> Vec<usize> {
    let mut prior: Vec<usize> = (0..n).collect();
    if let Some(eqs) = filter.eqs()
        && eqs.len() == n
    {
        let columns: Vec<&str> = eqs.iter().map(|(column, _)| column.as_ref()).collect();
        // A stable sort: ties keep the filter's order.
        prior.sort_by_key(|&i| column_rank(columns[i]));
    }
    prior
}

/// The order in which the split tasks of one evaluation test a filter's
/// conjuncts. Until every conjunct has been tested by some split, a prior by
/// column ([`column_rank`], ties in the filter's order); from then on,
/// ascending observed selectivity (rows kept over rows tested, summed over
/// the splits so far), so a split tests the conjunct that discards the most
/// rows first and skips the others wherever it leaves nothing. The pruning
/// phase runs in the same order. Any order answers the same mask.
struct ConjunctOrder {
    conjuncts: Vec<BoundExpression>,
    prior: Vec<usize>,
    /// Per conjunct, the rows tested and the rows kept so far.
    observed: Mutex<Vec<(u64, u64)>>,
}

impl ConjunctOrder {
    /// `conjuncts` are `filter`'s top-level conjuncts, bound, in its order.
    fn new(filter: &PushedFilter, conjuncts: Vec<BoundExpression>) -> Self {
        let prior = prior_order(filter, conjuncts.len());
        let observed = Mutex::new(vec![(0, 0); conjuncts.len()]);
        Self {
            conjuncts,
            prior,
            observed,
        }
    }

    /// The conjunct indexes in the order the next split tests them.
    fn sequence(&self) -> Vec<usize> {
        let observed = self.observed.lock().unwrap_or_else(PoisonError::into_inner);
        if observed.iter().any(|&(tested, _)| tested == 0) {
            return self.prior.clone();
        }
        let selectivity: Vec<f64> = observed
            .iter()
            .map(|&(tested, kept)| kept as f64 / tested as f64)
            .collect();
        drop(observed);
        let mut order = self.prior.clone();
        order.sort_by(|&a, &b| selectivity[a].total_cmp(&selectivity[b]));
        order
    }

    /// Record one split's test of conjunct `idx`: `tested` rows in, `kept`
    /// rows out.
    fn report(&self, idx: usize, tested: usize, kept: usize) {
        let mut observed = self.observed.lock().unwrap_or_else(PoisonError::into_inner);
        observed[idx].0 += tested as u64;
        observed[idx].1 += kept as u64;
    }
}

/// A filter evaluated over one file split, a narrowing mask threaded through
/// the layout reader's two phases: statistics-only pruning, then each
/// conjunct's evaluation over what survives, in the order `order` gives.
/// Answers the split-relative mask of the rows that match, exactly; mirrors
/// the filter phase of vortex's `split_exec`.
async fn evaluate_filter_split(
    reader: Arc<dyn LayoutReader>,
    order: &ConjunctOrder,
    range: &Range<u64>,
    start_mask: Mask,
) -> Result<Mask> {
    let sequence = order.sequence();
    // Phase 1: statistics-only pruning ([`prune_mask`]), no I/O beyond the
    // cached stats. The conjuncts are yielded by index: a closure over a
    // borrowed item is higher-ranked, and the futures holding it across the
    // awaits cannot be proven `Send`.
    let bound = (0..sequence.len()).map(|k| Ok(&order.conjuncts[sequence[k]]));
    let mut mask = prune_mask(&reader, bound, range, start_mask).await?;
    // Phase 2: for whatever the stats couldn't rule out, read and evaluate each
    // conjunct for real, threading the narrowing mask so later conjuncts see
    // fewer rows, and report what each kept.
    for &i in &sequence {
        if mask.all_false() {
            return Ok(mask);
        }
        let tested = mask.true_count();
        mask = reader
            .filter_evaluation(range, &order.conjuncts[i], MaskFuture::ready(mask))?
            .await?;
        order.report(i, tested, mask.true_count());
    }
    Ok(mask)
}

/// Whether a fold that stops at `want` matches evaluates its splits in
/// [`growing_pieces`]: only when the first piece alone could hold them, so
/// a fold whose cap the first piece cannot reach (a probing `LIMIT` past the
/// rows) evaluates whole splits.
fn cut_into_pieces(want: usize) -> bool {
    want < FIRST_PIECE_ROWS
}

/// The filter tasks over `ranges` (each inside one of the selection's split
/// ranges, [`selection_split_ranges`], whose `mask_selection` narrows them),
/// in order: each evaluates `filter` over its range (zone-map pruning first,
/// then the conjuncts) and answers the range-relative surviving mask with
/// its range. The prelude of [`kept_file_rows`], [`map_filter_splits`] and
/// [`fold_filter_splits_ordered`], which differ in how they drive the tasks.
///
/// The ranges are owned before the task futures are built: an iterator
/// borrowing the memoized splits across the awaits trips rustc's
/// higher-ranked lifetime inference when a caller spawns the future.
fn filter_split_tasks(
    file: &NativeStoreFile,
    filter: &PushedFilter,
    mask_selection: &Selection,
    ranges: Vec<Range<u64>>,
    deleted: Option<&Mask>,
) -> Result<Vec<SplitTask>> {
    // The cached layout reader tree, shared by every split task, so the
    // zone-map stats are looked up once.
    let reader = file.layout_reader()?;
    let order = Arc::new(ConjunctOrder::new(
        filter,
        bound_conjuncts(file, reader.as_ref(), filter.expr()).collect::<Result<_>>()?,
    ));
    Ok(ranges
        .into_iter()
        .map(|range| {
            let reader = Arc::clone(&reader);
            let order = Arc::clone(&order);
            let start_mask = split_start_mask(mask_selection, deleted, &range);
            async move {
                observe!(SplitEvaluated);
                let mask = evaluate_filter_split(reader, &order, &range, start_mask).await?;
                Ok::<_, VortexRdfError>((mask, range))
            }
            .boxed()
        })
        .collect())
}

/// One split's filter evaluation, owning everything it reads: the surviving
/// split-relative mask with the split's range.
pub(crate) type SplitTask = BoxFuture<'static, Result<(Mask, Range<u64>)>>;

/// The filter tasks of every split `selection` touches, in file order
/// ([`filter_split_tasks`]), tombstoned rows cleared from their masks. Each
/// evaluates only when polled.
pub(crate) fn split_filter_tasks(
    file: &NativeStoreFile,
    filter: &PushedFilter,
    selection: &RowSelection,
    deleted: Option<&Mask>,
) -> Result<Vec<SplitTask>> {
    let (mask_selection, ranges) = selection_split_ranges(file, selection)?;
    filter_split_tasks(file, filter, &mask_selection, ranges, deleted)
}

/// The natural file splits `selection` touches, each clamped to its bounds,
/// in file order, beside the [`Selection`] that narrows each split's mask
/// ([`split_bounds`]).
fn selection_split_ranges(
    file: &NativeStoreFile,
    selection: &RowSelection,
) -> Result<(Selection, Vec<Range<u64>>)> {
    let (mask_selection, bounds) = split_bounds(selection, file.row_count());
    let ranges = file
        .splits()?
        .iter()
        .filter_map(|split| {
            let start = split.start.max(bounds.start);
            let end = split.end.min(bounds.end);
            (start < end).then_some(start..end)
        })
        .collect();
    Ok((mask_selection, ranges))
}

/// The rows of `selection` a test admits, chunk by chunk: given one column's
/// values at a split's surviving rows, the mask of those it admits (one bit
/// per value, in order).
pub(crate) type ChunkTest =
    Arc<dyn Fn(ArrayRef, &mut ExecutionCtx) -> Result<Mask> + Send + Sync + 'static>;

/// The rows of `selection` (exact) that `filter` keeps, every one of them
/// when there is none, whose value in `column` passes `test`, as ascending
/// row ids: one pass, split by split. Each split evaluates the filter
/// ([`evaluate_filter_split`]: zone pruning, then its conjuncts), reads
/// `column` at the rows the filter left and tests them, and maps the result
/// back onto the split by rank, so the column is read only where the filter
/// kept rows and no id list of the filter's own matches is built. The test
/// runs on the values, never as an expression bound into the file's readers.
/// The splits are spawned onto the runtime's workers from two up, as
/// [`map_filter_splits`] spawns its own, and answer in file order.
/// Tombstones are not applied: the reads apply them, as for
/// [`matching_file_rows`].
pub(crate) async fn kept_file_rows(
    file: &NativeStoreFile,
    filter: Option<&PushedFilter>,
    selection: &RowSelection,
    column: &'static str,
    test: ChunkTest,
) -> Result<Vec<RowId>> {
    let (mask_selection, ranges) = selection_split_ranges(file, selection)?;
    let tasks = match filter {
        Some(filter) => filter_split_tasks(file, filter, &mask_selection, ranges, None)?,
        None => ranges
            .into_iter()
            .map(|range| {
                let mask = split_start_mask(&mask_selection, None, &range);
                future::ready(Ok((mask, range))).boxed()
            })
            .collect(),
    };
    let reader = file.layout_reader()?;
    let projection =
        file.bound_exprs()
            .bind(QUAD_SCOPE, &get_item(column, root()), reader.dtype())?;
    let tasks: Vec<SplitTask> = tasks
        .into_iter()
        .map(|task| {
            let reader = Arc::clone(&reader);
            let projection = projection.clone();
            let test = Arc::clone(&test);
            async move {
                let (mask, range) = task.await?;
                if mask.all_false() {
                    return Ok((mask, range));
                }
                let values = reader
                    .projection_evaluation(&range, &projection, MaskFuture::ready(mask.clone()))?
                    .await?;
                let mut ctx = crate::session::VORTEX_SESSION.create_execution_ctx();
                let admitted = test(values, &mut ctx)?;
                Ok((mask.intersect_by_rank(&admitted), range))
            }
            .boxed()
        })
        .collect();
    #[cfg(test)]
    file.note(FileCounter::ColumnStreams, 1);
    let window = split_window();
    let mut results = match ScanDriver::for_filter_evaluation(tasks.len()) {
        ScanDriver::Inline => stream::iter(tasks).buffered(window).left_stream(),
        ScanDriver::Spawned => spawned_splits(tasks, window).right_stream(),
    };
    let mut ids = Vec::new();
    while let Some(result) = results.next().await {
        let (mask, range) = result?;
        push_kept_ids(&mut ids, &mask, &range);
    }
    Ok(ids)
}

/// `filter` evaluated over every natural file split the selection touches,
/// each split's surviving mask mapped through `map`: the split loop behind
/// [`count_matching_rows`] and [`matching_file_rows`]. Up to [`split_window`]
/// splits are in flight, spawned onto the runtime's workers from two splits
/// up ([`ScanDriver::for_filter_evaluation`]), so the evaluations run on as
/// many cores; `map` is given each split's range.
async fn map_filter_splits<T, F>(
    file: &NativeStoreFile,
    filter: &PushedFilter,
    selection: &RowSelection,
    deleted: Option<&Mask>,
    map: F,
) -> Result<Vec<T>>
where
    F: Fn(Mask, &Range<u64>) -> T,
{
    let (mask_selection, ranges) = selection_split_ranges(file, selection)?;
    let splits = ranges.len();
    let tasks = filter_split_tasks(file, filter, &mask_selection, ranges, deleted)?;
    let concurrency = split_window();
    let mut results = match ScanDriver::for_filter_evaluation(splits) {
        ScanDriver::Inline => stream::iter(tasks)
            .buffer_unordered(concurrency)
            .left_stream(),
        ScanDriver::Spawned => spawned_splits(tasks, concurrency).right_stream(),
    };
    let mut out = Vec::new();
    while let Some(r) = results.next().await {
        let (mask, range) = r?;
        out.push(map(mask, &range));
    }
    Ok(out)
}

/// `filter` evaluated over the selection's splits in file order, each split's
/// surviving mask fed to `step` with the split's index among the selection's
/// splits, until it answers `false`: the early-exit twin of
/// [`map_filter_splits`], behind [`split_masks_until`],
/// [`first_matching_rows`] and [`count_matching_rows_capped`]. A fold that
/// stops at `want` matches, fewer than one piece holds
/// ([`cut_into_pieces`]), evaluates the splits in [`growing_pieces`], so
/// matches found in the first rows do not wait for the rest of their split;
/// `step` then gets each piece. The splits' pieces run on the row-scan
/// driver ([`ScanDriver::for_row_scan`]): inline up to its split limit,
/// spawned onto the runtime above it. Either way a few (the available
/// parallelism, at least two) run ahead of the consumer, so the I/O
/// overlaps; whatever is in flight past the exit is dropped unread, a
/// spawned piece aborted with its task.
async fn fold_filter_splits_ordered<F>(
    file: &NativeStoreFile,
    filter: &PushedFilter,
    selection: &RowSelection,
    deleted: Option<&Mask>,
    want: usize,
    mut step: F,
) -> Result<()>
where
    F: FnMut(Mask, &Range<u64>, usize) -> bool,
{
    let (mask_selection, ranges) = selection_split_ranges(file, selection)?;
    let splits = ranges.len();
    let pieces: Vec<(usize, Range<u64>)> = if cut_into_pieces(want) {
        growing_pieces(ranges.iter().cloned(), FIRST_PIECE_ROWS as u64).collect()
    } else {
        ranges.into_iter().enumerate().collect()
    };
    let (split_of, ranges): (Vec<usize>, Vec<Range<u64>>) = pieces.into_iter().unzip();
    let tasks = filter_split_tasks(file, filter, &mask_selection, ranges, deleted)?;
    let lookahead = available_parallelism().max(2);
    let mut results = match ScanDriver::for_row_scan(splits) {
        ScanDriver::Inline => stream::iter(tasks).buffered(lookahead).left_stream(),
        ScanDriver::Spawned => spawned_splits(tasks, lookahead).right_stream(),
    };
    let mut split_of = split_of.into_iter();
    while let Some(r) = results.next().await {
        let (mask, range) = r?;
        observe!(SplitConsumed(range.clone()));
        let split = split_of.next().expect("one split index per task");
        if !step(mask, &range, split) {
            break;
        }
    }
    Ok(())
}

/// A pending filter's evaluation over a selection: each evaluated split's
/// surviving mask (one bit per row of its range, tombstoned rows cleared)
/// beside the split's range, clamped to the selection, in file order. Of an
/// evaluation that stopped early, the last split may hold only its first
/// rows (see [`split_masks_until`]).
pub(crate) struct SplitMasks(Vec<(Mask, Range<u64>)>);

impl SplitMasks {
    /// One split's evaluation: its mask beside its range.
    pub(crate) fn of_split(mask: Mask, range: Range<u64>) -> Self {
        Self(vec![(mask, range)])
    }

    /// Each evaluated split's mask beside its range, in file order.
    pub(crate) fn iter(&self) -> std::slice::Iter<'_, (Mask, Range<u64>)> {
        self.0.iter()
    }

    /// How many rows the masks keep.
    pub(crate) fn kept(&self) -> usize {
        self.0.iter().map(|(mask, _)| mask.true_count()).sum()
    }

    /// The first `want` row ids the masks keep, in file order, read off the
    /// masks' bits (no index list is built on a mask).
    pub(crate) fn first_ids(&self, want: usize) -> Vec<RowId> {
        let mut ids = Vec::with_capacity(self.kept().min(want));
        for (mask, range) in &self.0 {
            if ids.len() >= want {
                break;
            }
            push_kept_ids(&mut ids, mask, range);
        }
        ids.truncate(want);
        ids
    }

    /// Whether the masks take less memory than the ids of the rows they
    /// keep: a mask keeping some but not all of its range's rows holds a bit
    /// a row of that range, a mask keeping all or none of them holds
    /// nothing, and an id is 64 bits.
    pub(crate) fn smaller_than_ids(&self) -> bool {
        let bits: u64 = self
            .0
            .iter()
            .filter(|(mask, _)| mask.values().is_some())
            .map(|(_, range)| range.end - range.start)
            .sum();
        (self.kept() as u64).saturating_mul(64) >= bits
    }
}

/// The masks of `filter` over every split the selection touches, in file
/// order: the evaluation a window runs, run to the end.
pub(crate) async fn split_masks(
    file: &NativeStoreFile,
    filter: &PushedFilter,
    selection: &RowSelection,
    deleted: Option<&Mask>,
) -> Result<SplitMasks> {
    split_masks_until(file, filter, selection, deleted, usize::MAX).await
}

/// [`split_masks`] in file order up to the first split that brings the
/// matches to `want`: every split when fewer match, none for no `want`. A
/// fold cut into pieces ([`growing_pieces`]) stops at the piece that
/// brings them; the pieces of one split are joined into its one mask, so a
/// split evaluated whole is one mask as without pieces.
pub(crate) async fn split_masks_until(
    file: &NativeStoreFile,
    filter: &PushedFilter,
    selection: &RowSelection,
    deleted: Option<&Mask>,
    want: usize,
) -> Result<SplitMasks> {
    let mut masks: Vec<(Mask, Range<u64>)> = Vec::new();
    if want == 0 {
        return Ok(SplitMasks(masks));
    }
    let mut found = 0usize;
    let mut last_split = None;
    fold_filter_splits_ordered(
        file,
        filter,
        selection,
        deleted,
        want,
        |mask, range, split| {
            found = found.saturating_add(mask.true_count());
            match masks.last_mut() {
                // The next piece of the split the last mask holds.
                Some((last, last_range)) if last_split == Some(split) => {
                    let before = std::mem::replace(last, Mask::new_false(0));
                    *last = [before, mask].into_iter().collect();
                    last_range.end = range.end;
                }
                _ => masks.push((mask, range.clone())),
            }
            last_split = Some(split);
            found < want
        },
    )
    .await?;
    Ok(SplitMasks(masks))
}

/// Append the row ids `mask` keeps over `range` to `ids`, ascending, read
/// off its bits.
pub(crate) fn push_kept_ids(ids: &mut Vec<RowId>, mask: &Mask, range: &Range<u64>) {
    match mask.bit_buffer() {
        AllOr::All => ids.extend(range.clone()),
        AllOr::None => {}
        AllOr::Some(bits) => {
            ids.reserve(mask.true_count());
            bits.for_each_set_index(|i| ids.push(range.start + i as u64));
        }
    }
}

/// The first `want` file rows matching `filter` inside the selection, in file
/// order (the rows a window `offset + limit` deep needs), tombstoned rows
/// excluded; the splits are evaluated in order up to the first that
/// completes the count.
pub(crate) async fn first_matching_rows(
    file: &NativeStoreFile,
    filter: &PushedFilter,
    selection: &RowSelection,
    deleted: Option<&Mask>,
    want: usize,
) -> Result<Buffer<RowId>> {
    let mut ids: Vec<RowId> = Vec::new();
    if want == 0 {
        return Ok(Buffer::from(ids));
    }
    fold_filter_splits_ordered(file, filter, selection, deleted, want, |mask, range, _| {
        push_kept_ids(&mut ids, &mask, range);
        ids.len() < want
    })
    .await?;
    ids.truncate(want);
    Ok(Buffer::from(ids))
}

/// [`count_matching_rows`] stopping once `cap` matches are counted, for
/// `size_capped` and `exists`: `min(matches, cap)`.
pub(crate) async fn count_matching_rows_capped(
    file: &NativeStoreFile,
    filter: &PushedFilter,
    selection: &RowSelection,
    deleted: Option<&Mask>,
    cap: usize,
) -> Result<usize> {
    let mut count = 0usize;
    if cap == 0 {
        return Ok(0);
    }
    fold_filter_splits_ordered(file, filter, selection, deleted, cap, |mask, _, _| {
        count += mask.true_count();
        count < cap
    })
    .await?;
    Ok(count.min(cap))
}

/// The rows matching `filter` inside the selection, tombstones excluded,
/// counted from the layout reader's pruning and filter evaluations: only the
/// filter's columns are read, and no column is projected.
pub(crate) async fn count_matching_rows(
    file: &NativeStoreFile,
    filter: &PushedFilter,
    selection: &RowSelection,
    deleted: Option<&Mask>,
) -> Result<usize> {
    let counts = map_filter_splits(file, filter, selection, deleted, |mask, _| {
        mask.true_count()
    })
    .await?;
    Ok(counts.into_iter().sum())
}

/// A file view's pending filter and selection evaluated to a base-wide mask
/// of the file rows they match; with no filter the selection is exact, so its
/// rows are the answer without a scan. Tombstones are not applied: the answer
/// is the rows the pattern names, which a delete unions into its tombstones.
pub(crate) async fn matching_file_rows(
    file: &NativeStoreFile,
    filter: Option<&PushedFilter>,
    selection: &RowSelection,
) -> Result<Mask> {
    let row_count = file.row_count();
    let Some(filter) = filter else {
        return Ok(selection.to_mask(row_count as usize));
    };
    // Same per-split evaluation as the counting path, but lifting each
    // split's surviving rows back to absolute file row ids.
    let ids = map_filter_splits(file, filter, selection, None, |mask, range| -> Vec<usize> {
        match mask.indices() {
            AllOr::All => (range.start as usize..range.end as usize).collect(),
            AllOr::None => Vec::new(),
            AllOr::Some(indices) => indices.iter().map(|&i| range.start as usize + i).collect(),
        }
    })
    .await?;
    let mut matched: Vec<usize> = ids.into_iter().flatten().collect();
    matched.sort_unstable();
    Ok(Mask::from_indices(row_count as usize, matched))
}

/// The zone-map envelope of `filter`: the contiguous row range outside which
/// the file's statistics prove no row can match. It is conservative: rows
/// inside it may still fail the filter, and interior non-matching stretches
/// stay (the scan's per-split pruning skips them from the same cached zone
/// masks). No sort order is assumed.
///
/// One `pruning_evaluation` per conjunct over the whole file, as the struct
/// layout prunes single-field expressions only: the zoned layout evaluates
/// its cached zone map vectorized, and footer stats short-circuit through the
/// `FileStatsLayoutReader` wrapper. `Some(0..0)` when nothing can match,
/// `None` when the stats exclude nothing.
pub(crate) async fn row_range_from_pruning(
    file: &NativeStoreFile,
    filter: &Expression,
) -> Result<Option<Range<u64>>> {
    let row_count = file.row_count();
    // A row count that doesn't fit in usize can't back a Mask; such a file
    // is answered as "no envelope known" and the match goes on.
    let Ok(len) = usize::try_from(row_count) else {
        return Ok(None);
    };
    if len == 0 {
        return Ok(Some(0..0));
    }
    // Statistics-only over an immutable file, so the envelope is a function
    // of the filter: memoized on the shared file handle, keyed by the
    // expression itself (structural `Eq`/`Hash`), so a hit allocates nothing.
    if let Some(envelope) = file.pruning_envelope(filter) {
        return Ok(envelope);
    }

    // Every row may match until the statistics prove otherwise. Each
    // conjunct is evaluated over the whole file in one call, and binds only
    // when the pruning reaches it.
    let reader = file.layout_reader()?;
    let bound = bound_conjuncts(file, reader.as_ref(), filter);
    let mask = prune_mask(&reader, bound, &(0..row_count), Mask::new_true(len)).await?;

    // The surviving mask's enclosing range, first to last set bit; interior
    // gaps stay.
    let envelope = match (mask.first(), mask.last()) {
        (Some(first), Some(last)) => {
            let range = first as u64..last as u64 + 1;
            // Nothing trimmed: no envelope to record.
            if range == (0..row_count) {
                None
            } else {
                Some(range)
            }
        }
        // No bit survived: the filter provably matches nothing in this file.
        _ => Some(0..0),
    };
    file.memoize_pruning_envelope(filter.clone(), envelope.clone());
    Ok(envelope)
}

#[cfg(test)]
mod tests {
    use super::*;
    use vortex_array::expr::{and, eq, get_item, lit, root};

    /// A filter of code equalities orders its conjuncts by column rank, ties
    /// in the filter's order; any other filter keeps its order.
    #[test]
    fn prior_order_ranks_columns() {
        let filter = and(
            and(
                eq(get_item("s", root()), lit(1u64)),
                eq(get_item("p", root()), lit(2u64)),
            ),
            and(
                eq(get_item("o", root()), lit(3u64)),
                eq(get_item("g", root()), lit(4u64)),
            ),
        );
        assert_eq!(prior_order(&PushedFilter::new(filter), 4), vec![0, 2, 1, 3]);
        assert_eq!(prior_order(&PushedFilter::new(lit(false)), 1), vec![0]);
    }

    /// Only a fold stopping at fewer matches than the first piece's rows is
    /// cut into pieces.
    #[test]
    fn only_a_fold_short_of_the_first_piece_is_cut() {
        assert!(cut_into_pieces(1));
        assert!(cut_into_pieces(FIRST_PIECE_ROWS - 1));
        assert!(!cut_into_pieces(FIRST_PIECE_ROWS));
        assert!(!cut_into_pieces(usize::MAX));
    }

    /// The prior holds until every conjunct has been tested; from then on the
    /// sequence follows ascending observed selectivity.
    #[test]
    fn sequence_follows_observed_selectivity() {
        let order = ConjunctOrder {
            conjuncts: Vec::new(),
            prior: vec![1, 0],
            observed: Mutex::new(vec![(0, 0); 2]),
        };
        order.report(1, 100, 50);
        assert_eq!(order.sequence(), vec![1, 0]);
        order.report(0, 50, 50);
        assert_eq!(order.sequence(), vec![1, 0]);
        order.report(0, 1000, 1);
        assert_eq!(order.sequence(), vec![0, 1]);
    }
}
