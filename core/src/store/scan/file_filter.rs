//! The file-backed filter half of the scan tier: per-split filter evaluation
//! and statistics-only pruning envelopes. Pure functions of a
//! [`NativeStoreFile`] and a filter expression — no store state.

use std::borrow::Borrow;
use std::ops::{BitAnd, Range};
use std::sync::Arc;

use futures::future::BoxFuture;
use futures::{FutureExt as _, StreamExt, stream};
use vortex_array::MaskFuture;
use vortex_array::expr::forms::conjuncts;
use vortex_array::expr::{BoundExpression, Expression};
use vortex_buffer::Buffer;
use vortex_layout::LayoutReader;
use vortex_mask::{AllOr, Mask};
use vortex_scan::selection::Selection;

use crate::error::{Result, VortexRdfError};
use crate::io::read::available_parallelism;
use crate::store::persist::native_file::NativeStoreFile;
use crate::store::scan::file_reads::{QUAD_SCOPE, strict_ids};
use crate::store::schema::RowId;
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
/// `range`, minus any that `deleted` has tombstoned. Returned split-relative
/// (one bit per row of `range`), ready for the store's per-split filter
/// evaluation (`evaluate_filter_split`).
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

/// `filter`'s top-level AND-ed conditions — the struct layout prunes one
/// single-field expression at a time — each bound against `reader`'s dtype
/// through the file's memo as the iterator reaches it: one bound identity per
/// shape, shared by every split task and every later call, is what keeps
/// vortex's identity-keyed reader caches hitting (see `BoundExprMemo`).
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

/// Narrow `mask` over `range` by each conjunct's statistics-only pruning —
/// zone maps and footer stats, no row data read — stopping once nothing
/// survives (further conjuncts can't un-prune rows). A conjunct is taken
/// from `conjuncts` only when the loop reaches it, so an iterator that binds
/// as it yields binds none past the one that left nothing. The pruning phase
/// both the per-split filter evaluation ([`evaluate_filter_split`]) and the
/// file-wide envelope ([`row_range_from_pruning`]) run.
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

/// Evaluate a filter over one file split, threading a narrowing mask through the
/// two phases the layout reader exposes — cheap zone-map/stats pruning first,
/// then real per-conjunct filter evaluation for whatever survives. Returns the
/// split-relative surviving mask; callers either count its set bits or lift them
/// to absolute row ids. Mirrors the filter phase of vortex's own `split_exec`.
async fn evaluate_filter_split(
    reader: Arc<dyn LayoutReader>,
    filter_conjuncts: &[BoundExpression],
    range: &Range<u64>,
    start_mask: Mask,
) -> Result<Mask> {
    // Phase 1: prune using zone-map/footer stats only — no I/O beyond the
    // cached stats tables ([`prune_mask`]). The conjuncts are yielded by
    // index: a closure over a borrowed item is higher-ranked, which the
    // futures holding it across the awaits cannot be proven `Send` for.
    let bound = (0..filter_conjuncts.len()).map(|i| Ok(&filter_conjuncts[i]));
    let mut mask = prune_mask(&reader, bound, range, start_mask).await?;
    // Phase 2: for whatever the stats couldn't rule out, read and evaluate each
    // conjunct for real, threading the narrowing mask so later conjuncts see
    // fewer rows.
    for conjunct in filter_conjuncts {
        if mask.all_false() {
            return Ok(mask);
        }
        mask = reader
            .filter_evaluation(range, conjunct, MaskFuture::ready(mask))?
            .await?;
    }
    Ok(mask)
}

/// The per-split filter tasks of a file view, in file order: one future per
/// natural split the selection touches, each evaluating `filter` over its
/// split (zone-map pruning first, then the conjuncts) and answering the
/// split-relative surviving mask with its range. The shared prelude of
/// [`map_filter_splits`] and [`fold_filter_splits_ordered`], which differ in
/// how they drive the tasks.
///
/// The clamped ranges are owned before the task futures are built: an
/// iterator borrowing the memoized splits held across the awaits trips
/// rustc's higher-ranked lifetime inference when callers spawn the resulting
/// future.
fn filter_split_tasks(
    file: &NativeStoreFile,
    filter: &Expression,
    selection: &RowSelection,
    deleted: Option<&Mask>,
) -> Result<Vec<SplitTask>> {
    // The cached layout reader tree — reused across every split task below,
    // so zone-map stats are looked up once, not once per split.
    let reader = file.layout_reader()?;
    let filter_conjuncts: Vec<BoundExpression> =
        bound_conjuncts(file, reader.as_ref(), filter).collect::<Result<_>>()?;
    // Translate the view's selection into the two knobs the split loop
    // understands: the bounds it iterates and the per-split starting mask
    // (see `split_start_mask`).
    let (mask_selection, bounds) = split_bounds(selection, file.row_count());

    let splits = file.splits()?;
    let ranges: Vec<Range<u64>> = splits
        .iter()
        .filter_map(|split| {
            let start = split.start.max(bounds.start);
            let end = split.end.min(bounds.end);
            (start < end).then_some(start..end)
        })
        .collect();
    Ok(ranges
        .into_iter()
        .map(|range| {
            let reader = Arc::clone(&reader);
            let filter_conjuncts = filter_conjuncts.clone();
            // The starting mask for this split: the selected rows within
            // `range`, minus any the caller has tombstoned.
            let start_mask = split_start_mask(&mask_selection, deleted, &range);
            async move {
                let mask =
                    evaluate_filter_split(reader, &filter_conjuncts, &range, start_mask).await?;
                Ok::<_, VortexRdfError>((mask, range))
            }
            .boxed()
        })
        .collect())
}

/// One split's filter evaluation, owning everything it reads: the surviving
/// split-relative mask with the split's range.
type SplitTask = BoxFuture<'static, Result<(Mask, Range<u64>)>>;

/// Evaluate `filter` over every natural file split the selection touches and
/// map each split's surviving mask through `map` — the shared split loop
/// behind [`count_matching_rows`] and [`matching_file_rows`], which differ
/// only in what they do with a split's mask.
///
/// The splits are evaluated concurrently (bounded by available parallelism)
/// and returned in completion order — the per-split results carry their own
/// range when order matters.
async fn map_filter_splits<T, F>(
    file: &NativeStoreFile,
    filter: &Expression,
    selection: &RowSelection,
    deleted: Option<&Mask>,
    map: F,
) -> Result<Vec<T>>
where
    F: Fn(Mask, &Range<u64>) -> T,
{
    let tasks = filter_split_tasks(file, filter, selection, deleted)?;
    let concurrency = available_parallelism() * 4;
    let mut results = stream::iter(tasks).buffer_unordered(concurrency);
    let mut out = Vec::new();
    while let Some(r) = results.next().await {
        let (mask, range) = r?;
        out.push(map(mask, &range));
    }
    Ok(out)
}

/// Evaluate `filter` over the selection's splits *in file order*, feeding
/// each split's surviving mask to `step` until it answers `false` — the
/// early-exit twin of [`map_filter_splits`] behind the windowed and capped
/// reads ([`first_matching_rows`], [`count_matching_rows_capped`]). A few
/// splits are evaluated ahead of the consumer so the exit stays cheap
/// without serializing the I/O; whatever was in flight past the exit is
/// dropped unread.
async fn fold_filter_splits_ordered<F>(
    file: &NativeStoreFile,
    filter: &Expression,
    selection: &RowSelection,
    deleted: Option<&Mask>,
    mut step: F,
) -> Result<()>
where
    F: FnMut(Mask, &Range<u64>) -> bool,
{
    let tasks = filter_split_tasks(file, filter, selection, deleted)?;
    let lookahead = available_parallelism().max(2);
    let mut results = stream::iter(tasks).buffered(lookahead);
    while let Some(r) = results.next().await {
        let (mask, range) = r?;
        if !step(mask, &range) {
            break;
        }
    }
    Ok(())
}

/// The first `want` file rows matching `filter` inside the selection, in file
/// order — the rows a window `offset + limit` deep needs — evaluating splits
/// in order and stopping at the first that completes the count. Tombstoned
/// rows are excluded (they would otherwise fill the window).
pub(crate) async fn first_matching_rows(
    file: &NativeStoreFile,
    filter: &Expression,
    selection: &RowSelection,
    deleted: Option<&Mask>,
    want: usize,
) -> Result<Buffer<RowId>> {
    let mut ids: Vec<RowId> = Vec::new();
    if want == 0 {
        return Ok(Buffer::from(ids));
    }
    fold_filter_splits_ordered(file, filter, selection, deleted, |mask, range| {
        match mask.indices() {
            AllOr::All => ids.extend(range.clone()),
            AllOr::None => {}
            AllOr::Some(indices) => {
                ids.extend(indices.iter().map(|&i| range.start + i as u64));
            }
        }
        ids.len() < want
    })
    .await?;
    ids.truncate(want);
    Ok(Buffer::from(ids))
}

/// [`count_matching_rows`] stopping as soon as `cap` matches are counted —
/// what `size_capped` and `exists` ask: whether (at least) that many rows
/// match, not how many. Answers `min(matches, cap)`.
pub(crate) async fn count_matching_rows_capped(
    file: &NativeStoreFile,
    filter: &Expression,
    selection: &RowSelection,
    deleted: Option<&Mask>,
    cap: usize,
) -> Result<usize> {
    let mut count = 0usize;
    if cap == 0 {
        return Ok(0);
    }
    fold_filter_splits_ordered(file, filter, selection, deleted, |mask, _| {
        count += mask.true_count();
        count < cap
    })
    .await?;
    Ok(count.min(cap))
}

/// Count rows matching `filter` by driving the layout reader's pruning and
/// filter evaluations directly and summing mask true-counts. No column is
/// ever projected or decoded.
pub(crate) async fn count_matching_rows(
    file: &NativeStoreFile,
    filter: &Expression,
    selection: &RowSelection,
    deleted: Option<&Mask>,
) -> Result<usize> {
    let counts = map_filter_splits(file, filter, selection, deleted, |mask, _| {
        mask.true_count()
    })
    .await?;
    Ok(counts.into_iter().sum())
}

/// Evaluate a file view's pending filter and selection to a base-wide mask of
/// the file rows it matches — the concrete row ids a deferred `match_pattern`
/// on a file resolves to. With no pending filter the selection alone is exact,
/// so its rows are the matches without a scan.
///
/// Tombstones are deliberately not applied: this answers "which rows does the
/// pattern name", and the caller unions the result into its existing
/// tombstones.
pub(crate) async fn matching_file_rows(
    file: &NativeStoreFile,
    filter: Option<&Expression>,
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

/// Zone-map envelope of `filter`: the contiguous row range outside of which
/// the file's statistics prove no row can match.
///
/// One `pruning_evaluation` per filter conjunct over the full file — the
/// zoned layout evaluates its cached zone map vectorized, chunks are
/// evaluated concurrently, and file-level footer stats short-circuit the
/// whole thing (the reader is wrapped in `FileStatsLayoutReader`). The
/// conjuncts are evaluated separately because the struct layout only prunes
/// single-field expressions.
///
/// The envelope is order-agnostic (no sortedness assumption) and keeps
/// interior non-matching stretches — the scan's own per-split pruning skips
/// those from the same cached zone masks.
///
/// Returns `Some(0..0)` when nothing can match and `None` when the stats
/// exclude nothing (leaving the range unset).
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
    // Statistics-only and file-immutable, so the envelope is a pure function
    // of the filter shape — memoized on the shared file handle for the
    // repeated-pattern workloads the bindings serve. Keyed by the expression
    // itself (structural `Eq`/`Hash`), so a hit allocates nothing.
    if let Some(envelope) = file.pruning_envelope(filter) {
        return Ok(envelope);
    }

    // Start from "everything might match" and narrow it down using only
    // statistics (zone maps / footer stats) — no row data is read here. Each
    // conjunct's prunability is evaluated over the *entire* file in one call:
    // the zoned reader vectorizes this over all its zones and the file-stats
    // wrapper checks footer-level bounds first. A conjunct binds only when
    // the pruning reaches it, so one that already proves nothing matches
    // leaves the rest unbound.
    let reader = file.layout_reader()?;
    let bound = bound_conjuncts(file, reader.as_ref(), filter);
    let mask = prune_mask(&reader, bound, &(0..row_count), Mask::new_true(len)).await?;

    // Collapse the surviving mask to its enclosing contiguous range: the
    // first and last set bit. Interior gaps of non-matching rows are kept
    // (the scan's own per-split pruning will skip those later using the same
    // cached zone masks) — only the outer dead space is trimmed.
    let envelope = match (mask.first(), mask.last()) {
        (Some(first), Some(last)) => {
            let range = first as u64..last as u64 + 1;
            // No trimming actually happened — leave the range unset rather
            // than recording a no-op range.
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
