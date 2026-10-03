//! The file-backed filter tier: per-split filter evaluation over a view's
//! selection, the consumers that count, collect or window its surviving
//! rows, and the statistics-only pruning envelope of a filter.

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
use crate::store::view::selection::RowSelection;

/// A file view's selection as the split loop's two knobs: a [`Selection`]
/// narrowing each split's mask (an id list) and the row bounds it iterates
/// (a range). `All` narrows neither.
fn split_bounds(selection: &RowSelection, row_count: u64) -> (Selection, Range<u64>) {
    match selection {
        RowSelection::All => (Selection::All, 0..row_count),
        RowSelection::Range(range) => (Selection::All, range.clone()),
        RowSelection::Ids(ids) => (Selection::IncludeByIndex(strict_ids(ids)), 0..row_count),
    }
}

/// One split's starting mask: the rows `mask_selection` covers within
/// `range`, minus the tombstoned ones; one bit per row of `range`.
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

/// `mask` narrowed by the statistics-only pruning of each conjunct over
/// `range`, stopping once nothing survives. No row data is read.
async fn prune_mask(
    reader: &Arc<dyn LayoutReader>,
    conjuncts: &[BoundExpression],
    range: &Range<u64>,
    mut mask: Mask,
) -> Result<Mask> {
    for conjunct in conjuncts {
        if mask.all_false() {
            break;
        }
        let pruned = reader
            .pruning_evaluation(range, conjunct, mask.clone())
            .map_err(VortexRdfError::Vortex)?
            .await
            .map_err(VortexRdfError::Vortex)?;
        mask = mask.bitand(&pruned);
    }
    Ok(mask)
}

/// `filter_conjuncts` evaluated over one split: pruning by statistics first,
/// then each conjunct for real over whatever survives. Returns the
/// split-relative surviving mask.
async fn evaluate_filter_split(
    reader: Arc<dyn LayoutReader>,
    filter_conjuncts: &[BoundExpression],
    range: &Range<u64>,
    start_mask: Mask,
) -> Result<Mask> {
    let mut mask = prune_mask(&reader, filter_conjuncts, range, start_mask).await?;
    for conjunct in filter_conjuncts {
        if mask.all_false() {
            return Ok(mask);
        }
        mask = reader
            .filter_evaluation(range, conjunct, MaskFuture::ready(mask))
            .map_err(VortexRdfError::Vortex)?
            .await
            .map_err(VortexRdfError::Vortex)?;
    }
    Ok(mask)
}

/// One future per natural split the selection touches, in file order, each
/// answering its split-relative surviving mask with its range. The clamped
/// ranges are owned before the futures are built (a compiler constraint when
/// callers spawn the result).
fn filter_split_tasks(
    file: &NativeStoreFile,
    filter: &Expression,
    selection: &RowSelection,
    deleted: Option<&Mask>,
) -> Result<Vec<SplitTask>> {
    let reader = file.layout_reader().map_err(VortexRdfError::Vortex)?;
    // The struct layout prunes one field at a time, so the filter is bound
    // conjunct by conjunct.
    let filter_conjuncts: Vec<BoundExpression> = conjuncts(filter)
        .iter()
        .map(|conjunct| {
            file.bound_exprs()
                .bind(QUAD_SCOPE, conjunct, reader.dtype())
        })
        .collect::<vortex_error::VortexResult<_>>()
        .map_err(VortexRdfError::Vortex)?;
    let (mask_selection, bounds) = split_bounds(selection, file.row_count());

    let splits = file.splits().map_err(VortexRdfError::Vortex)?;
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

/// One split's filter evaluation: the surviving split-relative mask with the
/// split's range.
type SplitTask = BoxFuture<'static, Result<(Mask, Range<u64>)>>;

/// `filter` evaluated over every split the selection touches, each surviving
/// mask mapped through `map`. Splits run concurrently and return in
/// completion order.
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

/// `filter` evaluated over the selection's splits in file order, each
/// surviving mask fed to `step` until it answers `false`. A few splits are
/// evaluated ahead of the consumer; whatever is in flight past the exit is
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

/// The first `want` live file rows matching `filter` inside the selection, in
/// file order.
pub(crate) async fn first_matching_rows(
    file: &NativeStoreFile,
    filter: &Expression,
    selection: &RowSelection,
    deleted: Option<&Mask>,
    want: usize,
) -> Result<Buffer<u64>> {
    let mut ids: Vec<u64> = Vec::new();
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

/// `min(matches, cap)`: [`count_matching_rows`] stopping once `cap` rows are
/// counted.
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

/// The number of live rows matching `filter` inside the selection; no column
/// is projected or decoded.
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

/// The file rows a view's pending filter and selection name, as a base-wide
/// mask. Tombstones are not applied; without a filter the selection alone is
/// the answer.
pub(crate) async fn matching_file_rows(
    file: &NativeStoreFile,
    filter: Option<&Expression>,
    selection: &RowSelection,
) -> Result<Mask> {
    let row_count = file.row_count();
    let Some(filter) = filter else {
        return Ok(selection.to_mask(row_count as usize));
    };
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

/// The contiguous row range outside of which the file's statistics prove no
/// row matches `filter`: `Some(0..0)` when nothing can match, `None` when the
/// statistics exclude nothing. Order-agnostic; interior gaps are kept.
/// Memoized per filter shape on the handle.
pub(crate) async fn row_range_from_pruning(
    file: &NativeStoreFile,
    filter: &Expression,
) -> Result<Option<Range<u64>>> {
    let row_count = file.row_count();
    let Ok(len) = usize::try_from(row_count) else {
        return Ok(None);
    };
    if len == 0 {
        return Ok(Some(0..0));
    }
    if let Some(envelope) = file.pruning_envelope(filter) {
        return Ok(envelope);
    }

    let reader = file.layout_reader().map_err(VortexRdfError::Vortex)?;
    let bound: Vec<BoundExpression> = conjuncts(filter)
        .iter()
        .map(|conjunct| {
            file.bound_exprs()
                .bind(QUAD_SCOPE, conjunct, reader.dtype())
        })
        .collect::<vortex_error::VortexResult<_>>()
        .map_err(VortexRdfError::Vortex)?;
    let mask = prune_mask(&reader, &bound, &(0..row_count), Mask::new_true(len)).await?;

    let envelope = match (mask.first(), mask.last()) {
        (Some(first), Some(last)) => {
            let range = first as u64..last as u64 + 1;
            if range == (0..row_count) {
                None
            } else {
                Some(range)
            }
        }
        _ => Some(0..0),
    };
    file.memoize_pruning_envelope(filter.clone(), envelope.clone());
    Ok(envelope)
}
