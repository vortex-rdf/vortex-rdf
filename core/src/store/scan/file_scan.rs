//! The file-backed half of the scan tier: per-split filter evaluation,
//! statistics-only pruning envelopes, pushed-down filter construction, row
//! and component point reads through the file's cached chunk probes, and
//! [`RowSelection::restrict_scan`]. Pure functions of a [`NativeStoreFile`]
//! and a filter expression — no store state.

use std::future::Future;
use std::ops::{BitAnd, Range};
use std::sync::Arc;

use futures::future::BoxFuture;
use futures::{FutureExt as _, StreamExt, stream};
use oxrdf::NamedOrBlankNode;
use vortex_array::expr::forms::conjuncts;
use vortex_array::expr::{BoundExpression, Expression, lit};
use vortex_array::{ArrayRef, MaskFuture};
use vortex_buffer::Buffer;
use vortex_error::VortexExpect as _;
use vortex_layout::LayoutReader;
use vortex_layout::scan::scan_builder::ScanBuilder;
use vortex_mask::{AllOr, Mask};
use vortex_scan::selection::Selection;
use vortex_scan::strict_sorted_buffer::StrictSortedBuffer;

use crate::error::{Result, VortexRdfError};
use crate::io::read::{available_parallelism, collect_scan};
use crate::store::layouts::{Constraints, PatternCodes, QuadPattern, TermRef};
use crate::store::native_file::NativeStoreFile;
use crate::store::scan::gather::primitive_from_u64_reads;
use crate::store::schema::{self, TermCode};
use crate::store::selection::RowSelection;

/// The bind-memo scope tag for expressions over the quad table's schema
/// (the transparent root the file scan reads).
pub(crate) const QUAD_SCOPE: &str = "quads";

/// Run `scan` to completion and materialize every row it yields into one
/// in-memory array, in the scan's row order.
///
/// The scan is driven inline: its splits are planned here and its split
/// futures polled on the calling task by [`collect_scan`], not spawned onto
/// the runtime as `ScanBuilder::into_array_stream` would. The files these
/// scans read are memory-mapped, so every segment is a slice of the mapped
/// file and resolves at once; the async driver would add only a spawn per
/// split and the buffers of the splits it keeps in flight (four per core
/// spawned, against one per core polled here). Measured in §7 D3 on
/// 2026-10-10: 6.5% of the BSBM 100K copy stream (17.0% of its Q3) and
/// RssAnon at exit about halved, from about 195 MB to about 93 MB; nothing
/// beyond noise on the reference-index store.
pub(crate) async fn read_all_rows(scan: ScanBuilder<ArrayRef>) -> Result<ArrayRef> {
    let dtype = scan.dtype().map_err(VortexRdfError::Vortex)?;
    let tasks = scan.build().map_err(VortexRdfError::Vortex)?;
    collect_scan(dtype, tasks).await
}

/// The rows of a point read when it answers, otherwise the rows of `scan` —
/// the fallback every point-read fast path keeps for a chunk its probes
/// decline.
pub(crate) async fn point_rows_or_scan(
    point: impl Future<Output = Result<Option<ArrayRef>>>,
    scan: ScanBuilder<ArrayRef>,
) -> Result<ArrayRef> {
    match point.await? {
        Some(rows) => Ok(rows),
        None => read_all_rows(scan).await,
    }
}

impl RowSelection {
    /// Apply this selection — and any tombstones — to a file scan.
    ///
    /// A range and an id list reach the scan through different knobs (a row
    /// range and a [`Selection`]), and the variants being exclusive is what
    /// keeps them from being set together — vortex's exact-range planning
    /// (`attempt_split_ranges`) bails out when a row range accompanies an
    /// `IncludeByIndex` selection. `All` leaves the scan's full row range in
    /// place (every file row is a quad row).
    ///
    /// Tombstoned rows are dropped inside the scan, so this composes with a
    /// pushed-down filter (whose output carries no row ids to re-align
    /// against). They ride the same `Selection`
    /// knob as an id list, so the one case where both would claim it — a sparse
    /// `Ids` selection with deletes — is resolved by subtracting the tombstones
    /// from the id list up front; `All`/`Range` leave that knob free for an
    /// `ExcludeByIndex` of the (sparse) deleted rows.
    pub(crate) fn restrict_scan<A: 'static + Send>(
        &self,
        scan: ScanBuilder<A>,
        deleted: Option<&Mask>,
    ) -> ScanBuilder<A> {
        match (self, deleted) {
            (RowSelection::All, None) => scan,
            (RowSelection::All, Some(deleted)) => {
                scan.with_selection(Selection::ExcludeByIndex(deleted_ids(deleted)))
            }
            (RowSelection::Range(range), None) => scan.with_row_range(range.clone()),
            (RowSelection::Range(range), Some(deleted)) => scan
                .with_row_range(range.clone())
                .with_selection(Selection::ExcludeByIndex(deleted_ids(deleted))),
            (RowSelection::Ids(ids), None) => scan.with_row_indices(strict_ids(ids)),
            (RowSelection::Ids(ids), Some(deleted)) => {
                scan.with_row_indices(subtract_deleted(ids, deleted))
            }
        }
    }
}

/// The set positions of a tombstone mask as an ascending id list — the sparse
/// form the scan wants for an exclusion.
fn deleted_ids(deleted: &Mask) -> StrictSortedBuffer<u64> {
    let ids = match deleted.indices() {
        AllOr::All => Buffer::from_iter(0..deleted.len() as u64),
        AllOr::None => Buffer::empty(),
        AllOr::Some(indices) => Buffer::from_iter(indices.iter().map(|&i| i as u64)),
    };
    StrictSortedBuffer::try_new(ids).vortex_expect("mask indices are ascending and unique")
}

/// An ascending id list with the tombstoned rows removed — used when a sparse
/// id selection and the deletions would both want the scan's selection knob.
fn subtract_deleted(ids: &Buffer<u64>, deleted: &Mask) -> StrictSortedBuffer<u64> {
    let ids = Buffer::from_iter(
        ids.iter()
            .copied()
            .filter(|&id| !deleted.value(id as usize)),
    );
    StrictSortedBuffer::try_new(ids).vortex_expect("a RowSelection id list is ascending and unique")
}

/// A [`RowSelection::Ids`] list as the strictly-sorted buffer the scan wants —
/// ascending and unique is that variant's construction invariant (index
/// resolutions answer in ascending unique row ids).
fn strict_ids(ids: &Buffer<u64>) -> StrictSortedBuffer<u64> {
    StrictSortedBuffer::try_new(ids.clone())
        .vortex_expect("a RowSelection id list is ascending and unique")
}

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
    let bound = filter_conjuncts;
    let mut mask = start_mask;
    // Phase 1: prune using zone-map/footer stats only — no I/O beyond the
    // cached stats tables. Each conjunct narrows the mask; stop once nothing
    // survives.
    for conjunct in bound {
        if mask.all_false() {
            return Ok(mask);
        }
        let pruned = reader
            .pruning_evaluation(range, conjunct, mask.clone())
            .map_err(VortexRdfError::Vortex)?
            .await
            .map_err(VortexRdfError::Vortex)?;
        mask = mask.bitand(&pruned);
    }
    // Phase 2: for whatever the stats couldn't rule out, read and evaluate each
    // conjunct for real, threading the narrowing mask so later conjuncts see
    // fewer rows.
    for conjunct in bound {
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
    let reader = file.layout_reader().map_err(VortexRdfError::Vortex)?;
    // Split the filter into its top-level AND-ed conditions (the struct
    // layout can only prune a single-field expression at a time) and bind
    // them through the handle's memo: one bound identity per shape, shared
    // by every split task and every later call, is what keeps vortex's
    // identity-keyed reader caches hitting (see `BoundExprMemo`).
    let filter_conjuncts: Vec<BoundExpression> = conjuncts(filter)
        .iter()
        .map(|conjunct| {
            file.bound_exprs()
                .bind(QUAD_SCOPE, conjunct, reader.dtype())
        })
        .collect::<vortex_error::VortexResult<_>>()
        .map_err(VortexRdfError::Vortex)?;
    // Translate the view's selection into the two knobs the split loop
    // understands: the bounds it iterates and the per-split starting mask
    // (see `split_start_mask`).
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

/// The positions — within the rows `selection` covers, in file order, as
/// `selection.apply` aligns them — whose code in `column` passes
/// `admit`. The column is streamed through the projected scan rather than
/// materialized: the scan reads several splits per worker ahead of this loop,
/// so that many chunks of codes are in flight at once, never the whole column.
/// Tombstones are not applied (the read paths apply them).
pub(crate) async fn column_positions(
    file: &NativeStoreFile,
    column: &'static str,
    selection: &RowSelection,
    admit: impl Fn(TermCode) -> bool,
) -> Result<Vec<usize>> {
    use vortex_array::VortexSessionExecute as _;
    use vortex_array::arrays::{PrimitiveArray, StructArray};
    use vortex_array::expr::{root, select};

    #[cfg(test)]
    file.note_column_stream();
    let mut scan = file.scan().map_err(VortexRdfError::Vortex)?;
    let scope = scan.dtype().map_err(VortexRdfError::Vortex)?;
    scan = scan.with_projection(
        file.bound_exprs()
            .bind(QUAD_SCOPE, &select(&[column][..], root()), &scope)
            .map_err(VortexRdfError::Vortex)?,
    );
    let mut chunks = Box::pin(
        selection
            .restrict_scan(scan, None)
            .into_array_stream()
            .map_err(VortexRdfError::Vortex)?,
    );
    let mut ctx = crate::session::VORTEX_SESSION.create_execution_ctx();
    let mut positions = Vec::new();
    let mut offset = 0usize;
    while let Some(chunk) = chunks.next().await {
        let chunk = chunk.map_err(VortexRdfError::Vortex)?;
        let struct_arr = chunk
            .execute::<StructArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?;
        let prim = crate::store::array::field_as::<PrimitiveArray>(&struct_arr, column, &mut ctx)?;
        let codes = prim.as_slice::<TermCode>();
        positions.extend(
            (0..codes.len())
                .filter(|&i| admit(codes[i]))
                .map(|i| offset + i),
        );
        offset += codes.len();
    }
    Ok(positions)
}

/// The pushed-down filter for a pattern under `codes`' layout: `AND` of
/// `column == code` per compiled constraint, `lit(false)` for an unmatchable
/// pattern, `None` when nothing is bound.
pub(crate) fn build_file_filter(
    pattern: QuadPattern<'_>,
    codes: &mut PatternCodes,
) -> Result<Option<Expression>> {
    Ok(match codes.constraints(pattern)? {
        Constraints::AlwaysFalse => Some(lit(false)),
        Constraints::Eq(eqs) => crate::store::indexes::row_ids::eq_conjunction(eqs),
    })
}

/// The exact row range of `subject`'s run in a sorted file, by binary search
/// over the subject column's encoded chunks. `None` declines — an unsorted
/// file, a subject without a native probe scalar (string-subject layouts, a
/// term the dictionary lacks), a column whose chunk shape has no probe — and
/// the caller keeps the scan path.
pub(crate) async fn locate_subject_run(
    file: &NativeStoreFile,
    codes: &mut PatternCodes,
    subject: &NamedOrBlankNode,
) -> Result<Option<Range<u64>>> {
    if !file.quads_sorted() {
        return Ok(None);
    }
    let Ok(Some(probe)) = codes.probe_scalar(TermRef::Subject(subject)) else {
        return Ok(None);
    };
    let Ok(needle) = u64::try_from(&probe) else {
        return Ok(None);
    };
    let Some(chunks) = file.column_chunks(schema::COL_S) else {
        return Ok(None);
    };
    chunks
        .bounds(needle, &file.segment_source(), file.session())
        .await
        .map_err(VortexRdfError::Vortex)
}

/// The subject column's chunk-probe handle for a file sorted by subject.
/// `None` declines — an unsorted file, or a column whose chunk shape has no
/// probe.
fn sorted_subject_chunks(
    file: &NativeStoreFile,
) -> Option<Arc<vortex_rdf_encoded_search::ColumnChunks>> {
    if !file.quads_sorted() {
        return None;
    }
    file.column_chunks(schema::COL_S)
}

/// The exact row range of subject `code` in the sorted subject column — empty,
/// at the insertion point, for a code the column lacks. `None` declines: a
/// chunk the search needs has no probe.
async fn subject_code_bounds(
    chunks: &vortex_rdf_encoded_search::ColumnChunks,
    file: &NativeStoreFile,
    code: TermCode,
) -> Result<Option<Range<u64>>> {
    chunks
        .bounds(code, &file.segment_source(), file.session())
        .await
        .map_err(VortexRdfError::Vortex)
}

/// The exact row range of the rows whose subject code lies in `range`
/// (`lo <= s < hi`) in a sorted file, by binary search over the subject
/// column's encoded chunks. The file is sorted by `s`, so those rows are one
/// run: it starts where the first code `>= lo` does and ends where the first
/// code `>= hi` does, and each bound is exact for a code the column lacks.
/// `None` declines — an unsorted file, a column whose chunk shape has no probe
/// — and the caller keeps the stream.
pub(crate) async fn locate_subject_code_range(
    file: &NativeStoreFile,
    range: Range<TermCode>,
) -> Result<Option<Range<u64>>> {
    let Some(chunks) = sorted_subject_chunks(file) else {
        return Ok(None);
    };
    let Some(from) = subject_code_bounds(&chunks, file, range.start).await? else {
        return Ok(None);
    };
    let Some(to) = subject_code_bounds(&chunks, file, range.end).await? else {
        return Ok(None);
    };
    Ok(Some(from.start..to.start.max(from.start)))
}

/// The rows holding each subject code of `codes` — ascending and unique, as
/// [`Keep::set`](crate::store::Keep::set) builds them — in a sorted file, as
/// ascending disjoint runs: a code without rows adds none, and codes whose
/// rows abut share one run. One binary search per code, over chunks fetched
/// once for the call. `None` declines as [`locate_subject_code_range`] does.
pub(crate) async fn locate_subject_code_runs(
    file: &NativeStoreFile,
    codes: &[TermCode],
) -> Result<Option<Vec<Range<u64>>>> {
    debug_assert!(
        codes.windows(2).all(|pair| pair[0] < pair[1]),
        "a set's codes are ascending and unique"
    );
    let Some(chunks) = sorted_subject_chunks(file) else {
        return Ok(None);
    };
    let mut runs: Vec<Range<u64>> = Vec::new();
    for &code in codes {
        let Some(run) = subject_code_bounds(&chunks, file, code).await? else {
            return Ok(None);
        };
        if run.is_empty() {
            continue;
        }
        match runs.last_mut() {
            Some(last) if last.end == run.start => last.end = run.end,
            _ => runs.push(run),
        }
    }
    Ok(Some(runs))
}

/// Rows of a small exact file selection, read point-by-point through the
/// file's cached wire-encoded chunk probes instead of a scan — the file
/// analogue of the in-memory [`gather_by_point_reads`](super::gather::gather_by_point_reads). A pushed filter of
/// equality conjuncts is applied per row during the read (an eq column's
/// surviving value is its constant, so only residual columns read twice).
/// `None` declines — a wide or non-exact selection, a filter that is not an
/// eq conjunction, a column without a probeable chunk handle — and the
/// caller keeps its scan path. Chunk fetches are cached on `file`, so warm
/// repeats touch no segments.
pub(crate) async fn file_point_rows(
    file: &NativeStoreFile,
    columns: &[&str],
    filter: Option<&Expression>,
    selection: &RowSelection,
    deleted: Option<&Mask>,
) -> Result<Option<ArrayRef>> {
    let Some(rows) = selection.point_sized_live_rows(deleted) else {
        return Ok(None);
    };
    let eqs = match filter {
        None => Vec::new(),
        Some(f) => match eq_code_pairs(f) {
            Some(pairs) => pairs,
            None => return Ok(None),
        },
    };
    let Some(handles) = columns
        .iter()
        .map(|c| file.column_chunks(c))
        .collect::<Option<Vec<_>>>()
    else {
        return Ok(None);
    };
    let source = file.segment_source();
    let session = file.session();

    // Filter first: a row survives when every eq column reads its code.
    let mut live = rows;
    for (col, code) in &eqs {
        let Some(idx) = columns.iter().position(|c| c == col) else {
            return Ok(None);
        };
        let mut kept = Vec::with_capacity(live.len());
        for &row in &live {
            match handles[idx]
                .value_at(row, &source, session)
                .await
                .map_err(VortexRdfError::Vortex)?
            {
                Some(v) if v == *code => kept.push(row),
                Some(_) => {}
                None => return Ok(None),
            }
        }
        live = kept;
        if live.is_empty() {
            break;
        }
    }

    point_read_struct(file, &handles, columns, &live, |col| {
        eqs.iter().find(|(c, _)| c == col).map(|(_, v)| *v)
    })
    .await
}

/// Read `columns` at `rows` point-by-point through their chunk-probe
/// `handles` into one canonical struct chunk. A column `constant` answers is
/// filled with that value instead of being read. `Ok(None)` declines — an
/// unprobeable chunk or a column type the canonical child cannot hold.
async fn point_read_struct(
    file: &NativeStoreFile,
    handles: &[Arc<vortex_rdf_encoded_search::ColumnChunks>],
    columns: &[&str],
    rows: &[u64],
    constant: impl Fn(&str) -> Option<u64>,
) -> Result<Option<ArrayRef>> {
    use vortex_array::IntoArray;
    use vortex_array::arrays::StructArray;
    use vortex_array::dtype::FieldName;
    use vortex_array::validity::Validity;

    let source = file.segment_source();
    let session = file.session();
    let mut children = Vec::with_capacity(columns.len());
    for (handle, col) in handles.iter().zip(columns) {
        let mut values = Vec::with_capacity(rows.len());
        match constant(col) {
            Some(v) => values.extend(std::iter::repeat_n(v, rows.len())),
            None => {
                for &row in rows {
                    match handle
                        .value_at(row, &source, session)
                        .await
                        .map_err(VortexRdfError::Vortex)?
                    {
                        Some(v) => values.push(v),
                        None => return Ok(None),
                    }
                }
            }
        }
        let Some(child) = primitive_from_u64_reads(handle.dtype().as_ptype(), values.into_iter())
        else {
            return Ok(None);
        };
        children.push(child);
    }
    let names: Vec<FieldName> = columns.iter().map(|c| FieldName::from(*c)).collect();
    Ok(Some(
        StructArray::try_new(names.into(), children, rows.len(), Validity::NonNullable)
            .map_err(VortexRdfError::Vortex)?
            .into_array(),
    ))
}

/// A located serve run's projected component columns, read point-by-point
/// through the component's cached chunk probes into one canonical
/// child-named chunk — the serve path's counterpart of [`file_point_rows`].
/// `Ok(None)` declines (an unprobeable column or chunk); the caller keeps
/// its filter scan.
pub(crate) async fn component_point_chunk(
    file: &NativeStoreFile,
    component: &str,
    columns: &[&'static str],
    range: Range<u64>,
) -> Result<Option<ArrayRef>> {
    let Some(handles) = columns
        .iter()
        .map(|c| file.component_column_chunks(component, c))
        .collect::<Option<Vec<_>>>()
    else {
        return Ok(None);
    };
    let rows: Vec<u64> = range.collect();
    point_read_struct(file, &handles, columns, &rows, |_| None).await
}

/// The `(column, code)` pairs of a filter built by [`build_file_filter`] —
/// a conjunction of `column == literal` over root fields. `None` for any
/// other shape (e.g. the `AlwaysFalse` literal), telling the caller the
/// filter cannot be applied as per-row equality tests.
pub(crate) fn eq_code_pairs(filter: &Expression) -> Option<Vec<(String, u64)>> {
    use vortex_array::scalar_fn::fns::binary::Binary;
    use vortex_array::scalar_fn::fns::get_item::GetItem;
    use vortex_array::scalar_fn::fns::literal::Literal;
    use vortex_array::scalar_fn::fns::operators::Operator;

    conjuncts(filter)
        .iter()
        .map(|c| {
            let op = c.as_opt::<Binary>()?;
            if *op != Operator::Eq {
                return None;
            }
            let field = c.child(0).as_opt::<GetItem>()?;
            let scalar = c.child(1).as_opt::<Literal>()?;
            let code = u64::try_from(scalar).ok()?;
            Some((field.to_string(), code))
        })
        .collect()
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
    // statistics (zone maps / footer stats) — no row data is read here.
    let reader = file.layout_reader().map_err(VortexRdfError::Vortex)?;
    let mut mask = Mask::new_true(len);
    for conjunct in conjuncts(filter) {
        // Once nothing can match, further conjuncts can't un-prune rows.
        if mask.all_false() {
            break;
        }
        let conjunct = file
            .bound_exprs()
            .bind(QUAD_SCOPE, &conjunct, reader.dtype())
            .map_err(VortexRdfError::Vortex)?;
        // Evaluate this conjunct's prunability over the *entire* file in one
        // call: the zoned reader vectorizes this over all its zones and the
        // file-stats wrapper checks footer-level bounds first.
        let pruned = reader
            .pruning_evaluation(&(0..row_count), &conjunct, mask.clone())
            .map_err(VortexRdfError::Vortex)?
            .await
            .map_err(VortexRdfError::Vortex)?;
        mask = mask.bitand(&pruned);
    }

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

#[cfg(test)]
mod tests {
    use super::*;
    use vortex_array::expr::{and, eq, get_item, root};

    /// Only a conjunction of `field == literal` over root fields decodes to
    /// `(column, code)` pairs; any other shape declines.
    #[test]
    fn eq_code_pairs_accepts_only_eq_conjunctions() {
        let filter = and(
            eq(get_item("p", root()), lit(3u32)),
            eq(get_item("o", root()), lit(7u32)),
        );
        assert_eq!(
            eq_code_pairs(&filter),
            Some(vec![("p".to_string(), 3), ("o".to_string(), 7)])
        );
        assert!(eq_code_pairs(&lit(false)).is_none());
        assert!(eq_code_pairs(&eq(get_item("p", root()), lit("x"))).is_none());
    }

    /// `read_all_rows` drives a scan whose rows come from several splits and
    /// hands them back whole and in the scan's row order, split after split.
    /// An order guard: it holds under the async driver and the inline one
    /// alike, which is the point of a guard (the two differ in cost, measured
    /// in §7 D3, not in result).
    #[tokio::test]
    async fn read_all_rows_keeps_order_inline() {
        use crate::io::read::{FileAccess, open_vortex_file};
        use vortex_array::VortexSessionExecute as _;
        use vortex_array::arrays::{PrimitiveArray, StructArray};
        use vortex_layout::scan::split_by::SplitBy;

        let quads = crate::tests::modular_quads_for_tests(50);
        let (_dir, path) = crate::tests::write_store_file_for_tests(quads, vec![]).await;
        let file =
            NativeStoreFile::try_new(open_vortex_file(&path, FileAccess::Mapped).await.unwrap())
                .unwrap();

        // Splits of 16 rows: four futures for the driver to run, the last short.
        let scan = || file.scan().unwrap().with_split_by(SplitBy::RowCount(16));
        assert_eq!(scan().build().unwrap().len(), 4);
        let rows = read_all_rows(scan()).await.unwrap();
        assert_eq!(rows.len(), 50);

        // The file is subject-sorted and its subjects are unique, so the
        // subject codes rise with the row order.
        let mut ctx = crate::session::VORTEX_SESSION.create_execution_ctx();
        let rows = rows.execute::<StructArray>(&mut ctx).unwrap();
        let subjects =
            crate::store::array::field_as::<PrimitiveArray>(&rows, schema::COL_S, &mut ctx)
                .unwrap();
        let codes = subjects.as_slice::<TermCode>();
        assert!(codes.windows(2).all(|pair| pair[0] < pair[1]), "{codes:?}");
    }
}
