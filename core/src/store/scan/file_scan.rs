//! Filter evaluation over a native store file's splits: the scan-side
//! machinery behind the store's file-backed counting, deletion, and pruning
//! paths, plus the translation of a [`RowSelection`] onto vortex's scan
//! knobs. Everything here is a pure function of a [`NativeStoreFile`] and a
//! filter expression — no store state.

use std::ops::{BitAnd, Range};
use std::sync::Arc;

use futures::{StreamExt, stream};
use oxrdf::{GraphName, NamedNode, NamedOrBlankNode, Term};
use vortex_array::expr::forms::conjuncts;
use vortex_array::expr::{Expression, and, eq, get_item, lit, root};
use vortex_array::{ArrayRef, MaskFuture};
use vortex_buffer::Buffer;
use vortex_layout::LayoutReader;
use vortex_layout::scan::scan_builder::ScanBuilder;
use vortex_mask::{AllOr, Mask};
use vortex_scan::selection::Selection;

use crate::error::{Result, VortexRdfError};
use crate::store::layouts::{Constraints, PatternCodes};
use crate::store::native_file::NativeStoreFile;
use crate::store::selection::RowSelection;

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
    /// Tombstoned rows are dropped inside the scan rather than by post-filtering
    /// its output, so this composes with a pushed-down filter (whose output
    /// carries no row ids to re-align against). They ride the same `Selection`
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
            (RowSelection::Ids(ids), None) => scan.with_row_indices(ids.clone()),
            (RowSelection::Ids(ids), Some(deleted)) => {
                scan.with_row_indices(subtract_deleted(ids, deleted))
            }
        }
    }
}

/// The set positions of a tombstone mask as an ascending id list — the sparse
/// form the scan wants for an exclusion.
fn deleted_ids(deleted: &Mask) -> Buffer<u64> {
    match deleted.indices() {
        AllOr::All => Buffer::from_iter(0..deleted.len() as u64),
        AllOr::None => Buffer::empty(),
        AllOr::Some(indices) => Buffer::from_iter(indices.iter().map(|&i| i as u64)),
    }
}

/// An ascending id list with the tombstoned rows removed — used when a sparse
/// id selection and the deletions would both want the scan's selection knob.
fn subtract_deleted(ids: &Buffer<u64>, deleted: &Mask) -> Buffer<u64> {
    Buffer::from_iter(
        ids.iter()
            .copied()
            .filter(|&id| !deleted.value(id as usize)),
    )
}

/// Split a file view's [`RowSelection`] into the two knobs the per-split filter
/// loop understands: a [`Selection`] narrowing the mask (an id list, e.g. from a
/// secondary index) and the row-id `bounds` it iterates. A `Range` narrows the
/// bounds; an `Ids` list narrows the mask; `All` narrows neither.
fn split_bounds(selection: &RowSelection, row_count: u64) -> (Selection, Range<u64>) {
    match selection {
        RowSelection::All => (Selection::All, 0..row_count),
        RowSelection::Range(range) => (Selection::All, range.clone()),
        RowSelection::Ids(ids) => (Selection::IncludeByIndex(ids.clone()), 0..row_count),
    }
}

/// The starting mask for one file split: the rows `selection` covers within
/// `range`, minus any that `deleted` has tombstoned. Returned split-relative
/// (one bit per row of `range`), ready for the store's per-split filter
/// evaluation (`evaluate_filter_split`).
fn split_start_mask(selection: &Selection, deleted: Option<&Mask>, range: &Range<u64>) -> Mask {
    let mask = selection.row_mask(range).mask().clone();
    match deleted {
        None => mask,
        Some(deleted) => {
            let live = !&deleted.slice(range.start as usize..range.end as usize);
            mask.bitand(&live)
        }
    }
}

/// The host's available parallelism, read once — split-loop concurrency is
/// derived from it on every counting/matching call, and the syscall is not
/// free on hot repeated-match paths (the bindings' `triples()` pattern).
static AVAILABLE_PARALLELISM: std::sync::LazyLock<usize> = std::sync::LazyLock::new(|| {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
});

/// Evaluate a filter over one file split, threading a narrowing mask through the
/// two phases the layout reader exposes — cheap zone-map/stats pruning first,
/// then real per-conjunct filter evaluation for whatever survives. Returns the
/// split-relative surviving mask; callers either count its set bits or lift them
/// to absolute row ids. Mirrors the filter phase of vortex's own `split_exec`.
pub(crate) async fn evaluate_filter_split(
    reader: Arc<dyn LayoutReader>,
    filter_conjuncts: &[Expression],
    range: &Range<u64>,
    start_mask: Mask,
) -> Result<Mask> {
    let mut mask = start_mask;
    // Phase 1: prune using zone-map/footer stats only — no I/O beyond the
    // cached stats tables. Each conjunct narrows the mask; stop once nothing
    // survives.
    for conjunct in filter_conjuncts {
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

/// Evaluate `filter` over every natural file split the selection touches and
/// map each split's surviving mask through `map` — the shared split loop
/// behind [`count_matching_rows`] and [`matching_file_rows`], which differ
/// only in what they do with a split's mask.
///
/// The splits are clamped to the selection's bounds, evaluated concurrently
/// (bounded by available parallelism), and returned in completion order — the
/// per-split results carry their own range when order matters. The clamped
/// ranges are owned before the task futures are built: an iterator borrowing
/// the memoized splits held across the awaits trips rustc's higher-ranked
/// lifetime inference when callers spawn the resulting future.
async fn map_filter_splits<T, F>(
    file: &NativeStoreFile,
    filter: &Expression,
    selection: &RowSelection,
    deleted: Option<&Mask>,
    map: F,
) -> Result<Vec<T>>
where
    F: Fn(Mask, &Range<u64>) -> T + Clone,
{
    // The cached layout reader tree — reused across every split task below,
    // so zone-map stats are looked up once, not once per split.
    let reader = file.layout_reader().map_err(VortexRdfError::Vortex)?;
    // Split the filter into its top-level AND-ed conditions: the struct
    // layout can only prune a single-field expression at a time.
    let filter_conjuncts = conjuncts(filter);
    // Translate the view's selection into the two knobs the split loop
    // understands: the bounds it iterates and the per-split starting mask
    // (see `split_start_mask`).
    let (row_selection, bounds) = split_bounds(selection, file.row_count());

    let splits = file.splits().map_err(VortexRdfError::Vortex)?;
    let ranges: Vec<Range<u64>> = splits
        .iter()
        .filter_map(|split| {
            let start = split.start.max(bounds.start);
            let end = split.end.min(bounds.end);
            (start < end).then_some(start..end)
        })
        .collect();
    let tasks = ranges.into_iter().map(|range| {
        let reader = Arc::clone(&reader);
        let filter_conjuncts = filter_conjuncts.clone();
        // The starting mask for this split: the selected rows within
        // `range`, minus any the caller has tombstoned.
        let start_mask = split_start_mask(&row_selection, deleted, &range);
        let map = map.clone();
        async move {
            let mask = evaluate_filter_split(reader, &filter_conjuncts, &range, start_mask).await?;
            Ok::<T, VortexRdfError>(map(mask, &range))
        }
    });

    let concurrency = *AVAILABLE_PARALLELISM * 4;
    let mut results = stream::iter(tasks).buffer_unordered(concurrency);
    let mut out = Vec::new();
    while let Some(r) = results.next().await {
        out.push(r?);
    }
    Ok(out)
}

/// Count rows matching `filter` by driving the layout reader's pruning and
/// filter evaluations directly and summing mask true-counts. A projection
/// scan would additionally decode a data column for every matching row just
/// to measure its length. No column is ever projected or decoded here.
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
    let ids = map_filter_splits(file, filter, selection, None, |mask, range| {
        let ids: Vec<usize> = match mask.indices() {
            vortex_mask::AllOr::All => (range.start as usize..range.end as usize).collect(),
            vortex_mask::AllOr::None => Vec::new(),
            vortex_mask::AllOr::Some(indices) => {
                indices.iter().map(|&i| range.start as usize + i).collect()
            }
        };
        ids
    })
    .await?;
    let mut matched: Vec<usize> = ids.into_iter().flatten().collect();
    matched.sort_unstable();
    Ok(Mask::from_indices(row_count as usize, matched))
}

/// Convert an RDF pattern (subject, predicate, object, graph) into a Vortex
/// filter expression that can be applied to a file-backed array during
/// scanning. This allows the file reader to push filters down and avoid
/// reading unnecessary data. `codes` is the match's prepared witness, which
/// carries the layout's term → constraint mapping.
pub(crate) fn build_file_filter(
    subject: Option<&NamedOrBlankNode>,
    predicate: Option<&NamedNode>,
    object: Option<&Term>,
    graph: Option<&GraphName>,
    codes: &mut PatternCodes,
) -> Result<Option<Expression>> {
    Ok(
        match codes.constraints(subject, predicate, object, graph)? {
            // If the layout determines that no rows can possibly match (e.g.,
            // asking for a term that doesn't exist in a dictionary layout),
            // return a filter that matches nothing (always evaluates to false).
            Constraints::AlwaysFalse => Some(lit(false)),

            // If the layout provides equality constraints (field_name, value
            // pairs), build a filter expression by combining them with AND
            // operations. Each constraint requires a specific column to equal a
            // specific value.
            Constraints::Eq(eqs) => {
                let mut filter: Option<Expression> = None;
                for (field, value) in eqs {
                    let expr = eq(get_item(field, root()), lit(value));
                    filter = Some(match filter.take() {
                        Some(f) => and(f, expr),
                        None => expr,
                    });
                }
                filter
            }
        },
    )
}

/// Rows of a small exact file selection, read point-by-point through the
/// file's cached wire-encoded chunk probes instead of a scan — the file
/// analogue of the in-memory `gather_by_point_reads`. A pushed filter of
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
    use vortex_array::IntoArray;
    use vortex_array::arrays::{PrimitiveArray, StructArray};
    use vortex_array::dtype::{FieldName, PType};
    use vortex_array::validity::Validity;

    use crate::store::selection::POINT_GATHER_MAX_ROWS;

    let rows: Vec<u64> = match selection {
        RowSelection::Range(r) if (r.end - r.start) as usize <= POINT_GATHER_MAX_ROWS => (r.start
            ..r.end)
            .filter(|&i| deleted.is_none_or(|d| !d.value(i as usize)))
            .collect(),
        RowSelection::Ids(ids) if ids.len() <= POINT_GATHER_MAX_ROWS => ids
            .iter()
            .copied()
            .filter(|&i| deleted.is_none_or(|d| !d.value(i as usize)))
            .collect(),
        _ => return Ok(None),
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

    let mut children = Vec::with_capacity(columns.len());
    for (idx, col) in columns.iter().enumerate() {
        let eq = eqs.iter().find(|(c, _)| c == col).map(|(_, v)| *v);
        let mut values = Vec::with_capacity(live.len());
        match eq {
            Some(v) => values.extend(std::iter::repeat_n(v, live.len())),
            None => {
                for &row in &live {
                    match handles[idx]
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
        let reads = values.into_iter();
        let child = match handles[idx].dtype().as_ptype() {
            PType::U8 => PrimitiveArray::from_iter(reads.map(|v| v as u8)).into_array(),
            PType::U16 => PrimitiveArray::from_iter(reads.map(|v| v as u16)).into_array(),
            PType::U32 => PrimitiveArray::from_iter(reads.map(|v| v as u32)).into_array(),
            PType::U64 => PrimitiveArray::from_iter(reads).into_array(),
            _ => return Ok(None),
        };
        children.push(child);
    }
    let names: Vec<FieldName> = columns.iter().map(|c| FieldName::from(*c)).collect();
    let rows_struct =
        StructArray::try_new(names.into(), children, live.len(), Validity::NonNullable)
            .map_err(VortexRdfError::Vortex)?
            .into_array();
    Ok(Some(rows_struct))
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
    use vortex_array::IntoArray;
    use vortex_array::arrays::{PrimitiveArray, StructArray};
    use vortex_array::dtype::{FieldName, PType};
    use vortex_array::validity::Validity;

    let Some(handles) = columns
        .iter()
        .map(|c| file.component_column_chunks(component, c))
        .collect::<Option<Vec<_>>>()
    else {
        return Ok(None);
    };
    let source = file.segment_source();
    let session = file.session();
    let len = (range.end - range.start) as usize;
    let mut children = Vec::with_capacity(columns.len());
    for handle in &handles {
        let mut values = Vec::with_capacity(len);
        for row in range.clone() {
            match handle
                .value_at(row, &source, session)
                .await
                .map_err(VortexRdfError::Vortex)?
            {
                Some(v) => values.push(v),
                None => return Ok(None),
            }
        }
        let reads = values.into_iter();
        let child = match handle.dtype().as_ptype() {
            PType::U8 => PrimitiveArray::from_iter(reads.map(|v| v as u8)).into_array(),
            PType::U16 => PrimitiveArray::from_iter(reads.map(|v| v as u16)).into_array(),
            PType::U32 => PrimitiveArray::from_iter(reads.map(|v| v as u32)).into_array(),
            PType::U64 => PrimitiveArray::from_iter(reads).into_array(),
            _ => return Ok(None),
        };
        children.push(child);
    }
    let names: Vec<FieldName> = columns.iter().map(|c| FieldName::from(*c)).collect();
    Ok(Some(
        StructArray::try_new(names.into(), children, len, Validity::NonNullable)
            .map_err(VortexRdfError::Vortex)?
            .into_array(),
    ))
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
    // A row count that doesn't fit in usize can't back a Mask; bail out to
    // "no envelope known" rather than fail the whole match.
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
