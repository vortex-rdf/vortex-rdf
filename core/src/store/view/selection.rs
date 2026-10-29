//! Which base rows a view covers, on both backends: [`RowSelection`], base
//! row ids that refinements narrow and never re-base, so the index
//! components' `rid` columns keep addressing them; and [`ViewSelection`],
//! which adds the *pending* state of an index-resolved match whose ids the
//! first consumer that needs them computes (serving reads never do, nor does
//! a count of a located run).
//!
//! A range and an id list are mutually exclusive: a file scan given both
//! loses vortex's exact-range planning (`attempt_split_ranges` bails when a
//! row range is also set).

use std::ops::Range;

use vortex_array::arrays::PrimitiveArray;
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray};
use vortex_buffer::Buffer;
use vortex_mask::{AllOr, Mask};

use crate::error::{Result, VortexRdfError};
use crate::store::indexes::LazyRowIds;
use crate::store::schema::RowId;

/// A view's base-row selection, exact or *pending*: an index resolution's
/// uncomputed ids ([`LazyRowIds`]). A pending selection rides with a serve
/// plan that reads without the ids (`serve: Some` on its `QuadsSource`), or
/// sits alone over a located run of known width on a view built only to be
/// counted or windowed (`IdsNeed::CountOrWindow`), which is counted,
/// windowed or kept and never streamed. Every consumer either reads through
/// the plan or materializes first, and a pending selection materializes to
/// the ids the eager path would have produced, so laziness changes only
/// when the ids are paid for, never what a view covers.
#[derive(Clone)]
pub(crate) enum ViewSelection {
    Exact(RowSelection),
    Pending(LazyRowIds),
}

impl ViewSelection {
    pub(crate) fn all() -> Self {
        ViewSelection::Exact(RowSelection::All)
    }

    /// Whether every base row is selected. A pending selection never is: it
    /// exists only on a view an index resolution restricted.
    pub(crate) fn is_all(&self) -> bool {
        matches!(self, ViewSelection::Exact(RowSelection::All))
    }

    /// The exact selection, computing a pending selection's ids on first need
    /// (cached across the view's clones, see [`LazyRowIds`]); the awaiting
    /// form, which also runs a file child's read.
    #[cfg(feature = "file-io")]
    pub(crate) async fn materialized_async(&self) -> Result<RowSelection> {
        match self {
            ViewSelection::Exact(selection) => Ok(selection.clone()),
            ViewSelection::Pending(lazy) => Ok(RowSelection::Ids(lazy.materialized_async().await?)),
        }
    }

    /// The exact selection; the synchronous form, for in-memory views, whose
    /// pending ids take no I/O (see
    /// [`materialized_async`](Self::materialized_async)).
    pub(crate) fn materialized(&self) -> Result<RowSelection> {
        match self {
            ViewSelection::Exact(selection) => Ok(selection.clone()),
            ViewSelection::Pending(lazy) => Ok(RowSelection::Ids(lazy.materialized()?)),
        }
    }

    /// The live rows this selection covers out of `base_len`, when known
    /// without I/O: an exact selection's rows less the `deleted` tombstones;
    /// a pending run's width while nothing is tombstoned
    /// ([`LazyRowIds::len_if_known`]); under tombstones, its ids less them
    /// when they take no I/O ([`LazyRowIds::ids_without_io`]). An in-memory
    /// run's ids are decoded and cached here, the one way this fails. Passed
    /// no tombstones, it counts the selection's rows, a bound on its live ones.
    pub(crate) fn len_if_known(
        &self,
        deleted: Option<&Mask>,
        base_len: usize,
    ) -> Result<Option<usize>> {
        Ok(match (self, deleted) {
            (ViewSelection::Exact(selection), _) => Some(selection.live_len(deleted, base_len)),
            (ViewSelection::Pending(lazy), None) => lazy.len_if_known(),
            (ViewSelection::Pending(lazy), Some(deleted)) => lazy
                .ids_without_io()?
                .map(|ids| RowSelection::Ids(ids).live_len(Some(deleted), base_len)),
        })
    }

    /// The exact selection, for consumers that never meet a pending one: a
    /// pending selection rides with a serve plan or on a view that is only
    /// counted or windowed, and these consumers run on neither. Unreachable
    /// on a pending selection.
    pub(crate) fn expect_exact(&self) -> &RowSelection {
        match self {
            ViewSelection::Exact(selection) => selection,
            ViewSelection::Pending(_) => {
                unreachable!(
                    "a pending selection rides with a serve plan or on a count/window view; \
                     consumers that stream rows only ever see an exact one"
                )
            }
        }
    }
}

/// The base rows a view covers, as base row ids; refinements narrow, never
/// re-base.
#[derive(Clone, Debug)]
pub(crate) enum RowSelection {
    All,
    /// A contiguous run of base rows (a sorted-column binary search, a
    /// zone-map envelope).
    Range(Range<u64>),
    /// An ascending, unique list of base row ids (an index lookup, a mask
    /// scan).
    Ids(Buffer<RowId>),
}

impl RowSelection {
    /// The canonical "matches nothing" selection.
    pub(crate) fn empty() -> Self {
        RowSelection::Range(0..0)
    }

    /// `Ids(ids)`, or the canonical empty selection for an empty list.
    pub(crate) fn from_ids(ids: Buffer<RowId>) -> Self {
        if ids.is_empty() {
            RowSelection::empty()
        } else {
            RowSelection::Ids(ids)
        }
    }

    pub(crate) fn len(&self, base_len: usize) -> usize {
        match self {
            RowSelection::All => base_len,
            RowSelection::Range(range) => {
                usize::try_from(range.end.saturating_sub(range.start)).unwrap_or(usize::MAX)
            }
            RowSelection::Ids(ids) => ids.len(),
        }
    }

    /// How many base rows this selection covers that the `deleted`
    /// tombstones leave live.
    pub(crate) fn live_len(&self, deleted: Option<&Mask>, base_len: usize) -> usize {
        match deleted {
            None => self.len(base_len),
            Some(deleted) => {
                #[cfg(test)]
                live_count_hooks::note();
                self.live_mask(deleted, base_len).true_count()
            }
        }
    }

    /// Whether the selection covers no row; `All` over an empty base counts
    /// as empty.
    pub(crate) fn is_empty(&self, base_len: usize) -> bool {
        self.len(base_len) == 0
    }

    /// The selected rows of `base`. Row identity is lost, so index columns
    /// must not cross this boundary.
    pub(crate) fn apply(&self, base: &ArrayRef) -> Result<ArrayRef> {
        match self {
            RowSelection::All => Ok(base.clone()),
            RowSelection::Range(range) => base
                .slice(clamped(range, base.len()))
                .map_err(VortexRdfError::Vortex),
            RowSelection::Ids(ids) => {
                let indices = PrimitiveArray::new(ids.clone(), Validity::NonNullable).into_array();
                base.take(indices).map_err(VortexRdfError::Vortex)
            }
        }
    }

    /// This selection cut into at most `max(n, 1)` contiguous pieces of about
    /// equal size, in base row order: sub-ranges of a range or of every row,
    /// slices of an id list. A selection of no row has no piece.
    pub(crate) fn split(&self, n: usize, base_len: usize) -> Vec<RowSelection> {
        let n = n.max(1);
        fn ranges(range: Range<u64>, n: usize) -> Vec<RowSelection> {
            let len = range.end.saturating_sub(range.start);
            if len == 0 {
                return Vec::new();
            }
            let per = len.div_ceil(n as u64);
            (range.start..range.end)
                .step_by(usize::try_from(per).unwrap_or(usize::MAX))
                .map(|start| RowSelection::Range(start..start.saturating_add(per).min(range.end)))
                .collect()
        }
        match self.as_rows(base_len) {
            SelectedRows::Run(run) => ranges(run.start as u64..run.end as u64, n),
            SelectedRows::Ids(ids) => {
                let len = ids.len();
                if len == 0 {
                    return Vec::new();
                }
                let per = len.div_ceil(n);
                (0..len)
                    .step_by(per)
                    .map(|start| RowSelection::Ids(ids.slice(start..(start + per).min(len))))
                    .collect()
            }
        }
    }

    /// Narrow to the base rows also covered by `range`.
    pub(crate) fn intersect_range(self, range: Range<u64>) -> Self {
        match self {
            RowSelection::All => RowSelection::Range(range),
            RowSelection::Range(current) => {
                // The overlap, clamped to a non-negative width when disjoint.
                let start = current.start.max(range.start);
                let end = current.end.min(range.end);
                RowSelection::Range(start..end.max(start))
            }
            RowSelection::Ids(ids) => RowSelection::Ids(restrict_ids(ids, &range)),
        }
    }

    /// Narrow to the base rows also named by `ids`, which must be ascending
    /// and unique.
    pub(crate) fn intersect_ids(self, ids: Buffer<RowId>) -> Self {
        match self {
            RowSelection::All => RowSelection::Ids(ids),
            RowSelection::Range(range) => RowSelection::Ids(restrict_ids(ids, &range)),
            RowSelection::Ids(current) => {
                RowSelection::Ids(intersect_sorted_ids(current.as_slice(), ids.as_slice()))
            }
        }
    }

    /// The `limit` live rows after skipping `offset`, in base row order, as
    /// base row ids: a sub-run of a range, a slice of an id list, or the
    /// surviving ids when there are tombstones to step over.
    pub(crate) fn window(
        &self,
        offset: usize,
        limit: usize,
        deleted: Option<&Mask>,
        base_len: usize,
    ) -> Self {
        let rows = self.as_rows(base_len);
        let Some(deleted) = deleted else {
            return match rows {
                SelectedRows::Run(run) => {
                    let start = run.start.saturating_add(offset).min(run.end);
                    let end = start.saturating_add(limit).min(run.end);
                    Self::range_or_empty(start as u64..end as u64)
                }
                SelectedRows::Ids(ids) => {
                    let start = offset.min(ids.len());
                    let end = start.saturating_add(limit).min(ids.len());
                    if start == end {
                        RowSelection::empty()
                    } else {
                        RowSelection::Ids(ids.slice(start..end))
                    }
                }
            };
        };
        let live = |id: &RowId| !deleted.value(*id as usize);
        let ids: Buffer<RowId> = match rows {
            SelectedRows::Run(run) => Buffer::from_iter(
                (run.start as u64..run.end as u64)
                    .filter(live)
                    .skip(offset)
                    .take(limit),
            ),
            SelectedRows::Ids(ids) => {
                Buffer::from_iter(ids.iter().copied().filter(live).skip(offset).take(limit))
            }
        };
        Self::from_ids(ids)
    }

    /// `Range(range)`, or the canonical empty selection for an empty one.
    fn range_or_empty(range: Range<u64>) -> Self {
        if range.end <= range.start {
            RowSelection::empty()
        } else {
            RowSelection::Range(range)
        }
    }

    /// Whether the selection has at most [`POINT_GATHER_MAX_ROWS`] rows, the
    /// point-read paths' size; `All` never does.
    pub(crate) fn is_point_sized(&self) -> bool {
        match self {
            RowSelection::All => false,
            RowSelection::Range(range) => point_sized(range.end - range.start),
            RowSelection::Ids(ids) => point_sized(ids.len() as u64),
        }
    }

    /// The base row ids this selection covers out of `base_len` that `keep`
    /// admits, ascending: one loop per shape of its rows ([`SelectedRows`]),
    /// each compiled for the caller's own `keep`.
    #[inline]
    pub(crate) fn filter_rows(&self, base_len: usize, keep: impl Fn(usize) -> bool) -> Vec<u64> {
        match self.as_rows(base_len) {
            SelectedRows::Run(run) => (run.start as u64..run.end as u64)
                .filter(|&i| keep(i as usize))
                .collect(),
            SelectedRows::Ids(ids) => ids.iter().copied().filter(|&i| keep(i as usize)).collect(),
        }
    }

    /// The rows this selection covers out of `base_len`, in the shape its
    /// readers consume: every row as one run, a range clamped to the base
    /// as one run, an id list as itself.
    pub(crate) fn as_rows(&self, base_len: usize) -> SelectedRows<'_> {
        match self {
            RowSelection::All => SelectedRows::Run(0..base_len),
            RowSelection::Range(range) => SelectedRows::Run(clamped(range, base_len)),
            RowSelection::Ids(ids) => SelectedRows::Ids(ids),
        }
    }

    /// The live base row ids of a point-sized selection out of `base_len`,
    /// ascending, with the `deleted` tombstones dropped; `None` when the
    /// selection is not point-sized.
    pub(crate) fn point_sized_live_rows(
        &self,
        deleted: Option<&Mask>,
        base_len: usize,
    ) -> Option<Vec<u64>> {
        self.is_point_sized()
            .then(|| self.filter_rows(base_len, move |id| deleted.is_none_or(|d| !d.value(id))))
    }

    /// This selection as a mask over the whole base, one bit per base row, to
    /// fold into another base-wide mask (the tombstones).
    pub(crate) fn to_mask(&self, base_len: usize) -> Mask {
        match self {
            RowSelection::All => Mask::new_true(base_len),
            RowSelection::Range(range) => {
                let range = clamped(range, base_len);
                // `from_slices` rejects an empty slice, which the canonical
                // empty selection (`0..0`) is.
                if range.is_empty() {
                    return Mask::new_false(base_len);
                }
                Mask::from_slices(base_len, vec![(range.start, range.end)])
            }
            RowSelection::Ids(ids) => {
                Mask::from_indices(base_len, ids.iter().map(|&id| id as usize))
            }
        }
    }

    /// Which of this selection's own rows are not tombstoned: one bit per row
    /// of `self.apply(base)`, in that order. Tombstones live in the base-wide
    /// `deleted` mask (a bit per base row), never in the selection, and every
    /// read applies them through this mask before handing rows out.
    pub(crate) fn live_mask(&self, deleted: &Mask, base_len: usize) -> Mask {
        match self {
            RowSelection::All => !deleted,
            RowSelection::Range(range) => !&deleted.slice(clamped(range, base_len)),
            RowSelection::Ids(ids) => Mask::from_indices(
                ids.len(),
                ids.iter()
                    .enumerate()
                    .filter(|(_, id)| !deleted.value(**id as usize))
                    .map(|(position, _)| position),
            ),
        }
    }

    /// Narrow by a mask over this selection's own rows (one bit per row of
    /// `self.apply(base)`, in that order), mapping the surviving positions
    /// back to base row ids: a mask scan over the gathered rows folds back
    /// into the view without re-evaluating the base.
    pub(crate) fn refine(self, keep: &Mask) -> Self {
        let local = match keep.indices() {
            AllOr::All => return self,
            AllOr::None => return RowSelection::empty(),
            AllOr::Some(indices) => indices,
        };
        let ids = match &self {
            RowSelection::All => Buffer::from_iter(local.iter().map(|&i| i as u64)),
            RowSelection::Range(range) => {
                Buffer::from_iter(local.iter().map(|&i| range.start + i as u64))
            }
            RowSelection::Ids(ids) => {
                let ids = ids.as_slice();
                Buffer::from_iter(local.iter().map(|&i| ids[i]))
            }
        };
        RowSelection::Ids(ids)
    }

    /// Narrow to `ids`, the ascending base rows of this selection's `len`
    /// that passed a test: every row passing leaves the selection as it is,
    /// a contiguous stretch of every row or of a range stays a range (no id
    /// list kept), and other survivors are an id list; an id list stays one
    /// even over a contiguous stretch.
    #[cfg(feature = "file-io")]
    pub(crate) fn narrow_to_ids(self, ids: Vec<RowId>, len: usize) -> Self {
        let (Some(&first), Some(&last)) = (ids.first(), ids.last()) else {
            return RowSelection::empty();
        };
        if ids.len() == len {
            return self;
        }
        let contiguous = last - first + 1 == ids.len() as u64;
        match self {
            RowSelection::All | RowSelection::Range(_) if contiguous => {
                RowSelection::Range(first..last + 1)
            }
            _ => RowSelection::Ids(Buffer::from(ids)),
        }
    }

    /// Narrow to the base rows also covered by any of `runs` (ascending,
    /// disjoint ranges, as a sorted column's per-code runs are). One surviving
    /// run is a [`Range`](RowSelection::Range) however many were offered, so
    /// a keep admitting one contiguous run builds no id list; several become
    /// an id list, and an id list stays one.
    pub(crate) fn intersect_runs(self, runs: &[Range<u64>]) -> Self {
        match self {
            RowSelection::Ids(ids) => {
                // Each run keeps one window of the ascending id list, and the
                // runs ascend, so the windows follow each other.
                let slice = ids.as_slice();
                let mut windows: Vec<Range<usize>> = Vec::new();
                let mut from = 0;
                for run in runs {
                    let lo = from + slice[from..].partition_point(|&id| id < run.start);
                    let hi = lo + slice[lo..].partition_point(|&id| id < run.end);
                    if lo < hi {
                        windows.push(lo..hi);
                    }
                    from = hi;
                }
                match windows.as_slice() {
                    [] => RowSelection::empty(),
                    [window] => RowSelection::Ids(ids.slice(window.clone())),
                    windows => RowSelection::Ids(Buffer::from_iter(
                        windows
                            .iter()
                            .flat_map(|window| slice[window.clone()].iter().copied()),
                    )),
                }
            }
            selection => {
                let within = match &selection {
                    RowSelection::Range(range) => range.clone(),
                    _ => 0..u64::MAX,
                };
                let clipped: Vec<Range<u64>> = runs
                    .iter()
                    .map(|run| run.start.max(within.start)..run.end.min(within.end))
                    .filter(|run| run.start < run.end)
                    .collect();
                match clipped.as_slice() {
                    [] => RowSelection::empty(),
                    [run] => RowSelection::Range(run.clone()),
                    runs => RowSelection::Ids(Buffer::from_iter(
                        runs.iter().flat_map(|run| run.clone()),
                    )),
                }
            }
        }
    }
}

/// An ascending id list restricted to `range`: a zero-copy slice, as the
/// survivors are a contiguous window of the list.
fn restrict_ids(ids: Buffer<RowId>, range: &Range<RowId>) -> Buffer<RowId> {
    let slice = ids.as_slice();
    let lo = slice.partition_point(|&id| id < range.start);
    let hi = slice.partition_point(|&id| id < range.end);
    ids.slice(lo..hi)
}

/// A selection's base rows ([`RowSelection::as_rows`]): one run of base row
/// positions, or an ascending id list, borrowed so a slice of it stays a
/// refcount bump.
pub(crate) enum SelectedRows<'a> {
    /// Every base row of this run, in order.
    Run(Range<usize>),
    /// The base rows these ids name, ascending and unique.
    Ids(&'a Buffer<RowId>),
}

/// A base row range as `usize` positions, clamped to `base_len` (an
/// out-of-range end lands on the base's end; an inverted range is empty).
fn clamped(range: &Range<u64>, base_len: usize) -> Range<usize> {
    let start = usize::try_from(range.start)
        .unwrap_or(usize::MAX)
        .min(base_len);
    let end = usize::try_from(range.end)
        .unwrap_or(usize::MAX)
        .min(base_len);
    start..end.max(start)
}

/// Intersection of two ascending id lists, by sorted merge.
fn intersect_sorted_ids(left: &[RowId], right: &[RowId]) -> Buffer<RowId> {
    let mut i = 0usize;
    let mut j = 0usize;
    let mut out = Vec::with_capacity(left.len().min(right.len()));
    while i < left.len() && j < right.len() {
        use std::cmp::Ordering;
        match left[i].cmp(&right[j]) {
            Ordering::Less => i += 1,
            Ordering::Greater => j += 1,
            Ordering::Equal => {
                out.push(left[i]);
                i += 1;
                j += 1;
            }
        }
    }
    Buffer::from(out)
}

/// Selection size up to which a gather reads rows point by point through
/// encoded-search probes instead of the slice/take pipeline
/// ([`gather_by_point_reads`](crate::store::scan::gather::gather_by_point_reads)):
/// the pipeline costs a fixed amount per column (optimizer pass, execution
/// context, canonicalization) whatever the row count, point reads a little
/// per row per column.
pub(crate) const POINT_GATHER_MAX_ROWS: usize = 256;

/// Whether `rows` rows fit the point-read paths (see
/// [`POINT_GATHER_MAX_ROWS`]).
pub(crate) fn point_sized(rows: u64) -> bool {
    rows <= POINT_GATHER_MAX_ROWS as u64
}

/// Test hook: how many live counts under tombstones this thread made
/// ([`live_len`](crate::store::view::selection::RowSelection::live_len),
/// which builds the selection's live mask to count it).
#[cfg(test)]
pub(crate) mod live_count_hooks {
    use std::cell::Cell;

    thread_local! {
        static COUNTS: Cell<usize> = const { Cell::new(0) };
    }

    /// Record one live count under tombstones.
    pub(super) fn note() {
        COUNTS.set(COUNTS.get() + 1);
    }

    /// The live counts under tombstones this thread made so far.
    pub(crate) fn counts() -> usize {
        COUNTS.get()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids(values: &[RowId]) -> Buffer<RowId> {
        Buffer::from_iter(values.iter().copied())
    }

    /// The set bits of a mask, whichever representation it happens to hold.
    fn set_bits(mask: &Mask) -> Vec<usize> {
        match mask.indices() {
            AllOr::All => (0..mask.len()).collect(),
            AllOr::None => vec![],
            AllOr::Some(indices) => indices.to_vec(),
        }
    }

    fn as_vec(selection: &RowSelection) -> Vec<u64> {
        match selection {
            RowSelection::Ids(ids) => ids.as_slice().to_vec(),
            RowSelection::Range(range) => range.clone().collect(),
            RowSelection::All => panic!("All has no explicit ids"),
        }
    }

    #[test]
    fn intersect_range_narrows() {
        assert_eq!(
            as_vec(&RowSelection::All.intersect_range(2..5)),
            vec![2, 3, 4]
        );
        assert_eq!(
            as_vec(&RowSelection::Range(1..8).intersect_range(4..20)),
            vec![4, 5, 6, 7]
        );
        assert_eq!(
            as_vec(&RowSelection::Ids(ids(&[1, 4, 9, 12])).intersect_range(4..10)),
            vec![4, 9]
        );
        // Disjoint ranges collapse to empty rather than an inverted range.
        assert!(RowSelection::Range(1..3).intersect_range(7..9).is_empty(10));
    }

    #[test]
    #[allow(
        clippy::single_range_in_vec_init,
        reason = "the runs are a slice of ranges, and these cases offer just one"
    )]
    fn intersect_runs_keeps_one_run_a_range() {
        // Several runs over every row: ids, in order.
        assert_eq!(
            as_vec(&RowSelection::All.intersect_runs(&[1..3, 6..7, 9..11])),
            vec![1, 2, 6, 9, 10]
        );
        // A range clips the runs to itself, and one survivor is a range.
        let clipped = RowSelection::Range(2..8).intersect_runs(&[0..3, 6..12]);
        assert_eq!(as_vec(&clipped), vec![2, 6, 7]);
        let one = RowSelection::Range(4..8).intersect_runs(&[0..3, 6..12, 20..30]);
        assert!(matches!(one, RowSelection::Range(ref r) if *r == (6..8)));
        let single = RowSelection::All.intersect_runs(&[5..9]);
        assert!(matches!(single, RowSelection::Range(ref r) if *r == (5..9)));
        // Nothing survives: the canonical empty selection.
        assert!(
            RowSelection::Range(0..2)
                .intersect_runs(&[5..9])
                .is_empty(10)
        );
        assert!(RowSelection::All.intersect_runs(&[]).is_empty(10));
        // An id list stays an id list, windowed by each run.
        let list = RowSelection::Ids(ids(&[1, 4, 5, 9, 12, 20]));
        assert_eq!(
            as_vec(&list.clone().intersect_runs(&[0..2, 4..6, 10..13])),
            vec![1, 4, 5, 12]
        );
        assert_eq!(
            as_vec(&list.clone().intersect_runs(&[4..10])),
            vec![4, 5, 9]
        );
        assert!(matches!(
            list.clone().intersect_runs(&[4..10]),
            RowSelection::Ids(_)
        ));
        assert!(list.clone().intersect_runs(&[6..8, 14..19]).is_empty(30));
        assert!(list.intersect_runs(&[]).is_empty(30));
    }

    #[test]
    fn narrow_to_ids_keeps_a_contiguous_stretch_a_range() {
        // A contiguous stretch of every row, or of a range, is a range.
        let stretch = RowSelection::All.narrow_to_ids(vec![3, 4, 5], 10);
        assert!(matches!(stretch, RowSelection::Range(ref r) if *r == (3..6)));
        let within = RowSelection::Range(10..20).narrow_to_ids(vec![13, 14, 15], 10);
        assert!(matches!(within, RowSelection::Range(ref r) if *r == (13..16)));
        // Gaps make ids; an id list stays one even over a contiguous stretch.
        assert_eq!(
            as_vec(&RowSelection::Range(10..20).narrow_to_ids(vec![11, 12, 15], 10)),
            vec![11, 12, 15]
        );
        let list = RowSelection::Ids(ids(&[2, 4, 5, 6, 8])).narrow_to_ids(vec![4, 5, 6], 5);
        assert!(matches!(list, RowSelection::Ids(_)));
        assert_eq!(as_vec(&list), vec![4, 5, 6]);
        // Every row surviving changes nothing; none survives empties it.
        assert!(matches!(
            RowSelection::Range(10..14).narrow_to_ids(vec![10, 11, 12, 13], 4),
            RowSelection::Range(ref r) if *r == (10..14)
        ));
        assert!(matches!(
            RowSelection::All.narrow_to_ids(vec![0, 1, 2], 3),
            RowSelection::All
        ));
        assert!(RowSelection::All.narrow_to_ids(vec![], 4).is_empty(4));
    }

    #[test]
    fn intersect_ids_narrows() {
        assert_eq!(
            as_vec(&RowSelection::All.intersect_ids(ids(&[3, 7]))),
            vec![3, 7]
        );
        assert_eq!(
            as_vec(&RowSelection::Range(2..6).intersect_ids(ids(&[1, 3, 5, 9]))),
            vec![3, 5]
        );
        assert_eq!(
            as_vec(&RowSelection::Ids(ids(&[1, 3, 5, 9])).intersect_ids(ids(&[3, 4, 9]))),
            vec![3, 9]
        );
    }

    #[test]
    fn to_mask_covers_the_selected_base_rows() {
        assert!(RowSelection::All.to_mask(3).all_true());
        assert_eq!(set_bits(&RowSelection::Range(1..3).to_mask(4)), vec![1, 2]);
        assert_eq!(
            set_bits(&RowSelection::Ids(ids(&[0, 3])).to_mask(4)),
            vec![0, 3]
        );
        // The canonical empty selection is an empty range, which the mask
        // builder would otherwise reject outright.
        assert!(RowSelection::empty().to_mask(4).all_false());
    }

    #[test]
    fn live_mask_excludes_tombstoned_rows() {
        // Base rows 1 and 2 are deleted, of 5.
        let deleted = Mask::from_indices(5, [1, 2]);

        // Over the whole base, the live rows are 0, 3, 4.
        assert_eq!(
            set_bits(&RowSelection::All.live_mask(&deleted, 5)),
            vec![0, 3, 4]
        );
        // Over rows 1..4, positions 0 and 1 (base 1 and 2) are gone, leaving
        // position 2 (base 3).
        assert_eq!(
            set_bits(&RowSelection::Range(1..4).live_mask(&deleted, 5)),
            vec![2]
        );
        // Of ids [0, 2, 4], the middle one is tombstoned.
        assert_eq!(
            set_bits(&RowSelection::Ids(ids(&[0, 2, 4])).live_mask(&deleted, 5)),
            vec![0, 2]
        );
    }

    #[test]
    fn window_slices_every_variant() {
        assert_eq!(
            as_vec(&RowSelection::All.window(2, 3, None, 10)),
            vec![2, 3, 4]
        );
        assert_eq!(
            as_vec(&RowSelection::All.window(8, 5, None, 10)),
            vec![8, 9]
        );
        assert!(RowSelection::All.window(12, 5, None, 10).is_empty(10));
        assert!(RowSelection::All.window(0, 0, None, 10).is_empty(10));
        assert_eq!(
            as_vec(&RowSelection::Range(5..9).window(1, 2, None, 10)),
            vec![6, 7]
        );
        assert_eq!(
            as_vec(&RowSelection::Range(5..9).window(3, 10, None, 10)),
            vec![8]
        );
        assert_eq!(
            as_vec(&RowSelection::Ids(ids(&[1, 4, 6, 9])).window(1, 2, None, 10)),
            vec![4, 6]
        );
        assert!(
            RowSelection::Ids(ids(&[1, 4]))
                .window(2, 2, None, 10)
                .is_empty(10)
        );
        // Tombstoned rows are stepped over, not counted.
        let deleted = Mask::from_indices(10, [2, 3, 6]);
        assert_eq!(
            as_vec(&RowSelection::All.window(1, 3, Some(&deleted), 10)),
            vec![1, 4, 5]
        );
        assert_eq!(
            as_vec(&RowSelection::Range(2..8).window(0, 2, Some(&deleted), 10)),
            vec![4, 5]
        );
        assert_eq!(
            as_vec(&RowSelection::Ids(ids(&[2, 5, 6, 7])).window(1, 5, Some(&deleted), 10)),
            vec![7]
        );
        // A selection of no row splits into no piece: the empty range, and
        // an empty id list (what narrowing an id list to a disjoint range
        // leaves).
        assert!(RowSelection::empty().split(4, 10).is_empty());
        let disjoint = RowSelection::Ids(ids(&[1, 4])).intersect_range(5..9);
        assert!(matches!(&disjoint, RowSelection::Ids(ids) if ids.is_empty()));
        assert!(disjoint.split(4, 10).is_empty());
    }

    #[test]
    fn refine_maps_local_positions_to_base_ids() {
        // Local positions 0 and 2 of the whole base are base rows 0 and 2.
        let keep = Mask::from_indices(4, [0, 2]);
        assert_eq!(as_vec(&RowSelection::All.refine(&keep)), vec![0, 2]);

        // Of rows 10..14, local 0 and 2 are base rows 10 and 12.
        assert_eq!(
            as_vec(&RowSelection::Range(10..14).refine(&keep)),
            vec![10, 12]
        );

        // Of ids [5, 6, 8, 11], local 0 and 2 are base rows 5 and 8.
        assert_eq!(
            as_vec(&RowSelection::Ids(ids(&[5, 6, 8, 11])).refine(&keep)),
            vec![5, 8]
        );

        // An all-true mask keeps the selection as-is (no id list is built).
        assert!(matches!(
            RowSelection::Range(10..14).refine(&Mask::new_true(4)),
            RowSelection::Range(r) if r == (10..14)
        ));
        assert!(RowSelection::All.refine(&Mask::new_false(4)).is_empty(4));
    }
}
