//! Which rows of a store's base data a view covers.
//!
//! A [`VortexRdfStore`] never rewrites its data to answer a pattern: it keeps
//! the base data it was constructed from and narrows a `RowSelection` over
//! it. Everything a selection names is a *base* row id, so ids stay meaningful
//! however many times a view is refined — which is what lets secondary indexes
//! (whose components' `rid` columns address the base rows) survive
//! `match_pattern`, and what lets a matched view later be handed back for
//! mutation.
//!
//! Both backends select rows in this same currency. The three variants also
//! encode an invariant the file backend needs: a range and an id list are
//! mutually exclusive, because setting both disables vortex's exact-range
//! planning (`attempt_split_ranges` bails when a row range is also set).
//!
//! A view's selection field wraps this in [`ViewSelection`], which adds one
//! more state: *pending* — an index-served match whose exact ids are a
//! deferred computation, run by the first consumer that needs the selection
//! (serving reads never do).
//!
//! [`VortexRdfStore`]: crate::store::VortexRdfStore

use std::ops::Range;

use vortex_array::arrays::PrimitiveArray;
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray};
use vortex_buffer::Buffer;
use vortex_mask::{AllOr, Mask};

use crate::error::{Result, VortexRdfError};
use crate::store::indexes::LazyRowIds;

/// A view's base-row selection, which may still be *pending*: a match served
/// by an index left its exact ids uncomputed ([`LazyRowIds`]), because the
/// attached serving plan answers reads without them.
///
/// The two variants keep the pending state impossible to overlook: every
/// consumer either takes the serving plan (and never touches the selection)
/// or materializes here first — there is no concrete-looking value to read
/// out of a pending selection by mistake. A pending selection exists only
/// alongside `serve: Some` on its view (`QuadsSource`), and only ever
/// materializes to the id set the eager path would have produced, so
/// laziness never changes what a view covers — only when the ids are paid
/// for.
#[derive(Clone)]
pub(crate) enum ViewSelection {
    Exact(RowSelection),
    Pending(LazyRowIds),
}

impl ViewSelection {
    /// The unrefined selection — every base row.
    pub(crate) fn all() -> Self {
        ViewSelection::Exact(RowSelection::All)
    }

    /// Whether this is the unrefined whole-base selection. A pending
    /// selection is never `All`: it exists only on a view an index resolution
    /// restricted.
    pub(crate) fn is_all(&self) -> bool {
        matches!(self, ViewSelection::Exact(RowSelection::All))
    }

    /// The concrete selection, running a pending resolution's deferred id
    /// computation if one is still outstanding (cached for every later
    /// consumer and every clone of the view — see [`LazyRowIds`]) — the
    /// awaiting form, which also runs a file child's deferred scan.
    #[cfg(feature = "file-io")]
    pub(crate) async fn materialized_async(&self) -> Result<RowSelection> {
        match self {
            ViewSelection::Exact(selection) => Ok(selection.clone()),
            ViewSelection::Pending(lazy) => Ok(RowSelection::Ids(lazy.materialized_async().await?)),
        }
    }

    /// The concrete selection, running a pending resolution's deferred id
    /// computation if one is still outstanding — the synchronous form for
    /// in-memory views, whose pending ids never take I/O (see
    /// [`materialized_async`](Self::materialized_async)).
    pub(crate) fn materialized(&self) -> Result<RowSelection> {
        match self {
            ViewSelection::Exact(selection) => Ok(selection.clone()),
            ViewSelection::Pending(lazy) => Ok(RowSelection::Ids(lazy.materialized()?)),
        }
    }

    /// The already-exact selection, for consumers that structurally cannot
    /// meet a pending one: a pending selection always rides with a serve
    /// plan, and these consumers only run on views without one.
    pub(crate) fn expect_exact(&self) -> &RowSelection {
        match self {
            ViewSelection::Exact(selection) => selection,
            ViewSelection::Pending(_) => {
                unreachable!(
                    "a pending selection always rides with a serve plan; consumers that \
                     cannot honor the plan materialize the selection first"
                )
            }
        }
    }
}

/// The rows of a base array (or file) that a store view covers, as base row
/// ids. Refinements only ever narrow a selection, never re-base it.
#[derive(Clone, Debug)]
pub(crate) enum RowSelection {
    /// Every row of the base — an unrefined view.
    All,
    /// A contiguous run of base rows: what a sorted-column binary search or a
    /// zone-map envelope yields.
    Range(Range<u64>),
    /// An explicit ascending, unique list of base row ids: what a secondary
    /// index lookup or a mask scan yields.
    Ids(Buffer<u64>),
}

impl RowSelection {
    /// The canonical "matches nothing" selection.
    pub(crate) fn empty() -> Self {
        RowSelection::Range(0..0)
    }

    /// How many base rows this selection covers.
    pub(crate) fn len(&self, base_len: usize) -> usize {
        match self {
            RowSelection::All => base_len,
            RowSelection::Range(range) => {
                usize::try_from(range.end.saturating_sub(range.start)).unwrap_or(usize::MAX)
            }
            RowSelection::Ids(ids) => ids.len(),
        }
    }

    /// Whether the selection provably covers no row. `All` over an empty base
    /// counts as empty, so callers can normalize without consulting the base.
    pub(crate) fn is_empty(&self, base_len: usize) -> bool {
        self.len(base_len) == 0
    }

    /// Gather the selected rows out of `base` — the one place a view turns
    /// into physical rows. Row identity is lost in the result, so index
    /// columns must not be carried across this boundary.
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

    /// Narrow to the base rows also named by `ids` (which must be ascending
    /// and unique, as every producer of an id list here guarantees).
    pub(crate) fn intersect_ids(self, ids: Buffer<u64>) -> Self {
        match self {
            RowSelection::All => RowSelection::Ids(ids),
            RowSelection::Range(range) => RowSelection::Ids(restrict_ids(ids, &range)),
            RowSelection::Ids(current) => {
                RowSelection::Ids(intersect_sorted_ids(current.as_slice(), ids.as_slice()))
            }
        }
    }

    /// How many of this selection's rows are live — its rows minus the
    /// `deleted` tombstones among them.
    pub(crate) fn live_count(&self, deleted: Option<&Mask>, base_len: usize) -> usize {
        match deleted {
            None => self.len(base_len),
            Some(deleted) => self.live_mask(deleted, base_len).true_count(),
        }
    }

    /// The `limit` live rows after the first `offset` live ones, in this
    /// selection's (ascending base) order with the `deleted` tombstones
    /// skipped, as a selection of their own — and how many rows it holds
    /// (fewer than `limit` when the selection runs out). Without tombstones
    /// a range stays a range; with them, or over an id list, the window is
    /// an id list.
    pub(crate) fn window(
        &self,
        deleted: Option<&Mask>,
        base_len: usize,
        offset: usize,
        limit: usize,
    ) -> (RowSelection, usize) {
        if limit == 0 {
            return (RowSelection::empty(), 0);
        }
        let contiguous: Option<Range<usize>> = match self {
            RowSelection::All => Some(0..base_len),
            RowSelection::Range(range) => Some(clamped(range, base_len)),
            RowSelection::Ids(_) => None,
        };
        match (contiguous, deleted) {
            (Some(range), None) => {
                let start = range.start + offset.min(range.len());
                let end = start.saturating_add(limit).min(range.end);
                (RowSelection::Range(start as u64..end as u64), end - start)
            }
            (None, None) => {
                let RowSelection::Ids(ids) = self else {
                    unreachable!("a non-contiguous selection is an id list");
                };
                let start = offset.min(ids.len());
                let end = start.saturating_add(limit).min(ids.len());
                (RowSelection::Ids(ids.slice(start..end)), end - start)
            }
            (Some(range), Some(deleted)) => {
                let ids: Vec<u64> = range
                    .filter(|&id| !deleted.value(id))
                    .skip(offset)
                    .take(limit)
                    .map(|id| id as u64)
                    .collect();
                let taken = ids.len();
                (RowSelection::Ids(Buffer::from_iter(ids)), taken)
            }
            (None, Some(deleted)) => {
                let RowSelection::Ids(ids) = self else {
                    unreachable!("a non-contiguous selection is an id list");
                };
                let ids: Vec<u64> = ids
                    .iter()
                    .copied()
                    .filter(|&id| !deleted.value(id as usize))
                    .skip(offset)
                    .take(limit)
                    .collect();
                let taken = ids.len();
                (RowSelection::Ids(Buffer::from_iter(ids)), taken)
            }
        }
    }

    /// Whether this selection is small enough for the point-read paths
    /// (at most [`POINT_GATHER_MAX_ROWS`] rows). `All` never is.
    pub(crate) fn is_point_sized(&self) -> bool {
        match self {
            RowSelection::All => false,
            RowSelection::Range(range) => point_sized(range.end - range.start),
            RowSelection::Ids(ids) => point_sized(ids.len() as u64),
        }
    }

    /// The live base row ids of a point-sized selection, ascending, with the
    /// `deleted` tombstones dropped; `None` when the selection is not
    /// point-sized.
    pub(crate) fn point_sized_live_rows(&self, deleted: Option<&Mask>) -> Option<Vec<u64>> {
        if !self.is_point_sized() {
            return None;
        }
        let live = |id: &u64| deleted.is_none_or(|d| !d.value(*id as usize));
        Some(match self {
            RowSelection::All => unreachable!("`All` is never point-sized"),
            RowSelection::Range(range) => range.clone().filter(live).collect(),
            RowSelection::Ids(ids) => ids.iter().copied().filter(live).collect(),
        })
    }

    /// This selection as a mask over the whole base — one bit per base row.
    ///
    /// The dense counterpart of the variants above, for folding a selection
    /// into another base-wide mask (tombstoning the rows a view covers).
    pub(crate) fn to_mask(&self, base_len: usize) -> Mask {
        match self {
            RowSelection::All => Mask::new_true(base_len),
            RowSelection::Range(range) => {
                let range = clamped(range, base_len);
                // `from_slices` rejects an empty slice, and the canonical empty
                // selection (`0..0`) is exactly that.
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

    /// Which of *this selection's own rows* are not tombstoned: one bit per row
    /// of `self.apply(base)`, in that order, ready to filter the gathered rows
    /// or to be counted.
    ///
    /// The tombstone contract: deletions live in a base-wide mask, never
    /// folded into the selection, so tombstoning one row of a large store
    /// costs a bit per base row — and every read path applies this mask
    /// before handing rows out.
    pub(crate) fn live_mask(&self, deleted: &Mask, base_len: usize) -> Mask {
        match self {
            RowSelection::All => !deleted,
            RowSelection::Range(range) => !&deleted.slice(clamped(range, base_len)),
            // A sparse selection asks the mask about only the rows it names.
            RowSelection::Ids(ids) => Mask::from_indices(
                ids.len(),
                ids.iter()
                    .enumerate()
                    .filter(|(_, id)| !deleted.value(**id as usize))
                    .map(|(position, _)| position),
            ),
        }
    }

    /// Narrow using a mask over *this selection's own rows* — i.e. one bit per
    /// row of `self.apply(base)`, in that order — translating those local
    /// positions back into base row ids.
    ///
    /// This is how an evaluation that only looked at the selected rows (a mask
    /// scan over the gathered rows) folds back into a base-relative view
    /// without the store having to re-evaluate anything over the full base.
    pub(crate) fn refine(self, keep: &Mask) -> Self {
        let local = match keep.indices() {
            // Every selected row survives: the selection is unchanged.
            AllOr::All => return self,
            AllOr::None => return RowSelection::empty(),
            AllOr::Some(indices) => indices,
        };
        let ids = match &self {
            // Local positions already are base row ids.
            RowSelection::All => Buffer::from_iter(local.iter().map(|&i| i as u64)),
            // Positions are relative to the range's start.
            RowSelection::Range(range) => {
                Buffer::from_iter(local.iter().map(|&i| range.start + i as u64))
            }
            // Positions index into the id list itself.
            RowSelection::Ids(ids) => {
                let ids = ids.as_slice();
                Buffer::from_iter(local.iter().map(|&i| ids[i]))
            }
        };
        RowSelection::Ids(ids)
    }
}

/// Restrict an ascending id list to a row range (zero-copy: the surviving ids
/// are always a contiguous window of a sorted list).
fn restrict_ids(ids: Buffer<u64>, range: &Range<u64>) -> Buffer<u64> {
    let slice = ids.as_slice();
    let lo = slice.partition_point(|&id| id < range.start);
    let hi = slice.partition_point(|&id| id < range.end);
    ids.slice(lo..hi)
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

/// Intersection of two ascending id lists.
fn intersect_sorted_ids(left: &[u64], right: &[u64]) -> Buffer<u64> {
    // Classic sorted-merge intersection: advance whichever side is behind,
    // emit a value only when both sides agree on it. The result can hold at
    // most the smaller list, so one up-front reservation replaces the
    // growth-doubling reallocations.
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

/// Selection size up to which a gather reads rows point-by-point through
/// encoded search probes instead of the slice/take pipeline (see
/// [`gather_by_point_reads`](crate::store::scan::gather::gather_by_point_reads)).
/// The pipeline's cost is fixed per column
/// (optimizer pass, execution context, canonicalization) whatever the row
/// count, while point reads cost per row per column. The file-backed
/// dictionary makes the same trade: `FileBackedDict::decode_many`
/// point-reads batches of up to this many codes through the chunk leaves and
/// scans wider ones.
pub(crate) const POINT_GATHER_MAX_ROWS: usize = 256;

/// Whether `rows` rows fit the point-read paths (see
/// [`POINT_GATHER_MAX_ROWS`]).
pub(crate) fn point_sized(rows: u64) -> bool {
    rows <= POINT_GATHER_MAX_ROWS as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids(values: &[u64]) -> Buffer<u64> {
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
