//! Which rows of a store's base data a view covers.
//!
//! A [`VortexRdfStore`] never rewrites its data to answer a pattern: it keeps
//! the base data it was constructed from and narrows a `RowSelection` over
//! it. Everything a selection names is a *base* row id, so ids stay meaningful
//! however many times a view is refined — which is what lets secondary indexes
//! (whose `_idx_*_rid` columns address the base rows) survive `match_pattern`,
//! and what lets a matched view later be handed back for mutation.
//!
//! Both backends select rows in this same currency. The three variants also
//! encode an invariant the file backend needs: a range and an id list are
//! mutually exclusive, because setting both disables vortex's exact-range
//! planning (`attempt_split_ranges` bails when a row range is also set).
//!
//! [`VortexRdfStore`]: crate::store::vortex_rdf_store::VortexRdfStore

use std::ops::Range;

use vortex_array::arrays::PrimitiveArray;
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray};
use vortex_buffer::Buffer;
#[cfg(feature = "file-io")]
use vortex_layout::scan::scan_builder::ScanBuilder;
use vortex_mask::{AllOr, Mask};
#[cfg(feature = "file-io")]
use vortex_scan::selection::Selection;

use crate::error::{Result, VortexRdfError};

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
            RowSelection::Range(range) => {
                let start = usize::try_from(range.start)
                    .unwrap_or(usize::MAX)
                    .min(base.len());
                let end = usize::try_from(range.end)
                    .unwrap_or(usize::MAX)
                    .min(base.len());
                base.slice(start..end).map_err(VortexRdfError::Vortex)
            }
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
                RowSelection::Ids(intersect_ids(current.as_slice(), ids.as_slice()))
            }
        }
    }

    /// This selection as a mask over the whole base — one bit per base row.
    ///
    /// The dense counterpart of the variants above, for folding a selection
    /// into another base-wide mask (tombstoning the rows a view covers).
    pub(crate) fn to_mask(&self, base_len: usize) -> Mask {
        match self {
            RowSelection::All => Mask::new_true(base_len),
            RowSelection::Range(range) => {
                let start = usize::try_from(range.start)
                    .unwrap_or(usize::MAX)
                    .min(base_len);
                let end = usize::try_from(range.end)
                    .unwrap_or(usize::MAX)
                    .min(base_len);
                // `from_slices` rejects an empty slice, and the canonical empty
                // selection (`0..0`) is exactly that.
                if start >= end {
                    return Mask::new_false(base_len);
                }
                Mask::from_slices(base_len, vec![(start, end)])
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
    /// Deletions live in a base-wide mask rather than being folded into the
    /// selection, so that tombstoning a single row of a large store costs a bit
    /// per row instead of an explicit id per surviving row. The price is that
    /// every read path has to apply this.
    pub(crate) fn live_mask(&self, deleted: &Mask, base_len: usize) -> Mask {
        match self {
            RowSelection::All => !deleted,
            RowSelection::Range(range) => {
                let start = usize::try_from(range.start)
                    .unwrap_or(usize::MAX)
                    .min(base_len);
                let end = usize::try_from(range.end)
                    .unwrap_or(usize::MAX)
                    .min(base_len);
                !&deleted.slice(start..end)
            }
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

    /// Apply this selection — and any tombstones — to a file scan.
    ///
    /// A range and an id list reach the scan through different knobs (a row
    /// range and a [`Selection`]), and the variants being exclusive is what
    /// keeps them from being set together — vortex's exact-range planning
    /// (`attempt_split_ranges`) bails out when a row range accompanies an
    /// `IncludeByIndex` selection.
    ///
    /// Tombstoned rows are dropped inside the scan rather than by post-filtering
    /// its output, so this composes with a pushed-down filter (whose output
    /// carries no row ids to re-align against). They ride the same `Selection`
    /// knob as an id list, so the one case where both would claim it — a sparse
    /// `Ids` selection with deletes — is resolved by subtracting the tombstones
    /// from the id list up front; `All`/`Range` leave that knob free for an
    /// `ExcludeByIndex` of the (sparse) deleted rows.
    #[cfg(feature = "file-io")]
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

/// The set positions of a tombstone mask as an ascending id list — the sparse
/// form the scan wants for an exclusion.
#[cfg(feature = "file-io")]
fn deleted_ids(deleted: &Mask) -> Buffer<u64> {
    match deleted.indices() {
        AllOr::All => Buffer::from_iter(0..deleted.len() as u64),
        AllOr::None => Buffer::empty(),
        AllOr::Some(indices) => Buffer::from_iter(indices.iter().map(|&i| i as u64)),
    }
}

/// An ascending id list with the tombstoned rows removed — used when a sparse
/// id selection and the deletions would both want the scan's selection knob.
#[cfg(feature = "file-io")]
fn subtract_deleted(ids: &Buffer<u64>, deleted: &Mask) -> Buffer<u64> {
    Buffer::from_iter(
        ids.iter()
            .copied()
            .filter(|&id| !deleted.value(id as usize)),
    )
}

/// Intersection of two ascending id lists.
fn intersect_ids(left: &[u64], right: &[u64]) -> Buffer<u64> {
    // Classic sorted-merge intersection: advance whichever side is behind,
    // emit a value only when both sides agree on it.
    let mut i = 0usize;
    let mut j = 0usize;
    let mut out = Vec::new();
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
    Buffer::from_iter(out)
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
