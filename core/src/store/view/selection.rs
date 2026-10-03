//! Which rows of a store's base a view covers: [`RowSelection`] (base row
//! ids, ascending and unique, never re-based; a range and an id list are
//! mutually exclusive so a file scan keeps exact-range planning) and
//! [`ViewSelection`], which adds the *pending* state of an index-served match
//! whose ids the first consumer that needs them computes.

use std::ops::Range;

use vortex_array::arrays::PrimitiveArray;
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray};
use vortex_buffer::Buffer;
use vortex_mask::{AllOr, Mask};

use crate::error::{Result, VortexRdfError};
use crate::store::indexes::LazyRowIds;

/// A view's base-row selection, exact or pending. `Pending` holds an
/// index-served match's uncomputed ids ([`LazyRowIds`]); it exists only on a
/// view with `serve: Some` and materializes to the ids the eager path would
/// give. Counts, chained matches, keeps, windows, partitions, deletes and
/// base-order gathers materialize it; served reads do not.
#[derive(Clone)]
pub(crate) enum ViewSelection {
    Exact(RowSelection),
    Pending(LazyRowIds),
}

impl ViewSelection {
    /// Every base row.
    pub(crate) fn all() -> Self {
        ViewSelection::Exact(RowSelection::All)
    }

    /// Whether every base row is selected; a pending selection never is.
    pub(crate) fn is_all(&self) -> bool {
        matches!(self, ViewSelection::Exact(RowSelection::All))
    }

    /// The exact selection, running a pending resolution's deferred ids
    /// (cached on the view); a file child's deferred scan runs here.
    pub(crate) async fn materialized_async(&self) -> Result<RowSelection> {
        match self {
            ViewSelection::Exact(selection) => Ok(selection.clone()),
            ViewSelection::Pending(lazy) => Ok(RowSelection::Ids(lazy.materialized_async().await?)),
        }
    }

    /// The exact selection, running a pending resolution's deferred ids
    /// synchronously; valid only for in-memory views.
    pub(crate) fn materialized(&self) -> Result<RowSelection> {
        match self {
            ViewSelection::Exact(selection) => Ok(selection.clone()),
            ViewSelection::Pending(lazy) => Ok(RowSelection::Ids(lazy.materialized()?)),
        }
    }

    /// The live rows this selection covers, when known without reading: an
    /// exact selection's live count, or a pending run's width while nothing
    /// is tombstoned.
    pub(crate) fn len_if_known(&self, deleted: Option<&Mask>, base_len: usize) -> Option<usize> {
        match self {
            ViewSelection::Exact(selection) => Some(selection.live_len(deleted, base_len)),
            ViewSelection::Pending(lazy) => {
                deleted.is_none().then(|| lazy.len_if_known()).flatten()
            }
        }
    }
}

/// The base rows a view covers, as base row ids; refinements narrow, never
/// re-base.
#[derive(Clone, Debug)]
pub(crate) enum RowSelection {
    /// Every row of the base.
    All,
    /// A contiguous run of base rows (a sorted-column binary search, a
    /// zone-map envelope).
    Range(Range<u64>),
    /// An ascending, unique list of base row ids (an index lookup, a mask
    /// scan).
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

    /// The live rows this selection covers: its rows minus the tombstones.
    pub(crate) fn live_len(&self, deleted: Option<&Mask>, base_len: usize) -> usize {
        match deleted {
            None => self.len(base_len),
            Some(deleted) => self.live_mask(deleted, base_len).true_count(),
        }
    }

    /// The base row ids this selection covers, ascending. Not for hot
    /// per-row loops: those stay monomorphic per variant.
    pub(crate) fn ids(&self, base_len: usize) -> impl Iterator<Item = u64> + '_ {
        let (range, ids): (Range<u64>, &[u64]) = match self {
            RowSelection::All => (0..base_len as u64, &[]),
            RowSelection::Range(range) => {
                let range = clamped(range, base_len);
                (range.start as u64..range.end as u64, &[])
            }
            RowSelection::Ids(ids) => (0..0, ids.as_slice()),
        };
        range.chain(ids.iter().copied())
    }

    /// [`ids`](Self::ids) with the tombstoned rows dropped.
    pub(crate) fn live_ids<'a>(
        &'a self,
        deleted: Option<&'a Mask>,
        base_len: usize,
    ) -> impl Iterator<Item = u64> + 'a {
        self.ids(base_len)
            .filter(move |&id| deleted.is_none_or(|d| !d.value(id as usize)))
    }

    /// `Ids(ids)`, or the canonical empty selection for an empty list.
    pub(crate) fn from_ids(ids: Buffer<u64>) -> Self {
        if ids.is_empty() {
            RowSelection::empty()
        } else {
            RowSelection::Ids(ids)
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

    /// This selection cut into at most `n` contiguous pieces of about equal
    /// size, in base row order; empty pieces are left out.
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
                .map(|start| RowSelection::Range(start..(start + per).min(range.end)))
                .collect()
        }
        match self {
            RowSelection::All => ranges(0..base_len as u64, n),
            RowSelection::Range(range) => ranges(range.clone(), n),
            RowSelection::Ids(ids) => {
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

    /// Narrow to the base rows also named by `ids` (ascending and unique).
    pub(crate) fn intersect_ids(self, ids: Buffer<u64>) -> Self {
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
        let Some(deleted) = deleted else {
            return match self {
                RowSelection::All => {
                    let start = offset.min(base_len);
                    let end = start.saturating_add(limit).min(base_len);
                    Self::range_or_empty(start as u64..end as u64)
                }
                RowSelection::Range(range) => {
                    let range = clamped(range, base_len);
                    let start = range.start.saturating_add(offset).min(range.end);
                    let end = start.saturating_add(limit).min(range.end);
                    Self::range_or_empty(start as u64..end as u64)
                }
                RowSelection::Ids(ids) => {
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
        Self::from_ids(Buffer::from_iter(
            self.live_ids(Some(deleted), base_len)
                .skip(offset)
                .take(limit),
        ))
    }

    /// `Range(range)`, or the canonical empty selection for an empty one.
    fn range_or_empty(range: Range<u64>) -> Self {
        if range.end <= range.start {
            RowSelection::empty()
        } else {
            RowSelection::Range(range)
        }
    }

    /// Whether the selection has at most [`POINT_GATHER_MAX_ROWS`] rows;
    /// `All` never does.
    pub(crate) fn is_point_sized(&self) -> bool {
        match self {
            RowSelection::All => false,
            RowSelection::Range(range) => point_sized(range.end - range.start),
            RowSelection::Ids(ids) => point_sized(ids.len() as u64),
        }
    }

    /// The live base row ids of a point-sized selection, ascending; `None`
    /// when the selection is not point-sized.
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

    /// This selection as a mask over the whole base, one bit per base row.
    pub(crate) fn to_mask(&self, base_len: usize) -> Mask {
        match self {
            RowSelection::All => Mask::new_true(base_len),
            RowSelection::Range(range) => {
                let range = clamped(range, base_len);
                // `from_slices` rejects an empty slice.
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
    /// `deleted` mask, never in the selection, and every read applies them.
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
    /// back to base row ids.
    pub(crate) fn refine(self, keep: &Mask) -> Self {
        let local = match keep.indices() {
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

/// An ascending id list restricted to `range`: a zero-copy slice.
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

/// Intersection of two ascending id lists, by sorted merge.
fn intersect_sorted_ids(left: &[u64], right: &[u64]) -> Buffer<u64> {
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
/// ([`gather_by_point_reads`](crate::store::scan::gather::gather_by_point_reads));
/// also the batch size the file-backed dictionary point-reads
/// (`FileBackedDict::decode_many`).
pub(crate) const POINT_GATHER_MAX_ROWS: usize = 256;

/// Whether `rows` rows are within [`POINT_GATHER_MAX_ROWS`].
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
