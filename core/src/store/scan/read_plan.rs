//! How the rows of an exact file selection are read, chosen before any is
//! read: point by point, taken at their ids, by the pending filter's masks
//! first, or by the restricted scan ([`ExactRead`]); and how the rows a
//! filter's split masks keep are read ([`MaskedRead`]). Both weigh where the
//! ids sit among the file's splits ([`Footprint`]) against what each read
//! costs.

use std::ops::Range;

use vortex_buffer::Buffer;
use vortex_mask::Mask;

use crate::error::Result;
use crate::store::persist::native_file::NativeStoreFile;
use crate::store::scan::file_exprs::PushedFilter;
use crate::store::scan::file_filter::SplitMasks;
use crate::store::schema::RowId;
use crate::store::view::selection::{RowSelection, point_sized};

/// One point read costs about as much as a filtered scan spends on this many
/// rows: the scan's filter evaluation decodes every row of each split it
/// touches, a point read one value per column.
pub(super) const POINT_READ_ROWS: u64 = 256;

/// A take costs about as much per id as the scan's row mask spends on this
/// many rows of the splits holding the ids: the mask filters a run-end column
/// run by run over every split it touches, a take binary-searches the runs at
/// each id.
const TAKE_ROWS: u64 = 16;

/// Exact ids at most this many per split they touch are read point by point:
/// a take through the chunk probes pays a fixed cost for every leaf it takes
/// from, which reading this few ids one by one undercuts.
const POINT_IDS_PER_SPLIT: u64 = 16;

/// How the rows of an exact file selection are read, cheapest first.
pub(crate) enum ExactRead {
    /// Point by point through the chunk probes, at these live rows.
    Points(Vec<RowId>),
    /// Taken at these live ids.
    Take(Buffer<RowId>),
    /// By the pending filter's masks first, then the rows they keep
    /// ([`MaskedRead`]): a filter of code equalities over columns that all
    /// have chunk probes, at a selection the reads by id decline. A streaming
    /// reader evaluates each split's mask when it reaches the split
    /// ([`masks_first_split_reads`](crate::store::scan::file_reads::masks_first_split_reads)).
    Masks,
    /// By the restricted scan.
    Scan,
}

/// Whether reads by id can check `filter` while reading `columns` of `file`
/// point by point: the filter is none or code equalities (checked per row),
/// and every column has chunk probes. [`ExactRead`]'s condition for its
/// point reads, and, with a filter pending, for reading the filter's masks
/// first.
pub(crate) fn point_readable(
    file: &NativeStoreFile,
    columns: &[&str],
    filter: Option<&PushedFilter>,
) -> bool {
    filter.is_none_or(|f| f.eqs().is_some())
        && columns.iter().all(|c| file.column_chunks(c).is_some())
}

impl ExactRead {
    /// The read of `columns` at `selection`, exact, with `filter` pending and
    /// `deleted` tombstoned, as [`ReadKind::of`] chooses it, with the live
    /// rows a read by id needs. An id list taken with nothing tombstoned is
    /// taken at its own buffer.
    pub(crate) fn of(
        file: &NativeStoreFile,
        columns: &[&str],
        filter: Option<&PushedFilter>,
        selection: &RowSelection,
        deleted: Option<&Mask>,
    ) -> Result<Self> {
        #[cfg(test)]
        exact_read_hooks::note();
        let live = || {
            selection.filter_rows(file.row_count() as usize, move |id| {
                deleted.is_none_or(|d| !d.value(id))
            })
        };
        Ok(match ReadKind::of(file, columns, filter, selection)? {
            ReadKind::Points => ExactRead::Points(live()),
            ReadKind::Take => ExactRead::Take(match (selection, deleted) {
                (RowSelection::Ids(ids), None) => ids.clone(),
                _ => Buffer::from(live()),
            }),
            ReadKind::Masks => ExactRead::Masks,
            ReadKind::Scan => ExactRead::Scan,
        })
    }

    /// Whether [`of`](Self::of) reads `columns` at `selection` with `filter`
    /// pending by the filter's masks first: the same choice, made without
    /// building an id list (tombstones never change it). A selection with no
    /// filter, or a point-sized one, never does: it reads by id or by the
    /// scan.
    pub(crate) fn reads_masks(
        file: &NativeStoreFile,
        columns: &[&str],
        filter: Option<&PushedFilter>,
        selection: &RowSelection,
    ) -> Result<bool> {
        if filter.is_none() || selection.is_point_sized() {
            return Ok(false);
        }
        Ok(ReadKind::of(file, columns, filter, selection)? == ReadKind::Masks)
    }
}

/// Which read [`ExactRead::of`] makes, before it builds any id list.
#[derive(Clone, Copy, PartialEq, Eq)]
enum ReadKind {
    Points,
    Take,
    Masks,
    Scan,
}

impl ReadKind {
    /// The read of `columns` at `selection` with `filter` pending, cheapest
    /// first: point reads for a point-sized selection or ids that fit them
    /// ([`Footprint::points_fit`]), when they can check the filter on the
    /// columns read ([`point_readable`]); a take for ids that fit one
    /// ([`Footprint::take_fits`]) under no filter; past those, the filter's
    /// masks first for a filter the point reads can check, and the scan
    /// otherwise. Only an id list reads the file's splits.
    fn of(
        file: &NativeStoreFile,
        columns: &[&str],
        filter: Option<&PushedFilter>,
        selection: &RowSelection,
    ) -> Result<Self> {
        let pointable = point_readable(file, columns, filter);
        if pointable && selection.is_point_sized() {
            return Ok(ReadKind::Points);
        }
        if let RowSelection::Ids(ids) = selection {
            let footprint = Footprint::of(ids, &file.splits()?);
            if pointable && footprint.points_fit() {
                return Ok(ReadKind::Points);
            }
            if filter.is_none() && footprint.take_fits() {
                return Ok(ReadKind::Take);
            }
        }
        Ok(if pointable && filter.is_some() {
            ReadKind::Masks
        } else {
            ReadKind::Scan
        })
    }
}

/// Test hook: how many times this thread chose an [`ExactRead`].
#[cfg(test)]
pub(crate) mod exact_read_hooks {
    use std::cell::Cell;

    thread_local! {
        static CHOSEN: Cell<usize> = const { Cell::new(0) };
    }

    /// Record one [`ExactRead::of`](super::ExactRead::of).
    pub(super) fn note() {
        CHOSEN.with(|c| c.set(c.get() + 1));
    }

    /// The reads chosen on this thread so far.
    pub(crate) fn chosen() -> usize {
        CHOSEN.with(Cell::get)
    }
}

/// How the rows a pending filter's split masks keep are read, cheapest first,
/// once the masks have applied the filter: point by point while the kept ids
/// fit point reads, taken at their ids while they fit a take (the reads
/// [`ExactRead`] makes of exact ids under no filter; see [`Footprint`]), and
/// past that density each split projected at its own mask.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MaskedRead {
    /// Point by point through the chunk probes.
    Points,
    /// Taken at the kept ids.
    Take,
    /// Each split with a match projected at its mask.
    Project,
}

impl MaskedRead {
    /// The read of the rows `masks` keep: each evaluated split's surviving
    /// mask beside its range, clamped to the view's selection, in file order.
    /// The kept ids are weighed, as [`ExactRead`] weighs exact ids, against
    /// the natural splits holding them.
    pub(crate) fn of(file: &NativeStoreFile, masks: &SplitMasks) -> Result<Self> {
        let footprint = Footprint::of_masks(masks, &file.splits()?);
        let read = if footprint.points_fit() {
            MaskedRead::Points
        } else if footprint.take_fits() {
            MaskedRead::Take
        } else {
            MaskedRead::Project
        };
        #[cfg(test)]
        masked_read_hooks::note(read);
        Ok(read)
    }
}

/// Test hook: how many reads of a filter's kept rows this thread chose
/// ([`MaskedRead`]), by kind.
#[cfg(test)]
pub(crate) mod masked_read_hooks {
    use std::cell::Cell;

    use super::MaskedRead;

    thread_local! {
        static READS: Cell<[usize; 3]> = const { Cell::new([0; 3]) };
    }

    /// Record one masked read of kind `read`.
    pub(super) fn note(read: MaskedRead) {
        let mut reads = READS.get();
        reads[read as usize] += 1;
        READS.set(reads);
    }

    /// The masked reads chosen on this thread so far: `[points, takes,
    /// projections]`.
    pub(crate) fn reads() -> [usize; 3] {
        READS.get()
    }
}

/// `ids` (ascending) grouped by the split of `splits` (ascending, disjoint)
/// holding them, in order: each split holding any of the ids, beside the ids
/// it holds. The ids past the last split come last, as one group with no
/// split.
pub(super) fn ids_by_split<'a>(
    ids: &'a [RowId],
    splits: &'a [Range<u64>],
) -> impl Iterator<Item = (Option<&'a Range<u64>>, &'a [RowId])> {
    let (mut rest, mut idx) = (ids, 0);
    std::iter::from_fn(move || {
        let &first = rest.first()?;
        idx += splits[idx..].partition_point(|s| s.end <= first);
        let split = splits.get(idx);
        let held = split.map_or(rest.len(), |s| rest.partition_point(|&id| id < s.end));
        let (group, after) = rest.split_at(held);
        rest = after;
        idx += 1;
        Some((split, group))
    })
}

/// Where ids sit among a file's splits: how many ids, how many splits hold
/// them, and how many rows those splits have. The reads by id weigh it
/// against what scanning those splits costs.
pub(super) struct Footprint {
    ids: u64,
    splits: u64,
    rows: u64,
}

impl Footprint {
    /// The footprint of exact `ids` (ascending) among `splits`; ids past the
    /// last split hold no split and count only as ids.
    pub(super) fn of(ids: &[RowId], splits: &[Range<u64>]) -> Self {
        let held = ids_by_split(ids, splits).map_while(|(split, _)| split);
        let (touched, rows) = held.fold((0, 0), |(n, rows), s| (n + 1, rows + (s.end - s.start)));
        Footprint {
            ids: ids.len() as u64,
            splits: touched,
            rows,
        }
    }

    /// The footprint of the ids a filter's split `masks` keep among the
    /// file's natural `splits`: each mask with a match counts once, with the
    /// rows of the natural split its range starts in (its own rows past the
    /// last one).
    fn of_masks(masks: &SplitMasks, splits: &[Range<u64>]) -> Self {
        let mut footprint = Footprint {
            ids: 0,
            splits: 0,
            rows: 0,
        };
        for (mask, range) in masks.iter() {
            let kept = mask.true_count() as u64;
            if kept == 0 {
                continue;
            }
            footprint.ids += kept;
            footprint.splits += 1;
            let split = splits.partition_point(|s| s.end <= range.start);
            footprint.rows += splits
                .get(split)
                .map_or(range.end - range.start, |s| s.end - s.start);
        }
        footprint
    }

    /// Whether the ids fit point reads: point-sized, or at most
    /// [`POINT_IDS_PER_SPLIT`] per split holding them.
    fn points_fit(&self) -> bool {
        point_sized(self.ids) || self.few_per_split(POINT_IDS_PER_SPLIT)
    }

    /// Whether the ids fit a take: at most one per [`TAKE_ROWS`] rows of the
    /// splits holding them.
    fn take_fits(&self) -> bool {
        self.sparse(TAKE_ROWS)
    }

    /// Whether the ids number at most one per `rows_per_id` rows of the splits
    /// holding them: the density below which reading them by id costs less
    /// than scanning those splits ([`POINT_READ_ROWS`], [`TAKE_ROWS`]).
    pub(super) fn sparse(&self, rows_per_id: u64) -> bool {
        self.ids.saturating_mul(rows_per_id) <= self.rows
    }

    /// Whether the ids number at most `per_split` per split holding them.
    fn few_per_split(&self, per_split: u64) -> bool {
        self.ids <= per_split.saturating_mul(self.splits)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Ids are sparse while they number at most one per `POINT_READ_ROWS`
    /// rows of the splits they touch; a split holding several counts once,
    /// and an untouched split not at all.
    #[test]
    fn footprint_weighs_ids_against_the_rows_of_their_splits() {
        let rows = POINT_READ_ROWS;
        let splits = [0..4 * rows, 4 * rows..8 * rows, 8 * rows..9 * rows];
        let sparse = |ids: &[u64]| Footprint::of(ids, &splits).sparse(POINT_READ_ROWS);
        // Four ids in the first split: exactly one per `rows` of its rows.
        assert!(sparse(&[0, 1, 2, 3]));
        assert!(!sparse(&[0, 1, 2, 3, 4]));
        // A second split adds its rows once, however many ids it holds.
        assert!(sparse(&[0, 1, 4 * rows, 4 * rows + 1, 5 * rows]));
        // Only touched splits count: two ids in the small last one.
        assert!(sparse(&[8 * rows]));
        assert!(!sparse(&[8 * rows, 8 * rows + 1]));
        assert!(sparse(&[]));
        assert!(!Footprint::of(&[0], &[]).sparse(POINT_READ_ROWS));
    }

    /// A footprint counts each split holding an id once, with its rows.
    #[test]
    fn footprint_counts_touched_splits_and_their_rows() {
        let splits = [0..100, 100..300, 300..310];
        let of = |ids: &[u64]| {
            let f = Footprint::of(ids, &splits);
            (f.ids, f.splits, f.rows)
        };
        assert_eq!(of(&[]), (0, 0, 0));
        assert_eq!(of(&[5, 7, 99]), (3, 1, 100));
        assert_eq!(of(&[5, 300, 309]), (3, 2, 110));
        assert_eq!(of(&[0, 100, 300]), (3, 3, 310));
    }

    /// Ids are few per split while they number at most `POINT_IDS_PER_SPLIT`
    /// per split they touch, on average over those splits.
    #[test]
    fn footprint_weighs_ids_against_the_splits_they_touch() {
        let per = POINT_IDS_PER_SPLIT;
        let splits = [0..10 * per, 10 * per..20 * per, 20 * per..30 * per];
        let few = |ids: &[u64]| Footprint::of(ids, &splits).few_per_split(per);
        let first: Vec<u64> = (0..per).collect();
        assert!(few(&first));
        let one_more: Vec<u64> = (0..=per).collect();
        assert!(!few(&one_more));
        // A second touched split doubles the allowance.
        let two: Vec<u64> = (0..=per).chain([10 * per, 10 * per + 1]).collect();
        assert!(few(&two));
        assert!(few(&[]));
    }
}
