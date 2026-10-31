//! Reads of an index child's row ids, each an ascending, unique
//! `Buffer<RowId>` of base rows: point reads through the child's cached
//! chunk probes, and rid-only pushed-down scans (by row range, or under a
//! term-equality filter). The file resolver ([`resolve_file`](super::file))
//! and the deferred ids ([`LazyRowIds`](super::LazyRowIds)) read through
//! them.

use std::ops::Range;

use vortex_array::arrays::struct_::{StructArray, StructArrayExt};
use vortex_array::scalar::Scalar;
use vortex_array::{ArrayRef, VortexSessionExecute};
use vortex_buffer::Buffer;
use vortex_layout::scan::scan_builder::ScanBuilder;

use super::COL_RID;
use super::components::{check_addressable, sorted_row_ids};
use crate::error::Result;
use crate::observe::observe;
use crate::session::VORTEX_SESSION;
#[cfg(test)]
use crate::store::persist::native_file::FileCounter;
use crate::store::persist::native_file::{ChildReader, NativeStoreFile};
use crate::store::scan::file_exprs::eq_conjunction;
use crate::store::scan::file_reads::values_at;
use crate::store::schema::RowId;
use crate::store::view::selection::point_sized;

/// The row ids of a located index-child run, read point by point from the
/// child's rid column through its cached chunk probes and re-sorted into
/// base row order: the file counterpart of slicing an in-memory rid run.
/// `Ok(None)` when the rid column's chunks decline (the caller keeps its
/// scan). Rids are unique by construction, so sorting alone suffices; a row
/// past what a `usize` holds is refused.
pub(super) async fn rid_point_reads(
    file: &NativeStoreFile,
    child: &ChildReader,
    range: Range<u64>,
) -> Result<Option<Buffer<RowId>>> {
    let Some(chunks) = file.component_column_chunks(child.name, COL_RID) else {
        return Ok(None);
    };
    // A point-read run is at most `POINT_GATHER_MAX_ROWS` wide.
    let rows: Vec<u64> = range.collect();
    let Some(mut ids) = values_at(file, &chunks, &rows).await? else {
        return Ok(None);
    };
    for id in &mut ids {
        *id = super::base_row(*id);
    }
    ids.sort_unstable();
    check_addressable(&ids)?;
    Ok(Some(Buffer::from(ids)))
}

/// The primary row ids of `child`'s rows where every `(value_column, probe)`
/// equality holds, by a rid-only scan, as an ascending, unique buffer (the
/// shape vortex's `Selection::IncludeByIndex` requires): the file probe the
/// secondary indexes share.
///
/// Each equality is a plain `eq`, as in the serve-plan filter: a binary `Eq`
/// falsifies against the same zone min/max envelope as a
/// `>= probe AND <= probe` range pair (see vortex's
/// `stats/rewrite/builtins.rs`) while evaluating a single conjunct. The
/// scan's output order does not matter, as the ids are sorted afterwards.
pub(crate) async fn scan_index_row_ids(
    child: &ChildReader,
    value_constraints: &[(&'static str, Scalar)],
) -> Result<Buffer<RowId>> {
    // Every index probes at least one value column; an empty constraint set
    // would mean "all rows", which no resolver asks for.
    let Some(filter) = eq_conjunction(value_constraints.iter().cloned()) else {
        return Ok(Buffer::empty());
    };
    let filter = child.bind(&filter)?;

    observe!(RidScan);
    read_scanned_row_ids(child.scan(&[COL_RID])?.with_filter(filter)).await
}

/// The row ids of a *located* index-child run (the rows a resolver bounded
/// by binary-searching the child's cached chunk probes), read by a rid-only
/// scan restricted to that range: the wide-run counterpart of
/// [`rid_point_reads`], for runs too large to read point by point.
///
/// The scan carries no filter: the location bounded exactly the rows the
/// constraints select. A resolver whose location covers only *some* of its
/// constraints must keep [`scan_index_row_ids`], whose filter tests them all.
async fn scan_located_row_ids(child: &ChildReader, range: Range<u64>) -> Result<Buffer<RowId>> {
    read_scanned_row_ids(child.scan(&[COL_RID])?.with_row_range(range)).await
}

/// The row ids of a located index-child run, or of a window of one, in base
/// row order: point reads through the rid column's chunk probes for a range
/// within the point-read cap, else (or when a chunk declines mid-read) a
/// rid-only scan restricted to the range, with no filter re-tested.
///
/// `range` is not empty: an empty run or window reads nothing at all, so its
/// caller answers it without asking.
pub(crate) async fn read_located_rids(
    file: &NativeStoreFile,
    child: &ChildReader,
    range: Range<u64>,
) -> Result<Buffer<RowId>> {
    debug_assert!(
        range.start < range.end,
        "an empty run or window reads nothing: its caller does not ask"
    );
    #[cfg(test)]
    file.note(
        FileCounter::LocatedRidReads,
        (range.end - range.start) as usize,
    );
    if point_sized(range.end - range.start)
        && let Some(ids) = rid_point_reads(file, child, range.clone()).await?
    {
        return Ok(ids);
    }
    #[cfg(test)]
    file.note(FileCounter::LocatedRidScans, 1);
    scan_located_row_ids(child, range).await
}

/// Run a rid-only scan and decode its row-id column into the ascending,
/// unique buffer every index resolution answers in. The scan is driven inline
/// by `file_scans::read_index_row_ids`.
async fn read_scanned_row_ids(scan: ScanBuilder<ArrayRef>) -> Result<Buffer<RowId>> {
    let arr = crate::store::scan::file_scans::read_index_row_ids(scan).await?;

    if arr.is_empty() {
        return Ok(Buffer::empty());
    }

    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let struct_arr = arr.execute::<StructArray>(&mut ctx)?;
    sorted_row_ids(struct_arr.unmasked_field_by_name(COL_RID).cloned()?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::scan::file_scans::read_all_rows;
    use vortex_array::arrays::PrimitiveArray;

    /// Both rid scans, the located run and the pushed-down equality, spawn
    /// nothing with the split limit at zero and bind through the file's memo,
    /// each shape once; a control row scan of the file spawns all of its
    /// splits.
    #[tokio::test]
    async fn index_row_id_scans_drive_inline_whatever_the_limit() {
        use crate::io::read::scan_all_reader;
        use crate::observe::{Event, Recorder};
        use crate::store::array::{field_as, into_struct_array};
        use crate::store::test_hooks::ForcedLimit;
        use crate::{IndexType, LayoutStrategy};

        let rows = 50;
        let quads = crate::tests::modular_quads(rows, 3, 4);
        let (_dir, path) = crate::tests::write_store_file(
            quads,
            LayoutStrategy::Dictionary,
            vec![IndexType::SecondaryByReference],
        )
        .await;
        let file = crate::tests::open_mapped(&path).await;
        let child = file
            .child_reader("index:ref-p")
            .unwrap()
            .expect("the reference child of the predicate column");
        let _limit = ForcedLimit::set(0);
        let mut spawned = Recorder::start();

        // The located-run scan: every row of the child.
        let ids = scan_located_row_ids(&child, 0..rows as u64).await.unwrap();
        assert_eq!(
            spawned.take_count(&Event::SplitSpawned),
            0,
            "located-run scan spawned"
        );
        assert_eq!(ids.as_slice(), (0..rows as RowId).collect::<Vec<_>>());

        // The pushed-down equality, probing the child's first value.
        let scanned = scan_all_reader(child.reader.clone()).await.unwrap();
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let scanned = into_struct_array(scanned).unwrap();
        let first = field_as::<PrimitiveArray>(&scanned, "val", &mut ctx)
            .unwrap()
            .as_slice::<u64>()[0];
        let matched = scan_index_row_ids(&child, &[("val", Scalar::from(first))])
            .await
            .unwrap();
        assert_eq!(
            spawned.take_count(&Event::SplitSpawned),
            0,
            "equality scan spawned"
        );
        assert!(!matched.is_empty() && matched.len() < rows);
        assert!(matched.as_slice().windows(2).all(|pair| pair[0] < pair[1]));
        // The rid projection both scans share, and the equality filter.
        assert_eq!(file.debug_bound_exprs(), 2);

        // Control: a row scan of the same file goes to the workers, every
        // split of it.
        let splits = file.scan().unwrap().build().unwrap().len();
        assert!(splits > 0);
        read_all_rows(file.scan().unwrap()).await.unwrap();
        assert_eq!(spawned.take_count(&Event::SplitSpawned), splits);
    }
}
