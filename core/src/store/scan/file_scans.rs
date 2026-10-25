//! Row scans of a [`NativeStoreFile`]: the driver a scan's splits run
//! under, a scan read whole or streamed, an index child's row-id scan,
//! [`RowSelection::restrict_scan`], and the projected column stream a keep
//! tests.

use std::future::Future;

use futures::stream::BoxStream;
use futures::{StreamExt, future};
use vortex_array::ArrayRef;
use vortex_array::dtype::DType;
use vortex_array::expr::{root, select};
use vortex_buffer::Buffer;
use vortex_error::{VortexExpect as _, VortexResult};
use vortex_layout::scan::scan_builder::ScanBuilder;
use vortex_mask::{AllOr, Mask};
use vortex_scan::selection::Selection;
use vortex_scan::strict_sorted_buffer::StrictSortedBuffer;

use crate::error::{Result, VortexRdfError};
use crate::io::read::{
    available_parallelism, collect_scan, collect_scan_spawned, collect_whole_scan, inline_splits,
    spawned_splits,
};
use crate::store::persist::native_file::NativeStoreFile;
use crate::store::scan::file_exprs::QUAD_SCOPE;
use crate::store::schema::{RowId, TermCode};
use crate::store::view::selection::RowSelection;

/// How a scan's split futures are driven.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ScanDriver {
    /// Polled on the calling task ([`inline_splits`]).
    Inline,
    /// Spawned onto the runtime's workers ([`spawned_splits`]).
    Spawned,
}

impl ScanDriver {
    /// The driver for a row scan of `splits` split futures, the one rule
    /// [`read_all_rows`] and [`stream_rows`] share: inline up to
    /// [`MAX_INLINE_SPLITS`], spawned above it, and inline whatever the count
    /// on a thread with no tokio runtime, which has nothing to spawn onto.
    pub(crate) fn for_row_scan(splits: usize) -> Self {
        if splits <= max_inline_splits() || tokio::runtime::Handle::try_current().is_err() {
            ScanDriver::Inline
        } else {
            ScanDriver::Spawned
        }
    }
}

/// Row scans of up to this many splits run inline: at that size, waking the
/// workers costs more than the parallel work saves.
const MAX_INLINE_SPLITS: usize = 42;

/// The limit [`ScanDriver::for_row_scan`] applies: [`MAX_INLINE_SPLITS`], or
/// what a test forced on this thread.
fn max_inline_splits() -> usize {
    #[cfg(test)]
    if let Some(limit) = driver_hooks::forced_limit() {
        return limit;
    }
    MAX_INLINE_SPLITS
}

/// Test hooks for the driver choice: a per-thread override of the split limit,
/// and the driver a scan ran under, read off how many of its splits were
/// spawned.
#[cfg(test)]
pub(crate) mod driver_hooks {
    use std::cell::Cell;

    use super::ScanDriver;

    thread_local! {
        static LIMIT: Cell<Option<usize>> = const { Cell::new(None) };
    }

    /// The limit forced on this thread, if any.
    pub(super) fn forced_limit() -> Option<usize> {
        LIMIT.get()
    }

    /// Forces the split limit to `limit` on this thread until dropped.
    pub(crate) struct ForcedLimit {
        previous: Option<usize>,
    }

    impl ForcedLimit {
        pub(crate) fn set(limit: usize) -> Self {
            Self {
                previous: LIMIT.replace(Some(limit)),
            }
        }
    }

    impl Drop for ForcedLimit {
        fn drop(&mut self) {
            LIMIT.set(self.previous);
        }
    }

    /// The splits spawned on this thread since the last call.
    pub(crate) fn take_spawned() -> usize {
        crate::io::read::spawn_probe::take()
    }

    /// The driver a scan of `splits` split futures ran under, given how many
    /// of them were spawned: none is the inline driver, all of them the
    /// spawned one. Anything between is neither, and fails the test.
    pub(crate) fn driver_of(splits: usize, spawned: usize) -> ScanDriver {
        match spawned {
            0 => ScanDriver::Inline,
            n if n == splits => ScanDriver::Spawned,
            n => panic!("{n} of {splits} splits were spawned: neither driver"),
        }
    }
}

/// Test hook: how many row scans this thread ran, collected whole
/// ([`read_all_rows`]) or streamed ([`stream_rows`], the quad decode's scan
/// stream); a read by id ([`ExactRead`]) runs none.
#[cfg(test)]
pub(crate) mod scan_hooks {
    use std::cell::Cell;

    thread_local! {
        static SCANS: Cell<usize> = const { Cell::new(0) };
    }

    /// Record one row scan.
    pub(crate) fn note() {
        SCANS.with(|c| c.set(c.get() + 1));
    }

    /// The row scans run on this thread so far.
    pub(crate) fn scans() -> usize {
        SCANS.with(Cell::get)
    }
}

/// Every row a row scan yields, in one in-memory array in the scan's row
/// order. [`ScanDriver::for_row_scan`] picks the driver from the split count,
/// so the splits are planned here: inline, [`collect_scan`] polls the split
/// futures on the calling task; spawned, [`collect_scan_spawned`] runs them
/// on the runtime's workers, `scan.concurrency()` splits per core in flight.
/// Either returns the chunks in split order, whatever the scan's `ordered`
/// flag says. Row splits do filter and decode work, so a large scan needs
/// the workers; an index child's row-id scan goes through
/// [`read_index_row_ids`] instead.
pub(crate) async fn read_all_rows(scan: ScanBuilder<ArrayRef>) -> Result<ArrayRef> {
    #[cfg(test)]
    scan_hooks::note();
    let dtype = scan.dtype()?;
    let window = scan.concurrency() * available_parallelism();
    collect_row_splits(dtype, scan.build()?, window).await
}

/// The rows of a row scan's split futures, in one array in split order,
/// driven as [`ScanDriver::for_row_scan`] picks for their count: polled on
/// the calling task by [`collect_scan`], or spawned by
/// [`collect_scan_spawned`], `window` of them in flight.
pub(crate) async fn collect_row_splits<F>(
    dtype: DType,
    tasks: Vec<F>,
    window: usize,
) -> Result<ArrayRef>
where
    F: Future<Output = VortexResult<Option<ArrayRef>>> + Send + 'static,
{
    match ScanDriver::for_row_scan(tasks.len()) {
        ScanDriver::Inline => collect_scan(dtype, tasks).await,
        ScanDriver::Spawned => collect_scan_spawned(dtype, tasks, window).await,
    }
}

/// [`read_all_rows`]' rows as a stream of the scan's chunks: one chunk per
/// split that keeps a row, in split order, the splits that keep none
/// dropped, and a split's error as one error item.
///
/// The splits are planned here, so a planning error is the `Err`; the
/// driver is [`ScanDriver::for_row_scan`]'s, as for [`read_all_rows`]:
/// [`inline_splits`] polls the split futures as the stream is polled,
/// [`spawned_splits`] spawns them, `scan.concurrency()` per core in flight.
pub(crate) fn stream_rows(
    scan: ScanBuilder<ArrayRef>,
) -> Result<BoxStream<'static, Result<ArrayRef>>> {
    #[cfg(test)]
    scan_hooks::note();
    let window = scan.concurrency() * available_parallelism();
    let tasks = scan.build()?;
    let splits = match ScanDriver::for_row_scan(tasks.len()) {
        ScanDriver::Inline => inline_splits(tasks).boxed(),
        ScanDriver::Spawned => spawned_splits(tasks, window).boxed(),
    };
    Ok(splits
        .filter_map(|split| future::ready(split.map_err(VortexRdfError::Vortex).transpose()))
        .boxed())
}

/// An index child's row-id scan run to completion, its rows in one array,
/// always inline whatever its split count: zone maps prune almost every
/// split of such a scan, and a located run is sliced out of the row-id
/// column, so there is no work to spread. A scan is an index scan because
/// its caller says so here, not by its `ordered` flag; any other scan goes
/// through [`read_all_rows`].
pub(crate) async fn read_index_row_ids(scan: ScanBuilder<ArrayRef>) -> Result<ArrayRef> {
    collect_whole_scan(scan).await
}

impl RowSelection {
    /// This selection and the `deleted` tombstones applied to a file scan.
    ///
    /// A range sets the scan's row range and an id list its [`Selection`];
    /// the variants are exclusive, as vortex's exact-range planning
    /// (`attempt_split_ranges`) bails when a row range accompanies an
    /// `IncludeByIndex` selection. `All` keeps the full row range (every file
    /// row is a quad row).
    ///
    /// Tombstoned rows are dropped inside the scan, so this composes with a
    /// pushed-down filter, whose output carries no row ids to realign
    /// against. Under `All` or a range they are an `ExcludeByIndex`
    /// selection; an id list, which would claim the same knob, has them
    /// subtracted up front.
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

/// The set positions of a tombstone mask as an ascending id list, the sparse
/// form the scan wants for an exclusion.
fn deleted_ids(deleted: &Mask) -> StrictSortedBuffer<RowId> {
    let ids = match deleted.indices() {
        AllOr::All => Buffer::from_iter(0..deleted.len() as RowId),
        AllOr::None => Buffer::empty(),
        AllOr::Some(indices) => Buffer::from_iter(indices.iter().map(|&i| i as RowId)),
    };
    StrictSortedBuffer::try_new(ids).vortex_expect("mask indices are ascending and unique")
}

/// An ascending id list with the tombstoned rows removed, for when an id
/// selection and the tombstones would both claim the scan's selection knob.
fn subtract_deleted(ids: &Buffer<RowId>, deleted: &Mask) -> StrictSortedBuffer<RowId> {
    let ids = Buffer::from_iter(
        ids.iter()
            .copied()
            .filter(|&id| !deleted.value(id as usize)),
    );
    StrictSortedBuffer::try_new(ids).vortex_expect("a RowSelection id list is ascending and unique")
}

/// A [`RowSelection::Ids`] list as the strictly sorted buffer the scan wants:
/// ascending and unique is that variant's invariant.
pub(super) fn strict_ids(ids: &Buffer<RowId>) -> StrictSortedBuffer<RowId> {
    StrictSortedBuffer::try_new(ids.clone())
        .vortex_expect("a RowSelection id list is ascending and unique")
}

/// The positions within the rows `selection` covers (in file order, as
/// `selection.apply` aligns them) whose code in `column` passes `admit`. The
/// column is streamed through a projected scan, which reads a few splits per
/// worker ahead of this loop, so a few chunks of codes are in flight at once,
/// never the whole column. Tombstones are not applied (the reads apply them).
pub(crate) async fn column_positions(
    file: &NativeStoreFile,
    column: &'static str,
    selection: &RowSelection,
    admit: impl Fn(TermCode) -> bool,
) -> Result<Vec<usize>> {
    use vortex_array::VortexSessionExecute as _;
    use vortex_array::arrays::{PrimitiveArray, StructArray};

    #[cfg(test)]
    file.note_column_stream();
    let mut scan = file.scan()?;
    let scope = scan.dtype()?;
    scan = scan.with_projection(file.bound_exprs().bind(
        QUAD_SCOPE,
        &select(&[column][..], root()),
        &scope,
    )?);
    let mut chunks = Box::pin(selection.restrict_scan(scan, None).into_array_stream()?);
    let mut ctx = crate::session::VORTEX_SESSION.create_execution_ctx();
    let mut positions = Vec::new();
    let mut offset = 0usize;
    while let Some(chunk) = chunks.next().await {
        let chunk = chunk?;
        let struct_arr = chunk.execute::<StructArray>(&mut ctx)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::Range;
    use vortex_array::VortexSessionExecute as _;
    use vortex_array::arrays::{PrimitiveArray, StructArray};
    use vortex_array::expr::{eq, get_item, lit};
    use vortex_layout::scan::split_by::SplitBy;

    use crate::store::schema;

    /// A scan of `rows` of `file`, cut into splits of `per_split` rows.
    fn scan_of(
        file: &NativeStoreFile,
        rows: Range<u64>,
        per_split: usize,
        ordered: bool,
    ) -> ScanBuilder<ArrayRef> {
        file.scan()
            .unwrap()
            .with_row_range(rows)
            .with_split_by(SplitBy::RowCount(per_split))
            .with_ordered(ordered)
    }

    /// The four code columns of `rows`, each in row order.
    fn quad_codes(rows: ArrayRef) -> Vec<Vec<TermCode>> {
        let mut ctx = crate::session::VORTEX_SESSION.create_execution_ctx();
        let rows = rows.execute::<StructArray>(&mut ctx).unwrap();
        schema::PRIMARY_COLUMNS
            .iter()
            .map(|&column| {
                crate::store::array::field_as::<PrimitiveArray>(&rows, column, &mut ctx)
                    .unwrap()
                    .as_slice::<TermCode>()
                    .to_vec()
            })
            .collect()
    }

    /// The four code columns of `chunks`, each concatenated in chunk order.
    fn chunk_code_columns(chunks: Vec<ArrayRef>) -> Vec<Vec<TermCode>> {
        let mut columns = vec![Vec::new(); schema::PRIMARY_COLUMNS.len()];
        for chunk in chunks {
            for (column, codes) in columns.iter_mut().zip(quad_codes(chunk)) {
                column.extend(codes);
            }
        }
        columns
    }

    /// What `read_all_rows` did with a scan: its split count, how many of
    /// them it spawned, and the rows it returned.
    struct Driven {
        splits: usize,
        spawned: usize,
        rows: Vec<Vec<TermCode>>,
    }

    impl Driven {
        /// The driver that ran the scan, read off the splits it spawned.
        fn driver(&self) -> ScanDriver {
            driver_hooks::driver_of(self.splits, self.spawned)
        }
    }

    async fn drive(
        file: &NativeStoreFile,
        rows: Range<u64>,
        per_split: usize,
        ordered: bool,
    ) -> Driven {
        drive_scan(|| scan_of(file, rows.clone(), per_split, ordered)).await
    }

    /// [`drive`] over the scans `scan` builds, one to count the splits and
    /// one to read.
    async fn drive_scan(scan: impl Fn() -> ScanBuilder<ArrayRef>) -> Driven {
        let splits = scan().build().unwrap().len();
        driver_hooks::take_spawned();
        let rows = read_all_rows(scan()).await.unwrap();
        Driven {
            splits,
            spawned: driver_hooks::take_spawned(),
            rows: quad_codes(rows),
        }
    }

    /// What `stream_rows` yielded for a scan: its split count, how many of
    /// them it spawned, each chunk's row count, and the rows.
    struct Streamed {
        splits: usize,
        spawned: usize,
        chunk_rows: Vec<usize>,
        rows: Vec<Vec<TermCode>>,
    }

    impl Streamed {
        /// The driver that ran the scan, read off the splits it spawned.
        fn driver(&self) -> ScanDriver {
            driver_hooks::driver_of(self.splits, self.spawned)
        }
    }

    /// [`drive_scan`] through `stream_rows`, its chunks collected.
    async fn stream_scan(scan: impl Fn() -> ScanBuilder<ArrayRef>) -> Streamed {
        use futures::TryStreamExt as _;

        let splits = scan().build().unwrap().len();
        driver_hooks::take_spawned();
        let chunks: Vec<ArrayRef> = stream_rows(scan()).unwrap().try_collect().await.unwrap();
        Streamed {
            splits,
            spawned: driver_hooks::take_spawned(),
            chunk_rows: chunks.iter().map(|chunk| chunk.len()).collect(),
            rows: chunk_code_columns(chunks),
        }
    }

    /// The first `n` rows of `columns`.
    fn head(columns: &[Vec<TermCode>], n: usize) -> Vec<Vec<TermCode>> {
        columns.iter().map(|c| c[..n].to_vec()).collect()
    }

    /// `read_all_rows` hands a scan's rows back whole and in row order, split
    /// after split, under either driver: the same four-split scan is forced
    /// down each path and must return all four columns identically.
    #[tokio::test]
    async fn read_all_rows_keeps_order_on_either_driver() {
        let (_dir, file) = crate::tests::mapped_file(50, vec![]).await;

        // The whole file in one split is the reference.
        let whole = drive(&file, 0..50, 50, true).await;
        assert_eq!((whole.splits, whole.rows[0].len()), (1, 50));
        // The file is subject-sorted and its subjects are unique, so the
        // subject codes rise with the row order.
        let subjects = &whole.rows[0];
        assert!(
            subjects.windows(2).all(|pair| pair[0] < pair[1]),
            "{subjects:?}"
        );

        // Splits of 16 rows: four futures for the driver to run, the last short.
        for (limit, driver) in [(4, ScanDriver::Inline), (3, ScanDriver::Spawned)] {
            let _limit = driver_hooks::ForcedLimit::set(limit);
            let driven = drive(&file, 0..50, 16, true).await;
            assert_eq!(driven.splits, 4);
            assert_eq!(
                driven.driver(),
                driver,
                "limit {limit} on a four-split scan"
            );
            assert_eq!(driven.rows, whole.rows, "{driver:?}: rows and order");
        }

        // An unordered scan is collected in split order as well, by either
        // driver: the flag relaxes what a caller may rely on, not what it gets.
        for (limit, driver) in [(4, ScanDriver::Inline), (0, ScanDriver::Spawned)] {
            let _limit = driver_hooks::ForcedLimit::set(limit);
            let driven = drive(&file, 0..50, 16, false).await;
            assert_eq!(driven.driver(), driver, "unordered, limit {limit}");
            assert_eq!(driven.rows, whole.rows, "unordered, {driver:?}");
        }
    }

    /// A scan of at most [`MAX_INLINE_SPLITS`] splits runs inline and one with
    /// more is spawned, with no limit forced.
    #[tokio::test]
    async fn read_all_rows_runs_inline_up_to_the_split_limit() {
        let limit = MAX_INLINE_SPLITS;
        let (_dir, file) = crate::tests::mapped_file(limit + 8, vec![]).await;
        let whole = drive(&file, 0..(limit + 8) as u64, limit + 8, true).await;
        assert_eq!(whole.driver(), ScanDriver::Inline, "a single split");

        // One row per split.
        let at = drive(&file, 0..limit as u64, 1, true).await;
        assert_eq!((at.splits, at.driver()), (limit, ScanDriver::Inline));
        assert_eq!(at.rows, head(&whole.rows, limit));
        let over = drive(&file, 0..limit as u64 + 1, 1, true).await;
        assert_eq!(
            (over.splits, over.driver()),
            (limit + 1, ScanDriver::Spawned)
        );
        assert_eq!(over.rows, head(&whole.rows, limit + 1));
    }

    /// A scan above the limit reads with no tokio runtime to spawn onto: it
    /// runs inline. Driven here by a bare executor on a plain thread.
    #[tokio::test]
    async fn read_all_rows_above_the_limit_reads_outside_a_tokio_runtime() {
        let limit = MAX_INLINE_SPLITS;
        let (_dir, file) = crate::tests::mapped_file(limit + 8, vec![]).await;
        let whole = drive(&file, 0..(limit + 8) as u64, limit + 8, true).await;
        let scan = scan_of(&file, 0..limit as u64 + 1, 1, true);
        assert_eq!(
            scan_of(&file, 0..limit as u64 + 1, 1, true)
                .build()
                .unwrap()
                .len(),
            limit + 1
        );

        let (rows, spawned) = std::thread::spawn(move || {
            futures::executor::block_on(async {
                let rows = read_all_rows(scan).await.unwrap();
                (quad_codes(rows), driver_hooks::take_spawned())
            })
        })
        .join()
        .expect("a scan above the limit must not panic without a tokio runtime");
        assert_eq!(spawned, 0, "no runtime to spawn onto, so nothing spawned");
        assert_eq!(rows, head(&whole.rows, limit + 1));
    }

    /// `stream_rows` yields `read_all_rows`' rows chunk by chunk, one chunk
    /// per split that keeps a row, in split order, under the driver
    /// `read_all_rows` picks for the same scan: inline at the split limit,
    /// spawned above it, and inline above it on a thread with no tokio
    /// runtime to spawn onto.
    #[tokio::test]
    async fn stream_rows_matches_read_all_rows_under_every_driver() {
        let (_dir, file) = crate::tests::mapped_file(50, vec![]).await;
        let whole = drive(&file, 0..50, 50, true).await;
        // Row 20's subject: the subjects are unique, so of the four 16-row
        // splits only the second keeps a row under this filter.
        let scope = file.scan().unwrap().dtype().unwrap();
        let filter = file
            .bound_exprs()
            .bind(
                QUAD_SCOPE,
                &eq(get_item(schema::COL_S, root()), lit(whole.rows[0][20])),
                &scope,
            )
            .unwrap();
        let scan = |filter: Option<&vortex_array::expr::BoundExpression>| {
            let scan = scan_of(&file, 0..50, 16, true);
            match filter {
                Some(filter) => scan.with_filter(filter.clone()),
                None => scan,
            }
        };

        for (limit, driver) in [(4, ScanDriver::Inline), (3, ScanDriver::Spawned)] {
            let _limit = driver_hooks::ForcedLimit::set(limit);
            for (tag, filter, chunk_rows) in [
                ("unfiltered", None, vec![16, 16, 16, 2]),
                ("filtered", Some(&filter), vec![1]),
            ] {
                let read = drive_scan(|| scan(filter)).await;
                let streamed = stream_scan(|| scan(filter)).await;
                assert_eq!(read.driver(), driver, "{tag}: read, limit {limit}");
                assert_eq!(
                    (streamed.splits, streamed.driver()),
                    (4, driver),
                    "{tag}: streamed, limit {limit}"
                );
                assert_eq!(streamed.chunk_rows, chunk_rows, "{tag}, {driver:?}: chunks");
                assert_eq!(
                    streamed.rows, read.rows,
                    "{tag}, {driver:?}: rows and order"
                );
            }
        }
        let row_20: Vec<Vec<TermCode>> = whole.rows.iter().map(|column| vec![column[20]]).collect();
        assert_eq!(
            drive_scan(|| scan(Some(&filter))).await.rows,
            row_20,
            "the filter keeps row 20 alone"
        );

        let no_runtime = scan(None);
        let (chunk_rows, rows, spawned) = std::thread::spawn(move || {
            use futures::TryStreamExt as _;

            let _limit = driver_hooks::ForcedLimit::set(0);
            futures::executor::block_on(async {
                let chunks: Vec<ArrayRef> = stream_rows(no_runtime)
                    .unwrap()
                    .try_collect()
                    .await
                    .unwrap();
                let chunk_rows: Vec<usize> = chunks.iter().map(|chunk| chunk.len()).collect();
                (
                    chunk_rows,
                    chunk_code_columns(chunks),
                    driver_hooks::take_spawned(),
                )
            })
        })
        .join()
        .expect("a streamed scan must not panic without a tokio runtime");
        assert_eq!(spawned, 0, "no runtime to spawn onto, so nothing spawned");
        assert_eq!(chunk_rows, [16, 16, 16, 2]);
        assert_eq!(rows, whole.rows);
    }

    /// An index child's row-id scan is driven inline however many splits it
    /// has, where a row scan of the same shape is spawned: the classification
    /// is the entry point's, not the scan's.
    #[tokio::test]
    async fn read_index_row_ids_drives_inline_above_the_split_limit() {
        let (_dir, file) = crate::tests::mapped_file(50, vec![]).await;
        let _limit = driver_hooks::ForcedLimit::set(1);

        driver_hooks::take_spawned();
        let rows = read_index_row_ids(scan_of(&file, 0..50, 16, false))
            .await
            .unwrap();
        assert_eq!(
            driver_hooks::take_spawned(),
            0,
            "no split of it was spawned"
        );
        let spawned = drive(&file, 0..50, 16, false).await;
        assert_eq!(
            spawned.driver(),
            ScanDriver::Spawned,
            "the same scan as a row scan"
        );
        assert_eq!(quad_codes(rows), spawned.rows);
    }
}
