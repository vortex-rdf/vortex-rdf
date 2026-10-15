//! The file-backed side of the index hub: the readers (point reads through
//! cached chunk probes, rid-only pushed-down scans) that produce a
//! resolution's ascending, unique `Buffer<RowId>` of base rows from an index
//! child, and [`FileServePlan`], which reads a resolved view's quads out of
//! the child's own columns.

use std::ops::Range;
use std::sync::Arc;

use vortex_array::arrays::struct_::{StructArray, StructArrayExt};
use vortex_array::expr::{root, select};
use vortex_array::scalar::Scalar;
use vortex_array::{ArrayRef, VortexSessionExecute};
use vortex_buffer::Buffer;
use vortex_layout::scan::split_by::SplitBy;
use vortex_mask::Mask;

use super::components::{check_addressable, sorted_row_ids};
use super::serve::ServeDecode;
use super::{IndexResolution, ResolvedRoles, ResolvedRowIds};
use crate::error::{Result, VortexRdfError};
use crate::session::VORTEX_SESSION;
use crate::store::layouts::{ChunkDecode, ResolvedLayout};
use crate::store::scan::file_reads::eq_conjunction;
use crate::store::schema::{RowId, TermCode};

/// The `[lo, hi)` run of a sorted component column's rows equal to `native`,
/// located by binary search over the column's cached chunk probes — reading
/// only the chunk leaves the bisection crosses. Searched over the whole
/// column, or `within` a row range whose slice of the column is itself sorted
/// (a lead run for a prefix probe).
///
/// `Ok(None)` declines the location (the caller keeps its pushed-down scan):
/// a child not globally sorted, a probe value that is not an integer (string
/// value columns), or a column whose chunks resolve no probe.
pub(crate) async fn locate_component_run(
    file: &crate::store::persist::native_file::NativeStoreFile,
    component: &str,
    column: &str,
    native: &Scalar,
    within: Option<Range<u64>>,
    sorted: bool,
) -> Result<Option<Range<u64>>> {
    if !sorted {
        return Ok(None);
    }
    let Ok(needle) = u64::try_from(native) else {
        return Ok(None);
    };
    let Some(chunks) = file.component_column_chunks(component, column) else {
        return Ok(None);
    };
    let source = file.segment_source();
    let session = file.session();
    match within {
        None => chunks.bounds(needle, &source, session).await,
        Some(range) => chunks.bounds_in(range, needle, &source, session).await,
    }
    .map_err(VortexRdfError::Vortex)
}

/// The row ids of a located index-child run, read point-by-point from the
/// child's rid column through its cached chunk probes and re-sorted into
/// base row order — the file counterpart of slicing an in-memory rid run.
/// `Ok(None)` when the rid column's chunks decline (the caller keeps its
/// scan); rids are unique by construction, so sorting alone suffices.
///
/// `rid_column` comes from the calling index — the hub names no index's
/// columns.
pub(crate) async fn rid_point_reads(
    file: &crate::store::persist::native_file::NativeStoreFile,
    component: &str,
    rid_column: &str,
    range: Range<u64>,
) -> Result<Option<Buffer<RowId>>> {
    let Some(chunks) = file.component_column_chunks(component, rid_column) else {
        return Ok(None);
    };
    let source = file.segment_source();
    let session = file.session();
    // A point-read run is at most `POINT_GATHER_MAX_ROWS` wide.
    let mut ids = Vec::with_capacity((range.end - range.start) as usize);
    for row in range {
        match chunks.value_at(row, &source, session).await? {
            Some(rid) => ids.push(super::base_row(rid)),
            None => return Ok(None),
        }
    }
    ids.sort_unstable();
    check_addressable(&ids)?;
    Ok(Some(Buffer::from(ids)))
}

/// Scan `rid_column` for the rows where every `(value_column, probe)`
/// equality holds, returning the primary row ids as an ascending, unique
/// buffer (the shape vortex's `Selection::IncludeByIndex` requires) — the
/// file-backed probe shared by the secondary indexes.
///
/// Each equality is a plain `eq`, the same encoding the serve-plan filter
/// uses: a binary `Eq` falsifies against the same zone min/max envelope as a
/// `>= probe AND <= probe` range pair (see vortex's
/// `stats/rewrite/builtins.rs`) while evaluating a single conjunct. Output
/// order is irrelevant: the ids are sorted afterwards.
pub(crate) async fn scan_index_row_ids(
    reader: vortex_layout::LayoutReaderRef,
    value_constraints: &[(&'static str, Scalar)],
    rid_column: &'static str,
    memo: &crate::store::persist::native_file::BoundExprMemo,
    scope: &'static str,
) -> Result<Buffer<RowId>> {
    // Every index probes at least one value column; an empty constraint set
    // would mean "all rows", which no resolver asks for.
    let Some(filter) = eq_conjunction(value_constraints.iter().cloned()) else {
        return Ok(Buffer::empty());
    };
    let filter = memo.bind(scope, &filter, reader.dtype())?;

    read_scanned_row_ids(
        rid_scan(reader, rid_column, memo, scope)?.with_filter(filter),
        rid_column,
    )
    .await
}

/// The row ids of a *located* index-child run — the rows a resolver bounded
/// by binary-searching the child's cached chunk probes — read by a rid-only
/// scan restricted to that range. The wide-run counterpart of
/// [`rid_point_reads`], for runs too large to read point by point.
///
/// The scan carries no filter: the location bounded exactly the rows the
/// constraints select, so re-testing the value columns would only re-read and
/// re-compare them. A resolver whose location covers only *some* of its
/// constraints must keep [`scan_index_row_ids`], whose filter tests them all.
pub(crate) async fn scan_located_row_ids(
    reader: vortex_layout::LayoutReaderRef,
    rid_column: &'static str,
    range: Range<u64>,
    memo: &crate::store::persist::native_file::BoundExprMemo,
    scope: &'static str,
) -> Result<Buffer<RowId>> {
    read_scanned_row_ids(
        rid_scan(reader, rid_column, memo, scope)?.with_row_range(range),
        rid_column,
    )
    .await
}

/// The row ids of a located index-child run — or of a window of one — in base
/// row order: point reads through the rid column's chunk probes for a range
/// within the point-read cap, otherwise — or when a chunk declines mid-read —
/// a rid-only scan restricted to the range (the location bounded exactly the
/// matched rows, so no filter is re-tested).
///
/// `range` is not empty: an empty run or window reads nothing at all, so its
/// caller answers it without asking.
pub(crate) async fn read_located_rids(
    file: &crate::store::persist::native_file::NativeStoreFile,
    component: &'static str,
    reader: &vortex_layout::LayoutReaderRef,
    rid_column: &'static str,
    range: Range<u64>,
    scope: &'static str,
) -> Result<Buffer<RowId>> {
    debug_assert!(
        range.start < range.end,
        "an empty run or window reads nothing: its caller does not ask"
    );
    #[cfg(test)]
    file.note_located_rid_reads(range.end - range.start);
    if crate::store::view::selection::point_sized(range.end - range.start)
        && let Some(ids) = rid_point_reads(file, component, rid_column, range.clone()).await?
    {
        return Ok(ids);
    }
    scan_located_row_ids(reader.clone(), rid_column, range, file.bound_exprs(), scope).await
}

/// A rid-only scan of an index child: just the row-id column. Restrictions — a
/// filter, a row range — are the caller's to add.
fn rid_scan(
    reader: vortex_layout::LayoutReaderRef,
    rid_column: &'static str,
    memo: &crate::store::persist::native_file::BoundExprMemo,
    scope: &'static str,
) -> Result<vortex_layout::scan::scan_builder::ScanBuilder<ArrayRef>> {
    let projection = memo.bind(scope, &select([rid_column], root()), reader.dtype())?;
    Ok(
        vortex_layout::scan::scan_builder::ScanBuilder::new(VORTEX_SESSION.clone(), reader)
            .with_projection(projection),
    )
}

/// Run a rid-only scan and decode its row-id column into the ascending,
/// unique buffer every index resolution answers in. The scan is driven inline
/// by `file_reads::read_index_row_ids`.
async fn read_scanned_row_ids(
    scan: vortex_layout::scan::scan_builder::ScanBuilder<ArrayRef>,
    rid_column: &'static str,
) -> Result<Buffer<RowId>> {
    let arr = crate::store::scan::file_reads::read_index_row_ids(scan).await?;

    if arr.is_empty() {
        return Ok(Buffer::empty());
    }

    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let struct_arr = arr.execute::<StructArray>(&mut ctx)?;
    sorted_row_ids(struct_arr.unmasked_field_by_name(rid_column).cloned()?)
}

/// An eager file resolution off the rid-only pushed-down scan — the shared
/// tail of the file resolvers whenever no serving plan defers the ids (a
/// back-reference child, or a copy resolution that couldn't build its plan):
/// `Empty` when the scan proves the probe matches nothing.
pub(crate) async fn resolve_eager_from_scan(
    reader: vortex_layout::LayoutReaderRef,
    constraints: &[(&'static str, Scalar)],
    rid_column: &'static str,
    resolves: ResolvedRoles,
    memo: &crate::store::persist::native_file::BoundExprMemo,
    scope: &'static str,
) -> Result<IndexResolution<FileServePlan>> {
    let row_ids = scan_index_row_ids(reader, constraints, rid_column, memo, scope).await?;
    if row_ids.is_empty() {
        return Ok(IndexResolution::Empty);
    }
    Ok(IndexResolution::Resolved {
        row_ids: ResolvedRowIds::Eager(row_ids),
        resolves,
        serve: None,
    })
}

/// An index's serving plan for a file-backed view: the matched rows are those
/// where every `(column, value)` term equality holds — a contiguous run of
/// the index child, which its sort order clusters — read by a scan of that
/// run when the resolution located it, else by a scan pushing the equalities
/// down as a zone-prunable filter, instead of scattering row-id reads across
/// the primary columns.
///
/// The file-backed half of the serving path (see the `serve` module docs;
/// [`InMemoryServePlan`] is the in-memory half). `QuadsSource::File` carries
/// exactly this type, so a file view can never hold an in-memory plan.
///
/// [`InMemoryServePlan`]: super::InMemoryServePlan
#[derive(Clone)]
pub(crate) struct FileServePlan {
    decode: ServeDecode,
    /// The index component child's cached layout reader.
    reader: vortex_layout::LayoutReaderRef,
    constraints: Vec<(&'static str, Scalar)>,
    /// The file handle's bind memo — the plan binds its projection and
    /// filter through it on FIRST READ, not at construction: a match-only
    /// call builds the plan without ever scanning through it, and must not
    /// pay for binds a count-only consumer will never use. The memo keys
    /// the bound trees by shape, so every plan for a repeated pattern
    /// carries the same identity and hits the child reader's
    /// identity-keyed caches (see `BoundExprMemo`).
    memo: Arc<crate::store::persist::native_file::BoundExprMemo>,
    /// The lazily bound (projection, filter) pair, shared across clones so
    /// the first reader's bind serves them all.
    bound: Arc<
        std::sync::OnceLock<(
            vortex_array::expr::BoundExpression,
            vortex_array::expr::BoundExpression,
        )>,
    >,
    /// The serving component's name, addressing its cached chunk probes on
    /// the file handle for point-read serving.
    component: &'static str,
    /// The child rows the constraints select, when the resolution located
    /// them by chunk probes — exactly the constrained rows, letting a small
    /// run be point-read and a wide one scanned by range
    /// ([`Self::located_run_scan`]) instead of filtered. `None` when
    /// unlocated (or when a constraint the location didn't cover would make
    /// the range over-approximate).
    row_range: Option<Range<u64>>,
}

/// The fewest rows a located run's scan split carries: below this the
/// per-split overhead (a spawned task, its segment requests, one decode
/// call) outweighs what spreading the decode buys.
const SERVE_SPLIT_MIN_ROWS: u64 = 1024;

/// Rows per split for a located run of `rows`: enough splits to hand every
/// worker a couple, never fewer than [`SERVE_SPLIT_MIN_ROWS`] rows each.
fn run_split_rows(rows: u64) -> usize {
    let workers = crate::io::read::available_parallelism() as u64;
    rows.div_ceil(2 * workers).max(SERVE_SPLIT_MIN_ROWS) as usize
}

impl FileServePlan {
    /// A plan serving a file's index columns by a pushed-down scan filtered to
    /// the rows where every `constraints` equality holds — or, over a located
    /// `row_range`, by point reads (a small run) or a range-restricted scan
    /// split across the workers (a wide one) — see
    /// [`Self::located_run_scan`].
    // The parameters are the plan itself: the column roles, the reader, the
    // constraints, and the bind memo.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        primary_columns: [&'static str; 4],
        rid_column: &'static str,
        decode_layout: ResolvedLayout,
        reader: vortex_layout::LayoutReaderRef,
        constraints: Vec<(&'static str, Scalar)>,
        component: &'static str,
        row_range: Option<Range<u64>>,
        memo: Arc<crate::store::persist::native_file::BoundExprMemo>,
    ) -> Self {
        Self {
            decode: ServeDecode::new(primary_columns, rid_column, decode_layout),
            reader,
            constraints,
            memo,
            bound: Arc::new(std::sync::OnceLock::new()),
            component,
            row_range,
        }
    }

    /// The plan's (projection, filter), bound through the handle's memo on
    /// first use and shared across clones thereafter.
    fn bound_exprs(
        &self,
    ) -> Result<(
        vortex_array::expr::BoundExpression,
        vortex_array::expr::BoundExpression,
    )> {
        if let Some(bound) = self.bound.get() {
            return Ok(bound.clone());
        }
        let projection = select(self.projection(), root());
        // A serve plan always carries at least one constraint (the resolved
        // lead component), so the conjunction is never empty.
        let filter = eq_conjunction(self.constraints.iter().cloned())
            .expect("a serve plan constrains at least one column");
        let scope = self.reader.dtype();
        let bound_projection = self.memo.bind(self.component, &projection, scope)?;
        let bound_filter = self.memo.bind(self.component, &filter, scope)?;
        Ok(self
            .bound
            .get_or_init(|| (bound_projection, bound_filter))
            .clone())
    }

    /// The serving component's name on the file handle.
    pub(crate) fn component(&self) -> &'static str {
        self.component
    }

    /// The located child-row range the constraints select, when known.
    pub(crate) fn row_range(&self) -> Option<Range<u64>> {
        self.row_range.clone()
    }

    /// The columns to project from the file to serve these rows: the four
    /// component sources plus the row-id column (for tombstones).
    pub(crate) fn projection(&self) -> [&'static str; 5] {
        let [s, p, o, g] = self.decode.primary_columns;
        [s, p, o, g, self.decode.rid_column]
    }

    /// A scan over the serving index child — where [`Self::projection`] and
    /// the plan's bound filter apply.
    fn child_scan(&self) -> vortex_layout::scan::scan_builder::ScanBuilder<ArrayRef> {
        vortex_layout::scan::scan_builder::ScanBuilder::new(
            VORTEX_SESSION.clone(),
            self.reader.clone(),
        )
    }

    /// [`Self::child_scan`] with the plan's projection and filter — bound on
    /// first use — applied. The form the streaming reads consume for an
    /// unlocated run.
    pub(crate) fn projected_filtered_scan(
        &self,
    ) -> Result<vortex_layout::scan::scan_builder::ScanBuilder<ArrayRef>> {
        let (projection, filter) = self.bound_exprs()?;
        Ok(self
            .child_scan()
            .with_projection(projection)
            .with_filter(filter))
    }

    /// A scan of the located run: [`Self::child_scan`] with the plan's
    /// projection, restricted to `row_range` and split by row count. `None`
    /// when the run is unlocated — [`Self::projected_filtered_scan`] answers
    /// then. The form the streaming reads consume for a wide located run.
    ///
    /// The scan spawns one task per split and the consumer decodes each
    /// chunk inside its task, so the split count is the decode's
    /// parallelism. The child's natural splits are its leaf chunks, which
    /// cluster a run into one split however wide it is; splitting the range
    /// by row count spreads the run's decode over the workers instead
    /// (`run_split_rows`). No filter rides along: the located range is
    /// exactly the constrained rows (the same fact `size` and the point
    /// reads rely on), so the term equalities would only re-read and
    /// re-compare the columns that bounded it.
    pub(crate) fn located_run_scan(
        &self,
    ) -> Result<Option<vortex_layout::scan::scan_builder::ScanBuilder<ArrayRef>>> {
        let Some(range) = self.row_range.clone() else {
            return Ok(None);
        };
        let (projection, _) = self.bound_exprs()?;
        let split_rows = run_split_rows(range.end - range.start);
        Ok(Some(
            self.child_scan()
                .with_projection(projection)
                .with_row_range(range)
                .with_split_by(SplitBy::RowCount(split_rows)),
        ))
    }

    /// The served rows' four [`TermCode`]s, read off the index child's own
    /// columns by a scan of exactly the located run, or by the filtered child
    /// scan when the run is unlocated — the file counterpart of
    /// [`InMemoryServePlan::code_columns`], for a view whose row ids are still
    /// deferred. Rows come back in the index's order, as the served stream
    /// reads them, tombstoned rows dropped via the row-id column. `None`
    /// declines to the caller's gather path: a non-Dictionary decode layout,
    /// whose columns hold terms, not codes.
    ///
    /// [`InMemoryServePlan::code_columns`]: super::InMemoryServePlan::code_columns
    pub(crate) async fn code_columns(
        &self,
        deleted: Option<&Mask>,
    ) -> Result<Option<[Buffer<TermCode>; 4]>> {
        if !matches!(self.decode.decode_layout, ResolvedLayout::Dictionary(_)) {
            return Ok(None);
        }
        let scan = match self.located_run_scan()? {
            Some(scan) => scan,
            None => self.projected_filtered_scan()?,
        };
        let chunk = crate::store::scan::file_reads::read_all_rows(scan).await?;
        self.decode.chunk_code_columns(&chunk, deleted).map(Some)
    }

    /// Decode the `(s, p, o, g)` rows out of a chunk of this plan's projected
    /// index columns, dropping rows tombstoned in `deleted` via the row-id
    /// column.
    pub(crate) fn decode_columns<T: ChunkDecode>(
        &self,
        chunk: &ArrayRef,
        deleted: Option<&Mask>,
    ) -> Vec<Result<T>> {
        self.decode.decode_columns(chunk, deleted)
    }

    /// [`decode_columns`](Self::decode_columns) through the layout's async
    /// decode — for serving a store whose term dictionary is file-backed,
    /// where each chunk's codes are resolved by reading the dictionary
    /// windows that hold them.
    pub(crate) async fn decode_columns_async<T: ChunkDecode>(
        &self,
        chunk: &ArrayRef,
        deleted: Option<&Mask>,
    ) -> Vec<Result<T>> {
        self.decode.decode_columns_async(chunk, deleted).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vortex_array::arrays::PrimitiveArray;

    /// Both rid scans, the located run and the pushed-down equality, spawn
    /// nothing with the split limit at zero; a control row scan of the file
    /// spawns all of its splits.
    #[tokio::test]
    async fn index_row_id_scans_drive_inline_whatever_the_limit() {
        use crate::IndexType;
        use crate::io::read::scan_all_reader;
        use crate::store::array::{field_as, into_struct_array};
        use crate::store::indexes::COL_RID;
        use crate::store::scan::file_reads::{driver_hooks, read_all_rows};

        const COMPONENT: &str = "index:ref-p";
        let rows = 50;
        let quads = crate::tests::modular_quads_for_tests(rows);
        let (_dir, path) =
            crate::tests::write_store_file_for_tests(quads, vec![IndexType::SecondaryByReference])
                .await;
        let file = crate::tests::open_mapped(&path).await;
        let (_, reader) = file
            .component_reader(COMPONENT)
            .unwrap()
            .expect("the reference child of the predicate column");
        let _limit = driver_hooks::ForcedLimit::set(0);
        driver_hooks::take_spawned();

        // The located-run scan: every row of the child.
        let ids = scan_located_row_ids(
            reader.clone(),
            COL_RID,
            0..rows as u64,
            file.bound_exprs(),
            COMPONENT,
        )
        .await
        .unwrap();
        assert_eq!(driver_hooks::take_spawned(), 0, "located-run scan spawned");
        assert_eq!(ids.as_slice(), (0..rows as RowId).collect::<Vec<_>>());

        // The pushed-down equality, probing the child's first value.
        let child = scan_all_reader(reader.clone()).await.unwrap();
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let child = into_struct_array(child).unwrap();
        let first = field_as::<PrimitiveArray>(&child, "val", &mut ctx)
            .unwrap()
            .as_slice::<u64>()[0];
        let matched = scan_index_row_ids(
            reader,
            &[("val", Scalar::from(first))],
            COL_RID,
            file.bound_exprs(),
            COMPONENT,
        )
        .await
        .unwrap();
        assert_eq!(driver_hooks::take_spawned(), 0, "equality scan spawned");
        assert!(!matched.is_empty() && matched.len() < rows);
        assert!(matched.as_slice().windows(2).all(|pair| pair[0] < pair[1]));

        // Control: a row scan of the same file goes to the workers, every
        // split of it.
        let splits = file.scan().unwrap().build().unwrap().len();
        assert!(splits > 0);
        read_all_rows(file.scan().unwrap()).await.unwrap();
        assert_eq!(driver_hooks::take_spawned(), splits);
    }

    #[test]
    fn run_split_rows_floor_and_arithmetic() {
        // Small runs never split below the per-split floor.
        assert_eq!(run_split_rows(100), SERVE_SPLIT_MIN_ROWS as usize);
        assert_eq!(run_split_rows(0), SERVE_SPLIT_MIN_ROWS as usize);

        // A wide run hands every worker a couple of splits.
        let rows = 1u64 << 20;
        let workers = crate::io::read::available_parallelism() as u64;
        let split = run_split_rows(rows);
        assert_eq!(
            split,
            rows.div_ceil(2 * workers).max(SERVE_SPLIT_MIN_ROWS) as usize
        );
        assert!(split as u64 >= SERVE_SPLIT_MIN_ROWS);
        assert!((rows as usize).div_ceil(split) as u64 <= 2 * workers + 1);
    }
}
