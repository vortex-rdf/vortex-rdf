//! [`FileServePlan`]: a resolved file view's quads read out of the serving
//! index child's own columns, its located run by row range (or point by
//! point when small), else under the pushed-down term-equality filter.

use std::ops::Range;
use std::sync::{Arc, OnceLock};

use vortex_array::ArrayRef;
use vortex_array::dtype::Nullability;
use vortex_array::expr::{BoundExpression, col, pack, root, select};
use vortex_array::scalar::Scalar;
use vortex_buffer::Buffer;
use vortex_layout::layouts::row_idx::row_idx;
use vortex_layout::scan::scan_builder::ScanBuilder;
use vortex_layout::scan::split_by::SplitBy;
use vortex_mask::Mask;

use super::{LiveRows, ServeDecode, ServedRows, ServedSpan};
use crate::error::Result;
use crate::observe::observe;
use crate::session::VORTEX_SESSION;
use crate::store::SortOrder;
use crate::store::indexes::copy::CHILD_COLUMNS;
use crate::store::indexes::{COL_RID, LazyRowIds, base_row, get_or_try_init};
use crate::store::persist::native_file::{ChildReader, NativeStoreFile};
use crate::store::read::chunks::extract_codes;
use crate::store::scan::file_exprs::eq_conjunction;
use crate::store::scan::file_reads::{component_point_rows, component_point_values};
use crate::store::scan::file_scans::read_all_rows;
use crate::store::schema::{RowId, TermCode};
use crate::store::view::selection::point_sized;

/// An index's serving plan for a file-backed view: the matched rows are those
/// where every `(column, value)` term equality holds, a contiguous run of
/// the index child that its sort order clusters, read by a scan of that run
/// when the resolution located it, else by a scan pushing the equalities
/// down as a zone-prunable filter, instead of scattering row-id reads across
/// the primary columns.
///
/// The file-backed half of the serving path (see the `serve` module docs;
/// [`InMemoryServePlan`] is the in-memory half). `FileBase` carries only this
/// type, so a file view can never hold an in-memory plan.
///
/// [`InMemoryServePlan`]: super::InMemoryServePlan
#[derive(Clone)]
pub(crate) struct FileServePlan {
    decode: ServeDecode,
    /// The serving index child: its cached layout reader, its name
    /// (addressing its cached chunk probes on the file handle for point-read
    /// serving), and the file handle's bind memo, through which the plan
    /// binds its projection and filter on FIRST READ, not at construction: a
    /// match-only call builds the plan without ever scanning through it, and
    /// must not pay for binds a count-only consumer never uses. The memo keys
    /// the bound trees by shape, so every plan for a repeated pattern carries
    /// the same identity and hits the child reader's identity-keyed caches
    /// (see `BoundExprMemo`).
    child: ChildReader,
    /// The term equalities the matched rows hold, shared by every clone.
    constraints: Arc<[(&'static str, Scalar)]>,
    /// What the plan fills on first use, shared across clones so the first
    /// reader's work serves them all ([`PlanCells`]): one allocation a plan.
    cells: Arc<PlanCells>,
    /// The child rows of the match's run, when the resolution located them
    /// by chunk probes: exactly the constrained rows, letting a small run be
    /// point-read and a wide one scanned by range ([`Self::read`]) instead of
    /// filtered. `None` when unlocated, or when a constraint the location did
    /// not cover would make the range over-approximate.
    run: Option<Range<u64>>,
    /// The rows of the run a narrowing (a window, keep, partition or chained
    /// match) left the view; `None` for the match's own plan.
    narrowed: Option<ServedSpan>,
}

/// How a [`FileServePlan`] reads the view's rows, decided once from what it
/// covers ([`FileServePlan::read`]).
// Returned once per read and matched at once: boxing the scan would cost
// every served file read an allocation for nothing.
#[allow(clippy::large_enum_variant)]
pub(crate) enum PlanRead {
    /// Point by point at these child rows: the kept rows, or a located range
    /// of at most [`POINT_GATHER_MAX_ROWS`] rows.
    ///
    /// [`POINT_GATHER_MAX_ROWS`]: crate::store::view::selection::POINT_GATHER_MAX_ROWS
    Points(Buffer<u64>),
    /// A wider located range by row range, split across the workers, or an
    /// unlocated run under the plan's filter.
    Scan(ScanBuilder<ArrayRef>),
}

/// What a [`FileServePlan`] fills on first use, behind one `Arc` its clones
/// share.
#[derive(Default)]
struct PlanCells {
    /// The plan's projection, bound on first use
    /// ([`projection`](FileServePlan::projection)).
    projection: OnceLock<BoundExpression>,
    /// The plan's filter, bound on first use
    /// ([`filter`](FileServePlan::filter)): only a scan of an unlocated run
    /// applies it, so a located read never binds it.
    filter: OnceLock<BoundExpression>,
    /// An unlocated run's rows as its first narrowing locates them
    /// ([`located_span`](FileServePlan::located_span)), read once by row
    /// index for every narrowing after. Its kept rows are the live ones under
    /// the first caller's tombstones, which every caller shares: a plan's
    /// views keep the tombstones of the view it was resolved for, as only an
    /// owner's source, which holds no plan, takes new ones
    /// ([`with_tombstones`](crate::store::QuadsSource::with_tombstones)).
    located: OnceLock<ServedSpan>,
}

/// The child rows `range` beside the base row id each mirrors, `rids` being
/// the range's rid column.
fn child_rows(range: Range<u64>, rids: &Buffer<u64>) -> impl Iterator<Item = (u64, RowId)> + '_ {
    range.zip(rids.iter().map(|&rid| base_row(rid)))
}

/// Whether the base row `rid` survives the `deleted` tombstones.
fn is_live(deleted: Option<&Mask>, rid: RowId) -> bool {
    deleted.is_none_or(|d| !d.value(rid as usize))
}

/// The fewest rows a located run's scan split carries: below this the
/// per-split overhead (a spawned task, its segment requests, one decode
/// call) outweighs what spreading the decode buys.
const SERVE_SPLIT_MIN_ROWS: u64 = 1024;

/// Rows per split for the located run of child rows `range`: enough splits to
/// hand each of `workers` a couple, never fewer than [`SERVE_SPLIT_MIN_ROWS`]
/// rows each.
fn run_split_rows(range: &Range<u64>, workers: u64) -> usize {
    (range.end - range.start)
        .div_ceil(2 * workers)
        .max(SERVE_SPLIT_MIN_ROWS) as usize
}

impl FileServePlan {
    /// A plan serving a file's index columns, decoded through `decode`, by a
    /// pushed-down scan filtered to the rows where every `constraints`
    /// equality holds, or, over a located `row_range`, by point reads (a
    /// small run) or a range-restricted scan split across the workers (a wide
    /// one; see [`Self::read`]).
    pub(in crate::store::indexes) fn new(
        decode: ServeDecode,
        child: ChildReader,
        constraints: Vec<(&'static str, Scalar)>,
        row_range: Option<Range<u64>>,
    ) -> Self {
        Self {
            decode,
            child,
            constraints: constraints.into(),
            cells: Arc::default(),
            run: row_range,
            narrowed: None,
        }
    }

    /// The rows of an unlocated run in the child's order, which is the
    /// index's: one scan of the child under the plan's filter, projecting
    /// each row's child position (vortex's `#row_idx`) beside its rid
    /// column's value.
    async fn read_run_rows(&self) -> Result<(Buffer<u64>, Buffer<u64>)> {
        observe!(ServedRunLocated);
        let filter = self.filter()?;
        let projection = self.child.bind(&pack(
            [("position", row_idx()), ("rid", col(COL_RID))],
            Nullability::NonNullable,
        ))?;
        let rows = read_all_rows(
            self.child_scan()
                .with_projection(projection)
                .with_filter(filter),
        )
        .await?;
        let mut columns = extract_codes(rows, &["position", "rid"])?;
        let rids = columns.remove(1);
        Ok((columns.remove(0), rids))
    }

    /// The decode the plan's reads take, tombstones included.
    pub(crate) fn decoder(&self) -> &ServeDecode {
        &self.decode
    }

    /// The child rows the view covers, when known without reading: what a
    /// narrowing left of the run, else the run the resolution located.
    /// `None` for an unlocated run, read under the plan's filter.
    pub(crate) fn span(&self) -> Option<ServedSpan> {
        self.narrowed
            .clone()
            .or_else(|| self.run.clone().map(ServedSpan::Range))
    }

    /// What a narrowing cuts: [`span`](Self::span), or an unlocated run
    /// located on the first call and kept for every clone of the plan. The
    /// run's rows are read once by row index; they are the range they span
    /// when contiguous in the child (a run no residual term filters, such as
    /// a string layout's), else kept rows, less the `deleted` tombstones.
    pub(crate) async fn located_span(&self, deleted: Option<&Mask>) -> Result<ServedSpan> {
        if let Some(span) = self.span().or_else(|| self.cells.located.get().cloned()) {
            return Ok(span);
        }
        let (positions, rids) = self.read_run_rows().await?;
        let span = match (positions.first(), positions.last()) {
            (Some(&first), Some(&last)) if last - first + 1 == positions.len() as u64 => {
                ServedSpan::Range(first..last + 1)
            }
            // Scattered: its live rows, each beside its base row id, built
            // in one pass.
            _ => {
                let rows = positions
                    .iter()
                    .copied()
                    .zip(rids.iter().map(|&rid| base_row(rid)));
                let live = ServedRows::collect(rows, positions.len(), |(position, rid)| {
                    is_live(deleted, rid).then_some((position, rid))
                });
                ServedSpan::Kept(Arc::new(live))
            }
        };
        Ok(self.cells.located.get_or_init(|| span).clone())
    }

    /// Whether a narrowing (a window, keep, partition or chained match) left
    /// the view only part of its match's run.
    pub(crate) fn is_narrowed(&self) -> bool {
        self.narrowed.is_some()
    }

    /// This plan over `span`, what a narrowing left of the view's rows.
    pub(crate) fn with_span(&self, span: ServedSpan) -> Self {
        Self {
            narrowed: Some(span),
            ..self.clone()
        }
    }

    /// The base row ids of the child rows `range` (a part of the run), read
    /// on first need off those rows of the child's rid column: what a window
    /// or partition of a range holds. A copy child's rids follow its own sort
    /// order, which nothing ties to base row order: they are not taken to
    /// ascend.
    pub(crate) fn range_ids(&self, file: &Arc<NativeStoreFile>, range: Range<u64>) -> LazyRowIds {
        LazyRowIds::from_located_run(Arc::clone(file), self.child.clone(), range, false)
    }

    /// The child's `columns` at the child rows `positions` (ascending, not
    /// empty), in that order: point by point through the child's cached
    /// chunk probes, or, where a probe declines (a string column), by a scan
    /// of those rows (Vortex's row-indices selection), which reads only the
    /// child's chunks that hold them.
    pub(crate) async fn columns_at(
        &self,
        file: &NativeStoreFile,
        columns: &[&'static str],
        positions: &Buffer<u64>,
    ) -> Result<ArrayRef> {
        if let Some(rows) = component_point_rows(file, self.child.name, columns, positions).await? {
            return Ok(rows);
        }
        // The scan is bounded to the positions' span as well as selected at
        // them: without a row range, Vortex plans the splits of dense
        // positions over the whole child, every chunk of every column read.
        let span = match (positions.first(), positions.last()) {
            (Some(&first), Some(&last)) => first..last + 1,
            _ => 0..0,
        };
        observe!(ServedPositionsScanned(span.clone()));
        let positions =
            vortex_scan::strict_sorted_buffer::StrictSortedBuffer::try_new(positions.clone())?;
        read_all_rows(
            self.child
                .scan(columns)?
                .with_row_range(span)
                .with_row_indices(positions),
        )
        .await
    }

    /// The child's rows at `positions` (ascending, not empty), every child
    /// column, in that order ([`columns_at`](Self::columns_at)).
    pub(crate) async fn rows_at(
        &self,
        file: &NativeStoreFile,
        positions: &Buffer<u64>,
    ) -> Result<ArrayRef> {
        self.columns_at(file, &CHILD_COLUMNS, positions).await
    }

    /// The child's `columns` over the child rows `range` (not empty), in the
    /// child's order: point by point when the range is point-sized, else a
    /// scan of the range projecting them.
    async fn range_read(
        &self,
        file: &NativeStoreFile,
        range: Range<u64>,
        columns: &[&'static str],
    ) -> Result<ArrayRef> {
        if point_sized(range.end - range.start) {
            let positions: Buffer<u64> = range.collect();
            self.columns_at(file, columns, &positions).await
        } else {
            read_all_rows(self.child.scan(columns)?.with_row_range(range)).await
        }
    }

    /// [`range_read`](Self::range_read) of code columns, as `u64` buffers.
    async fn range_columns(
        &self,
        file: &NativeStoreFile,
        range: Range<u64>,
        columns: &[&'static str],
    ) -> Result<Vec<Buffer<u64>>> {
        if range.is_empty() {
            return Ok(columns.iter().map(|_| Buffer::empty()).collect());
        }
        extract_codes(self.range_read(file, range, columns).await?, columns)
    }

    /// The view's live rows in the child's order, each with its base row id:
    /// the kept rows, or a range's (its rid column read) less the `deleted`
    /// tombstones, an unlocated run located first
    /// ([`located_span`](Self::located_span)).
    pub(crate) async fn live_rows(
        &self,
        file: &NativeStoreFile,
        deleted: Option<&Mask>,
    ) -> Result<ServedRows> {
        match self.located_span(deleted).await? {
            ServedSpan::Kept(kept) => Ok((*kept).clone()),
            ServedSpan::Range(range) => self.range_rows(file, range, deleted).await,
        }
    }

    /// The view's live rows in the child's order ([`LiveRows`]): a range with
    /// nothing tombstoned as it is, else the live rows listed, each beside
    /// its base row id (a range's rid column read), an unlocated run located
    /// first ([`located_span`](Self::located_span)). What a narrowing of the
    /// view cuts.
    pub(crate) async fn live(
        &self,
        file: &NativeStoreFile,
        deleted: Option<&Mask>,
    ) -> Result<LiveRows> {
        Ok(match self.located_span(deleted).await? {
            ServedSpan::Kept(kept) => LiveRows::Kept((*kept).clone()),
            ServedSpan::Range(range) if deleted.is_none() => LiveRows::Range(range),
            ServedSpan::Range(range) => {
                LiveRows::Spanned(self.range_rows(file, range, deleted).await?)
            }
        })
    }

    /// The child rows `range` with their base row ids (the range's rid
    /// column read), less the `deleted` tombstones, in the child's order.
    async fn range_rows(
        &self,
        file: &NativeStoreFile,
        range: Range<u64>,
        deleted: Option<&Mask>,
    ) -> Result<ServedRows> {
        let rids = self
            .range_columns(file, range.clone(), &[COL_RID])
            .await?
            .remove(0);
        let len = (range.end - range.start) as usize;
        Ok(ServedRows::collect(
            child_rows(range, &rids),
            len,
            |(position, rid)| is_live(deleted, rid).then_some((position, rid)),
        ))
    }

    /// The view's live rows whose `column` code `admits`, in the child's
    /// order: what a keep of the view keeps. Kept rows, which are live, have
    /// the column read at their positions; a range has it read beside its
    /// rid column, its tombstoned rows left out. An unlocated run is located
    /// first ([`located_span`](Self::located_span)); the view covers a row.
    pub(crate) async fn kept_rows(
        &self,
        file: &NativeStoreFile,
        deleted: Option<&Mask>,
        column: &'static str,
        admits: impl Fn(TermCode) -> bool,
    ) -> Result<ServedRows> {
        Ok(match self.located_span(deleted).await? {
            ServedSpan::Kept(kept) => {
                let read = self.columns_at(file, &[column], &kept.positions).await?;
                let codes = extract_codes(read, &[column])?.remove(0);
                let rows = kept.positions_and_rids().zip(codes.iter());
                ServedRows::collect(rows, kept.len(), |(row, &code)| admits(code).then_some(row))
            }
            ServedSpan::Range(range) => {
                let mut columns = self
                    .range_columns(file, range.clone(), &[column, COL_RID])
                    .await?;
                let (rids, codes) = (columns.remove(1), columns.remove(0));
                let rows = child_rows(range, &rids).zip(codes.iter());
                ServedRows::collect(rows, codes.len(), |(row, &code)| {
                    (admits(code) && is_live(deleted, row.1)).then_some(row)
                })
            }
        })
    }

    /// The view's live rows where `holds` holds, in the child's order: what a
    /// pattern chained on the view keeps. `holds` tests the child's `columns`
    /// over every row the view covers, read once: a range's beside its rid
    /// column (its tombstoned rows among them, then left out), kept rows' at
    /// their positions. An unlocated run is located first
    /// ([`located_span`](Self::located_span)).
    pub(crate) async fn rows_where(
        &self,
        file: &NativeStoreFile,
        deleted: Option<&Mask>,
        columns: &[&'static str],
        holds: impl FnOnce(&ArrayRef) -> Result<Mask>,
    ) -> Result<ServedRows> {
        Ok(match self.located_span(deleted).await? {
            ServedSpan::Kept(kept) => {
                let holding = holds(&self.columns_at(file, columns, &kept.positions).await?)?;
                ServedRows::collect(
                    kept.positions_and_rids().zip(0..),
                    kept.len(),
                    |(row, i)| holding.value(i).then_some(row),
                )
            }
            ServedSpan::Range(range) => {
                let with_rids = [columns, &[COL_RID]].concat();
                let read = self.range_read(file, range.clone(), &with_rids).await?;
                let holding = holds(&read)?;
                let rids = extract_codes(read, &[COL_RID])?.remove(0);
                let rows = child_rows(range, &rids).zip(0..);
                ServedRows::collect(rows, rids.len(), |(row, i)| {
                    (holding.value(i) && is_live(deleted, row.1)).then_some(row)
                })
            }
        })
    }

    /// The plan's projection, the copy family's child schema (the quad
    /// columns, and the row-id column for tombstones), bound through the
    /// handle's memo on first use and shared across clones thereafter.
    fn projection(&self) -> Result<BoundExpression> {
        get_or_try_init(&self.cells.projection, || {
            self.child.bind(&select(CHILD_COLUMNS, root()))
        })
    }

    /// The plan's filter, the conjunction of its term equalities, bound as
    /// [`projection`](Self::projection) is. A serve plan always carries at
    /// least one constraint (the resolved lead component), so the
    /// conjunction is never empty.
    fn filter(&self) -> Result<BoundExpression> {
        get_or_try_init(&self.cells.filter, || {
            let filter = eq_conjunction(self.constraints.iter().cloned())
                .expect("a serve plan constrains at least one column");
            self.child.bind(&filter)
        })
    }

    pub(crate) fn component(&self) -> &'static str {
        self.child.name
    }

    /// The order the view's reads follow, the child's
    /// ([`ServeDecode::order`]). `None` for a child whose rows are not
    /// globally sorted, read in its own order.
    pub(crate) fn read_order(&self) -> Option<SortOrder> {
        self.child.sorted.then_some(self.decode.order)
    }

    /// How the plan reads the view's rows, decided once from what it covers,
    /// beside the tombstones the read still drops of the view's `deleted`
    /// (none for kept rows, which are live): the kept rows, or a located
    /// range of at most [`POINT_GATHER_MAX_ROWS`] rows, point by point; a
    /// wider located range by row range, split across the workers
    /// (`run_split_rows`); an unlocated run under the plan's filter. A
    /// scan's plan is bound before the read starts, so a bind failure fails
    /// this call rather than the read; rows read point by point bind nothing
    /// of the plan's.
    ///
    /// The range is split by row count, across the workers. Only a stream of
    /// the view's quads (`quads()`, through Vortex's own scan stream) spawns
    /// a task per split, so that the split count is its decode's
    /// parallelism; a code read runs at most `MAX_INLINE_SPLITS` splits
    /// inline on its calling task. No filter rides along a located run: its
    /// range is exactly the constrained rows (the same fact `size` and the
    /// point reads rely on).
    ///
    /// [`POINT_GATHER_MAX_ROWS`]: crate::store::view::selection::POINT_GATHER_MAX_ROWS
    pub(crate) fn read<'a>(
        &self,
        deleted: Option<&'a Mask>,
    ) -> Result<(PlanRead, Option<&'a Mask>)> {
        Ok(match self.span() {
            Some(ServedSpan::Kept(kept)) => (PlanRead::Points(kept.positions.clone()), None),
            Some(ServedSpan::Range(range)) if point_sized(range.end - range.start) => {
                (PlanRead::Points(range.collect()), deleted)
            }
            Some(ServedSpan::Range(range)) => {
                let projection = self.projection()?;
                let workers = crate::io::read::available_parallelism() as u64;
                let split_rows = run_split_rows(&range, workers);
                let scan = self
                    .child_scan()
                    .with_projection(projection)
                    .with_row_range(range)
                    .with_split_by(SplitBy::RowCount(split_rows));
                (PlanRead::Scan(scan), deleted)
            }
            None => (PlanRead::Scan(self.filtered_scan()?), deleted),
        })
    }

    /// A scan over the serving index child, to which the plan's projection
    /// and filter apply.
    fn child_scan(&self) -> ScanBuilder<ArrayRef> {
        ScanBuilder::new(VORTEX_SESSION.clone(), self.child.reader.clone())
    }

    /// [`Self::child_scan`] with the plan's projection and pushed-down
    /// filter (bound on first use) applied: an unlocated run's scan.
    fn filtered_scan(&self) -> Result<ScanBuilder<ArrayRef>> {
        Ok(self
            .child_scan()
            .with_projection(self.projection()?)
            .with_filter(self.filter()?))
    }

    /// The served rows' four [`TermCode`] columns, read off the index child's
    /// own columns: the file counterpart of
    /// [`InMemoryServePlan::code_columns`], read as the plan reads its rows
    /// ([`read`](Self::read)): rows taken point by point straight into code
    /// buffers ([`point_codes`](Self::point_codes)), else their chunk, or
    /// the plan's scan read whole. Rows come back in the index's order, as
    /// the served streams read them, tombstoned rows dropped via the row-id
    /// column.
    ///
    /// Under a Dictionary decode layout only ([`ServeDecode::decodes_codes`]):
    /// the string layouts' copies hold terms, not codes.
    ///
    /// [`InMemoryServePlan::code_columns`]: super::InMemoryServePlan::code_columns
    pub(crate) async fn code_columns(
        &self,
        file: &NativeStoreFile,
        deleted: Option<&Mask>,
    ) -> Result<[Buffer<TermCode>; 4]> {
        match self.read(deleted)? {
            (PlanRead::Points(positions), deleted) => {
                self.codes_at(file, &positions, deleted).await
            }
            (PlanRead::Scan(scan), deleted) => self
                .decode
                .chunk_code_columns(&read_all_rows(scan).await?, deleted),
        }
    }

    /// The four code columns of the child rows `positions` (ascending, rows
    /// of the view's run, not empty), in that order, the `deleted` tombstones
    /// dropped: point by point straight into code buffers
    /// ([`point_codes`](Self::point_codes)), else off the rows' chunk.
    pub(crate) async fn codes_at(
        &self,
        file: &NativeStoreFile,
        positions: &Buffer<u64>,
        deleted: Option<&Mask>,
    ) -> Result<[Buffer<TermCode>; 4]> {
        if let Some(codes) = self.point_codes(file, positions, deleted).await? {
            return Ok(codes);
        }
        self.decode
            .chunk_code_columns(&self.rows_at(file, positions).await?, deleted)
    }

    /// The four code columns of the child rows `positions` (ascending, rows
    /// of the view's run), in that order, read point by point through the
    /// child's chunk probes straight into code buffers, with no chunk built:
    /// a column the plan's terms pin filled with its code, not read, and the
    /// row id read only under `deleted` tombstones, to drop those rows.
    /// `None` declines (a column or chunk without a probe, or a term that is
    /// not a code): the caller reads the rows' chunk.
    async fn point_codes(
        &self,
        file: &NativeStoreFile,
        positions: &[u64],
        deleted: Option<&Mask>,
    ) -> Result<Option<[Buffer<TermCode>; 4]>> {
        // Each quad column's code where the plan's terms pin it: every row
        // of the run holds it.
        let mut pinned: [Option<TermCode>; 4] = [None; 4];
        for (column, value) in self.constraints.iter() {
            let Some(k) = CHILD_COLUMNS[..4].iter().position(|c| c == column) else {
                continue;
            };
            let Some(code) = crate::store::array::unsigned_value(value) else {
                return Ok(None);
            };
            pinned[k] = Some(code);
        }
        let mut read: Vec<&'static str> = (0..4)
            .filter(|&k| pinned[k].is_none())
            .map(|k| CHILD_COLUMNS[k])
            .collect();
        if deleted.is_some() {
            read.push(COL_RID);
        }
        let Some(mut values) =
            component_point_values(file, self.child.name, &read, positions).await?
        else {
            return Ok(None);
        };
        observe!(ServedCodesRead(read));
        let live: Option<Vec<bool>> = deleted.map(|deleted| {
            let rids = values.pop().expect("the row id column was read");
            rids.iter()
                .map(|&rid| !deleted.value(base_row(rid) as usize))
                .collect()
        });
        let rows = live
            .as_ref()
            .map_or(positions.len(), |live| live.iter().filter(|&&l| l).count());
        let mut values = values.into_iter();
        Ok(Some(std::array::from_fn(|k| match pinned[k] {
            Some(code) => Buffer::from(vec![code; rows]),
            None => {
                let column = values.next().expect("every unpinned column was read");
                match &live {
                    None => Buffer::from(column),
                    Some(live) => column
                        .into_iter()
                        .zip(live)
                        .filter_map(|(value, &live)| live.then_some(value))
                        .collect(),
                }
            }
        })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_split_rows_floor_and_arithmetic() {
        // Small runs never split below the per-split floor, nor does a wide
        // one shared by many workers.
        for (range, workers) in [(0..100, 4), (5..5, 4), (10_000..20_000, 22)] {
            assert_eq!(
                run_split_rows(&range, workers),
                SERVE_SPLIT_MIN_ROWS as usize,
                "{range:?} over {workers}"
            );
        }

        // A wide run hands every worker a couple of splits: its width, not
        // where it sits, over twice the workers.
        for (range, workers, split) in [
            (10_000..20_000, 4, 1_250),
            (0..1 << 20, 8, 65_536),
            (1 << 20..1 << 21, 8, 65_536),
            (0..1 << 20, 22, 23_832),
        ] {
            assert_eq!(
                run_split_rows(&range, workers),
                split,
                "{range:?} over {workers}"
            );
        }
    }
}
