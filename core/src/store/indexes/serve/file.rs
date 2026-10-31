//! [`FileServePlan`]: a resolved file view's quads read out of the serving
//! index child's own columns, its located run by row range (or point by
//! point when small), else under the pushed-down term-equality filter.

use std::ops::Range;
use std::sync::Arc;

use futures::FutureExt;
use futures::future::BoxFuture;
use futures::stream::{self, BoxStream, StreamExt};
use vortex_array::arrays::PrimitiveArray;
use vortex_array::dtype::Nullability;
use vortex_array::expr::{col, pack, root, select};
use vortex_array::scalar::Scalar;
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray};
use vortex_buffer::Buffer;
use vortex_layout::layouts::row_idx::row_idx;
use vortex_layout::scan::scan_builder::ScanBuilder;
use vortex_layout::scan::split_by::SplitBy;
use vortex_mask::Mask;

use super::{ServeDecode, ServedRows};
use crate::error::{Result, VortexRdfError};
use crate::observe::observe;
use crate::session::VORTEX_SESSION;
use crate::store::SortOrder;
use crate::store::indexes::COL_RID;
use crate::store::indexes::copy::CHILD_COLUMNS;
use crate::store::layouts::{ChunkDecode, ResolvedLayout};
use crate::store::persist::native_file::{ChildReader, NativeStoreFile};
use crate::store::read::chunks::extract_codes;
use crate::store::scan::file_exprs::eq_conjunction;
use crate::store::scan::file_reads::{
    component_point_chunk, component_point_rows, component_point_values,
};
use crate::store::scan::file_scans::{read_all_rows, stream_rows};
use crate::store::schema::{COL_O, RowId, TermCode};
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
    constraints: Vec<(&'static str, Scalar)>,
    /// What the plan fills on first use, shared across clones so the first
    /// reader's work serves them all ([`PlanCells`]): one allocation a plan.
    cells: Arc<PlanCells>,
    /// The child rows the view covers, when the resolution located them by
    /// chunk probes: exactly constrained rows, the located run or the part of
    /// it a window cut, letting a small run be point-read
    /// ([`Self::point_run`]) and a wide one scanned by range
    /// ([`Self::scan`]) instead of filtered. `None` when unlocated, or when a
    /// constraint the location did not cover would make the range
    /// over-approximate.
    row_range: Option<Range<u64>>,
    /// The run the resolution located, which a window narrows `row_range`
    /// within; `None` when unlocated.
    run: Option<Range<u64>>,
    /// The rows a keep kept of the range, in the child's order; when set,
    /// the view covers these and not the whole range.
    kept: Option<Arc<ServedRows>>,
}

/// What a [`FileServePlan`] fills on first use, behind one `Arc` its clones
/// share.
#[derive(Default)]
struct PlanCells {
    /// The lazily bound (projection, filter) pair.
    bound: std::sync::OnceLock<(
        vortex_array::expr::BoundExpression,
        vortex_array::expr::BoundExpression,
    )>,
    /// An unlocated run's rows, read once by row index for its first
    /// narrowing ([`run_rows`](FileServePlan::run_rows)).
    run_rows: std::sync::OnceLock<ServedRows>,
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
    /// small run; see [`Self::point_run`]) or a range-restricted scan split
    /// across the workers (a wide one; see [`Self::scan`]).
    pub(in crate::store::indexes) fn new(
        decode: ServeDecode,
        child: ChildReader,
        constraints: Vec<(&'static str, Scalar)>,
        row_range: Option<Range<u64>>,
    ) -> Self {
        Self {
            decode,
            child,
            constraints,
            cells: Arc::default(),
            run: row_range.clone(),
            row_range,
            kept: None,
        }
    }

    /// The rows of an unlocated run in the child's order, which is the
    /// index's: one scan of the child under the plan's filter, projecting
    /// each row's child position (vortex's `#row_idx`) beside its base row
    /// id, read on the first call and kept for every clone of the plan. What
    /// a narrowing of a run the resolution did not locate starts from.
    pub(crate) async fn run_rows(&self) -> Result<ServedRows> {
        if let Some(rows) = self.cells.run_rows.get() {
            return Ok(rows.clone());
        }
        observe!(ServedRunLocated);
        let (_, filter) = self.bound_exprs()?;
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
        let rids = columns
            .remove(1)
            .iter()
            .map(|&rid| crate::store::indexes::base_row(rid))
            .collect();
        let rows = ServedRows {
            positions: columns.remove(0),
            rids,
        };
        Ok(self.cells.run_rows.get_or_init(|| rows).clone())
    }

    /// This plan with its run located, what a narrowing cuts: a located or
    /// kept plan as it is; an unlocated run read once by row index
    /// ([`run_rows`](Self::run_rows)), then the child range its rows span
    /// when they are contiguous there (a run no residual term filters, such
    /// as a string layout's), else kept rows.
    pub(crate) async fn located(&self) -> Result<Self> {
        if self.kept.is_some() || self.row_range.is_some() {
            return Ok(self.clone());
        }
        let rows = self.run_rows().await?;
        Ok(match (rows.positions.first(), rows.positions.last()) {
            (Some(&first), Some(&last)) if last - first + 1 == rows.len() as u64 => Self {
                row_range: Some(first..last + 1),
                ..self.clone()
            },
            _ => self.with_rows(rows),
        })
    }

    /// The child rows the view covers, when located: the run the resolution
    /// located, or the part of it a window cut. `None` for an unlocated run.
    pub(crate) fn located_range(&self) -> Option<Range<u64>> {
        self.row_range.clone()
    }

    /// The rows a keep kept, when the view covers those and not its whole
    /// range.
    pub(crate) fn kept(&self) -> Option<&ServedRows> {
        self.kept.as_deref()
    }

    /// Whether a window, keep, partition or chained match narrowed the view
    /// below the run the match resolved: it keeps rows, or covers a range
    /// other than the resolution's (an unlocated run's narrowing locates
    /// it).
    pub(crate) fn is_narrowed(&self) -> bool {
        self.kept.is_some() || self.row_range != self.run
    }

    /// This plan over `range`, a part of its located range: a window's of a
    /// view that keeps no rows.
    pub(crate) fn narrowed(&self, range: Range<u64>) -> Self {
        debug_assert!(self.kept.is_none());
        debug_assert!(
            self.row_range
                .as_ref()
                .is_some_and(|run| run.start <= range.start && range.end <= run.end)
        );
        Self {
            row_range: Some(range),
            ..self.clone()
        }
    }

    /// This plan over `rows`, rows of its range: a keep's, or a window or
    /// partition of them.
    pub(crate) fn with_rows(&self, rows: ServedRows) -> Self {
        Self {
            kept: Some(Arc::new(rows)),
            ..self.clone()
        }
    }

    /// The serving index child, which the view's lazy ids read.
    pub(crate) fn child(&self) -> &ChildReader {
        &self.child
    }

    /// The child's `columns` at the child rows `positions` (ascending, not
    /// empty), in that order: point by point through the child's cached
    /// chunk probes, or, where a probe declines, the rows they span scanned
    /// and the positions taken.
    pub(crate) async fn columns_at(
        &self,
        file: &NativeStoreFile,
        columns: &[&'static str],
        positions: &[u64],
    ) -> Result<ArrayRef> {
        if let Some(rows) =
            component_point_rows(file, self.child.name, columns, positions.to_vec()).await?
        {
            return Ok(rows);
        }
        let (first, last) = (positions[0], positions[positions.len() - 1]);
        let projection = self.child.bind(&select(columns.to_vec(), root()))?;
        let span = read_all_rows(
            self.child_scan()
                .with_projection(projection)
                .with_row_range(first..last + 1),
        )
        .await?;
        let indices: Buffer<u64> = positions.iter().map(|&position| position - first).collect();
        span.take(PrimitiveArray::new(indices, Validity::NonNullable).into_array())
            .map_err(VortexRdfError::Vortex)
    }

    /// The child's rows at `positions` (ascending, not empty), every child
    /// column, in that order ([`columns_at`](Self::columns_at)).
    pub(crate) async fn rows_at(
        &self,
        file: &NativeStoreFile,
        positions: &[u64],
    ) -> Result<ArrayRef> {
        self.columns_at(file, &CHILD_COLUMNS, positions).await
    }

    /// The child's `columns` over the located range, as `u64` buffers in the
    /// child's order: point by point when the range is point-sized, else a
    /// scan of the range projecting them.
    pub(crate) async fn range_columns(
        &self,
        file: &NativeStoreFile,
        columns: &[&'static str],
    ) -> Result<Vec<Buffer<u64>>> {
        let range = self
            .row_range
            .clone()
            .expect("only a located range's columns are read");
        if range.is_empty() {
            return Ok(columns.iter().map(|_| Buffer::empty()).collect());
        }
        let rows = if point_sized(range.end - range.start) {
            let positions: Vec<u64> = range.collect();
            self.columns_at(file, columns, &positions).await?
        } else {
            let projection = self.child.bind(&select(columns.to_vec(), root()))?;
            read_all_rows(
                self.child_scan()
                    .with_projection(projection)
                    .with_row_range(range),
            )
            .await?
        };
        extract_codes(rows, columns)
    }

    /// The view's live rows in the child's order: the kept rows, the located
    /// range's (its rid column read), or an unlocated run's
    /// ([`run_rows`](Self::run_rows)), each with its base row id, the
    /// `deleted` tombstones left out.
    pub(crate) async fn live_rows(
        &self,
        file: &NativeStoreFile,
        deleted: Option<&Mask>,
    ) -> Result<ServedRows> {
        if let Some(kept) = &self.kept {
            return Ok(kept.live(deleted));
        }
        let Some(range) = self.row_range.clone() else {
            return Ok(self.run_rows().await?.live(deleted));
        };
        let rids = self.range_columns(file, &[COL_RID]).await?.remove(0);
        let (positions, rids): (Vec<u64>, Vec<RowId>) = range
            .zip(rids.iter().map(|&rid| crate::store::indexes::base_row(rid)))
            .filter(|&(_, rid)| deleted.is_none_or(|d| !d.value(rid as usize)))
            .unzip();
        Ok(ServedRows {
            positions: Buffer::from(positions),
            rids: Buffer::from(rids),
        })
    }

    /// The plan's (projection, filter), bound through the handle's memo on
    /// first use and shared across clones thereafter.
    fn bound_exprs(
        &self,
    ) -> Result<(
        vortex_array::expr::BoundExpression,
        vortex_array::expr::BoundExpression,
    )> {
        if let Some(bound) = self.cells.bound.get() {
            return Ok(bound.clone());
        }
        // The serving child's quad columns plus the row-id column, for
        // tombstones: the copy family's child schema.
        let projection = select(CHILD_COLUMNS, root());
        // A serve plan always carries at least one constraint (the resolved
        // lead component), so the conjunction is never empty.
        let filter = eq_conjunction(self.constraints.iter().cloned())
            .expect("a serve plan constrains at least one column");
        let bound_projection = self.child.bind(&projection)?;
        let bound_filter = self.child.bind(&filter)?;
        Ok(self
            .cells
            .bound
            .get_or_init(|| (bound_projection, bound_filter))
            .clone())
    }

    pub(crate) fn component(&self) -> &'static str {
        self.child.name
    }

    /// The order the view's reads follow, the child's: `(p, o, s, g)` for
    /// an `index:posg` run whose terms leave the object unbound, else base
    /// order (see [`served_order`](super::served_order)). `None` for
    /// a child whose rows are not globally sorted, read in its own order.
    pub(crate) fn read_order(&self) -> Option<SortOrder> {
        let object_bound = self.constraints.iter().any(|&(column, _)| column == COL_O);
        self.child
            .sorted
            .then(|| super::served_order(self.child.name, object_bound))
    }

    /// The located child-row range the constraints select, when known.
    #[cfg(test)]
    pub(crate) fn row_range(&self) -> Option<Range<u64>> {
        self.row_range.clone()
    }

    /// The served rows read point by point as one chunk, when the view keeps
    /// rows ([`rows_at`](Self::rows_at) their positions) or covers a located,
    /// point-sized range: the projected columns through the component's
    /// cached chunk probes ([`component_point_chunk`]), or the range's
    /// [`scan`](Self::scan) when a probe declines a run's read mid-fetch.
    /// `None` for every other view, which [`Self::scan`] streams. The plan is
    /// bound before the read starts, so a bind failure fails this call rather
    /// than the read.
    pub(crate) fn point_run<'a>(
        &'a self,
        file: &'a NativeStoreFile,
    ) -> Result<Option<BoxFuture<'a, Result<ArrayRef>>>> {
        if let Some(kept) = &self.kept {
            self.bound_exprs()?;
            let positions = kept.positions.clone();
            return Ok(Some(
                async move { self.rows_at(file, &positions).await }.boxed(),
            ));
        }
        let range = match &self.row_range {
            Some(run) if point_sized(run.end - run.start) => run.clone(),
            _ => return Ok(None),
        };
        let fallback = self.scan()?;
        Ok(Some(
            async move {
                match component_point_chunk(file, self.child.name, &CHILD_COLUMNS, range).await? {
                    Some(rows) => Ok(rows),
                    None => read_all_rows(fallback).await,
                }
            }
            .boxed(),
        ))
    }

    /// The scan the served rows stream from: the serving index child with
    /// the plan's projection (bound on first use), over exactly the located
    /// run, split by row count, or, for an unlocated run, the
    /// [`filtered_scan`](Self::filtered_scan).
    ///
    /// The scan spawns one task per split and the consumer decodes each
    /// chunk inside its task, so the split count is the decode's
    /// parallelism. The child's natural splits are its leaf chunks, which
    /// cluster a run into one split however wide it is; splitting the range
    /// by row count spreads the run's decode over the workers instead
    /// (`run_split_rows`). No filter rides along a located run: its range is
    /// exactly the constrained rows (the same fact `size` and the point
    /// reads rely on).
    pub(crate) fn scan(&self) -> Result<ScanBuilder<ArrayRef>> {
        let Some(range) = self.row_range.clone() else {
            return self.filtered_scan();
        };
        let (projection, _) = self.bound_exprs()?;
        let workers = crate::io::read::available_parallelism() as u64;
        let split_rows = run_split_rows(&range, workers);
        Ok(self
            .child_scan()
            .with_projection(projection)
            .with_row_range(range)
            .with_split_by(SplitBy::RowCount(split_rows)))
    }

    /// A scan over the serving index child, to which the plan's projection
    /// and filter apply.
    fn child_scan(&self) -> ScanBuilder<ArrayRef> {
        ScanBuilder::new(VORTEX_SESSION.clone(), self.child.reader.clone())
    }

    /// [`Self::child_scan`] with the plan's projection and pushed-down
    /// filter (bound on first use) applied: an unlocated run's scan, and the
    /// fallback of a point read a probe declines.
    fn filtered_scan(&self) -> Result<ScanBuilder<ArrayRef>> {
        let (projection, filter) = self.bound_exprs()?;
        Ok(self
            .child_scan()
            .with_projection(projection)
            .with_filter(filter))
    }

    /// The served rows' four [`TermCode`] columns, read off the index child's
    /// own columns: the file counterpart of
    /// [`InMemoryServePlan::code_columns`]. A point-sized located run is
    /// read point by point ([`point_run`](Self::point_run)), any other by the
    /// plan's [`scan`](Self::scan). Rows come back in the index's order, as
    /// the served streams read them, tombstoned rows dropped via the row-id
    /// column. `None` declines to the caller's gather path: a non-Dictionary
    /// decode layout, whose columns hold terms, not codes.
    ///
    /// [`InMemoryServePlan::code_columns`]: super::InMemoryServePlan::code_columns
    pub(crate) async fn code_columns(
        &self,
        file: &NativeStoreFile,
        deleted: Option<&Mask>,
    ) -> Result<Option<[Buffer<TermCode>; 4]>> {
        if !matches!(self.decode.decode_layout, ResolvedLayout::Dictionary(_)) {
            return Ok(None);
        }
        if let Some(positions) = self.point_positions()
            && let Some(codes) = self.point_codes(file, &positions, deleted).await?
        {
            return Ok(Some(codes));
        }
        let chunk = match self.point_run(file)? {
            Some(read) => read.await?,
            None => read_all_rows(self.scan()?).await?,
        };
        self.decode.chunk_code_columns(&chunk, deleted).map(Some)
    }

    /// The child rows a code read takes point by point: the rows a keep
    /// kept, or a located point-sized run's. `None` for any other view, which
    /// the plan's [`scan`](Self::scan) reads.
    pub(crate) fn point_positions(&self) -> Option<Buffer<u64>> {
        match (&self.kept, &self.row_range) {
            (Some(kept), _) => Some(kept.positions.clone()),
            (None, Some(run)) if point_sized(run.end - run.start) => Some(run.clone().collect()),
            _ => None,
        }
    }

    /// The four code columns of the child rows `positions` (ascending, rows
    /// of the view's run), in that order, read point by point through the
    /// child's chunk probes straight into code buffers, with no chunk built:
    /// a column the plan's terms pin filled with its code, not read, and the
    /// row id read only under `deleted` tombstones, to drop those rows.
    /// `None` declines (a column or chunk without a probe, or a term that is
    /// not a code): the caller reads the rows' chunk.
    pub(crate) async fn point_codes(
        &self,
        file: &NativeStoreFile,
        positions: &[u64],
        deleted: Option<&Mask>,
    ) -> Result<Option<[Buffer<TermCode>; 4]>> {
        // Each quad column's code where the plan's terms pin it: every row
        // of the run holds it.
        let mut pinned: [Option<TermCode>; 4] = [None; 4];
        for (column, value) in &self.constraints {
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
            component_point_values(file, self.child.name, &read, positions.to_vec()).await?
        else {
            return Ok(None);
        };
        observe!(ServedCodesRead(read));
        let live: Option<Vec<bool>> = deleted.map(|deleted| {
            let rids = values.pop().expect("the row id column was read");
            rids.iter()
                .map(|&rid| !deleted.value(crate::store::indexes::base_row(rid) as usize))
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

    /// The served rows as chunks of the child's columns, in the index's
    /// order: kept rows read point by point, `batch_rows` a chunk; a
    /// point-sized located run read point by point as one chunk
    /// ([`point_run`](Self::point_run)); any other the plan's
    /// [`scan`](Self::scan), split by split. Nothing is read until the
    /// stream is polled.
    pub(crate) fn child_chunks(
        &self,
        file: Arc<NativeStoreFile>,
        batch_rows: usize,
    ) -> Result<BoxStream<'static, Result<ArrayRef>>> {
        if let Some(kept) = &self.kept {
            let (plan, positions) = (self.clone(), kept.positions.clone());
            let rows = positions.len();
            return Ok(stream::iter((0..rows).step_by(batch_rows))
                .then(move |start| {
                    let (plan, file) = (plan.clone(), Arc::clone(&file));
                    let end = start.saturating_add(batch_rows).min(rows);
                    let batch = positions.slice(start..end);
                    async move { plan.rows_at(&file, &batch).await }
                })
                .boxed());
        }
        if self
            .row_range
            .as_ref()
            .is_some_and(|run| point_sized(run.end - run.start))
        {
            let plan = self.clone();
            return Ok(stream::once(async move {
                match plan.point_run(&file)? {
                    Some(read) => read.await,
                    None => unreachable!("a point-sized located run reads point by point"),
                }
            })
            .boxed());
        }
        stream_rows(self.scan()?)
    }

    /// A child chunk's live rows as the four `(s, p, o, g)` code columns, in
    /// the chunk's order, tombstoned rows dropped: a served code stream's
    /// chunk.
    pub(crate) fn chunk_codes(
        &self,
        chunk: &ArrayRef,
        deleted: Option<&Mask>,
    ) -> Result<[Buffer<TermCode>; 4]> {
        self.decode.chunk_code_columns(chunk, deleted)
    }

    /// A child chunk's live rows as a primary-named `(s, p, o, g)` struct, in
    /// the chunk's order, tombstoned rows dropped: a served row stream's
    /// chunk.
    pub(crate) fn chunk_primary_rows(
        &self,
        chunk: &ArrayRef,
        deleted: Option<&Mask>,
    ) -> Result<ArrayRef> {
        self.decode.chunk_rows(chunk, deleted)
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
    /// decode, for a file-backed dictionary: each chunk's codes are resolved
    /// by reading the dictionary windows that hold them.
    pub(crate) async fn decode_columns_async<T: ChunkDecode>(
        &self,
        chunk: &ArrayRef,
        deleted: Option<&Mask>,
    ) -> Vec<Result<T>> {
        self.decode.decode_columns_async(chunk, deleted).await
    }

    /// [`decode_columns_async`](Self::decode_columns_async) where it
    /// finishes without waiting, for a scan's split task; `None` leaves the
    /// chunk to it.
    pub(crate) fn decode_columns_now<T: ChunkDecode>(
        &self,
        chunk: &ArrayRef,
        deleted: Option<&Mask>,
    ) -> Option<Vec<Result<T>>> {
        self.decode.decode_columns_now(chunk, deleted)
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
