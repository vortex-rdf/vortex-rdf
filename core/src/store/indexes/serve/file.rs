//! [`FileServePlan`]: a resolved file view's quads read out of the serving
//! index child's own columns, its located run by row range (or point by
//! point when small), else under the pushed-down term-equality filter.

use std::ops::Range;
use std::sync::Arc;

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

use super::{ServeDecode, ServedRows, ServedSpan};
use crate::error::{Result, VortexRdfError};
use crate::observe::observe;
use crate::session::VORTEX_SESSION;
use crate::store::SortOrder;
use crate::store::indexes::COL_RID;
use crate::store::indexes::copy::CHILD_COLUMNS;
use crate::store::persist::native_file::{ChildReader, NativeStoreFile};
use crate::store::read::chunks::extract_codes;
use crate::store::scan::file_exprs::eq_conjunction;
use crate::store::scan::file_reads::{component_point_rows, component_point_values};
use crate::store::scan::file_scans::{read_all_rows, stream_rows};
use crate::store::schema::{COL_O, TermCode};
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
    /// The kept rows, each live, read point by point at these child rows.
    Kept(Buffer<u64>),
    /// A located range of at most [`POINT_GATHER_MAX_ROWS`] rows, read point
    /// by point at these child rows; its tombstoned rows still to drop.
    ///
    /// [`POINT_GATHER_MAX_ROWS`]: crate::store::view::selection::POINT_GATHER_MAX_ROWS
    Points(Buffer<u64>),
    /// A wider located range by row range, split across the workers, or an
    /// unlocated run under the plan's filter; its tombstoned rows still to
    /// drop.
    Scan(ScanBuilder<ArrayRef>),
}

impl PlanRead {
    /// The tombstones a read of these rows still drops, of the view's
    /// `deleted`: none for kept rows, which are live.
    pub(crate) fn tombstones<'a>(&self, deleted: Option<&'a Mask>) -> Option<&'a Mask> {
        match self {
            PlanRead::Kept(_) => None,
            PlanRead::Points(_) | PlanRead::Scan(_) => deleted,
        }
    }
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
    /// An unlocated run's rows as its first narrowing locates them
    /// ([`located_span`](FileServePlan::located_span)), read once by row
    /// index for every narrowing after.
    located: std::sync::OnceLock<ServedSpan>,
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
    /// each row's child position (vortex's `#row_idx`) beside its base row
    /// id.
    async fn read_run_rows(&self) -> Result<ServedRows> {
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
        Ok(ServedRows {
            positions: columns.remove(0),
            rids,
        })
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
        let rows = self.read_run_rows().await?;
        let span = match (rows.positions.first(), rows.positions.last()) {
            (Some(&first), Some(&last)) if last - first + 1 == rows.len() as u64 => {
                ServedSpan::Range(first..last + 1)
            }
            _ => ServedSpan::Kept(Arc::new(rows.live(deleted))),
        };
        Ok(self.cells.located.get_or_init(|| span).clone())
    }

    /// The located child rows the view covers, a range: the run, or the part
    /// of it a window cut. `None` for an unlocated run or kept rows.
    #[cfg(test)]
    pub(crate) fn located_range(&self) -> Option<Range<u64>> {
        match self.span()? {
            ServedSpan::Range(range) => Some(range),
            ServedSpan::Kept(_) => None,
        }
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
        positions: &Buffer<u64>,
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
        positions: &Buffer<u64>,
    ) -> Result<ArrayRef> {
        self.columns_at(file, &CHILD_COLUMNS, positions).await
    }

    /// The child's `columns` over the child rows `range`, as `u64` buffers in
    /// the child's order: point by point when the range is point-sized, else
    /// a scan of the range projecting them.
    pub(crate) async fn range_columns(
        &self,
        file: &NativeStoreFile,
        range: Range<u64>,
        columns: &[&'static str],
    ) -> Result<Vec<Buffer<u64>>> {
        if range.is_empty() {
            return Ok(columns.iter().map(|_| Buffer::empty()).collect());
        }
        let rows = if point_sized(range.end - range.start) {
            let positions: Buffer<u64> = range.collect();
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

    /// The child rows `range` with their base row ids (the range's rid
    /// column read), less the `deleted` tombstones, in the child's order.
    pub(crate) async fn range_rows(
        &self,
        file: &NativeStoreFile,
        range: Range<u64>,
        deleted: Option<&Mask>,
    ) -> Result<ServedRows> {
        let rids = self
            .range_columns(file, range.clone(), &[COL_RID])
            .await?
            .remove(0);
        let rows = range
            .clone()
            .zip(rids.iter().map(|&rid| crate::store::indexes::base_row(rid)));
        Ok(ServedRows::collect(
            rows,
            (range.end - range.start) as usize,
            |_, rid| deleted.is_none_or(|d| !d.value(rid as usize)),
        ))
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

    /// How the plan reads the view's rows, decided once from what it covers:
    /// the kept rows, or a located range of at most
    /// [`POINT_GATHER_MAX_ROWS`] rows, point by point; a wider located range
    /// by row range, split across the workers (`run_split_rows`); an
    /// unlocated run under the plan's filter. The plan is bound before the
    /// read starts, so a bind failure fails this call rather than the read.
    ///
    /// The scan spawns one task per split and the consumer decodes each
    /// chunk inside its task, so the split count is the decode's
    /// parallelism. The child's natural splits are its leaf chunks, which
    /// cluster a run into one split however wide it is; splitting the range
    /// by row count spreads the run's decode over the workers instead. No
    /// filter rides along a located run: its range is exactly the
    /// constrained rows (the same fact `size` and the point reads rely on).
    ///
    /// [`POINT_GATHER_MAX_ROWS`]: crate::store::view::selection::POINT_GATHER_MAX_ROWS
    pub(crate) fn read(&self) -> Result<PlanRead> {
        Ok(match self.span() {
            Some(ServedSpan::Kept(kept)) => {
                self.bound_exprs()?;
                PlanRead::Kept(kept.positions.clone())
            }
            Some(ServedSpan::Range(range)) if point_sized(range.end - range.start) => {
                self.bound_exprs()?;
                PlanRead::Points(range.collect())
            }
            Some(ServedSpan::Range(range)) => {
                let (projection, _) = self.bound_exprs()?;
                let workers = crate::io::read::available_parallelism() as u64;
                let split_rows = run_split_rows(&range, workers);
                PlanRead::Scan(
                    self.child_scan()
                        .with_projection(projection)
                        .with_row_range(range)
                        .with_split_by(SplitBy::RowCount(split_rows)),
                )
            }
            None => PlanRead::Scan(self.filtered_scan()?),
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
        let (projection, filter) = self.bound_exprs()?;
        Ok(self
            .child_scan()
            .with_projection(projection)
            .with_filter(filter))
    }

    /// The served rows' four [`TermCode`] columns, read off the index child's
    /// own columns: the file counterpart of
    /// [`InMemoryServePlan::code_columns`], read as the plan reads its rows
    /// ([`read`](Self::read)): rows taken point by point straight into code
    /// buffers ([`point_codes`](Self::point_codes)), else their chunk, or
    /// the plan's scan read whole. Rows come back in the index's order, as
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
        if !self.decode.decodes_codes() {
            return Ok(None);
        }
        let read = self.read()?;
        let deleted = read.tombstones(deleted);
        let chunk = match read {
            PlanRead::Kept(positions) | PlanRead::Points(positions) => {
                if let Some(codes) = self.point_codes(file, &positions, deleted).await? {
                    return Ok(Some(codes));
                }
                self.rows_at(file, &positions).await?
            }
            PlanRead::Scan(scan) => read_all_rows(scan).await?,
        };
        self.decode.chunk_code_columns(&chunk, deleted).map(Some)
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
    /// order, as `read` (the plan's [`read`](Self::read)) takes them: rows
    /// taken point by point `batch_rows` a chunk, or the plan's scan split by
    /// split. Nothing is read until the stream is polled.
    pub(crate) fn read_chunks(
        &self,
        read: PlanRead,
        file: Arc<NativeStoreFile>,
        batch_rows: usize,
    ) -> Result<BoxStream<'static, Result<ArrayRef>>> {
        Ok(match read {
            PlanRead::Kept(positions) | PlanRead::Points(positions) => {
                let plan = self.clone();
                stream::iter(crate::store::read::chunks::batch_ranges(
                    positions.len(),
                    batch_rows,
                ))
                .then(move |batch| {
                    let (plan, file) = (plan.clone(), Arc::clone(&file));
                    let batch = positions.slice(batch);
                    async move { plan.rows_at(&file, &batch).await }
                })
                .boxed()
            }
            PlanRead::Scan(scan) => stream_rows(scan)?,
        })
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
