//! The file side of the index hub: the one file resolver every index's probe
//! runs through ([`resolve_file`]); the readers it and the deferred ids use
//! (point reads through cached chunk probes, rid-only pushed-down scans) to
//! produce a resolution's ascending, unique `Buffer<RowId>` of base rows
//! from an index child; and [`FileServePlan`], which reads a resolved view's
//! quads out of the child's own columns.

use std::ops::Range;
use std::sync::Arc;

use futures::FutureExt;
use futures::future::BoxFuture;
use futures::stream::{self, BoxStream, StreamExt};
use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::struct_::{StructArray, StructArrayExt};
use vortex_array::dtype::Nullability;
use vortex_array::expr::{col, pack, root, select};
use vortex_array::scalar::Scalar;
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute};
use vortex_buffer::Buffer;
use vortex_layout::layouts::row_idx::row_idx;
use vortex_layout::scan::scan_builder::ScanBuilder;
use vortex_layout::scan::split_by::SplitBy;
use vortex_mask::Mask;

use super::components::{check_addressable, sorted_row_ids};
use super::copy::CHILD_COLUMNS;
use super::resolve::IndexProbe;
use super::serve::{ServeDecode, ServedRows};
use super::{COL_RID, IndexResolution, LazyRowIds, ResolvedRoles, ResolvedRowIds};
use crate::error::{Result, VortexRdfError};
use crate::observe::observe;
use crate::session::VORTEX_SESSION;
use crate::store::SortOrder;
use crate::store::layouts::{ChunkDecode, PatternCodes, ResolvedLayout};
#[cfg(test)]
use crate::store::persist::native_file::FileCounter;
use crate::store::persist::native_file::{ChildReader, NativeStoreFile};
use crate::store::read::chunks::extract_codes;
use crate::store::scan::file_exprs::eq_conjunction;
use crate::store::scan::file_reads::{component_point_chunk, component_point_rows, values_at};
use crate::store::scan::file_scans::{read_all_rows, stream_rows};
use crate::store::schema::{COL_O, RowId, TermCode};
use crate::store::view::selection::point_sized;

/// The `[lo, hi)` run of a sorted component column's rows equal to `native`,
/// located by binary search over the column's cached chunk probes, reading
/// only the chunk leaves the bisection crosses. Searched over the whole
/// column, or `within` a row range whose slice of the column is itself sorted
/// (a lead run for a prefix probe).
///
/// `Ok(None)` declines the location (the caller keeps its pushed-down scan):
/// a child not globally sorted, a probe value that is not an integer (string
/// value columns), or a column whose chunks resolve no probe.
async fn locate_component_run(
    file: &NativeStoreFile,
    child: &ChildReader,
    column: &str,
    native: &Scalar,
    within: Option<Range<u64>>,
) -> Result<Option<Range<u64>>> {
    if !child.sorted {
        return Ok(None);
    }
    let Some(needle) = crate::store::array::unsigned_value(native) else {
        return Ok(None);
    };
    let Some(chunks) = file.component_column_chunks(child.name, column) else {
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

/// The row ids of a located index-child run, read point by point from the
/// child's rid column through its cached chunk probes and re-sorted into
/// base row order: the file counterpart of slicing an in-memory rid run.
/// `Ok(None)` when the rid column's chunks decline (the caller keeps its
/// scan). Rids are unique by construction, so sorting alone suffices; a row
/// past what a `usize` holds is refused.
async fn rid_point_reads(
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

/// An eager file resolution off the rid-only pushed-down scan: the file
/// resolver's answer for a non-serving probe whose ids no located run of
/// ascending rids defers. `Empty` when the scan proves it matches nothing.
async fn resolve_eager_from_scan(
    child: &ChildReader,
    constraints: &[(&'static str, Scalar)],
    resolves: ResolvedRoles,
) -> Result<IndexResolution<FileServePlan>> {
    let row_ids = scan_index_row_ids(child, constraints).await?;
    if row_ids.is_empty() {
        return Ok(IndexResolution::Empty);
    }
    Ok(IndexResolution::Resolved {
        row_ids: ResolvedRowIds::Eager(row_ids),
        resolves,
        serve: None,
    })
}

/// `probe` resolved against a file's index children: the file counterpart of
/// `resolve::resolve_in_memory`, reaching the probed child's columns through
/// their cached chunk probes (the file mirror of the in-memory binary search)
/// or, failing that, a pushed-down scan, whose filter answers whatever the
/// child's sortedness.
///
/// On a globally sorted child the keys' rows are one contiguous run, located
/// key by key, each search windowed to the run of the keys before it; a
/// located empty run proves the pattern matches nothing. What answers the
/// row ids then depends on the probe:
/// - A serving probe's ids are a deferred rid-only scan filtered by its keys,
///   which a located run's width counts (a small located run's are read now
///   by rid point reads, a handful of cached-chunk accesses), and a
///   [`FileServePlan`] reads the matched quads from the child: the located
///   run, or the rows its keys and residual terms filter.
/// - A non-serving probe whose rids ascend keeps a located run's ids deferred
///   ([`LazyRowIds::from_located_run`]), so that a count takes the run's
///   width and a window reads only its own rows.
/// - Any other non-serving resolution answers eager ids off the pushed-down
///   scan ([`resolve_eager_from_scan`]).
///
/// `Declined` when the probed child is absent (a foreign writer could omit
/// one); `Empty` when a key or residual term is absent from the dictionary.
pub(super) async fn resolve_file(
    probe: IndexProbe<'_>,
    file: &Arc<NativeStoreFile>,
    codes: &mut PatternCodes,
) -> Result<IndexResolution<FileServePlan>> {
    let (child, keys, located) = match locate(file, &probe, codes).await? {
        Located::Declined => return Ok(IndexResolution::Declined),
        Located::Absent => return Ok(IndexResolution::Empty),
        Located::Run { child, keys, range } => (child, keys, range),
    };
    // A located empty run proves the keys' combination absent from the data:
    // the short-circuit a scan reaches only after reading it.
    if located.as_ref().is_some_and(Range::is_empty) {
        return Ok(IndexResolution::Empty);
    }
    let Some(decode) = probe.serve else {
        return match located {
            // The located range is exactly the keys' rows. Its ids are read
            // only when a consumer needs them.
            Some(range) if probe.rids_ascend => Ok(IndexResolution::Resolved {
                row_ids: ResolvedRowIds::Lazy(LazyRowIds::from_located_run(
                    Arc::clone(file),
                    child,
                    range,
                )),
                resolves: probe.resolves,
                serve: None,
            }),
            _ => resolve_eager_from_scan(&child, &keys, probe.resolves).await,
        };
    };
    // The plan filters on every bound term the child holds: the keys, then
    // the residual terms. A residual term absent from the dictionary matches
    // nothing.
    let mut constraints = keys.clone();
    for &(column, term) in &probe.residual {
        let Some(native) = codes.probe_scalar(term)? else {
            return Ok(IndexResolution::Empty);
        };
        constraints.push((column, native));
    }
    // A small located run's row ids are read now by rid point reads, a
    // handful of cached-chunk accesses, instead of deferring a whole child
    // scan that a count or a chained match would then pay for.
    let point_read = match &located {
        Some(range) if point_sized(range.end - range.start) => {
            rid_point_reads(file, &child, range.clone()).await?
        }
        _ => None,
    };
    // A located run is exactly the keys' rows, so its width counts the
    // deferred ids whatever residual terms the plan filters on.
    let row_ids = match point_read {
        Some(ids) => ResolvedRowIds::Eager(ids),
        None => ResolvedRowIds::Lazy(LazyRowIds::from_index_child_scan(
            child.clone(),
            keys,
            located.clone(),
        )),
    };
    // A located range is exactly the constrained rows only when the location
    // covered every constraint: it searched the keys alone, so a residual
    // term (never a searched key) demotes the range back to the filter scan.
    let row_range = if probe.residual.is_empty() {
        located
    } else {
        None
    };
    Ok(IndexResolution::Resolved {
        row_ids,
        resolves: probe.resolves,
        // A serving resolution reads the matched quads straight from the
        // child's columns: point reads or a range scan over a located run,
        // or the pushed-down filter scan.
        serve: Some(FileServePlan::new(decode, child, constraints, row_range)),
    })
}

/// The file resolver's front half: the probed child, the keys as its native
/// probe values, and the run of child rows they select when the child's
/// cached chunk probes locate it.
enum Located {
    /// The probed child is absent (a foreign writer could omit one).
    Declined,
    /// A key's term is absent from the dictionary: nothing can match.
    Absent,
    /// The probe applies; `range` is the keys' run when located, `None` when
    /// the location declined (an unsorted child, a string value column, a
    /// chunk resolving no probe) and the pushed-down scan answers instead.
    Run {
        child: ChildReader,
        keys: Vec<(&'static str, Scalar)>,
        range: Option<Range<u64>>,
    },
}

/// Open `probe`'s child, translate its keys and locate their run through the
/// child's chunk probes: the first key over the whole child, each later one
/// windowed to the run before it. A declined search abandons the location
/// wholesale, and an empty run ends it, as no later key can widen it. The
/// shared front half of [`resolve_file`] and the test hook
/// `debug_located_run`.
async fn locate(
    file: &NativeStoreFile,
    probe: &IndexProbe<'_>,
    codes: &mut PatternCodes,
) -> Result<Located> {
    let Some(child) = file.child_reader(probe.identity.name)? else {
        return Ok(Located::Declined);
    };
    let mut keys = Vec::with_capacity(probe.keys.len());
    for &(column, term) in &probe.keys {
        let Some(native) = codes.probe_scalar(term)? else {
            return Ok(Located::Absent);
        };
        keys.push((column, native));
    }
    let mut range = None;
    for (column, native) in &keys {
        range = locate_component_run(file, &child, column, native, range).await?;
        match &range {
            Some(run) if !run.is_empty() => {}
            _ => break,
        }
    }
    Ok(Located::Run { child, keys, range })
}

/// Test-only hook exposing the run [`resolve_file`] locates when `index`
/// resolves `pattern` (`None` when the index declines the pattern, its child
/// is absent, a key's term is absent, or the location declines and the
/// resolution falls back to its pushed-down scan), so tests can assert
/// engagement instead of inferring it from results.
#[cfg(test)]
pub(crate) async fn debug_located_run(
    index: super::IndexType,
    file: &NativeStoreFile,
    layout: &ResolvedLayout,
    pattern: crate::store::layouts::QuadPattern<'_>,
    codes: &mut PatternCodes,
) -> Result<Option<Range<u64>>> {
    let Some(probe) = index.choose(pattern, layout) else {
        return Ok(None);
    };
    Ok(match locate(file, &probe, codes).await? {
        Located::Run { range, .. } => range,
        Located::Declined | Located::Absent => None,
    })
}

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
    /// The lazily bound (projection, filter) pair, shared across clones so
    /// the first reader's bind serves them all.
    bound: Arc<
        std::sync::OnceLock<(
            vortex_array::expr::BoundExpression,
            vortex_array::expr::BoundExpression,
        )>,
    >,
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
    /// An unlocated run's rows, read once by row index for its first
    /// narrowing ([`run_rows`](Self::run_rows)), shared across clones.
    run_rows_cell: Arc<std::sync::OnceLock<ServedRows>>,
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
    fn new(
        decode: ServeDecode,
        child: ChildReader,
        constraints: Vec<(&'static str, Scalar)>,
        row_range: Option<Range<u64>>,
    ) -> Self {
        Self {
            decode,
            child,
            constraints,
            bound: Arc::new(std::sync::OnceLock::new()),
            run: row_range.clone(),
            row_range,
            kept: None,
            run_rows_cell: Arc::new(std::sync::OnceLock::new()),
        }
    }

    /// The rows of an unlocated run in the child's order, which is the
    /// index's: one scan of the child under the plan's filter, projecting
    /// each row's child position (vortex's `#row_idx`) beside its base row
    /// id, read on the first call and kept for every clone of the plan. What
    /// a narrowing of a run the resolution did not locate starts from.
    pub(crate) async fn run_rows(&self) -> Result<ServedRows> {
        if let Some(rows) = self.run_rows_cell.get() {
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
            .map(|&rid| super::base_row(rid))
            .collect();
        let rows = ServedRows {
            positions: columns.remove(0),
            rids,
        };
        Ok(self.run_rows_cell.get_or_init(|| rows).clone())
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
            .zip(rids.iter().map(|&rid| super::base_row(rid)))
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
        if let Some(bound) = self.bound.get() {
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
            .bound
            .get_or_init(|| (bound_projection, bound_filter))
            .clone())
    }

    pub(crate) fn component(&self) -> &'static str {
        self.child.name
    }

    /// The order the view's reads follow, the child's: `(p, o, s, g)` for
    /// an `index:posg` run whose terms leave the object unbound, else base
    /// order (see [`served_order`](super::serve::served_order)). `None` for
    /// a child whose rows are not globally sorted, read in its own order.
    pub(crate) fn read_order(&self) -> Option<SortOrder> {
        let object_bound = self.constraints.iter().any(|&(column, _)| column == COL_O);
        self.child
            .sorted
            .then(|| super::serve::served_order(self.child.name, object_bound))
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
        let chunk = match self.point_run(file)? {
            Some(read) => read.await?,
            None => read_all_rows(self.scan()?).await?,
        };
        self.decode.chunk_code_columns(&chunk, deleted).map(Some)
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
