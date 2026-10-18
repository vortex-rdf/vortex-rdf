//! The file-backed read half of the scan tier: pushed-down filter
//! construction, row and component point reads through the file's cached
//! chunk probes, and [`RowSelection::restrict_scan`]. Pure functions of a
//! [`NativeStoreFile`] and a filter expression — no store state.

use std::future::Future;
use std::ops::Range;
use std::sync::Arc;

use futures::stream::BoxStream;
use futures::{StreamExt, future, stream};
use oxrdf::NamedOrBlankNode;
use vortex_array::expr::forms::conjuncts;
use vortex_array::expr::{Expression, and_collect, eq, get_item, lit, root};
use vortex_array::scalar::Scalar;
use vortex_array::{ArrayRef, MaskFuture};
use vortex_buffer::Buffer;
use vortex_error::VortexExpect as _;
use vortex_layout::scan::scan_builder::ScanBuilder;
use vortex_mask::{AllOr, Mask};
use vortex_scan::selection::Selection;
use vortex_scan::strict_sorted_buffer::StrictSortedBuffer;

use crate::error::{Result, VortexRdfError};
use crate::io::read::{
    available_parallelism, collect_scan, collect_scan_spawned, inline_splits, spawned_splits,
};
use crate::store::layouts::{Constraints, PatternCodes, QuadPattern, TermRef};
use crate::store::persist::native_file::NativeStoreFile;
use crate::store::scan::gather::primitive_from_u64_reads;
use crate::store::schema::{self, RowId, TermCode};
use crate::store::view::selection::RowSelection;

/// The bind-memo scope tag for expressions over the quad table's schema
/// (the transparent root the file scan reads).
pub(crate) const QUAD_SCOPE: &str = "quads";

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
    /// [`MAX_INLINE_SPLITS`], spawned above it — and inline whatever the
    /// count on a thread with no tokio runtime, which has nothing to spawn
    /// onto.
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
/// stream) — what a read by id ([`ExactRead`]) runs none of.
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

/// Run a row scan to completion and materialize every row it yields into one
/// in-memory array, in the scan's row order.
///
/// [`ScanDriver::for_row_scan`] picks the driver from the split count. Inline,
/// the split futures are polled on the calling task by [`collect_scan`];
/// spawned, they run on the runtime's workers through
/// [`collect_scan_spawned`], `scan.concurrency()` splits per core in flight.
/// Either driver returns the chunks in split order, whatever the scan's
/// `ordered` flag says.
///
/// The splits are planned here because their count picks the driver. Row
/// splits do filter and decode work, so a large scan needs the workers; an
/// index child's row-id scan goes through [`read_index_row_ids`] instead.
pub(crate) async fn read_all_rows(scan: ScanBuilder<ArrayRef>) -> Result<ArrayRef> {
    #[cfg(test)]
    scan_hooks::note();
    let dtype = scan.dtype()?;
    let window = scan.concurrency() * available_parallelism();
    let tasks = scan.build()?;
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
/// driver is [`ScanDriver::for_row_scan`]'s, as for [`read_all_rows`] —
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

/// Run an index child's row-id scan to completion and materialize its rows,
/// always inline, whatever its split count.
///
/// Zone maps prune almost every split of such a scan and a located run is
/// sliced out of the row-id column, so there is no work to spread; the caller
/// decodes the ids on its own thread. A scan is an index scan because its
/// caller says so here, not because of its `ordered` flag: any other scan goes
/// through [`read_all_rows`].
pub(crate) async fn read_index_row_ids(scan: ScanBuilder<ArrayRef>) -> Result<ArrayRef> {
    let dtype = scan.dtype()?;
    let tasks = scan.build()?;
    collect_scan(dtype, tasks).await
}

/// The rows of a point read when it answers, otherwise the rows of `scan` —
/// the fallback every point-read fast path keeps for a chunk its probes
/// decline.
pub(crate) async fn point_rows_or_scan(
    point: impl Future<Output = Result<Option<ArrayRef>>>,
    scan: ScanBuilder<ArrayRef>,
) -> Result<ArrayRef> {
    match point.await? {
        Some(rows) => Ok(rows),
        None => read_all_rows(scan).await,
    }
}

impl RowSelection {
    /// Apply this selection — and any tombstones — to a file scan.
    ///
    /// A range and an id list reach the scan through different knobs (a row
    /// range and a [`Selection`]), and the variants being exclusive is what
    /// keeps them from being set together — vortex's exact-range planning
    /// (`attempt_split_ranges`) bails out when a row range accompanies an
    /// `IncludeByIndex` selection. `All` leaves the scan's full row range in
    /// place (every file row is a quad row).
    ///
    /// Tombstoned rows are dropped inside the scan, so this composes with a
    /// pushed-down filter (whose output carries no row ids to re-align
    /// against). They ride the same `Selection`
    /// knob as an id list, so the one case where both would claim it — a sparse
    /// `Ids` selection with deletes — is resolved by subtracting the tombstones
    /// from the id list up front; `All`/`Range` leave that knob free for an
    /// `ExcludeByIndex` of the (sparse) deleted rows.
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

/// The set positions of a tombstone mask as an ascending id list — the sparse
/// form the scan wants for an exclusion.
fn deleted_ids(deleted: &Mask) -> StrictSortedBuffer<RowId> {
    let ids = match deleted.indices() {
        AllOr::All => Buffer::from_iter(0..deleted.len() as RowId),
        AllOr::None => Buffer::empty(),
        AllOr::Some(indices) => Buffer::from_iter(indices.iter().map(|&i| i as RowId)),
    };
    StrictSortedBuffer::try_new(ids).vortex_expect("mask indices are ascending and unique")
}

/// An ascending id list with the tombstoned rows removed — used when a sparse
/// id selection and the deletions would both want the scan's selection knob.
fn subtract_deleted(ids: &Buffer<RowId>, deleted: &Mask) -> StrictSortedBuffer<RowId> {
    let ids = Buffer::from_iter(
        ids.iter()
            .copied()
            .filter(|&id| !deleted.value(id as usize)),
    );
    StrictSortedBuffer::try_new(ids).vortex_expect("a RowSelection id list is ascending and unique")
}

/// A [`RowSelection::Ids`] list as the strictly-sorted buffer the scan wants —
/// ascending and unique is that variant's construction invariant (index
/// resolutions answer in ascending unique row ids).
pub(super) fn strict_ids(ids: &Buffer<RowId>) -> StrictSortedBuffer<RowId> {
    StrictSortedBuffer::try_new(ids.clone())
        .vortex_expect("a RowSelection id list is ascending and unique")
}

/// The positions — within the rows `selection` covers, in file order, as
/// `selection.apply` aligns them — whose code in `column` passes
/// `admit`. The column is streamed through the projected scan rather than
/// materialized: the scan reads several splits per worker ahead of this loop,
/// so that many chunks of codes are in flight at once, never the whole column.
/// Tombstones are not applied (the read paths apply them).
pub(crate) async fn column_positions(
    file: &NativeStoreFile,
    column: &'static str,
    selection: &RowSelection,
    admit: impl Fn(TermCode) -> bool,
) -> Result<Vec<usize>> {
    use vortex_array::VortexSessionExecute as _;
    use vortex_array::arrays::{PrimitiveArray, StructArray};
    use vortex_array::expr::{root, select};

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

/// The pushed-down filter for a pattern under `codes`' layout: `AND` of
/// `column == code` per compiled constraint, `lit(false)` for an unmatchable
/// pattern, `None` when nothing is bound.
pub(crate) fn build_file_filter(
    pattern: QuadPattern<'_>,
    codes: &mut PatternCodes<'_>,
) -> Result<Option<Expression>> {
    Ok(match codes.constraints(pattern)? {
        Constraints::AlwaysFalse => Some(lit(false)),
        Constraints::Eq(eqs) => eq_conjunction(eqs),
    })
}

/// The conjunction of `column == value` equalities over root fields — the
/// filter shape every pushed-down index probe and serve scan uses. `None`
/// for an empty constraint set.
pub(crate) fn eq_conjunction(
    constraints: impl IntoIterator<Item = (&'static str, Scalar)>,
) -> Option<Expression> {
    and_collect(
        constraints
            .into_iter()
            .map(|(column, value)| eq(get_item(column, root()), lit(value))),
    )
}

/// The exact row range of `subject`'s run in a sorted file, by binary search
/// over the subject column's encoded chunks. `None` declines — an unsorted
/// file, a subject without a native probe scalar (string-subject layouts, a
/// term the dictionary lacks), a column whose chunk shape has no probe — and
/// the caller keeps the scan path.
pub(crate) async fn locate_subject_run(
    file: &NativeStoreFile,
    codes: &mut PatternCodes<'_>,
    subject: &NamedOrBlankNode,
) -> Result<Option<Range<u64>>> {
    if !file.quads_sorted() {
        return Ok(None);
    }
    let Ok(Some(probe)) = codes.probe_scalar(TermRef::Subject(subject)) else {
        return Ok(None);
    };
    let Ok(needle) = u64::try_from(&probe) else {
        return Ok(None);
    };
    let Some(chunks) = file.column_chunks(schema::COL_S) else {
        return Ok(None);
    };
    chunks
        .bounds(needle, &file.segment_source(), file.session())
        .await
        .map_err(VortexRdfError::Vortex)
}

/// Narrow `run`, a bound subject's rows in a file sorted by `(s, p, o, g)`, by
/// the roles behind the subject in that order: each bound role whose term has
/// a code is binary-searched inside the run through its column's chunk probes
/// and cleared from `pattern` — the file mirror of the in-memory prefix probe.
/// The first role that is unbound, has no code or whose chunks decline ends
/// the prefix, as does an empty run. Answers how many roles it cleared.
pub(crate) async fn narrow_subject_run(
    file: &NativeStoreFile,
    codes: &mut PatternCodes<'_>,
    pattern: &mut QuadPattern<'_>,
    run: &mut Range<u64>,
) -> Result<usize> {
    let source = file.segment_source();
    let session = file.session();
    let roles = [
        (pattern.predicate.map(TermRef::Predicate), schema::COL_P),
        (pattern.object.map(TermRef::Object), schema::COL_O),
        (pattern.graph.map(TermRef::Graph), schema::COL_G),
    ];
    let mut answered = 0;
    for (term, column) in roles {
        if run.is_empty() {
            break;
        }
        let Some(term) = term else { break };
        let Some(chunks) = file.column_chunks(column) else {
            break;
        };
        let Some(needle) = codes
            .probe_scalar(term)?
            .and_then(|scalar| u64::try_from(&scalar).ok())
        else {
            break;
        };
        let Some(narrowed) = chunks
            .bounds_in(run.clone(), needle, &source, session)
            .await?
        else {
            break;
        };
        *run = narrowed;
        answered += 1;
    }
    if answered > 0 {
        pattern.predicate = None;
    }
    if answered > 1 {
        pattern.object = None;
    }
    if answered > 2 {
        pattern.graph = None;
    }
    Ok(answered)
}

/// The subject column's chunk-probe handle for a file sorted by subject.
/// `None` declines — an unsorted file, or a column whose chunk shape has no
/// probe.
fn sorted_subject_chunks(
    file: &NativeStoreFile,
) -> Option<Arc<vortex_rdf_encoded_search::ColumnChunks>> {
    if !file.quads_sorted() {
        return None;
    }
    file.column_chunks(schema::COL_S)
}

/// The exact row range of subject `code` in the sorted subject column — empty,
/// at the insertion point, for a code the column lacks. `None` declines: a
/// chunk the search needs has no probe.
async fn subject_code_bounds(
    chunks: &vortex_rdf_encoded_search::ColumnChunks,
    file: &NativeStoreFile,
    code: TermCode,
) -> Result<Option<Range<u64>>> {
    chunks
        .bounds(code, &file.segment_source(), file.session())
        .await
        .map_err(VortexRdfError::Vortex)
}

/// The exact row range of the rows whose subject code lies in `range`
/// (`lo <= s < hi`) in a sorted file, by binary search over the subject
/// column's encoded chunks. The file is sorted by `s`, so those rows are one
/// run: it starts where the first code `>= lo` does and ends where the first
/// code `>= hi` does, and each bound is exact for a code the column lacks.
/// `None` declines — an unsorted file, a column whose chunk shape has no probe
/// — and the caller keeps the stream.
pub(crate) async fn locate_subject_code_range(
    file: &NativeStoreFile,
    range: Range<TermCode>,
) -> Result<Option<Range<u64>>> {
    let Some(chunks) = sorted_subject_chunks(file) else {
        return Ok(None);
    };
    let Some(from) = subject_code_bounds(&chunks, file, range.start).await? else {
        return Ok(None);
    };
    let Some(to) = subject_code_bounds(&chunks, file, range.end).await? else {
        return Ok(None);
    };
    Ok(Some(from.start..to.start.max(from.start)))
}

/// The rows holding each subject code of `codes` — ascending and unique, as
/// [`Keep::set`](crate::store::Keep::set) builds them — in a sorted file, as
/// ascending disjoint runs: a code without rows adds none, and codes whose
/// rows abut share one run. One binary search per code, over chunks fetched
/// once for the call. `None` declines as [`locate_subject_code_range`] does.
pub(crate) async fn locate_subject_code_runs(
    file: &NativeStoreFile,
    codes: &[TermCode],
) -> Result<Option<Vec<Range<u64>>>> {
    debug_assert!(
        codes.windows(2).all(|pair| pair[0] < pair[1]),
        "a set's codes are ascending and unique"
    );
    let Some(chunks) = sorted_subject_chunks(file) else {
        return Ok(None);
    };
    let mut runs: Vec<Range<u64>> = Vec::new();
    for &code in codes {
        let Some(run) = subject_code_bounds(&chunks, file, code).await? else {
            return Ok(None);
        };
        if run.is_empty() {
            continue;
        }
        match runs.last_mut() {
            Some(last) if last.end == run.start => last.end = run.end,
            _ => runs.push(run),
        }
    }
    Ok(Some(runs))
}

/// The rows of `columns` at `rows`, the live rows [`ExactRead`] chose to read
/// point by point, read through the file's cached wire-encoded chunk probes
/// instead of a scan — the file analogue of the in-memory
/// [`gather_by_point_reads`](super::gather::gather_by_point_reads). A pushed filter of
/// equality conjuncts is applied per row during the read (an eq column's
/// surviving value is its constant, so only residual columns read twice).
/// `None` declines — a filter that is not an eq conjunction, a column without
/// a probeable chunk handle — and the caller keeps its scan path. Chunk
/// fetches are cached on `file`, so warm repeats touch no segments.
pub(crate) async fn file_point_rows(
    file: &NativeStoreFile,
    columns: &[&str],
    filter: Option<&Expression>,
    rows: Vec<u64>,
) -> Result<Option<ArrayRef>> {
    let eqs = match filter {
        None => Vec::new(),
        Some(f) => match eq_code_pairs(f) {
            Some(pairs) => pairs,
            None => return Ok(None),
        },
    };
    let Some(handles) = columns
        .iter()
        .map(|c| file.column_chunks(c))
        .collect::<Option<Vec<_>>>()
    else {
        return Ok(None);
    };

    // Filter first: a row survives when every eq column reads its code.
    let Some(checks) = eqs
        .iter()
        .map(|(col, code)| {
            let idx = columns.iter().position(|c| c == col)?;
            Some((Arc::clone(&handles[idx]), *code))
        })
        .collect::<Option<Vec<_>>>()
    else {
        return Ok(None);
    };
    let Some(live) = rows_reading(file, &checks, rows).await? else {
        return Ok(None);
    };

    point_read_struct(file, &handles, columns, &live, |col| {
        eqs.iter().find(|(c, _)| c == col).map(|(_, v)| *v)
    })
    .await
}

/// A column's chunk-probe handle beside the code its rows must read.
type CodeCheck = (Arc<vortex_rdf_encoded_search::ColumnChunks>, u64);

/// The rows of `rows` at which every check's column reads its code, read
/// point by point through the chunk probes. `None` when a chunk the reads
/// reach has no probe.
async fn rows_reading(
    file: &NativeStoreFile,
    checks: &[CodeCheck],
    rows: Vec<u64>,
) -> Result<Option<Vec<u64>>> {
    let source = file.segment_source();
    let session = file.session();
    let mut live = rows;
    for (handle, code) in checks {
        if live.is_empty() {
            break;
        }
        let mut kept = Vec::with_capacity(live.len());
        for &row in &live {
            match handle.value_at(row, &source, session).await? {
                Some(v) if v == *code => kept.push(row),
                Some(_) => {}
                None => return Ok(None),
            }
        }
        live = kept;
    }
    Ok(Some(live))
}

/// Test hook: how many reads this thread gathered by taking exact ids
/// ([`take_file_rows`]) — through the chunk probes' leaves, or split by split.
#[cfg(test)]
pub(crate) mod take_hooks {
    use std::cell::Cell;

    thread_local! {
        static CHUNKS: Cell<usize> = const { Cell::new(0) };
        static SPLITS: Cell<usize> = const { Cell::new(0) };
    }

    /// Record one gather by take, through the chunk probes or split by split.
    pub(super) fn note(through_chunks: bool) {
        let cell = if through_chunks { &CHUNKS } else { &SPLITS };
        cell.with(|c| c.set(c.get() + 1));
    }

    /// The gathers by take on this thread so far: `(through the chunk
    /// probes, split by split)`.
    pub(crate) fn takes() -> (usize, usize) {
        (CHUNKS.with(Cell::get), SPLITS.with(Cell::get))
    }
}

/// One point read costs about as much as a filtered scan spends on this many
/// rows: the scan's filter evaluation decodes every row of each split it
/// touches, a point read one value per column.
const POINT_READ_ROWS: u64 = 256;

/// The ids of `ids` whose rows satisfy `filter`, a conjunction of term
/// equalities, read point by point through the file's chunk probes — what a
/// filtered scan of the splits holding them answers, without decoding those
/// splits. `None` declines — a filter of another shape, a column without
/// chunk probes, ids more than one per [`POINT_READ_ROWS`] rows of their
/// splits, where the scan costs less — and the caller keeps the filter.
pub(crate) async fn point_filter_ids(
    file: &NativeStoreFile,
    filter: &Expression,
    ids: &Buffer<RowId>,
) -> Result<Option<Buffer<RowId>>> {
    let Some(eqs) = eq_code_pairs(filter) else {
        return Ok(None);
    };
    let splits = file.splits()?;
    if !Footprint::of(ids, &splits).sparse(POINT_READ_ROWS) {
        return Ok(None);
    }
    let Some(checks) = eqs
        .iter()
        .map(|(col, code)| Some((file.column_chunks(col)?, *code)))
        .collect::<Option<Vec<_>>>()
    else {
        return Ok(None);
    };
    Ok(rows_reading(file, &checks, ids.to_vec())
        .await?
        .map(Buffer::from))
}

/// A take costs about as much per id as the scan's row mask spends on this
/// many rows of the splits holding the ids: the mask filters a run-end column
/// run by run over every split it touches, a take binary-searches the runs at
/// each id.
const TAKE_ROWS: u64 = 16;

/// The rows of `columns` at `ids`, the live ids [`ExactRead`] chose to take:
/// taken at the ids so only what the take kernels touch is decoded, as in the
/// in-memory gather, where the scan would apply a row mask to every split —
/// which filters a run-end column run by run. Each column takes from its
/// chunk probes' leaves when every column has them ([`chunk_take`]), which the
/// probes resolved once over the mapped segments; otherwise each split's rows
/// from its first to its last id are projected unmasked and taken. `None`
/// for no id on the split path, and the caller scans.
pub(crate) async fn take_file_rows(
    file: &NativeStoreFile,
    columns: &[&'static str],
    ids: &[RowId],
) -> Result<Option<ArrayRef>> {
    use vortex_array::IntoArray;
    use vortex_array::arrays::PrimitiveArray;
    use vortex_array::expr::{root, select};
    use vortex_array::validity::Validity;

    if let Some(rows) = chunk_take(file, columns, ids).await? {
        #[cfg(test)]
        take_hooks::note(true);
        return Ok(Some(rows));
    }
    if ids.is_empty() {
        return Ok(None);
    }
    let splits = file.splits()?;
    // The ids grouped by split: the rows from the group's first to past its
    // last id, and each id's offset in them.
    let mut groups: Vec<(Range<u64>, Vec<u64>)> = Vec::new();
    let mut rest = ids;
    let mut idx = 0;
    while let Some(&first) = rest.first() {
        idx += splits[idx..].partition_point(|s| s.end <= first);
        let end = splits.get(idx).map_or(u64::MAX, |s| s.end);
        let inside = rest.partition_point(|&id| id < end);
        let last = rest[inside - 1];
        groups.push((
            first..last + 1,
            rest[..inside].iter().map(|&id| id - first).collect(),
        ));
        rest = &rest[inside..];
        idx += 1;
    }
    let reader = file.layout_reader()?;
    let projection =
        file.bound_exprs()
            .bind(QUAD_SCOPE, &select(columns, root()), reader.dtype())?;
    let mut tasks = Vec::with_capacity(groups.len());
    for (rows, offsets) in groups {
        let len = (rows.end - rows.start) as usize;
        let projected =
            reader.projection_evaluation(&rows, &projection, MaskFuture::new_true(len))?;
        tasks.push(async move {
            let indices =
                PrimitiveArray::new(Buffer::from(offsets), Validity::NonNullable).into_array();
            projected
                .await
                .and_then(|rows| rows.take(indices))
                .map_err(VortexRdfError::Vortex)
        });
    }
    let chunks = stream::iter(tasks)
        .buffered(available_parallelism())
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<ArrayRef>>>()?;
    #[cfg(test)]
    take_hooks::note(false);
    let dtype = chunks[0].dtype().clone();
    crate::store::array::chunked_or_single(chunks, dtype).map(Some)
}

/// `columns` at `ids` (ascending), each taken through its chunk probes
/// ([`ColumnChunks::take`](vortex_rdf_encoded_search::ColumnChunks::take)),
/// as one struct. `None` when a column has no chunk probes or a leaf the ids
/// reach declines its probe.
async fn chunk_take(
    file: &NativeStoreFile,
    columns: &[&'static str],
    ids: &[RowId],
) -> Result<Option<ArrayRef>> {
    use vortex_array::IntoArray;
    use vortex_array::arrays::StructArray;
    use vortex_array::dtype::FieldName;
    use vortex_array::validity::Validity;

    let Some(handles) = columns
        .iter()
        .map(|c| file.column_chunks(c))
        .collect::<Option<Vec<_>>>()
    else {
        return Ok(None);
    };
    let source = file.segment_source();
    let session = file.session();
    let mut children = Vec::with_capacity(handles.len());
    for handle in &handles {
        match handle.take(ids, &source, session).await? {
            Some(child) => children.push(child),
            None => return Ok(None),
        }
    }
    let names: Vec<FieldName> = columns.iter().map(|c| FieldName::from(*c)).collect();
    Ok(Some(
        StructArray::try_new(names.into(), children, ids.len(), Validity::NonNullable)?
            .into_array(),
    ))
}

/// Exact ids at most this many per split they touch are read point by point:
/// a take through the chunk probes pays a fixed cost for every leaf it takes
/// from, which reading this few ids one by one undercuts.
const POINT_IDS_PER_SPLIT: u64 = 16;

/// How the rows of an exact file selection are read, cheapest first.
pub(crate) enum ExactRead {
    /// Point by point through the chunk probes, at these live rows.
    Points(Vec<u64>),
    /// Taken at these live ids.
    Take(Vec<RowId>),
    /// By the restricted scan.
    Scan,
}

impl ExactRead {
    /// The read of `columns` at `selection`, exact, with `filter` pending and
    /// `deleted` tombstoned. Point reads serve a point-sized selection, or ids
    /// at most [`POINT_IDS_PER_SPLIT`] per split they touch, when every column
    /// has chunk probes and the filter is none or code equalities (checked per
    /// row); a take serves ids at most one per [`TAKE_ROWS`] rows of their
    /// splits under no filter; the scan serves the rest.
    pub(crate) fn of(
        file: &NativeStoreFile,
        columns: &[&str],
        filter: Option<&Expression>,
        selection: &RowSelection,
        deleted: Option<&Mask>,
    ) -> Result<Self> {
        let row_count = file.row_count() as usize;
        let pointable = filter.is_none_or(|f| eq_code_pairs(f).is_some())
            && columns.iter().all(|c| file.column_chunks(c).is_some());
        if pointable && let Some(rows) = selection.point_sized_live_rows(deleted, row_count) {
            return Ok(ExactRead::Points(rows));
        }
        let RowSelection::Ids(ids) = selection else {
            return Ok(ExactRead::Scan);
        };
        let splits = file.splits()?;
        let footprint = Footprint::of(ids, &splits);
        let live =
            || selection.filter_rows(row_count, move |id| deleted.is_none_or(|d| !d.value(id)));
        if pointable && footprint.few_per_split(POINT_IDS_PER_SPLIT) {
            return Ok(ExactRead::Points(live()));
        }
        if filter.is_none() && footprint.sparse(TAKE_ROWS) {
            return Ok(ExactRead::Take(live()));
        }
        Ok(ExactRead::Scan)
    }
}

/// Where exact ids (ascending) sit among a file's splits: how many ids, how
/// many splits hold them, and how many rows those splits have.
struct Footprint {
    ids: u64,
    splits: u64,
    rows: u64,
}

impl Footprint {
    fn of(ids: &[RowId], splits: &[Range<u64>]) -> Self {
        let (mut touched, mut rows) = (0, 0);
        let mut rest = ids;
        let mut idx = 0;
        while let Some(&first) = rest.first() {
            idx += splits[idx..].partition_point(|s| s.end <= first);
            let Some(split) = splits.get(idx) else {
                break;
            };
            touched += 1;
            rows += split.end - split.start;
            rest = &rest[rest.partition_point(|&id| id < split.end)..];
            idx += 1;
        }
        Footprint {
            ids: ids.len() as u64,
            splits: touched,
            rows,
        }
    }

    /// Whether the ids number at most one per `rows_per_id` rows of the splits
    /// holding them — the density below which reading them by id costs less
    /// than scanning those splits ([`POINT_READ_ROWS`], [`TAKE_ROWS`]).
    fn sparse(&self, rows_per_id: u64) -> bool {
        self.ids.saturating_mul(rows_per_id) <= self.rows
    }

    /// Whether the ids number at most `per_split` per split holding them.
    fn few_per_split(&self, per_split: u64) -> bool {
        self.ids <= per_split.saturating_mul(self.splits)
    }
}

/// Read `columns` at `rows` point-by-point through their chunk-probe
/// `handles` into one canonical struct chunk. A column `constant` answers is
/// filled with that value instead of being read. `Ok(None)` declines — an
/// unprobeable chunk or a column type the canonical child cannot hold.
async fn point_read_struct(
    file: &NativeStoreFile,
    handles: &[Arc<vortex_rdf_encoded_search::ColumnChunks>],
    columns: &[&str],
    rows: &[u64],
    constant: impl Fn(&str) -> Option<u64>,
) -> Result<Option<ArrayRef>> {
    use vortex_array::IntoArray;
    use vortex_array::arrays::StructArray;
    use vortex_array::dtype::FieldName;
    use vortex_array::validity::Validity;

    let source = file.segment_source();
    let session = file.session();
    let mut children = Vec::with_capacity(columns.len());
    for (handle, col) in handles.iter().zip(columns) {
        let mut values = Vec::with_capacity(rows.len());
        match constant(col) {
            Some(v) => values.extend(std::iter::repeat_n(v, rows.len())),
            None => {
                for &row in rows {
                    match handle.value_at(row, &source, session).await? {
                        Some(v) => values.push(v),
                        None => return Ok(None),
                    }
                }
            }
        }
        let Some(child) = primitive_from_u64_reads(handle.dtype().as_ptype(), values.into_iter())
        else {
            return Ok(None);
        };
        children.push(child);
    }
    let names: Vec<FieldName> = columns.iter().map(|c| FieldName::from(*c)).collect();
    Ok(Some(
        StructArray::try_new(names.into(), children, rows.len(), Validity::NonNullable)?
            .into_array(),
    ))
}

/// A located serve run's projected component columns, read point-by-point
/// through the component's cached chunk probes into one canonical
/// child-named chunk — the serve path's counterpart of [`file_point_rows`].
/// `Ok(None)` declines (an unprobeable column or chunk); the caller keeps
/// its filter scan.
pub(crate) async fn component_point_chunk(
    file: &NativeStoreFile,
    component: &str,
    columns: &[&'static str],
    range: Range<u64>,
) -> Result<Option<ArrayRef>> {
    let Some(handles) = columns
        .iter()
        .map(|c| file.component_column_chunks(component, c))
        .collect::<Option<Vec<_>>>()
    else {
        return Ok(None);
    };
    let rows: Vec<u64> = range.collect();
    point_read_struct(file, &handles, columns, &rows, |_| None).await
}

/// The `(column, code)` pairs of a filter built by [`build_file_filter`] —
/// a conjunction of `column == literal` over root fields. `None` for any
/// other shape (e.g. the `AlwaysFalse` literal), telling the caller the
/// filter cannot be applied as per-row equality tests.
pub(crate) fn eq_code_pairs(filter: &Expression) -> Option<Vec<(String, u64)>> {
    use vortex_array::scalar_fn::fns::binary::Binary;
    use vortex_array::scalar_fn::fns::get_item::GetItem;
    use vortex_array::scalar_fn::fns::literal::Literal;
    use vortex_array::scalar_fn::fns::operators::Operator;

    conjuncts(filter)
        .iter()
        .map(|c| {
            let op = c.as_opt::<Binary>()?;
            if *op != Operator::Eq {
                return None;
            }
            let field = c.child(0).as_opt::<GetItem>()?;
            let scalar = c.child(1).as_opt::<Literal>()?;
            let code = u64::try_from(scalar).ok()?;
            Some((field.to_string(), code))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::Range;
    use vortex_array::VortexSessionExecute as _;
    use vortex_array::arrays::{PrimitiveArray, StructArray};
    use vortex_array::expr::{and, eq, get_item, root};
    use vortex_layout::scan::split_by::SplitBy;

    /// Only a conjunction of `field == literal` over root fields decodes to
    /// `(column, code)` pairs; any other shape declines.
    #[test]
    fn eq_code_pairs_accepts_only_eq_conjunctions() {
        let filter = and(
            eq(get_item("p", root()), lit(3u32)),
            eq(get_item("o", root()), lit(7u32)),
        );
        assert_eq!(
            eq_code_pairs(&filter),
            Some(vec![("p".to_string(), 3), ("o".to_string(), 7)])
        );
        assert!(eq_code_pairs(&lit(false)).is_none());
        assert!(eq_code_pairs(&eq(get_item("p", root()), lit("x"))).is_none());
    }

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
