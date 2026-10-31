//! Reads by id through a [`NativeStoreFile`]'s cached chunk probes: rows and
//! codes point by point, takes at exact ids, a located serve run's component
//! columns, a filter checked at sparse ids, and the reads of the rows a
//! filter's split masks keep, as [`read_plan`](super::read_plan) chooses
//! them. Functions of the file, with no store state.

use std::borrow::Cow;
use std::ops::Range;
use std::sync::Arc;

use futures::future::BoxFuture;
use futures::{FutureExt as _, StreamExt, stream};
use vortex_array::dtype::DType;
use vortex_array::expr::{root, select};
use vortex_array::{ArrayRef, MaskFuture};
use vortex_buffer::Buffer;
use vortex_error::{VortexError, VortexResult};
use vortex_mask::{AllOr, Mask};

use crate::error::{Result, VortexRdfError};
use crate::io::read::available_parallelism;
use crate::store::array::named_struct;
use crate::store::persist::native_file::NativeStoreFile;
use crate::store::scan::file_exprs::{CodeEqs, PushedFilter, QUAD_SCOPE};
use crate::store::scan::file_filter::{self, SplitMasks};
use crate::store::scan::file_scans::{SPLITS_IN_FLIGHT_PER_CORE, collect_row_splits};
use crate::store::scan::gather::primitive_from_u64_reads;
use crate::store::scan::read_plan::{Footprint, MaskedRead, POINT_READ_ROWS, ids_by_split};
use crate::store::schema::{RowId, TermCode};
use crate::store::view::selection::RowSelection;

/// The rows of `columns` at `rows`, the live rows
/// [`ExactRead`](super::read_plan::ExactRead) chose to read point by point,
/// read through the file's cached wire-encoded chunk probes
/// instead of a scan: the file analogue of the in-memory
/// [`gather_by_point_reads`](super::gather::gather_by_point_reads). A pushed
/// filter of equality conjuncts is checked per row first, and each of its
/// columns, which must be among `columns`, is then filled with its constant,
/// so no column is read twice. `None` declines (a filter that is not an eq
/// conjunction, a column without a probeable chunk handle), and the caller
/// keeps its scan path. Chunk fetches are cached on `file`, so warm repeats
/// touch no segments.
pub(crate) async fn file_point_rows(
    file: &NativeStoreFile,
    columns: &[&str],
    filter: Option<&PushedFilter>,
    rows: Vec<u64>,
) -> Result<Option<ArrayRef>> {
    #[cfg(test)]
    masked_column_hooks::note(columns);
    let Some(points) = PointRows::read(file, columns, filter, rows).await? else {
        return Ok(None);
    };
    let Some(values) = points.columns(file, columns).await? else {
        return Ok(None);
    };
    u64_struct(&points.handles, columns, values, points.live.len())
}

/// [`file_point_rows`] as one code buffer per column of `columns`, in that
/// order, for a Dictionary file: the same rows, read point by point through
/// the chunk probes, without building the struct a row read returns.
/// `Ok(None)` declines as [`file_point_rows`] does.
pub(crate) async fn file_point_codes(
    file: &NativeStoreFile,
    columns: &[&str],
    filter: Option<&PushedFilter>,
    rows: Vec<u64>,
) -> Result<Option<Vec<Buffer<TermCode>>>> {
    #[cfg(test)]
    masked_column_hooks::note(columns);
    let Some(points) = PointRows::read(file, columns, filter, rows).await? else {
        return Ok(None);
    };
    let values = points.columns(file, columns).await?;
    Ok(values.map(|values| values.into_iter().map(Buffer::from).collect()))
}

/// What a point read by id reads: the chunk-probe handle of each read
/// column, the code equalities of the pending filter, and the rows that pass
/// them.
struct PointRows {
    handles: Vec<Arc<vortex_rdf_encoded_search::ColumnChunks>>,
    eqs: CodeEqs,
    live: Vec<u64>,
}

impl PointRows {
    /// The point read of `columns` at `rows` with `filter` pending: `None`
    /// when the filter is not code equalities, a column has no chunk probes,
    /// or a chunk the filter's reads reach has no probe.
    async fn read(
        file: &NativeStoreFile,
        columns: &[&str],
        filter: Option<&PushedFilter>,
        rows: Vec<u64>,
    ) -> Result<Option<Self>> {
        let eqs = match filter {
            None => CodeEqs::default(),
            Some(f) => match f.eqs() {
                Some(eqs) => eqs.clone(),
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
                let idx = columns.iter().position(|&c| c == col.as_ref())?;
                Some((Arc::clone(&handles[idx]), code))
            })
            .collect::<Option<Vec<_>>>()
        else {
            return Ok(None);
        };
        let Some(live) = rows_reading(file, &checks, rows).await? else {
            return Ok(None);
        };
        Ok(Some(Self { handles, eqs, live }))
    }

    /// The live rows' values of `columns`, the columns `handles` read, in
    /// their order, one value column each: a column the filter pins filled
    /// with its code, every other read point by point ([`values_at`]), so no
    /// column is read twice. `None` when a chunk the reads reach has no probe.
    async fn columns(
        &self,
        file: &NativeStoreFile,
        columns: &[&str],
    ) -> Result<Option<Vec<Vec<u64>>>> {
        let mut out = Vec::with_capacity(columns.len());
        for (handle, column) in self.handles.iter().zip(columns) {
            let values = match self.eqs.code(column) {
                Some(code) => vec![code; self.live.len()],
                None => match values_at(file, handle, &self.live).await? {
                    Some(values) => values,
                    None => return Ok(None),
                },
            };
            out.push(values);
        }
        Ok(Some(out))
    }
}

/// The values of `handle`'s column at `rows`, in order, read through its
/// chunk probes (chunk fetches cached on `file`), each leaf found once for
/// the rows that follow one another in it and each run of consecutive rows
/// read in one pass: the one read every point read by row makes. `None` when
/// a chunk the reads reach has no probe.
pub(crate) async fn values_at(
    file: &NativeStoreFile,
    handle: &vortex_rdf_encoded_search::ColumnChunks,
    rows: &[u64],
) -> Result<Option<Vec<u64>>> {
    Ok(handle
        .values_at(rows, &file.segment_source(), file.session())
        .await?)
}

/// A column's chunk-probe handle beside the code its rows must read.
type CodeCheck = (Arc<vortex_rdf_encoded_search::ColumnChunks>, u64);

/// The rows of `rows` at which every check's column reads its code, read
/// point by point through the chunk probes; `rows` itself when there is no
/// check. `None` when a chunk the reads reach has no probe.
async fn rows_reading(
    file: &NativeStoreFile,
    checks: &[CodeCheck],
    rows: Vec<u64>,
) -> Result<Option<Vec<u64>>> {
    let mut live = rows;
    for (handle, code) in checks {
        if live.is_empty() {
            break;
        }
        let Some(values) = values_at(file, handle, &live).await? else {
            return Ok(None);
        };
        live = (live.into_iter().zip(values))
            .filter_map(|(row, value)| (value == *code).then_some(row))
            .collect();
    }
    Ok(Some(live))
}

/// The ids of `ids` whose rows satisfy `filter`, a conjunction of term
/// equalities, read point by point through the file's chunk probes: what a
/// filtered scan of the splits holding them answers, without decoding those
/// splits. `None` declines (a filter of another shape, a column without
/// chunk probes, or ids denser than one per [`POINT_READ_ROWS`] rows of
/// their splits, where the scan costs less), and the caller keeps the filter.
pub(crate) async fn point_filter_ids(
    file: &NativeStoreFile,
    filter: &PushedFilter,
    ids: &Buffer<RowId>,
) -> Result<Option<Buffer<RowId>>> {
    let Some(eqs) = filter.eqs() else {
        return Ok(None);
    };
    let splits = file.splits()?;
    if !Footprint::of(ids, &splits).sparse(POINT_READ_ROWS) {
        return Ok(None);
    }
    let Some(checks) = eqs
        .iter()
        .map(|(col, code)| Some((file.column_chunks(col.as_ref())?, code)))
        .collect::<Option<Vec<_>>>()
    else {
        return Ok(None);
    };
    Ok(rows_reading(file, &checks, ids.to_vec())
        .await?
        .map(Buffer::from))
}

/// The rows of `columns` at `ids`, the live ids
/// [`ExactRead`](super::read_plan::ExactRead) chose to take: taken at the
/// ids, so only what the take kernels touch is decoded, as in
/// the in-memory gather, where the scan would run a row mask over every split
/// (which filters a run-end column run by run). Each column takes from its
/// chunk probes' leaves when every column has them ([`chunk_take`]);
/// otherwise each split's rows from its first to its last id are projected
/// unmasked and taken. `None` for no id on the split path, and the caller
/// scans.
pub(crate) async fn take_file_rows(
    file: &NativeStoreFile,
    columns: &[&'static str],
    ids: &[RowId],
) -> Result<Option<ArrayRef>> {
    use vortex_array::IntoArray;
    use vortex_array::arrays::PrimitiveArray;
    use vortex_array::validity::Validity;

    #[cfg(test)]
    masked_column_hooks::note(columns);
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
    let groups = ids_by_split(ids, &splits).map(|(_, group)| {
        let (first, last) = (group[0], group[group.len() - 1]);
        let offsets: Vec<u64> = group.iter().map(|&id| id - first).collect();
        (first..last + 1, offsets)
    });
    let reader = file.layout_reader()?;
    let projection =
        file.bound_exprs()
            .bind(QUAD_SCOPE, &select(columns, root()), reader.dtype())?;
    let mut tasks = Vec::new();
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

/// `columns` at `ids` (ascending), each taken from its chunk leaves
/// ([`ColumnChunks::take`](vortex_rdf_encoded_search::ColumnChunks::take)),
/// as one struct. `None` when a column has no chunk probes.
async fn chunk_take(
    file: &NativeStoreFile,
    columns: &[&'static str],
    ids: &[RowId],
) -> Result<Option<ArrayRef>> {
    use vortex_array::IntoArray;

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
        children.push(handle.take(ids, &source, session).await?);
    }
    Ok(Some(
        named_struct(columns, children, ids.len())?.into_array(),
    ))
}

/// One split's read of the rows its mask keeps.
pub(crate) type SplitRead = BoxFuture<'static, VortexResult<Option<ArrayRef>>>;

/// The reads of the rows `masks` keep, one per split with a match in split
/// order, beside their struct dtype: `columns` taken at the split's kept ids
/// (`take`), or projected at its mask. A projection is handed a mask of its
/// own over the kept mask's bits, so nothing the projection caches on its
/// mask stays on `masks`.
pub(crate) fn masked_split_tasks(
    file: &Arc<NativeStoreFile>,
    columns: &[&'static str],
    masks: &SplitMasks,
    take: bool,
) -> Result<(DType, Vec<SplitRead>)> {
    #[cfg(test)]
    masked_column_hooks::note(columns);
    let reader = file.layout_reader()?;
    let projection =
        file.bound_exprs()
            .bind(QUAD_SCOPE, &select(columns, root()), reader.dtype())?;
    let dtype = projection.dtype().clone();
    let columns: Arc<[&'static str]> = Arc::from(columns);
    let mut tasks: Vec<SplitRead> = Vec::new();
    for (mask, range) in masks.iter() {
        if mask.all_false() {
            continue;
        }
        if take {
            let mut ids = Vec::new();
            file_filter::push_kept_ids(&mut ids, mask, range);
            let (file, columns) = (Arc::clone(file), Arc::clone(&columns));
            tasks.push(
                async move {
                    Ok::<_, vortex_error::VortexError>(take_file_rows(&file, &columns, &ids).await?)
                }
                .boxed(),
            );
            continue;
        }
        let split_mask = match mask.bit_buffer() {
            AllOr::Some(bits) => Mask::from_buffer(bits.clone()),
            _ => Mask::new_true(mask.len()),
        };
        let projected =
            reader.projection_evaluation(range, &projection, MaskFuture::ready(split_mask))?;
        tasks.push(async move { projected.await.map(Some) }.boxed());
    }
    Ok((dtype, tasks))
}

/// The reads of the rows `filter` keeps in `selection`, the filter's masks
/// first, one future per split the selection touches, in file order: each
/// evaluates the filter over its split
/// ([`split_filter_tasks`](file_filter::split_filter_tasks), tombstoned rows
/// cleared), then reads `columns` at the rows its mask keeps, as
/// [`MaskedRead`] chooses for that split alone, filling the columns the
/// filter's equalities pin rather than reading them ([`kept_split_rows`]); a
/// split the filter keeps nothing of reads nothing.
/// Nothing is evaluated until a future is polled, so a stream of them
/// evaluates only the splits it reaches.
pub(crate) fn masks_first_split_reads(
    file: &Arc<NativeStoreFile>,
    filter: &PushedFilter,
    selection: &RowSelection,
    deleted: Option<&Mask>,
    columns: &[&'static str],
) -> Result<Vec<SplitRead>> {
    let columns: Arc<[&'static str]> = Arc::from(columns);
    let pinned = filter.eqs().cloned().unwrap_or_default();
    Ok(
        file_filter::split_filter_tasks(file, filter, selection, deleted)?
            .into_iter()
            .map(|evaluate| {
                let (file, columns) = (Arc::clone(file), Arc::clone(&columns));
                let pinned = pinned.clone();
                async move {
                    let (mask, range) = evaluate.await?;
                    if mask.all_false() {
                        return Ok(None);
                    }
                    let masks = SplitMasks::of_split(mask, range);
                    kept_split_rows(&file, &columns, &pinned, masks).await
                }
                .map(|rows| rows.map_err(VortexError::from))
                .boxed()
            })
            .collect(),
    )
}

/// `columns` at the rows one split's `masks` keep, read as [`MaskedRows`]
/// reads them, every row passing the equalities `pinned`.
async fn kept_split_rows(
    file: &Arc<NativeStoreFile>,
    columns: &[&'static str],
    pinned: &CodeEqs,
    masks: SplitMasks,
) -> Result<Option<ArrayRef>> {
    let masked = MaskedRows::of(file, columns, Arc::new(masks), pinned)?;
    if let Some(rows) = masked.points().await? {
        return Ok(Some(rows));
    }
    match masked.split_reads()?.into_iter().next() {
        Some(read) => Ok(read.await?),
        None => Ok(None),
    }
}

/// A read of `columns` at the rows a filter's split masks keep, as
/// [`MaskedRead`] chooses it for their density over the splits holding them:
/// point by point through the chunk probes, taken at the kept ids, or each
/// split projected at its own mask. Every row the masks keep passed the
/// filter, so the read reads only the columns its equalities leave free and
/// fills the others with their codes where it fills them
/// ([`CodeEqs::for_points`]). A read by id that a chunk declines falls to the
/// next: points to a take, a take to the projection. The one read of a
/// filter's kept rows: a window's kept masks', a one-shot read's evaluated
/// first, and a stream's split by split ([`masks_first_split_reads`]).
pub(crate) struct MaskedRows<'a> {
    file: &'a Arc<NativeStoreFile>,
    columns: &'a [&'static str],
    masks: Arc<SplitMasks>,
    read: MaskedRead,
    pins: CodeEqs,
}

impl<'a> MaskedRows<'a> {
    /// The read of `columns` (primary column names) at the rows `masks`
    /// keep, every one of which holds the codes of the equalities `pins`.
    pub(crate) fn of(
        file: &'a Arc<NativeStoreFile>,
        columns: &'a [&'static str],
        masks: Arc<SplitMasks>,
        pins: &CodeEqs,
    ) -> Result<Self> {
        let read = MaskedRead::of(file, &masks)?;
        let pins = match read {
            MaskedRead::Points => pins.for_points(masks.kept()),
            MaskedRead::Take | MaskedRead::Project => pins,
        }
        .clone();
        Ok(Self {
            file,
            columns,
            masks,
            read,
            pins,
        })
    }

    /// How many rows the masks keep.
    pub(crate) fn kept(&self) -> usize {
        self.masks.kept()
    }

    /// The equalities the read fills rather than reads.
    pub(crate) fn pins(&self) -> &CodeEqs {
        &self.pins
    }

    /// The columns the read reads, in order: those the equalities leave free
    /// ([`CodeEqs::columns_to_read`]).
    pub(crate) fn read_columns(&self) -> Cow<'a, [&'static str]> {
        self.pins.columns_to_read(self.columns)
    }

    /// The kept rows read point by point, every column of `columns` filled
    /// in, when the read is point by point; `None` for a denser read, or
    /// where a chunk declines.
    pub(crate) async fn points(&self) -> Result<Option<ArrayRef>> {
        if self.read != MaskedRead::Points {
            return Ok(None);
        }
        let ids = self.masks.first_ids(usize::MAX);
        file_point_rows(self.file, &self.read_columns(), None, ids)
            .await?
            .map(|rows| self.pins.fill_rows(rows, self.columns))
            .transpose()
    }

    /// [`points`](Self::points) as the `(s, p, o, g)` code columns, read
    /// point by point straight into the code buffers, `columns` the primary
    /// columns.
    pub(crate) async fn point_codes(&self) -> Result<Option<[Buffer<TermCode>; 4]>> {
        if self.read != MaskedRead::Points {
            return Ok(None);
        }
        let read_columns = self.read_columns();
        let ids = self.masks.first_ids(usize::MAX);
        file_point_codes(self.file, &read_columns, None, ids)
            .await?
            .map(|codes| self.pins.fill_codes(&read_columns, codes, self.kept()))
            .transpose()
    }

    /// One read per split with a match, in split order, of the rows its mask
    /// keeps, every column of `columns` filled in: taken at the kept ids (a
    /// point read is taken where a chunk declines it), or projected at the
    /// split's mask.
    pub(crate) fn split_reads(&self) -> Result<Vec<SplitRead>> {
        let take = self.read != MaskedRead::Project;
        let (_, tasks) = masked_split_tasks(self.file, &self.read_columns(), &self.masks, take)?;
        let columns: Arc<[&'static str]> = Arc::from(self.columns);
        Ok(tasks
            .into_iter()
            .map(|task| {
                let (pins, columns) = (self.pins.clone(), Arc::clone(&columns));
                async move {
                    task.await?
                        .map(|rows| pins.fill_rows(rows, &columns).map_err(VortexError::from))
                        .transpose()
                }
                .boxed()
            })
            .collect())
    }

    /// The read columns of every kept row in one array, in file order, the
    /// pinned ones not filled in: one take of all the kept ids, or, denser
    /// than a take (or a take declined), each split projected at its mask,
    /// the splits driven as a row scan's ([`collect_row_splits`]).
    pub(crate) async fn collected(&self) -> Result<ArrayRef> {
        let read_columns = self.read_columns();
        if self.read != MaskedRead::Project {
            let ids = self.masks.first_ids(usize::MAX);
            if let Some(rows) = take_file_rows(self.file, &read_columns, &ids).await? {
                return Ok(rows);
            }
        }
        let (dtype, tasks) = masked_split_tasks(self.file, &read_columns, &self.masks, false)?;
        let window = SPLITS_IN_FLIGHT_PER_CORE * available_parallelism();
        collect_row_splits(dtype, tasks, window).await
    }

    /// Every kept row in one array, in file order, every column of `columns`
    /// filled in: [`points`](Self::points), else
    /// [`collected`](Self::collected).
    pub(crate) async fn rows(&self) -> Result<ArrayRef> {
        if let Some(rows) = self.points().await? {
            return Ok(rows);
        }
        self.pins.fill_rows(self.collected().await?, self.columns)
    }
}

/// One canonical struct chunk of `columns` holding `values`, `len` rows of
/// each, every column as the primitive type its chunk-probe handle (of
/// `handles`) reads. `Ok(None)` for a column type the canonical child cannot
/// hold.
fn u64_struct(
    handles: &[Arc<vortex_rdf_encoded_search::ColumnChunks>],
    columns: &[&str],
    values: Vec<Vec<u64>>,
    len: usize,
) -> Result<Option<ArrayRef>> {
    use vortex_array::IntoArray;

    let mut children = Vec::with_capacity(columns.len());
    for (handle, values) in handles.iter().zip(values) {
        let Some(child) = primitive_from_u64_reads(handle.dtype().as_ptype(), values.into_iter())
        else {
            return Ok(None);
        };
        children.push(child);
    }
    Ok(Some(named_struct(columns, children, len)?.into_array()))
}

/// A located serve run's projected component columns, read point by point
/// through the component's cached chunk probes into one canonical
/// child-named chunk: the serve path's counterpart of [`file_point_rows`].
/// `Ok(None)` declines (a column or chunk without a probe); the caller keeps
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
    // The run's rows, under no filter: every column is read.
    let run = PointRows {
        handles,
        eqs: CodeEqs::default(),
        live: range.collect(),
    };
    let Some(values) = run.columns(file, columns).await? else {
        return Ok(None);
    };
    u64_struct(&run.handles, columns, values, run.live.len())
}

/// Test hook: how many reads this thread gathered by taking exact ids
/// ([`take_file_rows`]), through the chunk probes' leaves or split by split.
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

/// Test hook: the columns each point read ([`file_point_rows`],
/// [`file_point_codes`]), each take by id ([`take_file_rows`]) and each take
/// or projection of a filter's kept rows ([`masked_split_tasks`]) read on
/// this thread.
#[cfg(test)]
pub(crate) mod masked_column_hooks {
    use std::cell::RefCell;

    thread_local! {
        static READS: RefCell<Vec<Vec<String>>> = const { RefCell::new(Vec::new()) };
    }

    /// Record one read of `columns`.
    pub(super) fn note(columns: &[&str]) {
        READS.with_borrow_mut(|reads| {
            reads.push(columns.iter().map(|c| c.to_string()).collect());
        });
    }

    /// The columns of each point read, take or projection on this thread
    /// since the last call, in order.
    pub(crate) fn take() -> Vec<Vec<String>> {
        READS.with_borrow_mut(std::mem::take)
    }
}
