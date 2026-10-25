//! Reads by id through a [`NativeStoreFile`]'s cached chunk probes: rows and
//! codes point by point, takes at exact ids, a located serve run's component
//! columns, a filter checked at sparse ids, and the reads of the rows a
//! filter's split masks keep, as [`read_plan`](super::read_plan) chooses
//! them. Functions of the file, with no store state.

use std::future::Future;
use std::ops::Range;
use std::sync::Arc;

use futures::future::BoxFuture;
use futures::{FutureExt as _, StreamExt, stream};
use vortex_array::dtype::DType;
use vortex_array::expr::{Expression, root, select};
use vortex_array::{ArrayRef, MaskFuture};
use vortex_buffer::{Buffer, BufferMut};
use vortex_error::VortexResult;
use vortex_layout::scan::scan_builder::ScanBuilder;
use vortex_mask::{AllOr, Mask};

use crate::error::{Result, VortexRdfError};
use crate::io::read::available_parallelism;
use crate::store::persist::native_file::NativeStoreFile;
use crate::store::scan::file_exprs::{QUAD_SCOPE, eq_code_pairs};
use crate::store::scan::file_filter::{self, SplitMasks};
use crate::store::scan::file_scans::{collect_row_splits, read_all_rows};
use crate::store::scan::gather::primitive_from_u64_reads;
use crate::store::scan::read_plan::{Footprint, POINT_READ_ROWS};
use crate::store::schema::{RowId, TermCode};

/// The rows of a point read when it answers, otherwise the rows of `scan`:
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
    filter: Option<&Expression>,
    rows: Vec<u64>,
) -> Result<Option<ArrayRef>> {
    let Some(points) = PointRows::read(file, columns, filter, rows).await? else {
        return Ok(None);
    };
    point_read_struct(file, &points.handles, columns, &points.live, |col| {
        points.constant(col)
    })
    .await
}

/// [`file_point_rows`] as one code buffer per column of `columns`, in that
/// order, for a Dictionary file: the same rows, read point by point through
/// the chunk probes, without building the struct a row read returns.
/// `Ok(None)` declines as [`file_point_rows`] does.
pub(crate) async fn file_point_codes(
    file: &NativeStoreFile,
    columns: &[&str],
    filter: Option<&Expression>,
    rows: Vec<u64>,
) -> Result<Option<Vec<Buffer<TermCode>>>> {
    let Some(points) = PointRows::read(file, columns, filter, rows).await? else {
        return Ok(None);
    };
    let source = file.segment_source();
    let session = file.session();
    let mut out = Vec::with_capacity(columns.len());
    for (handle, col) in points.handles.iter().zip(columns) {
        let codes = match points.constant(col) {
            Some(code) => BufferMut::full(code, points.live.len()).freeze(),
            None => {
                let mut codes = BufferMut::with_capacity(points.live.len());
                for &row in &points.live {
                    match handle.value_at(row, &source, session).await? {
                        Some(code) => codes.push(code),
                        None => return Ok(None),
                    }
                }
                codes.freeze()
            }
        };
        out.push(codes);
    }
    Ok(Some(out))
}

/// What a point read by id reads: the chunk-probe handle of each read
/// column, the code equalities of the pending filter, and the rows that pass
/// them.
struct PointRows {
    handles: Vec<Arc<vortex_rdf_encoded_search::ColumnChunks>>,
    eqs: Vec<(String, u64)>,
    live: Vec<u64>,
}

impl PointRows {
    /// The point read of `columns` at `rows` with `filter` pending: `None`
    /// when the filter is not code equalities, a column has no chunk probes,
    /// or a chunk the filter's reads reach has no probe.
    async fn read(
        file: &NativeStoreFile,
        columns: &[&str],
        filter: Option<&Expression>,
        rows: Vec<u64>,
    ) -> Result<Option<Self>> {
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
        Ok(Some(Self { handles, eqs, live }))
    }

    /// The code every surviving row reads in `column`, when the filter pins
    /// it.
    fn constant(&self, column: &str) -> Option<u64> {
        self.eqs.iter().find(|(c, _)| c == column).map(|(_, v)| *v)
    }
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

/// The ids of `ids` whose rows satisfy `filter`, a conjunction of term
/// equalities, read point by point through the file's chunk probes: what a
/// filtered scan of the splits holding them answers, without decoding those
/// splits. `None` declines (a filter of another shape, a column without
/// chunk probes, or ids denser than one per [`POINT_READ_ROWS`] rows of
/// their splits, where the scan costs less), and the caller keeps the filter.
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

/// The rows of `columns` that `masks` keep, each split with a match projected
/// at its mask ([`masked_split_tasks`]), in one array in split order, the
/// splits driven as [`read_all_rows`] drives a scan's
/// ([`collect_row_splits`]).
pub(crate) async fn masked_projection(
    file: &Arc<NativeStoreFile>,
    columns: &[&'static str],
    masks: &SplitMasks,
) -> Result<ArrayRef> {
    let (dtype, tasks) = masked_split_tasks(file, columns, masks, false)?;
    collect_row_splits(dtype, tasks, 4 * available_parallelism()).await
}

/// `columns` at `rows`, read point by point through their chunk-probe
/// `handles` into one canonical struct chunk; a column `constant` answers is
/// filled with that value instead of being read. `Ok(None)` declines: a
/// chunk without a probe, or a column type the canonical child cannot hold.
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
    let rows: Vec<u64> = range.collect();
    point_read_struct(file, &handles, columns, &rows, |_| None).await
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
