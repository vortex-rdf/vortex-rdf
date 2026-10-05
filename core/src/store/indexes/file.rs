//! The file-backed side of the index hub: resolving a probe against a file's
//! index children (run location through the children's chunk probes, rid
//! point reads and rid scans) and the file serve plan.

use std::ops::Range;

use vortex_array::ArrayRef;
use vortex_array::arrays::struct_::StructArrayExt;
use vortex_array::scalar::Scalar;
use vortex_buffer::Buffer;
use vortex_layout::scan::scan_builder::ScanBuilder;
use vortex_layout::scan::split_by::SplitBy;
use vortex_mask::Mask;

use super::components::sorted_row_ids;
use super::resolve::IndexProbe;
use super::serve::ServeDecode;
use super::{COL_RID, IndexResolution, LazyRowIds, ResolvedRowIds};
use crate::error::{Result, VortexRdfError};
use crate::store::array::into_struct_array;
use crate::store::layouts::{ChunkDecode, PatternCodes, TermRef};
use crate::store::persist::native_file::{ChildReader, NativeStoreFile};
use crate::store::scan::file_reads::{
    component_point_chunk, eq_conjunction, locate_run, scan_column,
};
use crate::store::view::selection::point_sized;

/// Resolve `probe` against a file's index children.
///
/// The keys are located in order through the child's chunk probes; a located
/// empty run is `Empty`, a declined location falls back to the pushed-down
/// scan. A serving probe answers lazy ids beside its plan, except that a
/// point-sized located run reads its ids now; a non-serving probe answers
/// eager ids. `Empty` when a key or residual term has no code.
pub(crate) async fn resolve_file(
    probe: IndexProbe<'_>,
    file: &NativeStoreFile,
    codes: &mut PatternCodes,
) -> Result<IndexResolution<FileServePlan>> {
    let Some(child) = file
        .child_reader(probe.identity.name)
        .map_err(VortexRdfError::Vortex)?
    else {
        return Ok(IndexResolution::Declined);
    };
    let Some(located) = locate_keys(file, &child, &probe.keys, codes).await? else {
        return Ok(IndexResolution::Empty);
    };
    if located.range.as_ref().is_some_and(Range::is_empty) {
        return Ok(IndexResolution::Empty);
    }
    let mut plan_constraints = located.constraints.clone();
    for (column, term) in &probe.residual {
        let Some(native) = codes.probe_scalar(*term)? else {
            return Ok(IndexResolution::Empty);
        };
        plan_constraints.push((*column, native));
    }
    let point_read = match &located.range {
        Some(range) if point_sized(range.end - range.start) => {
            rid_point_reads(file, child.name, range.clone()).await?
        }
        _ => None,
    };
    let scan = FileRowIdScan {
        child: child.clone(),
        constraints: located.constraints,
        range: located.range.clone(),
    };
    let Some(decode) = probe.serve else {
        let row_ids = match point_read {
            Some(ids) => ids,
            None => scan.run().await?,
        };
        if row_ids.is_empty() {
            return Ok(IndexResolution::Empty);
        }
        return Ok(IndexResolution::Resolved {
            row_ids: ResolvedRowIds::Eager(row_ids),
            resolves: probe.resolves,
            serve: None,
        });
    };
    // The located range covers the keys alone; a residual term demotes the
    // plan to its filter scan.
    let row_range = if probe.residual.is_empty() {
        located.range
    } else {
        None
    };
    Ok(IndexResolution::Resolved {
        row_ids: match point_read {
            Some(ids) => ResolvedRowIds::Eager(ids),
            None => ResolvedRowIds::Lazy(LazyRowIds::from_file_scan(scan)),
        },
        resolves: probe.resolves,
        serve: Some(FileServePlan::new(
            decode,
            child,
            plan_constraints,
            row_range,
        )),
    })
}

/// A probe's keys as scalars, with the run they locate: `None` when the
/// location declined, `Some(empty)` when a key is absent from the child.
struct LocatedKeys {
    constraints: Vec<(&'static str, Scalar)>,
    range: Option<Range<u64>>,
}

/// Translate and locate `keys` in order, each inside the previous key's run;
/// `None` when a key's term has no code.
async fn locate_keys(
    file: &NativeStoreFile,
    child: &ChildReader,
    keys: &[(&'static str, TermRef<'_>)],
    codes: &mut PatternCodes,
) -> Result<Option<LocatedKeys>> {
    let mut constraints = Vec::with_capacity(keys.len());
    let mut range: Option<Range<u64>> = None;
    let mut locating = true;
    for (column, term) in keys {
        let Some(native) = codes.probe_scalar(*term)? else {
            return Ok(None);
        };
        if locating {
            range = locate_component_run(file, child, column, &native, range).await?;
            locating = range.as_ref().is_some_and(|run| !run.is_empty());
        }
        constraints.push((*column, native));
    }
    Ok(Some(LocatedKeys { constraints, range }))
}

/// The run `index` locates for `pattern` through its child's chunk probes;
/// `None` when the index declines the pattern or the location declines.
#[cfg(test)]
pub(crate) async fn debug_located_run(
    index: super::IndexType,
    file: &NativeStoreFile,
    layout: &crate::store::layouts::ResolvedLayout,
    pattern: crate::store::layouts::QuadPattern<'_>,
    codes: &mut PatternCodes,
) -> Result<Option<Range<u64>>> {
    let Some(probe) = index.choose(pattern, layout) else {
        return Ok(None);
    };
    let Some(child) = file
        .child_reader(probe.identity.name)
        .map_err(VortexRdfError::Vortex)?
    else {
        return Ok(None);
    };
    Ok(locate_keys(file, &child, &probe.keys, codes)
        .await?
        .and_then(|located| located.range))
}

/// The `[lo, hi)` run of a sorted component column's rows equal to `native`,
/// located through the column's chunk probes. `within` must be a range whose
/// slice of the column is itself sorted (the whole child, or a lead run for a
/// prefix probe). `Ok(None)` declines: an unsorted child, a non-integer probe
/// value, or a column resolving no chunk handle.
pub(crate) async fn locate_component_run(
    file: &NativeStoreFile,
    child: &ChildReader,
    column: &str,
    native: &Scalar,
    within: Option<Range<u64>>,
) -> Result<Option<Range<u64>>> {
    locate_run(file, child.sorted, native, within, || {
        file.component_column_chunks(child.name, column)
    })
    .await
}

/// The base row ids of a located run, read point by point from the child's
/// rid column and sorted. `Ok(None)` when the rid chunks decline.
pub(crate) async fn rid_point_reads(
    file: &NativeStoreFile,
    component: &str,
    range: Range<u64>,
) -> Result<Option<Buffer<u64>>> {
    let Some(chunk) = component_point_chunk(file, component, &[COL_RID], range).await? else {
        return Ok(None);
    };
    let rids = into_struct_array(chunk)?
        .unmasked_field_by_name(COL_RID)
        .cloned()
        .map_err(VortexRdfError::Vortex)?;
    sorted_row_ids(rids).map(Some)
}

/// A rid-only scan of a file's index child.
#[derive(Clone)]
pub(crate) struct FileRowIdScan {
    child: ChildReader,
    /// The `column == value` equalities the rows satisfy.
    constraints: Vec<(&'static str, Scalar)>,
    /// The located run, when every constraint was located: the scan reads
    /// exactly it and carries no filter.
    range: Option<Range<u64>>,
}

impl FileRowIdScan {
    /// The matching base row ids, ascending and unique.
    pub(crate) async fn run(&self) -> Result<Buffer<u64>> {
        let scan = self.child.scan(&[COL_RID])?.with_ordered(false);
        let scan = match &self.range {
            Some(range) => scan.with_row_range(range.clone()),
            None => {
                let Some(filter) = eq_conjunction(self.constraints.iter().cloned()) else {
                    return Ok(Buffer::empty());
                };
                scan.with_filter(self.child.bind(&filter)?)
            }
        };
        sorted_row_ids(scan_column(scan, COL_RID).await?)
    }
}

/// An index's serving plan for a file view: the matched rows are the child
/// rows where every constraint holds, read by a range scan or point reads of
/// the located run, else by a scan pushing the equalities down as a filter.
#[derive(Clone)]
pub(crate) struct FileServePlan {
    decode: ServeDecode,
    child: ChildReader,
    constraints: Vec<(&'static str, Scalar)>,
    /// The child rows the constraints select, when the resolution located
    /// every one of them.
    row_range: Option<Range<u64>>,
}

/// The fewest rows a located run's scan split carries.
const SERVE_SPLIT_MIN_ROWS: u64 = 1024;

/// Rows per split for a located run of `rows`: two splits per worker, never
/// fewer than [`SERVE_SPLIT_MIN_ROWS`] rows each.
fn run_split_rows(rows: u64) -> usize {
    let workers = crate::io::read::available_parallelism() as u64;
    rows.div_ceil(2 * workers).max(SERVE_SPLIT_MIN_ROWS) as usize
}

impl FileServePlan {
    pub(crate) fn new(
        decode: ServeDecode,
        child: ChildReader,
        constraints: Vec<(&'static str, Scalar)>,
        row_range: Option<Range<u64>>,
    ) -> Self {
        Self {
            decode,
            child,
            constraints,
            row_range,
        }
    }

    /// The serving component's name.
    pub(crate) fn component(&self) -> &'static str {
        self.child.name
    }

    /// The located child-row range, when known.
    pub(crate) fn row_range(&self) -> Option<Range<u64>> {
        self.row_range.clone()
    }

    /// The projected columns of the located run, read point by point through
    /// the component's chunk probes. `Ok(None)` when the run is unlocated or a
    /// chunk declines.
    pub(crate) async fn point_chunk(&self, file: &NativeStoreFile) -> Result<Option<ArrayRef>> {
        let Some(range) = self.row_range.clone() else {
            return Ok(None);
        };
        component_point_chunk(file, self.child.name, &self.decode.projection(), range).await
    }

    /// A scan of the child projecting the served columns, filtered to the
    /// constrained rows.
    pub(crate) fn projected_filtered_scan(&self) -> Result<ScanBuilder<ArrayRef>> {
        let filter = eq_conjunction(self.constraints.iter().cloned())
            .expect("a serve plan constrains at least one column");
        Ok(self
            .child
            .scan(&self.decode.projection())?
            .with_filter(self.child.bind(&filter)?))
    }

    /// A scan of the located run projecting the served columns, split by row
    /// count so the decode spreads over the workers; `None` when the run is
    /// unlocated. No filter: the range is exactly the constrained rows.
    pub(crate) fn located_run_scan(&self) -> Result<Option<ScanBuilder<ArrayRef>>> {
        let Some(range) = self.row_range.clone() else {
            return Ok(None);
        };
        let split_rows = run_split_rows(range.end - range.start);
        Ok(Some(
            self.child
                .scan(&self.decode.projection())?
                .with_row_range(range)
                .with_split_by(SplitBy::RowCount(split_rows)),
        ))
    }

    /// The `(s, p, o, g)` rows of a chunk of the projected columns, rows
    /// tombstoned in `deleted` dropped by their rid.
    pub(crate) fn decode_columns<T: ChunkDecode>(
        &self,
        chunk: &ArrayRef,
        deleted: Option<&Mask>,
    ) -> Vec<Result<T>> {
        match self.decode.chunk_rows(chunk, deleted, true) {
            Ok(rows) => T::decode(self.decode.layout(), &rows),
            Err(e) => vec![Err(e)],
        }
    }

    /// [`decode_columns`](Self::decode_columns) through the layout's async
    /// decode, for a file-backed dictionary.
    pub(crate) async fn decode_columns_async<T: ChunkDecode>(
        &self,
        chunk: &ArrayRef,
        deleted: Option<&Mask>,
    ) -> Vec<Result<T>> {
        match self.decode.chunk_rows(chunk, deleted, true) {
            Ok(rows) => T::decode_async(self.decode.layout(), &rows).await,
            Err(e) => vec![Err(e)],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_split_rows_floor_and_arithmetic() {
        assert_eq!(run_split_rows(100), SERVE_SPLIT_MIN_ROWS as usize);
        assert_eq!(run_split_rows(0), SERVE_SPLIT_MIN_ROWS as usize);

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
