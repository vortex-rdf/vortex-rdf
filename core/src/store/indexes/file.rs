//! The file side of the index hub: the one file resolver every index's probe
//! runs through ([`resolve_file`]). It locates a probe's run in the index
//! child (a binary search over the child's cached chunk probes), reads the
//! run's row ids or defers them, and hands a serving child to a
//! [`FileServePlan`], which reads a resolved view's quads out of the
//! child's own columns.

use std::ops::Range;
use std::sync::Arc;

use vortex_array::scalar::Scalar;

use super::resolve::IndexProbe;
use super::rid_reads::{rid_point_reads, scan_index_row_ids};
use super::serve::FileServePlan;
use super::{IndexResolution, LazyRowIds, ResolvedRoles, ResolvedRowIds};
use crate::error::{Result, VortexRdfError};
use crate::store::layouts::PatternCodes;
use crate::store::persist::native_file::{ChildReader, NativeStoreFile};
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
    layout: &crate::store::layouts::ResolvedLayout,
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
