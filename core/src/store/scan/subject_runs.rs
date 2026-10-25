//! Runs of a file's sorted subject column, located through its chunk probes:
//! a bound subject's run, narrowed by the roles behind it (the file mirror of
//! the in-memory prefix probe), and the runs a keep's subject codes hold.

use std::ops::Range;
use std::sync::Arc;

use oxrdf::NamedOrBlankNode;

use crate::error::{Result, VortexRdfError};
use crate::store::layouts::{PatternCodes, QuadPattern, TermRef};
use crate::store::persist::native_file::NativeStoreFile;
use crate::store::schema::{self, TermCode};

/// The exact row range of `subject`'s run in a sorted file, by binary search
/// over the subject column's encoded chunks (the column's codes ascend, as
/// codes are sorted ranks). `None` declines (an unsorted file, a column whose
/// chunk shape has no probe, or a subject without a native probe scalar:
/// under a string layout or for a term the dictionary lacks), and the caller
/// keeps the scan path.
pub(crate) async fn locate_subject_run(
    file: &NativeStoreFile,
    codes: &mut PatternCodes,
    subject: &NamedOrBlankNode,
) -> Result<Option<Range<u64>>> {
    if !file.quads_sorted() {
        return Ok(None);
    }
    let Ok(Some(needle)) = codes.probe_code(TermRef::Subject(subject)) else {
        return Ok(None);
    };
    let Some(chunks) = sorted_subject_chunks(file) else {
        return Ok(None);
    };
    subject_code_bounds(&chunks, file, needle).await
}

/// Narrow `run`, a bound subject's rows in a file sorted by `(s, p, o, g)`, by
/// the roles behind the subject in that order: each bound role whose term has
/// a code is binary-searched inside the run through its column's chunk probes
/// and cleared from `pattern`: the file mirror of the in-memory prefix probe.
/// The first role that is unbound, has no code or whose chunks decline ends
/// the prefix, as does an empty run. Answers how many roles it cleared.
pub(crate) async fn narrow_subject_run(
    file: &NativeStoreFile,
    codes: &mut PatternCodes,
    pattern: &mut QuadPattern<'_>,
    run: &mut Range<u64>,
) -> Result<usize> {
    let source = file.segment_source();
    let session = file.session();
    let mut answered = 0;
    for term in pattern.prefix_behind_subject() {
        if run.is_empty() {
            break;
        }
        let Some(chunks) = file.column_chunks(term.role().name()) else {
            break;
        };
        let Some(needle) = codes.probe_code(term)? else {
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
    pattern.clear_prefix_behind_subject(answered);
    Ok(answered)
}

/// The subject column's chunk-probe handle for a file sorted by subject.
/// `None` declines: an unsorted file, or a column whose chunk shape has no
/// probe.
fn sorted_subject_chunks(
    file: &NativeStoreFile,
) -> Option<Arc<vortex_rdf_encoded_search::ColumnChunks>> {
    if !file.quads_sorted() {
        return None;
    }
    file.column_chunks(schema::COL_S)
}

/// The exact row range of subject `code` in the sorted subject column; empty,
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
/// run: from the first row whose code is `>= lo` to the first whose code is
/// `>= hi`, one lower-bound search each, exact whether or not the column
/// holds `lo` or `hi`. `None` declines (an unsorted file, a column whose
/// chunk shape has no probe), and the caller keeps the stream.
pub(crate) async fn locate_subject_code_range(
    file: &NativeStoreFile,
    range: Range<TermCode>,
) -> Result<Option<Range<u64>>> {
    let Some(chunks) = sorted_subject_chunks(file) else {
        return Ok(None);
    };
    let source = file.segment_source();
    let lower_bound = |code| chunks.lower_bound(code, &source, file.session());
    let Some(from) = lower_bound(range.start).await? else {
        return Ok(None);
    };
    let Some(to) = lower_bound(range.end).await? else {
        return Ok(None);
    };
    Ok(Some(from..to.max(from)))
}

/// The rows holding each subject code of `codes` (ascending and unique, as
/// [`Keep::set`](crate::store::Keep::set) builds them) in a sorted file, as
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
