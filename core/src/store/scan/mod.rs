//! Executing a view against a backend: a selection plus residual constraints
//! turned into the rows that match. In memory, `typed_eq` holds the typed
//! residual-equality paths (slice compares over canonical code columns,
//! encoded-search point reads otherwise, view compares for strings) and
//! `gather` the tombstone-aware gather of a selection's rows. On file,
//! `file_exprs` builds the pushed-down filters, `file_scans` drives row scans
//! and maps a [`RowSelection`] onto vortex's scan knobs, `file_filter`
//! evaluates a filter split by split and computes statistics-only pruning
//! envelopes, `subject_runs` locates runs of the sorted subject column,
//! `read_plan` chooses how an exact selection's rows are read,
//! `file_reads` reads them by id through the file's cached chunk probes, and
//! `masked_chunks` streams a filtered view's rows by the filter's masks. The
//! point-read paths on both sides decline above a selection width to the
//! vectorized pipeline. The row-set algebra itself is in
//! [`selection`](super::view::selection).
//!
//! [`RowSelection`]: super::view::selection::RowSelection

#[cfg(feature = "file-io")]
pub(crate) mod file_exprs;
#[cfg(feature = "file-io")]
pub(crate) mod file_filter;
#[cfg(feature = "file-io")]
pub(crate) mod file_reads;
#[cfg(feature = "file-io")]
pub(crate) mod file_scans;
pub(crate) mod gather;
#[cfg(feature = "file-io")]
pub(crate) mod masked_chunks;
#[cfg(feature = "file-io")]
pub(crate) mod read_plan;
#[cfg(feature = "file-io")]
pub(crate) mod subject_runs;
pub(crate) mod typed_eq;
