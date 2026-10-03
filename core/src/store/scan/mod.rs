//! Executing a view against a backend: `typed_eq` (typed residual-equality
//! row loops over an in-memory base), `gather` (rows out of an in-memory
//! base), `file_filter` (per-split filter evaluation and pruning over a file)
//! and `file_reads` (scan and point reads over a file).

#[cfg(feature = "file-io")]
pub(crate) mod file_filter;
#[cfg(feature = "file-io")]
pub(crate) mod file_reads;
pub(crate) mod gather;
pub(crate) mod typed_eq;

/// Both file halves under the name the read, query and write paths outside
/// this module use.
#[cfg(feature = "file-io")]
pub(crate) mod file_scan {
    pub(crate) use super::file_filter::*;
    pub(crate) use super::file_reads::*;
}
