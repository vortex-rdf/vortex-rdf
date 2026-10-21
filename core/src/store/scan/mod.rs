//! Executing a view against a backend: a selection plus residual constraints
//! turned into the rows that match. In memory, `typed_eq` holds the typed
//! residual-equality paths (slice compares over canonical code columns,
//! encoded-search point reads otherwise, view compares for strings) and
//! `gather` the tombstone-aware gather of a selection's rows. On file,
//! `file_filter` evaluates a filter split by split and computes
//! statistics-only pruning envelopes, and `file_reads` builds pushed-down
//! filters, reads rows and components through the file's cached chunk
//! probes, and maps a [`RowSelection`] onto vortex's scan knobs. The
//! point-read paths on both sides decline above a selection width to the
//! vectorized pipeline. The row-set algebra itself is in
//! [`selection`](super::view::selection).
//!
//! [`RowSelection`]: super::view::selection::RowSelection

#[cfg(feature = "file-io")]
pub(crate) mod file_filter;
#[cfg(feature = "file-io")]
pub(crate) mod file_reads;
pub(crate) mod gather;
pub(crate) mod typed_eq;
