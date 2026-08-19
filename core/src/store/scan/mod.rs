//! The store's query-execution tier: turning a view (a selection plus
//! residual constraints) into the concrete rows that match it.
//!
//! `typed_eq` is the in-memory half — typed residual-equality fast paths over
//! a base's columns: slice compares where a code column is canonical,
//! encoded-search point reads where it is not, view-level compares for string
//! columns. `file_scan` is the file-backed half — per-split filter evaluation,
//! statistics-only pruning envelopes, pushed-down filter construction, row and
//! component gathers through the file's cached column-chunk probes, and the
//! translation of a [`RowSelection`] onto vortex's scan knobs. The point-read
//! paths on both sides are gated on selection width, declining to the
//! vectorized mask pipeline above it. The row-set algebra itself stays in
//! [`selection`](super::selection); what lives here is its execution against
//! a backend.
//!
//! [`RowSelection`]: super::selection::RowSelection

#[cfg(feature = "file-io")]
pub(crate) mod file_scan;
pub(crate) mod typed_eq;
