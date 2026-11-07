//! The store's zone statistics, each part of them owned here:
//! - [`value_counts`]: the `vortexrdf.value_counts` zone aggregate, each
//!   zone's distinct values with their row counts, and its state's format;
//! - [`zone_map`]: the exact zone maps a store file carries, written beside
//!   its code columns and read back ([`ColumnZones`](zone_map::ColumnZones)),
//!   and the footer's whole-column bounds;
//! - [`zone_set`]: one table's zone checks against those maps, which every
//!   zone walk asks;
//! - [`quad_zones`]: an in-memory base's zone maps, built in the pass that
//!   compresses its code columns, or read from a file's zone tables;
//! - [`rules`]: the falsifier Vortex's own pruning applies from the recorded
//!   values, and the switch that turns the store's zone decisions off
//!   (`VORTEX_RDF_ZONES=0`).

pub(crate) mod quad_zones;
pub(crate) mod rules;
pub(crate) mod value_counts;
pub(crate) mod zone_map;
pub(crate) mod zone_set;
