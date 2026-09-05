//! Resolving a pattern into a view (`matching`), narrowing a view beyond
//! its pattern ([`pushdown`]: keeps and windows) and cutting it into
//! partitions (`partition`).
mod matching;
mod partition;
pub(crate) mod pushdown;
