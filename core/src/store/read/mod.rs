//! Reading a view out: counts, rows, code columns, decoded quads, chunk
//! streams and the vortex data sources over them, and the metadata that
//! describes a view before it is read.

pub(crate) mod chunks;
pub(crate) mod count;
pub(crate) mod data_source;
pub(crate) mod metadata;
pub(crate) mod quads;
pub(crate) mod rows;
