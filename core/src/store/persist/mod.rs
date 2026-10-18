//! A store's persisted forms: opening files and byte buffers, the file
//! handle, serialization, and RDF export.

pub(crate) mod export;
#[cfg(feature = "file-io")]
pub(crate) mod native_file;
pub(crate) mod open;
pub(crate) mod serialize;
