//! A store's persisted forms: opening files and byte buffers, the file
//! handle, serialization, and RDF export.

pub(crate) mod export;
#[cfg(feature = "file-io")]
pub(crate) mod native_file;
pub(crate) mod open;
// The parts every write path produces: compiled in where a store is written
// (`file-io`, wasm32) and for the unit tests that build and adopt them.
#[cfg(any(feature = "file-io", target_arch = "wasm32", test))]
pub(crate) mod serialize;
