//! The store's persistence boundary: opening files and bytes ([`open`]), the
//! file handle and its caches ([`native_file`]), writing the native container
//! (`serialize`) and textual RDF (`export`).
mod export;
#[cfg(feature = "file-io")]
pub(crate) mod native_file;
pub(crate) mod open;
mod serialize;

pub use export::export_rdf;
