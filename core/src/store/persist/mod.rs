//! The store's persistence boundary: opening files and bytes ([`open`]) and
//! the resident forms an adoption picks ([`ResidentForm`]), the file handle
//! and its caches ([`native_file`]), writing the native container
//! (`serialize`) and textual RDF (`export`).
mod export;
mod forms;
#[cfg(feature = "file-io")]
pub(crate) mod native_file;
pub(crate) mod open;
mod serialize;

pub use export::export_rdf;
pub use forms::{CodeForm, ResidentForm};
