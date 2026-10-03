//! The native container codec: the `vortex-rdf.store.v1` grammar
//! (`container`), the write driver (`write`) and read-side file access
//! (`read`).

pub(crate) mod container;
pub(crate) mod read;
/// The write side; compiled natively behind `file-io` and on wasm, whose
/// bindings exchange file bytes.
#[cfg(any(feature = "file-io", target_arch = "wasm32"))]
pub(crate) mod write;

#[cfg(feature = "file-io")]
pub use write::{quads_stream_to_vortex_file, quads_stream_to_vortex_writer};
