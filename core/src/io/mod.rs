//! The native container codec: the `vortex-rdf.store.v2` grammar
//! (`container`), the write driver (`write`) and read-side file access
//! (`read`). Its public items are the two file writers, behind `file-io`.

pub(crate) mod container;
pub(crate) mod read;
pub(crate) mod write;

#[cfg(feature = "file-io")]
pub use write::{quads_stream_to_vortex_file, quads_stream_to_vortex_writer};
