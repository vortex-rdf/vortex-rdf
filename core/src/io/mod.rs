//! The native container codec: the `vortex-rdf.store.v2` grammar
//! (`container`), the write driver (`write`), the read side's scan drivers
//! and open errors (`read`), the exact zone maps a store writes and reads
//! back (`zone_map`), and, behind `file-io`, a store file's atomic
//! replacement at a path (`pending`) and its opening, mapped or read
//! (`file`). Its public items are the two file writers, behind `file-io`.

pub(crate) mod container;
#[cfg(feature = "file-io")]
pub(crate) mod file;
#[cfg(feature = "file-io")]
pub(crate) mod pending;
pub(crate) mod read;
pub(crate) mod write;
pub(crate) mod zone_map;

#[cfg(feature = "file-io")]
pub use write::{quads_stream_to_vortex_file, quads_stream_to_vortex_writer};
