pub mod de;
pub mod ser;

pub use de::{deserialize, array_from_reader};

#[cfg(feature = "file-io")]
pub use de::{load_vortex_file_ref, load_vortex_file_path};

pub use ser::{quads_stream_to_vortex, quads_stream_to_vortex_writer, serialize};
