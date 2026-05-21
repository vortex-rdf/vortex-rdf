pub mod de;
pub mod ser;

pub use de::{array_from_reader, deserialize};

#[cfg(feature = "file-io")]
pub use de::{load_vortex_file_path, load_vortex_file_ref};

pub use ser::{quads_stream_to_vortex, quads_stream_to_vortex_writer, serialize};
