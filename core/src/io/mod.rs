pub mod de;
pub mod ser;

pub use de::{
    deserialize,
    array_from_ipc_reader
};
#[cfg(feature = "file-io")]
pub use de::{load_vortex_file_ref, open_vortex_file};

pub use ser::{
    serialize,
    quads_stream_to_vortex,
    quads_stream_to_vortex_writer
};
