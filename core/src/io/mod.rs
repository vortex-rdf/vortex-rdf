pub mod de;
pub mod ser;
pub mod cottas_native;

pub use de::{array_from_reader, deserialize};

pub use de::{
    deserialize,
    array_from_ipc_reader
};
#[cfg(feature = "file-io")]
pub use de::{load_vortex_file_ref, open_vortex_file};

pub use ser::{quads_stream_to_vortex, quads_stream_to_vortex_writer, serialize};

pub use cottas_native::{
    CottasNativeConfig,
    match_cottas_native_file,
    serialize_cottas_native_file,
};