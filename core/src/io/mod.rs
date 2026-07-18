pub mod de;
pub mod ser;

use std::sync::LazyLock;
use vortex_session::VortexSession;
use vortex_array::session::ArraySession;
use vortex_array::scalar_fn::session::ScalarFnSession;

#[cfg(feature = "file-io")]
use vortex_io::session::RuntimeSession;
#[cfg(feature = "file-io")]
use vortex_io::session::RuntimeSessionExt;
#[cfg(feature = "file-io")]
use vortex_layout::session::LayoutSession;

/// Full session including layout and async runtime — used for file I/O.
#[cfg(feature = "file-io")]
pub static VORTEX_SESSION: LazyLock<VortexSession> = LazyLock::new(|| {
    let session = VortexSession::empty()
        .with::<ArraySession>()
        .with::<LayoutSession>()
        .with::<ScalarFnSession>()
        .with::<RuntimeSession>();
    #[cfg(not(target_arch = "wasm32"))]
    let session = session.with_tokio();
    vortex_file::register_default_encodings(&session);
    session
});

/// Minimal session for in-memory array operations and IPC.
pub static VORTEX_LIGHT_SESSION: LazyLock<VortexSession> = LazyLock::new(|| {
    let session = VortexSession::empty()
        .with::<ArraySession>()
        .with::<ScalarFnSession>();
    #[cfg(feature = "file-io")]
    vortex_file::register_default_encodings(&session);
    session
});

pub use de::{
    deserialize,
    array_from_ipc_reader
};
#[cfg(feature = "file-io")]
pub use de::{load_vortex_file_ref, open_vortex_file};

pub use ser::write_array_to_ipc;
#[cfg(feature = "file-io")]
pub use ser::{
    serialize,
    quads_stream_to_vortex,
    quads_stream_to_vortex_writer,
    quads_stream_to_vortex_writer_with_builder,
};
