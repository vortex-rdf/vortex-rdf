pub mod de;
pub mod ser;

use std::sync::LazyLock;
use vortex_session::VortexSession;
use vortex_array::session::ArraySession;
use vortex_array::scalar_fn::session::ScalarFnSession;
use vortex_io::session::RuntimeSession;
#[cfg(not(target_arch = "wasm32"))]
use vortex_io::session::RuntimeSessionExt;
use vortex_layout::session::LayoutSession;

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

pub static VORTEX_LIGHT_SESSION: LazyLock<VortexSession> = LazyLock::new(|| {
    let session = VortexSession::empty()
        .with::<ArraySession>()
        .with::<ScalarFnSession>();
    vortex_file::register_default_encodings(&session);
    session
});

pub use de::{
    deserialize,
    array_from_ipc_reader
};
#[cfg(feature = "file-io")]
pub use de::{load_vortex_file_ref, open_vortex_file};

pub use ser::{
    serialize,
    write_array_to_ipc,
    quads_stream_to_vortex,
    quads_stream_to_vortex_writer
};
