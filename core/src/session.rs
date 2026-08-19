//! Crate-wide Vortex session infrastructure. Lives at the crate root,
//! executing *any* Vortex kernel — an in-memory decode as much as a
//! file scan — needs the session's registries.

use std::sync::LazyLock;

use vortex_array::scalar_fn::session::ScalarFnSession;
use vortex_array::session::ArraySession;
use vortex_io::session::RuntimeSession;
use vortex_layout::session::LayoutSession;
use vortex_session::VortexSession;

#[cfg(any(
    all(feature = "file-io", not(target_arch = "wasm32")),
    all(target_arch = "wasm32", target_os = "unknown")
))]
use vortex_io::session::RuntimeSessionExt;

/// The one Vortex session: arrays, layouts, scalar kernels, and a runtime.
///
/// Every target reads and writes Vortex *files* (the wasm bindings exchange
/// file bytes via `open_buffer`/`to_bytes`), so every target needs the same
/// registries — a single session keeps the encoding registry from diverging
/// between targets. The runtime handle is the only per-target piece: tokio
/// natively, the microtask-queue `WasmRuntime` on wasm (required by the file
/// writer's task spawning), and none for native no-file-io builds, whose code
/// paths are all handle-free.
pub(crate) static VORTEX_SESSION: LazyLock<VortexSession> = LazyLock::new(|| {
    let session = VortexSession::empty()
        .with::<ArraySession>()
        .with::<LayoutSession>()
        .with::<ScalarFnSession>()
        .with::<RuntimeSession>();
    #[cfg(all(feature = "file-io", not(target_arch = "wasm32")))]
    let session = session.with_tokio();
    #[cfg(all(target_arch = "wasm32", target_os = "unknown"))]
    let session = session.with_handle(vortex_io::runtime::wasm::WasmRuntime::handle());
    vortex_file::register_default_encodings(&session);
    crate::io::container::register(&session);
    session
});
