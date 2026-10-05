//! The tokio runtime the bench targets drive their async calls through.
//! Included by `support/mod.rs` and, via `#[path]`, by `compare.rs`.

use std::sync::OnceLock;

use tokio::runtime::{Builder, Runtime};

/// The one tokio runtime every bench drives its async calls through.
///
/// A worker pool, as the library's consumers run it — except under CodSpeed
/// (`CODSPEED_ENV`, which its runner sets), where every cell runs once under
/// instruction-count simulation. valgrind serializes threads, so a pool adds
/// no parallelism there, only the spin-up, work-stealing and park-or-spin
/// instructions whose count moves from run to run: the scheduling flicker
/// that let one binary measure a cold cell at 2.6 ms once and 3.5 ms the
/// next time. A current-thread runtime makes the count deterministic to a
/// few hundred instructions and lower by the scheduler's own overhead. The
/// wall-clock dashboard runs and a local `cargo bench` keep the pool.
pub fn rt() -> &'static Runtime {
    static RT: OnceLock<Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        if std::env::var_os("CODSPEED_ENV").is_some() {
            Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("current-thread runtime")
        } else {
            Runtime::new().expect("multi-thread runtime")
        }
    })
}
