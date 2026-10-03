//! Debug-only timing helpers for `log::debug!` lines: [`timer`] starts a
//! timer only when debug logging is enabled, [`elapsed`] reads it.

use std::time::Duration;
use web_time::Instant;

/// A timer when debug logging is enabled, else `None` (on wasm
/// `Instant::now()` crosses the JS boundary).
pub fn timer() -> Option<Instant> {
    log::log_enabled!(log::Level::Debug).then(Instant::now)
}

/// The elapsed time on a [`timer`]; zero when none was started.
pub fn elapsed(t: Option<Instant>) -> Duration {
    t.map_or(Duration::ZERO, |started| started.elapsed())
}
