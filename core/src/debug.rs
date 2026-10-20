//! Timing helpers for `log::debug!` lines: [`timer`] starts a timer only when
//! debug logging is enabled, [`elapsed`] reads it, and `step!` logs a pipeline
//! step with the time on its timer.

use std::time::Duration;
use web_time::Instant;

/// A started timer when debug logging is enabled, else `None` (on wasm
/// `Instant::now()` crosses the JS boundary). The check is made for this
/// module's target, `vortex_rdf_core::debug`: a filter that enables other
/// modules of the crate but not this one gets `None`, so their timed lines
/// read zero and `step!` logs nothing.
pub(crate) fn timer() -> Option<Instant> {
    log::log_enabled!(log::Level::Debug).then(Instant::now)
}

/// The time elapsed on a [`timer`]; zero when none was started.
pub(crate) fn elapsed(t: Option<Instant>) -> Duration {
    t.map_or(Duration::ZERO, |started| started.elapsed())
}

/// Log `message` (a format string and its arguments), then ` at ` and the time
/// elapsed on the [`timer`] `t`, at debug level under the calling module's
/// target. Without a started timer nothing is logged and no argument is
/// evaluated.
macro_rules! step {
    ($t:expr, $($message:tt)+) => {
        if let Some(started) = $t {
            log::debug!("{} at {:?}", format_args!($($message)+), started.elapsed());
        }
    };
}
pub(crate) use step;

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use web_time::Instant;

    /// `step!` evaluates no argument unless it logs: not without a started
    /// timer, and not with one while debug logging is off — the state of the
    /// lib tests, which install no logger, so the `log` level stays `Off`.
    #[test]
    fn step_evaluates_no_argument_unless_it_logs() {
        assert!(
            log::max_level() < log::LevelFilter::Debug,
            "the lib tests install no logger, so debug logging is off"
        );
        let evaluations = Cell::new(0u32);
        let arg = || {
            evaluations.set(evaluations.get() + 1);
            evaluations.get()
        };

        step!(None::<Instant>, "{}", arg());
        assert_eq!(evaluations.get(), 0, "evaluated without a timer");

        step!(Some(Instant::now()), "{}", arg());
        assert_eq!(evaluations.get(), 0, "evaluated with debug logging off");
    }
}
