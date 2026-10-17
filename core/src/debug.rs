//! Debug-only timing helpers for `log::debug!` instrumentation: every timed
//! log line in this crate starts its timer with [`timer`] and reads it with
//! [`elapsed`], so timing costs nothing when debug logging is off. Only the
//! match pipeline's steps differ: they log through `step!`, which reads the
//! timer itself.

use std::time::Duration;
use web_time::Instant;

/// Start a timer only when debug logging will read it: on wasm
/// `Instant::now()` is a JS-boundary `performance.now()` call, so hot paths
/// must not pay for it when the log line is discarded anyway.
pub(crate) fn timer() -> Option<Instant> {
    log::log_enabled!(log::Level::Debug).then(Instant::now)
}

/// The elapsed time on a [`timer`] — zero when the timer was never started
/// (debug logging off, in which case `log::debug!` discards the value without
/// formatting it).
pub(crate) fn elapsed(t: Option<Instant>) -> Duration {
    t.map_or(Duration::ZERO, |started| started.elapsed())
}

/// Log one step of a pipeline that carries the [`timer`] `t`: `message` (a
/// format string and its arguments), then ` at ` and the time elapsed on
/// the timer, at debug level and under the calling module's target. Without
/// a started timer (`None`) nothing is logged and no argument is evaluated.
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
