//! The batch fan-out of the Python bindings, compiled without Python (the
//! crate's own test target is off: it would need to link libpython).
//!
//! Each probe is a future whose completion order the test fixes with
//! notifications, so which result the call reports never depends on timing.

#[path = "../src/fan_out.rs"]
mod fan_out;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use tokio::runtime::{Builder, Runtime};
use tokio::sync::Notify;
use vortex_rdf_core::VortexRdfError;

use fan_out::fan_out;

fn runtime() -> Runtime {
    Builder::new_multi_thread()
        .worker_threads(4)
        .build()
        .unwrap()
}

/// `count` probes that finish in reverse input order: probe `i` waits for
/// probe `i + 1` to finish, then answers `outcomes[i]`.
fn finishing_in_reverse(
    runtime: &Runtime,
    outcomes: Vec<Result<usize, &'static str>>,
) -> Result<Vec<usize>, VortexRdfError> {
    let count = outcomes.len();
    let finished: Arc<Vec<Notify>> = Arc::new((0..count).map(|_| Notify::new()).collect());
    let probes: Vec<(usize, Result<usize, &'static str>)> =
        outcomes.into_iter().enumerate().collect();
    fan_out(
        runtime,
        &finished,
        probes,
        move |finished, (index, outcome)| async move {
            if index + 1 < count {
                finished[index + 1].notified().await;
            }
            finished[index].notify_one();
            outcome.map_err(|message| VortexRdfError::InvalidOperation(message.to_string()))
        },
    )
}

fn message(error: VortexRdfError) -> String {
    match error {
        VortexRdfError::InvalidOperation(message) => message,
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn answers_come_back_in_input_order_whatever_the_finishing_order() {
    let runtime = runtime();
    let answers = finishing_in_reverse(&runtime, (0..8).map(Ok).collect()).unwrap();
    assert_eq!(answers, (0..8).collect::<Vec<_>>());
}

#[test]
fn no_probes_answer_nothing() {
    let runtime = runtime();
    let answers = fan_out(&runtime, &(), Vec::<usize>::new(), |(), probe| async move {
        Ok::<_, VortexRdfError>(probe)
    });
    assert!(answers.unwrap().is_empty());
}

/// The call raises the error of the first failing probe in input order, not
/// of the first to fail: probe 1 fails before probe 0 does.
#[test]
fn the_first_failing_probe_in_input_order_is_the_one_reported() {
    let runtime = runtime();
    for _ in 0..50 {
        let error = finishing_in_reverse(&runtime, vec![Err("probe 0"), Err("probe 1")])
            .expect_err("both probes fail");
        assert_eq!(message(error), "probe 0");
    }
}

#[test]
fn the_lowest_failing_index_is_reported_when_later_probes_fail_first() {
    let runtime = runtime();
    for _ in 0..50 {
        let outcomes = vec![
            Ok(0),
            Ok(1),
            Ok(2),
            Err("probe 3"),
            Ok(4),
            Err("probe 5"),
            Err("probe 6"),
            Ok(7),
        ];
        let error = finishing_in_reverse(&runtime, outcomes).expect_err("some probes fail");
        assert_eq!(message(error), "probe 3");
    }
}

/// Counts the futures dropped, which is what aborting a task does to its
/// future.
struct Dropped(Arc<AtomicUsize>);

impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn wait_until(what: &str, condition: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !condition() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// A probe that never finishes does not hold up the call once an earlier
/// probe has failed, and the call aborts it.
#[test]
fn a_failure_aborts_the_probes_still_running() {
    let runtime = runtime();
    let dropped = Arc::new(AtomicUsize::new(0));
    let error = fan_out(
        &runtime,
        &dropped,
        (0..4).collect::<Vec<usize>>(),
        |dropped, probe| {
            // The guard rides with the future from its creation, so a probe
            // aborted before its first poll is counted too.
            let guard = (probe != 0).then(|| Dropped(dropped));
            async move {
                if probe == 0 {
                    return Err(VortexRdfError::InvalidOperation("probe 0".to_string()));
                }
                let _guard = guard;
                std::future::pending::<()>().await;
                Ok(probe)
            }
        },
    )
    .expect_err("probe 0 fails");
    assert_eq!(message(error), "probe 0");
    wait_until("the three pending probes to be aborted", || {
        dropped.load(Ordering::SeqCst) == 3
    });
}

/// A probe that panics re-raises its panic from the call, and the probes
/// still running are aborted.
#[test]
fn a_panicking_probe_reraises_and_aborts_the_rest() {
    let runtime = runtime();
    let dropped = Arc::new(AtomicUsize::new(0));
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        fan_out(
            &runtime,
            &dropped,
            (0..4).collect::<Vec<usize>>(),
            |dropped, probe| {
                let guard = (probe != 0).then(|| Dropped(dropped));
                async move {
                    if probe == 0 {
                        panic!("probe 0 panicked");
                    }
                    let _guard = guard;
                    std::future::pending::<()>().await;
                    Ok::<_, VortexRdfError>(probe)
                }
            },
        )
    }));
    let payload = outcome.expect_err("the panic reaches the caller");
    let text = payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
        .unwrap_or_default();
    assert_eq!(text, "probe 0 panicked");
    wait_until("the three pending probes to be aborted", || {
        dropped.load(Ordering::SeqCst) == 3
    });
}
