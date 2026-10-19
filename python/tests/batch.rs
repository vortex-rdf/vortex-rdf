//! The bounded batch driver of the Python bindings, compiled without Python
//! (the crate's own test target is off: it would need to link libpython).
//!
//! Each probe is a future whose completion order the test fixes with
//! notifications, so which result the batch reports never depends on timing.

#[path = "../src/batch.rs"]
mod batch;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use tokio::runtime::{Builder, Runtime};
use tokio::sync::Notify;
use vortex_rdf_core::VortexRdfError;

use batch::Batch;

fn runtime() -> Runtime {
    Builder::new_multi_thread()
        .worker_threads(4)
        .build()
        .unwrap()
}

/// `count` probes that finish in reverse input order, all of them in the
/// window: probe `i` waits for probe `i + 1` to finish, then answers
/// `outcomes[i]`. The batch is pulled one answer at a time until it ends;
/// an error comes back as its message.
fn finishing_in_reverse(
    runtime: &Runtime,
    outcomes: Vec<Result<usize, &'static str>>,
) -> Vec<Result<usize, String>> {
    let count = outcomes.len();
    let finished: Arc<Vec<Notify>> = Arc::new((0..count).map(|_| Notify::new()).collect());
    let probes: Vec<(usize, Result<usize, &'static str>)> =
        outcomes.into_iter().enumerate().collect();
    let mut batch = Batch::spawn(
        runtime,
        &finished,
        probes,
        count,
        move |finished, (index, outcome)| async move {
            if index + 1 < count {
                finished[index + 1].notified().await;
            }
            finished[index].notify_one();
            outcome.map_err(|message| VortexRdfError::InvalidOperation(message.to_string()))
        },
    );
    std::iter::from_fn(|| batch.next(runtime))
        .map(|answer| answer.map_err(message))
        .collect()
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
    let answers = finishing_in_reverse(&runtime, (0..8).map(Ok).collect());
    assert_eq!(answers, (0..8).map(Ok).collect::<Vec<_>>());
}

#[test]
fn no_probes_answer_nothing() {
    let runtime = runtime();
    let answers = Batch::spawn(
        &runtime,
        &(),
        Vec::<usize>::new(),
        2,
        |(), probe| async move { Ok::<_, VortexRdfError>(probe) },
    )
    .collect(&runtime);
    assert!(answers.unwrap().is_empty());
}

/// The batch answers the error of the first failing probe in input order,
/// not of the first to fail (probe 1 fails before probe 0 does), and
/// nothing after it.
#[test]
fn the_first_failing_probe_in_input_order_is_the_one_reported() {
    let runtime = runtime();
    for _ in 0..50 {
        let answers = finishing_in_reverse(&runtime, vec![Err("probe 0"), Err("probe 1")]);
        assert_eq!(answers, vec![Err("probe 0".to_string())]);
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
        let answers = finishing_in_reverse(&runtime, outcomes);
        assert_eq!(
            answers,
            vec![Ok(0), Ok(1), Ok(2), Err("probe 3".to_string())]
        );
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

/// A probe that never finishes does not hold up the batch once an earlier
/// probe has failed, and the batch aborts it.
#[test]
fn a_failure_aborts_the_probes_still_running() {
    let runtime = runtime();
    let dropped = Arc::new(AtomicUsize::new(0));
    let error = Batch::spawn(
        &runtime,
        &dropped,
        (0..4).collect::<Vec<usize>>(),
        4,
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
    .collect(&runtime)
    .expect_err("probe 0 fails");
    assert_eq!(message(error), "probe 0");
    wait_until("the three pending probes to be aborted", || {
        dropped.load(Ordering::SeqCst) == 3
    });
}

/// A probe that panics re-raises its panic from the batch, and the probes
/// still running are aborted.
#[test]
fn a_panicking_probe_reraises_and_aborts_the_rest() {
    let runtime = runtime();
    let dropped = Arc::new(AtomicUsize::new(0));
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        Batch::spawn(
            &runtime,
            &dropped,
            (0..4).collect::<Vec<usize>>(),
            4,
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
        .collect(&runtime)
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

/// A batch dropped after its first answer aborts the probes in its window
/// and spawns none of the rest. With a window of two over eight probes that
/// never finish past the first, at most three are ever spawned, and every
/// spawned probe but the first is dropped unfinished.
#[test]
fn dropping_the_batch_aborts_what_is_in_flight_and_starts_nothing() {
    let runtime = runtime();
    let spawned = Arc::new(AtomicUsize::new(0));
    let finished = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let counters = (
        Arc::clone(&spawned),
        Arc::clone(&finished),
        Arc::clone(&dropped),
    );
    let mut batch = Batch::spawn(
        &runtime,
        &counters,
        (0..8).collect::<Vec<usize>>(),
        2,
        |(spawned, finished, dropped), probe| {
            spawned.fetch_add(1, Ordering::SeqCst);
            let guard = Dropped(dropped);
            async move {
                let _guard = guard;
                if probe != 0 {
                    std::future::pending::<()>().await;
                }
                finished.fetch_add(1, Ordering::SeqCst);
                Ok::<_, VortexRdfError>(probe)
            }
        },
    );
    assert_eq!(batch.next(&runtime).map(Result::unwrap), Some(0));
    drop(batch);
    let started = spawned.load(Ordering::SeqCst);
    assert!(started <= 3, "{started} probes spawned for a window of two");
    wait_until("every spawned probe to be dropped", || {
        dropped.load(Ordering::SeqCst) == started
    });
    assert_eq!(
        finished.load(Ordering::SeqCst),
        1,
        "an aborted probe finished"
    );
    assert_eq!(
        spawned.load(Ordering::SeqCst),
        started,
        "a probe was spawned after the drop"
    );
}

/// The answers of `outcomes` through a window of `lookahead`, collected:
/// probe `i` yields to the runtime once for each probe after it before it
/// answers, so with several in the window the later probes finish first.
fn answered_through(
    runtime: &Runtime,
    outcomes: &[Result<usize, &'static str>],
    lookahead: usize,
) -> Result<Vec<usize>, String> {
    let count = outcomes.len();
    let probes: Vec<(usize, Result<usize, &'static str>)> =
        outcomes.iter().copied().enumerate().collect();
    Batch::spawn(
        runtime,
        &(),
        probes,
        lookahead,
        move |(), (index, outcome)| async move {
            for _ in index..count {
                tokio::task::yield_now().await;
            }
            outcome.map_err(|message| VortexRdfError::InvalidOperation(message.to_string()))
        },
    )
    .collect(runtime)
    .map_err(message)
}

/// A window of one runs the probes one at a time and answers as the whole
/// batch in the window does: the same answers, or the same first error.
#[test]
fn lookahead_one_answers_like_the_whole_batch() {
    let runtime = runtime();
    let answering: Vec<Result<usize, &'static str>> = (0..8).map(Ok).collect();
    let failing = vec![Ok(0), Err("probe 1"), Ok(2), Err("probe 3")];
    assert_eq!(
        answered_through(&runtime, &answering, 1),
        Ok((0..8).collect())
    );
    assert_eq!(
        answered_through(&runtime, &answering, 1),
        answered_through(&runtime, &answering, answering.len())
    );
    assert_eq!(
        answered_through(&runtime, &failing, 1),
        Err("probe 1".to_string())
    );
    assert_eq!(
        answered_through(&runtime, &failing, 1),
        answered_through(&runtime, &failing, failing.len())
    );
}
