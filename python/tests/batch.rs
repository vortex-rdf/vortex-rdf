//! The bounded batch driver of the Python bindings, compiled without Python
//! (the crate's own test target is off: it would need to link libpython),
//! together with the cancellable pull it answers through.
//!
//! Each probe is a future whose completion order the test fixes with
//! notifications, so which result the batch reports never depends on timing.

#[path = "../src/batch.rs"]
mod batch;
#[path = "../src/cancel.rs"]
mod cancel;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, mpsc};
use std::time::{Duration, Instant};

use tokio::runtime::{Builder, Runtime};
use tokio::sync::Notify;
use vortex_rdf_core::VortexRdfError;

use batch::Batch;
use cancel::{Pulled, Token};

fn runtime() -> Runtime {
    Builder::new_multi_thread()
        .worker_threads(4)
        .build()
        .unwrap()
}

/// What a pull without a token answers: it is never cancelled.
fn done<T>(pulled: Pulled<T>) -> T {
    match pulled {
        Pulled::Done(value) => value,
        Pulled::Cancelled => panic!("a pull without a token was cancelled"),
    }
}

/// `count` probes that finish in reverse input order, all of them in the
/// window: probe `i` waits for probe `i + 1` to finish, then answers
/// `outcomes[i]`. The batch is pulled one answer at a time, without a token,
/// until it ends; an error comes back as its message.
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
    std::iter::from_fn(|| done(batch.next(runtime, None)))
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
    let answers = done(
        Batch::spawn(
            &runtime,
            &(),
            Vec::<usize>::new(),
            2,
            |(), probe| async move { Ok::<_, VortexRdfError>(probe) },
        )
        .collect(&runtime, None),
    );
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
    let error = done(
        Batch::spawn(
            &runtime,
            &dropped,
            (0..4).collect::<Vec<usize>>(),
            4,
            |dropped, probe| {
                // The guard rides with the future from its creation, so a
                // probe aborted before its first poll is counted too.
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
        .collect(&runtime, None),
    )
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
        .collect(&runtime, None)
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

/// Eight probes through a window of two, counting the probes spawned,
/// finished and dropped: the first answers `first`, and the rest never
/// finish.
fn stalled_batch(
    runtime: &Runtime,
    first: Result<usize, &'static str>,
) -> (
    Batch<usize>,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
) {
    let spawned = Arc::new(AtomicUsize::new(0));
    let finished = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let counters = (
        Arc::clone(&spawned),
        Arc::clone(&finished),
        Arc::clone(&dropped),
    );
    let batch = Batch::spawn(
        runtime,
        &counters,
        (0..8).collect::<Vec<usize>>(),
        2,
        move |(spawned, finished, dropped), probe| {
            spawned.fetch_add(1, Ordering::SeqCst);
            let guard = Dropped(dropped);
            async move {
                let _guard = guard;
                if probe != 0 {
                    std::future::pending::<()>().await;
                }
                finished.fetch_add(1, Ordering::SeqCst);
                first.map_err(|message| VortexRdfError::InvalidOperation(message.to_string()))
            }
        },
    );
    (batch, spawned, finished, dropped)
}

/// A batch dropped after its first answer aborts the probes in its window
/// and spawns none of the rest: the driver, running freely, spawns at most
/// one probe past a window of two before the drop lands, and every spawned
/// probe but the first is dropped unfinished.
#[test]
fn dropping_the_batch_aborts_what_is_in_flight_and_starts_nothing() {
    let runtime = runtime();
    let (mut batch, spawned, finished, dropped) = stalled_batch(&runtime, Ok(0));
    assert_eq!(
        done(batch.next(&runtime, None)).map(Result::unwrap),
        Some(0)
    );
    drop(batch);
    // Once every spawned probe is dropped the window is gone: a probe still
    // in it holds the count below the spawns.
    wait_until("every spawned probe to be dropped", || {
        dropped.load(Ordering::SeqCst) == spawned.load(Ordering::SeqCst)
    });
    let started = spawned.load(Ordering::SeqCst);
    assert!(started <= 3, "{started} probes spawned for a window of two");
    assert_eq!(
        finished.load(Ordering::SeqCst),
        1,
        "an aborted probe finished"
    );
}

/// A failing first probe ends the batch at its answer and starts none of
/// the rest: with nothing pulled yet, the driver stops at the failure it
/// forwards, so exactly two probes are ever spawned through a window of two
/// and the second is dropped unfinished; the batch then answers the
/// failure.
#[test]
fn a_failure_starts_none_of_the_rest() {
    let runtime = runtime();
    let (batch, spawned, finished, dropped) = stalled_batch(&runtime, Err("probe 0"));
    // The batch is held and unpulled, so its channel stays open: only the
    // driver's own stop at the failure drops the window and the stalled
    // second probe in it.
    wait_until("both spawned probes to be dropped", || {
        dropped.load(Ordering::SeqCst) == 2
    });
    assert_eq!(
        spawned.load(Ordering::SeqCst),
        2,
        "a probe was spawned past the failure"
    );
    assert_eq!(
        finished.load(Ordering::SeqCst),
        1,
        "an aborted probe finished"
    );
    let error = done(batch.collect(&runtime, None)).expect_err("probe 0 fails");
    assert_eq!(message(error), "probe 0");
}

/// Between pulls the driver keeps the window full until the answers
/// waiting fill their channel: with a window of two over probes that
/// answer at once, it spawns four (two answers in the channel, one in its
/// hand, one in the window) and no more until a pull takes an answer.
#[test]
fn the_window_refills_between_pulls_up_to_its_bound() {
    let runtime = runtime();
    let spawned = Arc::new(AtomicUsize::new(0));
    let mut batch = Batch::spawn(
        &runtime,
        &spawned,
        (0..8).collect::<Vec<usize>>(),
        2,
        |spawned, probe| {
            spawned.fetch_add(1, Ordering::SeqCst);
            async move { Ok::<_, VortexRdfError>(probe) }
        },
    );
    wait_until("the driver to fill its bound", || {
        spawned.load(Ordering::SeqCst) == 4
    });
    std::thread::sleep(Duration::from_millis(50));
    assert_eq!(
        spawned.load(Ordering::SeqCst),
        4,
        "the driver spawned past its bound"
    );
    let answers: Vec<usize> = std::iter::from_fn(|| done(batch.next(&runtime, None)))
        .map(Result::unwrap)
        .collect();
    assert_eq!(answers, (0..8).collect::<Vec<_>>());
}

/// A pull whose token is cancelled while it waits ends the batch: the
/// probes in its window are aborted, none of the rest is spawned, and every
/// later pull answers nothing.
#[test]
fn a_cancelled_pull_aborts_the_window() {
    let runtime = Arc::new(runtime());
    let (mut batch, spawned, finished, dropped) = stalled_batch(&runtime, Ok(0));
    let token = Token::new();
    assert_eq!(
        done(batch.next(&runtime, Some(&token))).map(Result::unwrap),
        Some(0)
    );
    let canceller = std::thread::spawn({
        let token = token.clone();
        move || {
            std::thread::sleep(Duration::from_millis(50));
            token.cancel();
        }
    });
    // Pulled on a thread of its own, so a pull that misses the cancel fails
    // the test rather than hanging it.
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn({
        let (runtime, token) = (Arc::clone(&runtime), token.clone());
        move || {
            let cancelled = matches!(batch.next(&runtime, Some(&token)), Pulled::Cancelled);
            let _ = sender.send((cancelled, batch));
        }
    });
    let (cancelled, mut batch) = receiver
        .recv_timeout(Duration::from_secs(1))
        .expect("the cancel did not land within a second");
    assert!(cancelled, "the pull answered instead of stopping");
    canceller.join().unwrap();
    wait_until("every spawned probe to be dropped", || {
        dropped.load(Ordering::SeqCst) == spawned.load(Ordering::SeqCst)
    });
    assert_eq!(
        finished.load(Ordering::SeqCst),
        1,
        "an aborted probe finished"
    );
    for token in [Some(&token), None] {
        assert!(matches!(batch.next(&runtime, token), Pulled::Done(None)));
    }
    let started = spawned.load(Ordering::SeqCst);
    assert!(
        started <= 3,
        "{started} probes spawned for a window of two: one after the cancel"
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
    done(
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
        .collect(runtime, None),
    )
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
