//! The cancellable pull of the Python bindings, compiled without Python
//! (the crate's own test target is off: it would need to link libpython).
//!
//! A pull waits at an await nobody completes, as a file read waits at a
//! split boundary, so where a cancel lands never depends on timing.

#[path = "../src/cancel.rs"]
mod cancel;

use std::sync::Arc;
use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use futures::stream::{self, StreamExt};
use tokio::runtime::{Builder, Runtime};
use tokio::sync::Notify;

use cancel::{Pulled, Token, pull, pull_hooks, pull_next};

fn runtime() -> Arc<Runtime> {
    Arc::new(
        Builder::new_multi_thread()
            .worker_threads(2)
            .build()
            .unwrap(),
    )
}

/// Runs `then` on another thread 50 ms from now.
fn after_50_ms(then: impl FnOnce() + Send + 'static) -> JoinHandle<()> {
    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(50));
        then();
    })
}

/// What `work` returns on a thread of its own, with the pulls that thread
/// spawned; `None` when it has not returned within a second, so a pull
/// that misses its cancel fails the test rather than hanging it.
fn within_a_second<T: Send + 'static>(
    work: impl FnOnce() -> T + Send + 'static,
) -> Option<(T, usize)> {
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        let spawns = pull_hooks::SPAWNS.get();
        let answer = work();
        let _ = sender.send((answer, pull_hooks::SPAWNS.get() - spawns));
    });
    receiver.recv_timeout(Duration::from_secs(1)).ok()
}

/// A pull waiting at an await nothing will complete stops there once
/// another thread cancels its token, within a second, and runs as a task of
/// its own. A stream pull with the cancelled token drops its stream.
#[test]
fn cancel_lands_at_the_next_await() {
    let runtime = runtime();
    let token = Token::new();
    let never = Arc::new(Notify::new());
    let canceller = after_50_ms({
        let token = token.clone();
        move || token.cancel()
    });
    let (pulled, spawns) = within_a_second({
        let (runtime, token, never) = (Arc::clone(&runtime), token.clone(), Arc::clone(&never));
        move || {
            pull(&runtime, Some(&token), async move {
                never.notified().await;
                "resumed"
            })
        }
    })
    .expect("the cancel did not land within a second");
    assert!(
        matches!(pulled, Pulled::Cancelled),
        "the pull did not stop at its await"
    );
    assert_eq!(spawns, 1, "a pull with a token runs as a spawned task");
    canceller.join().unwrap();
    assert!(token.is_cancelled());

    let (stream_dropped, _) = within_a_second({
        let (runtime, token) = (Arc::clone(&runtime), token.clone());
        move || {
            let mut slot = Some(
                stream::once(async move {
                    never.notified().await;
                    "resumed"
                })
                .boxed(),
            );
            let pulled = pull_next(&runtime, Some(&token), &mut slot);
            matches!(pulled, Pulled::Cancelled) && slot.is_none()
        }
    })
    .expect("a pull with a cancelled token did not stop within a second");
    assert!(stream_dropped, "a cancelled stream pull kept its stream");
}

/// Without a token a pull is the runtime's `block_on`: it waits as long as
/// its future does and spawns no task, and a stream pull hands its stream
/// back.
#[test]
fn without_a_token_pull_is_block_on() {
    let runtime = runtime();
    let spawns = pull_hooks::SPAWNS.get();
    assert!(matches!(pull(&runtime, None, async { 7 }), Pulled::Done(7)));

    let release = Arc::new(Notify::new());
    let notifier = after_50_ms({
        let release = Arc::clone(&release);
        move || release.notify_one()
    });
    let started = Instant::now();
    let pulled = pull(&runtime, None, async move {
        release.notified().await;
        "notified"
    });
    assert!(matches!(pulled, Pulled::Done("notified")));
    assert!(
        started.elapsed() >= Duration::from_millis(50),
        "the pull returned before its future was notified"
    );
    notifier.join().unwrap();

    let mut slot = Some(stream::iter([1, 2]));
    assert!(matches!(
        pull_next(&runtime, None, &mut slot),
        Pulled::Done(Some(1))
    ));
    assert!(slot.is_some(), "a finished pull kept the stream");
    assert_eq!(
        pull_hooks::SPAWNS.get(),
        spawns,
        "a pull without a token spawned a task"
    );
}
