//! A batch of probes run on the bindings' runtime through a bounded,
//! ordered window that a driver task keeps full: at most `lookahead` probes
//! are in flight, at most `lookahead` answers wait to be pulled, and the
//! answers come back in input order, each pulled as `cancel.rs` pulls, with
//! or without a cancel token. The calls default `lookahead` to twice the
//! machine's parallelism, and at least 64 (`DEFAULT_LOOKAHEAD`, `store.rs`).
//!
//! Free of Python types, so `tests/batch.rs` compiles this file (with
//! `cancel.rs`) as it is.

use std::future::Future;
use std::pin::{Pin, pin};
use std::task::{Context, Poll};

use futures::stream::{self, BoxStream, StreamExt};
use tokio::runtime::Runtime;
use tokio::sync::mpsc;
use tokio::task::{JoinError, JoinHandle};
use vortex_rdf_core::VortexRdfError as CoreError;

use crate::cancel::{Pulled, Token, pull, pull_next};

/// A spawned task's handle, which aborts the task when dropped before the
/// task has finished.
struct AbortOnDrop<T>(JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl<T> Future for AbortOnDrop<T> {
    type Output = Result<T, JoinError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx)
    }
}

/// A spawned probe's outcome as it joins: its answer, or how its task
/// failed.
type Joined<T> = Result<Result<T, CoreError>, JoinError>;

/// The answers of a batch of probes, pulled in input order. Each probe runs
/// as a task of its own, so the probes of the window spread over the
/// runtime's workers (an in-memory match is CPU work; a file-backed one
/// overlaps its reads). A driver task, spawned with the batch, polls an
/// ordered `buffered(lookahead)` window over the spawns and forwards each
/// answer, in input order, into a channel of `lookahead` answers: the
/// window refills as soon as a probe answers, whether or not anyone is
/// pulling, until the channel is full. So at most `lookahead` probes are in
/// the window, at most `lookahead` answers wait in the channel, and one more
/// waits in the driver's hand. The driver forwards a failure and stops,
/// dropping the window. Dropping the batch, or cancelling a pull's token,
/// closes the channel and aborts the driver, which drops the window and
/// aborts the probes in it at once.
pub(crate) struct Batch<T> {
    /// The answers the driver forwards, in input order; `None` once the
    /// batch is over.
    answers: Option<BoxStream<'static, Joined<T>>>,
    /// The driver; dropping it aborts the driver, dropping its window.
    driver: Option<AbortOnDrop<()>>,
    /// The number of probes.
    len: usize,
}

impl<T: Send + 'static> Batch<T> {
    /// A batch running `task` on `runtime` for each probe, with `shared`
    /// cloned into every task and at most `lookahead` probes (at least one)
    /// in flight. The driver starts at once, spawning the first window.
    pub(crate) fn spawn<S, P, F, Fut>(
        runtime: &Runtime,
        shared: &S,
        probes: Vec<P>,
        lookahead: usize,
        task: F,
    ) -> Self
    where
        S: Clone + Send + 'static,
        P: Send + 'static,
        F: Fn(S, P) -> Fut + Send + 'static,
        Fut: Future<Output = Result<T, CoreError>> + Send + 'static,
    {
        assert!(lookahead > 0, "a batch's window holds at least one probe");
        let len = probes.len();
        let handle = runtime.handle().clone();
        let shared = shared.clone();
        let (sender, mut receiver) = mpsc::channel(lookahead);
        let driver = runtime.spawn(async move {
            let mut window = pin!(
                stream::iter(probes)
                    .map(move |probe| AbortOnDrop(handle.spawn(task(shared.clone(), probe))))
                    .buffered(lookahead)
            );
            loop {
                let joined = tokio::select! {
                    biased;
                    () = sender.closed() => break,
                    joined = window.next() => match joined {
                        Some(joined) => joined,
                        None => break,
                    },
                };
                let failed = !matches!(joined, Ok(Ok(_)));
                if sender.send(joined).await.is_err() || failed {
                    break;
                }
            }
        });
        Batch {
            answers: Some(stream::poll_fn(move |cx| receiver.poll_recv(cx)).boxed()),
            driver: Some(AbortOnDrop(driver)),
            len,
        }
    }

    /// The next answer in input order, blocking the calling thread on
    /// `runtime` until it is in, pulled as [`pull_next`] pulls with `token`;
    /// `None` after the last. An error ends the batch: the driver has
    /// stopped at it, the channel is dropped, and every later call answers
    /// `None`. A cancelled pull ends it the same way, the channel dropped
    /// with the pull. A probe that panics ends the batch, then re-raises its
    /// panic, as it would from a single call.
    pub(crate) fn next(
        &mut self,
        runtime: &Runtime,
        token: Option<&Token>,
    ) -> Pulled<Option<Result<T, CoreError>>> {
        let joined = match pull_next(runtime, token, &mut self.answers) {
            Pulled::Done(joined) => joined,
            Pulled::Cancelled => {
                self.driver = None;
                return Pulled::Cancelled;
            }
        };
        if let Some(Ok(Ok(value))) = joined {
            return Pulled::Done(Some(Ok(value)));
        }
        // The batch is over: dropping the channel and the driver aborts the
        // probes still in flight.
        self.answers = None;
        self.driver = None;
        Pulled::Done(joined.map(answer))
    }

    /// Every answer in input order, or the error of the first probe in input
    /// order that fails. The answers are collected by a task of their own
    /// beside the driver, and the calling thread waits for that task on
    /// `runtime` in one pull with `token`, rather than once per answer.
    pub(crate) fn collect(
        self,
        runtime: &Runtime,
        token: Option<&Token>,
    ) -> Pulled<Result<Vec<T>, CoreError>> {
        let Batch {
            answers,
            driver,
            len,
        } = self;
        let collecting = runtime.spawn(async move {
            // The driver runs as long as the collect does.
            let _driver = driver;
            let mut collected = Vec::with_capacity(len);
            if let Some(mut answers) = answers {
                while let Some(joined) = answers.next().await {
                    collected.push(answer(joined)?);
                }
            }
            Ok(collected)
        });
        match pull(runtime, token, AbortOnDrop(collecting)) {
            Pulled::Done(joined) => Pulled::Done(answer(joined)),
            Pulled::Cancelled => Pulled::Cancelled,
        }
    }
}

/// A joined task's answer (a probe's, or a collect's): its own result, a
/// panic re-raised, or the error of a task the runtime cancelled.
fn answer<T>(joined: Joined<T>) -> Result<T, CoreError> {
    match joined {
        Ok(answer) => answer,
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        Err(error) => Err(CoreError::InvalidOperation(format!(
            "a batch task failed: {error}"
        ))),
    }
}
