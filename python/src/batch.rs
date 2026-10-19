//! A batch of probes run on the bindings' runtime through a bounded,
//! ordered window: at most `lookahead` probes are spawned and unanswered at
//! a time, and their answers come back in input order.
//!
//! Free of Python types, so `tests/batch.rs` compiles this file as it is.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::stream::{self, BoxStream, StreamExt};
use tokio::runtime::Runtime;
use tokio::task::{JoinError, JoinHandle};
use vortex_rdf_core::VortexRdfError as CoreError;

/// A spawned probe's handle, which aborts the probe when dropped before the
/// probe has finished.
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

/// The answers of a batch of probes, pulled in input order. Each probe runs
/// as a task of its own, so the probes of the window spread over the
/// runtime's workers (an in-memory match is CPU work; a file-backed one
/// overlaps its reads). The window is an ordered `buffered(lookahead)` over
/// the spawns: a pull spawns the probes that bring it back to `lookahead`
/// unanswered ones, then waits for the earliest, so nothing runs ahead of
/// the window and nothing is spawned between pulls. Dropping the batch
/// aborts the probes in its window and spawns none of the rest.
pub(crate) struct Batch<T> {
    /// The answers as the window yields them; `None` once the batch is
    /// over.
    window: Option<BoxStream<'static, Result<Result<T, CoreError>, JoinError>>>,
    /// The number of probes.
    len: usize,
}

impl<T: Send + 'static> Batch<T> {
    /// A batch running `task` on `runtime` for each probe, with `shared`
    /// cloned into every task and at most `lookahead` probes (at least one)
    /// spawned and unanswered at a time. The first pull spawns the first
    /// window.
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
        let window = stream::iter(probes)
            .map(move |probe| AbortOnDrop(handle.spawn(task(shared.clone(), probe))))
            .buffered(lookahead)
            .boxed();
        Batch {
            window: Some(window),
            len,
        }
    }

    /// The next answer in input order, blocking the calling thread on
    /// `runtime` until it is in; `None` after the last. An error ends the
    /// batch: the window is dropped, aborting the probes in flight, and
    /// every later call answers `None`. A probe that panics ends the batch
    /// the same way, then re-raises its panic, as it would from a single
    /// call.
    pub(crate) fn next(&mut self, runtime: &Runtime) -> Option<Result<T, CoreError>> {
        runtime.block_on(self.next_answer())
    }

    /// Every answer in input order, or the error of the first probe in input
    /// order that fails, blocking the calling thread on `runtime`.
    pub(crate) fn collect(mut self, runtime: &Runtime) -> Result<Vec<T>, CoreError> {
        runtime.block_on(async {
            let mut answers = Vec::with_capacity(self.len);
            while let Some(answer) = self.next_answer().await {
                answers.push(answer?);
            }
            Ok(answers)
        })
    }

    /// [`next`](Self::next), awaited.
    async fn next_answer(&mut self) -> Option<Result<T, CoreError>> {
        let joined = self.window.as_mut()?.next().await;
        let last = match joined {
            Some(Ok(Ok(answer))) => return Some(Ok(answer)),
            Some(Ok(Err(error))) => Some(Err(error)),
            Some(Err(error)) if error.is_panic() => {
                self.window = None;
                std::panic::resume_unwind(error.into_panic())
            }
            Some(Err(error)) => Some(Err(CoreError::InvalidOperation(format!(
                "a batch probe task failed: {error}"
            )))),
            None => None,
        };
        // The batch is over: dropping the window aborts the probes still in
        // flight.
        self.window = None;
        last
    }
}
