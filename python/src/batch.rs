//! A batch of probes run on the bindings' runtime through a bounded,
//! ordered window: at most `lookahead` probes are spawned and unanswered at
//! a time, and their answers come back in input order, each pulled as
//! `cancel.rs` pulls, with or without a cancel token.
//!
//! Free of Python types, so `tests/batch.rs` compiles this file (with
//! `cancel.rs`) as it is.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::stream::{self, BoxStream, StreamExt};
use tokio::runtime::Runtime;
use tokio::task::{JoinError, JoinHandle};
use vortex_rdf_core::VortexRdfError as CoreError;

use crate::cancel::{Pulled, Token, pull, pull_next};

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

/// The answers of the window, as its spawned probes join.
type Window<T> = BoxStream<'static, Result<Result<T, CoreError>, JoinError>>;

/// The answers of a batch of probes, pulled in input order. Each probe runs
/// as a task of its own, so the probes of the window spread over the
/// runtime's workers (an in-memory match is CPU work; a file-backed one
/// overlaps its reads). The window is an ordered `buffered(lookahead)` over
/// the spawns: a pull first spawns probes until `lookahead` of them are
/// unanswered (or none is left to spawn), then waits for the earliest, so
/// nothing runs ahead of the window and nothing is spawned between pulls.
/// Dropping the batch, or cancelling a pull's token, aborts the probes in
/// its window and spawns none of the rest.
pub(crate) struct Batch<T> {
    /// The answers as the window yields them; `None` once the batch is
    /// over.
    window: Option<Window<T>>,
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
    /// `runtime` until it is in, pulled as [`pull_next`] pulls with `token`;
    /// `None` after the last. An error ends the batch: the window is
    /// dropped, aborting the probes in flight, and every later pull answers
    /// `None`. A cancelled pull ends it the same way, its window dropped
    /// with the pull. A probe that panics ends the batch, then re-raises
    /// its panic, as it would from a single call.
    pub(crate) fn next(
        &mut self,
        runtime: &Runtime,
        token: Option<&Token>,
    ) -> Pulled<Option<Result<T, CoreError>>> {
        let joined = match pull_next(runtime, token, &mut self.window) {
            Pulled::Done(joined) => joined,
            Pulled::Cancelled => return Pulled::Cancelled,
        };
        if let Some(Ok(Ok(value))) = joined {
            return Pulled::Done(Some(Ok(value)));
        }
        // The batch is over: dropping the window aborts the probes still in
        // flight.
        self.window = None;
        Pulled::Done(joined.map(answer))
    }

    /// Every answer in input order, or the error of the first probe in input
    /// order that fails, blocking the calling thread on `runtime` in one
    /// pull with `token`.
    pub(crate) fn collect(
        self,
        runtime: &Runtime,
        token: Option<&Token>,
    ) -> Pulled<Result<Vec<T>, CoreError>> {
        let Batch { window, len } = self;
        pull(runtime, token, async move {
            let mut answers = Vec::with_capacity(len);
            if let Some(mut window) = window {
                while let Some(joined) = window.next().await {
                    answers.push(answer(joined)?);
                }
            }
            Ok(answers)
        })
    }
}

/// A joined probe's answer: its own result, a panic re-raised, or the
/// error of a task the runtime cancelled.
fn answer<T>(joined: Result<Result<T, CoreError>, JoinError>) -> Result<T, CoreError> {
    match joined {
        Ok(answer) => answer,
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        Err(error) => Err(CoreError::InvalidOperation(format!(
            "a batch probe task failed: {error}"
        ))),
    }
}
