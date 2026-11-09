//! A per-query cancel token and the pull that races it: a blocking wait on
//! the bindings' runtime that a cancel from any thread stops at its next
//! await.
//!
//! Free of Python types, so `tests/cancel.rs` compiles this file as it is.

use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use futures::{Stream, StreamExt};
use tokio::runtime::Runtime;
use tokio::sync::Notify;

/// A cancel flag shared by the pulls of one query: set once, from any
/// thread, and never cleared. Clones share the flag.
#[derive(Clone, Default)]
pub(crate) struct Token(Arc<Flag>);

#[derive(Default)]
struct Flag {
    cancelled: AtomicBool,
    /// Wakes the pulls waiting in [`Token::cancelled`].
    wake: Notify,
}

impl Token {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Sets the flag: a pull racing the token stops at its next await, and
    /// a pull started later stops before its first.
    pub(crate) fn cancel(&self) {
        self.0.cancelled.store(true, Ordering::Release);
        self.0.wake.notify_waiters();
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.0.cancelled.load(Ordering::Acquire)
    }

    /// Completes once the token is cancelled.
    pub(crate) async fn cancelled(&self) {
        // Created before the flag is read: `notify_waiters` wakes every
        // `Notified` created before it, so a cancel landing after the read
        // still completes this wait.
        let woken = self.0.wake.notified();
        if !self.is_cancelled() {
            woken.await;
        }
    }
}

/// How a pull ended: its future's output, or its token's cancel.
#[derive(Debug)]
pub(crate) enum Pulled<T> {
    Done(T),
    Cancelled,
}

impl<T> Pulled<T> {
    /// The pull's output passed through `f`; a cancelled pull stays
    /// cancelled.
    pub(crate) fn map<U>(self, f: impl FnOnce(T) -> U) -> Pulled<U> {
        match self {
            Pulled::Done(output) => Pulled::Done(f(output)),
            Pulled::Cancelled => Pulled::Cancelled,
        }
    }
}

/// Drives `fut` to completion on `runtime`, blocking the calling thread.
/// Without a token this is `runtime.block_on(fut)`, with nothing spawned and
/// `fut` pinned where the pull holds it rather than moved into the runtime.
/// With one, `fut` runs as a spawned task raced with the token, the
/// token's side polled first: a token cancelled before the pull or while
/// `fut` waits at an await ends the pull there, dropping `fut` on the
/// runtime; a `fut` that has completed is returned whatever the token says
/// since. A panic in `fut` is re-raised on the calling thread.
pub(crate) fn pull<F>(runtime: &Runtime, token: Option<&Token>, fut: F) -> Pulled<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let Some(token) = token else {
        return Pulled::Done(runtime.block_on(pin!(fut)));
    };
    let token = token.clone();
    let task = runtime.spawn(async move {
        tokio::select! {
            biased;
            () = token.cancelled() => Pulled::Cancelled,
            output = fut => Pulled::Done(output),
        }
    });
    match runtime.block_on(task) {
        Ok(pulled) => pulled,
        Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
        // The runtime aborted the task: the pull never completed.
        Err(_) => Pulled::Cancelled,
    }
}

/// The next item of the stream in `slot`, pulled as [`pull`] drives a
/// future: the pull owns the stream while it waits and puts it back in
/// `slot` when it ends with an item or with the stream's end; a cancelled
/// pull has dropped it, leaving `slot` empty. An empty `slot` answers
/// `Done(None)`.
pub(crate) fn pull_next<S>(
    runtime: &Runtime,
    token: Option<&Token>,
    slot: &mut Option<S>,
) -> Pulled<Option<S::Item>>
where
    S: Stream + Unpin + Send + 'static,
    S::Item: Send + 'static,
{
    let Some(mut stream) = slot.take() else {
        return Pulled::Done(None);
    };
    pull(runtime, token, async move {
        let item = stream.next().await;
        (stream, item)
    })
    .map(|(stream, item)| {
        *slot = Some(stream);
        item
    })
}
