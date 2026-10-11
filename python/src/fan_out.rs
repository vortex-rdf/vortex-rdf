//! Fan a batch of probes out over the bindings' runtime.
//!
//! Free of Python types, so `tests/fan_out.rs` compiles this file as it is.

use std::future::Future;

use tokio::runtime::Runtime;
use tokio::task::JoinHandle;
use vortex_rdf_core::VortexRdfError as CoreError;

/// The tasks of a batch. Dropping the batch aborts every task not yet awaited,
/// whether the call returns early, a probe's panic unwinds, or a spawn panics.
struct Batch<T> {
    handles: Vec<JoinHandle<T>>,
    awaited: usize,
}

impl<T> Drop for Batch<T> {
    fn drop(&mut self) {
        for handle in &self.handles[self.awaited..] {
            handle.abort();
        }
    }
}

/// Run `task` for every probe on `runtime`, one task per probe so a batch
/// spreads over its workers (an in-memory match is CPU work; a file-backed
/// one overlaps its reads), and collect the answers in input order. `shared`
/// is cloned into every task. Called GIL-released.
///
/// The tasks are awaited in input order, so the call ends with the error of
/// the first probe in input order that fails, however the probes finish, and
/// aborts the probes after it that are still queued or running. A probe that
/// panics re-raises its panic, as it would from a single call.
pub(crate) fn fan_out<S, P, T, F, Fut>(
    runtime: &Runtime,
    shared: &S,
    probes: Vec<P>,
    task: F,
) -> Result<Vec<T>, CoreError>
where
    S: Clone,
    T: Send + 'static,
    F: Fn(S, P) -> Fut,
    Fut: Future<Output = Result<T, CoreError>> + Send + 'static,
{
    let mut batch = Batch {
        handles: Vec::with_capacity(probes.len()),
        awaited: 0,
    };
    for probe in probes {
        batch
            .handles
            .push(runtime.spawn(task(shared.clone(), probe)));
    }
    runtime.block_on(async move {
        let mut answers = Vec::with_capacity(batch.handles.len());
        while batch.awaited < batch.handles.len() {
            let joined = (&mut batch.handles[batch.awaited]).await;
            batch.awaited += 1;
            match joined {
                Ok(answer) => answers.push(answer?),
                Err(error) if error.is_panic() => std::panic::resume_unwind(error.into_panic()),
                Err(error) => {
                    return Err(CoreError::InvalidOperation(format!(
                        "a batch probe task failed: {error}"
                    )));
                }
            }
        }
        Ok(answers)
    })
}
