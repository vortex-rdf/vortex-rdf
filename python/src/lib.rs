//! Python bindings for Vortex-RDF, exposed as the private extension module
//! `vortex_rdf._native`, re-exported by the pure-Python `vortex_rdf`
//! package. The rdflib `Store` integration lives in the separate
//! `vortex-rdflib` package/repository, built on top of these bindings.
//!
//! Unlike the wasm bindings, this crate keeps core's `file-io` feature on:
//! stores are opened lazily from `.vortex` files and queried in place.

mod batch;
mod cancel;
mod chunks;
mod codes;
mod probes;
mod serialize;
mod store;

use std::future::Future;
use std::pin::pin;
use std::sync::LazyLock;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use vortex_rdf_core::VortexRdfError as CoreError;

use crate::cancel::{Pulled, Token, pull};

// Each exception's module is the extension's import path, as each class's is:
// pickle finds the exception there, and a traceback names it as it is imported.
pyo3::create_exception!(
    vortex_rdf._native,
    VortexRdfError,
    pyo3::exceptions::PyException,
    "Raised when a Vortex-RDF store operation fails: a store that does not read, an operation \
     the store refuses. An invalid argument raises ValueError instead, and an I/O failure \
     OSError."
);

pyo3::create_exception!(
    vortex_rdf._native,
    JoinTooLarge,
    VortexRdfError,
    "Raised when a join would make more row pairs than its `max_pairs` allows, before any pair \
     is built. `pairs` is the join's pair count; `args` is `(message, pairs)`, the message \
     naming the count and the cap."
);

pyo3::create_exception!(
    vortex_rdf._native,
    Cancelled,
    VortexRdfError,
    "Raised by a call or a pull whose `CancelToken` was cancelled before it completed: the work \
     it had in flight is dropped, and an iterator that raises it stops."
);

/// Shared runtime for all blocking native calls. Created lazily so a process
/// that forks before touching the bindings never inherits runtime threads.
pub(crate) static RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to create tokio runtime for vortex_rdf._native")
});

/// A core error as the Python exception it raises: an invalid argument (a
/// malformed term, an unknown name, predicate kind or option, candidate codes
/// out of order) is a `ValueError`; an I/O failure is the `OSError` subclass
/// its kind names (`FileNotFoundError`, `PermissionError`, …); everything
/// else raises the package's `VortexRdfError`.
pub(crate) fn py_err(e: CoreError) -> PyErr {
    match e {
        CoreError::InvalidInput(_) => PyValueError::new_err(e.to_string()),
        CoreError::Io(io) => io.into(),
        other => VortexRdfError::new_err(other.to_string()),
    }
}

/// Builds the future `task` returns and drives it to completion on
/// [`RUNTIME`], both with the GIL released, raising its error as [`py_err`]
/// maps it. The future is pinned where it is built, so the runtime's
/// `block_on` layers pass a pointer to it rather than moving it.
pub(crate) fn detached<T, F, Fut>(py: Python<'_>, task: F) -> PyResult<T>
where
    T: Send,
    F: FnOnce() -> Fut + Send,
    Fut: Future<Output = Result<T, CoreError>>,
{
    py.detach(|| RUNTIME.block_on(pin!(task()))).map_err(py_err)
}

/// The [`Cancelled`] a call or a pull raises once its token is cancelled.
pub(crate) fn cancelled() -> PyErr {
    Cancelled::new_err("cancelled through its CancelToken")
}

/// A pull's outcome as the Python call raises it: [`Cancelled`] for a
/// cancelled pull, a failed one as [`py_err`] maps it.
pub(crate) fn settled<T>(pulled: Pulled<Result<T, CoreError>>) -> PyResult<T> {
    match pulled {
        Pulled::Done(result) => result.map_err(py_err),
        Pulled::Cancelled => Err(cancelled()),
    }
}

/// [`detached`] for a call given a cancel token: `fut` runs as a task on
/// [`RUNTIME`] raced with `token` (see [`pull`]) while the calling thread
/// waits with the GIL released.
pub(crate) fn raced<T, Fut>(py: Python<'_>, token: &Token, fut: Fut) -> PyResult<T>
where
    T: Send + 'static,
    Fut: Future<Output = Result<T, CoreError>> + Send + 'static,
{
    settled(py.detach(|| pull(&RUNTIME, Some(token), fut)))
}

/// A cancel token for one query's long calls: passed as `cancel=` to the
/// calls that take it, it stops them from any thread. A call or pull still
/// in progress when the token is cancelled stops at its next await (on
/// file, the next split boundary) and raises [`Cancelled`]; one that
/// completes without awaiting again (an in-memory gather) returns its
/// result, and the next call or pull given the token raises [`Cancelled`].
/// Cancelling is final.
#[pyclass(frozen, module = "vortex_rdf._native")]
pub struct CancelToken {
    pub(crate) token: Token,
}

#[pymethods]
impl CancelToken {
    #[new]
    fn new() -> Self {
        CancelToken {
            token: Token::new(),
        }
    }

    /// Cancels the token, from any thread; cancelling it again does
    /// nothing.
    fn cancel(&self) {
        self.token.cancel();
    }

    /// Whether the token has been cancelled.
    #[getter]
    fn cancelled(&self) -> bool {
        self.token.is_cancelled()
    }
}

/// The encoded-search work counters (`VORTEX_RDF_COUNTERS=1`; all zero otherwise), by name.
#[pyfunction]
fn engine_counters() -> std::collections::HashMap<&'static str, u64> {
    use vortex_rdf_encoded_search::counters::{NAMES, snapshot};
    NAMES.into_iter().zip(snapshot()).collect()
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<store::VortexRdfStore>()?;
    m.add_class::<store::ProbeAnswers>()?;
    m.add_class::<chunks::CodeChunks>()?;
    m.add_class::<CancelToken>()?;
    m.add_class::<codes::TermDict>()?;
    m.add_class::<codes::U64Column>()?;
    m.add_class::<codes::CodeGroups>()?;
    m.add_function(wrap_pyfunction!(serialize::serialize_rdf, m)?)?;
    m.add_function(wrap_pyfunction!(engine_counters, m)?)?;
    m.add("VortexRdfError", m.py().get_type::<VortexRdfError>())?;
    m.add("JoinTooLarge", m.py().get_type::<JoinTooLarge>())?;
    m.add("Cancelled", m.py().get_type::<Cancelled>())?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
