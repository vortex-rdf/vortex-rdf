//! Python bindings for Vortex-RDF, exposed as the private extension module
//! `vortex_rdf._native`, re-exported by the pure-Python `vortex_rdf`
//! package. The rdflib `Store` integration lives in the separate
//! `vortex-rdflib` package/repository, built on top of these bindings.
//!
//! Unlike the wasm bindings, this crate keeps core's `file-io` feature on:
//! stores are opened lazily from `.vortex` files and queried in place.

mod batch;
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

pyo3::create_exception!(
    _native,
    VortexRdfError,
    pyo3::exceptions::PyException,
    "Raised when a Vortex-RDF store operation fails: a store that does not read, an operation \
     the store refuses. An invalid argument raises ValueError instead, and an I/O failure \
     OSError."
);

pyo3::create_exception!(
    _native,
    JoinTooLarge,
    VortexRdfError,
    "Raised when a join would make more row pairs than its `max_pairs` allows, before any pair \
     is built. `args` is `(message, pairs)`: `args[1]` is the join's pair count."
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

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<store::VortexRdfStore>()?;
    m.add_class::<store::ProbeAnswers>()?;
    m.add_class::<codes::TermDict>()?;
    m.add_class::<codes::U64Column>()?;
    m.add_class::<codes::CodeGroups>()?;
    m.add_function(wrap_pyfunction!(serialize::serialize_rdf, m)?)?;
    m.add("VortexRdfError", m.py().get_type::<VortexRdfError>())?;
    m.add("JoinTooLarge", m.py().get_type::<JoinTooLarge>())?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
