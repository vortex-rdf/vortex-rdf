//! The crate's error type and `Result` alias.

use thiserror::Error;
use vortex_error::VortexError;

use crate::store::builders::error::BuilderError;

/// Every failure the crate reports, by origin.
#[derive(Error, Debug)]
pub enum VortexRdfError {
    /// A failure inside the Vortex array, layout, or file machinery.
    #[error("Vortex error: {0}")]
    Vortex(#[from] VortexError),

    /// A filesystem or writer I/O failure (creating, renaming, or flushing a
    /// store file, spilling sort runs).
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// The store's parts cannot be encoded: a serialization precondition
    /// (a Dictionary-layout array without its dictionary, a quad outside the
    /// default graph in a graph format) or a builder/spill encoding failure.
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// Serialized bytes cannot be interpreted as a store: a foreign root
    /// layout, a missing or unknown required component, a code outside its
    /// dictionary, malformed metadata.
    #[error("Deserialization error: {0}")]
    Deserialization(String),

    /// The operation is not valid on this store as it stands — a mutation on
    /// a view derived from `match_pattern`, which does not own its rows.
    #[error("Invalid operation: {0}")]
    InvalidOperation(String),

    #[error("Store building error")]
    Building(#[from] BuilderError),
}

/// `std::result::Result` with [`VortexRdfError`] as the error type.
pub type Result<T> = std::result::Result<T, VortexRdfError>;
