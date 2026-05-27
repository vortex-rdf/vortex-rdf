use thiserror::Error;
use vortex_error::VortexError;

#[derive(Error, Debug)]
pub enum VortexRdfError {
    #[error("Vortex error: {0}")]
    Vortex(#[from] VortexError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Deserialization error: {0}")]
    Deserialization(String),

    #[error("Invalid operation: {0}")]
    InvalidOperation(String),
}

pub type Result<T> = std::result::Result<T, VortexRdfError>;
