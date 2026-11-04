
#[cfg(feature = "arrow")]
use arrow_schema::ArrowError;
use thiserror::Error;

use crate::VortexRdfError;

#[derive(Error, Debug)]
pub enum BuilderError {
    #[cfg(feature = "arrow")]
    #[error("error while building from arrow")]
    ArrowBuilder(#[from] ArrowError),
}

#[cfg(feature = "arrow")]
impl From<ArrowError> for VortexRdfError {
    fn from(value: ArrowError) -> Self {
        VortexRdfError::Building(BuilderError::ArrowBuilder(value))
    }
}
