//! The crate's error type and `Result` alias.

use thiserror::Error;
use vortex_error::VortexError;

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
}

/// Whether an I/O error of this kind says a file cannot be written by this
/// process: the permission is denied, or the filesystem is read-only. Both
/// mean "do not write this store", and neither goes away by trying again.
pub(crate) fn kind_means_unwritable(kind: std::io::ErrorKind) -> bool {
    matches!(
        kind,
        std::io::ErrorKind::PermissionDenied | std::io::ErrorKind::ReadOnlyFilesystem
    )
}

impl VortexRdfError {
    /// Whether this is an I/O error saying a file cannot be written by this
    /// process (see [`kind_means_unwritable`]) — what refuses a store's
    /// rewrite when its source file is read-only.
    pub(crate) fn is_unwritable(&self) -> bool {
        matches!(self, Self::Io(error) if kind_means_unwritable(error.kind()))
    }
}

/// `std::result::Result` with [`VortexRdfError`] as the error type.
pub type Result<T> = std::result::Result<T, VortexRdfError>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Error, ErrorKind};

    /// A permission refusal or a read-only filesystem is "cannot be written";
    /// a missing file, a directory in the way, a failed parse or any error
    /// that is not I/O is not.
    #[test]
    fn unwritable_is_permission_denied_or_a_read_only_filesystem() {
        let io = |kind| VortexRdfError::Io(Error::from(kind));
        assert!(io(ErrorKind::PermissionDenied).is_unwritable());
        assert!(io(ErrorKind::ReadOnlyFilesystem).is_unwritable());
        for kind in [
            ErrorKind::NotFound,
            ErrorKind::IsADirectory,
            ErrorKind::AlreadyExists,
            ErrorKind::Other,
        ] {
            assert!(!io(kind).is_unwritable(), "{kind:?}");
        }
        assert!(!VortexRdfError::Deserialization("permission denied".into()).is_unwritable());
        assert!(!VortexRdfError::InvalidOperation("read-only".into()).is_unwritable());
    }

    /// The errors the operating system gives for those two cases map to
    /// them: EACCES and EROFS (30 on Linux, macOS and the BSDs).
    #[cfg(unix)]
    #[test]
    fn the_os_errors_for_a_denied_write_are_unwritable() {
        const EACCES: i32 = 13;
        const EROFS: i32 = 30;
        for errno in [EACCES, EROFS] {
            let error = VortexRdfError::Io(Error::from_raw_os_error(errno));
            assert!(error.is_unwritable(), "errno {errno}: {error}");
        }
        assert!(!VortexRdfError::Io(Error::from_raw_os_error(2)).is_unwritable());
    }
}
