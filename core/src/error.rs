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
#[cfg(feature = "file-io")]
pub(crate) fn kind_means_unwritable(kind: std::io::ErrorKind) -> bool {
    matches!(
        kind,
        std::io::ErrorKind::PermissionDenied | std::io::ErrorKind::ReadOnlyFilesystem
    )
}

/// `what` went wrong with the file at `path`: the I/O error, with the path in
/// the message and its kind kept.
#[cfg(feature = "file-io")]
pub(crate) fn path_error(what: &str, path: &std::path::Path, e: std::io::Error) -> VortexRdfError {
    VortexRdfError::Io(std::io::Error::new(
        e.kind(),
        format!("{what} {path:?}: {e}"),
    ))
}

/// The source of the `Io` error a store writer returns when it finds, before
/// anything is built, that the store file (or the directory it lives in)
/// cannot be written by this process. The error keeps its kind
/// ([`kind_means_unwritable`]) and its message; this marker is what
/// [`VortexRdfError::is_unwritable`] looks for.
///
/// A permission error from further into a rewrite (a spill directory, a
/// rename over a file someone else owns) is not this refusal: it comes after
/// the work was done, and is reported as the failure it is.
#[cfg(feature = "file-io")]
#[derive(Error, Debug)]
#[error("{0}")]
pub(crate) struct StoreNotWritable(String);

#[cfg(feature = "file-io")]
impl StoreNotWritable {
    /// The I/O error for a refusal of kind `kind` (see
    /// [`kind_means_unwritable`]) described by `message`.
    pub(crate) fn error(kind: std::io::ErrorKind, message: String) -> std::io::Error {
        debug_assert!(kind_means_unwritable(kind), "{kind:?}");
        std::io::Error::new(kind, Self(message))
    }
}

impl VortexRdfError {
    /// Whether this is a store writer's refusal to write a file this process
    /// cannot write ([`StoreNotWritable`]) — made before anything is built —
    /// as opposed to any other I/O failure. It is what lets an append do
    /// without the compaction of a read-only store. Never true without
    /// `file-io`, where no store is written to a path.
    pub(crate) fn is_unwritable(&self) -> bool {
        #[cfg(feature = "file-io")]
        if let Self::Io(error) = self {
            return error
                .get_ref()
                .is_some_and(|source| source.is::<StoreNotWritable>());
        }
        false
    }
}

/// `std::result::Result` with [`VortexRdfError`] as the error type.
pub type Result<T> = std::result::Result<T, VortexRdfError>;

#[cfg(all(test, feature = "file-io"))]
mod tests {
    use super::*;
    use std::io::{Error, ErrorKind};

    /// A permission refusal and a read-only filesystem are "cannot be
    /// written"; nothing else is.
    #[test]
    fn unwritable_kinds_are_permission_denied_and_a_read_only_filesystem() {
        assert!(kind_means_unwritable(ErrorKind::PermissionDenied));
        assert!(kind_means_unwritable(ErrorKind::ReadOnlyFilesystem));
        for kind in [
            ErrorKind::NotFound,
            ErrorKind::IsADirectory,
            ErrorKind::AlreadyExists,
            ErrorKind::Other,
        ] {
            assert!(!kind_means_unwritable(kind), "{kind:?}");
        }
    }

    /// The errors the operating system gives for those two cases map to
    /// them: EACCES and EROFS (30 on Linux, macOS and the BSDs).
    #[cfg(unix)]
    #[test]
    fn the_os_errors_for_a_denied_write_are_unwritable_kinds() {
        const EACCES: i32 = 13;
        const EROFS: i32 = 30;
        for errno in [EACCES, EROFS] {
            let error = Error::from_raw_os_error(errno);
            assert!(
                kind_means_unwritable(error.kind()),
                "errno {errno}: {error}"
            );
        }
        assert!(!kind_means_unwritable(Error::from_raw_os_error(2).kind()));
    }

    /// The writer's refusal is recognised, whichever of the two kinds it
    /// carries, and keeps its kind and message. A bare I/O error of the same
    /// kind is not the refusal (it can come from anywhere in a rewrite), and
    /// neither is an error of another origin.
    #[test]
    fn only_the_writers_refusal_is_unwritable() {
        for kind in [ErrorKind::PermissionDenied, ErrorKind::ReadOnlyFilesystem] {
            let refusal = VortexRdfError::Io(StoreNotWritable::error(kind, "no write".into()));
            assert!(refusal.is_unwritable(), "{kind:?}");
            assert_eq!(refusal.to_string(), "IO error: no write");
            assert!(matches!(&refusal, VortexRdfError::Io(e) if e.kind() == kind));

            assert!(!VortexRdfError::Io(Error::from(kind)).is_unwritable());
            assert!(!VortexRdfError::Io(Error::new(kind, "no write")).is_unwritable());
        }
        assert!(!VortexRdfError::Io(Error::from(ErrorKind::NotFound)).is_unwritable());
        assert!(!VortexRdfError::Deserialization("permission denied".into()).is_unwritable());
        assert!(!VortexRdfError::InvalidOperation("read-only".into()).is_unwritable());
    }
}
