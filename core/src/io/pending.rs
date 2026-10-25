//! A store file's atomic replacement at a path: [`PendingStore`], the one way
//! a store reaches a path on disk. The store is written beside the file it
//! replaces and renamed over it once complete, keeping what that file was set
//! up as (its links, its permissions); a store this process cannot write is
//! never replaced.

use crate::error::{Result, VortexRdfError, path_error};
use crate::io::file::FileIdentity;
use crate::io::write::refuse_experimental_patches;

/// A store file about to be written all-or-nothing: the path has been checked,
/// the file that takes the bytes is open, and nothing has been built yet.
/// [`create`](Self::create) refuses a path that cannot take a store before
/// anything is built: a directory at `path`, and, since the temp file is
/// created at once, a missing directory or a refused write permission.
/// [`write`](Self::write) fills the file and renames it into place.
///
/// The temp file is a sibling, so the rename stays on one filesystem and is
/// atomic. The uuid in its name avoids a temp left behind by an earlier
/// interrupted write, and the `.tmp` extension keeps it from being taken for
/// a store.
///
/// What the old file was set up as survives the replacement. A symbolic link
/// at `path` is followed (a chain of them, and a link to a file that is not
/// there yet, included): the store is written beside the file the links end
/// at and renamed over it, so a `current -> versions/v3.vortex` setup keeps
/// its link. The temp file for an existing store is created private (`0o600`
/// on Unix) and the old file's permission bits are set on it before a byte is
/// written, so a private store is never copied into a more readable temp file,
/// not even for a moment. A filesystem that refuses that `chmod` fails the
/// write, with the temp file removed and the old store as it was. Owner, ACLs
/// and extended attributes are not preserved: the new file belongs to the
/// writing process. The replacement gives the path a new inode, so another
/// hard link to the old file keeps the old store.
///
/// A path that resolves to a character device or a pipe (`/dev/null`, or
/// `/dev/stdout` with a pipe behind it) takes the bytes in place, with no temp
/// file and no rename; opening a pipe waits for its reader. Any other file
/// that is not a regular file is refused.
///
/// A store written over the file it was opened from
/// ([`create_over`](Self::create_over)) replaces only that file: the path must
/// still name the file with the given identity when the temp file is created
/// and again just before the rename, or the write is refused with
/// [`InvalidOperation`](VortexRdfError::InvalidOperation) and nothing is
/// replaced. A file that is gone is not another file: the store is written
/// there.
///
/// A store the process cannot write is never replaced: before anything is
/// built, the old file is opened for writing as a probe (not truncated, not
/// created, nothing changes), and a `PermissionDenied` answer (or a read-only
/// filesystem's) fails the write with an error naming the path and keeping
/// the kind. A directory that takes no new file is refused the same way, when
/// the temp file cannot be created. These two refusals, and only these, are
/// marked ([`StoreNotWritable`](crate::error::StoreNotWritable)), so a caller
/// that can do without the write (an append whose auto-compaction is refused)
/// tells them from a permission error that comes later, such as a refused
/// rename.
///
/// The old file is never modified: overwriting it in place would pull its
/// pages from under a reader that maps it (SIGBUS, or another store's bytes
/// read as the old ones), and a process that died mid-write would leave a
/// half-written file at `path`. A reader that mapped the old file keeps its
/// pages until it drops (Windows refuses the rename over a mapped file, and
/// the write then fails with the I/O error until that store is closed), and
/// `path` is untouched on any failure before the rename.
///
/// The swap is safe against a process crash, not durable against power loss:
/// nothing is `fsync`ed before the rename, so on a filesystem that does not
/// order a file's data before the rename, a power failure right after it can
/// leave a short or empty file at `path`.
///
/// The temp file is removed on every way out that does not rename it: an
/// error from `write`, a failed rename, a panic, or this value being dropped
/// (including by the future that holds it being dropped, at any step of
/// `create`).
pub(crate) struct PendingStore {
    /// The path the caller named, for error messages.
    path: std::path::PathBuf,
    sink: Sink,
    file: tokio::fs::File,
}

/// Where a [`PendingStore`]'s bytes end up.
enum Sink {
    /// A temp file beside `target`, renamed over it once complete.
    Replace {
        /// The file the store replaces: `path`, or where the links at `path`
        /// end.
        target: std::path::PathBuf,
        tmp: TempFile,
        /// The identity `target` must still have when it is replaced.
        opened: Option<FileIdentity>,
    },
    /// A character device or a pipe, written in place.
    InPlace,
}

impl PendingStore {
    /// Check that `path` can take a store and open the file for it: resolve the
    /// links at `path`; take a character device or a pipe in place; refuse a
    /// directory, any other file that is not a regular file and a store this
    /// process cannot write; otherwise create the temp file beside the file
    /// the store will replace (private, for an existing store: see
    /// [`create_temp`]) and copy that file's permissions onto it.
    pub(crate) async fn create(path: &std::path::Path) -> Result<Self> {
        Self::create_over(path, None).await
    }

    /// [`create`](Self::create) for a store that replaces the file `opened`
    /// identifies, if there is one.
    pub(crate) async fn create_over(
        path: &std::path::Path,
        opened: Option<FileIdentity>,
    ) -> Result<Self> {
        refuse_experimental_patches()?;
        let io_error = |what: &str, e: std::io::Error| path_error(what, path, e);
        // The refusal to write this path: an error like `io_error`'s, marked as
        // the writer's own (see `StoreNotWritable`).
        let refusal = |what: &str, e: std::io::Error| {
            VortexRdfError::Io(crate::error::StoreNotWritable::error(
                e.kind(),
                format!("{what} {path:?}: {e}"),
            ))
        };

        // What the kernel resolves the path to, links included (`/dev/stdout`
        // is a link to the descriptor): a character device or a pipe takes the
        // store in place.
        if let Ok(meta) = tokio::fs::metadata(path).await
            && is_stream(&meta)
        {
            if opened.is_some() {
                return Err(replaced_since_open(path));
            }
            let file = tokio::fs::OpenOptions::new()
                .write(true)
                .open(path)
                .await
                .map_err(|e| io_error("open", e))?;
            return Ok(Self {
                path: path.to_path_buf(),
                sink: Sink::InPlace,
                file,
            });
        }

        // The file this store replaces: `path`, or where the links at `path` end.
        let target = replacement_target(path)
            .await
            .map_err(|e| io_error("resolve", e))?;
        // A directory can never be replaced by a store: refuse now, not at the
        // rename after the build. Otherwise keep the old file's permissions
        // (none for a fresh path).
        let permissions = match tokio::fs::metadata(&target).await {
            Ok(meta) if meta.is_dir() => {
                return Err(io_error(
                    "replace",
                    std::io::Error::new(std::io::ErrorKind::IsADirectory, "is a directory"),
                ));
            }
            Ok(meta) if !meta.is_file() => {
                return Err(io_error(
                    "replace",
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "it is not a regular file, a character device or a pipe",
                    ),
                ));
            }
            Ok(meta) => {
                if opened.is_some_and(|opened| FileIdentity::of(&meta) != Some(opened)) {
                    return Err(replaced_since_open(path));
                }
                // A read-only store signals that it should not be overwritten,
                // but the rename needs only the directory, so the file itself
                // is asked: opened for writing, neither truncated nor created,
                // so nothing changes (root still bypasses).
                match tokio::fs::OpenOptions::new()
                    .write(true)
                    .open(&target)
                    .await
                {
                    Ok(_probe) => {}
                    // Gone since the stat: a fresh path after all.
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    // Permission denied, or a read-only filesystem: either
                    // way the store is not to be written.
                    Err(e) if crate::error::kind_means_unwritable(e.kind()) => {
                        let via = if target == path {
                            String::new()
                        } else {
                            format!(" ({target:?})")
                        };
                        return Err(refusal(
                            "replace",
                            std::io::Error::new(
                                e.kind(),
                                format!(
                                    "the existing store{via} is not writable by this process, \
                                     and a store it cannot write is never replaced"
                                ),
                            ),
                        ));
                    }
                    Err(e) => return Err(io_error("inspect", e)),
                }
                Some(meta.permissions())
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
            Err(e) => return Err(io_error("inspect", e)),
        };

        let tmp_path = target.with_extension(format!("write-{}.tmp", uuid::Uuid::new_v4()));
        let file = create_temp(&tmp_path, permissions.is_some()).map_err(|e| {
            // A directory that takes no new file is as unwritable as a
            // read-only store; a missing one is just an error.
            if crate::error::kind_means_unwritable(e.kind()) {
                refusal("create a temporary file beside", e)
            } else {
                io_error("create a temporary file beside", e)
            }
        })?;
        // No await between the creation and the guard: dropping this future
        // from here on removes the file.
        let tmp = TempFile(Some(tmp_path));
        let file = tokio::fs::File::from_std(file);
        if let Some(permissions) = permissions {
            // Before the first byte, not before the rename: the temp file was
            // born private and only becomes as readable as the old file was.
            tokio::fs::set_permissions(tmp.path(), permissions)
                .await
                .map_err(|e| io_error("copy the permissions onto the temporary file of", e))?;
        }

        Ok(Self {
            path: path.to_path_buf(),
            sink: Sink::Replace {
                target,
                tmp,
                opened,
            },
            file,
        })
    }

    /// Where a build that fills this store spills: the name of the file it
    /// replaces and that file's directory, where the temp file is (links
    /// followed: not beside the link a store was opened through). The store is
    /// renamed within that directory, the one volume known to take a file the
    /// store's size. A store written in place has none.
    pub(crate) fn spill_site(&self) -> Option<(&std::ffi::OsStr, &std::path::Path)> {
        match &self.sink {
            Sink::Replace { target, .. } => target.file_name().zip(target.parent()),
            Sink::InPlace => None,
        }
    }

    /// Fill the file with `write` and, for a temp file, rename it over the
    /// file it replaces. If `write` fails, or the rename does, the temp file is
    /// removed and the old file is as it was.
    pub(crate) async fn write<F, Fut>(self, write: F) -> Result<()>
    where
        F: FnOnce(tokio::fs::File) -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        let Self { path, sink, file } = self;
        match sink {
            Sink::InPlace => write(file).await,
            Sink::Replace {
                target,
                tmp,
                opened,
            } => {
                write(file).await?;
                ensure_unreplaced(&path, &target, opened).await?;
                tokio::fs::rename(tmp.path(), &target)
                    .await
                    .map_err(|e| path_error("replace", &path, e))?;
                // Renamed into place: there is no temp file left to remove.
                tmp.persist();
                Ok(())
            }
        }
    }
}

/// Whether `meta` describes a character device or a pipe: a file a store is
/// written into, not renamed over.
fn is_stream(meta: &std::fs::Metadata) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileTypeExt as _;
        let kind = meta.file_type();
        kind.is_char_device() || kind.is_fifo()
    }
    #[cfg(not(unix))]
    {
        let _ = meta;
        false
    }
}

/// The refusal to replace a file that is not the one the store opened.
fn replaced_since_open(path: &std::path::Path) -> VortexRdfError {
    VortexRdfError::InvalidOperation(format!(
        "the file at {path:?} was replaced since this store opened it; reopen it"
    ))
}

/// Refuse unless the file at `target` is still the one `opened` identifies.
/// A file that is gone is not another file.
async fn ensure_unreplaced(
    path: &std::path::Path,
    target: &std::path::Path,
    opened: Option<FileIdentity>,
) -> Result<()> {
    let Some(opened) = opened else {
        return Ok(());
    };
    match tokio::fs::metadata(target).await {
        Ok(meta) if FileIdentity::of(&meta) == Some(opened) => Ok(()),
        Ok(_) => Err(replaced_since_open(path)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(path_error("inspect", path, e)),
    }
}

/// Create the temp file at `path`, open for writing: `create_new`, so a file
/// already at the temp name is never opened, let alone truncated and then
/// deleted by the [`TempFile`] guard, and synchronous, so a caller guards the
/// file in the same poll that makes it.
///
/// A temp file that stands in for an existing store (`replacing`) is born
/// readable and writable by its owner alone on Unix, whatever the umask, and
/// the old file's permissions are set on it right after: a private store is
/// never in a file others can read, not even between the creation and that
/// `chmod`. A temp file for a fresh path gets the permissions any new file
/// gets.
pub(crate) fn create_temp(
    path: &std::path::Path,
    replacing: bool,
) -> std::io::Result<std::fs::File> {
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    if replacing {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    #[cfg(not(unix))]
    let _ = replacing;
    options.open(path)
}

/// How many links [`replacement_target`] follows before it gives up, as many
/// as Linux does before `ELOOP`.
const MAX_LINK_HOPS: usize = 40;

/// The file a store written "to `path`" replaces: `path` itself, unless it is
/// a symbolic link, in which case the file the chain of links ends at. A link
/// to a file that is not there yet ends at that file, which is then created,
/// as creating through a link does.
async fn replacement_target(path: &std::path::Path) -> std::io::Result<std::path::PathBuf> {
    let mut target = path.to_path_buf();
    for _ in 0..MAX_LINK_HOPS {
        match tokio::fs::symlink_metadata(&target).await {
            Ok(meta) if meta.file_type().is_symlink() => {
                let link = tokio::fs::read_link(&target).await?;
                // A relative link is relative to the directory that holds it.
                target = match target.parent() {
                    Some(dir) => dir.join(link),
                    None => link,
                };
            }
            // A file, a directory, or nothing there: a fresh path, or the end
            // of a link to nowhere. Anything the stat cannot say surfaces from
            // the calls that follow.
            _ => return Ok(target),
        }
    }
    Err(std::io::Error::other("too many levels of symbolic links"))
}

/// A temp file that deletes itself when dropped, unless it was
/// [persisted](Self::persist): every error path of a [`PendingStore`], and a
/// dropped future, cleans up without a line of its own.
struct TempFile(Option<std::path::PathBuf>);

impl TempFile {
    fn path(&self) -> &std::path::Path {
        self.0.as_deref().expect("a live temp file has a path")
    }

    /// Keep the file: it has been renamed away, so there is nothing to remove.
    fn persist(mut self) {
        self.0 = None;
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        if let Some(path) = &self.0 {
            let _ = std::fs::remove_file(path);
        }
    }
}
