//! The write side of the native store container.
//!
//! Packs a store's parts — the primary quad table plus each index
//! component's and the dictionary's own
//! [`NativeComponentWrite`](crate::io::container::NativeComponentWrite) —
//! into a [`BuiltStream`] and drives
//! [`write_store`](crate::io::container::write_store) over it, carrying
//! each part's sortedness provenance onto the descriptors a reader will
//! trust. Also owns the `quads_stream_to_*` entry points, which run a
//! builder's chunk stream straight into that writer, and the one way a store
//! reaches a path on disk: written beside it and renamed into place
//! ([`write_store_atomically`](crate::io::ser::write_store_atomically)).
//!
//! Reading these bytes back is [`read`](crate::io::read)'s job,
//! and the container's own on-disk grammar is
//! [`container`](crate::io::container)'s.

use crate::error::{Result, VortexRdfError};

use crate::debug;
use crate::io::container::{self, child_strategy};
use crate::store::LayoutStrategy;
use crate::store::StoreParts;
use crate::store::builders::BuiltStream;
use futures::StreamExt as _;
use vortex_array::stream::ArrayStreamAdapter;
use vortex_io::VortexWrite;

#[cfg(feature = "file-io")]
use crate::error::path_error;
#[cfg(feature = "file-io")]
use crate::io::read::FileIdentity;
#[cfg(feature = "file-io")]
use crate::store::builders::{SortedStreamBuilder, VortexArrayBuilder};
#[cfg(feature = "file-io")]
use crate::store::{Indexes, RawQuad};
#[cfg(feature = "file-io")]
use futures::Stream;

/// Serialize a store's split parts — the primary quad array, its in-memory
/// index components, and (for the Dictionary layout) the term dictionary —
/// as a native store file. Sortedness provenance is carried faithfully: the
/// root's `quads_sorted` (see `WireMetadata::quads_sorted`) is
/// `parts.quads_sorted`, and each index child records its component's
/// `sorted` flag.
///
/// Precondition: a Dictionary-layout primary comes with its dictionary
/// (`to_serializable_parts` always pairs them).
pub(crate) async fn serialize_parts<W: VortexWrite + Unpin + Send>(
    parts: &StoreParts,
    writer: W,
) -> Result<()> {
    refuse_experimental_patches()?;
    let start = debug::timer();

    let primary = parts.array.clone();
    debug_assert!(
        !matches!(
            LayoutStrategy::from_dtype(primary.dtype()),
            LayoutStrategy::Dictionary
        ) || parts.dict.is_some(),
        "to_serializable_parts always pairs a Dictionary primary with its dictionary"
    );

    let mut components = Vec::with_capacity(parts.components.len());
    for component in &parts.components {
        components.push(component.to_write()?);
    }

    let dtype = primary.dtype().clone();
    let built = BuiltStream {
        dtype,
        chunks: futures::stream::once(async move { Ok(primary) }).boxed(),
        components,
        quads_sorted: parts.quads_sorted,
        dict: parts.dict.clone(),
    };
    built_stream_to_vortex_writer(built, writer).await?;

    log::debug!(
        "[ser::serialize_parts] Vortex writing took {:?}",
        debug::elapsed(start)
    );
    Ok(())
}

/// Stream quads directly into a native store file as compressed chunks.
///
/// The build pipeline is the target's, not the caller's: writing a file means
/// a filesystem exists, so the rows go through the out-of-core global sort
/// ([`SortedStreamBuilder`]) — the one pipeline whose peak memory does not
/// scale with the dataset. (The in-memory sort is what targets without a
/// filesystem use; see [`SortedInMemoryBuilder`].)
///
/// [`SortedInMemoryBuilder`]: crate::SortedInMemoryBuilder
///
/// Without index children peak memory is bounded by the chunk size; with
/// them it also includes the in-flight components' compressed size (see
/// `RdfStoreWriteStrategy::write_stream` for why). The dictionary is complete
/// before any chunk flows and becomes the required `dictionary` child.
///
/// The quads must be canonical: build them with [`RawQuad::from_quad`], the
/// parser ([`parse_quads_from_reader`]) or [`RawQuad::canonical`]. The
/// builder interns and compares the spelling it is given, so a hand-built
/// `RawQuad` in another spelling of the same terms is a different quad.
///
/// [`parse_quads_from_reader`]: crate::common::terms::parse_quads_from_reader
#[cfg(feature = "file-io")]
pub async fn quads_stream_to_vortex_writer<S, W>(
    quads: S,
    writer: W,
    layout: LayoutStrategy,
    indexes: Indexes,
) -> Result<()>
where
    S: Stream<Item = Result<RawQuad>> + Unpin + Send + 'static,
    W: VortexWrite + Unpin + Send,
{
    refuse_experimental_patches()?;
    let start = debug::timer();

    let built = SortedStreamBuilder::build_vortex_stream(Box::new(quads), layout, indexes).await?;
    built_stream_to_vortex_writer(built, writer).await?;

    log::debug!(
        "[ser::quads_stream_to_vortex_writer] Streaming write took {:?}",
        debug::elapsed(start)
    );
    Ok(())
}

/// Drive an already-built chunk stream into `writer`: the primary chunks as
/// the transparent root child, each component and the dictionary as
/// auxiliary children. The one writer tail — `serialize_parts` wraps a
/// store's single primary array in it, `quads_stream_to_vortex_writer` (the
/// streaming entry point) feeds it a builder's stream, and compaction a
/// stream it built with its own spill-directory placement. The memory bound
/// is `RdfStoreWriteStrategy::write_stream`'s.
pub(crate) async fn built_stream_to_vortex_writer<W>(
    built: BuiltStream,
    mut writer: W,
) -> Result<()>
where
    W: VortexWrite + Unpin + Send,
{
    let mut components = built.components;
    if let Some(dict) = &built.dict {
        components.push(dict.to_write()?);
    }

    let quad_strategy = child_strategy(&built.dtype);
    container::write_store(
        &crate::session::VORTEX_SESSION,
        &mut writer,
        ArrayStreamAdapter::new(built.dtype, built.chunks),
        quad_strategy,
        built.quads_sorted,
        components,
    )
    .await
    .map_err(VortexRdfError::Vortex)?;

    // A shutdown failure is writer I/O, not an encoding problem — surface it
    // through the `Io` variant.
    writer.shutdown().await.map_err(VortexRdfError::Io)
}

/// Serialize a quad stream to a native store file at `path` — the path-based
/// convenience over [`quads_stream_to_vortex_writer`], with the store written
/// beside `path` and renamed into place.
///
/// The file is all-or-nothing. An input that fails partway — a parse error
/// halfway through the stream — leaves no file at a fresh `path` and the
/// previous store untouched at an existing one, and a store that has `path`
/// memory-mapped keeps reading the file it mapped (the old file is replaced,
/// never rewritten in place; on Windows the rename is refused while a store
/// has the file mapped, so the write fails until that store is closed).
///
/// A path that cannot take a store — a missing directory, no permission to
/// write there, a directory at `path` — is reported before any input is
/// read, not after the whole ingest, sort and dictionary have run. A device
/// or a pipe at `path` (`/dev/null`, `/dev/stdout` with a pipe behind it)
/// takes the store in place, with no temp file and so no all-or-nothing
/// guarantee.
///
/// The quads must be canonical, as for [`quads_stream_to_vortex_writer`]:
/// from [`RawQuad::from_quad`], the parser ([`parse_quads_from_reader`]) or
/// [`RawQuad::canonical`].
///
/// [`parse_quads_from_reader`]: crate::common::terms::parse_quads_from_reader
#[cfg(feature = "file-io")]
pub async fn quads_stream_to_vortex_file<S>(
    quads: S,
    path: &std::path::Path,
    layout: LayoutStrategy,
    indexes: Indexes,
) -> Result<()>
where
    S: Stream<Item = Result<RawQuad>> + Unpin + Send + 'static,
{
    let start = debug::timer();

    // The temp file comes first and the build runs inside the write: the
    // path is checked (and the temp created) before any input is read, and an
    // input that fails leaves nothing behind, the empty temp file going with
    // the rest.
    write_store_atomically(path, |writer| async move {
        let built =
            SortedStreamBuilder::build_vortex_stream(Box::new(quads), layout, indexes).await?;
        built_stream_to_vortex_writer(built, writer).await
    })
    .await?;

    log::debug!(
        "[ser::quads_stream_to_vortex_file] Streaming write took {:?}",
        debug::elapsed(start)
    );
    Ok(())
}

/// Write a store file all-or-nothing: `write` fills a temp file created
/// beside `path`, which is renamed over `path` only once `write` succeeded.
/// Every store a path-taking writer produces goes through a [`PendingStore`]:
/// this is the form for a caller that has its bytes to write straight away
/// (`quads_stream_to_vortex_file` builds inside `write`), which is
/// [`PendingStore::create`] followed by [`PendingStore::write`]. A caller that
/// has work to do before it has bytes (compaction gathers, sorts and builds
/// first) makes the two calls itself, with the work in between, so that a
/// path that cannot take a store is refused before that work starts.
#[cfg(feature = "file-io")]
pub(crate) async fn write_store_atomically<F, Fut>(path: &std::path::Path, write: F) -> Result<()>
where
    F: FnOnce(tokio::fs::File) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    PendingStore::create(path).await?.write(write).await
}

/// A store file about to be written all-or-nothing: the path has been checked
/// and the file that takes the bytes is open, and nothing has been built yet.
/// [`create`](Self::create) is where a path that cannot take a store is
/// refused; [`write`](Self::write) fills the file and renames it into place.
///
/// A path that cannot take a store fails in `create`, before anything is
/// built: a directory at `path` is refused, and the temp file is created
/// first, so a missing directory or a refused write permission is reported at
/// once.
///
/// The temp file is a sibling so the rename stays on one filesystem, which
/// is what makes it atomic. The uuid in its name avoids colliding with a
/// temp left behind by an earlier interrupted write, and the `.tmp`
/// extension keeps it from being mistaken for a store.
///
/// What the old file was set up as survives the replacement. A symbolic link
/// at `path` is followed (a chain of them, and a link to a file that is not
/// there yet, included): the store is written beside the file the links end
/// at and renamed over it, so a `current -> versions/v3.vortex` setup keeps
/// its link. The temp file for an existing store is created private (`0o600`
/// on Unix) and the old file's permission bits are set on it before a byte is
/// written, so a private store is never copied into a more readable temp file,
/// not even for a moment. A filesystem that refuses that `chmod` fails the
/// write (there is no store to promise the old permissions to), with the temp
/// file removed and the old store as it was. Owner, ACLs and extended
/// attributes are not preserved: the new file belongs to the writing process.
/// The replacement gives the path a new inode, so another hard link to the old
/// file keeps the old store.
///
/// A path that resolves to a device or a pipe (`/dev/null`, or `/dev/stdout`
/// with a pipe behind it) takes the bytes in place: they are written straight
/// into it, with no temp file and no rename, and opening a pipe waits for its
/// reader. Any other file that is not a regular file is refused.
///
/// A store written over the file it was opened from
/// ([`create_over`](Self::create_over)) replaces only that file: the path must
/// still name the file with the given identity when the temp file is created
/// and again just before the rename. Otherwise the write is refused with
/// [`InvalidOperation`](VortexRdfError::InvalidOperation) and nothing is
/// replaced. A file that is gone is not another file: the store is written
/// there.
///
/// A store the process cannot write is never replaced: the old file is opened
/// for writing as a probe (not truncated, not created, nothing changes) before
/// anything is built, and a `PermissionDenied` answer (or a read-only
/// filesystem's) fails the write with an error naming the path and keeping
/// the kind. A read-only store signals that it should not be overwritten. A
/// directory that takes no new file is refused the same way, when the temp
/// file cannot be created. These two refusals, and only these, are marked
/// ([`StoreNotWritable`](crate::error::StoreNotWritable)), so a caller that
/// can do without the write (an append whose auto-compaction is refused) tells
/// them from a permission error that comes later, such as a refused rename.
///
/// The old file is never modified. Overwriting it in place would be
/// unsafe while a reader still maps it (its pages would be pulled out from
/// under the mapping: SIGBUS, or another store's bytes read as the old
/// ones), and a process that dies mid-write would leave a half-written file
/// at `path`. The rename makes the swap atomic: a reader that mapped the old
/// file keeps its pages until it drops (Windows refuses the rename over a
/// mapped file, and the write then fails with the I/O error until that store
/// is closed), and `path` is untouched on any earlier failure.
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
#[cfg(feature = "file-io")]
pub(crate) struct PendingStore {
    /// The path the caller named, for error messages.
    path: std::path::PathBuf,
    sink: Sink,
    file: tokio::fs::File,
}

/// Where a [`PendingStore`]'s bytes end up.
#[cfg(feature = "file-io")]
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
    /// A device or a pipe, written in place.
    InPlace,
}

#[cfg(feature = "file-io")]
impl PendingStore {
    /// Check that `path` can take a store and open the file for it: resolve the
    /// links at `path`; take a device or a pipe in place; refuse a directory,
    /// any other file that is not a regular file and a store this process
    /// cannot write; otherwise create the temp file beside the file the store
    /// will replace (private, for an existing store: see [`create_temp`]) and
    /// copy that file's permissions onto it.
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
        // is a link to the descriptor): a device or a pipe takes the store in
        // place.
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
        // A directory can never be replaced by a store: say so now, not at the
        // rename after the build. Otherwise remember the old file's permissions
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
                        "it is not a regular file, a device or a pipe",
                    ),
                ));
            }
            Ok(meta) => {
                if opened.is_some_and(|opened| FileIdentity::of(&meta) != Some(opened)) {
                    return Err(replaced_since_open(path));
                }
                // A store this process could not write is never replaced: a
                // read-only store signals that it should not be overwritten.
                // The rename itself needs only the directory, so the file is
                // asked directly: opened for writing, neither truncated nor
                // created, so nothing changes (root still bypasses).
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
            // born private, so nothing of a private store is ever readable
            // through its mode, and it only becomes as readable as the old
            // file was.
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

    /// The directory the temp file is in: beside the file this store
    /// replaces, links followed (not beside the link a store was opened
    /// through). The finished store is renamed within it, which makes it the
    /// one volume known to take a file as large as the store, and so the
    /// place for a build's scratch space. A store written in place has none.
    pub(crate) fn dir(&self) -> Option<&std::path::Path> {
        match &self.sink {
            Sink::Replace { target, .. } => target.parent(),
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

/// Whether `meta` describes a device or a pipe: a file a store is written
/// into, not renamed over.
#[cfg(feature = "file-io")]
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
#[cfg(feature = "file-io")]
fn replaced_since_open(path: &std::path::Path) -> VortexRdfError {
    VortexRdfError::InvalidOperation(format!(
        "the file at {path:?} was replaced since this store opened it; reopen it"
    ))
}

/// Refuse unless the file at `target` is still the one `opened` identifies.
/// A file that is gone is not another file.
#[cfg(feature = "file-io")]
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

/// Refuse a store write while Vortex's experimental patched-array switch is
/// on: its compressor then emits an array no store edition admits, and the
/// write would fail at the first chunk that holds one.
pub(crate) fn refuse_experimental_patches() -> Result<()> {
    if vortex_array::arrays::patched::use_experimental_patches() {
        return Err(VortexRdfError::Serialization(
            "VORTEX_EXPERIMENTAL_PATCHED_ARRAY=1 makes Vortex's compressor emit the \
             experimental patched array, which no store file may hold; unset it to write a store"
                .to_string(),
        ));
    }
    Ok(())
}

/// Create the temp file at `path`, open for writing.
///
/// `create_new`: a file already at the temp name is never opened, let alone
/// truncated and then deleted by the [`TempFile`] guard. The creation is
/// synchronous, so a caller guards the file in the same poll that makes it.
///
/// A temp file that stands in for an existing store (`replacing`) is born
/// readable and writable by its owner alone on Unix, whatever the umask would
/// give a new file; the old file's permissions are set on it right after. So a
/// private store is never in a file others can read, not even for the moment
/// between the temp file's creation and that `chmod`. A temp file for a fresh
/// path gets the permissions any new file gets.
#[cfg(feature = "file-io")]
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
#[cfg(feature = "file-io")]
const MAX_LINK_HOPS: usize = 40;

/// The file a store written "to `path`" replaces: `path` itself, unless it is
/// a symbolic link, in which case the file the chain of links ends at. A link
/// to a file that is not there yet ends at that file, which is then created,
/// as creating through the link always did.
#[cfg(feature = "file-io")]
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
/// [persisted](Self::persist) — so each error path of [`write_store_atomically`]
/// (and a dropped future) cleans up without a line of its own.
#[cfg(feature = "file-io")]
struct TempFile(Option<std::path::PathBuf>);

#[cfg(feature = "file-io")]
impl TempFile {
    fn path(&self) -> &std::path::Path {
        self.0.as_deref().expect("a live temp file has a path")
    }

    /// Keep the file: it has been renamed away, so there is nothing to remove.
    fn persist(mut self) {
        self.0 = None;
    }
}

#[cfg(feature = "file-io")]
impl Drop for TempFile {
    fn drop(&mut self) {
        if let Some(path) = &self.0 {
            let _ = std::fs::remove_file(path);
        }
    }
}
