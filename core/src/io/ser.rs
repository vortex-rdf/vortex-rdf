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
use crate::io::container::{self, default_child_strategy};
use crate::store::LayoutStrategy;
use crate::store::StoreParts;
use crate::store::builders::BuiltStream;
use futures::StreamExt as _;
use vortex_array::stream::ArrayStreamAdapter;
use vortex_io::VortexWrite;

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

    container::write_store(
        &crate::session::VORTEX_SESSION,
        &mut writer,
        ArrayStreamAdapter::new(built.dtype, built.chunks),
        default_child_strategy(),
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
/// read, not after the whole ingest, sort and dictionary have run.
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
/// Every store a path-taking writer produces (`quads_stream_to_vortex_file`,
/// and compaction's rewrite of its own source file) goes through here.
///
/// A path that cannot take a store fails before `write` runs: a directory at
/// `path` is refused, and the temp file is created first, so a missing
/// directory or a refused write permission is reported at once — a caller
/// that reads its input inside `write` has not read it yet.
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
/// its link. The old file's permission bits are copied onto the temp file
/// before a byte is written, so a private store is never copied into a more
/// readable temp file. Owner, ACLs and extended attributes are not
/// preserved: the new file belongs to the writing process.
///
/// The old file is never opened for writing. Overwriting it in place would be
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
/// error from `write`, a failed rename, a panic, or this future being
/// dropped.
#[cfg(feature = "file-io")]
pub(crate) async fn write_store_atomically<F, Fut>(path: &std::path::Path, write: F) -> Result<()>
where
    F: FnOnce(tokio::fs::File) -> Fut,
    Fut: std::future::Future<Output = Result<()>>,
{
    let io_error = |what: &str, e: std::io::Error| {
        VortexRdfError::Io(std::io::Error::new(
            e.kind(),
            format!("{what} {path:?}: {e}"),
        ))
    };

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
        Ok(meta) => Some(meta.permissions()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => return Err(io_error("inspect", e)),
    };

    let tmp_path = target.with_extension(format!("write-{}.tmp", uuid::Uuid::new_v4()));
    // `create_new`: a file already at the temp name is never opened, let alone
    // truncated and then deleted by the guard below.
    let file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&tmp_path)
        .await
        .map_err(|e| io_error("create a temporary file beside", e))?;
    let tmp = TempFile(Some(tmp_path));
    if let Some(permissions) = permissions {
        // Before the first byte, not before the rename: nothing of a private
        // store is ever readable through the temp file's default mode.
        tokio::fs::set_permissions(tmp.path(), permissions)
            .await
            .map_err(|e| io_error("copy the permissions onto the temporary file of", e))?;
    }

    write(file).await?;
    tokio::fs::rename(tmp.path(), &target)
        .await
        .map_err(|e| io_error("replace", e))?;
    // Renamed into place: there is no temp file left to remove.
    tmp.persist();
    Ok(())
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
