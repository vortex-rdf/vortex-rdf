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
//! ([`write_store_file`](crate::io::ser::write_store_file)).
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
/// never rewritten in place).
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

    // Ingest, sort and the dictionary run to completion before the temp file
    // exists, so an input that fails never creates one.
    let built = SortedStreamBuilder::build_vortex_stream(Box::new(quads), layout, indexes).await?;
    write_store_file(path, |writer| built_stream_to_vortex_writer(built, writer)).await?;

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
/// The temp file is a sibling so the rename stays on one filesystem, which
/// is what makes it atomic. The uuid in its name avoids colliding with a
/// temp left behind by an earlier interrupted write, and the `.tmp`
/// extension keeps it from being mistaken for a store.
///
/// The old file is never opened for writing. Overwriting it in place would be
/// unsafe while a reader still maps it (its pages would be pulled out from
/// under the mapping: SIGBUS, or another store's bytes read as the old
/// ones), and a crash mid-write must never leave the only on-disk copy
/// half-written. The rename makes the swap atomic: a reader that mapped the
/// old file keeps its pages until it drops (Windows refuses the rename over
/// a mapped file, and the write then fails with the I/O error), and `path`
/// is untouched on any earlier failure.
///
/// The temp file is removed on every way out that does not rename it: an
/// error from `write`, a failed rename, a panic, or this future being
/// dropped.
#[cfg(feature = "file-io")]
pub(crate) async fn write_store_file<F, Fut>(path: &std::path::Path, write: F) -> Result<()>
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

    let tmp_path = path.with_extension(format!("write-{}.tmp", uuid::Uuid::new_v4()));
    let file = tokio::fs::File::create(&tmp_path)
        .await
        .map_err(|e| io_error("create a temporary file beside", e))?;
    let tmp = TempFile(Some(tmp_path));

    write(file).await?;
    tokio::fs::rename(tmp.path(), path)
        .await
        .map_err(|e| io_error("replace", e))?;
    // Renamed into place: there is no temp file left to remove.
    tmp.persist();
    Ok(())
}

/// A temp file that deletes itself when dropped, unless it was
/// [persisted](Self::persist) — so each error path of [`write_store_file`]
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
