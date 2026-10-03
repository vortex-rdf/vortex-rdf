//! The write side of the native store container.
//!
//! Packs a store's parts — the primary quad table plus each index
//! component's and the dictionary's own
//! [`NativeComponentWrite`](crate::io::container::NativeComponentWrite) —
//! into a [`BuiltStream`] and drives
//! [`write_store`](crate::io::container::write_store) over it, carrying
//! each part's sortedness provenance onto the descriptors a reader will
//! trust. Also owns the `quads_stream_to_*` entry points, which run a
//! builder's chunk stream straight into that writer.
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
/// convenience over [`quads_stream_to_vortex_writer`].
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
    let writer = create_store_file(path).await?;
    quads_stream_to_vortex_writer(quads, writer, layout, indexes).await
}

/// Create the file a store is written to, reporting a failure as
/// [`VortexRdfError::Io`] with `path` in the message.
#[cfg(feature = "file-io")]
pub(crate) async fn create_store_file(path: &std::path::Path) -> Result<tokio::fs::File> {
    tokio::fs::File::create(path).await.map_err(|e| {
        VortexRdfError::Io(std::io::Error::new(
            e.kind(),
            format!("create {path:?}: {e}"),
        ))
    })
}
