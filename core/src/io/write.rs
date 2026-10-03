//! The write side of the native store container: driving a [`BuiltStream`]
//! into a writer, and the `quads_stream_to_*` entry points that run the
//! sorted builder straight into one.

use crate::error::Result;

#[cfg(feature = "file-io")]
use crate::debug;
use crate::io::container::{self, default_child_strategy};
#[cfg(feature = "file-io")]
use crate::store::LayoutStrategy;
use crate::store::builders::BuiltStream;
use vortex_array::stream::ArrayStreamAdapter;
use vortex_io::VortexWrite;

#[cfg(feature = "file-io")]
use crate::store::builders::{SortedStreamBuilder, VortexArrayBuilder};
#[cfg(feature = "file-io")]
use crate::store::{Indexes, RawQuad};
#[cfg(feature = "file-io")]
use futures::Stream;

/// Stream quads into a native store file as compressed chunks, through the
/// out-of-core [`SortedStreamBuilder`]. Peak memory is bounded by the chunk
/// size plus, with index children, the in-flight components' compressed
/// size; the dictionary is complete before any chunk flows and becomes the
/// required `dictionary` child.
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
        "[quads_stream_to_vortex_writer] Streaming write took {:?}",
        debug::elapsed(start)
    );
    Ok(())
}

/// Drive a built chunk stream into `writer`: the primary chunks as the
/// transparent root child, each component and the dictionary as auxiliary
/// children; then shut the writer down.
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
    .await?;
    writer.shutdown().await?;
    Ok(())
}

/// Serialize a quad stream to a native store file at `path`.
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

/// Create the file a store is written to; a failure is reported as
/// `VortexRdfError::Io` naming `path`.
#[cfg(feature = "file-io")]
pub(crate) async fn create_store_file(path: &std::path::Path) -> Result<tokio::fs::File> {
    tokio::fs::File::create(path).await.map_err(|e| {
        crate::error::VortexRdfError::Io(std::io::Error::new(
            e.kind(),
            format!("create {path:?}: {e}"),
        ))
    })
}
