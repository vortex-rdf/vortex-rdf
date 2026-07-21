use crate::error::{Result, VortexRdfError};

use vortex_array::ArrayRef;
use vortex_ipc::iterator::ArrayIteratorIPC;

#[cfg(feature = "file-io")]
use crate::error;
#[cfg(feature = "file-io")]
use crate::store::builders::{UnsortedStreamBuilder, VortexArrayBuilder};
#[cfg(feature = "file-io")]
use crate::store::{Indexes, LayoutStrategy};
#[cfg(feature = "file-io")]
use futures::{Stream, stream};
#[cfg(feature = "file-io")]
use oxrdf::Quad;
#[cfg(feature = "file-io")]
use vortex_array::expr::stats::Stat;
#[cfg(feature = "file-io")]
use vortex_array::stats::PRUNING_STATS;
#[cfg(feature = "file-io")]
use vortex_array::stream::ArrayStreamAdapter;
#[cfg(feature = "file-io")]
use vortex_file::WriteOptionsSessionExt;
#[cfg(feature = "file-io")]
use vortex_io::VortexWrite;
#[cfg(feature = "file-io")]
use web_time::Instant;

#[cfg(feature = "file-io")]
fn write_options_with_subject_stats() -> vortex_file::VortexWriteOptions {
    let mut stats = PRUNING_STATS.to_vec();
    if !stats.contains(&Stat::IsSorted) {
        stats.push(Stat::IsSorted);
    }
    super::VORTEX_SESSION
        .write_options()
        .with_file_statistics(stats)
}

/// Serialize an already-materialized Vortex array to a Vortex file writer.
///
/// Prefer [`quads_stream_to_vortex_writer_with_builder`] when serializing from
/// a quad stream: it feeds chunks to the writer as they are built instead of
/// requiring the whole array up front.
#[cfg(feature = "file-io")]
pub async fn serialize<W: VortexWrite + Unpin + Send>(
    vortex_array: ArrayRef,
    mut writer: W,
) -> Result<()> {
    let start = Instant::now();

    let dtype = vortex_array.dtype().clone();
    let vortex_stream = ArrayStreamAdapter::new(
        dtype,
        Box::pin(stream::once(async move { Ok(vortex_array) })),
    );

    let _summary = write_options_with_subject_stats()
        .write(&mut writer, vortex_stream)
        .await
        .map_err(VortexRdfError::Vortex)?;

    writer
        .shutdown()
        .await
        .map_err(|e| VortexRdfError::Serialization(format!("Failed to shutdown writer: {}", e)))?;

    log::debug!("[ser::serialize] Vortex writing took {:?}", start.elapsed());
    Ok(())
}

/// Serialize a Vortex array to IPC bytes.
pub fn write_array_to_ipc<W: std::io::Write>(vortex_array: ArrayRef, mut writer: W) -> Result<()> {
    let ipc_iter = vortex_array
        .to_array_iterator()
        .into_ipc(&super::VORTEX_LIGHT_SESSION)
        .map_err(VortexRdfError::Vortex)?;

    for msg_res in ipc_iter {
        let msg = msg_res.map_err(VortexRdfError::Vortex)?;
        writer
            .write_all(&msg)
            .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    }

    Ok(())
}

/// Stream quads directly into a Vortex file writer as compressed chunks.
///
/// The builder's [`VortexArrayBuilder::build_vortex_stream`] produces chunks
/// lazily; the Vortex writer consumes, compresses, and flushes each chunk as
/// it arrives. For streaming-capable builders (e.g. `UnsortedStreamBuilder`)
/// peak memory is bounded by the chunk size instead of the dataset size.
#[cfg(feature = "file-io")]
pub async fn quads_stream_to_vortex_writer_with_builder<B, S, W>(
    quads: S,
    mut writer: W,
    layout: LayoutStrategy,
    indexes: Indexes,
) -> Result<()>
where
    B: VortexArrayBuilder,
    S: Stream<Item = error::Result<Quad>> + Unpin + Send + 'static,
    W: VortexWrite + Unpin + Send,
{
    let start = Instant::now();

    let (dtype, chunks) = B::build_vortex_stream(Box::new(quads), layout, indexes).await?;
    let vortex_stream = ArrayStreamAdapter::new(dtype, chunks);

    let _summary = write_options_with_subject_stats()
        .write(&mut writer, vortex_stream)
        .await
        .map_err(VortexRdfError::Vortex)?;

    writer
        .shutdown()
        .await
        .map_err(|e| VortexRdfError::Serialization(format!("Failed to shutdown writer: {}", e)))?;

    log::debug!(
        "[ser::quads_stream_to_vortex_writer_with_builder] Streaming write took {:?}",
        start.elapsed()
    );
    Ok(())
}

/// Serialize a stream of quads directly to a Vortex file writer using the
/// default configuration (UnsortedStream builder, Default layout, no indexes).
#[cfg(feature = "file-io")]
pub async fn quads_stream_to_vortex_writer<S, W>(quads: S, writer: W) -> error::Result<()>
where
    S: Stream<Item = error::Result<Quad>> + Unpin + Send + 'static,
    W: VortexWrite + Unpin + Send,
{
    quads_stream_to_vortex_writer_with_builder::<UnsortedStreamBuilder, _, _>(
        quads,
        writer,
        LayoutStrategy::Default,
        Vec::new(),
    )
    .await
}

/// Serialize a stream of quads to an in-memory Vortex file byte buffer.
#[cfg(feature = "file-io")]
pub async fn quads_stream_to_vortex<S>(quads: S) -> error::Result<Vec<u8>>
where
    S: Stream<Item = error::Result<Quad>> + Unpin + Send + 'static,
{
    let mut buffer = Vec::new();
    quads_stream_to_vortex_writer(quads, &mut buffer).await?;
    Ok(buffer)
}
