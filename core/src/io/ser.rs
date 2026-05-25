use crate::error::{Result, VortexRdfError};
use crate::store::VortexRdfStore;
use crate::index::SimpleDictionary;
use crate::error;

use std::sync::Arc;
use std::sync::LazyLock;
use std::time::Instant;
use oxrdf::Quad;

use vortex_array::ArrayRef;
use vortex_array::stream::ArrayStreamAdapter;
use vortex_array::dtype::FieldPath;
use vortex_array::LEGACY_SESSION;
use vortex_array::session::ArraySession;
use vortex_array::scalar_fn::session::ScalarFnSession;
use vortex_io::VortexWrite;
use vortex_io::session::RuntimeSession;
use vortex_layout::LayoutStrategy;
use vortex_layout::session::LayoutSession;
use vortex_layout::layouts::flat::writer::FlatLayoutStrategy;
use vortex_layout::layouts::chunked::writer::ChunkedLayoutStrategy;
use vortex_ipc::iterator::ArrayIteratorIPC;
use futures::{stream, Stream};
use vortex_file::{WriteStrategyBuilder, WriteOptionsSessionExt};
use vortex_session::VortexSession;

/// A lazily-initialized session configured for Vortex file I/O.
static WRITE_SESSION: LazyLock<VortexSession> = LazyLock::new(|| {
    let session = VortexSession::empty()
        .with::<ArraySession>()
        .with::<LayoutSession>()
        .with::<ScalarFnSession>()
        .with::<RuntimeSession>();
    vortex_file::register_default_encodings(&session);
    session
});

/// High-level function to serialize RDF from a reader directly to a Vortex-RDF writer.
pub async fn serialize<W: VortexWrite + Unpin + Send>(
    vortex_array: ArrayRef,
    mut writer: W,
) -> Result<()> {
    let session_start = Instant::now();

    // Configure layout strategies for dictionary storage.
    let flat_strategy: Arc<dyn LayoutStrategy> = Arc::new(FlatLayoutStrategy::default());
    let chunked_strategy: Arc<dyn LayoutStrategy> = Arc::new(ChunkedLayoutStrategy::new(flat_strategy));

    // Mark all _dict_* columns to be written in a [chunked[flat]] layout.
    // This bypasses the default BtrBlocks adaptive compression heuristics on dictionary fields.
    // The adaptive compressor can consume exponential memory when evaluating highly complex
    // nested structures like listviews / dictionaries (which we already pre-compress using FSST anyway).
    // TODO: check if we can directly apply suitable compression strategies based on DType.
    let mut builder = WriteStrategyBuilder::default();
    if let Some(struct_fields) = vortex_array.dtype().as_struct_fields_opt() {
        for name in struct_fields.names().iter() {
            let name_str: &str = name.as_ref();
            if name_str.starts_with("_dict_") {
                builder = builder.with_field_writer(
                    FieldPath::from_name(name_str),
                    chunked_strategy.clone(),
                );
            }
        }
    }

    // Initialize the file write options with our layout bypass strategy.
    let write_opts = WRITE_SESSION
        .write_options()
        .with_strategy(builder.build());
        
    let dtype = vortex_array.dtype().clone();
    let vortex_stream = ArrayStreamAdapter::new(
        dtype,
        Box::pin(stream::once(async move { Ok(vortex_array) }))
    );
    log::debug!("[ser::write_stream_to_vortex] Vortex writer options setup took {:?}", session_start.elapsed());

    let write_start = Instant::now();
    // Serialize the stream to the destination writer.
    let _summary = write_opts
        .write(&mut writer, vortex_stream)
        .await
        .map_err(|e: vortex_error::VortexError| VortexRdfError::from(e))?;
    log::debug!("[ser::write_stream_to_vortex] Vortex writing took {:?}", write_start.elapsed());

    // Flush and finalize the writer stream.
    writer.shutdown().await
        .map_err(|e| VortexRdfError::Serialization(format!("Failed to shutdown/flush writer: {}", e)))?;

    Ok(())
}

/// Serializes an in-memory Vortex ArrayRef directly to an IPC byte writer.
/// Exclusive to standard in-memory IPC transport layers.
pub fn write_array_to_ipc<W: std::io::Write>(vortex_array: ArrayRef, mut writer: W) -> Result<()> {
    // Convert the array into an IPC-compatible iterator.
    let ipc_iter = vortex_array
        .to_array_iterator()
        .into_ipc(&LEGACY_SESSION)
        .map_err(VortexRdfError::Vortex)?;

    // Stream IPC messages sequentially to the output writer.
    for msg_res in ipc_iter {
        let msg = msg_res.map_err(VortexRdfError::Vortex)?;
        writer
            .write_all(&msg)
            .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    }

    Ok(())
}

/// High-level function to serialize a stream of quads to a Vortex-RDF writer.
pub async fn quads_stream_to_vortex_writer<S, W>(quads: S, writer: W) -> error::Result<()>
where
    S: Stream<Item = error::Result<Quad>> + Unpin + Send + 'static,
    W: VortexWrite + Unpin + Send,
{
    // Build index using flat SimpleDictionary schema and serialize.
    // TODO: allow for index type selection
    let stream = VortexRdfStore::<SimpleDictionary>::build_vortex_array(quads).await?;
    serialize(stream, writer).await?;
    Ok(())
}

/// High-level function to serialize a stream of quads directly to a byte buffer.
pub async fn quads_stream_to_vortex<S>(quads: S) -> error::Result<Vec<u8>>
where
    S: Stream<Item = error::Result<Quad>> + Unpin + Send + 'static,
{
    let mut buffer = Vec::new();
    quads_stream_to_vortex_writer(quads, &mut buffer).await?;
    Ok(buffer)
}