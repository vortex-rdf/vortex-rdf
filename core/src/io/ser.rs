use crate::error::{Result, VortexRdfError};
use crate::store::simple_dictionary_store::SimpleDictionaryStore;
use crate::store::VortexRdfStore;
use crate::error;

use std::time::Instant;
use oxrdf::Quad;

use vortex_array::ArrayRef;
use vortex_array::stream::ArrayStreamAdapter;
use futures::stream;
use vortex_file::{WriteStrategyBuilder, WriteOptionsSessionExt};
use vortex_session::VortexSession;
use vortex_io::VortexWrite;
use vortex::VortexSessionDefault;
#[cfg(feature = "file-io")]
use vortex::compressor::CompactCompressor;

/// High-level function to serialize RDF from a reader directly to a Vortex-RDF writer.
pub async fn serialize<W: VortexWrite + Unpin + Send>(
    vortex_array: ArrayRef,
    mut writer: W,
) -> Result<()> {
    let session_start = Instant::now();
    let session = VortexSession::default();

    let mut strategy = WriteStrategyBuilder::new();
    
    #[cfg(feature = "file-io")]
    {
        strategy = strategy.with_compressor(CompactCompressor::default());
    }

    let write_opts = session.write_options().with_strategy(strategy.build());
    let dtype = vortex_array.dtype().clone();
    let vortex_stream = ArrayStreamAdapter::new(
        dtype,
        Box::pin(stream::once(async move { Ok(vortex_array) }))
    );
    log::debug!("[ser::write_stream_to_vortex] Vortex writer options setup took {:?}", session_start.elapsed());

    let write_start = Instant::now();
    let _summary = write_opts
        .write(&mut writer, vortex_stream)
        .await
        .map_err(|e: vortex_error::VortexError| VortexRdfError::from(e))?;
    log::debug!("[ser::write_stream_to_vortex] Vortex writing took {:?}", write_start.elapsed());
    
    Ok(())
}

pub fn write_array_to_ipc<W: std::io::Write>(vortex_array: ArrayRef, mut writer: W) -> Result<()> {
    use vortex_ipc::iterator::ArrayIteratorIPC;
    let ipc_iter = vortex_array.to_array_iterator().into_ipc();

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
    S: futures::Stream<Item = error::Result<Quad>> + Unpin + Send + 'static,
    W: VortexWrite + Unpin + Send,
{
    // TODO: allow for index type selection
    let stream = SimpleDictionaryStore::build_vortex_index(quads).await?;
    serialize(stream, writer).await?;
    Ok(())
}

/// High-level function to serialize a stream of quads directly to a byte buffer.
pub async fn quads_stream_to_vortex<S>(quads: S) -> error::Result<Vec<u8>>
where
    S: futures::Stream<Item = error::Result<Quad>> + Unpin + Send + 'static,
{
    let mut buffer = Vec::new();
    quads_stream_to_vortex_writer(quads, &mut buffer).await?;
    Ok(buffer)
}