use crate::error;
use crate::error::{Result, VortexRdfError};
use crate::index::SimpleDictionary;
use crate::store::VortexRdfStore;

use oxrdf::Quad;
use std::sync::LazyLock;
use std::time::Instant;
use vortex::VortexSessionDefault;

use futures::stream;

/// A lazily-initialized session configured for Vortex file I/O.
static WRITE_SESSION: LazyLock<VortexSession> = LazyLock::new(|| {
    use vortex_array::scalar_fn::session::ScalarFnSession;
    use vortex_array::session::ArraySession;
    use vortex_io::session::RuntimeSession;
    use vortex_layout::session::LayoutSession;

    let session = VortexSession::empty()
        .with::<ArraySession>()
        .with::<LayoutSession>()
        .with::<ScalarFnSession>()
        .with::<RuntimeSession>();
    vortex_file::register_default_encodings(&session);
    session
});
use vortex_array::stream::ArrayStreamAdapter;
use vortex_array::{ArrayRef};
use vortex_file::{WriteOptionsSessionExt, WriteStrategyBuilder};
use vortex_io::VortexWrite;
use vortex_session::VortexSession;
use vortex_ipc::iterator::ArrayIteratorIPC;

/// High-level function to serialize RDF from a reader directly to a Vortex-RDF writer.
pub async fn serialize<W: VortexWrite + Unpin + Send>(
    vortex_array: ArrayRef,
    mut writer: W,
) -> Result<()> {
    let session_start = Instant::now();

    let write_opts = WRITE_SESSION
        .write_options()
        .with_strategy(WriteStrategyBuilder::default().build());
    let dtype = vortex_array.dtype().clone();
    let vortex_stream = ArrayStreamAdapter::new(
        dtype,
        Box::pin(stream::once(async move { Ok(vortex_array) })),
    );
    log::debug!(
        "[ser::write_stream_to_vortex] Vortex writer options setup took {:?}",
        session_start.elapsed()
    );

    let write_start = Instant::now();
    let _summary = write_opts
        .write(&mut writer, vortex_stream)
        .await
        .map_err(|e: vortex_error::VortexError| VortexRdfError::from(e))?;
    log::debug!(
        "[ser::write_stream_to_vortex] Vortex writing took {:?}",
        write_start.elapsed()
    );

    Ok(())
}

pub fn write_array_to_ipc<W: std::io::Write>(vortex_array: ArrayRef, mut writer: W) -> Result<()> {
    // TODO: we should be able to reuse the same session for writing and reading, 
    //otherwise we might run into issues with incompatible encodings, etc.
    let session = VortexSession::default();

    // Pass a reference to the local default session instead
    let ipc_iter = vortex_array
        .to_array_iterator()
        .into_ipc(&session)
        .map_err(VortexRdfError::Vortex)?;

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
    let stream = VortexRdfStore::<SimpleDictionary, crate::store::layout::flat::FlatLayout>::build_vortex_index(quads).await?;
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
