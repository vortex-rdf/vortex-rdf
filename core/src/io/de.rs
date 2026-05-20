use crate::error;
use crate::error::{Result, VortexRdfError};
use crate::store::QuadStore;

use futures::StreamExt;
use oxrdfio::{RdfFormat, RdfSerializer};
#[cfg(feature = "file-io")]
use std::io::Write;
use std::time::Instant;

#[cfg(feature = "file-io")]
use vortex::VortexSessionDefault;
use vortex_array::ArrayRef;
use vortex_array::session::ArraySession;
#[cfg(feature = "file-io")]
use vortex_array::stream::ArrayStreamExt;
#[cfg(feature = "file-io")]
use vortex_file::OpenOptionsSessionExt;
#[cfg(feature = "file-io")]
use vortex_io::file::IntoReadSource;
use vortex_ipc::iterator::SyncIPCReader;
#[cfg(feature = "file-io")]
use vortex_session::VortexSession;

/// High-level function to deserialize Vortex-RDF data store into an RDF writer.
pub async fn deserialize<Store, W>(store: Store, writer: W, format: RdfFormat) -> error::Result<()>
where
    Store: QuadStore,
    W: Write,
{
    let decode_start = Instant::now();
    let mut quads_stream = store.quads()?;
    log::debug!(
        "[deserialize] Quad stream setup took {:?}",
        decode_start.elapsed()
    );

    let write_start = Instant::now();
    let mut rdf_serializer = RdfSerializer::from_format(format).for_writer(writer);
    while let Some(quad_res) = quads_stream.next().await {
        let quad = quad_res?;
        rdf_serializer
            .serialize_quad(&quad)
            .map_err(|e| error::VortexRdfError::Deserialization(e.to_string()))?;
    }
    rdf_serializer
        .finish()
        .map_err(|e| error::VortexRdfError::Deserialization(e.to_string()))?;

    log::debug!(
        "[deserialize] Serialization/write loop took {:?}",
        write_start.elapsed()
    );

    Ok(())
}

pub fn array_from_reader<R: std::io::Read>(reader: R) -> Result<ArrayRef> {
    let array_session = ArraySession::default();
    let registry = array_session.registry();

    // Register FSST encoding - check where it moved
    // registry.register(EncodingRef::new_ref(vortex_fsst::FSSTEncoding.as_ref()));

    let mut reader =
        SyncIPCReader::try_new(reader, registry.clone()).map_err(VortexRdfError::Vortex)?;

    let array = reader
        .next()
        .transpose()
        .map_err(VortexRdfError::Vortex)?
        .ok_or_else(|| VortexRdfError::Deserialization("No array in IPC stream".to_string()))?;

    Ok(array)
}

#[cfg(feature = "file-io")]
pub async fn load_vortex_file_ref<S: IntoReadSource>(source: S) -> Result<ArrayRef> {
    let start = Instant::now();
    let session = VortexSession::default();

    let file = session
        .open_options()
        .open(source)
        .await
        .map_err(|e| VortexRdfError::from(e))?;
    log::debug!(
        "[de::read_array_from_vortex] File Open Session took {:?}",
        start.elapsed()
    );

    let scan_start = Instant::now();
    let scan = file.scan().map_err(|e| VortexRdfError::from(e))?;
    let stream = scan
        .into_array_stream()
        .map_err(|e| VortexRdfError::from(e))?;
    log::debug!(
        "[de::read_array_from_vortex] Scan took {:?}",
        scan_start.elapsed()
    );

    let read_start = Instant::now();
    let vortex_array: ArrayRef = stream
        .read_all()
        .await
        .map_err(|e: vortex_error::VortexError| VortexRdfError::from(e))?;
    log::debug!(
        "[de::read_array_from_vortex] Stream read_all took {:?}",
        read_start.elapsed()
    );

    Ok(vortex_array)
}
