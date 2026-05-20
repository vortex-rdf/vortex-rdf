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
use vortex_ipc::iterator::SyncIPCReader;

#[cfg(feature = "file-io")]
use std::sync::Arc;
#[cfg(feature = "file-io")]
use vortex_io::VortexReadAt;
#[cfg(feature = "file-io")]
use vortex_array::stream::ArrayStreamExt;
#[cfg(feature = "file-io")]
use vortex_file::OpenOptionsSessionExt;

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

    log::debug!("[deserialize] Serialization/write loop took {:?}", write_start.elapsed());

    Ok(())
}

pub fn array_from_reader<R: std::io::Read>(reader: R) -> Result<ArrayRef> {
    use vortex_array::LEGACY_SESSION;

    let mut ipc_reader =
        SyncIPCReader::try_new(reader, &LEGACY_SESSION).map_err(VortexRdfError::Vortex)?;

    let array = ipc_reader
        .next()
        .transpose()
        .map_err(VortexRdfError::Vortex)?
        .ok_or_else(|| VortexRdfError::Deserialization("No array in IPC stream".to_string()))?;

    Ok(array)
}

#[cfg(feature = "file-io")]
fn file_session() -> &'static vortex_session::VortexSession {
    use std::sync::LazyLock;
    use vortex_session::VortexSession;
    use vortex_array::session::ArraySession;
    use vortex_array::scalar_fn::session::ScalarFnSession;
    use vortex_io::session::RuntimeSession;
    use vortex_layout::session::LayoutSession;

    static FILE_SESSION: LazyLock<VortexSession> = LazyLock::new(|| {
        let session = VortexSession::empty()
            .with::<ArraySession>()
            .with::<LayoutSession>()
            .with::<ScalarFnSession>()
            .with::<RuntimeSession>();
        vortex_file::register_default_encodings(&session);
        session
    });
    &FILE_SESSION
}

#[cfg(feature = "file-io")]
pub async fn load_vortex_file_ref<S: VortexReadAt + 'static>(
    source: S,
) -> Result<ArrayRef> {
    let start = Instant::now();

    let file = file_session()
        .open_options()
        .open(Arc::new(source))
        .await
        .map_err(VortexRdfError::from)?;
    log::debug!("[de::read_array_from_vortex] File Open Session took {:?}", start.elapsed());

    let scan_start = Instant::now();
    let scan = file.scan().map_err(VortexRdfError::from)?;
    let stream = scan.into_array_stream().map_err(VortexRdfError::from)?;
    log::debug!("[de::read_array_from_vortex] Scan took {:?}", scan_start.elapsed());

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

#[cfg(feature = "file-io")]
pub async fn load_vortex_file_path<P: AsRef<std::path::Path>>(path: P) -> Result<ArrayRef> {
    let start = Instant::now();

    let file = file_session()
        .open_options()
        .open_path(path)
        .await
        .map_err(VortexRdfError::from)?;
    log::debug!("[de::load_vortex_file_path] File Open Session took {:?}", start.elapsed());

    let scan_start = Instant::now();
    let scan = file.scan().map_err(VortexRdfError::from)?;
    let stream = scan.into_array_stream().map_err(VortexRdfError::from)?;
    log::debug!("[de::load_vortex_file_path] Scan took {:?}", scan_start.elapsed());

    let read_start = Instant::now();
    let vortex_array: ArrayRef = stream
        .read_all()
        .await
        .map_err(|e: vortex_error::VortexError| VortexRdfError::from(e))?;
    log::debug!("[de::load_vortex_file_path] Stream read_all took {:?}", read_start.elapsed());

    Ok(vortex_array)
}

#[cfg(feature = "file-io")]
pub async fn load_vortex_file_path<P: AsRef<std::path::Path>>(path: P) -> Result<ArrayRef> {
    let start = Instant::now();

    let file = file_session()
        .open_options()
        .open_path(path)
        .await
        .map_err(VortexRdfError::from)?;
    log::debug!("[de::load_vortex_file_path] File Open Session took {:?}", start.elapsed());

    let scan_start = Instant::now();
    let scan = file.scan().map_err(VortexRdfError::from)?;
    let stream = scan.into_array_stream().map_err(VortexRdfError::from)?;
    log::debug!("[de::load_vortex_file_path] Scan took {:?}", scan_start.elapsed());

    let read_start = Instant::now();
    let vortex_array: ArrayRef = stream
        .read_all()
        .await
        .map_err(|e: vortex_error::VortexError| VortexRdfError::from(e))?;
    log::debug!("[de::load_vortex_file_path] Stream read_all took {:?}", read_start.elapsed());

    Ok(vortex_array)
}
