use crate::error;
use crate::error::{Result, VortexRdfError};
use crate::store::QuadStore;

use futures::StreamExt;
use oxrdfio::{RdfFormat, RdfSerializer};
#[cfg(feature = "file-io")]
use std::io::Write;
use std::time::Instant;
use vortex::VortexSessionDefault;

#[cfg(feature = "file-io")]
use vortex_array::ArrayRef;
use vortex_ipc::iterator::SyncIPCReader;
use vortex_array::LEGACY_SESSION;

#[cfg(feature = "file-io")]
use std::sync::Arc;
#[cfg(feature = "file-io")]
use vortex_array::stream::ArrayStreamExt;
#[cfg(feature = "file-io")]
use vortex_file::OpenOptionsSessionExt;
#[cfg(feature = "file-io")]
use vortex_io::VortexReadAt;
use vortex_session::VortexSession;

/// High-level function to deserialize Vortex-RDF data store into an RDF writer.
/// Pulls quads sequentially from the store and serializes them in the specified format (Turtle, N-Triples, etc.).
pub async fn deserialize<Store, W>(
    store: Store,
    writer: W,
    format: RdfFormat,
) -> error::Result<()>
where
    Store: QuadStore,
    W: Write,
{
    let decode_start = Instant::now();
    // Retrieve the quad stream (either in-memory or lazy file-backed stream).
    let mut quads_stream = store.quads()?;
    log::debug!(
        "[deserialize] Quad stream setup took {:?}",
        decode_start.elapsed()
    );

    let write_start = Instant::now();
    // Construct the oxrdf serialization helper for streaming output.
    let mut rdf_serializer = RdfSerializer::from_format(format).for_writer(writer);
    
    // Dynamically iterate over each quad and push it to the output writer.
    while let Some(quad_res) = quads_stream.next().await {
        let quad = quad_res?;
        rdf_serializer
            .serialize_quad(&quad)
            .map_err(|e| error::VortexRdfError::Deserialization(e.to_string()))?;
    }
    
    // Finalize the serialization output (e.g. closing syntax blocks).
    rdf_serializer
        .finish()
        .map_err(|e| error::VortexRdfError::Deserialization(e.to_string()))?;

    log::debug!(
        "[deserialize] Serialization/write loop took {:?}",
        write_start.elapsed()
    );

    Ok(())
}

/// Reads a Vortex ArrayRef from a synchronous IPC reader stream.
/// Used for decoding in-memory IPC message payloads.
pub fn array_from_ipc_reader<R: std::io::Read>(reader: R) -> Result<ArrayRef> {
    let mut ipc_reader =
        SyncIPCReader::try_new(reader, &session).map_err(VortexRdfError::Vortex)?;

    let array = ipc_reader
        .next()
        .transpose()
        .map_err(VortexRdfError::Vortex)?
        .ok_or_else(|| VortexRdfError::Deserialization("No array in IPC stream".to_string()))?;

    Ok(array)
}

/// Construct a lazily-initialized static VortexSession for file reading/scanning.
#[cfg(feature = "file-io")]
fn file_session() -> &'static vortex_session::VortexSession {
    use std::sync::LazyLock;
    use vortex_array::scalar_fn::session::ScalarFnSession;
    use vortex_array::session::ArraySession;
    use vortex_io::session::RuntimeSession;
    use vortex_layout::session::LayoutSession;
    use vortex_session::VortexSession;

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

/// Loads a fully in-memory Vortex array from a generic read-at source (e.g. a byte buffer in memory).
#[cfg(feature = "file-io")]
pub async fn load_vortex_file_ref<S: VortexReadAt + 'static>(source: S) -> Result<ArrayRef> {
    let start = Instant::now();

    // 1. Open the source under our file read session context.
    let file = file_session()
        .open_options()
        .open(Arc::new(source))
        .await
        .map_err(VortexRdfError::from)?;
    log::debug!(
        "[de::read_array_from_vortex] File Open Session took {:?}",
        start.elapsed()
    );

    // 2. Initiate a file scan and convert it to an array stream.
    let scan_start = Instant::now();
    let scan = file.scan().map_err(VortexRdfError::from)?;
    let stream = scan.into_array_stream().map_err(VortexRdfError::from)?;
    log::debug!(
        "[de::read_array_from_vortex] Scan took {:?}",
        scan_start.elapsed()
    );

    // 3. Read the stream fully to load the array in host memory.
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

/// Open a Vortex file lazily — no data is read until the returned `VortexFile`
/// is scanned. This is the core entrypoint for our zero-copy, memory-efficient lazy store.
#[cfg(feature = "file-io")]
pub async fn open_vortex_file<P: AsRef<std::path::Path>>(
    path: P,
) -> Result<vortex_file::VortexFile> {
    file_session()
        .open_options()
        .open_path(path)
        .await
        .map_err(VortexRdfError::from)
}
