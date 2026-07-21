use crate::error;
use crate::error::{Result, VortexRdfError};
use crate::store::VortexRdfStore;

use futures::StreamExt;
use oxrdfio::{RdfFormat, RdfSerializer};
use std::io::Write;
use web_time::Instant;

use vortex_array::arrays::ChunkedArray;
use vortex_array::{ArrayRef, IntoArray};
use vortex_ipc::iterator::SyncIPCReader;

#[cfg(feature = "file-io")]
use std::sync::Arc;
#[cfg(feature = "file-io")]
use vortex_array::stream::ArrayStreamExt;
#[cfg(feature = "file-io")]
use vortex_file::OpenOptionsSessionExt;
#[cfg(feature = "file-io")]
use vortex_io::VortexReadAt;

/// High-level function to deserialize Vortex-RDF data store into an RDF writer.
/// Pulls quads sequentially from the store and serializes them in the specified format (Turtle, N-Triples, etc.).
pub async fn deserialize<W: Write>(
    store: VortexRdfStore,
    writer: W,
    format: RdfFormat,
) -> error::Result<()> {
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
///
/// [`write_array_to_ipc`](super::write_array_to_ipc) emits one IPC message per
/// chunk, so a chunked array arrives as a sequence of arrays. All of them are
/// collected and reassembled into the original [`ChunkedArray`]; reading only
/// the first message would silently drop every quad past the first chunk.
pub fn array_from_ipc_reader<R: std::io::Read>(reader: R) -> Result<ArrayRef> {
    let ipc_reader = SyncIPCReader::try_new(reader, &super::VORTEX_LIGHT_SESSION)
        .map_err(VortexRdfError::Vortex)?;

    let mut chunks = Vec::new();
    for chunk in ipc_reader {
        chunks.push(chunk.map_err(VortexRdfError::Vortex)?);
    }

    match chunks.len() {
        0 => Err(VortexRdfError::Deserialization(
            "No array in IPC stream".to_string(),
        )),
        // Keep a single-chunk payload as the plain array it was written as.
        1 => Ok(chunks.pop().expect("length checked above")),
        _ => {
            let dtype = chunks[0].dtype().clone();
            Ok(ChunkedArray::try_new(chunks, dtype)
                .map_err(VortexRdfError::Vortex)?
                .into_array())
        }
    }
}

/// Loads a fully in-memory Vortex array from a generic read-at source (e.g. a byte buffer in memory).
#[cfg(feature = "file-io")]
pub async fn load_vortex_file_ref<S: VortexReadAt + 'static>(source: S) -> Result<ArrayRef> {
    let start = Instant::now();

    // 1. Open the source under our file read session context.
    let file = super::VORTEX_SESSION
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
///
/// The layout reader is cached on the file handle: every scan and pruning
/// evaluation over the store shares one reader tree, so zone-map stats tables
/// are read and decoded once and per-expression pruning masks are reused across data access calls.
#[cfg(feature = "file-io")]
pub async fn open_vortex_file<P: AsRef<std::path::Path>>(
    path: P,
) -> Result<vortex_file::VortexFile> {
    super::VORTEX_SESSION
        .open_options()
        .with_layout_reader_cache()
        .open_path(path)
        .await
        .map_err(VortexRdfError::from)
}
