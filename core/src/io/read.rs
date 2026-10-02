//! Read-side access to native store files: opening them, requiring the
//! native root, and materializing whole files or auxiliary children on any
//! target. The opened-store runtime handle the store's file-backed query
//! paths drive lives store-side, in
//! [`store::native_file`](crate::store::persist::native_file), beside the scan
//! machinery that defines its memo semantics; the wire format itself (layout
//! VTable, descriptors, write strategy) lives in
//! [`container`](super::container).

use std::future::Future;

use futures::StreamExt as _;
use vortex_array::ArrayRef;
use vortex_error::VortexResult;

use crate::error::{Result, VortexRdfError};
use crate::store::array::chunked_or_single;

/// The host's available parallelism, read once; 1 on targets that cannot
/// answer (wasm). Read by [`collect_chunks`] for its in-flight split count and
/// exposed through [`available_parallelism`] to the store's scan and serve
/// split loops.
static AVAILABLE_PARALLELISM: std::sync::LazyLock<usize> = std::sync::LazyLock::new(|| {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
});

/// The host's available parallelism — how many workers a scan's per-split
/// tasks can spread over.
#[cfg(feature = "file-io")]
pub(crate) fn available_parallelism() -> usize {
    *AVAILABLE_PARALLELISM
}

/// Drive a scan's per-split futures inline and collect the chunks in split
/// (row) order — the shared tail of [`scan_all`], [`scan_all_reader`] and
/// [`scan_reader_chunks`].
///
/// The futures are polled through a `buffered` window — several in flight on
/// this same task, still no runtime handle — so one split's I/O overlaps
/// another's decode.
async fn collect_chunks<F>(tasks: Vec<F>) -> Result<Vec<ArrayRef>>
where
    F: Future<Output = VortexResult<Option<ArrayRef>>>,
{
    let mut results = futures::stream::iter(tasks).buffered(*AVAILABLE_PARALLELISM);
    let mut chunks = Vec::new();
    while let Some(chunk) = results.next().await {
        if let Some(chunk) = chunk.map_err(VortexRdfError::Vortex)? {
            chunks.push(chunk);
        }
    }
    Ok(chunks)
}

/// [`collect_chunks`] assembled into one array of `dtype`.
async fn collect_scan<F>(dtype: vortex_array::dtype::DType, tasks: Vec<F>) -> Result<ArrayRef>
where
    F: Future<Output = VortexResult<Option<ArrayRef>>>,
{
    chunked_or_single(collect_chunks(tasks).await?, dtype)
}

/// Materialize a whole file by driving its scan's per-split futures inline.
///
/// `ScanBuilder::into_array_stream` spawns onto the session's runtime handle;
/// this drives `ScanBuilder::build`'s futures directly instead, so it needs no
/// handle at all — which is what lets buffer-backed files (whose segment reads
/// resolve synchronously) be read on wasm and in no-file-io builds.
pub(crate) async fn scan_all(file: &vortex_file::VortexFile) -> Result<ArrayRef> {
    let dtype = file.dtype().clone();
    let scan = file.scan().map_err(VortexRdfError::Vortex)?;
    let tasks = scan.build().map_err(VortexRdfError::Vortex)?;
    collect_scan(dtype, tasks).await
}

/// [`scan_all`] over an arbitrary layout reader — how the native store root's
/// auxiliary children (dictionary, index copies) are materialized from a
/// buffer-backed file on every target, runtime handle included or not.
pub(crate) async fn scan_all_reader(reader: vortex_layout::LayoutReaderRef) -> Result<ArrayRef> {
    let dtype = reader.dtype().clone();
    chunked_or_single(scan_reader_chunks(reader).await?, dtype)
}

/// [`scan_all_reader`] before assembly: the reader's scan chunks in split
/// (row) order, each in its stored encoding.
pub(crate) async fn scan_reader_chunks(
    reader: vortex_layout::LayoutReaderRef,
) -> Result<Vec<ArrayRef>> {
    let scan = vortex_layout::scan::scan_builder::ScanBuilder::new(
        crate::session::VORTEX_SESSION.clone(),
        reader,
    );
    let tasks = scan.build().map_err(VortexRdfError::Vortex)?;
    collect_chunks(tasks).await
}

/// The actionable error for a file whose root is not the native store layout.
pub(crate) fn unsupported_file_error(file: &vortex_file::VortexFile) -> VortexRdfError {
    VortexRdfError::Deserialization(format!(
        "not a vortex-rdf store file: expected the {} root layout, found {}",
        super::container::STORE_LAYOUT_ID,
        file.footer().layout().encoding_id()
    ))
}

/// Open a Vortex file lazily — no data is read until the returned `VortexFile`
/// is scanned.
///
/// The layout reader is cached on the file handle: every scan and pruning
/// evaluation over the store shares one reader tree, so zone-map stats tables
/// are read and decoded once and per-expression pruning masks are reused across data access calls.
#[cfg(feature = "file-io")]
pub(crate) async fn open_vortex_file<P: AsRef<std::path::Path>>(
    path: P,
) -> Result<vortex_file::VortexFile> {
    use vortex_file::OpenOptionsSessionExt;
    crate::session::VORTEX_SESSION
        .open_options()
        .with_layout_reader_cache()
        .open_path(path)
        .await
        .map_err(VortexRdfError::Vortex)
}
