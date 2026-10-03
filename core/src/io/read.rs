//! Read-side access to native store files: opening them, requiring the
//! native root, and materializing whole files or auxiliary children on any
//! target.

use std::future::Future;

use futures::StreamExt as _;
use vortex_array::ArrayRef;
use vortex_error::VortexResult;

use crate::error::{Result, VortexRdfError};
use crate::store::array::chunked_or_single;

/// The host's available parallelism, read once; 1 where the host cannot
/// answer (wasm).
static AVAILABLE_PARALLELISM: std::sync::LazyLock<usize> = std::sync::LazyLock::new(|| {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
});

/// The host's available parallelism: how many per-split tasks a scan keeps
/// in flight.
#[cfg(feature = "file-io")]
pub(crate) fn available_parallelism() -> usize {
    *AVAILABLE_PARALLELISM
}

/// A scan's per-split futures driven inline, `AVAILABLE_PARALLELISM` of them
/// in flight, the chunks collected in split (row) order; no runtime handle
/// is needed.
async fn collect_chunks<F>(tasks: Vec<F>) -> Result<Vec<ArrayRef>>
where
    F: Future<Output = VortexResult<Option<ArrayRef>>>,
{
    let mut results = futures::stream::iter(tasks).buffered(*AVAILABLE_PARALLELISM);
    let mut chunks = Vec::new();
    while let Some(chunk) = results.next().await {
        if let Some(chunk) = chunk? {
            chunks.push(chunk);
        }
    }
    Ok(chunks)
}

/// A whole file materialized by driving its scan's per-split futures inline,
/// so a buffer-backed file reads on every target.
pub(crate) async fn scan_all(file: &vortex_file::VortexFile) -> Result<ArrayRef> {
    let dtype = file.dtype().clone();
    let tasks = file.scan()?.build()?;
    chunked_or_single(collect_chunks(tasks).await?, dtype)
}

/// [`scan_all`] over a layout reader (an auxiliary child of a native root).
pub(crate) async fn scan_all_reader(reader: vortex_layout::LayoutReaderRef) -> Result<ArrayRef> {
    let dtype = reader.dtype().clone();
    chunked_or_single(scan_reader_chunks(reader).await?, dtype)
}

/// [`scan_all_reader`] driven to completion on the current thread. The
/// reader MUST sit over a buffer-backed segment source, whose reads resolve
/// without pending.
pub(crate) fn scan_all_reader_sync(reader: vortex_layout::LayoutReaderRef) -> Result<ArrayRef> {
    use futures::FutureExt as _;
    scan_all_reader(reader).now_or_never().unwrap_or_else(|| {
        unreachable!("a buffer-backed segment source resolves its reads synchronously")
    })
}

/// The reader's scan chunks in split (row) order, each in its stored
/// encoding.
pub(crate) async fn scan_reader_chunks(
    reader: vortex_layout::LayoutReaderRef,
) -> Result<Vec<ArrayRef>> {
    let scan = vortex_layout::scan::scan_builder::ScanBuilder::new(
        crate::session::VORTEX_SESSION.clone(),
        reader,
    );
    collect_chunks(scan.build()?).await
}

/// The error for a file whose root is not the native store layout.
pub(crate) fn unsupported_file_error(file: &vortex_file::VortexFile) -> VortexRdfError {
    VortexRdfError::Deserialization(format!(
        "not a vortex-rdf store file: expected the {} root layout, found {}",
        super::container::STORE_LAYOUT_ID,
        file.footer().layout().encoding_id()
    ))
}

/// Open a Vortex file lazily, its layout reader cached on the handle so
/// every scan and pruning evaluation over the file shares one reader tree.
#[cfg(feature = "file-io")]
pub(crate) async fn open_vortex_file<P: AsRef<std::path::Path>>(
    path: P,
) -> Result<vortex_file::VortexFile> {
    use vortex_file::OpenOptionsSessionExt;
    Ok(crate::session::VORTEX_SESSION
        .open_options()
        .with_layout_reader_cache()
        .open_path(path)
        .await?)
}
