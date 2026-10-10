//! Read-side access to native store files: opening them, requiring the
//! native root, and materializing whole files or auxiliary children on any
//! target. The opened-store runtime handle the store's file-backed query
//! paths drive lives store-side, in
//! [`store::native_file`](crate::store::native_file), beside the scan
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
    drain_chunks(futures::stream::iter(tasks).buffered(*AVAILABLE_PARALLELISM)).await
}

/// The non-empty chunks of a stream of per-split results, in the order the
/// stream yields them — the loop `collect_chunks` and `collect_scan_spawned`
/// share.
async fn drain_chunks<S>(mut results: S) -> Result<Vec<ArrayRef>>
where
    S: futures::Stream<Item = VortexResult<Option<ArrayRef>>> + Unpin,
{
    let mut chunks = Vec::new();
    while let Some(chunk) = results.next().await {
        if let Some(chunk) = chunk.map_err(VortexRdfError::Vortex)? {
            chunks.push(chunk);
        }
    }
    Ok(chunks)
}

/// [`collect_chunks`] assembled into one array of `dtype` — the tail of
/// [`scan_all`] and of the store's `read_all_rows`, which hands it the futures
/// of a restricted file scan.
pub(crate) async fn collect_scan<F>(
    dtype: vortex_array::dtype::DType,
    tasks: Vec<F>,
) -> Result<ArrayRef>
where
    F: Future<Output = VortexResult<Option<ArrayRef>>>,
{
    chunked_or_single(collect_chunks(tasks).await?, dtype)
}

/// [`collect_scan`] with the split futures spawned onto the session's runtime
/// handle, for a scan large enough to need the workers: `window` futures are
/// in flight, each spawned as the window reaches it, and the chunks come back
/// in split order, as the inline driver returns them, so the two drivers give
/// the same rows whatever the scan's `ordered` flag says.
///
/// The windowing is that of `ScanBuilder::into_array_stream` — one spawn per
/// split, `buffered` over the spawned handles — applied to futures already
/// planned (`ScanBuilder::build`), which is what lets the caller count the
/// splits before choosing a driver.
#[cfg(feature = "file-io")]
pub(crate) async fn collect_scan_spawned<F>(
    dtype: vortex_array::dtype::DType,
    tasks: Vec<F>,
    window: usize,
) -> Result<ArrayRef>
where
    F: Future<Output = VortexResult<Option<ArrayRef>>> + Send + 'static,
{
    use vortex_io::session::RuntimeSessionExt as _;

    let handle = crate::session::VORTEX_SESSION.handle();
    let spawned = futures::stream::iter(tasks).map(move |task| handle.spawn(task));
    chunked_or_single(drain_chunks(spawned.buffered(window)).await?, dtype)
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

/// The actionable error for a file whose root is not the native store layout:
/// for the root of vortex-rdf 0.11 and earlier, what the file is and how to
/// rebuild it ([`legacy_store_message`](super::container::legacy_store_message));
/// for anything else, the layout it expected and the one it found.
pub(crate) fn unsupported_file_error(file: &vortex_file::VortexFile) -> VortexRdfError {
    if super::container::is_legacy_file(file) {
        return VortexRdfError::Deserialization(super::container::legacy_store_message());
    }
    VortexRdfError::Deserialization(format!(
        "not a vortex-rdf store file: expected the {} root layout, found {}",
        super::container::STORE_LAYOUT_ID,
        file.footer().layout().encoding_id()
    ))
}

/// How an opened store file's bytes are reached.
#[cfg(feature = "file-io")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FileAccess {
    /// Memory-mapped (`memmap2`) and opened over the mapping with
    /// `open_buffer`: a segment fetch is a slice of the map, and what stays
    /// in RAM is the kernel's page cache, not this process's heap.
    Mapped,
    /// Read through Vortex's file reader (`open_path`): every segment fetch
    /// is a positioned read into a buffer the caller then owns — the path a
    /// whole-store load takes, so the loaded store owns its memory.
    Read,
}

/// Open a Vortex file lazily — no data is read until the returned
/// `VortexFile` is scanned. The layout reader is cached on the handle: every
/// scan and pruning evaluation shares one reader tree.
///
/// A mapped file must not be truncated or rewritten in place while open
/// (its pages would fault); replacing it by rename is fine on Unix.
#[cfg(feature = "file-io")]
pub(crate) async fn open_vortex_file<P: AsRef<std::path::Path>>(
    path: P,
    access: FileAccess,
) -> Result<vortex_file::VortexFile> {
    use vortex_file::OpenOptionsSessionExt;
    let options = crate::session::VORTEX_SESSION
        .open_options()
        .with_layout_reader_cache();
    match access {
        FileAccess::Mapped => {
            let file = std::fs::File::open(path.as_ref())?;
            // SAFETY: a store file is read-only while open; truncating or
            // rewriting it in place is unsupported (docs/file-format.md §8).
            let mmap = unsafe { memmap2::Mmap::map(&file) }?;
            options
                .open_buffer(vortex_buffer::ByteBuffer::from(mmap))
                .map_err(VortexRdfError::Vortex)
        }
        FileAccess::Read => options
            .open_path(path)
            .await
            .map_err(VortexRdfError::Vortex),
    }
}

#[cfg(all(test, feature = "file-io"))]
mod tests {
    use super::*;
    use vortex_array::arrays::PrimitiveArray;
    use vortex_array::dtype::{DType, Nullability, PType};
    use vortex_array::{IntoArray as _, VortexSessionExecute as _};

    /// The spawned collector returns the chunks in split order even when the
    /// splits finish in the opposite order: split `i` yields to the scheduler
    /// `splits - 1 - i` times before it answers, so on this single-threaded
    /// runtime the last split finishes first and the first split last.
    #[tokio::test]
    async fn spawned_scan_collects_in_split_order_whatever_the_finishing_order() {
        let splits = 6u64;
        let tasks: Vec<_> = (0..splits)
            .map(|i| async move {
                for _ in 0..splits - 1 - i {
                    tokio::task::yield_now().await;
                }
                Ok::<_, vortex_error::VortexError>(Some(
                    PrimitiveArray::from_iter([i]).into_array(),
                ))
            })
            .collect();
        let dtype = DType::Primitive(PType::U64, Nullability::NonNullable);
        let array = collect_scan_spawned(dtype, tasks, splits as usize)
            .await
            .unwrap();

        let mut ctx = crate::session::VORTEX_SESSION.create_execution_ctx();
        let values = array.execute::<PrimitiveArray>(&mut ctx).unwrap();
        assert_eq!(values.as_slice::<u64>(), (0..splits).collect::<Vec<_>>());
    }
}
