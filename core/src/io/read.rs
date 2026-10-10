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

/// The non-empty chunks of a stream of per-split results, in stream order.
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

/// [`collect_chunks`] assembled into one array of `dtype`: the tail of
/// [`scan_all`], of `read_index_row_ids` and of `read_all_rows` for a scan
/// within its inline limit.
pub(crate) async fn collect_scan<F>(
    dtype: vortex_array::dtype::DType,
    tasks: Vec<F>,
) -> Result<ArrayRef>
where
    F: Future<Output = VortexResult<Option<ArrayRef>>>,
{
    chunked_or_single(collect_chunks(tasks).await?, dtype)
}

/// [`collect_scan`] with each split future spawned onto the session's runtime,
/// `window` of them in flight. The chunks come back in split order, as the
/// inline driver returns them.
///
/// With no tokio runtime on the calling thread there is nothing to spawn
/// onto: the futures run inline and nothing is spawned.
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

    if tokio::runtime::Handle::try_current().is_err() {
        return collect_scan(dtype, tasks).await;
    }
    let handle = crate::session::VORTEX_SESSION.handle();
    let spawned = futures::stream::iter(tasks).map(move |task| {
        #[cfg(test)]
        spawn_probe::note();
        handle.spawn(task)
    });
    chunked_or_single(drain_chunks(spawned.buffered(window)).await?, dtype)
}

/// Test hook: the number of split futures [`collect_scan_spawned`] has spawned
/// on this thread. Zero means a scan ran inline.
#[cfg(all(test, feature = "file-io"))]
pub(crate) mod spawn_probe {
    use std::cell::Cell;

    thread_local! {
        static SPAWNED: Cell<usize> = const { Cell::new(0) };
    }

    /// Count one spawned split.
    pub(super) fn note() {
        SPAWNED.set(SPAWNED.get() + 1);
    }

    /// The splits spawned on this thread since the last call.
    pub(crate) fn take() -> usize {
        SPAWNED.take()
    }
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

/// The error for bytes Vortex could not open: the one that says a newer
/// vortex-rdf wrote them when their root layout is a store root this version
/// does not know, Vortex's own otherwise.
pub(crate) fn open_failure(
    error: vortex_error::VortexError,
    bytes: &vortex_buffer::ByteBuffer,
) -> VortexRdfError {
    match super::container::newer_root_id(bytes) {
        Some(id) => VortexRdfError::Deserialization(format!(
            "this store was written by a newer vortex-rdf (root layout {id}), which this \
             version cannot read; open it with a newer version of vortex-rdf"
        )),
        None => VortexRdfError::Vortex(error),
    }
}

/// Which file a path named when it was opened: the device and inode on Unix.
/// A store keeps it to tell, before it rewrites its source file, whether the
/// path still names the file it opened. There is no identity elsewhere.
#[cfg(feature = "file-io")]
#[cfg_attr(not(unix), allow(dead_code))]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FileIdentity {
    dev: u64,
    ino: u64,
}

#[cfg(feature = "file-io")]
impl FileIdentity {
    /// The identity of the file `meta` describes, where the platform has one.
    pub(crate) fn of(meta: &std::fs::Metadata) -> Option<Self> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt as _;
            Some(Self {
                dev: meta.dev(),
                ino: meta.ino(),
            })
        }
        #[cfg(not(unix))]
        {
            let _ = meta;
            None
        }
    }
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

/// A store file opened by [`open_vortex_file`], with how its bytes are
/// reached. `mapped` and `identity` describe what the open did, not what was
/// asked for.
#[cfg(feature = "file-io")]
pub(crate) struct OpenedFile {
    pub(crate) file: vortex_file::VortexFile,
    /// Whether the bytes are a memory mapping of the file.
    pub(crate) mapped: bool,
    /// The identity of the mapped file; none where the bytes are not a mapping.
    pub(crate) identity: Option<FileIdentity>,
}

/// A file opened over a buffer: no mapping, no identity.
#[cfg(feature = "file-io")]
impl From<vortex_file::VortexFile> for OpenedFile {
    fn from(file: vortex_file::VortexFile) -> Self {
        Self {
            file,
            mapped: false,
            identity: None,
        }
    }
}

#[cfg(feature = "file-io")]
impl std::ops::Deref for OpenedFile {
    type Target = vortex_file::VortexFile;

    fn deref(&self) -> &Self::Target {
        &self.file
    }
}

/// Open a Vortex file lazily — no data is read until the returned
/// `VortexFile` is scanned. The handle caches no reader: the
/// [`NativeStoreFile`](crate::store::native_file::NativeStoreFile) around it
/// owns the reader tree.
///
/// Both modes reach the file through `File::open` first, so a path that
/// cannot be opened is the same I/O error in either, with the path in its
/// message.
///
/// A mapped file must not be truncated or rewritten in place while open
/// (its pages would fault); replacing it by rename is fine on Unix.
#[cfg(feature = "file-io")]
pub(crate) async fn open_vortex_file<P: AsRef<std::path::Path>>(
    path: P,
    access: FileAccess,
) -> Result<OpenedFile> {
    use crate::error::path_error;
    use vortex_file::OpenOptionsSessionExt;

    let path = path.as_ref();
    let options = crate::session::VORTEX_SESSION.open_options();
    let file = std::fs::File::open(path).map_err(|e| path_error("open", path, e))?;
    match access {
        FileAccess::Mapped => {
            let identity = file.metadata().ok().as_ref().and_then(FileIdentity::of);
            // SAFETY: a store file is read-only while open; truncating or
            // rewriting it in place is unsupported (docs/file-format.md §8).
            let mmap =
                unsafe { memmap2::Mmap::map(&file) }.map_err(|e| path_error("map", path, e))?;
            let bytes = vortex_buffer::ByteBuffer::from(mmap);
            let opened = options
                .open_buffer(bytes.clone())
                .map_err(|e| open_failure(e, &bytes))?;
            Ok(OpenedFile {
                file: opened,
                mapped: true,
                identity,
            })
        }
        FileAccess::Read => {
            drop(file);
            match options.open_path(path).await {
                Ok(opened) => Ok(opened.into()),
                // A mapping reads nothing up front: enough to name a newer
                // store's root.
                Err(error) => Err(
                    match std::fs::File::open(path)
                        // SAFETY: as above; the mapping only lives for the call.
                        .and_then(|file| unsafe { memmap2::Mmap::map(&file) })
                    {
                        Ok(mmap) => open_failure(error, &vortex_buffer::ByteBuffer::from(mmap)),
                        Err(_) => VortexRdfError::Vortex(error),
                    },
                ),
            }
        }
    }
}

#[cfg(all(test, feature = "file-io"))]
mod tests {
    use super::*;
    use vortex_array::arrays::PrimitiveArray;
    use vortex_array::dtype::{DType, Nullability, PType};
    use vortex_array::{IntoArray as _, VortexSessionExecute as _};

    /// The spawned collector returns the chunks in split order when the splits
    /// finish in reverse: split `i` yields to the scheduler `splits - 1 - i`
    /// times, then answers, so on this single-threaded runtime the last split
    /// finishes first.
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
        spawn_probe::take();
        let array = collect_scan_spawned(dtype, tasks, splits as usize)
            .await
            .unwrap();
        assert_eq!(
            spawn_probe::take(),
            splits as usize,
            "every split ran spawned, none inline"
        );

        let mut ctx = crate::session::VORTEX_SESSION.create_execution_ctx();
        let values = array.execute::<PrimitiveArray>(&mut ctx).unwrap();
        assert_eq!(values.as_slice::<u64>(), (0..splits).collect::<Vec<_>>());
    }

    /// A failing split fails the scan: its error comes out of the spawned
    /// collector.
    #[tokio::test]
    async fn spawned_scan_propagates_a_split_error() {
        let splits = 4u64;
        let tasks: Vec<_> = (0..splits)
            .map(|i| async move {
                if i == 2 {
                    return Err(vortex_error::vortex_err!("split {i} failed"));
                }
                Ok::<_, vortex_error::VortexError>(Some(
                    PrimitiveArray::from_iter([i]).into_array(),
                ))
            })
            .collect();
        let dtype = DType::Primitive(PType::U64, Nullability::NonNullable);
        spawn_probe::take();
        let error = collect_scan_spawned(dtype, tasks, splits as usize)
            .await
            .expect_err("the failing split fails the scan");
        assert!(error.to_string().contains("split 2 failed"), "{error}");
        assert_eq!(
            spawn_probe::take(),
            splits as usize,
            "every split ran spawned, none inline"
        );
    }

    /// A panicking split panics the awaiting scan, as it would inline.
    #[tokio::test]
    async fn spawned_scan_re_raises_a_split_panic() {
        use futures::FutureExt as _;

        let splits = 4u64;
        let tasks: Vec<_> = (0..splits)
            .map(|i| async move {
                if i == 2 {
                    panic!("split 2 panicked");
                }
                Ok::<_, vortex_error::VortexError>(Some(
                    PrimitiveArray::from_iter([i]).into_array(),
                ))
            })
            .collect();
        let dtype = DType::Primitive(PType::U64, Nullability::NonNullable);
        spawn_probe::take();
        let outcome =
            std::panic::AssertUnwindSafe(collect_scan_spawned(dtype, tasks, splits as usize))
                .catch_unwind()
                .await;

        let payload = outcome.expect_err("the panic reaches the awaiting scan");
        let message = payload
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| payload.downcast_ref::<String>().map(String::as_str))
            .unwrap_or_default();
        assert!(message.contains("split 2 panicked"), "{message}");
        assert_eq!(
            spawn_probe::take(),
            splits as usize,
            "every split ran spawned, none inline"
        );
    }

    /// Dropping the scan aborts the splits it spawned: a split that never
    /// finishes is dropped with its task, not left running.
    #[tokio::test]
    async fn dropping_the_spawned_scan_cancels_its_splits() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        /// Counts the split futures that are dropped.
        struct Dropped(Arc<AtomicUsize>);

        impl Drop for Dropped {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let splits = 4usize;
        let started = Arc::new(AtomicUsize::new(0));
        let dropped = Arc::new(AtomicUsize::new(0));
        let tasks: Vec<_> = (0..splits)
            .map(|_| {
                let (started, guard) = (started.clone(), Dropped(dropped.clone()));
                async move {
                    let _guard = guard;
                    started.fetch_add(1, Ordering::SeqCst);
                    std::future::pending::<()>().await;
                    Ok::<Option<ArrayRef>, vortex_error::VortexError>(None)
                }
            })
            .collect();
        let dtype = DType::Primitive(PType::U64, Nullability::NonNullable);
        spawn_probe::take();

        tokio::time::timeout(
            std::time::Duration::from_millis(200),
            collect_scan_spawned(dtype, tasks, splits),
        )
        .await
        .expect_err("a scan of splits that never finish is still running when it is dropped");

        assert_eq!(
            started.load(Ordering::SeqCst),
            splits,
            "the splits were running"
        );
        assert_eq!(spawn_probe::take(), splits);
        for _ in 0..100 {
            if dropped.load(Ordering::SeqCst) == splits {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(
            dropped.load(Ordering::SeqCst),
            splits,
            "the dropped scan left its splits running"
        );
    }
}
