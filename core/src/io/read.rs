//! Read-side access to native store files: the open errors, and the scan
//! drivers that run a scan's splits inline or spawned and materialize a whole
//! reader, on any target. Opening a file at a path, mapped or read, is
//! `file`'s, behind `file-io`.

use std::future::Future;

use futures::StreamExt as _;
use vortex_array::ArrayRef;
use vortex_error::VortexResult;
use vortex_layout::scan::scan_builder::ScanBuilder;

use crate::error::{Result, VortexRdfError};
use crate::store::array::chunked_or_single;

/// The host's available parallelism, read once; 1 where the target cannot
/// answer (wasm). The in-flight split count of [`inline_splits`] and of the
/// store's scan and serve split loops ([`available_parallelism`]).
static AVAILABLE_PARALLELISM: std::sync::LazyLock<usize> = std::sync::LazyLock::new(|| {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
});

/// The host's available parallelism: how many workers a scan's per-split
/// tasks can spread over.
#[cfg(feature = "file-io")]
pub(crate) fn available_parallelism() -> usize {
    *AVAILABLE_PARALLELISM
}

/// Per-split futures (a scan's, or any other split loop's) driven inline:
/// polled on the calling task, a `buffered` window of the host's parallelism
/// in flight (so one split's I/O overlaps another's decode, with no runtime
/// handle), their results in split (row) order.
pub(crate) fn inline_splits<F, T>(tasks: Vec<F>) -> impl futures::Stream<Item = T> + Unpin
where
    F: Future<Output = T>,
{
    futures::stream::iter(tasks).buffered(*AVAILABLE_PARALLELISM)
}

/// Drive a scan's per-split futures inline ([`inline_splits`]) and collect
/// the chunks in split (row) order.
async fn collect_chunks<F>(tasks: Vec<F>) -> Result<Vec<ArrayRef>>
where
    F: Future<Output = VortexResult<Option<ArrayRef>>>,
{
    drain_chunks(inline_splits(tasks)).await
}

/// The non-empty chunks of a stream of per-split results, in stream order.
async fn drain_chunks<S>(mut results: S) -> Result<Vec<ArrayRef>>
where
    S: futures::Stream<Item = VortexResult<Option<ArrayRef>>> + Unpin,
{
    let mut chunks = Vec::new();
    while let Some(chunk) = results.next().await {
        if let Some(chunk) = chunk? {
            chunks.push(chunk);
        }
    }
    Ok(chunks)
}

/// [`collect_chunks`] assembled into one array of `dtype`.
pub(crate) async fn collect_scan<F>(
    dtype: vortex_array::dtype::DType,
    tasks: Vec<F>,
) -> Result<ArrayRef>
where
    F: Future<Output = VortexResult<Option<ArrayRef>>>,
{
    chunked_or_single(collect_chunks(tasks).await?, dtype)
}

/// `scan` planned and run to completion inline ([`collect_scan`]) into one
/// array.
pub(crate) async fn collect_whole_scan(scan: ScanBuilder<ArrayRef>) -> Result<ArrayRef> {
    let dtype = scan.dtype()?;
    collect_scan(dtype, scan.build()?).await
}

/// Per-split futures (a scan's, or any other split loop's) each spawned onto
/// the session's runtime as the stream is polled, `window` of them in flight,
/// their results in split order as [`inline_splits`] yields them. Dropping
/// the stream aborts the spawned splits it still holds. Spawning needs a
/// tokio runtime on the polling thread: [`ScanDriver::for_row_scan`] picks
/// this driver only where there is one.
///
/// [`ScanDriver::for_row_scan`]: crate::store::scan::file_scans::ScanDriver::for_row_scan
#[cfg(feature = "file-io")]
pub(crate) fn spawned_splits<F, T>(
    tasks: Vec<F>,
    window: usize,
) -> impl futures::Stream<Item = T> + Unpin + Send + 'static
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    use vortex_io::session::RuntimeSessionExt as _;

    let handle = crate::session::VORTEX_SESSION.handle();
    futures::stream::iter(tasks)
        .map(move |task| {
            #[cfg(test)]
            spawn_probe::note();
            handle.spawn(task)
        })
        .buffered(window)
}

/// [`collect_scan`] with each split future spawned onto the session's runtime
/// ([`spawned_splits`]), `window` of them in flight. The chunks come back in
/// split order, as the inline driver returns them. Needs a tokio runtime on
/// the polling thread, as [`spawned_splits`] does.
#[cfg(feature = "file-io")]
pub(crate) async fn collect_scan_spawned<F>(
    dtype: vortex_array::dtype::DType,
    tasks: Vec<F>,
    window: usize,
) -> Result<ArrayRef>
where
    F: Future<Output = VortexResult<Option<ArrayRef>>> + Send + 'static,
{
    chunked_or_single(drain_chunks(spawned_splits(tasks, window)).await?, dtype)
}

/// Test hook: the number of split futures [`spawned_splits`] has spawned on
/// this thread. Zero means a scan ran inline.
#[cfg(test)]
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

/// Materialize a whole layout reader (a file's root, or one of the store
/// root's auxiliary children) by driving its scan's per-split futures inline.
/// It needs no runtime handle, which is what lets a buffer-backed file (whose
/// segment reads resolve synchronously) be read on wasm.
pub(crate) async fn scan_all_reader(reader: vortex_layout::LayoutReaderRef) -> Result<ArrayRef> {
    collect_whole_scan(reader_scan(reader)).await
}

/// [`scan_all_reader`] completed on its first poll, for a reader that must sit
/// over a buffer-backed segment source: its segment reads resolve
/// synchronously, the invariant every handle-free `from_bytes` open relies on.
pub(crate) fn scan_all_reader_sync(reader: vortex_layout::LayoutReaderRef) -> Result<ArrayRef> {
    use futures::FutureExt as _;
    scan_all_reader(reader).now_or_never().unwrap_or_else(|| {
        unreachable!(
            "a synchronous scan only ever reads a buffer-backed segment source, whose reads \
             resolve without pending"
        )
    })
}

/// [`scan_all_reader`] before assembly: the reader's scan chunks in split
/// (row) order, each in its stored encoding.
pub(crate) async fn scan_reader_chunks(
    reader: vortex_layout::LayoutReaderRef,
) -> Result<Vec<ArrayRef>> {
    collect_chunks(reader_scan(reader).build()?).await
}

/// A scan of every row and column of `reader`, on the crate's session.
fn reader_scan(reader: vortex_layout::LayoutReaderRef) -> ScanBuilder<ArrayRef> {
    ScanBuilder::new(crate::session::VORTEX_SESSION.clone(), reader)
}

/// The error for a file whose root is not the native store layout: the layout
/// it expected and the one it found.
pub(crate) fn unsupported_file_error(file: &vortex_file::VortexFile) -> VortexRdfError {
    VortexRdfError::Deserialization(format!(
        "not a vortex-rdf store file: expected the {} root layout, found {}",
        super::container::STORE_LAYOUT_ID,
        file.footer().layout().encoding_id()
    ))
}

/// The error for a file Vortex could not open: the one that says a newer
/// vortex-rdf wrote it when `newer_root` names its root layout
/// ([`newer_root_id`](super::container::newer_root_id)), Vortex's own
/// otherwise.
pub(crate) fn open_failure(
    error: vortex_error::VortexError,
    newer_root: Option<String>,
) -> VortexRdfError {
    match newer_root {
        Some(id) => VortexRdfError::Deserialization(format!(
            "this store was written by a newer vortex-rdf (root layout {id}), which this \
             version cannot read; open it with a newer version of vortex-rdf"
        )),
        None => VortexRdfError::Vortex(error),
    }
}

#[cfg(test)]
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
