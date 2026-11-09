use super::*;
use crate::store::persist::native_file::NativeStoreFile;
use futures::FutureExt as _;
use vortex_array::arrays::VarBinViewArray;
use vortex_layout::segments::{SegmentFuture, SegmentId};

/// A segment source over another whose every read is pending on its
/// first poll (and wakes its task), then completes as the inner read.
pub(super) struct WaitingSource(pub(super) Arc<dyn SegmentSource>);

impl SegmentSource for WaitingSource {
    fn request(&self, id: SegmentId) -> SegmentFuture {
        let read = self.0.request(id);
        let mut waited = false;
        let wait = futures::future::poll_fn(move |cx| {
            if waited {
                std::task::Poll::Ready(())
            } else {
                waited = true;
                cx.waker().wake_by_ref();
                std::task::Poll::Pending
            }
        });
        async move {
            wait.await;
            read.await
        }
        .boxed()
    }
}

/// `terms` (sorted, unique) compressed in windows of `window` terms and
/// written as a store's dictionary child: the opened file and the
/// resident dictionary they were written from.
pub(super) async fn native_over(
    terms: &[String],
    window: usize,
) -> (NativeStoreFile, TermDictionary) {
    let plain = VarBinViewArray::from_iter_str(terms.iter().map(String::as_str));
    let d = TermDictionary::compress_windowed(plain, window).unwrap();
    let native = crate::tests::open_native_bytes(crate::tests::write_dict_only_store(&d).await);
    (native, d)
}

/// `n` IRIs compressed in windows of `window` terms and written as a
/// store's dictionary child: the opened file, the resident dictionary
/// they were written from, and the terms.
pub(super) async fn windowed_native(
    n: usize,
    window: usize,
) -> (NativeStoreFile, TermDictionary, Vec<String>) {
    let terms: Vec<String> = (0..n)
        .map(|i| format!("<http://example.org/term/{i:04}>"))
        .collect();
    let (native, d) = native_over(&terms, window).await;
    (native, d, terms)
}

/// [`windowed_native`] opened file-backed.
pub(super) async fn windowed_handle(n: usize, window: usize) -> (FileBackedDict, Vec<String>) {
    let (native, _, terms) = windowed_native(n, window).await;
    let fbd = FileBackedDict::open(&native)
        .await
        .unwrap()
        .expect("the written dictionary child must be window-searchable");
    (fbd, terms)
}
