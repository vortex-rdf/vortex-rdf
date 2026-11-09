//! The zone maps of a store file's code columns ([`ZoneMaps`]): one per
//! table ([`FileTable`]) and `(s, p, o, g)` slot, each read from its zone
//! table on first use and kept with the file. Located runs check their rows
//! against them, and a filter of code equalities takes the quad table's
//! zones where every row holds every code from them.

use std::sync::Arc;

use vortex_layout::LayoutRef;
use vortex_layout::segments::SegmentSource;

use crate::error::Result;
use crate::observe::observe;
use crate::zone_stats::zone_map::ColumnZones;

/// A table of a store file whose code columns' zone maps a read may ask for:
/// the quad table, a copy index's child or a reference index's.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FileTable {
    Quads,
    Posg,
    Ospg,
    RefP,
    RefO,
}

impl FileTable {
    /// Every table, in the order [`ZoneMaps`] keeps them.
    pub(crate) const ALL: [FileTable; 5] = [
        FileTable::Quads,
        FileTable::Posg,
        FileTable::Ospg,
        FileTable::RefP,
        FileTable::RefO,
    ];

    /// The index child holding the table, `None` for the quad table.
    pub(crate) fn component(self) -> Option<&'static str> {
        match self {
            FileTable::Quads => None,
            FileTable::Posg => Some("index:posg"),
            FileTable::Ospg => Some("index:ospg"),
            FileTable::RefP => Some("index:ref-p"),
            FileTable::RefO => Some("index:ref-o"),
        }
    }
}

/// The layouts of every table's `(s, p, o, g)` code columns, in
/// [`FileTable::ALL`] order; `None` for a column a table lacks (a reference
/// child holds `g` alone).
pub(crate) type TableColumns = [[Option<LayoutRef>; 4]; 5];

/// The zone maps of a store file's code columns, per table ([`FileTable`])
/// and `(s, p, o, g)` slot, each read from its zone table on first use
/// ([`ColumnZones::read`]): `None` for a column the table lacks or one
/// without exact zone bounds. A shared handle, as split tasks hold it past a
/// borrow of the file. A map is read once however many readers ask for it at
/// once, and a read that fails is not kept, for the next reader to retry.
/// Each map holds two `u64` per zone, and a graph column's recorded values.
#[derive(Clone)]
pub(crate) struct ZoneMaps(Arc<ZoneMapsInner>);

/// What a [`ZoneMaps`] handle shares.
struct ZoneMapsInner {
    cells: [[tokio::sync::OnceCell<Option<Arc<ColumnZones>>>; 4]; 5],
    columns: TableColumns,
    source: Arc<dyn SegmentSource>,
}

impl ZoneMaps {
    /// The maps of `columns`, read through `source` on first use.
    pub(crate) fn new(columns: TableColumns, source: Arc<dyn SegmentSource>) -> Self {
        Self(Arc::new(ZoneMapsInner {
            cells: Default::default(),
            columns,
            source,
        }))
    }

    /// The zone map of `table`'s column at `slot`, read on first use.
    pub(crate) async fn get(
        &self,
        table: FileTable,
        slot: usize,
    ) -> Result<Option<Arc<ColumnZones>>> {
        let inner = &*self.0;
        let at = table as usize;
        inner.cells[at][slot]
            .get_or_try_init(|| async {
                observe!(ZoneMapRead);
                Ok(match &inner.columns[at][slot] {
                    Some(column) => ColumnZones::read(column, &inner.source)
                        .await?
                        .map(Arc::new),
                    None => None,
                })
            })
            .await
            .cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::IndexType;
    use crate::observe::{Event, Recorder};
    use vortex_layout::segments::{SegmentFuture, SegmentId};

    /// A segment source over another whose every read is pending on its
    /// first poll (and wakes its task), then completes as the inner read:
    /// readers joined on one task interleave at each read.
    struct WaitingSource(Arc<dyn SegmentSource>);

    impl SegmentSource for WaitingSource {
        fn request(&self, id: SegmentId) -> SegmentFuture {
            use futures::FutureExt as _;

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

    /// A segment source over another whose first read fails, every later one
    /// the inner source's.
    struct FailingOnce {
        inner: Arc<dyn SegmentSource>,
        failed: std::sync::atomic::AtomicBool,
    }

    impl SegmentSource for FailingOnce {
        fn request(&self, id: SegmentId) -> SegmentFuture {
            use futures::FutureExt as _;

            if !self.failed.swap(true, std::sync::atomic::Ordering::Relaxed) {
                let failed = Err(vortex_error::vortex_err!("a failing segment read"));
                return futures::future::ready(failed).boxed();
            }
            self.inner.request(id)
        }
    }

    /// A zone map is read once however many readers ask for it at once: two
    /// readers of `index:ref-p`'s `g` map, joined on one task over a source
    /// whose every read waits, get the one map, read once.
    #[tokio::test]
    async fn joined_readers_read_a_zone_map_once() {
        let (_dir, native) =
            crate::tests::mapped_file(50, vec![IndexType::SecondaryByReference]).await;
        let source = Arc::new(WaitingSource(native.segment_source()));
        let maps = ZoneMaps::new(native.zone_map_layouts(), source);
        let rec = Recorder::start();
        let (first, second) =
            futures::join!(maps.get(FileTable::RefP, 3), maps.get(FileTable::RefP, 3));
        let first = first.unwrap().expect("index:ref-p's g map");
        let second = second.unwrap().expect("index:ref-p's g map");
        assert!(Arc::ptr_eq(&first, &second), "the one map");
        assert_eq!(rec.count(&Event::ZoneMapRead), 1, "read once");
    }

    /// A zone map whose read fails is not kept: the next reader reads it
    /// again and gets it.
    #[tokio::test]
    async fn a_failed_zone_map_read_is_read_again() {
        let (_dir, native) =
            crate::tests::mapped_file(50, vec![IndexType::SecondaryByReference]).await;
        let source = Arc::new(FailingOnce {
            inner: native.segment_source(),
            failed: std::sync::atomic::AtomicBool::new(false),
        });
        let maps = ZoneMaps::new(native.zone_map_layouts(), source);
        let rec = Recorder::start();
        assert!(
            maps.get(FileTable::RefP, 3).await.is_err(),
            "the failing read"
        );
        assert!(
            maps.get(FileTable::RefP, 3).await.unwrap().is_some(),
            "read again"
        );
        assert_eq!(rec.count(&Event::ZoneMapRead), 2, "two reads");
    }
}
