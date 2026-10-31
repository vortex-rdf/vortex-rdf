//! Views a by-copy index serves read straight off the index, in its order,
//! and the views cut from them stay served. Underneath: the lazy id caches a
//! served view carries, which count and narrow a run without sorting its ids
//! into base order.

use super::*;
use crate::store::indexes::LazyRowIds;
use vortex_buffer::Buffer;
use vortex_mask::Mask;

/// How many times `rec` saw a pending selection's ids sorted into base order.
fn ids_sorted(rec: &Recorder) -> usize {
    rec.count_where(|e| matches!(e, Event::IdsSorted(_)))
}

#[tokio::test]
async fn unsorted_ids_sort_only_when_asked() {
    let ids = LazyRowIds::from_unsorted_ids(Buffer::from(vec![5u64, 1, 3]), true);
    let mut rec = Recorder::start();
    assert_eq!(ids.len_if_known(), Some(3));
    let deleted = Mask::from_indices(8, vec![1]);
    // Live: the ids already leave every tombstone out, so the mask is not read.
    assert_eq!(ids.live_len_without_sort(&deleted).unwrap(), Some(3));
    assert_eq!(
        rec.take_picked(|e| matches!(e, Event::IdsSorted(_)).then_some(()))
            .len(),
        0
    );
    assert_eq!(ids.materialized().unwrap().as_slice(), &[1, 3, 5]);
    ids.materialized().unwrap();
    assert_eq!(
        rec.picked(|e| matches!(e, Event::IdsSorted(3)).then_some(()))
            .len(),
        1
    );
}

#[tokio::test]
async fn unsorted_ids_not_known_live_count_against_the_tombstones() {
    let ids = LazyRowIds::from_unsorted_ids(Buffer::from(vec![5u64, 1, 3]), false);
    let rec = Recorder::start();
    let deleted = Mask::from_indices(8, vec![1, 6]);
    assert_eq!(ids.live_len_without_sort(&deleted).unwrap(), Some(2));
    assert_eq!(ids_sorted(&rec), 0);
}

/// Under tombstones an in-memory served run is counted off its ids in the
/// index's order, never sorted into base order, and counts what an unindexed
/// twin counts: the tombstones fall both outside the run (`?p0??`) and inside
/// it (one object's rows).
#[tokio::test]
async fn pending_counts_without_sort_under_tombstones() {
    let quads = modular_quads(2_000, 7, 11);
    let (p0, p2, doomed) = (iri("p0"), iri("p2"), plain_literal("object 3"));
    let mut stores = Vec::new();
    for indexes in [vec![IndexType::SecondaryByCopy], vec![]] {
        let store = built_store(quads.clone(), LayoutStrategy::Dictionary, indexes).await;
        let store = store
            .delete_matching(None, Some(&p0), None, None)
            .await
            .unwrap();
        let store = store
            .delete_matching(None, None, Some(&doomed), None)
            .await
            .unwrap();
        stores.push(store);
    }
    let want = matched_size(&stores[1], None, Some(&p2), None, None).await;
    assert!(
        want > 0 && want < 2_000 / 7,
        "tombstones fall inside the run"
    );
    let view = matched_view(&stores[0], None, Some(&p2), None, None).await;
    assert_eq!(
        view.debug_row_ids_materialized(),
        Some(false),
        "a served run"
    );
    let rec = Recorder::start();
    assert_eq!(view.size().await.unwrap(), want);
    assert_eq!(ids_sorted(&rec), 0, "the count sorted no ids");
    assert_eq!(view.debug_row_ids_materialized(), Some(false));
}

/// An in-memory run's ids decode once, in the component's order, however
/// many clones ask; sorting them into base order afterwards sorts that
/// decode rather than decoding the rids again.
#[tokio::test]
async fn a_run_decodes_once_for_both_orders() {
    use vortex_array::IntoArray;
    use vortex_array::arrays::PrimitiveArray;

    let rids = PrimitiveArray::from_iter([5u64, 1, 3]).into_array();
    let ids = LazyRowIds::from_component_run(rids, false);
    let rec = Recorder::start();
    let run_order = |ids: &LazyRowIds| ids.run_order_ids().unwrap().unwrap();
    assert_eq!(run_order(&ids).as_slice(), &[5, 1, 3]);
    assert_eq!(run_order(&ids.clone()).as_slice(), &[5, 1, 3]);
    assert_eq!(ids.materialized().unwrap().as_slice(), &[1, 3, 5]);
    assert_eq!(rids_decoded(&rec), 3, "one decode of the run's three rids");
    assert_eq!(ids_sorted(&rec), 1);
}
