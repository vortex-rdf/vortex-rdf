//! The lazy id caches a served view carries: ids handed over unsorted
//! count and narrow without a sort into base order, a run's ids decode once
//! for both orders and every clone, a narrowed residual file view counts
//! without a sort, and a located run's ids decline what they cannot answer by
//! position.

use std::sync::Arc;

use super::*;
use crate::store::indexes::{ComponentRun, LazyRowIds};
use vortex_buffer::Buffer;
use vortex_mask::Mask;

#[tokio::test]
async fn unsorted_ids_sort_only_when_asked() {
    let ids = LazyRowIds::from_unsorted_ids(Buffer::from(vec![5u64, 1, 3]));
    let rec = Recorder::start();
    assert_eq!(ids.len_if_known(), Some(3));
    let deleted = Mask::from_indices(8, vec![1]);
    // Live: the ids already leave every tombstone out, so the mask is not read.
    assert_eq!(ids.live_len_without_io(&deleted).unwrap(), Some(3));
    assert_eq!(ids_sorted(&rec), 0);
    assert_eq!(ids.materialized().unwrap().as_slice(), &[1, 3, 5]);
    ids.materialized().unwrap();
    assert_eq!(rec.count(&Event::IdsSorted(3)), 1, "sorted once");
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
    let ids = LazyRowIds::Component(Arc::new(ComponentRun::new(rids, false)));
    let rec = Recorder::start();
    let run_order = |ids: &LazyRowIds| match ids {
        LazyRowIds::Component(run) => run.run_order().unwrap(),
        _ => unreachable!("an in-memory run"),
    };
    assert_eq!(run_order(&ids).as_slice(), &[5, 1, 3]);
    assert_eq!(run_order(&ids.clone()).as_slice(), &[5, 1, 3]);
    assert_eq!(ids.materialized().unwrap().as_slice(), &[1, 3, 5]);
    assert_eq!(rids_decoded(&rec), 3, "one decode of the run's three rids");
    assert_eq!(ids_sorted(&rec), 1);
}

/// A served view's run ids decode once however many clones stream it (every
/// chunk stream reads a clone of its view), and no stream sorts them: on a
/// built store, whose streams gather the base at the ids. A store read off a
/// file streams off the component and decodes them never.
#[tokio::test]
async fn run_ids_decode_once_across_clones() {
    let built = built_store(
        modular_quads(20_000, 7, 11),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    let (_dir, read) = memory_store(LayoutStrategy::Dictionary).await;
    for (tag, store, decodes) in [("built", built, 1), ("read off a file", read, 0)] {
        let p = matched_view(&store, None, Some(&iri("p2")), None, None).await;
        let rec = Recorder::start();
        for _ in 0..2 {
            collect_code_chunks(&p.clone(), &QuadColumn::ALL, 64).await;
            collect_row_chunks(&p.clone(), 64).await;
        }
        assert_eq!(
            rec.count_where(|e| matches!(e, Event::RidsDecoded(_))),
            decodes,
            "{tag}"
        );
        assert_eq!(ids_sorted(&rec), 0, "{tag}");
    }
}

/// A served file view that keeps a residual term as a filter beside its plan
/// (`?p2?g`, the graph a residual) narrows to rows that all pass it, so its
/// window, keep, partition and chained match carry it resolved: their counts
/// take the narrowed rows' width, sorting no ids, and their statistics are
/// exact, with no filter pending.
#[tokio::test]
async fn narrowed_residual_file_views_count_without_sorting() {
    // The default graph is a residual that keeps rows of the predicate's
    // run, not all of them.
    let quads = graphed_quads(5_000);
    let (_dir, path) = write_store_file(
        quads,
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let (p2, default) = (iri("p2"), GraphName::DefaultGraph);
    let view = matched_view(&store, None, Some(&p2), None, Some(&default)).await;
    assert!(served_component(&view).is_some(), "served");
    let rows = row_list(&view).await;
    let n = rows.len();
    assert!(n > 100, "a run to narrow");
    let window = view.window(10, 100).await.unwrap();
    let kept = view
        .keep(
            QuadColumn::O,
            &Keep::set(objects_of(&rows).into_iter().step_by(2)),
        )
        .await
        .unwrap();
    let part = view
        .partitions(std::num::NonZeroUsize::new(3).unwrap())
        .await
        .unwrap()
        .swap_remove(1);
    // An object the window holds.
    let object = window.quads_vec().await.unwrap()[50].object.clone();
    let chained = window
        .match_pattern(None, None, Some(&object), None)
        .await
        .unwrap();
    for (tag, narrowed) in [
        ("window", window),
        ("keep", kept),
        ("partition", part),
        ("chain", chained),
    ] {
        assert!(served_component(&narrowed).is_some(), "{tag}: served");
        let want = row_list(&narrowed).await.len();
        assert!(want > 0, "{tag}: rows to count");
        let stats = narrowed.view_statistics().unwrap();
        assert!(!stats.pending_filter, "{tag}: no filter pending");
        assert_eq!(stats.rows.exact, Some(want), "{tag}: an exact count");
        let rec = Recorder::start();
        assert_eq!(narrowed.size().await.unwrap(), want, "{tag}: size");
        assert_eq!(
            narrowed.size_capped(want / 2).await.unwrap(),
            want / 2,
            "{tag}: size_capped"
        );
        assert!(narrowed.exists().await.unwrap(), "{tag}: exists");
        assert_eq!(ids_sorted(&rec), 0, "{tag}: the counts sort no ids");
    }
}

/// The lazy ids of a served file window over a located range read the copy
/// child's rid column on first need. Its rids follow the index's order, not
/// row order, so the ids decline a window by position (which reads a run's
/// rids as already in row order): a window of the window cuts the ids it
/// holds instead.
#[tokio::test]
async fn served_located_ids_decline_a_window_by_position() {
    let (_dir, store) = file_store().await;
    let (p, want) = p2_view(&store).await;
    let w = p.window(10, 100).await.unwrap();
    assert_eq!(w.debug_pending_window(0, 10).await.unwrap(), None);
    assert_eq!(
        row_list(&w.window(5, 50).await.unwrap()).await,
        want[15..65]
    );
}
