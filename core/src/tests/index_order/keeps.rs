//! Keeps of a served view filter its rows in the index's order and stay
//! served, and compose with windows, partitions and each other.

use super::*;

/// A keep of a served view filters its rows in the index's order and stays
/// served: its count sorts no ids, its streams and quads read the kept rows
/// in order, a window of it slices them and a keep of a window filters the
/// window; under tombstones inside the run it keeps live rows only. In
/// memory and on file.
#[tokio::test]
async fn keeps_filter_index_order() {
    for (residency, _dir, store) in served_stores().await {
        let (p, want) = p2_view(&store).await;
        let sparse = Keep::set(objects_of(&want).into_iter().step_by(3));
        let filtered: Vec<_> = want
            .iter()
            .copied()
            .filter(|r| sparse.admits(r[2]))
            .collect();
        assert!(
            !filtered.is_empty() && filtered.len() < want.len(),
            "{residency}"
        );
        let kept = p.keep(QuadColumn::O, &sparse).await.unwrap();
        assert_eq!(row_list(&kept).await, filtered, "{residency}");
        assert_eq!(served_component(&kept), Some("index:posg"), "{residency}");
        let rec = Recorder::start();
        assert_eq!(kept.size().await.unwrap(), filtered.len(), "{residency}");
        assert_eq!(
            ids_sorted(&rec),
            0,
            "{residency}: a kept view counts without a sort"
        );
        assert_eq!(
            collect_code_chunks(&kept, &QuadColumn::ALL, 64).await.0,
            filtered,
            "{residency}: chunks"
        );
        let window = kept.window(2, 7).await.unwrap();
        assert_eq!(row_list(&window).await, filtered[2..9], "{residency}");
        assert_eq!(served_component(&window), Some("index:posg"), "{residency}");
        let window_then_keep = p
            .window(5, 300)
            .await
            .unwrap()
            .keep(QuadColumn::O, &sparse)
            .await
            .unwrap();
        let want_window: Vec<_> = want[5..305]
            .iter()
            .copied()
            .filter(|r| sparse.admits(r[2]))
            .collect();
        assert_eq!(
            row_list(&window_then_keep).await,
            want_window,
            "{residency}: a keep of a window"
        );
        // The view's quads are in its rows' order, so the kept quads are the
        // ones at the kept rows' places.
        let quads = p.quads_vec().await.unwrap();
        let want_quads: Vec<Quad> = want
            .iter()
            .zip(&quads)
            .filter(|(row, _)| sparse.admits(row[2]))
            .map(|(_, quad)| quad.clone())
            .collect();
        assert_eq!(
            kept.quads_vec().await.unwrap(),
            want_quads,
            "{residency}: the kept quads, in order"
        );

        let tombstoned = store
            .delete_matching(None, None, Some(&plain_literal("object 3")), None)
            .await
            .unwrap();
        let (p, want) = p2_view(&tombstoned).await;
        let half = Keep::set(objects_of(&want).into_iter().step_by(2));
        let filtered: Vec<_> = want.iter().copied().filter(|r| half.admits(r[2])).collect();
        let kept = p.keep(QuadColumn::O, &half).await.unwrap();
        assert_eq!(row_list(&kept).await, filtered, "{residency}: tombstoned");
        assert_eq!(
            kept.size().await.unwrap(),
            filtered.len(),
            "{residency}: tombstoned size"
        );
        assert_eq!(
            row_list(&kept.window(1, 5).await.unwrap()).await,
            filtered[1..6],
            "{residency}: a tombstoned keep's window"
        );
    }
}

/// Keeps compose: `keep_many`, partitions of a kept view, its code chunks
/// and decoded quads all follow the filtered index order, and a keep that
/// admits none of the view's rows is empty.
#[tokio::test]
async fn memory_keeps_compose() {
    let (_dir, store) = memory_store(LayoutStrategy::Dictionary).await;
    let (p, want) = p2_view(&store).await;
    let objects = objects_of(&want);
    let sparse = Keep::set(objects.iter().step_by(2).copied());
    let subjects: Vec<TermCode> = want
        .iter()
        .map(|r| r[0])
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    let lower = Keep::range(subjects[0]..subjects[subjects.len() / 2]);
    let both = p
        .keep_many(&[
            (QuadColumn::O, sparse.clone()),
            (QuadColumn::S, lower.clone()),
        ])
        .await
        .unwrap();
    let filtered: Vec<_> = want
        .iter()
        .copied()
        .filter(|r| sparse.admits(r[2]) && lower.admits(r[0]))
        .collect();
    assert!(!filtered.is_empty());
    assert_eq!(row_list(&both).await, filtered);
    assert_eq!(served_component(&both), Some("index:posg"));
    assert_eq!(
        collect_code_chunks(&both, &QuadColumn::ALL, 64).await.0,
        filtered
    );
    assert_eq!(partition_rows(&both, 3).await.1, filtered);
    let dict = both.dict_reader().unwrap();
    let codes: [vortex_buffer::Buffer<TermCode>; 4] =
        std::array::from_fn(|c| filtered.iter().map(|r| r[c]).collect());
    let rows: Vec<Row> = decoded_rows(&codes, &dict).await;
    let quads = tuple_rows(&both.quads_vec().await.unwrap());
    assert_eq!(quads, rows, "the decoded quads, in order");
    // A window of the kept rows small enough to read point by point.
    let small = tuple_rows(&both.window(3, 10).await.unwrap().quads_vec().await.unwrap());
    assert_eq!(small, rows[3..13], "a small kept window's quads, in order");

    let outside = Keep::set([objects[0] + 1_000_000]);
    let none = p.keep(QuadColumn::O, &outside).await.unwrap();
    assert_eq!(none.size().await.unwrap(), 0);
}
