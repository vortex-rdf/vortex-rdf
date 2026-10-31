//! Windows and partitions of a served view are slices of its rows in the
//! index's order, served in turn: plain and under tombstones, nested, through
//! the probe API, across the append tail, over wide row ids and child chunks,
//! and of a file run located only when first read.

use super::*;

/// A window of a served view, plain or under tombstones inside and outside
/// its run, is the slice of its live rows in the index's order and stays
/// served: neither the window nor its count and read sorts ids, windows
/// nest, and a window's streams and quads read its rows in order. In memory
/// and on file.
#[tokio::test]
async fn windows_are_slices_of_index_order() {
    for (residency, _dir, store) in served_stores().await {
        let tombstoned = store
            .delete_matching(None, Some(&iri("p0")), None, None)
            .await
            .unwrap()
            .delete_matching(None, None, Some(&plain_literal("object 3")), None)
            .await
            .unwrap();
        for (tombstones, store) in [("plain", &store), ("tombstoned", &tombstoned)] {
            let tag = format!("{residency}, {tombstones}");
            let (p, want) = p2_view(store).await;
            let n = want.len();
            if tombstones == "tombstoned" {
                assert!(
                    n > 110 && n < 20_000 / 7,
                    "{tag}: tombstones fall inside the run"
                );
            }
            for (offset, limit) in [
                (0, 10),
                (10, 100),
                (n / 2, n / 4),
                (n - 1, 5),
                (n, 5),
                (0, 0),
            ] {
                let rec = Recorder::start();
                let w = p.window(offset, limit).await.unwrap();
                let got = window_rows(&w).await;
                assert_eq!(
                    got,
                    slice_of(&want, offset, limit),
                    "{tag} ({offset}, {limit})"
                );
                assert_eq!(
                    w.size().await.unwrap(),
                    got.len(),
                    "{tag} ({offset}, {limit}) size"
                );
                assert_eq!(
                    ids_sorted(&rec),
                    0,
                    "{tag} ({offset}, {limit}) sorts no ids"
                );
                if !got.is_empty() {
                    assert_eq!(
                        served_component(&w),
                        Some("index:posg"),
                        "{tag} ({offset}, {limit})"
                    );
                }
            }
            let w = p.window(10, 100).await.unwrap();
            let inner = w.window(5, 50).await.unwrap();
            assert_eq!(row_list(&inner).await, want[15..65], "{tag} nested");
            assert_eq!(served_component(&inner), Some("index:posg"), "{tag} nested");
            assert_eq!(
                row_list(&w.window(95, 50).await.unwrap()).await,
                want[105..110],
                "{tag} nested past the outer window"
            );
            assert_eq!(
                collect_code_chunks(&w, &QuadColumn::ALL, 32).await.0,
                want[10..110],
                "{tag} chunks"
            );
            assert_eq!(
                struct_chunk_codes(&collect_row_chunks(&w, 32).await),
                want[10..110],
                "{tag} rows"
            );
            let quads = p.quads_vec().await.unwrap();
            assert_eq!(w.quads_vec().await.unwrap(), quads[10..110], "{tag} quads");
        }
    }
}

/// The partitions of a served view are served windows that read, in turn,
/// the view in the index's order. In memory and on file.
#[tokio::test]
async fn partitions_concatenate_to_index_order() {
    for (residency, _dir, store) in served_stores().await {
        let (p, want) = p2_view(&store).await;
        let parts = p
            .partitions(std::num::NonZeroUsize::new(4).unwrap())
            .await
            .unwrap();
        assert_eq!(parts.len(), 4, "{residency}");
        let mut got = vec![];
        for part in &parts {
            assert_eq!(served_component(part), Some("index:posg"), "{residency}");
            got.extend(row_list(part).await);
        }
        assert_eq!(got, want, "{residency}");
    }
}

/// The probe API, as the bindings call it: a windowed probe of a served
/// pattern is the index-order slice, and its count is the view's size. In
/// memory and on file.
#[tokio::test]
async fn probe_windows_follow_index_order() {
    for (residency, _dir, store) in served_stores().await {
        let (p, want) = p2_view(&store).await;
        let probe = Probe::parse(None, Some("<http://example.org/p2>"), None, None).unwrap();
        let w = store
            .run_probe(&probe.clone().window(10, Some(100)))
            .await
            .unwrap();
        assert_eq!(row_list(&w).await, want[10..110], "{residency}");
        assert_eq!(
            store.count_probe(&probe).await.unwrap(),
            p.size().await.unwrap(),
            "{residency}"
        );
    }
}

/// Windows of a served view with an append tail run past the base's rows,
/// in the index's order, into the tail's: each window is the slice of the
/// view's quads, and the counts add up, in memory and on file.
#[tokio::test]
async fn window_crosses_into_the_tail() {
    let (_dir, memory) = memory_store(LayoutStrategy::Default).await;
    let (_file_dir, path) = write_store_file(
        modular_quads(20_000, 7, 11),
        LayoutStrategy::Default,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    let file = VortexRdfStore::from_file(&path).await.unwrap();
    for store in [memory, file] {
        window_crosses_into_the_tail_of(store).await;
    }
}

async fn window_crosses_into_the_tail_of(store: VortexRdfStore) {
    let file = store.view_statistics().unwrap().file_backed;
    let extra: Vec<Quad> = (0..3)
        .map(|k| {
            make_quad(
                &format!("http://example.org/t{k}"),
                "http://example.org/p2",
                "tail",
                GraphName::DefaultGraph,
            )
        })
        .collect();
    let store = store.add_quads(extra).await.unwrap();
    let p = matched_view(&store, None, Some(&iri("p2")), None, None).await;
    assert_eq!(served_component(&p), Some("index:posg"));
    let quads = p.quads_vec().await.unwrap();
    let n = quads.len() - 3;
    for offset in n - 2..=n + 2 {
        let w = p.window(offset, 4).await.unwrap();
        let want = slice_of(&quads, offset, 4);
        assert_eq!(
            quad_strings(&w.quads_vec().await.unwrap()),
            quad_strings(&want),
            "offset {offset}"
        );
        assert_eq!(
            w.quads_vec().await.unwrap(),
            want,
            "file {file} offset {offset}: the order"
        );
        assert_eq!(
            w.size().await.unwrap(),
            want.len(),
            "file {file} offset {offset}"
        );
    }
}

/// Row ids past `u32::MAX` through served windows and partitions: the same
/// rows as the index-order slices of an unindexed twin's.
#[tokio::test]
async fn wide_row_ids_through_served_windows() {
    let _base = crate::store::test_hooks::RowIdBase::set((1 << 32) + 5);
    let quads = modular_quads(2_000, 7, 11);
    let store = built_store(
        quads.clone(),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    let twin = built_store(quads, LayoutStrategy::Dictionary, vec![]).await;
    let p = matched_view(&store, None, Some(&iri("p2")), None, None).await;
    assert_eq!(served_component(&p), Some("index:posg"));
    let want = posg(row_list(&matched_view(&twin, None, Some(&iri("p2")), None, None).await).await);
    for (offset, limit) in [(0, 10), (100, 50), (200, 1_000)] {
        let w = p.window(offset, limit).await.unwrap();
        assert_eq!(
            row_list(&w).await,
            slice_of(&want, offset, limit),
            "({offset}, {limit})"
        );
    }
    let parts = p
        .partitions(std::num::NonZeroUsize::new(3).unwrap())
        .await
        .unwrap();
    let mut got = vec![];
    for part in &parts {
        got.extend(row_list(part).await);
    }
    assert_eq!(got, want);
    // A keep of half the objects, and a chain of one, filter the same rows.
    let objects = objects_of(&want);
    let kept_objects: Vec<TermCode> = objects.iter().copied().step_by(2).collect();
    let kept = p
        .keep(QuadColumn::O, &Keep::set(kept_objects.iter().copied()))
        .await
        .unwrap();
    let filtered: Vec<[TermCode; 4]> = want
        .iter()
        .copied()
        .filter(|row| kept_objects.contains(&row[2]))
        .collect();
    assert_eq!(row_list(&kept).await, filtered, "a keep");
    assert_eq!(kept.size().await.unwrap(), filtered.len(), "its count");
    let o3 = plain_literal("object 3");
    let o3_code = store
        .dict_reader()
        .unwrap()
        .encode(&o3.to_string())
        .await
        .unwrap()
        .unwrap();
    let window = p.window(0, want.len()).await.unwrap();
    let chained = window
        .match_pattern(None, None, Some(&o3), None)
        .await
        .unwrap();
    let o3_rows: Vec<[TermCode; 4]> = want
        .iter()
        .copied()
        .filter(|row| row[2] == o3_code)
        .collect();
    assert!(!o3_rows.is_empty());
    assert_eq!(row_list(&chained).await, o3_rows, "a chain");
}

/// A located file run spanning several child leaves, windowed across their
/// boundaries: each window is the index-order slice.
#[tokio::test]
async fn windows_across_child_chunks() {
    let quads = modular_quads(2_000, 7, 11);
    let (_dir, path) =
        write_chunked_store(&quads, 32, Some(IndexType::SecondaryByCopy), false, None).await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let (p, want) = p2_view(&store).await;
    assert_eq!(served_component(&p), Some("index:posg"));
    let n = want.len();
    for (offset, limit) in [(31, 2), (30, 70), (0, n)] {
        let w = p.window(offset, limit).await.unwrap();
        assert_eq!(
            row_list(&w).await,
            slice_of(&want, offset, limit),
            "({offset}, {limit})"
        );
        assert_eq!(
            collect_code_chunks(&w, &QuadColumn::ALL, 16).await.0,
            slice_of(&want, offset, limit),
            "({offset}, {limit}) chunks"
        );
    }
}

/// A served file run the index cannot locate (a string layout's, whose
/// keys are not binary-searched, or a run a residual graph filters) is read
/// once by row index, then narrowed: a run whose rows are contiguous in the
/// child (no residual) as a located range, a residual one's scattered rows
/// as kept rows. Two windows and a partitioning of each give the quads in
/// the index's order, and the run is located once across them.
#[tokio::test]
async fn unlocated_runs_read_once_then_narrow() {
    let quads = modular_quads(5_000, 7, 11);
    // Every third quad in a named graph: the default graph's rows of a
    // predicate are scattered over its run in (p, o, s, g) order.
    let graphed: Vec<Quad> = quads
        .iter()
        .enumerate()
        .map(|(i, quad)| {
            let mut quad = quad.clone();
            if i % 3 == 0 {
                quad.graph_name = iri_graph("g1");
            }
            quad
        })
        .collect();
    let p2 = iri("p2");
    let default = GraphName::DefaultGraph;
    // Each view beside the unindexed twin's match, sorted (p, o, s, g), and
    // whether its narrowings are located ranges.
    let mut views = Vec::new();
    let mut dirs = Vec::new();
    let twin = built_store(quads.clone(), LayoutStrategy::Default, vec![]).await;
    let want = posg_quads(
        &matched_view(&twin, None, Some(&p2), None, None)
            .await
            .quads_vec()
            .await
            .unwrap(),
    );
    for layout in [LayoutStrategy::Default, LayoutStrategy::TypedObject] {
        let (dir, path) =
            write_store_file(quads.clone(), layout, vec![IndexType::SecondaryByCopy]).await;
        let store = VortexRdfStore::from_file(&path).await.unwrap();
        views.push((
            format!("{layout:?} ?p2??"),
            matched_view(&store, None, Some(&p2), None, None).await,
            want.clone(),
            true,
        ));
        dirs.push(dir);
    }
    let (dir, path) = write_store_file(
        graphed.clone(),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let twin = built_store(graphed, LayoutStrategy::Default, vec![]).await;
    views.push((
        "Dictionary ?p2?g".into(),
        matched_view(&store, None, Some(&p2), None, Some(&default)).await,
        posg_quads(
            &matched_view(&twin, None, Some(&p2), None, Some(&default))
                .await
                .quads_vec()
                .await
                .unwrap(),
        ),
        false,
    ));
    dirs.push(dir);

    for (tag, view, want, located) in views {
        let n = want.len();
        assert!(n > 100, "{tag}: a run to narrow");
        assert!(served_component(&view).is_some(), "{tag}: served");
        assert_eq!(view.debug_serve_row_range(), None, "{tag}: unlocated");
        let rec = Recorder::start();
        for (offset, limit) in [(10, 100), (n / 2, n / 4)] {
            let w = view.window(offset, limit).await.unwrap();
            let got: Vec<[String; 4]> =
                w.quads_vec().await.unwrap().iter().map(spellings).collect();
            assert_eq!(
                got,
                slice_of(&want, offset, limit),
                "{tag} ({offset}, {limit})"
            );
            assert!(
                served_component(&w).is_some(),
                "{tag} ({offset}, {limit}): served"
            );
            assert_eq!(
                w.debug_serve_row_range().is_some(),
                located,
                "{tag} ({offset}, {limit}): a located range, else kept rows"
            );
        }
        let parts = view
            .partitions(std::num::NonZeroUsize::new(3).unwrap())
            .await
            .unwrap();
        let mut got = Vec::new();
        for part in &parts {
            assert!(
                served_component(part).is_some(),
                "{tag}: a served partition"
            );
            got.extend(part.quads_vec().await.unwrap().iter().map(spellings));
        }
        assert_eq!(got, want, "{tag}: partitions");
        assert_eq!(
            rec.count(&Event::ServedRunLocated),
            1,
            "{tag}: located once"
        );
    }
}
