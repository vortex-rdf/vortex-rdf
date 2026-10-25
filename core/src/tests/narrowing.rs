//! Narrowing beyond a pattern — keeps, windows, capped counts — checked
//! against brute force over the gathered code columns, on every backend
//! and over served, scanned and chained views.

use super::*;
use crate::store::{Keep, QuadColumn, TermCode};

fn brute_keep(rows: &[[TermCode; 4]], column: QuadColumn, keep: &Keep) -> Vec<[TermCode; 4]> {
    rows.iter()
        .copied()
        .filter(|row| keep.admits(row[column.index()]))
        .collect()
}

/// The keeps every column is exercised with: a namespace range, sparse and
/// dense sets, a singleton, an all-admitting range and an empty set.
async fn keeps_for(store: &VortexRdfStore, column: QuadColumn) -> Vec<Keep> {
    let dict = store.dict_reader().unwrap();
    let (lo, hi) = match column {
        QuadColumn::S => dict.prefix_range("<http://example.org/s1").await.unwrap(),
        QuadColumn::P => dict.prefix_range("<http://example.org/p").await.unwrap(),
        QuadColumn::O => dict.prefix_range("\"o").await.unwrap(),
        QuadColumn::G => dict.prefix_range("<http://example.org/g").await.unwrap(),
    };
    let mid = lo + (hi - lo) / 2;
    let codes: Vec<TermCode> = (lo..hi).collect();
    vec![
        Keep::range(lo..hi),
        Keep::range(lo..mid),
        Keep::range(mid..mid + 1),
        Keep::set(codes.iter().copied().step_by(3)),
        Keep::set(codes.iter().copied().step_by(97)),
        Keep::set([mid]),
        Keep::set([0]),
        Keep::range(0..TermCode::MAX),
        Keep::set([]),
        Keep::range(hi..hi),
    ]
}

/// The views both the keeps and the windows below are checked over, each
/// beside its tag: the whole store, and a bound predicate (`p1`), object
/// (`"o3"`), subject (`s0042`) and predicate-object pair.
async fn bound_views(store: &VortexRdfStore) -> Vec<(&'static str, VortexRdfStore)> {
    let p1 = iri("p1");
    let o3 = plain_literal("o3");
    let s42 = subject_node(42, 4);
    vec![
        ("whole", store.clone()),
        (
            "p-bound",
            matched_view(store, None, Some(&p1), None, None).await,
        ),
        (
            "o-bound",
            matched_view(store, None, None, Some(&o3), None).await,
        ),
        (
            "s-bound",
            matched_view(store, Some(&s42), None, None, None).await,
        ),
        (
            "po-bound",
            matched_view(store, None, Some(&p1), Some(&o3), None).await,
        ),
    ]
}

/// Every keep over every column of every view agrees with brute force, and
/// the narrowed view's size with its row count: the [`bound_views`], and a
/// bound subject-predicate pair beside the subject's.
async fn assert_keeps_match_brute_force(store: &VortexRdfStore, tag: &str) {
    let p1 = iri("p1");
    let s42 = subject_node(42, 4);
    let mut views = bound_views(store).await;
    views.insert(
        4,
        (
            "sp-bound",
            matched_view(store, Some(&s42), Some(&p1), None, None).await,
        ),
    );
    for (view_tag, view) in &views {
        let rows = sorted_rows(view).await;
        for column in QuadColumn::ALL {
            for keep in keeps_for(store, column).await {
                let narrowed = view.keep(column, &keep).await.unwrap();
                let want = brute_keep(&rows, column, &keep);
                let got = sorted_rows(&narrowed).await;
                assert_eq!(got, want, "{tag}/{view_tag}: keep {column:?} {keep:?}");
                assert_eq!(
                    narrowed.size().await.unwrap(),
                    want.len(),
                    "{tag}/{view_tag}: size"
                );
                assert_eq!(
                    narrowed.exists().await.unwrap(),
                    !want.is_empty(),
                    "{tag}/{view_tag}: exists"
                );
                // A keep composes with a later match, and with a second keep.
                let chained = matched_view(&narrowed, None, Some(&p1), None, None).await;
                let p1_code = store
                    .dict_reader()
                    .unwrap()
                    .encode("<http://example.org/p1>")
                    .await
                    .unwrap()
                    .unwrap();
                let want_chained: Vec<[TermCode; 4]> =
                    want.iter().copied().filter(|r| r[1] == p1_code).collect();
                assert_eq!(
                    sorted_rows(&chained).await,
                    want_chained,
                    "{tag}/{view_tag}: chained match"
                );
            }
        }
        // Keeps on two columns at once.
        let keeps = [
            (
                QuadColumn::P,
                keeps_for(store, QuadColumn::P).await.remove(3),
            ),
            (
                QuadColumn::G,
                keeps_for(store, QuadColumn::G).await.remove(1),
            ),
        ];
        let narrowed = view.keep_many(&keeps).await.unwrap();
        let want = brute_keep(
            &brute_keep(&rows, keeps[0].0, &keeps[0].1),
            keeps[1].0,
            &keeps[1].1,
        );
        assert_eq!(
            sorted_rows(&narrowed).await,
            want,
            "{tag}/{view_tag}: keep_many"
        );
    }
}

/// The views [`assert_windows_match_slicing`] windows, each beside its tag:
/// the [`bound_views`] and an object keep.
async fn slicing_views(store: &VortexRdfStore) -> Vec<(&'static str, VortexRdfStore)> {
    let mut views = bound_views(store).await;
    views.push((
        "kept",
        store
            .keep(QuadColumn::O, &keeps_for(store, QuadColumn::O).await[3])
            .await
            .unwrap(),
    ));
    views
}

/// Windows and capped counts of every view agree with slicing its
/// base-ordered rows. A window that keeps a serve plan is one that cuts no
/// row, and reads the rows of the same match built anew, read plain, in its
/// order: its codes, and its quads in the index's order, which on a store
/// with the copy index is not always base order.
async fn assert_windows_match_slicing(store: &VortexRdfStore, tag: &str) {
    let views = slicing_views(store).await;
    let plain_views = slicing_views(store).await;
    let mut index_ordered = 0;
    for ((view_tag, view), (_, plain)) in views.iter().zip(&plain_views) {
        let rows = base_ordered_codes(view).await;
        let n = rows.len();
        let base_quads = view
            .keep(QuadColumn::S, &Keep::range(0..TermCode::MAX))
            .await
            .unwrap()
            .quads_vec()
            .await
            .unwrap();
        for (offset, limit) in [
            (0, 0),
            (0, 1),
            (0, 5),
            (3, 7),
            (n / 2, 10),
            (n.saturating_sub(3), 100),
            (n, 1),
            (n + 10, 1),
            (0, n + 10),
            (1, n),
        ] {
            let windowed = view.window(offset, limit).await.unwrap();
            let want: Vec<[TermCode; 4]> = rows.iter().copied().skip(offset).take(limit).collect();
            let read = if served_component(&windowed).is_some() {
                assert!(
                    offset == 0 && limit >= n,
                    "{tag}/{view_tag}: window({offset}, {limit}) cuts rows and keeps the plan"
                );
                let quads = plain.quads_vec().await.unwrap();
                assert_eq!(
                    windowed.quads_vec().await.unwrap(),
                    quads,
                    "{tag}/{view_tag}: window({offset}, {limit}): the plain quads, in order"
                );
                index_ordered += usize::from(quads != base_quads);
                row_list(plain).await
            } else {
                want.clone()
            };
            assert_eq!(
                row_list(&windowed).await,
                read,
                "{tag}/{view_tag}: window({offset}, {limit})"
            );
            assert_eq!(
                windowed.size().await.unwrap(),
                want.len(),
                "{tag}/{view_tag}: size"
            );
            assert_eq!(
                view.size_capped(offset + limit).await.unwrap(),
                n.min(offset + limit),
                "{tag}/{view_tag}: size_capped({})",
                offset + limit
            );
            // Windows compose: a window of a window re-bases on the first.
            let inner = windowed.window(1, 2).await.unwrap();
            let want_inner: Vec<[TermCode; 4]> = want.iter().copied().skip(1).take(2).collect();
            assert_eq!(
                row_list(&inner).await,
                want_inner,
                "{tag}/{view_tag}: nested window"
            );
        }
        assert_eq!(
            view.exists().await.unwrap(),
            n > 0,
            "{tag}/{view_tag}: exists"
        );
        assert_eq!(
            view.size_capped(usize::MAX).await.unwrap(),
            n,
            "{tag}/{view_tag}"
        );
        assert_eq!(view.size_capped(0).await.unwrap(), 0, "{tag}/{view_tag}");
    }
    if store.indexes().contains(&IndexType::SecondaryByCopy) {
        assert!(
            index_ordered > 0,
            "{tag}: a served window whose plain order is not base order"
        );
    }
}

/// Inside a subject run of width 8, a range keep on `p` and a set of one code
/// (one bounded search of ⌊log2 8⌋ + 1 = 4 steps, fewer than the run's rows;
/// a code inside the run, and its last row, which ends the search) are
/// answered by binary search inside the run; a set of two codes (8 steps)
/// takes the pass over the run. Every keep equals brute force.
async fn assert_sorted_run_keeps_engage(indexes: Indexes, tag: &str) {
    use crate::store::query::keep::keep_hooks::sorted_run_keeps;
    let quads: Vec<Quad> = (0..64)
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{}", i / 8),
                &format!("http://example.org/p{}", i % 8),
                &format!("o{}", i % 3),
                GraphName::DefaultGraph,
            )
        })
        .collect();
    let store = streamed_store(quads, LayoutStrategy::Dictionary, indexes).await;
    let s3 = iri_subject("s3");
    let run = matched_view(&store, Some(&s3), None, None, None).await;
    assert_eq!(run.debug_selection_range(), Some(24..32), "{tag}");
    let rows = sorted_rows(&run).await;
    let dict = store.dict_reader().unwrap();
    let mut p = Vec::new();
    for i in 0..8 {
        let term = format!("<http://example.org/p{i}>");
        p.push(dict.encode(&term).await.unwrap().unwrap());
    }
    for (keep, searched) in [
        (Keep::range(p[2]..p[5]), true),
        (Keep::set([p[4]]), true),
        (Keep::set([p[7]]), true),
        (Keep::set([p[1], p[6]]), false),
    ] {
        let before = sorted_run_keeps();
        let narrowed = run.keep(QuadColumn::P, &keep).await.unwrap();
        assert_eq!(
            sorted_run_keeps() - before,
            usize::from(searched),
            "{tag}: keep {keep:?}"
        );
        assert_eq!(
            sorted_rows(&narrowed).await,
            brute_keep(&rows, QuadColumn::P, &keep),
            "{tag}: keep {keep:?}"
        );
    }
}

#[tokio::test]
async fn test_keep_in_memory_matches_brute_force() {
    for (tag, indexes) in [
        ("no-index", vec![]),
        ("copy", vec![IndexType::SecondaryByCopy]),
        ("reference", vec![IndexType::SecondaryByReference]),
    ] {
        let store =
            streamed_store(fixture_quads(), LayoutStrategy::Dictionary, indexes.clone()).await;
        assert_keeps_match_brute_force(&store, tag).await;
        assert_sorted_run_keeps_engage(indexes, tag).await;
    }
}

/// A keep on a column with neither a canonical primitive nor a probe to read
/// in place (the pcodec-compressed `o` of [`unprobed_object_store`])
/// decodes the column at the view's selected rows once and tests those:
/// every keep on `o`, of the whole store and of a predicate-bound view,
/// equals brute force.
#[tokio::test]
async fn test_keep_on_a_column_no_probe_reads_decodes_the_selection() {
    let store = unprobed_object_store().await;
    assert_eq!(store.debug_base_child_int_canonical("o"), Some(false));
    let p1 = iri("p1");
    let by_p = matched_view(&store, None, Some(&p1), None, None).await;
    for (tag, view) in [("whole", store.clone()), ("p-bound", by_p)] {
        let rows = sorted_rows(&view).await;
        for keep in keeps_for(&store, QuadColumn::O).await {
            let narrowed = view.keep(QuadColumn::O, &keep).await.unwrap();
            assert_eq!(
                sorted_rows(&narrowed).await,
                brute_keep(&rows, QuadColumn::O, &keep),
                "{tag}: keep {keep:?}"
            );
        }
    }
}

/// Over every row of a store sorted by subject, a keep on `s` is a binary
/// search, the whole base being one run of the subject column: ranges and
/// small sets equal brute force over the store's rows in base order; a set
/// of more codes than its searches would beat takes the pass.
#[tokio::test]
async fn test_subject_keep_over_every_row_searches_the_sorted_column() {
    use crate::store::query::keep::keep_hooks::sorted_run_keeps;
    let store = streamed_store(fixture_quads(), LayoutStrategy::Dictionary, vec![]).await;
    let rows = row_list(&store).await;
    let (lo, hi) = store
        .dict_reader()
        .unwrap()
        .prefix_range("<http://example.org/s")
        .await
        .unwrap();
    let mid = lo + (hi - lo) / 2;
    for (keep, searched) in [
        (Keep::range(lo..hi), true),
        (Keep::range(lo + 7..mid), true),
        (Keep::range(mid..mid + 1), true),
        (Keep::set([lo, lo + 41, mid, hi - 1]), true),
        (Keep::set(lo..hi), false),
    ] {
        let before = sorted_run_keeps();
        let narrowed = store.keep(QuadColumn::S, &keep).await.unwrap();
        assert_eq!(
            sorted_run_keeps() - before,
            usize::from(searched),
            "keep {keep:?}"
        );
        assert_eq!(
            row_list(&narrowed).await,
            brute_keep(&rows, QuadColumn::S, &keep),
            "keep {keep:?}"
        );
    }
}

/// A store loaded whole from a file of 500-row leaves holds its code columns
/// chunked, so a keep's pass reads them through their probes a piece at a
/// time. Over every row and over a predicate-bound view, ranges on `g` and
/// sets on `p` decode the pieces they select densely, read no row through
/// the probe and equal brute force over the view's rows in base order; a
/// point-sized view reads each of its rows through the probe and decodes
/// nothing.
#[tokio::test]
async fn test_keep_over_chunked_columns_decodes_dense_pieces() {
    use crate::store::query::keep::keep_hooks::{decoded_pieces, probed_rows};
    let (_dir, path) = write_chunked_store_file(&fixture_quads(), 500).await;
    let store = VortexRdfStore::from_file_in_memory(&path).await.unwrap();
    for column in ["p", "g"] {
        assert_eq!(
            store.debug_base_child_int_canonical(column),
            Some(false),
            "{column}: chunked"
        );
    }
    let dict = store.dict_reader().unwrap();
    let (g_lo, g_hi) = dict.prefix_range("<http://example.org/g").await.unwrap();
    let (p_lo, p_hi) = dict.prefix_range("<http://example.org/p").await.unwrap();
    let p1 = iri("p1");
    let by_p = matched_view(&store, None, Some(&p1), None, None).await;
    let keeps = [
        (QuadColumn::G, Keep::range(g_lo..g_hi)),
        (QuadColumn::G, Keep::range(g_lo..g_lo + 1)),
        (QuadColumn::P, Keep::set([p_lo, p_hi - 1])),
        (QuadColumn::P, Keep::set(p_lo..p_hi)),
    ];
    for (tag, view) in [("whole", store.clone()), ("p-bound", by_p)] {
        let rows = row_list(&view).await;
        for (column, keep) in &keeps {
            let (pieces, probed) = (decoded_pieces(), probed_rows());
            let narrowed = view.keep(*column, keep).await.unwrap();
            assert!(
                decoded_pieces() > pieces,
                "{tag}: {column:?} {keep:?} decodes"
            );
            assert_eq!(
                probed_rows(),
                probed,
                "{tag}: {column:?} {keep:?} probes no row"
            );
            assert_eq!(
                row_list(&narrowed).await,
                brute_keep(&rows, *column, keep),
                "{tag}: {column:?} {keep:?}"
            );
        }
    }
    let o3 = plain_literal("o3");
    let point = matched_view(&store, None, Some(&p1), Some(&o3), None).await;
    let rows = row_list(&point).await;
    assert!(
        !rows.is_empty() && rows.len() <= 256,
        "a point-sized view: {} rows",
        rows.len()
    );
    let keep = Keep::range(g_lo..g_hi);
    let (pieces, probed) = (decoded_pieces(), probed_rows());
    let narrowed = point.keep(QuadColumn::G, &keep).await.unwrap();
    assert_eq!(
        decoded_pieces(),
        pieces,
        "a point-sized view decodes nothing"
    );
    assert_eq!(
        probed_rows() - probed,
        rows.len(),
        "each of its rows probed"
    );
    assert_eq!(
        row_list(&narrowed).await,
        brute_keep(&rows, QuadColumn::G, &keep)
    );
}

#[tokio::test]
async fn test_window_in_memory_matches_slicing() {
    for (tag, indexes) in [
        ("no-index", vec![]),
        ("copy", vec![IndexType::SecondaryByCopy]),
    ] {
        let store = streamed_store(fixture_quads(), LayoutStrategy::Dictionary, indexes).await;
        assert_windows_match_slicing(&store, tag).await;
    }
}

/// Tombstoned rows are neither kept, windowed nor counted.
#[tokio::test]
async fn test_narrowing_skips_tombstones() {
    let quads = fixture_quads();
    let store = streamed_store(
        fixture_quads(),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    let deleted = delete_all(&store, quads.iter().step_by(7).take(50)).await;
    assert_eq!(deleted.size().await.unwrap(), quads.len() - 50);
    assert_keeps_match_brute_force(&deleted, "tombstoned").await;
    assert_windows_match_slicing(&deleted, "tombstoned").await;
    // The string view of a window agrees with the code view.
    let windowed = deleted.window(10, 20).await.unwrap();
    assert_eq!(windowed.quads_vec().await.unwrap().len(), 20);
    assert_eq!(view_strings(&windowed).await.len(), 20);
}

/// Keeps need codes: a string layout and a tailed dictionary view refuse;
/// windows and capped counts work on every layout and through a tail.
#[tokio::test]
async fn test_narrowing_gates_and_tails() {
    let strings = streamed_store(fixture_quads(), LayoutStrategy::Default, vec![]).await;
    assert!(matches!(
        strings.keep(QuadColumn::P, &Keep::range(0..10)).await,
        Err(VortexRdfError::InvalidOperation(_))
    ));
    let all = quad_strings(&fixture_quads());
    let windowed = strings.window(100, 25).await.unwrap();
    assert_eq!(windowed.size().await.unwrap(), 25);
    assert!(all.contains(&windowed.quads_vec().await.unwrap()[0].to_string()));
    assert_eq!(strings.size_capped(7).await.unwrap(), 7);
    assert!(strings.exists().await.unwrap());
    assert!(
        !strings
            .window(all.len(), 5)
            .await
            .unwrap()
            .exists()
            .await
            .unwrap()
    );

    let store = streamed_store(fixture_quads(), LayoutStrategy::Dictionary, vec![]).await;
    let appended: Vec<Quad> = (0..30)
        .map(|i| {
            make_quad(
                &format!("http://example.org/tail{i:02}"),
                "http://example.org/p1",
                "tail",
                GraphName::DefaultGraph,
            )
        })
        .collect();
    let tailed = store.add_quads(appended.clone()).await.unwrap();
    assert_ne!(tailed.tail_len(), 0);
    assert!(matches!(
        tailed.keep(QuadColumn::P, &Keep::range(0..10)).await,
        Err(VortexRdfError::InvalidOperation(_))
    ));
    // A window runs base rows first, then the tail's, in append order.
    let n = store.size().await.unwrap();
    let total = tailed.size().await.unwrap();
    assert_eq!(total, n + 30);
    let base_only = tailed.window(0, n).await.unwrap();
    assert_eq!(base_only.size().await.unwrap(), n);
    assert_eq!(base_only.tail_len(), tailed.tail_len());
    assert_eq!(view_strings(&base_only).await, view_strings(&store).await);
    let straddling = tailed.window(n - 5, 10).await.unwrap();
    let got = straddling.quads_vec().await.unwrap();
    assert_eq!(got.len(), 10);
    assert_eq!(
        got[5..].iter().map(|q| q.to_string()).collect::<Vec<_>>(),
        appended[..5]
            .iter()
            .map(|q| q.to_string())
            .collect::<Vec<_>>()
    );
    let tail_only = tailed.window(n + 20, 100).await.unwrap();
    assert_eq!(
        tail_only
            .quads_vec()
            .await
            .unwrap()
            .iter()
            .map(|q| q.to_string())
            .collect::<Vec<_>>(),
        appended[20..]
            .iter()
            .map(|q| q.to_string())
            .collect::<Vec<_>>()
    );
    assert_eq!(tailed.size_capped(n + 7).await.unwrap(), n + 7);
    assert_eq!(tailed.size_capped(usize::MAX).await.unwrap(), total);
    assert!(tailed.window(total, 1).await.unwrap().size().await.unwrap() == 0);
    let compacted = tailed.compact().await.unwrap();
    assert!(
        compacted
            .keep(QuadColumn::P, &Keep::range(0..10))
            .await
            .is_ok()
    );
}

// ─── Windows that cannot cut a row ────────────────────────────────────────

/// The stores the window rules are checked over, each beside its tag: every
/// open of the fixture's files ([`file_stores`]), and the fixture built in
/// memory with no index, the copy index and the reference index. The dir
/// guards keep the files alive for the stores' lifetime.
async fn window_stores() -> (
    Vec<std::sync::Arc<tempfile::TempDir>>,
    Vec<(String, VortexRdfStore)>,
) {
    let mut dirs = Vec::new();
    let mut stores = Vec::new();
    for (tag, dir, store) in file_stores().await {
        dirs.push(dir);
        stores.push((tag, store));
    }
    for (tag, indexes) in [
        ("memory/no-index", vec![]),
        ("memory/copy", vec![IndexType::SecondaryByCopy]),
        ("memory/reference", vec![IndexType::SecondaryByReference]),
    ] {
        stores.push((
            tag.to_string(),
            streamed_store(fixture_quads(), LayoutStrategy::Dictionary, indexes).await,
        ));
    }
    (dirs, stores)
}

/// The probes the window rules are checked with, each beside its shape: `P`,
/// `PO` and `O`, which the copy index serves and the reference index
/// resolves to a run; the located subject runs `S` and `SP`; and `SPG`, a
/// located `(s, p)` run whose graph is left to a filter, pending on file.
fn window_probes() -> Vec<(&'static str, Probe)> {
    // The fixture's row 42: `s0042 p2 "o0"` in the default graph.
    let s = subject_node(42, 4);
    let p1 = iri("p1");
    let p2 = iri("p2");
    let o3 = plain_literal("o3");
    vec![
        ("P", Probe::new(None, Some(p1.clone()), None, None)),
        ("PO", Probe::new(None, Some(p1), Some(o3.clone()), None)),
        ("O", Probe::new(None, None, Some(o3), None)),
        ("S", Probe::new(Some(s.clone()), None, None, None)),
        (
            "SP",
            Probe::new(Some(s.clone()), Some(p2.clone()), None, None),
        ),
        (
            "SPG",
            Probe::new(Some(s), Some(p2), None, Some(GraphName::DefaultGraph)),
        ),
    ]
}

/// A window with no offset whose limit is at least the match's rows (the row
/// count, one more, or every row) is the plain match: the same rows in the
/// same order, the same serve plan, all the same statistics under an
/// unbounded limit, and never a view pending without a plan, on every open
/// and index set, in memory and on file. The exception is a filtered file
/// match whose reads take its filter by the masks first: its window
/// evaluates the filter whatever the limit, so the plain rows, in order, come
/// with exact statistics.
#[tokio::test]
async fn test_window_that_cannot_cut_is_the_plain_view() {
    let (_dirs, stores) = window_stores().await;
    for (tag, store) in &stores {
        for (shape, probe) in window_probes() {
            let label = format!("{tag} {shape}");
            let plain = store.run_probe(&probe).await.unwrap();
            let n = plain.size().await.unwrap();
            assert!(n > 0, "{label}: the fixture matches");
            let rows = row_list(&plain).await;
            assert_eq!(rows.len(), n, "{label}");
            let stats = plain.view_statistics().unwrap();
            // Each shape is what it is named for: the copy index serves the
            // index-eligible ones, and on file the graph's filter is left
            // pending beside the located run. A mapped file reads a pending
            // filter of code equalities by its masks first: the no-index
            // file's index-eligible ones, and the predicate the reference
            // index leaves beside the object's run.
            assert_eq!(
                stats.served_component.is_some(),
                tag.contains("copy") && matches!(shape, "P" | "PO" | "O"),
                "{label}: served"
            );
            if shape == "SPG" && stats.file_backed {
                assert!(stats.pending_filter, "{label}: the graph's filter");
            }
            let masks_first = plain.debug_reads_masks_first();
            let expected = match tag.as_str() {
                "file/no-index/mapped" => matches!(shape, "P" | "PO" | "O"),
                "file/reference/mapped" => shape == "PO",
                _ => false,
            };
            assert_eq!(
                masks_first, expected,
                "{label}: read by the filter's masks first"
            );
            for limit in [n, n + 1, usize::MAX] {
                let windowed = store
                    .run_probe(&probe.clone().window(0, Some(limit)))
                    .await
                    .unwrap();
                assert!(
                    !windowed.debug_pending_without_plan(),
                    "{label} limit {limit}: readable"
                );
                let got = windowed.view_statistics().unwrap();
                assert_eq!(
                    got.served_component, stats.served_component,
                    "{label} limit {limit}: the plan"
                );
                if masks_first {
                    assert!(!got.pending_filter, "{label} limit {limit}: evaluated");
                    assert_eq!(
                        got.rows,
                        RowCountHint::exact(n),
                        "{label} limit {limit}: exact rows"
                    );
                } else if limit == usize::MAX {
                    assert_eq!(got, stats, "{label}: the plain view's statistics");
                }
                assert_eq!(
                    row_list(&windowed).await,
                    rows,
                    "{label} limit {limit}: the plain rows, in order"
                );
            }
        }
    }
}

/// A window that cuts rows (one row short of the match, or past its first
/// row) is a position range over base order: the plain match's rows in base
/// row order, sliced, with no serve plan, on every store and every shape
/// with two rows to cut between.
#[tokio::test]
async fn test_window_that_cuts_keeps_base_order() {
    let (_dirs, stores) = window_stores().await;
    let mut served = 0;
    for (tag, store) in &stores {
        for (shape, probe) in window_probes() {
            let label = format!("{tag} {shape}");
            let plain = store.run_probe(&probe).await.unwrap();
            let n = plain.size().await.unwrap();
            if n < 2 {
                continue;
            }
            served += usize::from(served_component(&plain).is_some());
            let rows = base_ordered_codes(&plain).await;
            for (offset, limit) in [(0, n - 1), (1, n)] {
                let windowed = store
                    .run_probe(&probe.clone().window(offset, Some(limit)))
                    .await
                    .unwrap();
                assert!(
                    !windowed.debug_pending_without_plan(),
                    "{label} ({offset}, {limit}): readable"
                );
                assert_eq!(
                    served_component(&windowed),
                    None,
                    "{label} ({offset}, {limit}): the plan is dropped"
                );
                let want: Vec<[TermCode; 4]> =
                    rows.iter().copied().skip(offset).take(limit).collect();
                assert_eq!(
                    row_list(&windowed).await,
                    want,
                    "{label} ({offset}, {limit}): base order"
                );
            }
        }
    }
    assert!(served > 0, "a served match is among those cut");
}

/// Under tombstones and an appended tail, a window that cannot cut a row is
/// still the plain match, rows and order, the tail's rows after the base's,
/// and the copy index still serves its shapes. Its bound is the one the
/// view's statistics report for a fresh match: the base's live rows, exact
/// wherever they are known without reading (in memory always), plus the
/// tail's rows, so a limit of exactly that cuts nothing, while the live base
/// rows alone cut the tail. On file, a served run whose ids are unread is
/// bounded by its width, tombstones included, and once they are read by its
/// live rows.
#[tokio::test]
async fn test_window_that_cannot_cut_spans_tombstones_and_tail() {
    let quads = fixture_quads();
    // Every 11th row up to row 649: rows of `p1`, of `"o3"` and of both are
    // among them, and row 42, which `S`, `SP` and `SPG` match, is not.
    let doomed: Vec<Quad> = quads.iter().step_by(11).take(60).cloned().collect();
    assert!(!doomed.contains(&quads[42]));
    // Twenty rows that `P`, `PO` and `O` match, and three that `S`, `SP` and
    // `SPG` match.
    let appended: Vec<Quad> = (0..20)
        .map(|i| {
            make_quad(
                &format!("http://example.org/tail{i:02}"),
                "http://example.org/p1",
                "o3",
                GraphName::DefaultGraph,
            )
        })
        .chain((0..3).map(|i| {
            make_quad(
                "http://example.org/s0042",
                "http://example.org/p2",
                &format!("late{i}"),
                GraphName::DefaultGraph,
            )
        }))
        .collect();
    let (_dirs, stores) = window_stores().await;
    for (tag, store) in stores {
        let store = delete_all(&store, &doomed)
            .await
            .add_quads(appended.clone())
            .await
            .unwrap();
        for (shape, probe) in window_probes() {
            let label = format!("{tag} {shape}");
            let plain = store.run_probe(&probe).await.unwrap();
            // Taken before any read, as a fresh match knows itself.
            let stats = plain.view_statistics().unwrap();
            let rows = plain.quads_vec().await.unwrap();
            let n = rows.len();
            assert_eq!(plain.size().await.unwrap(), n, "{label}");
            assert!(stats.tombstones > 0, "{label}");
            let tail_rows = if matches!(shape, "P" | "PO" | "O") {
                20
            } else {
                3
            };
            assert_eq!(stats.tail_rows, tail_rows, "{label}");
            assert_eq!(
                stats.served_component.is_some(),
                tag.contains("copy") && matches!(shape, "P" | "PO" | "O"),
                "{label}: served"
            );
            let bound = stats.rows.upper_bound;
            assert!(bound >= n, "{label}: a bound");
            if !stats.file_backed {
                assert_eq!(stats.rows.exact, Some(n), "{label}: the live rows");
            }
            if tag == "file/copy/mapped" && matches!(shape, "P" | "O") {
                assert_eq!(stats.rows.exact, None, "{label}: the run's ids unread");
                assert!(bound > n, "{label}: the run's width");
                // The size above read the run's ids into the view: its live
                // rows now bound it, so a window of exactly its rows is the
                // served view as it is.
                assert_eq!(
                    plain.view_statistics().unwrap().rows,
                    RowCountHint::exact(n),
                    "{label}: the ids read"
                );
                let windowed = plain.window(0, n).await.unwrap();
                assert_eq!(
                    served_component(&windowed),
                    stats.served_component,
                    "{label}: the ids read, the plan"
                );
                assert_eq!(
                    windowed.quads_vec().await.unwrap(),
                    rows,
                    "{label}: the ids read, the plain rows, in order"
                );
            }
            for limit in [bound, bound + 1, usize::MAX] {
                let windowed = store
                    .run_probe(&probe.clone().window(0, Some(limit)))
                    .await
                    .unwrap();
                assert!(
                    !windowed.debug_pending_without_plan(),
                    "{label} limit {limit}: readable"
                );
                assert_eq!(
                    served_component(&windowed),
                    stats.served_component,
                    "{label} limit {limit}: the plan"
                );
                assert_eq!(
                    windowed.quads_vec().await.unwrap(),
                    rows,
                    "{label} limit {limit}: the plain rows, in order"
                );
            }
            // The live base rows alone cut the tail's.
            let base_rows = n - tail_rows;
            let cut = store
                .run_probe(&probe.clone().window(0, Some(base_rows)))
                .await
                .unwrap();
            assert_eq!(cut.size().await.unwrap(), base_rows, "{label}: no tail");
            assert_eq!(
                view_strings(&cut).await,
                quad_strings(&rows[..base_rows]),
                "{label}: the base's rows"
            );
        }
    }
}

/// On file, a subject and predicate located to a run whose graph is left to
/// a pending filter are bounded by the run's width: a limit of the filter's
/// matches, or anything else short of the width, takes the window route,
/// which evaluates the filter, while a limit of the width or more keeps the
/// filter pending. Either way the rows are the plain match's, in order. In
/// memory the graph is tested at match time, so none of these limits cuts.
#[tokio::test]
async fn test_window_that_the_run_width_cannot_clear_evaluates_the_filter() {
    // Subjects `s00` to `s31`, each with six objects under `p0`, then six
    // under `p1`, cycling through the default graph, `g1` and `g2`.
    let quads: Vec<Quad> = (0..384)
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{:02}", i / 12),
                &format!("http://example.org/p{}", i / 6 % 2),
                &format!("o{i:03}"),
                numbered_graph(i % 3),
            )
        })
        .collect();
    let (_dir, path) = write_store_file(quads, LayoutStrategy::Dictionary, vec![]).await;
    // `s05 p0` is a run of six rows, two of them in the default graph.
    let probe = Probe::new(
        Some(subject_node(5, 2)),
        Some(iri("p0")),
        None,
        Some(GraphName::DefaultGraph),
    );
    for store in [
        VortexRdfStore::from_file(&path).await.unwrap(),
        VortexRdfStore::from_file_in_memory(&path).await.unwrap(),
    ] {
        let plain = store.run_probe(&probe).await.unwrap();
        let rows = row_list(&plain).await;
        assert_eq!(rows.len(), 2);
        let stats = plain.view_statistics().unwrap();
        let on_file = stats.file_backed;
        if on_file {
            assert!(stats.pending_filter);
            assert_eq!(stats.rows, RowCountHint::at_most(6), "the run's width");
        } else {
            assert_eq!(stats.rows, RowCountHint::exact(2));
        }
        for (limit, short_of_width) in [
            (2, true),
            (5, true),
            (6, false),
            (7, false),
            (usize::MAX, false),
        ] {
            let windowed = store
                .run_probe(&probe.clone().window(0, Some(limit)))
                .await
                .unwrap();
            assert_eq!(row_list(&windowed).await, rows, "limit {limit}");
            let got = windowed.view_statistics().unwrap();
            if on_file && short_of_width {
                assert!(!got.pending_filter, "limit {limit}: the filter evaluated");
                assert_eq!(got.rows, RowCountHint::exact(2), "limit {limit}");
            } else {
                assert_eq!(got, stats, "limit {limit}: the plain view");
            }
        }
    }
}

/// `run_probe` hands back the matched view itself when the probe's window
/// cannot cut a row (no offset, a limit at least the view's bound): no window
/// is built, and the view reads the plain probe's codes in its order, with
/// its statistics, readable. Over every store but the reference ones, whose
/// windowed match holds its run pending for the window: copy-index served
/// matches, located runs with a pending filter and in-memory views. A
/// filtered file view read by its filter's masks first (the no-index file's
/// index-eligible shapes) builds its window, which evaluates the filter: the
/// plain rows, in order, known exactly.
#[tokio::test]
async fn test_run_probe_window_that_cannot_cut_is_the_matched_view() {
    use crate::store::query::window::window_hooks;

    let (_dirs, stores) = window_stores().await;
    for (tag, store) in stores.iter().filter(|(tag, _)| !tag.contains("reference")) {
        for (shape, probe) in window_probes() {
            let label = format!("{tag} {shape}");
            let plain = store.run_probe(&probe).await.unwrap();
            let stats = plain.view_statistics().unwrap();
            let rows = row_list(&plain).await;
            let bound = stats.rows.upper_bound;
            let masks_first = plain.debug_reads_masks_first();
            assert_eq!(
                masks_first,
                tag == "file/no-index/mapped" && matches!(shape, "P" | "PO" | "O"),
                "{label}: read by the filter's masks first"
            );
            for limit in [bound, bound + 1, usize::MAX] {
                let windows = window_hooks::windows();
                let windowed = store
                    .run_probe(&probe.clone().window(0, Some(limit)))
                    .await
                    .unwrap();
                assert!(
                    !windowed.debug_pending_without_plan(),
                    "{label} limit {limit}: readable"
                );
                let got = windowed.view_statistics().unwrap();
                if masks_first {
                    assert_eq!(
                        window_hooks::windows(),
                        windows + 1,
                        "{label} limit {limit}: the filter's window"
                    );
                    assert!(!got.pending_filter, "{label} limit {limit}: evaluated");
                    assert_eq!(
                        got.rows,
                        RowCountHint::exact(rows.len()),
                        "{label} limit {limit}: exact rows"
                    );
                } else {
                    assert_eq!(
                        window_hooks::windows(),
                        windows,
                        "{label} limit {limit}: no window built"
                    );
                    assert_eq!(
                        got, stats,
                        "{label} limit {limit}: the plain view's statistics"
                    );
                }
                assert_eq!(
                    row_list(&windowed).await,
                    rows,
                    "{label} limit {limit}: the plain rows, in order"
                );
            }
        }
    }
}

/// On file, a copy index's run under a string layout is unlocated: its width
/// is unknown until its ids are read. A window with no offset reads them once,
/// into the view's shared cell: under a limit they fit (the match's rows, or
/// one more, short of the base's) it is the served view as it is, the plain
/// match's rows in the index's order, while one row short it cuts in base
/// order, taking the ids from the cell. Under tombstones the ids read are the
/// run's width, and its live rows are what fits the limit. `run_probe` takes
/// the same routes.
#[tokio::test]
async fn test_window_over_an_unlocated_served_run_reads_its_ids_once() {
    use crate::store::IdsNeed;
    use crate::store::indexes::file::rid_scan_hooks;

    let p1 = iri("p1");
    let probe = Probe::new(None, Some(p1.clone()), None, None);
    let matching: Vec<Quad> = fixture_quads()
        .into_iter()
        .filter(|quad| quad.predicate == p1)
        .collect();
    // Five of the run's rows, spread over it.
    let doomed: Vec<Quad> = matching.iter().step_by(120).cloned().collect();
    assert_eq!(doomed.len(), 5);
    for layout in [LayoutStrategy::Default, LayoutStrategy::TypedObject] {
        let (_dir, path) =
            write_store_file(fixture_quads(), layout, vec![IndexType::SecondaryByCopy]).await;
        let tombstoned =
            delete_all(&VortexRdfStore::from_file(&path).await.unwrap(), &doomed).await;
        let written = VortexRdfStore::from_file(&path).await.unwrap();
        for (variant, store) in [("as written", written), ("tombstoned", tombstoned)] {
            let label = format!("{layout:?} {variant}");
            let plain = store.run_probe(&probe).await.unwrap();
            assert_eq!(served_component(&plain), Some("index:posg"), "{label}");
            assert_eq!(plain.debug_serve_row_range(), None, "{label}: unlocated");
            assert_eq!(plain.debug_pending_len(), Some(None), "{label}: unread");
            let rows = plain.quads_vec().await.unwrap();
            let n = rows.len();
            let deleted = if variant == "tombstoned" {
                doomed.len()
            } else {
                0
            };
            assert_eq!(n, matching.len() - deleted, "{label}: the live rows");
            assert!(
                n + 1 < store.size().await.unwrap(),
                "{label}: a base to cut"
            );
            // The plain match's rows in base order: the store's own, in its
            // order.
            let matched: std::collections::HashSet<&Quad> = rows.iter().collect();
            let base: Vec<Quad> = store
                .quads_vec()
                .await
                .unwrap()
                .into_iter()
                .filter(|quad| matched.contains(quad))
                .collect();
            assert_ne!(base, rows, "{label}: the index's order is not base order");
            for (limit, cuts) in [(n - 1, true), (n, false), (n + 1, false)] {
                let label = format!("{label} limit {limit}");
                let view = store
                    .match_pattern_for(None, Some(&p1), None, None, IdsNeed::CountOrWindow)
                    .await
                    .unwrap();
                assert_eq!(view.debug_pending_len(), Some(None), "{label}: unread");
                let scans = rid_scan_hooks::scans();
                let windowed = view.window(0, limit).await.unwrap();
                assert_eq!(rid_scan_hooks::scans() - scans, 1, "{label}: one id read");
                assert_eq!(
                    view.debug_pending_len(),
                    Some(Some(matching.len())),
                    "{label}: the run's ids in the shared cell"
                );
                let (plan, want) = if cuts {
                    (None, &base[..limit])
                } else {
                    (Some("index:posg"), &rows[..])
                };
                assert_eq!(served_component(&windowed), plan, "{label}: the plan");
                assert_eq!(
                    windowed.quads_vec().await.unwrap(),
                    want,
                    "{label}: the rows"
                );
                let probed = store
                    .run_probe(&probe.clone().window(0, Some(limit)))
                    .await
                    .unwrap();
                assert_eq!(served_component(&probed), plan, "{label}: run_probe's plan");
                assert_eq!(
                    probed.quads_vec().await.unwrap(),
                    want,
                    "{label}: run_probe's rows"
                );
            }
        }
    }
}

/// Under tombstones, a window over a file view whose reads take its pending
/// filter by the masks first counts none of the view's live rows: its window
/// evaluates the filter whatever the bound, so a count for the bound would
/// go unused. The window still reads the plain rows, known exactly.
#[tokio::test]
async fn test_masks_first_window_under_tombstones_counts_no_live_rows() {
    use crate::store::view::selection::live_count_hooks;

    let quads = fixture_quads();
    let (_dir, path) = write_store_file(quads.clone(), LayoutStrategy::Dictionary, vec![]).await;
    let store = delete_all(
        &VortexRdfStore::from_file(&path).await.unwrap(),
        quads.iter().step_by(11).take(40),
    )
    .await;
    let p1 = iri("p1");
    let probe = Probe::new(None, Some(p1), None, None);
    let plain = store.run_probe(&probe).await.unwrap();
    assert!(
        plain.debug_reads_masks_first(),
        "read by the filter's masks first"
    );
    let rows = row_list(&plain).await;
    let n = rows.len();
    // A limit short of the selected rows: the bound would count the live
    // rows.
    assert!(
        n + 1 < quads.len(),
        "{n} rows under a base of {}",
        quads.len()
    );
    let counts = live_count_hooks::counts();
    let windowed = store
        .run_probe(&probe.window(0, Some(n + 1)))
        .await
        .unwrap();
    assert_eq!(live_count_hooks::counts(), counts, "no live rows counted");
    let stats = windowed.view_statistics().unwrap();
    assert!(!stats.pending_filter, "the filter evaluated");
    assert_eq!(stats.rows, RowCountHint::exact(n), "exact rows");
    assert_eq!(row_list(&windowed).await, rows, "the plain rows");
}

mod file;
