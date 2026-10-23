//! Narrowing beyond a pattern — keeps, windows, capped counts — checked
//! against brute force over the gathered code columns, on every backend
//! and over served, scanned and chained views.

use super::*;
use crate::store::{Keep, QuadColumn, TermCode};

/// The row tuples of a view's gathered codes, sorted — order-free, since a
/// served view gathers in its index's order and a narrowed one in base
/// order.
async fn row_set(view: &VortexRdfStore) -> Vec<[TermCode; 4]> {
    let mut rows = row_list(view).await;
    rows.sort_unstable();
    rows
}

fn brute_keep(rows: &[[TermCode; 4]], column: QuadColumn, keep: &Keep) -> Vec<[TermCode; 4]> {
    rows.iter()
        .copied()
        .filter(|row| keep.admits(row[column.index()]))
        .collect()
}

async fn memory_store(indexes: Indexes) -> VortexRdfStore {
    VortexRdfStore::from_quads(
        quad_stream(fixture_quads()),
        LayoutStrategy::Dictionary,
        indexes,
    )
    .await
    .unwrap()
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

/// Every keep over every column of every view agrees with brute force, and
/// the narrowed view's size with its row count.
async fn assert_keeps_match_brute_force(store: &VortexRdfStore, tag: &str) {
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let o3 = Term::Literal(Literal::new_simple_literal("o3"));
    let s42 = subject_node(42, 4);
    let views: Vec<(&str, VortexRdfStore)> = vec![
        ("whole", store.clone()),
        (
            "p-bound",
            store
                .match_pattern(None, Some(&p1), None, None)
                .await
                .unwrap(),
        ),
        (
            "o-bound",
            store
                .match_pattern(None, None, Some(&o3), None)
                .await
                .unwrap(),
        ),
        (
            "s-bound",
            store
                .match_pattern(Some(&s42), None, None, None)
                .await
                .unwrap(),
        ),
        (
            "sp-bound",
            store
                .match_pattern(Some(&s42), Some(&p1), None, None)
                .await
                .unwrap(),
        ),
        (
            "po-bound",
            store
                .match_pattern(None, Some(&p1), Some(&o3), None)
                .await
                .unwrap(),
        ),
    ];
    for (view_tag, view) in &views {
        let rows = row_set(view).await;
        for column in QuadColumn::ALL {
            for keep in keeps_for(store, column).await {
                let narrowed = view.keep(column, &keep).await.unwrap();
                let want = brute_keep(&rows, column, &keep);
                let got = row_set(&narrowed).await;
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
                let chained = narrowed
                    .match_pattern(None, Some(&p1), None, None)
                    .await
                    .unwrap();
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
                    row_set(&chained).await,
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
            row_set(&narrowed).await,
            want,
            "{tag}/{view_tag}: keep_many"
        );
    }
}

/// Windows and capped counts of every view agree with slicing its
/// base-ordered rows. A window that keeps a serve plan is one that cuts no
/// row, and reads the view's own rows in the view's order.
async fn assert_windows_match_slicing(store: &VortexRdfStore, tag: &str) {
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let o3 = Term::Literal(Literal::new_simple_literal("o3"));
    let s42 = subject_node(42, 4);
    let views: Vec<(&str, VortexRdfStore)> = vec![
        ("whole", store.clone()),
        (
            "p-bound",
            store
                .match_pattern(None, Some(&p1), None, None)
                .await
                .unwrap(),
        ),
        (
            "o-bound",
            store
                .match_pattern(None, None, Some(&o3), None)
                .await
                .unwrap(),
        ),
        (
            "s-bound",
            store
                .match_pattern(Some(&s42), None, None, None)
                .await
                .unwrap(),
        ),
        (
            "po-bound",
            store
                .match_pattern(None, Some(&p1), Some(&o3), None)
                .await
                .unwrap(),
        ),
        (
            "kept",
            store
                .keep(QuadColumn::O, &keeps_for(store, QuadColumn::O).await[3])
                .await
                .unwrap(),
        ),
    ];
    for (view_tag, view) in &views {
        let rows = base_ordered_codes(view).await;
        let n = rows.len();
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
                row_list(view).await
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
}

/// Inside a subject run of width 8, a range keep on `p` and a set of one code
/// (one bounded search of ⌊log2 8⌋ + 1 = 4 steps, fewer than the run's rows;
/// a code inside the run, and its last row, which ends the search) are
/// answered by binary search inside the run; a set of two codes (8 steps)
/// takes the pass over the run. Every keep equals brute force.
async fn assert_sorted_run_keeps_engage(indexes: Indexes, tag: &str) {
    use crate::store::query::narrowing::narrow_hooks::sorted_run_keeps;
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
    let store = VortexRdfStore::from_quads(quad_stream(quads), LayoutStrategy::Dictionary, indexes)
        .await
        .unwrap();
    let s3 = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s3").unwrap());
    let run = store
        .match_pattern(Some(&s3), None, None, None)
        .await
        .unwrap();
    assert_eq!(run.debug_selection_range(), Some(24..32), "{tag}");
    let rows = row_set(&run).await;
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
            row_set(&narrowed).await,
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
        let store = memory_store(indexes.clone()).await;
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
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let by_p = store
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    for (tag, view) in [("whole", store.clone()), ("p-bound", by_p)] {
        let rows = row_set(&view).await;
        for keep in keeps_for(&store, QuadColumn::O).await {
            let narrowed = view.keep(QuadColumn::O, &keep).await.unwrap();
            assert_eq!(
                row_set(&narrowed).await,
                brute_keep(&rows, QuadColumn::O, &keep),
                "{tag}: keep {keep:?}"
            );
        }
    }
}

#[tokio::test]
async fn test_window_in_memory_matches_slicing() {
    for (tag, indexes) in [
        ("no-index", vec![]),
        ("copy", vec![IndexType::SecondaryByCopy]),
    ] {
        let store = memory_store(indexes).await;
        assert_windows_match_slicing(&store, tag).await;
    }
}

/// Tombstoned rows are neither kept, windowed nor counted.
#[tokio::test]
async fn test_narrowing_skips_tombstones() {
    let quads = fixture_quads();
    let store = memory_store(vec![IndexType::SecondaryByCopy]).await;
    let mut deleted = store.clone();
    for quad in quads.iter().step_by(7).take(50) {
        deleted = deleted.delete_quad(quad).await.unwrap();
    }
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
    let strings = VortexRdfStore::from_quads(
        quad_stream(fixture_quads()),
        LayoutStrategy::Default,
        vec![],
    )
    .await
    .unwrap();
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

    let store = memory_store(vec![]).await;
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
        stores.push((tag.to_string(), memory_store(indexes).await));
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
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let p2 = NamedNode::new("http://example.org/p2").unwrap();
    let o3 = Term::Literal(Literal::new_simple_literal("o3"));
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
/// same order (a served match's in its index's), the same serve plan, all
/// the same statistics under an unbounded limit, and never a view pending
/// without a plan, on every open and index set, in memory and on file.
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
            // pending beside the located run.
            assert_eq!(
                stats.served_component.is_some(),
                tag.contains("copy") && matches!(shape, "P" | "PO" | "O"),
                "{label}: served"
            );
            if shape == "SPG" && stats.file_backed {
                assert!(stats.pending_filter, "{label}: the graph's filter");
            }
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
                if limit == usize::MAX {
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
/// still the plain match, rows and order, the tail's rows after the base's.
/// Its bound is the one the view's statistics report for a fresh match: the
/// base's live rows, exact wherever they are known without reading (in
/// memory always), plus the tail's rows, so a limit of exactly that cuts
/// nothing, while the live base rows alone cut the tail. On file, a served
/// run whose ids are unread is bounded by its width, tombstones included.
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
    for (tag, mut store) in stores {
        for quad in &doomed {
            store = store.delete_quad(quad).await.unwrap();
        }
        let store = store.add_quads(appended.clone()).await.unwrap();
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
            let bound = stats.rows.upper_bound;
            assert!(bound >= n, "{label}: a bound");
            if !stats.file_backed {
                assert_eq!(stats.rows.exact, Some(n), "{label}: the live rows");
            }
            if tag == "file/copy/mapped" && matches!(shape, "P" | "O") {
                assert_eq!(stats.rows.exact, None, "{label}: the run's ids unread");
                assert!(bound > n, "{label}: the run's width");
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
    let graph = |k: usize| match k {
        0 => GraphName::DefaultGraph,
        k => GraphName::NamedNode(NamedNode::new(format!("http://example.org/g{k}")).unwrap()),
    };
    // Subjects `s00` to `s31`, each with six objects under `p0`, then six
    // under `p1`, cycling through the default graph, `g1` and `g2`.
    let quads: Vec<Quad> = (0..384)
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{:02}", i / 12),
                &format!("http://example.org/p{}", i / 6 % 2),
                &format!("o{i:03}"),
                graph(i % 3),
            )
        })
        .collect();
    let (_dir, path) = write_store_file(quads, LayoutStrategy::Dictionary, vec![]).await;
    // `s05 p0` is a run of six rows, two of them in the default graph.
    let probe = Probe::new(
        Some(subject_node(5, 2)),
        Some(NamedNode::new("http://example.org/p0").unwrap()),
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

mod file {
    use super::*;
    use std::ops::Range;

    #[tokio::test]
    async fn test_keep_on_file_matches_brute_force() {
        for (tag, _dir, store) in file_stores().await {
            assert_keeps_match_brute_force(&store, &tag).await;
        }
    }

    #[tokio::test]
    async fn test_window_on_file_matches_slicing() {
        let strings =
            |quads: Vec<Quad>| -> Vec<String> { quads.iter().map(ToString::to_string).collect() };
        for (tag, _dir, store) in file_stores().await {
            assert_windows_match_slicing(&store, &tag).await;
            if !tag.ends_with("/mapped") {
                continue;
            }
            // With quads appended, a match on the unindexed graph keeps its
            // filter pending: a window starting past the base's matches takes
            // the tail's rows from the offset minus the base's match count.
            let graph = GraphName::DefaultGraph;
            let base_matches = matched_size(&store, None, None, None, Some(&graph)).await;
            let tailed = store.add_quads(appended_quads(30)).await.unwrap();
            let matched = tailed
                .match_pattern(None, None, None, Some(&graph))
                .await
                .unwrap();
            assert!(matched.view_statistics().unwrap().pending_filter, "{tag}");
            let rows = strings(matched.quads_vec().await.unwrap());
            assert_eq!(rows.len(), base_matches + 30, "{tag}");
            for (past, limit) in [(1, 5), (10, 100), (29, 1), (30, 1)] {
                let offset = base_matches + past;
                let window = matched.window(offset, limit).await.unwrap();
                let want: Vec<String> = rows.iter().skip(offset).take(limit).cloned().collect();
                assert_eq!(
                    strings(window.quads_vec().await.unwrap()),
                    want,
                    "{tag}: window({offset}, {limit})"
                );
            }
        }
    }

    /// A very wide set — too many codes to look up run by run — is streamed
    /// and tested in memory over the column and still agrees with brute force,
    /// over a pending filter too.
    #[tokio::test]
    async fn test_keep_wide_set_on_file() {
        let (_dir, path) =
            write_store_file(fixture_quads(), LayoutStrategy::Dictionary, vec![]).await;
        let store = VortexRdfStore::from_file(&path).await.unwrap();
        let dict = store.dict_reader().unwrap();
        let (lo, hi) = dict.prefix_range("<http://example.org/s").await.unwrap();
        assert!(hi - lo > 2_000);
        let wide = Keep::set((lo..hi).filter(|c| c % 3 != 0).chain([0, 1, 2]));
        let p1 = NamedNode::new("http://example.org/p1").unwrap();
        for (tag, view) in [
            ("whole", store.clone()),
            (
                "filtered",
                store
                    .match_pattern(None, Some(&p1), None, None)
                    .await
                    .unwrap(),
            ),
        ] {
            let rows = row_set(&view).await;
            let narrowed = view.keep(QuadColumn::S, &wide).await.unwrap();
            assert_eq!(
                row_set(&narrowed).await,
                brute_keep(&rows, QuadColumn::S, &wide),
                "{tag}"
            );
            assert_eq!(
                narrowed.size().await.unwrap(),
                brute_keep(&rows, QuadColumn::S, &wide).len()
            );
        }
    }

    /// A copy-indexed file store serves a predicate + object + graph match
    /// from the index and keeps the graph test as a residual filter on the
    /// served view. Counting that view up to a cap, and asking whether it
    /// holds anything, count through the filter.
    #[tokio::test]
    async fn test_a_served_view_with_a_residual_filter_counts_up_to_a_cap() {
        let quads = fixture_quads();
        let (_dir, path) = write_store_file(
            quads.clone(),
            LayoutStrategy::Dictionary,
            vec![IndexType::SecondaryByCopy],
        )
        .await;
        let store = VortexRdfStore::from_file(&path).await.unwrap();
        let p1 = NamedNode::new("http://example.org/p1").unwrap();
        let o3 = Term::Literal(Literal::new_simple_literal("o3"));
        let g1 = GraphName::NamedNode(NamedNode::new("http://example.org/g1").unwrap());
        let expected = quads
            .iter()
            .filter(|q| q.predicate == p1 && q.object == o3 && q.graph_name == g1)
            .count();
        assert!(expected > 2, "the fixture has matches to cap");

        let view = store
            .match_pattern(None, Some(&p1), Some(&o3), Some(&g1))
            .await
            .unwrap();

        assert_eq!(view.size().await.unwrap(), expected);
        assert_eq!(view.size_capped(2).await.unwrap(), 2);
        assert_eq!(view.size_capped(usize::MAX).await.unwrap(), expected);
        assert!(view.exists().await.unwrap());
    }

    /// Tombstones on a file store are honoured by every narrowing.
    #[tokio::test]
    async fn test_file_narrowing_skips_tombstones() {
        let quads = fixture_quads();
        let (_dir, path) = write_store_file(
            quads.clone(),
            LayoutStrategy::Dictionary,
            vec![IndexType::SecondaryByCopy],
        )
        .await;
        let mut store = VortexRdfStore::from_file(&path).await.unwrap();
        for quad in quads.iter().step_by(11).take(40) {
            store = store.delete_quad(quad).await.unwrap();
        }
        assert_eq!(store.size().await.unwrap(), quads.len() - 40);
        assert_keeps_match_brute_force(&store, "file/tombstoned").await;
        assert_windows_match_slicing(&store, "file/tombstoned").await;
    }

    /// A keep on a file view is applied by the store's own kernel, never as a
    /// filter expression: however many distinct code sets and ranges are
    /// kept, the file handle binds no shape beyond the ones a first keep and
    /// a first row read bind.
    #[tokio::test]
    async fn test_file_keeps_bind_no_expressions() {
        let quads = modular_quads(64, 3, 8);
        let (_dir, path) =
            write_store_file(quads.clone(), LayoutStrategy::Dictionary, vec![]).await;
        let store = VortexRdfStore::from_file(&path).await.unwrap();
        let dict = store.dict_reader().unwrap();
        let p0 = NamedNode::new("http://example.org/p0").unwrap();
        let view = store
            .match_pattern(None, Some(&p0), None, None)
            .await
            .unwrap();
        let first = dict.encode("\"object 0\"").await.unwrap().unwrap();
        // One keep and one row read bind every shape the loop uses: the
        // keep's filter and column projection, and the row read's projection.
        let warm = view.keep(QuadColumn::O, &Keep::set([first])).await.unwrap();
        view_strings(&warm).await;
        let bound = store.debug_bound_exprs().unwrap();
        for i in 0..8 {
            let code = dict
                .encode(&format!("\"object {i}\""))
                .await
                .unwrap()
                .unwrap();
            let kept = view.keep(QuadColumn::O, &Keep::set([code])).await.unwrap();
            assert_eq!(
                view_strings(&kept).await,
                expected_strings(&quads, |j| j % 3 == 0 && j % 8 == i),
                "object {i}"
            );
            let ranged = view
                .keep(QuadColumn::O, &Keep::range(code..code + 1))
                .await
                .unwrap();
            assert_eq!(ranged.size().await.unwrap(), kept.size().await.unwrap());
        }
        assert_eq!(store.debug_bound_exprs().unwrap(), bound);
    }

    /// Every new constant asked about on an unindexed role binds a new
    /// expression, and Vortex's reader caches key their entries by that
    /// identity without evicting any. Past the bound tree memo's cap — which
    /// clears wholesale — the file's reader tree is retired: the tree that
    /// was current at the start is gone once the loop has run, answers stay
    /// exact across the retirement, and the memo stays within its cap.
    #[tokio::test]
    async fn test_distinct_constants_retire_the_reader_tree() {
        let distinct = crate::store::persist::native_file::BIND_MEMO_MAX + 200;
        let quads: Vec<Quad> = (0..distinct)
            .map(|i| {
                make_quad(
                    &format!("http://example.org/s{i}"),
                    "http://example.org/p0",
                    &format!("o{i}"),
                    GraphName::DefaultGraph,
                )
            })
            .collect();
        let (_dir, path) = write_store_file(quads, LayoutStrategy::Dictionary, vec![]).await;
        let store = VortexRdfStore::from_file(&path).await.unwrap();
        let first_tree = {
            let reader = store.debug_root_reader().unwrap();
            std::sync::Arc::downgrade(&reader)
        };

        for i in 0..distinct {
            let object = Term::Literal(Literal::new_simple_literal(format!("o{i}")));
            let matched = store
                .match_pattern(None, None, Some(&object), None)
                .await
                .unwrap();
            assert_eq!(matched.size().await.unwrap(), 1, "object o{i}");
        }

        assert!(
            store.debug_bound_exprs().unwrap() <= crate::store::persist::native_file::BIND_MEMO_MAX,
            "the bound tree memo is capped"
        );
        assert!(
            first_tree.upgrade().is_none(),
            "the reader tree from before the memo cleared is still alive"
        );
    }

    /// One 160,000-row file, past one scan split's worth of rows, serves both
    /// multi-split keep checks below.
    #[tokio::test]
    async fn test_keeps_on_a_multi_split_file() {
        let (_dir, path) = multi_window_dictionary_file().await;
        let opened = crate::tests::open_mapped(&path).await;
        assert!(
            opened.splits().unwrap().len() > 1,
            "the fixture must span several scan splits"
        );
        keep_spans_scan_splits(&path).await;
        s_keeps_match_brute_force(&path).await;
    }

    /// Past one scan split's worth of rows the column streams in several
    /// chunks: the positions a keep admits must carry across the chunk
    /// boundaries, over the whole file, a pending filter, a row window and a
    /// keep chained on a keep.
    async fn keep_spans_scan_splits(path: &std::path::Path) {
        let store = VortexRdfStore::from_file(path).await.unwrap();
        let dict = store.dict_reader().unwrap();
        let (o_lo, _) = dict.prefix_range("\"object ").await.unwrap();
        let (s_lo, s_hi) = dict.prefix_range("<http://example.org/s").await.unwrap();
        let objects = Keep::set([o_lo + 1, o_lo + 5]);
        let subjects = Keep::range(s_lo..s_lo + (s_hi - s_lo) / 2);
        let p0 = NamedNode::new("http://example.org/p0").unwrap();
        let views = [
            ("whole", store.clone()),
            (
                "filtered",
                store
                    .match_pattern(None, Some(&p0), None, None)
                    .await
                    .unwrap(),
            ),
            ("window", store.window(10_000, 140_000).await.unwrap()),
        ];
        for (tag, view) in views {
            let rows = row_set(&view).await;
            let narrowed = view.keep(QuadColumn::O, &objects).await.unwrap();
            let expected = brute_keep(&rows, QuadColumn::O, &objects);
            assert!(!expected.is_empty() && expected.len() < rows.len(), "{tag}");
            assert_eq!(row_set(&narrowed).await, expected, "{tag}");
            let chained = narrowed.keep(QuadColumn::S, &subjects).await.unwrap();
            let chained_expected = brute_keep(&expected, QuadColumn::S, &subjects);
            assert!(
                !chained_expected.is_empty() && chained_expected.len() < expected.len(),
                "{tag} chained"
            );
            assert_eq!(row_set(&chained).await, chained_expected, "{tag} chained");
        }
    }

    // ─── Keeps on the sorted subject column: located runs ─────────────────

    /// 12,000 distinct quads in subject runs of five rows (2,400 subjects,
    /// three predicates, eight objects).
    fn run_quads() -> Vec<Quad> {
        (0..12_000)
            .map(|i| {
                make_quad(
                    &format!("http://example.org/s{:05}", i / 5),
                    &format!("http://example.org/p{}", i % 3),
                    &format!("object {}", i % 8),
                    GraphName::DefaultGraph,
                )
            })
            .collect()
    }

    /// How a view's selection reads, which fixes the selection a keep leaves.
    #[derive(Clone, Copy, PartialEq, Eq, Debug)]
    enum Sel {
        /// Every row or one range of rows, nothing pending.
        Range,
        /// Every row or one range of rows, with a filter still pending.
        Pending,
        /// An explicit id list.
        Ids,
    }

    /// A view over the store with the shape of its selection.
    struct View {
        tag: &'static str,
        view: VortexRdfStore,
        sel: Sel,
    }

    /// A mapped store file sorted by subject whose columns are twelve
    /// 1,001-row leaves (so a five-row subject run straddles nine of the
    /// eleven leaf boundaries), with its rows in file order as the oracle
    /// every located keep is checked against.
    struct RunStore {
        _dir: tempfile::TempDir,
        store: VortexRdfStore,
        /// Every row's codes, in file order.
        rows: Vec<[TermCode; 4]>,
        /// The subject namespace's code range.
        subjects: Range<TermCode>,
        /// The first row of every leaf after the first.
        boundaries: Vec<usize>,
    }

    impl RunStore {
        async fn open() -> Self {
            let (dir, path) = write_chunked_store_file(&run_quads(), 1_001).await;
            let opened = crate::tests::open_mapped(&path).await;
            let boundaries: Vec<usize> = opened
                .splits()
                .unwrap()
                .iter()
                .skip(1)
                .map(|split| split.start as usize)
                .collect();
            assert_eq!(boundaries.len(), 11, "twelve leaves");
            let store = VortexRdfStore::from_file(&path).await.unwrap();
            assert_eq!(store.debug_file_mapped(), Some(true));
            let rows = row_list(&store).await;
            assert!(rows.is_sorted_by_key(|row| row[0]), "the file is s-sorted");
            assert!(
                boundaries.iter().any(|&b| rows[b - 1][0] == rows[b][0]),
                "a subject's run must straddle a leaf boundary"
            );
            let (lo, hi) = store
                .dict_reader()
                .unwrap()
                .prefix_range("<http://example.org/s")
                .await
                .unwrap();
            Self {
                _dir: dir,
                store,
                rows,
                subjects: lo..hi,
                boundaries,
            }
        }

        /// Views over every selection shape the located kernel meets: the
        /// whole store, a pending filter, an id list (an `o` keep's result),
        /// a row window and a window over the pending filter.
        async fn views(&self) -> Vec<View> {
            let p0 = NamedNode::new("http://example.org/p0").unwrap();
            let pending = self
                .store
                .match_pattern(None, Some(&p0), None, None)
                .await
                .unwrap();
            let (o_lo, _) = self
                .store
                .dict_reader()
                .unwrap()
                .prefix_range("\"object ")
                .await
                .unwrap();
            let ids = self
                .store
                .keep(QuadColumn::O, &Keep::set([o_lo + 1, o_lo + 5]))
                .await
                .unwrap();
            vec![
                View {
                    tag: "whole",
                    view: self.store.clone(),
                    sel: Sel::Range,
                },
                View {
                    tag: "pending",
                    view: pending.clone(),
                    sel: Sel::Pending,
                },
                View {
                    tag: "ids",
                    view: ids,
                    sel: Sel::Ids,
                },
                View {
                    tag: "window",
                    view: self.store.window(1_000, 10_000).await.unwrap(),
                    sel: Sel::Range,
                },
                View {
                    tag: "pending-window",
                    view: pending.window(5, 3_000).await.unwrap(),
                    sel: Sel::Ids,
                },
            ]
        }

        /// Range keeps from the block's ends to the runs a leaf boundary cuts.
        fn range_keeps(&self) -> Vec<(String, Keep)> {
            let Range { start: lo, end: hi } = self.subjects.clone();
            let mid = lo + (hi - lo) / 2;
            let mut keeps = vec![
                ("every subject".to_string(), Keep::range(lo..hi)),
                ("first half".to_string(), Keep::range(lo..mid)),
                ("second half".to_string(), Keep::range(mid..hi)),
                ("a slice".to_string(), Keep::range(lo + 7..lo + 1_500)),
                ("one subject".to_string(), Keep::range(mid..mid + 1)),
                ("past the first".to_string(), Keep::range(lo - 3..lo + 3)),
                ("past the last".to_string(), Keep::range(hi - 3..hi + 3)),
                ("every code".to_string(), Keep::range(0..TermCode::MAX)),
                ("below the subjects".to_string(), Keep::range(0..lo)),
                ("above the subjects".to_string(), Keep::range(hi..hi + 100)),
                ("empty".to_string(), Keep::range(hi..hi)),
            ];
            for &b in &self.boundaries {
                // The subject run a leaf boundary cuts: alone, among its
                // neighbours, and as the end of a range and the start of one.
                let (before, after) = (self.rows[b - 1][0], self.rows[b][0]);
                keeps.extend([
                    (format!("{b}: the cut run"), Keep::range(after..after + 1)),
                    (
                        format!("{b}: with neighbours"),
                        Keep::range(before - 1..after + 2),
                    ),
                    (format!("{b}: up to it"), Keep::range(lo..after)),
                    (format!("{b}: from it"), Keep::range(after..hi)),
                ]);
            }
            keeps
        }

        /// Set keeps with whether the located kernel serves each, and whether
        /// the rows it admits form one contiguous run.
        fn set_keeps(&self) -> Vec<(&'static str, Keep, bool, bool)> {
            let Range { start: lo, end: hi } = self.subjects.clone();
            let mid = lo + (hi - lo) / 2;
            let cut = self.rows[self.boundaries[0]][0];
            vec![
                ("one", Keep::set([mid]), true, true),
                // Codes outside the subject block name no row.
                (
                    "few",
                    Keep::set([0, lo + 3, mid, mid + 9, hi - 1, hi + 5]),
                    true,
                    false,
                ),
                // Consecutive subjects' runs are one run.
                ("adjacent", Keep::set([mid, mid + 1, mid + 2]), true, true),
                (
                    "the cut run",
                    Keep::set([cut - 1, cut, cut + 1]),
                    true,
                    true,
                ),
                ("absent", Keep::set([0, 1, hi + 1]), true, true),
                // 1,200 codes: more than one per sixteen rows of any view.
                ("many", Keep::set((lo..hi).step_by(2)), false, false),
                // Consecutive, but still past it.
                (
                    "adjacent many",
                    Keep::set(mid - 500..mid + 500),
                    false,
                    true,
                ),
            ]
        }

        /// The run of file rows holding exactly the subjects `lo..hi`.
        fn run(&self, lo: TermCode, hi: TermCode) -> Range<u64> {
            let start = self.rows.partition_point(|row| row[0] < lo);
            let end = self.rows.partition_point(|row| row[0] < hi);
            start as u64..end.max(start) as u64
        }
    }

    /// The located run is exactly the rows whose subject code is in range:
    /// at the block's ends, around absent codes, around the run a leaf
    /// boundary cuts, and for an inverted range.
    #[tokio::test]
    async fn test_locate_subject_code_range_is_exact() {
        let fx = RunStore::open().await;
        let Range { start: lo, end: hi } = fx.subjects.clone();
        let mut edges = vec![
            0,
            1,
            lo - 1,
            lo,
            lo + 1,
            (lo + hi) / 2,
            hi - 1,
            hi,
            hi + 1,
            TermCode::MAX - 1,
            TermCode::MAX,
        ];
        for &b in &fx.boundaries {
            for code in [fx.rows[b - 1][0], fx.rows[b][0]] {
                edges.extend([code - 1, code, code + 1]);
            }
        }
        edges.sort_unstable();
        edges.dedup();
        for &a in &edges {
            for &b in &edges {
                assert_eq!(
                    fx.store.debug_subject_code_range(a..b).await.unwrap(),
                    Some(fx.run(a, b)),
                    "subject codes {a}..{b}"
                );
            }
        }
    }

    /// A range keep on the sorted subject column is a located run: every
    /// selection shape agrees with brute force in file order, nothing is
    /// streamed, and a run stays a range wherever the selection was one.
    #[tokio::test]
    async fn test_s_range_keeps_are_located() {
        let fx = RunStore::open().await;
        let views = fx.views().await;
        let streams = fx.store.debug_column_streams();
        for View { tag, view, sel } in &views {
            let base = row_list(view).await;
            let mut partial = 0;
            for (name, keep) in fx.range_keeps() {
                let narrowed = view.keep(QuadColumn::S, &keep).await.unwrap();
                let want = brute_keep(&base, QuadColumn::S, &keep);
                partial += usize::from(!want.is_empty() && want.len() < base.len());
                assert_eq!(row_list(&narrowed).await, want, "{tag}: {name}");
                assert_eq!(
                    narrowed.size().await.unwrap(),
                    want.len(),
                    "{tag}: {name}: size"
                );
                if !want.is_empty() {
                    assert_eq!(
                        narrowed.debug_selection_range().is_some(),
                        *sel != Sel::Ids,
                        "{tag}: {name}: a range selection stays a range"
                    );
                }
            }
            assert!(partial >= 6, "{tag}: the keeps must tell rows apart");
            // A keep chains on a keep: the located run of a located run.
            let Range { start: lo, end: hi } = fx.subjects.clone();
            let first_half = Keep::range(lo..(lo + hi) / 2);
            let slice = Keep::range(lo + 7..lo + 1_500);
            let once = view.keep(QuadColumn::S, &first_half).await.unwrap();
            let twice = once.keep(QuadColumn::S, &slice).await.unwrap();
            let want = brute_keep(
                &brute_keep(&base, QuadColumn::S, &first_half),
                QuadColumn::S,
                &slice,
            );
            assert!(!want.is_empty(), "{tag}: chained");
            assert_eq!(row_list(&twice).await, want, "{tag}: chained");
        }
        assert_eq!(
            fx.store.debug_column_streams(),
            streams,
            "no range keep streamed a column"
        );

        // On the whole store the located run is the selection itself.
        let Range { start: lo, end: hi } = fx.subjects.clone();
        let whole = fx
            .store
            .keep(QuadColumn::S, &Keep::range(lo..hi))
            .await
            .unwrap();
        assert_eq!(whole.debug_selection_range(), Some(0..fx.rows.len() as u64));
        let half = fx
            .store
            .keep(QuadColumn::S, &Keep::range(lo..(lo + hi) / 2))
            .await
            .unwrap();
        assert_eq!(
            half.debug_selection_range(),
            Some(fx.run(lo, (lo + hi) / 2))
        );
    }

    /// A set keep on the sorted subject column is looked up code by code
    /// while the set is small relative to its selection, and streamed past
    /// that: both agree with brute force in file order, and the admitted rows
    /// stay one range whenever they form one run.
    #[tokio::test]
    async fn test_s_set_keeps_are_located_up_to_a_bound() {
        let fx = RunStore::open().await;
        let views = fx.views().await;
        for View { tag, view, sel } in &views {
            let base = row_list(view).await;
            for (name, keep, located, contiguous) in fx.set_keeps() {
                let before = fx.store.debug_column_streams().unwrap();
                let narrowed = view.keep(QuadColumn::S, &keep).await.unwrap();
                assert_eq!(
                    fx.store.debug_column_streams().unwrap() - before,
                    usize::from(!located),
                    "{tag}: {name}: streamed only past the bound"
                );
                let want = brute_keep(&base, QuadColumn::S, &keep);
                assert_eq!(row_list(&narrowed).await, want, "{tag}: {name}");
                assert_eq!(
                    narrowed.size().await.unwrap(),
                    want.len(),
                    "{tag}: {name}: size"
                );
                if !want.is_empty() {
                    // Looked up, a run stays a range over a pending filter
                    // too; streamed, the filter is resolved to ids first.
                    let range_kept = match (located, sel) {
                        (_, Sel::Ids) => false,
                        (true, _) => contiguous,
                        (false, sel) => contiguous && *sel == Sel::Range,
                    };
                    assert_eq!(
                        narrowed.debug_selection_range().is_some(),
                        range_kept,
                        "{tag}: {name}: selection kind"
                    );
                }
            }
        }
        // The run of consecutive subjects is exactly their rows.
        let Range { start: lo, end: hi } = fx.subjects.clone();
        let mid = lo + (hi - lo) / 2;
        let adjacent = fx
            .store
            .keep(QuadColumn::S, &Keep::set([mid, mid + 1, mid + 2]))
            .await
            .unwrap();
        assert_eq!(adjacent.debug_selection_range(), Some(fx.run(mid, mid + 3)));
    }

    /// A set of more than one code per sixteen rows of its selection is
    /// cheaper to stream than to look up: of 160 rows, ten codes are looked
    /// up and eleven streamed.
    #[tokio::test]
    async fn test_s_set_keep_dense_in_its_selection_streams() {
        let (_dir, path) =
            write_store_file(fixture_quads(), LayoutStrategy::Dictionary, vec![]).await;
        let store = VortexRdfStore::from_file(&path).await.unwrap();
        let rows = row_list(&store).await;
        let window = store.window(100, 160).await.unwrap();
        // Subjects are unique, so each code names one of the window's rows.
        let own: Vec<TermCode> = rows[100..111].iter().map(|row| row[0]).collect();
        let streams = store.debug_column_streams().unwrap();

        let ten = window
            .keep(QuadColumn::S, &Keep::set(own[..10].iter().copied()))
            .await
            .unwrap();
        assert_eq!(store.debug_column_streams().unwrap(), streams);
        assert_eq!(row_list(&ten).await, rows[100..110]);

        let eleven = window
            .keep(QuadColumn::S, &Keep::set(own.iter().copied()))
            .await
            .unwrap();
        assert_eq!(store.debug_column_streams().unwrap(), streams + 1);
        assert_eq!(row_list(&eleven).await, rows[100..111]);
    }

    /// Located keeps bind no expression and read no column: however many
    /// distinct subject ranges and sets are kept, over the whole store or a
    /// pending filter, the file handle binds nothing new and streams nothing.
    /// Another column's keep still streams.
    #[tokio::test]
    async fn test_s_keeps_bind_no_expressions_and_stream_nothing() {
        let quads = fixture_quads();
        let (_dir, path) =
            write_store_file(quads.clone(), LayoutStrategy::Dictionary, vec![]).await;
        let store = VortexRdfStore::from_file(&path).await.unwrap();
        let (lo, hi) = store
            .dict_reader()
            .unwrap()
            .prefix_range("<http://example.org/s")
            .await
            .unwrap();
        assert!(hi - lo >= quads.len() as TermCode);
        let p1 = NamedNode::new("http://example.org/p1").unwrap();
        let pending = store
            .match_pattern(None, Some(&p1), None, None)
            .await
            .unwrap();
        // Warm the shapes a count and a row read bind on the pending view.
        let warm = pending.size().await.unwrap();
        view_strings(&pending).await;
        let bound = store.debug_bound_exprs().unwrap();
        let streams = store.debug_column_streams().unwrap();

        let whole = store
            .keep(QuadColumn::S, &Keep::range(lo..hi))
            .await
            .unwrap();
        assert_eq!(whole.debug_selection_range(), Some(0..quads.len() as u64));
        assert_eq!(whole.size().await.unwrap(), quads.len());
        let kept = pending
            .keep(QuadColumn::S, &Keep::range(lo..hi))
            .await
            .unwrap();
        assert_eq!(kept.size().await.unwrap(), warm);

        for i in 0..32u64 {
            let from = lo + 11 * i;
            let ranged = pending
                .keep(QuadColumn::S, &Keep::range(from..from + 100 + i))
                .await
                .unwrap();
            let set = Keep::set([from, from + 3, from + 9 + i]);
            let picked = store.keep(QuadColumn::S, &set).await.unwrap();
            assert_eq!(picked.size().await.unwrap(), 3, "keep {i}");
            assert!(ranged.size().await.unwrap() > 0, "keep {i}");
        }
        assert_eq!(store.debug_bound_exprs().unwrap(), bound);
        assert_eq!(store.debug_column_streams().unwrap(), streams);

        let (o_lo, _) = store
            .dict_reader()
            .unwrap()
            .prefix_range("\"o")
            .await
            .unwrap();
        pending
            .keep(QuadColumn::O, &Keep::set([o_lo]))
            .await
            .unwrap();
        assert_eq!(store.debug_column_streams().unwrap(), streams + 1);
    }

    /// A file that is not stamped sorted declines the located kernel whatever
    /// the range, and its keeps answer through the stream instead — still
    /// agreeing with brute force, and keeping a contiguous result a range.
    #[tokio::test]
    async fn test_s_keeps_on_an_unsorted_file_stream() {
        let (_dir, path) = write_unsorted_store_file(&fixture_quads(), 7).await;
        let store = VortexRdfStore::from_file(&path).await.unwrap();
        let (lo, hi) = store
            .dict_reader()
            .unwrap()
            .prefix_range("<http://example.org/s")
            .await
            .unwrap();
        let mid = lo + (hi - lo) / 2;
        for range in [lo..hi, lo..mid, 0..TermCode::MAX, mid..mid + 1] {
            assert_eq!(
                store.debug_subject_code_range(range).await.unwrap(),
                None,
                "an unsorted file declines"
            );
        }
        let p1 = NamedNode::new("http://example.org/p1").unwrap();
        let views = [
            ("whole", store.clone()),
            (
                "pending",
                store
                    .match_pattern(None, Some(&p1), None, None)
                    .await
                    .unwrap(),
            ),
        ];
        let keeps = [
            ("range", Keep::range(lo..mid)),
            // Past the seven rows the rotation moved to the end.
            ("run", Keep::range(lo + 10..mid)),
            // Subject i is quad i: these three are on the `p1` rows (i % 5 == 1).
            ("set", Keep::set([lo + 1, lo + 1501, lo + 2996])),
            ("many", Keep::set((lo..hi).step_by(8))),
        ];
        for (tag, view) in &views {
            let base = row_list(view).await;
            for (name, keep) in &keeps {
                let before = store.debug_column_streams().unwrap();
                let narrowed = view.keep(QuadColumn::S, keep).await.unwrap();
                assert_eq!(
                    store.debug_column_streams().unwrap(),
                    before + 1,
                    "{tag}: {name}: streamed"
                );
                let want = brute_keep(&base, QuadColumn::S, keep);
                assert!(!want.is_empty() && want.len() < base.len(), "{tag}: {name}");
                assert_eq!(row_list(&narrowed).await, want, "{tag}: {name}");
                assert_eq!(narrowed.size().await.unwrap(), want.len());
            }
        }
        // A range of consecutive file rows is one range once streamed.
        let run = views[0]
            .1
            .keep(QuadColumn::S, &Keep::range(lo + 10..mid))
            .await
            .unwrap();
        assert!(run.debug_selection_range().is_some());
    }

    /// The default writer's file past one scan split's worth of rows: subject
    /// keeps — ranges, small and mid-size sets looked up, a wide set streamed
    /// — over the whole file, a pending filter, a row window and an id list
    /// agree with brute force.
    async fn s_keeps_match_brute_force(path: &std::path::Path) {
        let store = VortexRdfStore::from_file(path).await.unwrap();
        let dict = store.dict_reader().unwrap();
        let (o_lo, _) = dict.prefix_range("\"object ").await.unwrap();
        let (lo, hi) = dict.prefix_range("<http://example.org/s").await.unwrap();
        let p0 = NamedNode::new("http://example.org/p0").unwrap();
        let views = [
            ("whole", store.clone()),
            (
                "filtered",
                store
                    .match_pattern(None, Some(&p0), None, None)
                    .await
                    .unwrap(),
            ),
            ("window", store.window(10_000, 140_000).await.unwrap()),
            (
                "ids",
                store
                    .keep(QuadColumn::O, &Keep::set([o_lo + 1, o_lo + 5]))
                    .await
                    .unwrap(),
            ),
        ];
        // (name, keep, looked up rather than streamed)
        let keeps = [
            ("first half", Keep::range(lo..lo + (hi - lo) / 2), true),
            ("1%", Keep::range(lo + 70_000..lo + 71_600), true),
            ("one", Keep::set([lo + 80_000]), true),
            (
                "few",
                Keep::set([lo + 5, lo + 40_000, lo + 80_001, lo + 159_999]),
                true,
            ),
            ("2,000", Keep::set((lo..hi).step_by(80)), true),
            ("20,000", Keep::set((lo..hi).step_by(8)), false),
        ];
        for (tag, view) in &views {
            let rows = row_set(view).await;
            for (name, keep, located) in &keeps {
                let before = store.debug_column_streams().unwrap();
                let narrowed = view.keep(QuadColumn::S, keep).await.unwrap();
                assert_eq!(
                    store.debug_column_streams().unwrap() - before,
                    usize::from(!located),
                    "{tag}: {name}: streamed or looked up"
                );
                let want = brute_keep(&rows, QuadColumn::S, keep);
                if *tag == "whole" {
                    // Every subject has one row, so every keep admits some.
                    assert!(!want.is_empty(), "{name}");
                }
                assert_eq!(row_set(&narrowed).await, want, "{tag}: {name}");
                assert_eq!(narrowed.size().await.unwrap(), want.len(), "{tag}: {name}");
            }
        }
    }
}
