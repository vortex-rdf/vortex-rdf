//! Narrowing beyond a pattern — keeps, windows, capped counts — checked
//! against brute force over the gathered code columns, on every backend
//! and over served, scanned and chained views.

use super::*;
use crate::store::{Keep, QuadColumn};

/// The row tuples of a view's gathered codes, sorted — order-free, since a
/// served view gathers in its index's order and a narrowed one in base
/// order.
async fn row_set(view: &VortexRdfStore) -> Vec<[u32; 4]> {
    let mut rows = row_list(view).await;
    rows.sort_unstable();
    rows
}

/// The row tuples of a view's gathered codes, in the order gathered.
async fn row_list(view: &VortexRdfStore) -> Vec<[u32; 4]> {
    let cols = view
        .code_columns_gathered()
        .await
        .unwrap()
        .expect("a Dictionary-layout view without a tail gathers codes");
    (0..cols[0].len())
        .map(|i| [cols[0][i], cols[1][i], cols[2][i], cols[3][i]])
        .collect()
}

/// The view's rows in base row order: a keep that admits every code drops
/// the serve plan without dropping a row.
async fn base_ordered(view: &VortexRdfStore) -> Vec<[u32; 4]> {
    let all = Keep::range(0..u32::MAX);
    row_list(&view.keep(QuadColumn::S, &all).await.unwrap()).await
}

fn brute_keep(rows: &[[u32; 4]], column: QuadColumn, keep: &Keep) -> Vec<[u32; 4]> {
    rows.iter()
        .copied()
        .filter(|row| keep.admits(row[column.index()]))
        .collect()
}

fn fixture_quads() -> Vec<Quad> {
    let g = |name: &str| {
        GraphName::NamedNode(NamedNode::new(format!("http://example.org/{name}")).unwrap())
    };
    graph_modular_quads(3_000, 4, 5, 7, &[GraphName::DefaultGraph, g("g1"), g("g2")])
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
    let codes: Vec<u32> = (lo..hi).collect();
    vec![
        Keep::range(lo..hi),
        Keep::range(lo..mid),
        Keep::range(mid..mid + 1),
        Keep::set(codes.iter().copied().step_by(3)),
        Keep::set(codes.iter().copied().step_by(97)),
        Keep::set([mid]),
        Keep::set([0]),
        Keep::range(0..u32::MAX),
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
                let want_chained: Vec<[u32; 4]> =
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
/// base-ordered rows.
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
        let rows = base_ordered(view).await;
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
            let want: Vec<[u32; 4]> = rows.iter().copied().skip(offset).take(limit).collect();
            assert_eq!(
                row_list(&windowed).await,
                want,
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
            let want_inner: Vec<[u32; 4]> = want.iter().copied().skip(1).take(2).collect();
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

#[tokio::test]
async fn test_keep_in_memory_matches_brute_force() {
    for (tag, indexes) in [
        ("no-index", vec![]),
        ("copy", vec![IndexType::SecondaryByCopy]),
        ("reference", vec![IndexType::SecondaryByReference]),
    ] {
        let store = memory_store(indexes).await;
        assert_keeps_match_brute_force(&store, tag).await;
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

#[cfg(feature = "file-io")]
mod file {
    use super::*;

    /// Both open modes of a file store — loaded whole and mapped — with and
    /// without indexes (the dir guards ride along, shared by both opens).
    async fn file_stores() -> Vec<(String, std::sync::Arc<tempfile::TempDir>, VortexRdfStore)> {
        let mut stores = Vec::new();
        for (tag, indexes) in [
            ("file/no-index", vec![]),
            ("file/copy", vec![IndexType::SecondaryByCopy]),
            ("file/reference", vec![IndexType::SecondaryByReference]),
        ] {
            let (dir, path) =
                write_store_file(fixture_quads(), LayoutStrategy::Dictionary, indexes).await;
            let loaded = VortexRdfStore::from_file_in_memory(&path).await.unwrap();
            let mapped = VortexRdfStore::from_file(&path).await.unwrap();
            assert!(mapped.debug_dict_file_backed());
            let dir = std::sync::Arc::new(dir);
            stores.push((format!("{tag}/loaded"), std::sync::Arc::clone(&dir), loaded));
            stores.push((format!("{tag}/mapped"), dir, mapped));
        }
        stores
    }

    #[tokio::test]
    async fn test_keep_on_file_matches_brute_force() {
        for (tag, _dir, store) in file_stores().await {
            assert_keeps_match_brute_force(&store, &tag).await;
        }
    }

    #[tokio::test]
    async fn test_window_on_file_matches_slicing() {
        for (tag, _dir, store) in file_stores().await {
            assert_windows_match_slicing(&store, &tag).await;
        }
    }

    /// A very wide set — beyond what the scan takes as an expression — is
    /// tested in memory over the column and still agrees with brute force,
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
}
