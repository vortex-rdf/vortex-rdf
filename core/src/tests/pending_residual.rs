//! A residual compare left pending: an in-memory match that a count or a
//! window may stop short of (`count_probe` with a limit, `run_probe` with a
//! window) leaves its residual compare pending and compares the selection in
//! pieces, a zone's rows first and four times as many each after, only until
//! the count or the window has its live rows. Its answers are the eager
//! compare's, over every kind of selection, under tombstones and with a tail;
//! a count or a window that reaches every row, and every other use of a
//! match, compares eagerly.

use super::*;
use crate::io::zone_map::ZONE_ROWS;
use crate::store::view::pending::{FIRST_PIECE_ROWS, residual_hooks};

/// The rows of [`residual_quads`]: three pieces, the last of a thousand rows.
const ROWS: usize = 5 * ZONE_ROWS + 1_000;

/// [`ROWS`] quads, one per subject `s{i:06}`, so in row order, with the
/// predicate `p{i % 3}`, the object `"o{i % 50}"` and the default graph:
/// `p0` holds every third row, `"o7"` every fiftieth, both every 150th.
fn residual_quads() -> Vec<Quad> {
    (0..ROWS)
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{i:06}"),
                &format!("http://example.org/p{}", i % 3),
                &format!("o{}", i % 50),
                GraphName::DefaultGraph,
            )
        })
        .collect()
}

/// [`residual_quads`]' in-memory Dictionary stores without an index: built,
/// loaded from a file and opened from its bytes (whose code columns read
/// through their probes). Keep the `TempDir` alive for the stores'
/// lifetime.
async fn residual_stores() -> (tempfile::TempDir, Vec<(&'static str, VortexRdfStore)>) {
    let quads = residual_quads();
    let (dir, path) = write_store_file(quads.clone(), LayoutStrategy::Dictionary, vec![]).await;
    let bytes = std::fs::read(&path).unwrap();
    let stores = vec![
        (
            "built",
            built_store(quads, LayoutStrategy::Dictionary, vec![]).await,
        ),
        (
            "loaded",
            VortexRdfStore::from_file_in_memory(&path).await.unwrap(),
        ),
        (
            "from bytes",
            VortexRdfStore::from_bytes(&bytes).await.unwrap(),
        ),
    ];
    (dir, stores)
}

/// The probe of `(p, o)`, each `None` or the term of that name.
fn probe(p: Option<&str>, o: Option<&str>) -> Probe {
    Probe::new(None, p.map(iri), o.map(plain_literal), None)
}

/// A count at a limit compares the residual only until it has the limit's
/// rows: an ASK stops after the first piece, a count of more rows than it
/// holds after the next one, four times as large, and a count that finds
/// too few compares every piece to the end; each answer is the full
/// compare's.
#[tokio::test]
async fn a_count_compares_only_the_pieces_its_limit_needs() {
    let (_dir, stores) = residual_stores().await;
    // `p0` matches every third row: 2,731 in the first piece, 13,654 in the
    // first two.
    let first = FIRST_PIECE_ROWS.div_ceil(3);
    let two = (5 * FIRST_PIECE_ROWS).div_ceil(3);
    let p0 = probe(Some("p0"), None);
    for (label, store) in &stores {
        let deferred = residual_hooks::deferred();
        residual_hooks::take_pieces();
        let count = |limit: usize| {
            let probe = p0.clone().window(0, Some(limit));
            async move { store.count_probe(&probe).await }
        };
        assert_eq!(count(1).await.unwrap(), 1, "{label}: ASK");
        assert_eq!(
            residual_hooks::take_pieces(),
            [FIRST_PIECE_ROWS],
            "{label}: ASK compares the first piece"
        );
        assert_eq!(count(first + 1).await.unwrap(), first + 1, "{label}");
        assert_eq!(
            residual_hooks::take_pieces(),
            [FIRST_PIECE_ROWS, 4 * FIRST_PIECE_ROWS],
            "{label}: one row past the first piece's compares the second"
        );
        assert_eq!(count(two + 1).await.unwrap(), two + 1, "{label}");
        assert_eq!(
            residual_hooks::take_pieces(),
            [FIRST_PIECE_ROWS, 4 * FIRST_PIECE_ROWS, 1_000],
            "{label}: one row past the second piece's compares the rest"
        );
        // `"o7"` in `p0` (every 150th row, from row 57) has far fewer rows
        // than the limit: every piece is compared, to the end.
        let p0_o7 = probe(Some("p0"), Some("o7")).window(0, Some(ROWS / 2));
        let want = (ROWS - 57).div_ceil(150);
        assert_eq!(store.count_probe(&p0_o7).await.unwrap(), want, "{label}");
        assert_eq!(
            residual_hooks::take_pieces(),
            [FIRST_PIECE_ROWS, 4 * FIRST_PIECE_ROWS, 1_000],
            "{label}: a limit the rows never reach compares every piece"
        );
        assert_eq!(
            residual_hooks::deferred() - deferred,
            4,
            "{label}: every count left its compare pending"
        );
    }
}

/// A window compares the residual only until it has the window's last row,
/// its rows the eager match's: a first page stops after the first piece.
#[tokio::test]
async fn a_window_compares_only_the_pieces_it_reaches() {
    let (_dir, stores) = residual_stores().await;
    let p0 = probe(Some("p0"), None);
    for (label, store) in &stores {
        let eager = matched_view(store, None, Some(&iri("p0")), None, None)
            .await
            .quads_vec()
            .await
            .unwrap();
        residual_hooks::take_pieces();
        let page = store
            .run_probe(&p0.clone().window(10, Some(64)))
            .await
            .unwrap();
        assert_eq!(
            residual_hooks::take_pieces(),
            [FIRST_PIECE_ROWS],
            "{label}: a first page compares the first piece"
        );
        assert_eq!(page.quads_vec().await.unwrap(), eager[10..74], "{label}");
    }
}

/// Every count and window of a pending compare answers as the eager match:
/// patterns whose rows every piece holds (`p0`, `"o7"`, the default graph)
/// and fewer (`p1` with `"o7"`), at windows that end in each piece, at the
/// matches' end and past it, over the store, a window of it (a range from
/// its 1,000th row), a chained match's id list,
/// tombstones (among them every `p0` row with `"o0"`, the first `p0` row
/// included) and a tail holding matches of every pattern.
#[tokio::test]
async fn a_pending_compare_answers_as_the_eager_match() {
    let (_dir, stores) = residual_stores().await;
    let tail_quads = [
        make_quad(
            "http://example.org/tail0",
            "http://example.org/p0",
            "o7",
            GraphName::DefaultGraph,
        ),
        make_quad(
            "http://example.org/tail1",
            "http://example.org/p1",
            "o7",
            GraphName::DefaultGraph,
        ),
    ];
    let probes = [
        probe(Some("p0"), None),
        probe(None, Some("o7")),
        probe(Some("p1"), Some("o7")),
        Probe::new(None, None, None, Some(GraphName::DefaultGraph)),
    ];
    for (label, store) in &stores {
        let views = [
            ("plain", store.clone()),
            ("windowed", store.window(1_000, ROWS).await.unwrap()),
            (
                "chained",
                matched_view(store, None, Some(&iri("p0")), None, None).await,
            ),
            (
                "tombstoned",
                store
                    .delete_matching(None, Some(&iri("p0")), Some(&plain_literal("o0")), None)
                    .await
                    .unwrap(),
            ),
            ("tailed", store.add_quads(tail_quads.clone()).await.unwrap()),
        ];
        for (shape, view) in &views {
            for probe in &probes {
                let eager = view
                    .match_pattern(
                        None,
                        probe.predicate.as_ref(),
                        probe.object.as_ref(),
                        probe.graph.as_ref(),
                    )
                    .await
                    .unwrap()
                    .quads_vec()
                    .await
                    .unwrap();
                let n = eager.len();
                let tag = format!(
                    "{label}, {shape}, ({:?}, {:?}, {:?})",
                    probe.predicate, probe.object, probe.graph
                );
                for (offset, limit) in [
                    (0, 1),
                    (0, 64),
                    (5, 2_000),
                    (100, 3_000),
                    (0, 12_000),
                    (n.saturating_sub(3), 2),
                    (n.saturating_sub(3), 10),
                    (0, n.saturating_sub(1).max(1)),
                    (n, 1),
                    (n + 1, 5),
                ] {
                    let windowed = probe.clone().window(offset, Some(limit));
                    let end = offset.saturating_add(limit).min(n);
                    let want = &eager[offset.min(n)..end];
                    assert_eq!(
                        view.count_probe(&windowed).await.unwrap(),
                        want.len(),
                        "{tag}: count ({offset}, {limit})"
                    );
                    let got = view.run_probe(&windowed).await.unwrap();
                    assert_eq!(
                        got.quads_vec().await.unwrap(),
                        want,
                        "{tag}: window ({offset}, {limit})"
                    );
                    assert_eq!(
                        got.size().await.unwrap(),
                        want.len(),
                        "{tag}: window ({offset}, {limit}) size"
                    );
                }
            }
        }
    }
}

/// Tombstones that leave a pending compare no live row: a count compares
/// every piece and finds none, and a window takes nothing.
#[tokio::test]
async fn a_pending_compare_counts_only_live_rows() {
    let (_dir, stores) = residual_stores().await;
    let p0_o0 = probe(Some("p0"), Some("o0"));
    for (label, store) in &stores {
        let deleted = store
            .delete_matching(None, Some(&iri("p0")), Some(&plain_literal("o0")), None)
            .await
            .unwrap();
        residual_hooks::take_pieces();
        let ask = p0_o0.clone().window(0, Some(1));
        assert_eq!(deleted.count_probe(&ask).await.unwrap(), 0, "{label}");
        assert_eq!(
            residual_hooks::take_pieces(),
            [FIRST_PIECE_ROWS, 4 * FIRST_PIECE_ROWS, 1_000],
            "{label}: no live row stops the count early"
        );
        let page = deleted.run_probe(&ask).await.unwrap();
        assert!(page.quads_vec().await.unwrap().is_empty(), "{label}");
    }
}

/// A count or a window that reaches every row the selection holds compares
/// the residual eagerly, as do a probe with keeps, a window with no limit
/// and every match a caller reads row by row; and so does a selection no
/// wider than the first piece.
#[tokio::test]
async fn a_count_or_window_reaching_every_row_compares_eagerly() {
    let (_dir, stores) = residual_stores().await;
    let p0 = probe(Some("p0"), None);
    let p0_rows = ROWS.div_ceil(3);
    for (label, store) in &stores {
        let deferred = residual_hooks::deferred();
        let count = |probe: Probe| async move { store.count_probe(&probe).await.unwrap() };
        assert_eq!(count(p0.clone()).await, p0_rows, "{label}: no limit");
        assert_eq!(
            count(p0.clone().window(0, Some(ROWS))).await,
            p0_rows,
            "{label}: a limit of every row"
        );
        assert_eq!(
            count(p0.clone().window(3, None)).await,
            p0_rows - 3,
            "{label}: an offset alone"
        );
        assert_eq!(
            count(p0.clone().window(3, Some(ROWS - 3))).await,
            p0_rows - 3,
            "{label}: an offset and a limit that reach every row"
        );
        let kept = p0
            .clone()
            .keep(QuadColumn::O, Keep::range(0..TermCode::MAX))
            .window(0, Some(1));
        assert_eq!(count(kept).await, 1, "{label}: a probe with keeps");
        let page = store
            .run_probe(&p0.clone().window(0, Some(ROWS)))
            .await
            .unwrap();
        assert_eq!(page.size().await.unwrap(), p0_rows, "{label}: a window");
        assert!(
            matched_view(store, None, Some(&iri("p0")), None, None)
                .await
                .exists()
                .await
                .unwrap(),
            "{label}: match_pattern"
        );
        let narrow = store.window(0, FIRST_PIECE_ROWS).await.unwrap();
        assert_eq!(
            narrow
                .count_probe(&p0.clone().window(0, Some(1)))
                .await
                .unwrap(),
            1,
            "{label}: a selection of one piece"
        );
        assert_eq!(
            residual_hooks::deferred(),
            deferred,
            "{label}: no compare left pending"
        );
    }
}

/// A pending compare a count runs to the end keeps its matches: later counts
/// and windows of the same view compare nothing more, and under tombstones
/// still count and take only the live rows.
#[tokio::test]
async fn a_pending_compare_run_to_the_end_keeps_its_matches() {
    use crate::store::IdsNeed;

    let (_dir, stores) = residual_stores().await;
    let p0 = iri("p0");
    for (label, store) in &stores {
        // Every `p0` row with `"o0"` deleted, the first `p0` row among them.
        let store = store
            .delete_matching(None, Some(&p0), Some(&plain_literal("o0")), None)
            .await
            .unwrap();
        let eager = matched_view(&store, None, Some(&p0), None, None)
            .await
            .quads_vec()
            .await
            .unwrap();
        let need = IdsNeed::CountOrWindow { reach: 1 };
        let view = store
            .match_pattern_for(None, Some(&p0), None, None, need)
            .await
            .unwrap();
        assert!(view.debug_pending_without_plan(), "{label}: pending");
        residual_hooks::take_pieces();
        assert_eq!(
            view.size_capped(usize::MAX).await.unwrap(),
            eager.len(),
            "{label}: every live row"
        );
        assert_eq!(
            residual_hooks::take_pieces(),
            [ROWS],
            "{label}: a count of every row compares one piece"
        );
        assert_eq!(
            view.size_capped(usize::MAX).await.unwrap(),
            eager.len(),
            "{label}: every live row, again"
        );
        assert_eq!(view.size_capped(1).await.unwrap(), 1, "{label}");
        let page = view.window(2, 3).await.unwrap();
        assert_eq!(page.quads_vec().await.unwrap(), eager[2..5], "{label}");
        assert!(
            residual_hooks::take_pieces().is_empty(),
            "{label}: the kept matches answer"
        );
    }
}
