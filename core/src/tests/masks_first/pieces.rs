//! A filter fold that stops at fewer matches than a zone holds — `exists`, a
//! small capped size or window — evaluates a split in pieces: its first zone,
//! then pieces four times larger, so matches in the first rows end the fold
//! there. A fold whose cap one zone cannot hold evaluates whole splits, and a
//! window that keeps the masks it found keeps one a split, the pieces of each
//! joined.

use super::*;
use crate::store::scan::file_filter::split_hooks;

/// The rows of one leaf, and so of one split, of [`pieces_file`]: three
/// zones.
const PIECES_SPLIT_ROWS: u64 = 24_576;
/// The splits of [`pieces_file`].
const PIECES_SPLITS: u64 = 2;
/// The rows a fold that may stop early evaluates first: one zone.
const FIRST_PIECE: u64 = 8_192;
/// The first row of [`pieces_quads`] with the object `"late"`.
const LATE_FROM: usize = 10_000;

/// [`PIECES_SPLITS`] × [`PIECES_SPLIT_ROWS`] quads with subjects `s{i:05}`,
/// so in row order: the graph `gd` on every other row, the predicate `pm` on
/// every eighth, and the object `"late"` on every thousandth row from
/// [`LATE_FROM`] on, past the first zone. Any other row has the predicate
/// `p{i % 3}`, the object `"o{i % 7}"` and the default graph.
fn pieces_quads() -> Vec<Quad> {
    let gd = iri_graph("gd");
    (0..(PIECES_SPLITS * PIECES_SPLIT_ROWS) as usize)
        .map(|i| {
            let p = if i % 8 == 0 {
                "http://example.org/pm".to_string()
            } else {
                format!("http://example.org/p{}", i % 3)
            };
            let o = if i >= LATE_FROM && i % 1_000 == 0 {
                "late".to_string()
            } else {
                format!("o{}", i % 7)
            };
            let g = if i % 2 == 1 {
                gd.clone()
            } else {
                GraphName::DefaultGraph
            };
            make_quad(&format!("http://example.org/s{i:05}"), &p, &o, g)
        })
        .collect()
}

/// [`pieces_quads`] as a Dictionary-layout file of [`PIECES_SPLIT_ROWS`]-row
/// leaves with no index, mapped, in a fresh temp dir. Serialized once per
/// process (see [`cached_store_bytes`]). Keep the `TempDir` alive for the
/// store's lifetime.
async fn pieces_store() -> (tempfile::TempDir, VortexRdfStore) {
    static BYTES: OnceLock<Vec<u8>> = OnceLock::new();
    let bytes = cached_store_bytes(&BYTES, || async {
        let (_dir, path) =
            write_chunked_store_file(&pieces_quads(), PIECES_SPLIT_ROWS as usize).await;
        std::fs::read(&path).unwrap()
    })
    .await;
    let (dir, path) = temp_store_file(bytes);
    assert_eq!(
        open_mapped(&path).await.splits().unwrap().len(),
        PIECES_SPLITS as usize
    );
    (dir, VortexRdfStore::from_file(&path).await.unwrap())
}

/// A capped count stops at the piece whose matches reach its cap: the first
/// zone for `exists` and for a cap the zone's matches reach, the rest of the
/// split next. A cap of a zone's rows, which no first piece could reach,
/// evaluates whole splits. Each count is the cap.
#[tokio::test]
#[allow(
    clippy::single_range_in_vec_init,
    reason = "the consumed pieces are a list of ranges, and some cases hold just one"
)]
async fn a_capped_count_stops_at_the_piece_that_reaches_it() {
    let (_dir, store) = pieces_store().await;
    let view = matched_view(&store, None, None, None, Some(&iri_graph("gd"))).await;
    assert!(view.view_statistics().unwrap().pending_filter);
    // `gd` is on every other row.
    let per_zone = (FIRST_PIECE / 2) as usize;
    let rest = FIRST_PIECE..PIECES_SPLIT_ROWS;
    for (cap, consumed) in [
        (1, vec![0..FIRST_PIECE]),
        (per_zone, vec![0..FIRST_PIECE]),
        (per_zone + 1, vec![0..FIRST_PIECE, rest]),
        (FIRST_PIECE as usize, vec![0..PIECES_SPLIT_ROWS]),
    ] {
        split_hooks::take_consumed_ranges();
        assert_eq!(view.size_capped(cap).await.unwrap(), cap, "cap {cap}");
        assert_eq!(split_hooks::take_consumed_ranges(), consumed, "cap {cap}");
    }
    split_hooks::take_consumed_ranges();
    assert!(view.exists().await.unwrap());
    assert_eq!(
        split_hooks::take_consumed_ranges(),
        vec![0..FIRST_PIECE],
        "exists"
    );
}

/// A window of fewer rows than a zone stops at the piece that fills it: the
/// first zone for matches on every other row, the piece after it for matches
/// that start past it. Its rows are the first matches either way.
#[tokio::test]
#[allow(
    clippy::single_range_in_vec_init,
    reason = "the consumed pieces are a list of ranges, and some cases hold just one"
)]
async fn a_window_stops_at_the_piece_that_fills_it() {
    let (_dir, store) = pieces_store().await;
    let late = plain_literal("late");
    let gd = iri_graph("gd");
    let cases = [
        ("G", None, Some(&gd), 100, vec![0..FIRST_PIECE]),
        (
            "O",
            Some(&late),
            None,
            3,
            vec![0..FIRST_PIECE, FIRST_PIECE..PIECES_SPLIT_ROWS],
        ),
    ];
    for (label, o, g, limit, consumed) in cases {
        let view = matched_view(&store, None, None, o, g).await;
        let rows = row_list(&view).await;
        assert!(rows.len() > limit, "{label}: the window cuts");
        split_hooks::take_consumed_ranges();
        let windowed = view.window(0, limit).await.unwrap();
        assert_eq!(split_hooks::take_consumed_ranges(), consumed, "{label}");
        assert_eq!(
            row_list(&windowed).await,
            rows[..limit],
            "{label}: the first matches"
        );
    }
}

/// A window of fewer rows than a zone that finds fewer matches than its
/// limit has evaluated every split, the first in pieces. It keeps the masks,
/// one a split, the pieces of the first joined, and reads the plain view's
/// rows from them.
#[tokio::test]
async fn a_window_keeps_one_mask_a_split_whatever_its_pieces() {
    let (_dir, store) = pieces_store().await;
    // `pm` on every eighth row: fewer matches than a zone has rows, and
    // dense enough for masks.
    let view = matched_view(&store, None, Some(&iri("pm")), None, None).await;
    let rows = row_list(&view).await;
    let n = rows.len();
    assert_eq!(n as u64, PIECES_SPLITS * PIECES_SPLIT_ROWS / 8);
    assert!(((n + 1) as u64) < FIRST_PIECE);
    let second = PIECES_SPLIT_ROWS..2 * PIECES_SPLIT_ROWS;
    split_hooks::take_consumed_ranges();
    let windowed = view.window(0, n + 1).await.unwrap();
    assert_eq!(
        split_hooks::take_consumed_ranges(),
        vec![
            0..FIRST_PIECE,
            FIRST_PIECE..PIECES_SPLIT_ROWS,
            second.clone()
        ],
        "every piece"
    );
    assert_eq!(
        windowed.debug_evaluated_ranges(),
        Some(vec![0..PIECES_SPLIT_ROWS, second]),
        "one mask a split"
    );
    assert_eq!(windowed.debug_evaluated_len(), Some(n));
    assert_eq!(row_list(&windowed).await, rows, "the plain rows");
}
