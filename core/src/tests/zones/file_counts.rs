//! File general-match counts take every-row zones from their sizes: a
//! pushed filter of code equalities prunes each equality's negation
//! (`col != c`), and the zones where no row can differ are taken without
//! evaluating the filter; the filter runs over the rest.

use super::*;
use crate::store::test_hooks::ForcedZones;

/// Rows of [`whole_quads`]: eight zones.
const ROWS: usize = 8 * ZONE_ROWS;

/// The graph of row `i`, by zone: zones 0 to 3 graph A, zones 4 and 5
/// graph B, zone 6 A and B in turn row by row, and zone 7 C and D in turn,
/// so C and D are alone in no zone.
fn graph_of(i: usize) -> GraphName {
    let name = match (i / ZONE_ROWS, i % 2) {
        (0..=3, _) => "A",
        (4 | 5, _) => "B",
        (6, 0) => "A",
        (6, _) => "B",
        (_, 0) => "C",
        _ => "D",
    };
    iri_graph(name)
}

/// [`ROWS`] quads `(s{i:05}, type, "Taxon", graph_of(i))`, in row order: the
/// layout of the located bench's whole file (its `whole_*` cells), its last
/// zone split between two graphs.
fn whole_quads() -> Vec<Quad> {
    (0..ROWS)
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{i:05}"),
                "http://example.org/type",
                "Taxon",
                graph_of(i),
            )
        })
        .collect()
}

/// The rows a recorder saw a file filter take from every-row zones, and
/// evaluate.
fn split_rows(rec: &Recorder) -> (u64, u64) {
    let all = rec.rows(|e| match e {
        Event::SplitAllRows(rows) => Some(*rows),
        _ => None,
    });
    let evaluated = rec.rows(|e| match e {
        Event::SplitFilterRows(rows) => Some(*rows),
        _ => None,
    });
    (all, evaluated)
}

/// The count of `(?, p, ?, g)` on `store`, through the general match.
async fn count(store: &VortexRdfStore, p: Option<&str>, g: &str) -> usize {
    let probe = Probe::new(None, p.map(iri), None, Some(iri_graph(g)));
    store.count_probe(&probe).await.unwrap()
}

/// On a file without an index, `(? ? ? A)` and `(? type ? A)` count the four
/// zones of graph A alone from their sizes and evaluate the filter on zone 6
/// alone, the zones of other graphs pruned; `(? ? ? C)`, alone in no zone,
/// evaluates the filter over every zone; zones off, no zone is taken.
#[tokio::test]
async fn file_counts_take_every_row_zones_from_sizes() {
    let _zones_on = crate::store::test_hooks::ForcedZones::set(true);
    let (_dir, path) = write_store_file(whole_quads(), LayoutStrategy::Dictionary, vec![]).await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let zone = ZONE_ROWS as u64;
    for (shape, p) in [("(? ? ? A)", None), ("(? type ? A)", Some("type"))] {
        let rec = Recorder::start();
        assert_eq!(
            count(&store, p, "A").await,
            4 * ZONE_ROWS + ZONE_ROWS / 2,
            "{shape}"
        );
        assert_eq!(
            split_rows(&rec),
            (4 * zone, zone),
            "{shape}: zones 0 to 3 taken, zone 6 evaluated"
        );
    }
    let rec = Recorder::start();
    assert_eq!(count(&store, None, "C").await, ZONE_ROWS / 2);
    assert_eq!(split_rows(&rec), (0, zone), "C: zone 7 evaluated");

    let _zones = ForcedZones::set(false);
    let rec = Recorder::start();
    assert_eq!(
        count(&store, None, "A").await,
        4 * ZONE_ROWS + ZONE_ROWS / 2
    );
    assert_eq!(
        split_rows(&rec),
        (0, 5 * zone),
        "zones off: every zone of A evaluated"
    );
}

/// Rows deleted in zones of graph A alone and in zone 6: the count is the
/// live rows', the deleted rows left out of the zones taken.
#[tokio::test]
async fn file_all_side_respects_tombstones() {
    let _zones_on = crate::store::test_hooks::ForcedZones::set(true);
    let (_dir, path) = write_store_file(whole_quads(), LayoutStrategy::Dictionary, vec![]).await;
    let mut store = VortexRdfStore::from_file(&path).await.unwrap();
    for i in [100, ZONE_ROWS + 8, 6 * ZONE_ROWS + 2] {
        let subject = NamedOrBlankNode::from(iri(&format!("s{i:05}")));
        store = store
            .delete_matching(Some(&subject), None, None, None)
            .await
            .unwrap();
    }
    let want = 4 * ZONE_ROWS + ZONE_ROWS / 2 - 3;
    let rec = Recorder::start();
    assert_eq!(count(&store, None, "A").await, want, "the live rows");
    let (all, _) = split_rows(&rec);
    assert_eq!(
        all,
        4 * ZONE_ROWS as u64 - 2,
        "the zones taken, their deleted rows left out"
    );
    let _zones = ForcedZones::set(false);
    assert_eq!(count(&store, None, "A").await, want, "as with zones off");
}

/// A filter's every-row zones come from the quad table's zone maps, each
/// read once per file whatever the splits and the filters: the first count
/// of `(? ? ? A)` reads `g`'s, a count of graph B reads none more, and
/// `(? type ? A)` reads `p`'s alone.
#[tokio::test]
async fn every_row_zones_read_each_zone_map_once() {
    let _zones = ForcedZones::set(true);
    let (_dir, path) = write_store_file(whole_quads(), LayoutStrategy::Dictionary, vec![]).await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let zone = ZONE_ROWS;
    for (p, g, want, taken, read) in [
        (None, "A", 4 * zone + zone / 2, 4 * zone, 1),
        (None, "B", 2 * zone + zone / 2, 2 * zone, 0),
        (Some("type"), "A", 4 * zone + zone / 2, 4 * zone, 1),
        (Some("type"), "B", 2 * zone + zone / 2, 2 * zone, 0),
    ] {
        let rec = Recorder::start();
        assert_eq!(count(&store, p, g).await, want, "{p:?} {g}");
        assert_eq!(split_rows(&rec).0, taken as u64, "{p:?} {g}: zones taken");
        assert_eq!(
            rec.count(&Event::ZoneMapRead),
            read,
            "{p:?} {g}: zone maps read"
        );
    }
}
