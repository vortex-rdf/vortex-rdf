//! The in-memory residual on its zone classes: the zones a residual's
//! selection touches are classed once against its equalities, "none" zones
//! skipped, "all" zones taken uncompared, and mixed zones compared on the
//! columns they leave unsettled. Its answers are the row-by-row compare's.

use super::located_zones::{Graphs, RUN, zone_quads};
use super::*;
use crate::io::zone_map::ZONE_ROWS;
use crate::store::test_hooks::ForcedZones;

/// Every [`Graphs`] layout.
const LAYOUTS: [Graphs; 8] = [
    Graphs::One,
    Graphs::Long,
    Graphs::Stretches,
    Graphs::Alternating,
    Graphs::Gapped,
    Graphs::Seventeen,
    Graphs::DefaultAndThree,
    Graphs::RecordedByTurns,
];

/// The rows of a code read, as tuples.
fn tuples(columns: &[vortex_buffer::Buffer<TermCode>; 4]) -> Vec<[TermCode; 4]> {
    (0..columns[0].len())
        .map(|i| std::array::from_fn(|c| columns[c][i]))
        .collect()
}

/// What a read of `probe` answers on `store`: its count, a count capped
/// from its middle, its windows from its first, middle and last row, one
/// row and 64 rows long, and every row in order (base rows, then tail
/// rows).
#[derive(Debug, PartialEq, Eq)]
struct Answers {
    count: usize,
    capped: usize,
    windows: Vec<Vec<String>>,
    rows: Vec<String>,
}

/// The rows of `view`, in the order its reads yield them: their codes, or
/// for a view with a tail (which has none to gather) its quads.
async fn quads_of(view: &VortexRdfStore) -> Vec<String> {
    match view.code_columns_gathered().await.unwrap() {
        Some(columns) => code_rows::<4>(&columns)
            .iter()
            .map(|row| format!("{row:?}"))
            .collect(),
        None => view
            .quads_vec()
            .await
            .unwrap()
            .iter()
            .map(ToString::to_string)
            .collect(),
    }
}

async fn answers(store: &VortexRdfStore, probe: &Probe) -> Answers {
    let count = store.count_probe(probe).await.unwrap();
    let capped = store
        .count_probe(&Probe {
            offset: count / 2,
            limit: Some(100),
            ..probe.clone()
        })
        .await
        .unwrap();
    let mut windows = Vec::new();
    for offset in [0, count / 2, count.saturating_sub(1)] {
        for limit in [1, 64] {
            let window = Probe {
                offset,
                limit: Some(limit),
                ..probe.clone()
            };
            windows.push(quads_of(&store.run_probe(&window).await.unwrap()).await);
        }
    }
    let rows = quads_of(&store.run_probe(probe).await.unwrap()).await;
    Answers {
        count,
        capped,
        windows,
        rows,
    }
}

/// The probes asked of a store, by graph: `(? ? ? g)`, `(? type ? g)` and
/// `(hub ? ? g)`, for the default graph and graphs 1 to 3 (a graph the
/// store lacks matches nothing).
fn probes() -> Vec<(String, Probe)> {
    let hub = NamedOrBlankNode::from(iri("hub"));
    let mut probes = Vec::new();
    for k in 0..=3 {
        let g = numbered_graph(k);
        probes.push((
            format!("(? ? ? g{k})"),
            Probe::new(None, None, None, Some(g.clone())),
        ));
        probes.push((
            format!("(? type ? g{k})"),
            Probe::new(None, Some(iri("type")), None, Some(g.clone())),
        ));
        probes.push((
            format!("(hub ? ? g{k})"),
            Probe::new(Some(hub.clone()), None, None, Some(g)),
        ));
    }
    probes
}

/// `store`'s answers to every probe, with zones on and off, equal.
async fn assert_zones_answer_as_rows_do(tag: &str, store: &VortexRdfStore) {
    for (shape, probe) in probes() {
        let off = {
            let _zones = ForcedZones::set(false);
            answers(store, &probe).await
        };
        let on = {
            let _zones = ForcedZones::set(true);
            answers(store, &probe).await
        };
        assert_eq!(on, off, "{tag} {shape}");
    }
}

/// `store` with a tombstone in every zone: the first row of each zone of
/// both runs, deleted by subject or by object.
async fn tombstoned(store: &VortexRdfStore) -> VortexRdfStore {
    let mut store = store.clone();
    for i in (0..RUN).step_by(ZONE_ROWS) {
        let subject = NamedOrBlankNode::from(iri(&format!("s{i:05}")));
        store = store
            .delete_matching(Some(&subject), None, None, None)
            .await
            .unwrap();
        let object = plain_literal(&format!("o{i:05}"));
        store = store
            .delete_matching(None, None, Some(&object), None)
            .await
            .unwrap();
    }
    store
}

/// `store` with a tail of quads in graphs 1 and 3 and the default graph.
async fn tailed(store: &VortexRdfStore) -> VortexRdfStore {
    let quads = (0..4).map(|k| {
        make_quad(
            &format!("http://example.org/tail{k}"),
            "http://example.org/type",
            "Taxon",
            numbered_graph([0, 1, 3, 1][k]),
        )
    });
    store.add_quads(quads).await.unwrap()
}

/// Zones on or off, a residual answers the same: counts, windows and every
/// row in order, on built and loaded stores of every graph layout, with no
/// index and with the copy index, and with tombstones and a tail.
#[tokio::test]
async fn residual_zones_answer_as_rows_do() {
    for graphs in LAYOUTS {
        for index in [IndexConfig::NoIndex, IndexConfig::Copy] {
            let quads = zone_quads(graphs);
            let built = built_store(quads.clone(), LayoutStrategy::Dictionary, index.types()).await;
            let (_dir, path) =
                write_store_file(quads, LayoutStrategy::Dictionary, index.types()).await;
            let loaded = VortexRdfStore::from_file_in_memory(&path).await.unwrap();
            for (backend, store) in [("built", built), ("loaded", loaded)] {
                let tag = format!("{graphs:?} {index:?} {backend}");
                assert_zones_answer_as_rows_do(&tag, &store).await;
                assert_zones_answer_as_rows_do(
                    &format!("{tag} tombstoned"),
                    &tombstoned(&store).await,
                )
                .await;
                assert_zones_answer_as_rows_do(&format!("{tag} tailed"), &tailed(&store).await)
                    .await;
            }
        }
    }
}

/// `(? ? ? g1)` on `Graphs::Long` in memory: each run's first half is graph
/// 1, so the zones of graph 1 alone are taken uncompared, the zones where
/// graphs 1 and 2 meet (inside each run, and where the runs meet) are
/// compared, and the zones of graph 2 alone are skipped.
#[tokio::test]
async fn all_zones_are_taken_uncompared() {
    let store = built_store(zone_quads(Graphs::Long), LayoutStrategy::Dictionary, vec![]).await;
    let g1 = iri_graph("g1");
    let read = |on: bool| {
        let (store, g1) = (&store, &g1);
        async move {
            let _zones = ForcedZones::set(on);
            let rec = Recorder::start();
            let view = store
                .match_pattern(None, None, None, Some(g1))
                .await
                .unwrap();
            let rows = row_list(&view).await;
            let sum = |taken: bool| -> u64 {
                rec.picked(|e| match (e, taken) {
                    (Event::ResidualZoneTaken(rows), true)
                    | (Event::ResidualCompared(rows), false) => Some(*rows),
                    _ => None,
                })
                .iter()
                .sum()
            };
            (rows, sum(true), sum(false))
        }
    };
    let (on, taken, compared) = read(true).await;
    let (off, _, _) = read(false).await;
    assert_eq!(on.len(), RUN, "graph 1's rows");
    assert_eq!(on, off, "the rows and their order, as with zones off");
    assert_eq!(taken, 4 * ZONE_ROWS as u64, "the zones of graph 1 alone");
    assert_eq!(
        compared,
        3 * ZONE_ROWS as u64,
        "the zones where the graphs meet"
    );
}

/// A window of a deferred residual, compared in several pieces, classes
/// the residual's zones once.
#[tokio::test]
async fn a_pending_residual_classes_once() {
    let store = built_store(zone_quads(Graphs::Long), LayoutStrategy::Dictionary, vec![]).await;
    let g1 = super::located_zones::code_of(&store, "<http://example.org/g1>").await;
    let pattern = crate::CodePattern::new(None, None, None, Some(g1));
    let window = |on: bool| {
        let store = &store;
        async move {
            let _zones = ForcedZones::set(on);
            let rec = Recorder::start();
            let rows = tuples(
                &store
                    .match_code_pattern(pattern, 30_000, Some(64))
                    .await
                    .unwrap(),
            );
            (
                rows,
                rec.count_where(|e| matches!(e, Event::ZonesClassed(_))),
                rec.count_where(|e| matches!(e, Event::ResidualPiece(_))),
                rec.count(&Event::ResidualDeferred),
            )
        }
    };
    let (on, classed, pieces, deferred) = window(true).await;
    let (off, ..) = window(false).await;
    assert_eq!(on.len(), 64);
    assert_eq!(on, off, "the window, as with zones off");
    assert_eq!(deferred, 1, "a deferred residual");
    assert!(pieces > 1, "compared in {pieces} pieces");
    assert_eq!(classed, 1, "its zones classed once");
}

/// The rows a recorder saw counted each way: `(taken, recorded, counted,
/// compared)`, from "all" zones, from records, on the encoding, and by a
/// compare.
fn zone_rows_seen(rec: &Recorder) -> (u64, u64, u64, u64) {
    let sum = |pick: fn(&Event) -> Option<u64>| -> u64 { rec.picked(pick).iter().sum() };
    (
        sum(|e| match e {
            Event::ResidualZoneTaken(rows) => Some(*rows),
            _ => None,
        }),
        sum(|e| match e {
            Event::ZoneRecorded(rows) => Some(*rows),
            _ => None,
        }),
        sum(|e| match e {
            Event::ZoneCounted(rows) => Some(*rows),
            _ => None,
        }),
        sum(|e| match e {
            Event::ResidualCompared(rows) => Some(*rows),
            _ => None,
        }),
    )
}

/// `(? ? ? g1)` on `Graphs::Alternating` in memory: every zone holds
/// graphs 1 to 4 in turn, so every zone is mixed, and its count takes each
/// whole zone's rows of graph 1 from the zone's record, comparing no row;
/// `(hub ? ? g1)`, over `hub`'s range, counts the zone its range ends
/// inside on the encoding.
#[tokio::test]
async fn residual_counts_settle_zones() {
    use super::located_zones::code_of;

    let store = built_store(
        zone_quads(Graphs::Alternating),
        LayoutStrategy::Dictionary,
        vec![],
    )
    .await;
    let g1 = code_of(&store, "<http://example.org/g1>").await;
    let hub = code_of(&store, "<http://example.org/hub>").await;

    let rec = Recorder::start();
    let every = crate::CodePattern::new(None, None, None, Some(g1));
    assert_eq!(
        store.count_code_pattern(every).await.unwrap(),
        (RUN / 2) as u64
    );
    let (taken, recorded, counted, compared) = zone_rows_seen(&rec);
    assert_eq!(compared, 0, "no row compared");
    assert_eq!(
        recorded,
        2 * RUN as u64,
        "every zone counted from its record"
    );
    assert_eq!((taken, counted), (0, 0));

    let rec = Recorder::start();
    let hubs = crate::CodePattern::new(Some(hub), None, None, Some(g1));
    assert_eq!(
        store.count_code_pattern(hubs).await.unwrap(),
        (RUN / 4) as u64
    );
    let (_, recorded, counted, compared) = zone_rows_seen(&rec);
    assert_eq!(compared, 0, "no row compared");
    assert!(
        counted > 0,
        "the range's partial zone counted on the encoding"
    );
    assert_eq!(recorded + counted, RUN as u64, "every row of the range");
}

/// Counts after deleting a row in an "all" zone of `(? ? ? g1)` and rows of
/// both graphs in a mixed one, on `Graphs::Long` in memory: they equal the
/// rows', and the mixed zone with tombstones is compared, not counted from
/// its record.
#[tokio::test]
async fn residual_counts_respect_tombstones() {
    use super::located_zones::code_of;

    let store = built_store(zone_quads(Graphs::Long), LayoutStrategy::Dictionary, vec![]).await;
    let g1 = iri_graph("g1");
    // Each zone's graphs, in base order.
    let quads = store.quads_vec().await.unwrap();
    let zones: Vec<&[Quad]> = quads.chunks(ZONE_ROWS).collect();
    let holds_g1 = |zone: &[Quad]| zone.iter().filter(|q| q.graph_name == g1).count();
    let all = zones
        .iter()
        .position(|zone| holds_g1(zone) == zone.len())
        .expect("a zone of graph 1 alone");
    let mixed = zones
        .iter()
        .position(|zone| (1..zone.len()).contains(&holds_g1(zone)))
        .expect("a zone of both graphs");
    let doomed = [
        zones[all][0].clone(),
        zones[mixed]
            .iter()
            .find(|q| q.graph_name == g1)
            .unwrap()
            .clone(),
        zones[mixed]
            .iter()
            .find(|q| q.graph_name != g1)
            .unwrap()
            .clone(),
    ];
    let mut deleted = store.clone();
    for quad in &doomed {
        let object = quad.object.clone();
        deleted = deleted
            .delete_matching(
                Some(&quad.subject),
                Some(&quad.predicate),
                Some(&object),
                Some(&quad.graph_name),
            )
            .await
            .unwrap();
    }
    let code = code_of(&store, "<http://example.org/g1>").await;
    let pattern = crate::CodePattern::new(None, None, None, Some(code));
    let count = |on: bool| {
        let deleted = &deleted;
        async move {
            let _zones = ForcedZones::set(on);
            let rec = Recorder::start();
            let n = deleted.count_code_pattern(pattern).await.unwrap();
            (n, zone_rows_seen(&rec))
        }
    };
    let (on, (taken, recorded, _, compared)) = count(true).await;
    let (off, _) = count(false).await;
    assert_eq!(on, (RUN - 2) as u64, "graph 1's rows, two deleted");
    assert_eq!(on, off, "as with zones off");
    assert_eq!(
        compared, ZONE_ROWS as u64,
        "the mixed zone with tombstones compared"
    );
    let mixed_zones = zones
        .iter()
        .filter(|zone| (1..zone.len()).contains(&holds_g1(zone)))
        .count() as u64;
    assert_eq!(
        recorded,
        (mixed_zones - 1) * ZONE_ROWS as u64,
        "the other mixed zones, from their records"
    );
    let all_zones = zones
        .iter()
        .filter(|zone| holds_g1(zone) == zone.len())
        .count() as u64;
    assert_eq!(
        taken,
        all_zones * ZONE_ROWS as u64,
        "the all zones taken, their tombstone left out of the count"
    );
}
