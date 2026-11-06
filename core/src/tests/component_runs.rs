//! In-memory index runs bound to a graph, checked inside their component: a
//! by-copy run whose graph follows a free slot (`(? p ? g)`, `(? p o g)`,
//! `(? ? o g)`) and a by-reference run of a bound predicate or object read
//! the component's own graph column zone by zone, where the run's rows are
//! contiguous, instead of comparing the graph at the base rows the run
//! lists. Their answers are the base compare's.

use super::located_zones::{Graphs, RUN, zone_quads};
use super::residual_zones::{LAYOUTS, answers, quads_of, tailed, tombstoned};
use super::*;
use crate::io::zone_map::ZONE_ROWS;
use crate::store::test_hooks::ForcedZones;

/// The probes an index run answers, by graph: `(? type ? g)`,
/// `(? type Taxon g)` and `(? ? Taxon g)`, for the default graph and graphs
/// 1 to 3 (a graph the store lacks matches nothing).
fn component_probes() -> Vec<(String, Probe)> {
    let taxon = plain_literal("Taxon");
    let mut probes = Vec::new();
    for k in 0..=3 {
        let g = numbered_graph(k);
        probes.push((
            format!("(? type ? g{k})"),
            Probe::new(None, Some(iri("type")), None, Some(g.clone())),
        ));
        probes.push((
            format!("(? type Taxon g{k})"),
            Probe::new(
                None,
                Some(iri("type")),
                Some(taxon.clone()),
                Some(g.clone()),
            ),
        ));
        probes.push((
            format!("(? ? Taxon g{k})"),
            Probe::new(None, None, Some(taxon.clone()), Some(g)),
        ));
    }
    probes
}

/// `store`'s answers to every index-run probe, with zones on and off, equal.
async fn assert_runs_answer_as_rows_do(tag: &str, store: &VortexRdfStore) {
    for (shape, probe) in component_probes() {
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

/// Every graph layout's store with `index`, built, loaded from its file in
/// memory and adopted from its bytes, each also with a tombstone in every
/// zone and with a tail: zones on or off, every index-run probe answers the
/// same.
async fn assert_component_runs_answer_as_rows_do(index: IndexConfig) {
    for graphs in LAYOUTS {
        let quads = zone_quads(graphs);
        let built = built_store(quads.clone(), LayoutStrategy::Dictionary, index.types()).await;
        let (_dir, path) = write_store_file(quads, LayoutStrategy::Dictionary, index.types()).await;
        let loaded = VortexRdfStore::from_file_in_memory(&path).await.unwrap();
        let bytes = std::fs::read(&path).unwrap();
        let adopted = VortexRdfStore::from_bytes(&bytes).await.unwrap();
        for (backend, store) in [("built", built), ("loaded", loaded), ("adopted", adopted)] {
            let tag = format!("{graphs:?} {} {backend}", index.name());
            assert_runs_answer_as_rows_do(&tag, &store).await;
            assert_runs_answer_as_rows_do(&format!("{tag} tombstoned"), &tombstoned(&store).await)
                .await;
            assert_runs_answer_as_rows_do(&format!("{tag} tailed"), &tailed(&store).await).await;
        }
    }
}

/// Zones on or off, a by-copy index run bound to a graph answers the same:
/// counts, windows and every row in order.
#[tokio::test]
async fn component_runs_answer_as_rows_do_by_copy() {
    assert_component_runs_answer_as_rows_do(IndexConfig::Copy).await;
}

/// Zones on or off, a by-reference index run bound to a graph answers the
/// same: counts, windows and every row in order.
#[tokio::test]
async fn component_runs_answer_as_rows_do_by_reference() {
    assert_component_runs_answer_as_rows_do(IndexConfig::Reference).await;
}

/// The rows `rec` saw an event of `pick` name, summed.
fn rows_of(rec: &Recorder, pick: impl Fn(&Event) -> Option<u64>) -> u64 {
    rec.picked(pick).iter().sum()
}

/// `(? type ? g1)` on `Graphs::Alternating`, by copy and by reference, in
/// memory: the count, a quarter of the run, is taken zone by zone inside
/// the component, the run's whole zones from their recorded graphs and its
/// partial zones on the graph column's encoding, with no row id decoded and
/// no base row compared.
#[tokio::test]
async fn component_counts_read_no_ids() {
    let _zones = ForcedZones::set(true);
    let probe = Probe::new(None, Some(iri("type")), None, Some(numbered_graph(1)));
    for index in [IndexConfig::Copy, IndexConfig::Reference] {
        let name = index.name();
        let store = built_store(
            zone_quads(Graphs::Alternating),
            LayoutStrategy::Dictionary,
            index.types(),
        )
        .await;
        let rec = Recorder::start();
        assert_eq!(store.count_probe(&probe).await.unwrap(), RUN / 4, "{name}");
        assert_eq!(rids_decoded(&rec), 0, "{name}: no row id decoded");
        assert_eq!(
            rec.count_where(|e| matches!(e, Event::IdsSorted(_))),
            0,
            "{name}: no ids sorted"
        );
        assert_eq!(
            rec.count_where(|e| matches!(e, Event::ResidualCompared(_))),
            0,
            "{name}: no base row compared"
        );
        assert_eq!(
            rows_of(&rec, |e| match e {
                Event::ComponentChecked(rows) => Some(*rows),
                _ => None,
            }),
            0,
            "{name}: no graph read"
        );
        let recorded = rows_of(&rec, |e| match e {
            Event::ZoneRecorded(rows) => Some(*rows),
            _ => None,
        });
        let counted = rows_of(&rec, |e| match e {
            Event::ZoneCounted(rows) => Some(*rows),
            _ => None,
        });
        assert_eq!(
            recorded + counted,
            RUN as u64,
            "{name}: every row of the run counted unread"
        );
        assert!(
            recorded > 0 && counted > 0,
            "{name}: whole zones from their records ({recorded} rows), partial ones on the \
             encoding ({counted} rows)"
        );
    }
}

/// By reference, a 64-row window from the middle of `(? type ? g1)` on
/// `Graphs::Alternating` counts the zones before it and reads the graphs of
/// at most the two zones it spans, and answers as with zones off.
#[tokio::test]
async fn component_windows_jump_by_zone_counts() {
    let store = built_store(
        zone_quads(Graphs::Alternating),
        LayoutStrategy::Dictionary,
        IndexConfig::Reference.types(),
    )
    .await;
    let window = Probe {
        offset: RUN / 8,
        limit: Some(64),
        ..Probe::new(None, Some(iri("type")), None, Some(numbered_graph(1)))
    };
    let off = {
        let _zones = ForcedZones::set(false);
        quads_of(&store.run_probe(&window).await.unwrap()).await
    };
    let _zones = ForcedZones::set(true);
    let rec = Recorder::start();
    let on = quads_of(&store.run_probe(&window).await.unwrap()).await;
    assert_eq!(on.len(), 64);
    assert_eq!(on, off, "the window, as with zones off");
    let checked = rows_of(&rec, |e| match e {
        Event::ComponentChecked(rows) => Some(*rows),
        _ => None,
    });
    assert!(
        (1..=2 * ZONE_ROWS as u64).contains(&checked),
        "{checked} graphs read inside the component"
    );
    assert!(
        rids_decoded(&rec) <= 2 * ZONE_ROWS,
        "{} row ids decoded",
        rids_decoded(&rec)
    );
}

/// `(? type ? g1)` by copy on `Graphs::Stretches`, the `type` run's objects
/// in the reverse of its subjects' order: `index:posg` lists the run by
/// object, so its rows do not ascend, and the checked run still answers in
/// base order, its counts, windows and rows as with zones off.
#[tokio::test]
async fn copy_pg_answers_in_base_order() {
    let quads: Vec<Quad> = zone_quads(Graphs::Stretches)
        .into_iter()
        .enumerate()
        .map(|(i, quad)| match i < RUN {
            true => Quad::new(
                quad.subject,
                quad.predicate,
                plain_literal(&format!("t{:05}", RUN - 1 - i)),
                quad.graph_name,
            ),
            false => quad,
        })
        .collect();
    let store = built_store(quads, LayoutStrategy::Dictionary, IndexConfig::Copy.types()).await;
    let probe = Probe::new(None, Some(iri("type")), None, Some(numbered_graph(1)));
    let read = |on: bool| {
        let (store, probe) = (&store, &probe);
        async move {
            let _zones = ForcedZones::set(on);
            let rec = Recorder::start();
            let answered = answers(store, probe).await;
            let view = store.run_probe(probe).await.unwrap();
            let checked = rec.count_where(|e| matches!(e, Event::ComponentChecked(_)));
            (answered, row_list(&view).await, checked)
        }
    };
    let (on, on_rows, checked) = read(true).await;
    let (off, off_rows, unchecked) = read(false).await;
    assert_eq!(on_rows.len(), RUN / 4, "graph 1's rows");
    assert_eq!(on, off, "counts, windows and rows, as with zones off");
    assert_eq!(on_rows, off_rows);
    let mut sorted = on_rows.clone();
    sorted.sort_unstable();
    assert_eq!(on_rows, sorted, "base row order");
    assert!(checked > 0, "checked inside the component with zones on");
    assert_eq!(unchecked, 0, "compared at base rows with zones off");
}

/// Components without a graph zone map (a Default-layout store's), and
/// zones off, leave the graph to the residual, compared at base rows as
/// before, with no check inside the component.
#[tokio::test]
async fn unzoned_components_keep_the_residual() {
    let probe = Probe::new(None, Some(iri("type")), None, Some(numbered_graph(1)));
    let cases = [
        ("default layout", LayoutStrategy::Default, true),
        ("zones off", LayoutStrategy::Dictionary, false),
    ];
    for (case, layout, zones) in cases {
        for index in [IndexConfig::Copy, IndexConfig::Reference] {
            let tag = format!("{case} {}", index.name());
            let store = built_store(zone_quads(Graphs::Stretches), layout, index.types()).await;
            let _zones = ForcedZones::set(zones);
            let rec = Recorder::start();
            let rows = quads_of(&store.run_probe(&probe).await.unwrap()).await;
            assert_eq!(rows.len(), RUN / 4, "{tag}: graph 1's rows");
            assert!(
                rec.count_where(|e| matches!(e, Event::ResidualCompared(_))) > 0,
                "{tag}: the graph compared at base rows"
            );
            assert_eq!(
                rec.count_where(|e| matches!(e, Event::ComponentChecked(_))),
                0,
                "{tag}: no check inside the component"
            );
        }
    }
}
