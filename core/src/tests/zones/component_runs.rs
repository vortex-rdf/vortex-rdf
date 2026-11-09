//! In-memory index runs bound to a graph, checked inside their component: a
//! by-copy run whose graph follows a free slot (`(? p ? g)`, `(? p o g)`,
//! `(? ? o g)`) and a by-reference run of a bound predicate or object read
//! the component's own graph column zone by zone, where the run's rows are
//! contiguous, instead of comparing the graph at the base rows the run
//! lists. Their answers are the base compare's.

use super::*;
use crate::store::test_hooks::ForcedZones;
use crate::store::{Keep, QuadColumn};

/// The probes an index run answers, by graph: `(? type ? g)`,
/// `(? type Taxon g)` and `(? ? Taxon g)`, for the default graph and graphs
/// 1 to 3 (a graph the store lacks matches nothing); and one-row runs,
/// `(? link "o{i}" g)` and `(? ? "o{i}" g)` for graphs 1 and 2, at rows
/// that start, end or sit inside zones (the `link` run is rows `0..RUN` of
/// `index:posg`, the objects `"o{i}"` rows `RUN..2 * RUN` of `index:ospg` and
/// `index:ref-o`, the last of them the component's last row).
fn component_probes() -> Vec<(String, Probe)> {
    let taxon = plain_literal("Taxon");
    let mut probes = Vec::new();
    for i in [960, 8_191, 8_192, RUN - 1] {
        let object = plain_literal(&format!("o{i:05}"));
        for k in [1, 2] {
            let g = numbered_graph(k);
            probes.push((
                format!("(? link o{i} g{k})"),
                Probe::new(
                    None,
                    Some(iri("link")),
                    Some(object.clone()),
                    Some(g.clone()),
                ),
            ));
            probes.push((
                format!("(? ? o{i} g{k})"),
                Probe::new(None, None, Some(object.clone()), Some(g)),
            ));
        }
    }
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
        zones_agree(&format!("{tag} {shape}"), || probe_answers(store, &probe)).await;
    }
}

/// `graphs`' store with `index`, built, loaded from its file in memory and
/// adopted from its bytes, each also with a tombstone in every zone and
/// with a tail: zones on or off, every index-run probe answers the same.
async fn assert_component_runs_answer_as_rows_do(index: IndexConfig, graphs: Graphs) {
    let built = built_store(
        zone_quads(graphs),
        LayoutStrategy::Dictionary,
        index.types(),
    )
    .await;
    let (_dir, path) = zone_file(ZoneFile::new(graphs, index)).await;
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

/// Zones on or off, a by-copy index run bound to a graph answers the same:
/// counts, windows and every row in order. One test per layout.
mod component_runs_answer_as_rows_do_by_copy {
    use super::*;

    /// The by-copy runs of `graphs`.
    async fn by_copy(graphs: Graphs) {
        assert_component_runs_answer_as_rows_do(IndexConfig::Copy, graphs).await;
    }

    layout_tests!(zone_graphs: by_copy);
}

/// Zones on or off, a by-reference index run bound to a graph answers the
/// same: counts, windows and every row in order. One test per layout.
mod component_runs_answer_as_rows_do_by_reference {
    use super::*;

    /// The by-reference runs of `graphs`.
    async fn by_reference(graphs: Graphs) {
        assert_component_runs_answer_as_rows_do(IndexConfig::Reference, graphs).await;
    }

    layout_tests!(zone_graphs: by_reference);
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
            rec.rows(|e| match e {
                Event::ComponentChecked(rows) => Some(*rows),
                _ => None,
            }),
            0,
            "{name}: no graph read"
        );
        let recorded = rec.rows(|e| match e {
            Event::ZoneRecorded(rows) => Some(*rows),
            _ => None,
        });
        let counted = rec.rows(|e| match e {
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
    let read = || async { quads_of(&store.run_probe(&window).await.unwrap()).await };
    let (rows, rec) = zones_agree("the window", read).await;
    assert_eq!(rows.len(), 64);
    let checked = rec.rows(|e| match e {
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

/// A window of no rows of a checked run counts no zone: no record, no
/// encoding and no graph read, at the run's start and past it. It holds no
/// row.
#[tokio::test]
async fn checked_run_limit_zero_windows_count_nothing() {
    let _zones = ForcedZones::set(true);
    let store = built_store(
        zone_quads(Graphs::Alternating),
        LayoutStrategy::Dictionary,
        IndexConfig::Reference.types(),
    )
    .await;
    for offset in [0, RUN / 8] {
        let window = Probe::new(None, Some(iri("type")), None, Some(numbered_graph(1)))
            .window(offset, Some(0));
        let rec = Recorder::start();
        let page = store.run_probe(&window).await.unwrap();
        let counted = rec.count_where(|e| {
            matches!(
                e,
                Event::ZoneRecorded(_) | Event::ZoneCounted(_) | Event::ComponentChecked(_)
            )
        });
        drop(rec);
        assert_eq!(counted, 0, "offset {offset}: no zone counted");
        assert!(quads_of(&page).await.is_empty(), "offset {offset}");
    }
}

/// How many ids a checked run holds is known only once it is counted: before,
/// its pending length reads nothing (no zone counted, no graph read) and is
/// unknown; after a count, it is the count.
#[tokio::test]
async fn checked_run_lengths_read_nothing_until_counted() {
    use crate::store::IdsNeed;

    let _zones = ForcedZones::set(true);
    let store = built_store(
        zone_quads(Graphs::Alternating),
        LayoutStrategy::Dictionary,
        IndexConfig::Reference.types(),
    )
    .await;
    let view = store
        .match_pattern_for(
            None,
            Some(&iri("type")),
            None,
            Some(&numbered_graph(1)),
            IdsNeed::CountOrWindow { reach: 64 },
        )
        .await
        .unwrap();
    let rec = Recorder::start();
    let known = view.debug_pending_len();
    let read = rec.count_where(|e| {
        matches!(
            e,
            Event::ZoneRecorded(_) | Event::ZoneCounted(_) | Event::ComponentChecked(_)
        )
    });
    drop(rec);
    assert_eq!(known, Some(None), "unknown before a count");
    assert_eq!(read, 0, "nothing read for the length");
    let size = view.size().await.unwrap();
    assert_eq!(
        view.debug_pending_len(),
        Some(Some(size)),
        "known once counted"
    );
}

/// A view bound to a graph that a checked run answers reports its rows
/// exactly in its statistics, as every in-memory view does (interfaces.md
/// §3): only a count or a window leaves the run pending, uncounted, and a
/// view built for its rows holds the run's matches; by copy and by
/// reference.
#[tokio::test]
async fn graph_bound_views_report_exact_statistics() {
    use crate::store::IdsNeed;

    let _zones = ForcedZones::set(true);
    for index in [IndexConfig::Copy, IndexConfig::Reference] {
        let store = built_store(
            zone_quads(Graphs::Alternating),
            LayoutStrategy::Dictionary,
            index.types(),
        )
        .await;
        let (ty, g1) = (iri("type"), numbered_graph(1));
        let tag = index.name();
        let counted = store
            .match_pattern_for(None, Some(&ty), None, Some(&g1), IdsNeed::Count)
            .await
            .unwrap();
        assert_eq!(
            counted.debug_pending_len(),
            Some(None),
            "{tag}: a count's match, a checked run left pending"
        );
        let view = store
            .match_pattern(None, Some(&ty), None, Some(&g1))
            .await
            .unwrap();
        assert_eq!(view.debug_pending_len(), None, "{tag}: the run's matches");
        assert_eq!(
            view.view_statistics().unwrap().rows,
            RowCountHint::exact(view.size().await.unwrap()),
            "{tag}"
        );
    }
}

/// A checked run's windows on a tailed view, from the base's last match to
/// past the view's last row (the tail's first row, its second, the end and
/// past it), each one row or five: each is that slice of every row, as with
/// zones off, so the run hands the tail the offset its matches leave (Review
/// Focus 1); by copy and by reference.
#[tokio::test]
async fn checked_run_windows_past_the_base_hand_the_tail_its_offset() {
    for index in [IndexConfig::Copy, IndexConfig::Reference] {
        let base = built_store(
            zone_quads(Graphs::Alternating),
            LayoutStrategy::Dictionary,
            index.types(),
        )
        .await;
        let store = tailed(&base).await;
        let probe = Probe::new(None, Some(iri("type")), None, Some(numbered_graph(1)));
        let (rows, matched) = {
            let _zones = ForcedZones::set(false);
            (
                quads_of(&store.run_probe(&probe).await.unwrap()).await,
                base.count_probe(&probe).await.unwrap(),
            )
        };
        let count = rows.len();
        let tag = index.name();
        assert_eq!(count - matched, 2, "{tag}: graph 1's tail rows");
        let _zones = ForcedZones::set(true);
        for offset in [matched - 1, matched, matched + 1, count, count + 1] {
            for limit in [1, 5] {
                let window = probe.clone().window(offset, Some(limit));
                let got = quads_of(&store.run_probe(&window).await.unwrap()).await;
                let want = &rows[offset.min(count)..(offset + limit).min(count)];
                assert_eq!(got, want, "{tag}: offset {offset}, limit {limit}");
            }
        }
    }
}

/// A capped count is exactly `min(size, limit)` on every route a count
/// takes: a checked run, a residual left pending by zones and in pieces (the
/// Default layout's strings), and a located file run, at a limit of none,
/// one, a few, the size and past it.
#[tokio::test]
async fn capped_counts_never_exceed_their_limit() {
    let _zones = ForcedZones::set(true);
    let quads = zone_quads(Graphs::Alternating);
    let checked = built_store(
        quads.clone(),
        LayoutStrategy::Dictionary,
        IndexConfig::Reference.types(),
    )
    .await;
    let zoned = built_store(quads.clone(), LayoutStrategy::Dictionary, vec![]).await;
    let pieces = built_store(quads, LayoutStrategy::Default, vec![]).await;
    let (_dir, path) = zone_file(ZoneFile::new(Graphs::Alternating, IndexConfig::Reference)).await;
    let file = VortexRdfStore::from_file(&path).await.unwrap();
    let probe = Probe::new(None, Some(iri("type")), None, Some(numbered_graph(1)));
    for (label, store) in [
        ("checked", &checked),
        ("zoned", &zoned),
        ("pieces", &pieces),
        ("file", &file),
    ] {
        let size = store.count_probe(&probe).await.unwrap();
        assert!(size > 7, "{label}: {size} matches");
        for limit in [0, 1, 7, size, size + 3] {
            let capped = store
                .count_probe(&probe.clone().window(0, Some(limit)))
                .await
                .unwrap();
            assert_eq!(capped, size.min(limit), "{label} limit {limit}");
        }
    }
}

/// By reference, the first page of `(? type ? g1)` on `Graphs::Alternating`
/// and its ASK (a count capped at one) count the run's zones only until the
/// page fills or the cap is met: the run's part of its first zone, a quarter
/// of whose rows are graph 1's, fills both, so no other zone is counted.
/// Both answer as with zones off.
#[tokio::test]
async fn component_first_pages_count_their_first_zone_alone() {
    let store = built_store(
        zone_quads(Graphs::Alternating),
        LayoutStrategy::Dictionary,
        IndexConfig::Reference.types(),
    )
    .await;
    let probe = Probe::new(None, Some(iri("type")), None, Some(numbered_graph(1)));
    let page = probe.clone().window(0, Some(64));
    let ask = probe.clone().window(0, Some(1));
    let counted = |rec: &Recorder| {
        rec.rows(|e| match e {
            Event::ZoneCounted(rows) | Event::ZoneRecorded(rows) => Some(*rows),
            _ => None,
        })
    };
    // The `type` run is `index:ref-p`'s rows `RUN..2 * RUN`.
    let first = (RUN.next_multiple_of(ZONE_ROWS) - RUN) as u64;
    let read = || async { quads_of(&store.run_probe(&page).await.unwrap()).await };
    let (rows, rec) = zones_agree("the page", read).await;
    assert_eq!(rows.len(), 64);
    assert_eq!(
        counted(&rec),
        first,
        "the page: the first zone's part counted alone"
    );
    drop(rec);
    let count = || async { store.count_probe(&ask).await.unwrap() };
    let (count, rec) = zones_agree("ASK", count).await;
    assert_eq!(count, 1, "ASK: capped at one");
    assert_eq!(
        counted(&rec),
        first,
        "ASK: the first zone's part counted alone"
    );
}

/// By reference, `(? type ? g1)` on [`Graphs::UnrecordedGap`]: one zone of
/// the run records nothing (seventeen graphs) and its bounds hold graph 1's
/// code, but none of its rows does. Once counted empty, that zone is
/// neither read nor decoded: a window spanning it reads no graph inside the
/// component, and the run's rows decode the ids of the zones holding the
/// graph alone. Both answer as with zones off.
#[tokio::test]
async fn component_runs_skip_zones_counted_empty() {
    let store = built_store(
        zone_quads(Graphs::UnrecordedGap),
        LayoutStrategy::Dictionary,
        IndexConfig::Reference.types(),
    )
    .await;
    let probe = Probe::new(None, Some(iri("type")), None, Some(numbered_graph(1)));
    // The matches before the gap's zone and 64 past it.
    let window = probe.clone().window(0, Some(UNRECORDED_GAP.start + 64));
    let read = || async { quads_of(&store.run_probe(&window).await.unwrap()).await };
    let (rows, rec) = zones_agree("the window", read).await;
    assert_eq!(rows.len(), UNRECORDED_GAP.start + 64);
    assert_eq!(
        rec.count_where(|e| matches!(e, Event::ComponentChecked(_))),
        0,
        "no graph read inside the component: the zones before and after the gap are \
         graph 1's alone, the gap's counted empty"
    );
    drop(rec);
    let read = || async { quads_of(&store.run_probe(&probe).await.unwrap()).await };
    let (rows, rec) = zones_agree("every row", read).await;
    assert_eq!(rows.len(), RUN - ZONE_ROWS);
    assert_eq!(
        rids_decoded(&rec),
        RUN - ZONE_ROWS,
        "the ids of the zones holding graph 1 alone"
    );
}

/// `(? type ? g1)` by copy on `Graphs::Stretches`, the `type` run's objects
/// in the reverse of its subjects' order: `index:posg` lists the run by
/// object, so its rows do not ascend, and the checked run still answers in
/// base order, its counts, windows and rows as with zones off. A window with
/// no limit and a kept window reach every match, so they check the run
/// inside the component and take their rows from its sorted ids.
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
    let wide = probe.clone().window(RUN / 8, None);
    let kept = probe
        .clone()
        .keep(QuadColumn::O, Keep::range(0..TermCode::MAX))
        .window(RUN / 8, Some(64));
    let read = |on: bool| {
        let (store, probe, wide, kept) = (&store, &probe, &wide, &kept);
        async move {
            let _zones = ForcedZones::set(on);
            let answered = probe_answers(store, probe).await;
            // Each read, and how many zones it checked inside the component.
            let mut rec = Recorder::start();
            let mut reads = Vec::new();
            for read in [probe, wide, kept] {
                let rows = row_list(&store.run_probe(read).await.unwrap()).await;
                let checked = rec
                    .take()
                    .iter()
                    .filter(|e| matches!(e, Event::ComponentChecked(_)))
                    .count();
                reads.push((rows, checked));
            }
            let count = store.count_probe(kept).await.unwrap();
            (answered, reads, count)
        }
    };
    let (on, on_reads, on_count) = read(true).await;
    let (off, off_reads, off_count) = read(false).await;
    assert_eq!(on, off, "counts, windows and rows, as with zones off");
    assert_eq!(on_count, off_count, "the kept window's count");
    let lens = [RUN / 4, RUN / 4 - RUN / 8, 64];
    for (((label, len), (on_rows, checked)), (off_rows, unchecked)) in ["rows", "wide", "kept"]
        .into_iter()
        .zip(lens)
        .zip(&on_reads)
        .zip(&off_reads)
    {
        assert_eq!(on_rows.len(), len, "{label}: graph 1's rows");
        assert_eq!(on_rows, off_rows, "{label}: as with zones off");
        let mut sorted = on_rows.clone();
        sorted.sort_unstable();
        assert_eq!(on_rows, &sorted, "{label}: base row order");
        assert!(
            *checked > 0,
            "{label}: checked inside the component with zones on"
        );
        assert_eq!(
            *unchecked, 0,
            "{label}: compared at base rows with zones off"
        );
    }
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

/// By copy, a run's ids follow its copy's order, so a window of its matches
/// in base order would find every match first: a window of `(? type ? g1)`
/// leaves the graph to the residual over the run's ids, whose zones over base
/// order reach the window (as with zones off), and reads no graph inside the
/// component.
#[tokio::test]
async fn copy_windows_leave_the_graph_to_the_residual() {
    let store = built_store(
        zone_quads(Graphs::Alternating),
        LayoutStrategy::Dictionary,
        IndexConfig::Copy.types(),
    )
    .await;
    let window = Probe {
        offset: RUN / 8,
        limit: Some(64),
        ..Probe::new(None, Some(iri("type")), None, Some(numbered_graph(1)))
    };
    let read = || async { quads_of(&store.run_probe(&window).await.unwrap()).await };
    let (rows, rec) = zones_agree("the window", read).await;
    assert_eq!(rows.len(), 64);
    assert_eq!(
        rec.count_where(|e| matches!(e, Event::ComponentChecked(_))),
        0,
        "no graph read inside the component"
    );
    assert_eq!(
        rec.count(&Event::ResidualDeferred),
        1,
        "the graph left to the residual, pending for the window"
    );
}
