//! Located runs decided zone by zone: a column's zone map read back with
//! its exact bounds and recorded graphs, and the runs of the sorted tables
//! (the quad table's and the copy index's) checked against a bound graph.
//! The reference index's runs are in `reference`, and what the runs ask of
//! the file's statistics, and when, in `statistics`. With
//! `VORTEX_RDF_LOCATE=0` no located run answers, so each test that asserts
//! how one read returns at once.
use super::*;
use crate::store::query::located::locate_enabled;
use crate::zone_stats::zone_map::ColumnZones;

mod reference;
mod statistics;

/// A store file's `g` column read as a zone map: 20,000 rows of `g1` then
/// `g2` make three zones, and `g = g1` holds on every row of the first, on
/// some of the second and on none of the last.
#[tokio::test]
async fn column_zones_read_from_a_written_column() {
    let quads: Vec<Quad> = (0..20_000)
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{i:05}"),
                "http://example.org/p",
                "o",
                numbered_graph(1 + i / 10_000),
            )
        })
        .collect();
    let (_dir, path) = write_store_file(quads, LayoutStrategy::Dictionary, vec![]).await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let g1 = row_list(&store).await[0][3];
    let native = open_mapped(&path).await;
    let column = native
        .table_column_layout(None, "g")
        .expect("the quad table has a g column");
    let zones = ColumnZones::read(&column, &native.segment_source())
        .await
        .unwrap()
        .expect("a written code column is zoned with exact bounds");
    assert_eq!((zones.zones(), zones.zone_len()), (3, 8_192));
    assert_eq!(
        (0..3).map(|z| zones.eq_holds(z, g1)).collect::<Vec<_>>(),
        [Some(true), None, Some(false)]
    );
}

/// A file written in seven-row leaves has no zone maps, so its columns
/// have none to read.
#[tokio::test]
async fn column_zones_absent_without_zone_maps() {
    let (_dir, path) = write_chunked_store_file(&fixture_quads(), 7).await;
    let native = open_mapped(&path).await;
    let column = native.table_column_layout(None, "g").unwrap();
    assert!(
        ColumnZones::read(&column, &native.segment_source())
            .await
            .unwrap()
            .is_none()
    );
}

/// A store file's graph column read with its recorded values: on the
/// 1,000-row stretches' `index:posg`, each zone's count of each graph is its
/// rows' count, and a code no row holds (the `type` predicate's) counts
/// none. A file written without the statistic records no zone's.
#[tokio::test]
async fn column_zones_read_recorded_counts() {
    for recorded in [true, false] {
        let (_dir, path) =
            zone_file(ZoneFile::new(Graphs::Stretches, IndexConfig::Copy).value_counts(recorded))
                .await;
        let store = VortexRdfStore::from_file(&path).await.unwrap();
        let native = open_mapped(&path).await;
        let column = native
            .table_column_layout(Some("index:posg"), "g")
            .expect("index:posg has a g column");
        let zones = ColumnZones::read(&column, &native.segment_source())
            .await
            .unwrap()
            .expect("a written code column is zoned with exact bounds");
        // index:posg holds the quad table's rows in (p, o, s, g) order.
        let mut rows = row_list(&store).await;
        rows.sort_unstable_by_key(|row| (row[1], row[2], row[0], row[3]));
        assert_eq!(zones.zones(), rows.len().div_ceil(ZONE_ROWS) as u64);
        let mut graphs = Vec::new();
        for g in 1..=Graphs::Stretches.last() {
            graphs.push(code_of(&store, &format!("<http://example.org/g{g}>")).await);
        }
        let held_by_none = code_of(&store, "<http://example.org/type>").await;
        for (zone, rows) in rows.chunks(ZONE_ROWS).enumerate() {
            let zone = zone as u64;
            for &g in &graphs {
                let want = rows.iter().filter(|row| row[3] == g).count() as u64;
                assert_eq!(
                    zones.recorded_count(zone, g),
                    recorded.then_some(want),
                    "recorded {recorded}: zone {zone}, graph {g}"
                );
            }
            assert_eq!(
                zones.recorded_count(zone, held_by_none),
                recorded.then_some(0),
                "recorded {recorded}: zone {zone}, a code no row holds"
            );
        }
    }
}

/// Every pattern of every store answers the same with zones and without:
/// counts, windows and streams, row for row and in order, and as the term
/// path does. One test per layout.
mod zoned_runs_answer_as_rows_do {
    use super::*;

    layout_tests!(all: zoned_runs_answer_as_rows_do_on);
}

/// [`zoned_runs_answer_as_rows_do`] on the stores of `layout`.
async fn zoned_runs_answer_as_rows_do_on(layout: Graphs) {
    for fixture in zone_stores_of(layout).await {
        let store = &fixture.store;
        let mut graphs = vec![1, fixture.graphs.last()];
        graphs.dedup();
        for g in graphs {
            for pattern in zone_patterns(store, g).await {
                // Zones decide located runs only; the general match answers
                // the rest as it always has.
                if store
                    .locate_codes(pattern, 0, Some(0))
                    .await
                    .unwrap()
                    .is_none()
                {
                    continue;
                }
                let label = format!("{} g{g} {pattern:?}", fixture.tag);
                let (zoned, _) = zones_agree(&label, || code_answers(store, pattern)).await;
                let want = term_rows(store, pattern).await;
                assert_eq!(zoned.count, want.len() as u64, "{label}: the count");
                let unwindowed =
                    code_rows::<4>(&store.match_code_pattern(pattern, 0, None).await.unwrap());
                let mut sorted = unwindowed.clone();
                sorted.sort_unstable();
                assert_eq!(sorted, want, "{label}: the rows");
                for stream in &zoned.streams {
                    assert_eq!(stream, &unwindowed, "{label}: a stream");
                }
            }
        }
    }
}

/// With zones, a count reads no checked rows: a zone of the graph alone
/// counts its size, a zone without it nothing, and a mixed one is counted
/// from its record or on its encoding; a stream reads the mixed zones only.
/// A store without zone maps reads its run row by row.
#[tokio::test]
async fn zoned_runs_skip_settled_zones() {
    use crate::store::test_hooks::ForcedZones;

    if !locate_enabled() {
        return;
    }

    let _on = ForcedZones::set(true);
    for fixture in zone_stores().await {
        let store = &fixture.store;
        let patterns = zone_patterns(store, 1).await;
        let (taxa, links) = (patterns[0], patterns[1]);
        match (
            fixture.graphs,
            fixture.index,
            fixture.tag.ends_with("leaves"),
        ) {
            (Graphs::Long, IndexConfig::Copy, _) => {
                let rec = Recorder::start();
                assert_eq!(store.count_code_pattern(taxa).await.unwrap(), 20_000);
                let (counted, read) = zone_work(&rec);
                assert_eq!(read, 0, "{}: a count reads no checked rows", fixture.tag);
                assert!(
                    counted < RUN as u64,
                    "{}: {counted} rows counted",
                    fixture.tag
                );
                drop(rec);
                let rec = Recorder::start();
                assert_eq!(streamed(store, taxa, 4096).await.len(), 20_000);
                let (_, read) = zone_work(&rec);
                assert!(
                    read < RUN as u64,
                    "{}: a stream read {read} rows",
                    fixture.tag
                );
            }
            (Graphs::Alternating, IndexConfig::Copy, _) => {
                // Every zone is mixed: the whole ones count from their
                // records, the partial first one on its encoding.
                let rec = Recorder::start();
                assert_eq!(store.count_code_pattern(taxa).await.unwrap(), 10_000);
                let (counted, read) = zone_work(&rec);
                assert_eq!(read, 0, "{}: a count reads no checked rows", fixture.tag);
                assert!(
                    counted < crate::zone_stats::zone_map::ZONE_ROWS as u64,
                    "{}: {counted} rows counted",
                    fixture.tag
                );
            }
            (_, _, true) => {
                let rec = Recorder::start();
                assert_eq!(store.count_code_pattern(links).await.unwrap(), 10_000);
                let blocks: u64 = rec.rows(|e| match e {
                    Event::LocatedBlock(rows) => Some(*rows),
                    _ => None,
                });
                assert_eq!(blocks, RUN as u64, "{}: the run read", fixture.tag);
                assert_eq!(
                    zone_work(&rec).0,
                    0,
                    "{}: nothing counted on zones",
                    fixture.tag
                );
            }
            _ => {}
        }
    }
}

/// A graph whose rows lie in one zone of a run, among zones of two graphs
/// that bracket its code ([`Graphs::Gapped`]): every zone's bounds hold graph
/// 2, so only the recorded values leave out the zones without it. Its count
/// reads no checked rows and counts graph 2's zone alone, from its record,
/// and zones and rows answer alike. A file written without the statistic
/// counts every zone of the run on its encoding.
#[tokio::test]
async fn recorded_graphs_settle_zones_without_the_code() {
    if !locate_enabled() {
        return;
    }
    for recorded in [true, false] {
        let (_dir, path) =
            zone_file(ZoneFile::new(Graphs::Gapped, IndexConfig::Copy).value_counts(recorded))
                .await;
        let store = &VortexRdfStore::from_file(&path).await.unwrap();
        let taxa = zone_patterns(store, 2).await[0];
        let want = term_rows(store, taxa).await.len() as u64;
        assert_eq!(want, 1_000, "graph 2's rows");
        let tag = format!("recorded {recorded}");
        let count = || async { store.count_code_pattern(taxa).await.unwrap() };
        let (count, rec) = zones_agree(&tag, count).await;
        assert_eq!(count, want, "{tag}");
        // Graph 2's rows lie in one zone, whole within the run, which
        // counts from its record.
        let (counted, from_records) = match recorded {
            true => (0, vec![ZONE_ROWS as u64]),
            false => (RUN as u64, vec![]),
        };
        assert_eq!(
            zone_work(&rec),
            (counted, 0),
            "recorded {recorded}: rows counted on zones, and read"
        );
        assert_eq!(zone_recorded(&rec), from_records, "recorded {recorded}");
        drop(rec);
        zones_agree(&tag, || code_answers(store, taxa)).await;
    }
}

/// A first page whose graph's rows lie past the run's first block, on a
/// file written without the statistic (every zone of the run mixed,
/// [`Graphs::Gapped`]), counts the run's zones on their encoding up to its
/// graph's, skipping those counted empty unread, and reads that zone alone
/// after the first block: by copy (the `index:posg` run) and by reference
/// (`index:ref-p`'s, checked against its own graphs). Its rows are as with
/// zones off.
#[tokio::test]
async fn first_pages_count_the_zones_before_their_first_match() {
    use crate::CodePattern;

    if !locate_enabled() {
        return;
    }

    // The run is rows RUN..2 * RUN of its child, graph 2's rows its rows
    // 20,000..21,000: they lie in the child's zone from 57,344.
    let first_match_zone = (RUN + 20_000) / ZONE_ROWS * ZONE_ROWS;
    for index in [IndexConfig::Copy, IndexConfig::Reference] {
        let (_dir, path) =
            zone_file(ZoneFile::new(Graphs::Gapped, index).value_counts(false)).await;
        let store = &VortexRdfStore::from_file(&path).await.unwrap();
        let ty = code_of(store, "<http://example.org/type>").await;
        let g2 = code_of(store, "<http://example.org/g2>").await;
        let pattern = CodePattern::new(None, Some(ty), None, Some(g2));
        let page = || async move {
            code_rows::<4>(
                &store
                    .match_code_pattern(pattern, 0, Some(10))
                    .await
                    .unwrap(),
            )
        };
        let tag = index.name();
        let (got, rec) = zones_agree(tag, page).await;
        let (counted, read) = zone_work(&rec);
        drop(rec);
        assert_eq!(got.len(), 10, "{tag}");
        assert_eq!(
            read,
            4_096 + ZONE_ROWS as u64,
            "{tag}: the first block, then graph 2's zone alone"
        );
        assert_eq!(
            counted,
            (first_match_zone + ZONE_ROWS - RUN) as u64,
            "{tag}: the zones up to graph 2's counted on their encoding"
        );
    }
}

/// Level 3: a mixed zone whose one unsettled slot's column records its
/// values counts that slot's code from its record, when the zone lies whole
/// within the run. On the alternating graphs' `index:posg` run, which
/// reaches the table's end, `(? type Taxon g1)` counts 10,000: the whole
/// zones from their records, the table's short last zone among them (Review
/// Focus 2), and the partial first zone, whose record counts rows outside
/// the run, on its encoding. It reads no checked rows.
#[tokio::test]
async fn recorded_counts_answer_mixed_zones() {
    use crate::store::test_hooks::ForcedZones;

    if !locate_enabled() {
        return;
    }

    let _on = ForcedZones::set(true);
    let (_dir, path) = zone_file(ZoneFile::new(Graphs::Alternating, IndexConfig::Copy)).await;
    let store = &VortexRdfStore::from_file(&path).await.unwrap();
    let taxa = zone_patterns(store, 1).await[0];
    // index:posg holds the quad table's rows in (p, o, s, g) order.
    let mut rows = row_list(store).await;
    rows.sort_unstable_by_key(|row| (row[1], row[2], row[0], row[3]));
    let start = rows
        .iter()
        .position(|row| Some(row[1]) == taxa.predicate && Some(row[2]) == taxa.object)
        .expect("the run's first row") as u64;
    let (zone, table) = (ZONE_ROWS as u64, rows.len() as u64);
    assert_eq!(start + RUN as u64, table, "the run reaches the table's end");
    assert_ne!(start % zone, 0, "the run starts within a zone");
    assert_ne!(table % zone, 0, "the table's last zone is short");
    let partial = start.next_multiple_of(zone) - start;
    let rec = Recorder::start();
    assert_eq!(store.count_code_pattern(taxa).await.unwrap(), 10_000);
    let recorded = zone_recorded(&rec);
    assert_eq!(
        recorded.iter().sum::<u64>(),
        RUN as u64 - partial,
        "the whole zones, from their records"
    );
    assert_eq!(
        recorded.last(),
        Some(&(table % zone)),
        "the short last zone"
    );
    assert_eq!(zone_work(&rec), (partial, 0), "the partial first zone");
}

/// Zones of seventeen graphs, past what the statistic records, record none
/// ([`Graphs::Seventeen`]); where every zone of a column is past it, Vortex
/// writes no statistic column at all, so the column reads as one written
/// without the statistic. `(? type Taxon g1)` counts exactly, every zone of
/// the run on its encoding and none from a record.
#[tokio::test]
async fn zones_past_max_values_count_their_encoding() {
    use crate::store::test_hooks::ForcedZones;

    if !locate_enabled() {
        return;
    }

    let _on = ForcedZones::set(true);
    let (_dir, path) = zone_file(ZoneFile::new(Graphs::Seventeen, IndexConfig::Copy)).await;
    let store = &VortexRdfStore::from_file(&path).await.unwrap();
    let native = store.debug_native_file().unwrap();
    let column = native.table_column_layout(Some("index:posg"), "g").unwrap();
    let zones = ColumnZones::read(&column, &native.segment_source())
        .await
        .unwrap()
        .unwrap();
    let g1 = code_of(store, "<http://example.org/g1>").await;
    assert!(
        (0..zones.zones()).all(|zone| zones.recorded_count(zone, g1).is_none()),
        "no zone recorded"
    );
    let taxa = zone_patterns(store, 1).await[0];
    let want = term_rows(store, taxa).await.len() as u64;
    assert_eq!(want, RUN.div_ceil(17) as u64);
    let rec = Recorder::start();
    assert_eq!(store.count_code_pattern(taxa).await.unwrap(), want);
    assert!(
        zone_recorded(&rec).is_empty(),
        "no zone counted from a record"
    );
    assert_eq!(
        zone_work(&rec),
        (RUN as u64, 0),
        "every zone on its encoding"
    );
}

/// A graph column whose zones are recorded and past the statistic's limit
/// by turns ([`Graphs::RecordedByTurns`], on `hub`'s quad-table run, which
/// starts the table): a zone past the limit reads as unrecorded. A count
/// takes the recorded zones from their records and counts the
/// others on their encoding, for graph 1, which every zone holds, and for
/// graph 18, which the unrecorded zones' bounds hold but their rows don't:
/// those zones aren't settled none. Every answer is the rows'.
#[tokio::test]
async fn recorded_and_unrecorded_zones_by_turns() {
    use crate::CodePattern;
    use crate::store::test_hooks::ForcedZones;
    use std::collections::BTreeSet;

    if !locate_enabled() {
        return;
    }

    let (_dir, path) =
        zone_file(ZoneFile::new(Graphs::RecordedByTurns, IndexConfig::NoIndex)).await;
    let store = &VortexRdfStore::from_file(&path).await.unwrap();
    let native = store.debug_native_file().unwrap();
    let column = native.table_column_layout(None, "g").unwrap();
    let zones = ColumnZones::read(&column, &native.segment_source())
        .await
        .unwrap()
        .unwrap();
    let rows = row_list(store).await;
    let hub = code_of(store, "<http://example.org/hub>").await;
    let run = rows.iter().filter(|row| row[0] == hub).count();
    assert_eq!(run, RUN, "hub's run");
    assert_eq!(rows[0][0], hub, "the run starts the table");
    let (g1, g18) = (
        code_of(store, "<http://example.org/g1>").await,
        code_of(store, "<http://example.org/g18>").await,
    );
    // The run's zones: unrecorded ones (past 16 graphs) and recorded ones,
    // with the rows of each graph.
    let mut unrecorded = Vec::new();
    for (zone, chunk) in rows
        .chunks(ZONE_ROWS)
        .enumerate()
        .take(RUN.div_ceil(ZONE_ROWS))
    {
        let zone = zone as u64;
        let graphs: BTreeSet<u64> = chunk.iter().map(|row| row[3]).collect();
        for g in [g1, g18] {
            let held = chunk.iter().filter(|row| row[3] == g).count() as u64;
            let want = (graphs.len() <= 16).then_some(held);
            assert_eq!(
                zones.recorded_count(zone, g),
                want,
                "zone {zone}, graph {g}"
            );
        }
        if graphs.len() > 16 {
            let (min, max) = (graphs.first().unwrap(), graphs.last().unwrap());
            assert!(
                *min < g18 && g18 < *max && !graphs.contains(&g18),
                "zone {zone}: graph 18 within its bounds, not its rows"
            );
            unrecorded.push(zone);
        }
    }
    assert_eq!(unrecorded, [0, 2, 4], "the unrecorded zones");
    // Zones 0 and 2 are whole within the run, zone 4 holds its last rows.
    let counted = 2 * ZONE_ROWS as u64 + (RUN - 4 * ZONE_ROWS) as u64;
    let link = code_of(store, "<http://example.org/link>").await;
    let _on = ForcedZones::set(true);
    for g in [g1, g18] {
        let pattern = CodePattern::new(Some(hub), Some(link), None, Some(g));
        let want = term_rows(store, pattern).await.len() as u64;
        let rec = Recorder::start();
        assert_eq!(
            store.count_code_pattern(pattern).await.unwrap(),
            want,
            "graph {g}"
        );
        assert_eq!(
            zone_recorded(&rec),
            [ZONE_ROWS as u64; 2],
            "graph {g}: the recorded zones, from their records"
        );
        assert_eq!(
            zone_work(&rec),
            (counted, 0),
            "graph {g}: the unrecorded zones, on their encoding"
        );
        drop(rec);
        zones_agree(&format!("graph {g}"), || code_answers(store, pattern)).await;
    }
}

/// The default graph is counted from records like any named graph: on
/// zones mixing it with graph 3 ([`Graphs::DefaultAndThree`]),
/// `(? type Taxon default)` counts its rows exactly, the run's whole zones
/// from their records.
#[tokio::test]
async fn default_graph_counts_from_records() {
    use crate::CodePattern;
    use crate::store::test_hooks::ForcedZones;

    if !locate_enabled() {
        return;
    }

    let _on = ForcedZones::set(true);
    let (_dir, path) = zone_file(ZoneFile::new(Graphs::DefaultAndThree, IndexConfig::Copy)).await;
    let store = &VortexRdfStore::from_file(&path).await.unwrap();
    // The default graph's spelling is the empty one.
    let default = code_of(store, "").await;
    let taxa = CodePattern {
        graph: Some(default),
        ..zone_patterns(store, 3).await[0]
    };
    let want = row_list(store)
        .await
        .iter()
        .filter(|row| {
            [Some(row[1]), Some(row[2]), Some(row[3])] == [taxa.predicate, taxa.object, taxa.graph]
        })
        .count() as u64;
    assert_eq!(want, RUN as u64 / 2, "the default graph's rows");
    let rec = Recorder::start();
    assert_eq!(store.count_code_pattern(taxa).await.unwrap(), want);
    let recorded: u64 = zone_recorded(&rec).iter().sum();
    let (counted, read) = zone_work(&rec);
    assert_eq!(read, 0, "no checked rows read");
    assert_eq!(
        recorded + counted,
        RUN as u64,
        "every zone of the run mixed"
    );
    assert!(
        counted < ZONE_ROWS as u64,
        "only a partial zone on its encoding: {counted} rows"
    );
}
