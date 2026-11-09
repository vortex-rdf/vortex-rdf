//! By-reference runs (`index:ref-p`, `index:ref-o`) checked against a bound
//! graph on the child's own `g` column, zone by zone.
use super::*;

/// The reads `rec` saw a reference run make of graphs: its child's own
/// (`ReferenceGraphRead`), and the quad table's at its rows (`CheckTaken` of
/// slot 3).
fn reference_graph_reads(rec: &Recorder) -> (u64, u64) {
    let own = rec.rows(|e| match e {
        Event::ReferenceGraphRead(rows) => Some(*rows),
        _ => None,
    });
    let taken = rec.rows(|e| match e {
        Event::CheckTaken { slot: 3, rows } => Some(*rows),
        _ => None,
    });
    (own, taken)
}

/// A by-reference run checked against the graph alone is decided zone by
/// zone from its child's own `g`, as a table run is: a count reads no graph
/// (the child's or the quad table's) and no checked row. On long stretches
/// the zones of graph 1 alone and those without it settle; on alternating
/// graphs the whole zones count from their records, the child's short last
/// zone among them (the run reaches the child's end), and the partial first
/// zone on its encoding; zones past 16 graphs count on their encoding; the
/// default graph counts from records like any other, and on a long stretch
/// of it its zones settle as graph 1's do.
#[tokio::test]
async fn reference_runs_skip_settled_zones() {
    use crate::CodePattern;
    use crate::store::test_hooks::ForcedZones;

    if !locate_enabled() {
        return;
    }

    let _on = ForcedZones::set(true);
    let zone = ZONE_ROWS as u64;
    for graphs in [
        Graphs::Long,
        Graphs::Alternating,
        Graphs::RecordedByTurns,
        Graphs::DefaultAndThree,
        Graphs::DefaultThenThree,
    ] {
        let (_dir, path) = zone_file(ZoneFile::new(graphs, IndexConfig::Reference)).await;
        let store = &VortexRdfStore::from_file(&path).await.unwrap();
        let rows = row_list(store).await;
        let ty = code_of(store, "<http://example.org/type>").await;
        let link = code_of(store, "<http://example.org/link>").await;
        let graph = match graphs {
            Graphs::DefaultAndThree | Graphs::DefaultThenThree => code_of(store, "").await,
            _ => code_of(store, "<http://example.org/g1>").await,
        };
        // RecordedByTurns lays its turns out along `link`'s run, which
        // starts the child; the others read `type`'s, which ends it.
        let predicate = match graphs {
            Graphs::RecordedByTurns => link,
            _ => ty,
        };
        let pattern = CodePattern::new(None, Some(predicate), None, Some(graph));
        let want = rows
            .iter()
            .filter(|row| row[1] == predicate && row[3] == graph)
            .count() as u64;
        let tag = format!("{graphs:?}");
        let rec = Recorder::start();
        assert_eq!(
            store.count_code_pattern(pattern).await.unwrap(),
            want,
            "{tag}"
        );
        assert_eq!(reference_graph_reads(&rec), (0, 0), "{tag}: no graph read");
        let (counted, read) = zone_work(&rec);
        let recorded: u64 = zone_recorded(&rec).iter().sum();
        assert_eq!(read, 0, "{tag}: no checked row read");
        match graphs {
            Graphs::Long => {
                assert!(counted < RUN as u64, "{tag}: {counted} counted");
                // A stream reads the mixed zones' graphs to check them, and
                // only those.
                drop(rec);
                let rec = Recorder::start();
                let rows = streamed(store, pattern, 4096).await;
                assert_eq!(rows.len() as u64, want, "{tag}: the stream's rows");
                let read = zone_work(&rec).1;
                assert!(
                    read > 0 && read < RUN as u64,
                    "{tag}: a stream read {read} rows"
                );
            }
            Graphs::Alternating => {
                // The run is the child's last RUN positions.
                let start = 2 * RUN as u64 - RUN as u64;
                let partial = start.next_multiple_of(zone) - start;
                assert_eq!(
                    (counted, recorded),
                    (partial, RUN as u64 - partial),
                    "{tag}"
                );
                assert_eq!(
                    zone_recorded(&rec).last(),
                    Some(&(2 * RUN as u64 % zone)),
                    "{tag}: the child's short last zone"
                );
            }
            Graphs::RecordedByTurns => {
                // Zones 0, 2 and 4 hold seventeen graphs; 1 and 3 recorded.
                assert_eq!(recorded, 2 * zone, "{tag}: the recorded zones");
                assert_eq!(counted, RUN as u64 - 2 * zone, "{tag}: the others");
            }
            Graphs::DefaultThenThree => {
                // The run is the child's last RUN positions, the default
                // graph on its first half. The zones of the default graph
                // alone are taken and those of graph 3 alone skipped: only
                // the run's partial first zone counts on its encoding, and
                // the whole zone where the graphs change from its record.
                let start = RUN as u64;
                let partial = start.next_multiple_of(zone) - start;
                assert_eq!((counted, recorded), (partial, zone), "{tag}");
                // A stream reads those two zones' graphs, and only those.
                drop(rec);
                let rec = Recorder::start();
                let rows = streamed(store, pattern, 4096).await;
                assert_eq!(rows.len() as u64, want, "{tag}: the stream's rows");
                assert_eq!(zone_work(&rec).1, partial + zone, "{tag}: the stream read");
            }
            _ => {
                assert_eq!(recorded + counted, RUN as u64, "{tag}: every zone mixed");
                assert!(counted < zone, "{tag}: a partial zone alone counted");
            }
        }
    }
}

/// `GAP_ROWS` caps a by-reference run only where zones cannot decide it:
/// with the cap forced below the run, `(? type ? g1)` is located when its
/// child holds `g` and declines when it doesn't, as `(? type Taxon g1)`, two
/// checks, declines. Counts equal the rows' either way.
#[tokio::test]
async fn reference_cap_only_where_zones_cannot_decide() {
    use crate::CodePattern;
    use crate::store::test_hooks::{ForcedGapRows, ForcedZones};

    if !locate_enabled() {
        return;
    }

    let _on = ForcedZones::set(true);
    for graphs in [true, false] {
        let (_dir, path) = zone_file(
            ZoneFile::new(Graphs::Alternating, IndexConfig::Reference).reference_graphs(graphs),
        )
        .await;
        let store = &VortexRdfStore::from_file(&path).await.unwrap();
        let rows = row_list(store).await;
        let ty = code_of(store, "<http://example.org/type>").await;
        let taxon = code_of(store, "\"Taxon\"").await;
        let g1 = code_of(store, "<http://example.org/g1>").await;
        let _cap = ForcedGapRows::set(1_000);
        for (pattern, located) in [
            (CodePattern::new(None, Some(ty), None, Some(g1)), graphs),
            (
                CodePattern::new(None, Some(ty), Some(taxon), Some(g1)),
                false,
            ),
        ] {
            let tag = format!("graphs {graphs} {pattern:?}");
            let found = store.locate_codes(pattern, 0, Some(0)).await.unwrap();
            assert_eq!(found.is_some(), located, "{tag}");
            let want = rows
                .iter()
                .filter(|row| {
                    row[1] == ty && row[3] == g1 && pattern.object.is_none_or(|o| row[2] == o)
                })
                .count() as u64;
            assert_eq!(
                store.count_code_pattern(pattern).await.unwrap(),
                want,
                "{tag}"
            );
        }
    }
}

/// A reference child whose `g` has no zone maps (written in seven-row
/// leaves, as a foreign writer might): a graph-bound reference run reads the
/// child's `g` row by row and answers as its rows say, and `GAP_ROWS` caps
/// it, for zones cannot decide it.
#[tokio::test]
async fn reference_runs_without_zone_maps_check_rows() {
    use crate::CodePattern;
    use crate::store::test_hooks::{ForcedGapRows, ForcedZones};

    if !locate_enabled() {
        return;
    }

    let _on = ForcedZones::set(true);
    let (_dir, path) =
        zone_file(ZoneFile::new(Graphs::Alternating, IndexConfig::Reference).leaves(7)).await;
    let store = &VortexRdfStore::from_file(&path).await.unwrap();
    let rows = row_list(store).await;
    let ty = code_of(store, "<http://example.org/type>").await;
    let g1 = code_of(store, "<http://example.org/g1>").await;
    let pattern = CodePattern::new(None, Some(ty), None, Some(g1));
    let want = rows
        .iter()
        .filter(|row| row[1] == ty && row[3] == g1)
        .count() as u64;
    let rec = Recorder::start();
    assert_eq!(store.count_code_pattern(pattern).await.unwrap(), want);
    let (own, taken) = reference_graph_reads(&rec);
    assert_eq!((own, taken), (RUN as u64, 0), "the child's g, row by row");
    assert!(
        zone_recorded(&rec).is_empty(),
        "no zone counted from a record"
    );
    drop(rec);
    let mut got = code_rows::<4>(&store.match_code_pattern(pattern, 0, None).await.unwrap());
    let mut expected: Vec<[TermCode; 4]> = rows
        .iter()
        .filter(|row| row[1] == ty && row[3] == g1)
        .copied()
        .collect();
    got.sort_unstable();
    expected.sort_unstable();
    assert_eq!(got, expected, "the rows");
    let _cap = ForcedGapRows::set(1_000);
    assert!(
        store
            .locate_codes(pattern, 0, Some(0))
            .await
            .unwrap()
            .is_none(),
        "capped: zones cannot decide it"
    );
    assert_eq!(store.count_code_pattern(pattern).await.unwrap(), want);
}

/// A by-reference run asks its file's statistics only when a read needs
/// them: on alternating graphs a 64-row first page is read from the run's
/// first block and asks none; a later page and a count ask them. Every page
/// is the rows'.
#[tokio::test]
async fn reference_first_page_reads_the_first_block_before_statistics() {
    use crate::CodePattern;

    if !locate_enabled() {
        return;
    }

    let (_dir, path) = zone_file(ZoneFile::new(Graphs::Alternating, IndexConfig::Reference)).await;
    let store = &VortexRdfStore::from_file(&path).await.unwrap();
    let ty = code_of(store, "<http://example.org/type>").await;
    let g1 = code_of(store, "<http://example.org/g1>").await;
    let pattern = CodePattern::new(None, Some(ty), None, Some(g1));
    let page = |offset| async move {
        code_rows::<4>(
            &store
                .match_code_pattern(pattern, offset, Some(64))
                .await
                .unwrap(),
        )
    };
    let (_, rec) = zones_agree("the first page", || page(0)).await;
    assert_eq!(statistics_asked(&rec), 0, "none asked");
    drop(rec);
    let (_, rec) = zones_agree("the second page", || page(64)).await;
    assert_eq!(statistics_asked(&rec), 1, "a later page asks");
    drop(rec);
    let count = || async { store.count_code_pattern(pattern).await.unwrap() };
    let (_, rec) = zones_agree("a count", count).await;
    assert_eq!(statistics_asked(&rec), 1, "a count asks");
}

/// By reference, `(? type Taxon g1)` on alternating graphs: the key's run
/// (`index:ref-p`'s `type`, which ties `index:ref-o`'s `"Taxon"` and wins)
/// checks the graph in place, on the child's own `g` at every position, and
/// the object by id; with a check by id it asks no statistics. Its count is
/// the term path's, and it answers as with zones off.
#[tokio::test]
async fn reference_runs_check_their_graph_in_place_and_the_rest_by_id() {
    use crate::CodePattern;

    if !locate_enabled() {
        return;
    }

    let (_dir, path) = zone_file(ZoneFile::new(Graphs::Alternating, IndexConfig::Reference)).await;
    let store = &VortexRdfStore::from_file(&path).await.unwrap();
    let ty = code_of(store, "<http://example.org/type>").await;
    let taxon = code_of(store, "\"Taxon\"").await;
    let g1 = code_of(store, "<http://example.org/g1>").await;
    let pattern = CodePattern::new(None, Some(ty), Some(taxon), Some(g1));
    let want = term_rows(store, pattern).await.len() as u64;
    let count = || async { store.count_code_pattern(pattern).await.unwrap() };
    let (count, rec) = zones_agree("the count", count).await;
    assert_eq!(count, want);
    let (own, taken) = reference_graph_reads(&rec);
    assert_eq!(own, RUN as u64, "the graph read in place at every position");
    assert_eq!(taken, 0, "never taken by id");
    assert_eq!(statistics_asked(&rec), 0, "a check by id asks none");
    drop(rec);
    zones_agree("the answers", || code_answers(store, pattern)).await;
}

/// By reference, on a one-graph store, the footer settles `(? type ? g1)`'s
/// graph (every row holds the one graph), so the run reads its row ids alone:
/// no graph read and no zone map, its stream a block of row ids at a time,
/// as with zones off.
#[tokio::test]
async fn one_graph_reference_runs_read_row_ids_only() {
    use crate::CodePattern;

    if !locate_enabled() {
        return;
    }

    let (_dir, path) = zone_file(ZoneFile::new(Graphs::One, IndexConfig::Reference)).await;
    let store = &VortexRdfStore::from_file(&path).await.unwrap();
    let ty = code_of(store, "<http://example.org/type>").await;
    let g1 = code_of(store, "<http://example.org/g1>").await;
    let pattern = CodePattern::new(None, Some(ty), None, Some(g1));
    let (rows, rec) = zones_agree("the stream", || streamed(store, pattern, 4096)).await;
    assert_eq!(rows.len(), RUN, "every type row");
    assert_eq!(reference_graph_reads(&rec), (0, 0), "no graph read");
    assert_eq!(zone_maps_read(&rec), 0, "no zone map read");
    let blocks: u64 = rec.rows(|e| match e {
        Event::LocatedBlock(rows) => Some(*rows),
        _ => None,
    });
    assert_eq!(blocks, RUN as u64, "every position, once");
}
