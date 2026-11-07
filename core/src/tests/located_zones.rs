//! Located runs decided zone by zone: the zone maps of a store file's
//! columns, read back with their exact bounds and recorded graphs.

use super::*;
use crate::io::zone_map::ColumnZones;

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
/// none. A file written without the statistic, as before it, records no
/// zone's.
#[tokio::test]
async fn column_zones_read_recorded_counts() {
    use crate::io::zone_map::ZONE_ROWS;
    use crate::store::test_hooks::ForcedValueCounts;

    for recorded in [true, false] {
        let (_dir, path) = {
            let _statistic = ForcedValueCounts::set(recorded);
            write_store_file(
                zone_quads(Graphs::Stretches),
                LayoutStrategy::Dictionary,
                IndexConfig::Copy.types(),
            )
            .await
        };
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

/// Rows of each run of a [`zone_quads`] store.
pub(super) const RUN: usize = 40_000;

/// The rows of [`Graphs::UnrecordedGap`]'s seventeen graphs: zone 6 of
/// `index:ref-p`, whose `type` run is its rows `RUN..2 * RUN`.
pub(super) const UNRECORDED_GAP: std::ops::Range<usize> =
    6 * crate::io::zone_map::ZONE_ROWS - RUN..7 * crate::io::zone_map::ZONE_ROWS - RUN;

/// How a run's rows fall into graphs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Graphs {
    /// One graph only.
    One,
    /// Two graphs, each a half of the run.
    Long,
    /// Four graphs in turn, 1,000 rows each time.
    Stretches,
    /// Four graphs in turn, row by row.
    Alternating,
    /// Graph 1 on even rows and graph 3 on odd ones, but graph 2 on rows
    /// 20,000..21,000: every zone's bounds hold graph 2's code, one zone
    /// holds its rows.
    Gapped,
    /// Seventeen graphs in turn, row by row: every zone holds more graphs
    /// than `vortexrdf.value_counts` records.
    Seventeen,
    /// The default graph on even rows, graph 3 on odd ones.
    DefaultAndThree,
    /// The default graph on the first half of the run, graph 3 on the
    /// second.
    DefaultThenThree,
    /// Graph 1, but seventeen other graphs in turn on [`UNRECORDED_GAP`]:
    /// one zone of `index:ref-p`'s `type` run recording nothing, its bounds
    /// holding graph 1's code but none of its rows.
    UnrecordedGap,
    /// By turns of 8,192 rows: seventeen graphs in turn (past what the
    /// statistic records), then graphs 1, 3 and 18 in turn (recorded).
    RecordedByTurns,
}

impl Graphs {
    const ALL: [Graphs; 4] = [
        Graphs::One,
        Graphs::Long,
        Graphs::Stretches,
        Graphs::Alternating,
    ];

    /// The graph number of row `i` of a run.
    fn of(self, i: usize) -> usize {
        match self {
            Graphs::One => 1,
            Graphs::Long => 1 + i / (RUN / 2),
            Graphs::Stretches => 1 + (i / 1000) % 4,
            Graphs::Alternating => 1 + i % 4,
            Graphs::Gapped if (20_000..21_000).contains(&i) => 2,
            Graphs::Gapped => 1 + 2 * (i % 2),
            Graphs::Seventeen => 1 + i % 17,
            Graphs::DefaultAndThree => 3 * (i % 2),
            Graphs::DefaultThenThree => 3 * (i / (RUN / 2)),
            // The default graph, then graphs 2 to 17.
            Graphs::UnrecordedGap if UNRECORDED_GAP.contains(&i) => match i % 17 {
                0 => 0,
                k => k + 1,
            },
            Graphs::UnrecordedGap => 1,
            Graphs::RecordedByTurns if (i / 8_192).is_multiple_of(2) => 1 + i % 17,
            Graphs::RecordedByTurns => [1, 3, 18][i % 3],
        }
    }

    /// The last graph number a run holds.
    fn last(self) -> usize {
        match self {
            Graphs::One => 1,
            Graphs::Long => 2,
            Graphs::Gapped | Graphs::DefaultAndThree | Graphs::DefaultThenThree => 3,
            Graphs::Seventeen | Graphs::UnrecordedGap => 17,
            Graphs::RecordedByTurns => 18,
            Graphs::Stretches | Graphs::Alternating => 4,
        }
    }
}

/// [`RUN`] quads `(s{i}, type, "Taxon", g(i))`, an `index:posg` run, and
/// [`RUN`] quads `(hub, link, "o{i}", g(i))`, a quad-table run of `hub`.
pub(super) fn zone_quads(graphs: Graphs) -> Vec<Quad> {
    let taxa = (0..RUN).map(|i| {
        make_quad(
            &format!("http://example.org/s{i:05}"),
            "http://example.org/type",
            "Taxon",
            numbered_graph(graphs.of(i)),
        )
    });
    let links = (0..RUN).map(|i| {
        make_quad(
            "http://example.org/hub",
            "http://example.org/link",
            &format!("o{i:05}"),
            numbered_graph(graphs.of(i)),
        )
    });
    taxa.chain(links).collect()
}

/// A store under test, its tag and its file's guard.
struct ZoneStore {
    tag: String,
    graphs: Graphs,
    index: IndexConfig,
    store: VortexRdfStore,
    _dir: tempfile::TempDir,
}

/// Each [`Graphs`] layout written without indexes, with the copy index and
/// with the reference index, and the 1,000-row stretches in seven-row
/// leaves, which have no zone maps.
async fn zone_stores() -> Vec<ZoneStore> {
    let mut stores = Vec::new();
    for graphs in Graphs::ALL {
        for index in [
            IndexConfig::NoIndex,
            IndexConfig::Copy,
            IndexConfig::Reference,
        ] {
            let (dir, path) = write_store_file(
                zone_quads(graphs),
                LayoutStrategy::Dictionary,
                index.types(),
            )
            .await;
            stores.push(ZoneStore {
                tag: format!("{graphs:?}/{}", index.name()),
                graphs,
                index,
                store: VortexRdfStore::from_file(&path).await.unwrap(),
                _dir: dir,
            });
        }
    }
    let (dir, path) = write_chunked_store_file(&zone_quads(Graphs::Stretches), 7).await;
    stores.push(ZoneStore {
        tag: "Stretches/seven-row-leaves".to_string(),
        graphs: Graphs::Stretches,
        index: IndexConfig::NoIndex,
        store: VortexRdfStore::from_file(&path).await.unwrap(),
        _dir: dir,
    });
    stores
}

/// The code of `term` (its N-Triples spelling) in `store`'s dictionary.
pub(super) async fn code_of(store: &VortexRdfStore, term: &str) -> TermCode {
    store
        .dict_reader()
        .unwrap()
        .encode(term)
        .await
        .unwrap()
        .unwrap_or_else(|| panic!("{term} is in the dictionary"))
}

/// The patterns checked on `store` for graph `g`: the `index:posg` run, the
/// quad table's `(hub, link)` run, `hub`'s run checked at two slots, and
/// `(? type ? g)` and `(? ? Taxon g)`, by reference the runs of `index:ref-p`
/// and `index:ref-o` checked against the graph alone.
async fn zone_patterns(store: &VortexRdfStore, g: usize) -> Vec<crate::CodePattern> {
    use crate::CodePattern;
    let graph = code_of(store, &format!("<http://example.org/g{g}>")).await;
    let ty = code_of(store, "<http://example.org/type>").await;
    let taxon = code_of(store, "\"Taxon\"").await;
    let hub = code_of(store, "<http://example.org/hub>").await;
    let link = code_of(store, "<http://example.org/link>").await;
    let mut patterns = vec![
        CodePattern::new(None, Some(ty), Some(taxon), Some(graph)),
        CodePattern::new(Some(hub), Some(link), None, Some(graph)),
    ];
    for i in [0, RUN / 2] {
        let o = code_of(store, &format!("\"o{i:05}\"")).await;
        patterns.push(CodePattern::new(Some(hub), None, Some(o), Some(graph)));
    }
    patterns.push(CodePattern::new(None, Some(ty), None, Some(graph)));
    patterns.push(CodePattern::new(None, None, Some(taxon), Some(graph)));
    patterns
}

/// The rows `pattern` streams in chunks of `batch` rows, in order.
async fn streamed(
    store: &VortexRdfStore,
    pattern: crate::CodePattern,
    batch: usize,
) -> Vec<[TermCode; 4]> {
    let chunks: Vec<Vec<vortex_buffer::Buffer<TermCode>>> = store
        .match_code_chunks(pattern, batch)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    chunks
        .iter()
        .flat_map(|chunk| code_rows::<4>(chunk))
        .collect()
}

/// What a pattern answers: its count, its windows, its streams, and its
/// located windows with their totals.
type Answers = (
    u64,
    Vec<Vec<[TermCode; 4]>>,
    Vec<Vec<[TermCode; 4]>>,
    Vec<(u64, Vec<[TermCode; 4]>)>,
);

/// What `pattern` answers on `store`: its count, its windows (pages of 1,
/// 3 and 64 at the run's ends and middle, and 32 one-row reads spread over
/// it), its streams (chunks of 999 and 4,096 rows), and the pages again
/// through `locate_codes`, each with the run's count beside it.
async fn answers(store: &VortexRdfStore, pattern: crate::CodePattern) -> Answers {
    let total = store.count_code_pattern(pattern).await.unwrap();
    if let Some(located) = store.locate_codes(pattern, 0, Some(0)).await.unwrap() {
        assert_eq!(located.total, total, "{pattern:?}: the located count");
    }
    let t = total as usize;
    let mut reads: Vec<(usize, usize)> = Vec::new();
    for limit in [1, 3, 64] {
        for offset in [0, 1, t / 2, t.saturating_sub(1), t] {
            reads.push((offset, limit));
        }
    }
    let mut located = Vec::new();
    for &(offset, limit) in &reads {
        if let Some(found) = store
            .locate_codes(pattern, offset, Some(limit))
            .await
            .unwrap()
        {
            assert_eq!(found.total, total, "{pattern:?}: a located page's count");
            located.push((found.total, code_rows::<4>(&found.columns)));
        }
    }
    reads.extend((0..32).map(|k| (k * t / 32, 1)));
    let mut windows = Vec::new();
    for (offset, limit) in reads {
        let columns = store
            .match_code_pattern(pattern, offset, Some(limit))
            .await
            .unwrap();
        windows.push(code_rows::<4>(&columns));
    }
    let mut streams = Vec::new();
    for batch in [999, 4096] {
        streams.push(streamed(store, pattern, batch).await);
    }
    (total, windows, streams, located)
}

/// The term path's rows of `pattern`: its codes spelled out and matched.
async fn term_rows(store: &VortexRdfStore, pattern: crate::CodePattern) -> Vec<[TermCode; 4]> {
    let dict = store.dict_reader().unwrap();
    let mut spelled: [Option<String>; 4] = Default::default();
    let slots = [
        pattern.subject,
        pattern.predicate,
        pattern.object,
        pattern.graph,
    ];
    for (spelling, code) in spelled.iter_mut().zip(slots) {
        if let Some(code) = code {
            *spelling = dict.decode(code).await.unwrap();
        }
    }
    let probe = crate::Probe::parse(
        spelled[0].as_deref(),
        spelled[1].as_deref(),
        spelled[2].as_deref(),
        spelled[3].as_deref(),
    )
    .unwrap();
    sorted_rows(&store.run_probe(&probe).await.unwrap()).await
}

/// Every pattern of every store answers the same with zones and without:
/// counts, windows and streams, row for row and in order, and as the term
/// path does.
#[tokio::test]
async fn zoned_runs_answer_as_rows_do() {
    use crate::store::test_hooks::ForcedZones;

    for fixture in zone_stores().await {
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
                let zoned = {
                    let _on = ForcedZones::set(true);
                    answers(store, pattern).await
                };
                let rows = {
                    let _off = ForcedZones::set(false);
                    answers(store, pattern).await
                };
                assert!(zoned == rows, "{label}: zones and rows answer alike");
                let want = term_rows(store, pattern).await;
                assert_eq!(zoned.0, want.len() as u64, "{label}: the count");
                let unwindowed =
                    code_rows::<4>(&store.match_code_pattern(pattern, 0, None).await.unwrap());
                let mut sorted = unwindowed.clone();
                sorted.sort_unstable();
                assert_eq!(sorted, want, "{label}: the rows");
                for stream in &zoned.2 {
                    assert_eq!(stream, &unwindowed, "{label}: a stream");
                }
            }
        }
    }
}

/// Rows `rec` saw counted on a mixed zone's encoding, and read to check.
fn zone_work(rec: &Recorder) -> (u64, u64) {
    let counted = rec.picked(|e| match e {
        Event::ZoneCounted(rows) => Some(*rows),
        _ => None,
    });
    let read = rec.picked(|e| match e {
        Event::CheckRead(rows) => Some(*rows),
        _ => None,
    });
    (counted.iter().sum(), read.iter().sum())
}

/// Rows of each mixed zone `rec` saw counted from its recorded values, in
/// order.
fn zone_recorded(rec: &Recorder) -> Vec<u64> {
    rec.picked(|e| match e {
        Event::ZoneRecorded(rows) => Some(*rows),
        _ => None,
    })
}

/// With zones, a count reads no checked rows: a zone of the graph alone
/// counts its size, a zone without it nothing, and a mixed one is counted
/// from its record or on its encoding; a stream reads the mixed zones only.
/// A store without zone maps reads its run as before.
#[tokio::test]
async fn zoned_runs_skip_settled_zones() {
    use crate::store::test_hooks::ForcedZones;

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
                    counted < crate::io::zone_map::ZONE_ROWS as u64,
                    "{}: {counted} rows counted",
                    fixture.tag
                );
            }
            (_, _, true) => {
                let rec = Recorder::start();
                assert_eq!(store.count_code_pattern(links).await.unwrap(), 10_000);
                let blocks: u64 = rec
                    .picked(|e| match e {
                        Event::LocatedBlock(rows) => Some(*rows),
                        _ => None,
                    })
                    .iter()
                    .sum();
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
/// and zones and rows answer alike. A file written without the statistic,
/// as before it, counts as P1 did: every zone of the run on its encoding
/// (Review Focus 4).
#[tokio::test]
async fn recorded_graphs_settle_zones_without_the_code() {
    use crate::io::zone_map::ZONE_ROWS;
    use crate::store::test_hooks::{ForcedValueCounts, ForcedZones};

    for recorded in [true, false] {
        let (_dir, path) = {
            let _statistic = ForcedValueCounts::set(recorded);
            write_store_file(
                zone_quads(Graphs::Gapped),
                LayoutStrategy::Dictionary,
                IndexConfig::Copy.types(),
            )
            .await
        };
        let store = &VortexRdfStore::from_file(&path).await.unwrap();
        let taxa = zone_patterns(store, 2).await[0];
        let want = term_rows(store, taxa).await.len() as u64;
        assert_eq!(want, 1_000, "graph 2's rows");
        let rows = {
            let _off = ForcedZones::set(false);
            answers(store, taxa).await
        };
        let _on = ForcedZones::set(true);
        let rec = Recorder::start();
        assert_eq!(store.count_code_pattern(taxa).await.unwrap(), want);
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
        assert!(
            answers(store, taxa).await == rows,
            "recorded {recorded}: zones and rows answer alike"
        );
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
    use crate::io::zone_map::ZONE_ROWS;
    use crate::store::test_hooks::{ForcedValueCounts, ForcedZones};

    // The run is rows RUN..2 * RUN of its child, graph 2's rows its rows
    // 20,000..21,000: they lie in the child's zone from 57,344.
    let first_match_zone = (RUN + 20_000) / ZONE_ROWS * ZONE_ROWS;
    for index in [IndexConfig::Copy, IndexConfig::Reference] {
        let (_dir, path) = {
            let _statistic = ForcedValueCounts::set(false);
            write_store_file(
                zone_quads(Graphs::Gapped),
                LayoutStrategy::Dictionary,
                index.types(),
            )
            .await
        };
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
        let want = {
            let _off = ForcedZones::set(false);
            page().await
        };
        let _on = ForcedZones::set(true);
        let rec = Recorder::start();
        let got = page().await;
        let (counted, read) = zone_work(&rec);
        drop(rec);
        let tag = index.name();
        assert_eq!(got.len(), 10, "{tag}");
        assert_eq!(got, want, "{tag}: the first page");
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
    use crate::io::zone_map::ZONE_ROWS;
    use crate::store::test_hooks::ForcedZones;

    let _on = ForcedZones::set(true);
    let (_dir, path) = write_store_file(
        zone_quads(Graphs::Alternating),
        LayoutStrategy::Dictionary,
        IndexConfig::Copy.types(),
    )
    .await;
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
/// before the statistic. `(? type Taxon g1)` counts exactly, every zone of
/// the run on its encoding and none from a record.
#[tokio::test]
async fn zones_past_max_values_count_their_encoding() {
    use crate::store::test_hooks::ForcedZones;

    let _on = ForcedZones::set(true);
    let (_dir, path) = write_store_file(
        zone_quads(Graphs::Seventeen),
        LayoutStrategy::Dictionary,
        IndexConfig::Copy.types(),
    )
    .await;
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
/// starts the table): a zone past the limit reads as unrecorded (Review Focus
/// 1). A count takes the recorded zones from their records and counts the
/// others on their encoding, for graph 1, which every zone holds, and for
/// graph 18, which the unrecorded zones' bounds hold but their rows don't:
/// those zones aren't settled none. Every answer is the rows'.
#[tokio::test]
async fn recorded_and_unrecorded_zones_by_turns() {
    use crate::CodePattern;
    use crate::io::zone_map::ZONE_ROWS;
    use crate::store::test_hooks::ForcedZones;
    use std::collections::BTreeSet;

    let (_dir, path) = write_store_file(
        zone_quads(Graphs::RecordedByTurns),
        LayoutStrategy::Dictionary,
        IndexConfig::NoIndex.types(),
    )
    .await;
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
        let zoned = answers(store, pattern).await;
        let rows = {
            let _off = ForcedZones::set(false);
            answers(store, pattern).await
        };
        assert!(zoned == rows, "graph {g}: zones and rows answer alike");
    }
}

/// The default graph is counted from records like any named graph: on
/// zones mixing it with graph 3 ([`Graphs::DefaultAndThree`]),
/// `(? type Taxon default)` counts its rows exactly, the run's whole zones
/// from their records (Review Focus 3).
#[tokio::test]
async fn default_graph_counts_from_records() {
    use crate::CodePattern;
    use crate::io::zone_map::ZONE_ROWS;
    use crate::store::test_hooks::ForcedZones;

    let _on = ForcedZones::set(true);
    let (_dir, path) = write_store_file(
        zone_quads(Graphs::DefaultAndThree),
        LayoutStrategy::Dictionary,
        IndexConfig::Copy.types(),
    )
    .await;
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

/// The reads `rec` saw a reference run make of graphs: its child's own
/// (`ReferenceGraphRead`), and the quad table's at its rows (`CheckTaken` of
/// slot 3).
fn reference_graph_reads(rec: &Recorder) -> (u64, u64) {
    let own = rec.picked(|e| match e {
        Event::ReferenceGraphRead(rows) => Some(*rows),
        _ => None,
    });
    let taken = rec.picked(|e| match e {
        Event::CheckTaken { slot: 3, rows } => Some(*rows),
        _ => None,
    });
    (own.iter().sum(), taken.iter().sum())
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
    use crate::io::zone_map::ZONE_ROWS;
    use crate::store::test_hooks::ForcedZones;

    let _on = ForcedZones::set(true);
    let zone = ZONE_ROWS as u64;
    for graphs in [
        Graphs::Long,
        Graphs::Alternating,
        Graphs::RecordedByTurns,
        Graphs::DefaultAndThree,
        Graphs::DefaultThenThree,
    ] {
        let (_dir, path) = write_store_file(
            zone_quads(graphs),
            LayoutStrategy::Dictionary,
            IndexConfig::Reference.types(),
        )
        .await;
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
    use crate::store::test_hooks::{ForcedGapRows, ForcedReferenceGraphs, ForcedZones};

    let _on = ForcedZones::set(true);
    for graphs in [true, false] {
        let (_dir, path) = {
            let _hook = ForcedReferenceGraphs::set(graphs);
            write_store_file(
                zone_quads(Graphs::Alternating),
                LayoutStrategy::Dictionary,
                IndexConfig::Reference.types(),
            )
            .await
        };
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
/// it as before, for zones cannot decide it.
#[tokio::test]
async fn reference_runs_without_zone_maps_check_rows() {
    use crate::CodePattern;
    use crate::store::test_hooks::{ForcedGapRows, ForcedZones};

    let _on = ForcedZones::set(true);
    let (_dir, path) =
        write_chunked_reference_store_file(&zone_quads(Graphs::Alternating), 7).await;
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
    use crate::store::test_hooks::ForcedZones;

    let (_dir, path) = write_store_file(
        zone_quads(Graphs::Alternating),
        LayoutStrategy::Dictionary,
        IndexConfig::Reference.types(),
    )
    .await;
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
    let want = {
        let _off = ForcedZones::set(false);
        (page(0).await, page(64).await)
    };
    let _on = ForcedZones::set(true);
    let rec = Recorder::start();
    assert_eq!(page(0).await, want.0, "the first page");
    assert_eq!(statistics_asked(&rec), 0, "none asked");
    drop(rec);
    let rec = Recorder::start();
    assert_eq!(page(64).await, want.1, "the second page");
    assert_eq!(statistics_asked(&rec), 1, "a later page asks");
    drop(rec);
    let rec = Recorder::start();
    store.count_code_pattern(pattern).await.unwrap();
    assert_eq!(statistics_asked(&rec), 1, "a count asks");
}

/// Zone maps `rec` saw read from a file.
fn zone_maps_read(rec: &Recorder) -> usize {
    rec.count(&Event::ZoneMapRead)
}

/// Each zone map a file's reads ask for is read once per file and column,
/// whoever asks: `(? type ? g1)` counted twice reads `index:posg`'s `g` map
/// once by copy, `index:ref-p`'s once by reference, and two such counts
/// joined on one task, on a store opened afresh, read it once in all; with
/// no index, the general match's every-row step and a gap run of the quad
/// table checked on `g` share the quad table's `g` map. Every count is the
/// zones-off count.
#[tokio::test]
async fn each_zone_map_reads_once_per_file() {
    use crate::CodePattern;
    use crate::store::test_hooks::ForcedZones;

    for index in [IndexConfig::Copy, IndexConfig::Reference] {
        let tag = index.name();
        let (_dir, path) = write_store_file(
            zone_quads(Graphs::Alternating),
            LayoutStrategy::Dictionary,
            index.types(),
        )
        .await;
        let store = VortexRdfStore::from_file(&path).await.unwrap();
        let ty = code_of(&store, "<http://example.org/type>").await;
        let g1 = code_of(&store, "<http://example.org/g1>").await;
        let pattern = CodePattern::new(None, Some(ty), None, Some(g1));
        let want = {
            let _off = ForcedZones::set(false);
            store.count_code_pattern(pattern).await.unwrap()
        };
        let _on = ForcedZones::set(true);
        for read in [1, 0] {
            let rec = Recorder::start();
            assert_eq!(
                store.count_code_pattern(pattern).await.unwrap(),
                want,
                "{tag}"
            );
            assert_eq!(zone_maps_read(&rec), read, "{tag}: maps read by this count");
        }
        let fresh = VortexRdfStore::from_file(&path).await.unwrap();
        let rec = Recorder::start();
        let (first, second) = futures::join!(
            fresh.count_code_pattern(pattern),
            fresh.count_code_pattern(pattern)
        );
        assert_eq!((first.unwrap(), second.unwrap()), (want, want), "{tag}");
        assert_eq!(
            zone_maps_read(&rec),
            1,
            "{tag}: joined counts read the map once"
        );
    }
    let (_dir, path) = write_store_file(
        zone_quads(Graphs::Alternating),
        LayoutStrategy::Dictionary,
        IndexConfig::NoIndex.types(),
    )
    .await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let hub = code_of(&store, "<http://example.org/hub>").await;
    let g1 = code_of(&store, "<http://example.org/g1>").await;
    let every_row = CodePattern::new(None, None, None, Some(g1));
    let gap = CodePattern::new(Some(hub), None, None, Some(g1));
    let want = {
        let _off = ForcedZones::set(false);
        (
            store.count_code_pattern(every_row).await.unwrap(),
            store.count_code_pattern(gap).await.unwrap(),
        )
    };
    let _on = ForcedZones::set(true);
    let rec = Recorder::start();
    assert_eq!(store.count_code_pattern(every_row).await.unwrap(), want.0);
    assert_eq!(zone_maps_read(&rec), 1, "the every-row step reads g's map");
    drop(rec);
    let rec = Recorder::start();
    assert_eq!(store.count_code_pattern(gap).await.unwrap(), want.1);
    assert_eq!(zone_maps_read(&rec), 0, "the gap run shares it");
}

/// By reference, `(? type Taxon g1)` on alternating graphs: the key's run
/// (`index:ref-p`'s `type`, which ties `index:ref-o`'s `"Taxon"` and wins)
/// checks the graph in place, on the child's own `g` at every position, and
/// the object by id; with a check by id it asks no statistics. Its count is
/// the term path's, and it answers as with zones off.
#[tokio::test]
async fn reference_runs_check_their_graph_in_place_and_the_rest_by_id() {
    use crate::CodePattern;
    use crate::store::test_hooks::ForcedZones;

    let (_dir, path) = write_store_file(
        zone_quads(Graphs::Alternating),
        LayoutStrategy::Dictionary,
        IndexConfig::Reference.types(),
    )
    .await;
    let store = &VortexRdfStore::from_file(&path).await.unwrap();
    let ty = code_of(store, "<http://example.org/type>").await;
    let taxon = code_of(store, "\"Taxon\"").await;
    let g1 = code_of(store, "<http://example.org/g1>").await;
    let pattern = CodePattern::new(None, Some(ty), Some(taxon), Some(g1));
    let want = term_rows(store, pattern).await.len() as u64;
    let off = {
        let _off = ForcedZones::set(false);
        answers(store, pattern).await
    };
    let _on = ForcedZones::set(true);
    let rec = Recorder::start();
    assert_eq!(store.count_code_pattern(pattern).await.unwrap(), want);
    let (own, taken) = reference_graph_reads(&rec);
    assert_eq!(own, RUN as u64, "the graph read in place at every position");
    assert_eq!(taken, 0, "never taken by id");
    assert_eq!(statistics_asked(&rec), 0, "a check by id asks none");
    drop(rec);
    assert!(
        answers(store, pattern).await == off,
        "zones on and off answer alike"
    );
}

/// By reference, on a one-graph store, the footer settles `(? type ? g1)`'s
/// graph (every row holds the one graph), so the run reads its row ids alone:
/// no graph read and no zone map, its stream a block of row ids at a time,
/// as with zones off.
#[tokio::test]
async fn one_graph_reference_runs_read_row_ids_only() {
    use crate::CodePattern;
    use crate::store::test_hooks::ForcedZones;

    let (_dir, path) = write_store_file(
        zone_quads(Graphs::One),
        LayoutStrategy::Dictionary,
        IndexConfig::Reference.types(),
    )
    .await;
    let store = &VortexRdfStore::from_file(&path).await.unwrap();
    let ty = code_of(store, "<http://example.org/type>").await;
    let g1 = code_of(store, "<http://example.org/g1>").await;
    let pattern = CodePattern::new(None, Some(ty), None, Some(g1));
    let want = {
        let _off = ForcedZones::set(false);
        streamed(store, pattern, 4096).await
    };
    assert_eq!(want.len(), RUN, "every type row");
    let _on = ForcedZones::set(true);
    let rec = Recorder::start();
    assert_eq!(streamed(store, pattern, 4096).await, want);
    assert_eq!(reference_graph_reads(&rec), (0, 0), "no graph read");
    assert_eq!(zone_maps_read(&rec), 0, "no zone map read");
    let blocks: u64 = rec
        .picked(|e| match e {
            Event::LocatedBlock(rows) => Some(*rows),
            _ => None,
        })
        .iter()
        .sum();
    assert_eq!(blocks, RUN as u64, "every position, once");
}

/// Runs with no checks never ask the file's statistics, however long: by
/// copy, `(? type ? ?)` is an exact run of `index:posg`; by reference, the
/// run of `index:ref-p`'s `type` alone. Counted and streamed, each answers
/// as the term path.
#[tokio::test]
async fn runs_without_checks_ask_no_statistics() {
    use crate::CodePattern;
    use crate::store::test_hooks::ForcedZones;

    for index in [IndexConfig::Copy, IndexConfig::Reference] {
        let tag = index.name();
        let (_dir, path) = write_store_file(
            zone_quads(Graphs::Alternating),
            LayoutStrategy::Dictionary,
            index.types(),
        )
        .await;
        let store = &VortexRdfStore::from_file(&path).await.unwrap();
        let ty = code_of(store, "<http://example.org/type>").await;
        let pattern = CodePattern::new(None, Some(ty), None, None);
        let mut want = term_rows(store, pattern).await;
        want.sort_unstable();
        assert_eq!(want.len(), RUN, "{tag}: every type row");
        let _on = ForcedZones::set(true);
        let rec = Recorder::start();
        assert_eq!(
            store.count_code_pattern(pattern).await.unwrap(),
            RUN as u64,
            "{tag}"
        );
        let mut got = streamed(store, pattern, 4096).await;
        assert_eq!(statistics_asked(&rec), 0, "{tag}: no statistics asked");
        got.sort_unstable();
        assert_eq!(got, want, "{tag}");
    }
}

/// A long checked run's stream asks the file's statistics once, on its first
/// pull, after a 64-row first page, read from the run's first block, asked
/// none: `(? type ? g1)` by copy on alternating graphs. Its rows are the
/// term path's.
#[tokio::test]
async fn streams_ask_statistics_once() {
    use crate::CodePattern;
    use crate::store::test_hooks::ForcedZones;

    let (_dir, path) = write_store_file(
        zone_quads(Graphs::Alternating),
        LayoutStrategy::Dictionary,
        IndexConfig::Copy.types(),
    )
    .await;
    let store = &VortexRdfStore::from_file(&path).await.unwrap();
    let ty = code_of(store, "<http://example.org/type>").await;
    let g1 = code_of(store, "<http://example.org/g1>").await;
    let pattern = CodePattern::new(None, Some(ty), None, Some(g1));
    let _on = ForcedZones::set(true);
    let rec = Recorder::start();
    let page = code_rows::<4>(
        &store
            .match_code_pattern(pattern, 0, Some(64))
            .await
            .unwrap(),
    );
    assert_eq!(page.len(), 64);
    assert_eq!(statistics_asked(&rec), 0, "the first page asks none");
    drop(rec);
    let rec = Recorder::start();
    let mut got = streamed(store, pattern, 4096).await;
    assert_eq!(statistics_asked(&rec), 1, "the stream asks once");
    drop(rec);
    let mut want = term_rows(store, pattern).await;
    got.sort_unstable();
    want.sort_unstable();
    assert_eq!(got, want);
}

/// The footer settles a checked slot for the whole file before any zone map
/// is read: on a one-graph store a graph check needs no reading at all, and a
/// graph code no row holds (here the `type` predicate's) matches nothing,
/// still from the located run's table.
#[tokio::test]
async fn footer_settles_a_whole_file() {
    use crate::CodePattern;
    use crate::store::test_hooks::ForcedZones;

    let _on = ForcedZones::set(true);
    for fixture in zone_stores().await {
        let store = &fixture.store;
        let patterns = zone_patterns(store, 1).await;
        // The copy index's posg run; on a reference store `index:ref-p`'s
        // `type` run, checked against the graph alone; else the quad
        // table's (hub, link) run.
        let located = match fixture.index {
            IndexConfig::Copy => patterns[0],
            IndexConfig::Reference => patterns[4],
            IndexConfig::NoIndex => patterns[1],
        };
        let ty = code_of(store, "<http://example.org/type>").await;
        let held_by_none = CodePattern {
            graph: Some(ty),
            ..located
        };
        let rec = Recorder::start();
        assert_eq!(
            store.count_code_pattern(held_by_none).await.unwrap(),
            0,
            "{}",
            fixture.tag
        );
        let answered = store.locate_codes(held_by_none, 0, Some(0)).await.unwrap();
        assert_eq!(
            answered.map(|l| l.total),
            Some(0),
            "{}: still located",
            fixture.tag
        );
        if !fixture.tag.ends_with("leaves") {
            assert_eq!(
                zone_maps_read(&rec),
                0,
                "{}: the footer settles it",
                fixture.tag
            );
            assert_eq!(zone_work(&rec), (0, 0), "{}", fixture.tag);
        }
        drop(rec);
        if fixture.graphs == Graphs::One {
            let rec = Recorder::start();
            assert_eq!(store.count_code_pattern(located).await.unwrap(), RUN as u64);
            assert_eq!(zone_maps_read(&rec), 0, "{}: one graph", fixture.tag);
            assert_eq!(zone_work(&rec), (0, 0), "{}: one graph", fixture.tag);
            assert_eq!(rec.count_where(|e| matches!(e, Event::LocatedBlock(_))), 0);
        }
    }
}

/// `GAP_ROWS` caps only a run whose checks zones cannot decide: with the cap
/// forced below the run, the copy index's run is still located with zones,
/// and declines without them, as does a run whose column has no zone map.
/// Counts answer alike either way.
#[tokio::test]
async fn gap_cap_only_where_zones_cannot_decide() {
    use crate::store::test_hooks::{ForcedGapRows, ForcedZones};

    let _cap = ForcedGapRows::set(1_000);
    for fixture in zone_stores().await {
        let store = &fixture.store;
        let patterns = zone_patterns(store, 1).await;
        let (pattern, zoned) = match (
            fixture.graphs,
            fixture.index,
            fixture.tag.ends_with("leaves"),
        ) {
            (Graphs::Alternating, IndexConfig::Copy, _) => (patterns[0], true),
            (_, _, true) => (patterns[1], false),
            _ => continue,
        };
        let want = term_rows(store, pattern).await.len() as u64;
        {
            let _on = ForcedZones::set(true);
            let located = store.locate_codes(pattern, 0, Some(0)).await.unwrap();
            assert_eq!(
                located.is_some(),
                zoned,
                "{}: located with zones",
                fixture.tag
            );
            assert_eq!(store.count_code_pattern(pattern).await.unwrap(), want);
        }
        let _off = ForcedZones::set(false);
        assert!(
            store
                .locate_codes(pattern, 0, Some(0))
                .await
                .unwrap()
                .is_none(),
            "{}",
            fixture.tag
        );
        assert_eq!(store.count_code_pattern(pattern).await.unwrap(), want);
    }
}

/// With `VORTEX_RDF_COUNTERS=1`, a zoned count is counted where it settles
/// zones and counts mixed ones (a no-op without the variable).
#[tokio::test]
async fn zone_counters_count_decisions() {
    use crate::store::test_hooks::ForcedZones;
    use vortex_rdf_encoded_search::counters::{
        ZONE_ALL, ZONE_COUNTED, ZONE_NONE, ZONE_RECORDED, on, snapshot,
    };

    if !on() {
        return;
    }
    let _on = ForcedZones::set(true);
    let quads = zone_quads(Graphs::Long);
    let (_dir, path) =
        write_store_file(quads, LayoutStrategy::Dictionary, IndexConfig::Copy.types()).await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let taxa = zone_patterns(&store, 1).await[0];
    let before = snapshot();
    assert_eq!(store.count_code_pattern(taxa).await.unwrap(), 20_000);
    let after = snapshot();
    for site in [ZONE_ALL, ZONE_NONE, ZONE_COUNTED] {
        assert!(after[site] > before[site], "site {site}");
    }
    // Alternating graphs mix every zone: the whole ones count from their
    // records.
    let (_dir, path) = write_store_file(
        zone_quads(Graphs::Alternating),
        LayoutStrategy::Dictionary,
        IndexConfig::Copy.types(),
    )
    .await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let taxa = zone_patterns(&store, 1).await[0];
    let before = snapshot();
    assert_eq!(store.count_code_pattern(taxa).await.unwrap(), 10_000);
    assert!(snapshot()[ZONE_RECORDED] > before[ZONE_RECORDED]);
}

/// A gap run shorter than a zone checks its few rows directly: asking the
/// footer and the zone maps would cost a short probe (a join's per-binding
/// lookup) more than the rows. It reads no zone map, answers as without
/// zones, and its rows are checked.
#[tokio::test]
async fn short_runs_check_rows_without_statistics() {
    use crate::CodePattern;
    use crate::store::test_hooks::ForcedZones;

    for index in [IndexConfig::NoIndex, IndexConfig::Copy] {
        let (_dir, path) = write_store_file(
            zone_quads(Graphs::Long),
            LayoutStrategy::Dictionary,
            index.types(),
        )
        .await;
        let store = VortexRdfStore::from_file(&path).await.unwrap();
        let ty = code_of(&store, "<http://example.org/type>").await;
        let g1 = code_of(&store, "<http://example.org/g1>").await;
        for (i, want) in [(5, 1), (30_000, 0)] {
            let s = code_of(&store, &format!("<http://example.org/s{i:05}>")).await;
            let pattern = CodePattern::new(Some(s), Some(ty), None, Some(g1));
            let rows = {
                let _off = ForcedZones::set(false);
                code_rows::<4>(&store.match_code_pattern(pattern, 0, None).await.unwrap())
            };
            let _on = ForcedZones::set(true);
            let rec = Recorder::start();
            assert_eq!(store.count_code_pattern(pattern).await.unwrap(), want);
            let got = code_rows::<4>(&store.match_code_pattern(pattern, 0, None).await.unwrap());
            assert_eq!(got, rows, "{} s{i}", index.name());
            assert_eq!(
                zone_maps_read(&rec),
                0,
                "{} s{i}: no zone map",
                index.name()
            );
            assert!(
                zone_work(&rec).1 > 0,
                "{} s{i}: its rows checked",
                index.name()
            );
        }
    }
}

/// Statistics `rec` saw a located run ask of its file.
fn statistics_asked(rec: &Recorder) -> usize {
    rec.count(&Event::StatisticsAsked)
}

/// A first page that a long checked run's first block fills is read from
/// that block, row by row, before the file's statistics are asked, which
/// cost more than the block: on interleaved graphs a 64-row page reads
/// 4,096 rows and asks nothing. A first page the block leaves short (its
/// graph starts half-way), a later page and a count ask them. Every page
/// is the rows' own.
#[tokio::test]
async fn first_page_reads_the_first_block_before_statistics() {
    use crate::store::test_hooks::ForcedZones;

    for index in [IndexConfig::NoIndex, IndexConfig::Copy] {
        for (graphs, g, fills) in [(Graphs::Alternating, 1, true), (Graphs::Long, 2, false)] {
            let (_dir, path) = write_store_file(
                zone_quads(graphs),
                LayoutStrategy::Dictionary,
                index.types(),
            )
            .await;
            let store = &VortexRdfStore::from_file(&path).await.unwrap();
            let patterns = zone_patterns(store, g).await;
            // The quad table's (hub, link) run, and on a copy store the
            // index:posg run.
            let patterns = match index {
                IndexConfig::Copy => &patterns[..2],
                _ => &patterns[1..2],
            };
            for (i, &pattern) in patterns.iter().enumerate() {
                let tag = format!("{graphs:?}/{} g{g} pattern {i}", index.name());
                let page = |offset| async move {
                    code_rows::<4>(
                        &store
                            .match_code_pattern(pattern, offset, Some(64))
                            .await
                            .unwrap(),
                    )
                };
                let want = {
                    let _off = ForcedZones::set(false);
                    (page(0).await, page(64).await)
                };
                let _on = ForcedZones::set(true);
                let rec = Recorder::start();
                let first = page(0).await;
                assert_eq!(first.len(), 64, "{tag}");
                assert_eq!(first, want.0, "{tag}: the first page");
                if fills {
                    assert_eq!(statistics_asked(&rec), 0, "{tag}: none asked");
                    assert_eq!(zone_work(&rec), (0, 4096), "{tag}: the first block");
                } else {
                    assert_eq!(statistics_asked(&rec), 1, "{tag}: the block fell short");
                }
                drop(rec);
                let rec = Recorder::start();
                assert_eq!(page(64).await, want.1, "{tag}: the second page");
                assert_eq!(statistics_asked(&rec), 1, "{tag}: a later page asks");
                drop(rec);
                let rec = Recorder::start();
                store.count_code_pattern(pattern).await.unwrap();
                assert_eq!(statistics_asked(&rec), 1, "{tag}: a count asks");
            }
        }
    }
}
