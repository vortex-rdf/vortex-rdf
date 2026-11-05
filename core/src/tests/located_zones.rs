//! Located runs decided zone by zone: the zone maps of a store file's
//! columns read as Vortex `ZoneMap`s.

use super::*;
use crate::io::zone_map::ColumnZones;

/// A store file's `g` column read as a zone map: 20,000 rows of `g1` then
/// `g2` make three zones, and Vortex's falsifier of `g = g1` holds on the
/// last one only.
#[tokio::test]
async fn column_zones_read_from_a_written_column() {
    use vortex_array::dtype::{DType, Nullability, PType};
    use vortex_array::expr::{eq, lit, root};

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
    let none = eq(root(), lit(g1))
        .bind(&DType::Primitive(PType::U64, Nullability::NonNullable))
        .unwrap()
        .falsify(&crate::session::VORTEX_SESSION)
        .unwrap()
        .unwrap();
    let mask = zones.prune(0..3, &none).unwrap();
    assert_eq!(
        (0..3).map(|i| mask.value(i)).collect::<Vec<_>>(),
        [false, false, true]
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

/// Rows of each run of a [`zone_quads`] store.
const RUN: usize = 40_000;

/// How a run's rows fall into graphs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Graphs {
    /// One graph only.
    One,
    /// Two graphs, each a half of the run.
    Long,
    /// Four graphs in turn, 1,000 rows each time.
    Stretches,
    /// Four graphs in turn, row by row.
    Alternating,
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
        }
    }

    /// The last graph number a run holds.
    fn last(self) -> usize {
        match self {
            Graphs::One => 1,
            Graphs::Long => 2,
            Graphs::Stretches | Graphs::Alternating => 4,
        }
    }
}

/// [`RUN`] quads `(s{i}, type, "Taxon", g(i))`, an `index:posg` run, and
/// [`RUN`] quads `(hub, link, "o{i}", g(i))`, a quad-table run of `hub`.
fn zone_quads(graphs: Graphs) -> Vec<Quad> {
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

/// Each [`Graphs`] layout written without indexes and with the copy index,
/// and the 1,000-row stretches in seven-row leaves, which have no zone maps.
async fn zone_stores() -> Vec<ZoneStore> {
    let mut stores = Vec::new();
    for graphs in Graphs::ALL {
        for index in [IndexConfig::NoIndex, IndexConfig::Copy] {
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
async fn code_of(store: &VortexRdfStore, term: &str) -> TermCode {
    store
        .dict_reader()
        .unwrap()
        .encode(term)
        .await
        .unwrap()
        .unwrap_or_else(|| panic!("{term} is in the dictionary"))
}

/// The patterns checked on `store` for graph `g`: the `index:posg` run, the
/// quad table's `(hub, link)` run, and `hub`'s run checked at two slots.
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

/// What `pattern` answers on `store`: its count, its windows (pages of 1,
/// 3 and 64 at the run's ends and middle, and 32 one-row reads spread over
/// it) and its streams (chunks of 999 and 4,096 rows).
async fn answers(
    store: &VortexRdfStore,
    pattern: crate::CodePattern,
) -> (u64, Vec<Vec<[TermCode; 4]>>, Vec<Vec<[TermCode; 4]>>) {
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
    (total, windows, streams)
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

/// With zones, a count reads no checked rows: a zone of the graph alone
/// counts its size, a zone without it nothing, and a mixed one is counted on
/// its encoding; a stream reads the mixed zones only. A store without zone
/// maps reads its run as before.
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
                let rec = Recorder::start();
                assert_eq!(store.count_code_pattern(taxa).await.unwrap(), 10_000);
                assert_eq!(zone_work(&rec), (RUN as u64, 0), "{}", fixture.tag);
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

/// Zone maps `rec` saw read from a file.
fn zone_maps_read(rec: &Recorder) -> usize {
    rec.count(&Event::ZoneMapRead)
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
        let located = match fixture.index {
            IndexConfig::Copy => patterns[0],
            _ => patterns[1],
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
    use vortex_rdf_encoded_search::counters::{ZONE_ALL, ZONE_COUNTED, ZONE_NONE, on, snapshot};

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
