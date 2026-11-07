//! The zone tests, and the one fixture layer they share. Zone maps and
//! their statistics, written into store files and kept in memory, and the
//! reads they decide: located runs (`located`), the in-memory residual
//! (`residual`, `pending_residual`, `bounds`), index runs checked inside
//! their component (`component_runs`, `component_zones`), reference
//! indexes' graph column (`reference_graphs`), file counts (`file_counts`)
//! and the recorded graphs (`value_counts`).
//!
//! The layer: the zone-quads stores ([`Graphs`], [`zone_quads`],
//! [`zone_stores`]) and their files, each [`ZoneFile`] written once per
//! process ([`zone_file`]); the codes and rows their reads answer
//! ([`code_of`], [`streamed`], [`term_rows`], [`code_answers`],
//! [`probe_answers`]); the stores a residual reads under tombstones and with
//! a tail ([`tombstoned`], [`tailed`]); the harness that reads zones off,
//! then on, and holds the answers equal ([`zones_agree`]); and what a
//! recorder saw of their zones ([`zone_work`], [`zone_recorded`],
//! [`zone_maps_read`], [`statistics_asked`]).

use super::*;
use crate::zone_stats::zone_map::ZONE_ROWS;

mod bounds;
mod component_runs;
mod component_zones;
mod file_counts;
mod located;
mod pending_residual;
mod reference_graphs;
mod residual;
mod value_counts;

/// Rows of each run of a [`zone_quads`] store.
const RUN: usize = 40_000;

/// The rows of [`Graphs::UnrecordedGap`]'s seventeen graphs: zone 6 of
/// `index:ref-p`, whose `type` run is its rows `RUN..2 * RUN`.
const UNRECORDED_GAP: std::ops::Range<usize> = 6 * ZONE_ROWS - RUN..7 * ZONE_ROWS - RUN;

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

/// A zone-quads store file: [`zone_quads`] of `graphs`, written under
/// `layout` with `index`'s indexes, in `leaves`-row flat leaves where set,
/// and recording the graph columns' `vortexrdf.value_counts` and the
/// reference children's graphs unless the key turns them off. The
/// write-time hooks are in the key, so each key is one file's bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ZoneFile {
    graphs: Graphs,
    index: IndexConfig,
    layout: LayoutStrategy,
    leaves: Option<usize>,
    value_counts: bool,
    reference_graphs: bool,
}

impl ZoneFile {
    /// `graphs`' quads with `index`'s indexes, as the Dictionary layout
    /// writes them in its own leaves, recording both.
    fn new(graphs: Graphs, index: IndexConfig) -> Self {
        Self {
            graphs,
            index,
            layout: LayoutStrategy::Dictionary,
            leaves: None,
            value_counts: true,
            reference_graphs: true,
        }
    }

    /// Written under `layout`.
    fn layout(self, layout: LayoutStrategy) -> Self {
        Self { layout, ..self }
    }

    /// Written in `rows`-row flat leaves (the Dictionary layout, with no
    /// index or the reference index).
    fn leaves(self, rows: usize) -> Self {
        Self {
            leaves: Some(rows),
            ..self
        }
    }

    /// Recording the graph columns' `vortexrdf.value_counts` (`true`), or
    /// written as before them (`false`).
    fn value_counts(self, on: bool) -> Self {
        Self {
            value_counts: on,
            ..self
        }
    }

    /// Giving the reference children each row's graph (`true`), or written
    /// as before that column (`false`).
    fn reference_graphs(self, on: bool) -> Self {
        Self {
            reference_graphs: on,
            ..self
        }
    }

    /// The file's bytes, written under the key's hooks by the writer of its
    /// shape.
    async fn write(self) -> Vec<u8> {
        use crate::store::test_hooks::{ForcedReferenceGraphs, ForcedValueCounts};

        let _value_counts = ForcedValueCounts::set(self.value_counts);
        let _reference_graphs = ForcedReferenceGraphs::set(self.reference_graphs);
        let quads = zone_quads(self.graphs);
        let (_dir, path) = match (self.leaves, self.layout, self.index) {
            (None, layout, index) => write_store_file(quads, layout, index.types()).await,
            (Some(rows), LayoutStrategy::Dictionary, IndexConfig::NoIndex) => {
                write_chunked_store_file(&quads, rows).await
            }
            (Some(rows), LayoutStrategy::Dictionary, IndexConfig::Reference) => {
                write_chunked_reference_store_file(&quads, rows).await
            }
            (Some(_), layout, index) => panic!("no writer of {layout:?} leaves with {index:?}"),
        };
        std::fs::read(path).unwrap()
    }
}

/// A [`ZoneFile`]'s bytes, written on the key's first ask, and the times
/// they were written.
#[derive(Default)]
struct ZoneFileCell {
    bytes: tokio::sync::OnceCell<Vec<u8>>,
    builds: std::sync::atomic::AtomicUsize,
}

/// The cell of `key`, made on its first ask and kept for the process.
fn zone_file_cell(key: ZoneFile) -> &'static ZoneFileCell {
    static CELLS: std::sync::Mutex<Vec<(ZoneFile, &'static ZoneFileCell)>> =
        std::sync::Mutex::new(Vec::new());
    let mut cells = CELLS.lock().unwrap();
    if let Some(&(_, cell)) = cells.iter().find(|(k, _)| *k == key) {
        return cell;
    }
    let cell: &'static ZoneFileCell = Box::leak(Box::default());
    cells.push((key, cell));
    cell
}

/// `key`'s store file in a fresh temp dir. Each key is written once per
/// process, its first askers waiting on that one write, and its bytes are
/// copied into each file asked for, so every caller opens a file of its own.
/// Keep the `TempDir` alive for the file's lifetime.
async fn zone_file(key: ZoneFile) -> (tempfile::TempDir, std::path::PathBuf) {
    use std::sync::atomic::Ordering;

    let cell = zone_file_cell(key);
    let bytes = cell
        .bytes
        .get_or_init(|| async {
            cell.builds.fetch_add(1, Ordering::Relaxed);
            key.write().await
        })
        .await;
    temp_store_file(bytes)
}

/// The times `key`'s file was written in this process.
fn zone_file_builds(key: ZoneFile) -> usize {
    zone_file_cell(key)
        .builds
        .load(std::sync::atomic::Ordering::Relaxed)
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
            let (dir, path) = zone_file(ZoneFile::new(graphs, index)).await;
            stores.push(ZoneStore {
                tag: format!("{graphs:?}/{}", index.name()),
                graphs,
                index,
                store: VortexRdfStore::from_file(&path).await.unwrap(),
                _dir: dir,
            });
        }
    }
    let (dir, path) =
        zone_file(ZoneFile::new(Graphs::Stretches, IndexConfig::NoIndex).leaves(7)).await;
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

/// The rows [`match_code_chunks`](VortexRdfStore::match_code_chunks) streams
/// for `pattern`, in the stream's order, each chunk its four code columns of
/// `batch` rows or fewer, none empty.
pub(super) async fn streamed(
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
    let mut rows = Vec::new();
    for chunk in &chunks {
        assert_eq!(chunk.len(), 4, "the four code columns");
        assert!(
            (1..=batch).contains(&chunk[0].len()),
            "a chunk of {} rows",
            chunk[0].len()
        );
        rows.extend(code_rows::<4>(chunk));
    }
    rows
}

/// The term path's rows of `pattern`: its codes spelled out and matched.
pub(super) async fn term_rows(
    store: &VortexRdfStore,
    pattern: crate::CodePattern,
) -> Vec<[TermCode; 4]> {
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
            *spelling = Some(
                dict.decode(code)
                    .await
                    .unwrap()
                    .expect("a code the dictionary holds"),
            );
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

/// Rows `rec` saw counted on a mixed zone's encoding, and read to check.
fn zone_work(rec: &Recorder) -> (u64, u64) {
    let counted = rec.rows(|e| match e {
        Event::ZoneCounted(rows) => Some(*rows),
        _ => None,
    });
    let read = rec.rows(|e| match e {
        Event::CheckRead(rows) => Some(*rows),
        _ => None,
    });
    (counted, read)
}

/// Rows of each mixed zone `rec` saw counted from its recorded values, in
/// order.
fn zone_recorded(rec: &Recorder) -> Vec<u64> {
    rec.picked(|e| match e {
        Event::ZoneRecorded(rows) => Some(*rows),
        _ => None,
    })
}

/// Zone maps `rec` saw read from a file.
fn zone_maps_read(rec: &Recorder) -> usize {
    rec.count(&Event::ZoneMapRead)
}

/// Statistics `rec` saw a located run ask of its file.
fn statistics_asked(rec: &Recorder) -> usize {
    rec.count(&Event::StatisticsAsked)
}

/// What `read` answers with zones on, with the recorder of that read, once
/// it answered the same with zones off: a read that zones decide answers as
/// the rows do. Each read runs under its own [`ForcedZones`] guard, and the
/// recorder starts just before the zones-on read, so until the caller reads
/// again it holds that read's events alone. A difference fails as `what`'s,
/// without the answers, which may hold tens of thousands of rows.
///
/// [`ForcedZones`]: crate::store::test_hooks::ForcedZones
async fn zones_agree<T, F, Fut>(what: &str, read: F) -> (T, Recorder)
where
    T: PartialEq,
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = T>,
{
    use crate::store::test_hooks::ForcedZones;

    let off = {
        let _zones = ForcedZones::set(false);
        read().await
    };
    let _zones = ForcedZones::set(true);
    let rec = Recorder::start();
    let on = read().await;
    assert!(on == off, "{what}: zones on and off answer apart");
    (on, rec)
}

/// Every [`Graphs`] layout.
const ZONE_GRAPHS: [Graphs; 8] = [
    Graphs::One,
    Graphs::Long,
    Graphs::Stretches,
    Graphs::Alternating,
    Graphs::Gapped,
    Graphs::Seventeen,
    Graphs::DefaultAndThree,
    Graphs::RecordedByTurns,
];

/// What a located pattern answers: its count, its pages again through
/// `locate_codes` with the run's count beside each, its windows, and its
/// streams, every row in order.
#[derive(Debug, PartialEq, Eq)]
struct CodeAnswers {
    count: u64,
    located: Vec<(u64, Vec<[TermCode; 4]>)>,
    windows: Vec<Vec<[TermCode; 4]>>,
    streams: Vec<Vec<[TermCode; 4]>>,
}

/// What `pattern` answers on `store`: its count, its windows (pages of 1,
/// 3 and 64 at the run's ends and middle, and 32 one-row reads spread over
/// it), its streams (chunks of 999 and 4,096 rows), and the pages again
/// through `locate_codes`, each with the run's count beside it, which must
/// be the count.
async fn code_answers(store: &VortexRdfStore, pattern: crate::CodePattern) -> CodeAnswers {
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
    CodeAnswers {
        count: total,
        located,
        windows,
        streams,
    }
}

/// What a read of `probe` answers on `store`: its count, a count capped
/// from its middle, its windows from its first, middle and last row, one
/// row and 64 rows long, and every row in order (base rows, then tail
/// rows).
#[derive(Debug, PartialEq, Eq)]
struct ProbeAnswers {
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

async fn probe_answers(store: &VortexRdfStore, probe: &Probe) -> ProbeAnswers {
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
    // An offset with no limit: every match past it.
    let rest = Probe {
        offset: count / 2,
        limit: None,
        ..probe.clone()
    };
    windows.push(quads_of(&store.run_probe(&rest).await.unwrap()).await);
    let rows = quads_of(&store.run_probe(probe).await.unwrap()).await;
    ProbeAnswers {
        count,
        capped,
        windows,
        rows,
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

/// Whether `column` is zoned with the statistic among its aggregates.
fn records_value_counts(column: &vortex_layout::LayoutRef) -> bool {
    use crate::zone_stats::value_counts::value_counts;
    use vortex_layout::layouts::zoned::Zoned;
    column.as_opt::<Zoned>().is_some_and(|zoned| {
        zoned
            .present_aggregates()
            .contains(&value_counts().to_string())
    })
}

/// A zone file is written once per key, however often it is asked for: a
/// second ask copies the first write's bytes, a key that differs only in a
/// write-time hook is written apart (and records what its hook says), and a
/// copied file answers as a fresh write of the same quads does.
#[tokio::test]
async fn zone_files_are_written_once_per_key() {
    use crate::CodePattern;

    let key = ZoneFile::new(Graphs::Long, IndexConfig::Copy);
    let (_first_dir, first) = zone_file(key).await;
    let (_second_dir, second) = zone_file(key).await;
    assert_eq!(zone_file_builds(key), 1, "two asks, one write");
    assert_eq!(
        std::fs::read(&first).unwrap(),
        std::fs::read(&second).unwrap()
    );

    let unrecorded = key.value_counts(false);
    let (_unrecorded_dir, third) = zone_file(unrecorded).await;
    assert_eq!(zone_file_builds(unrecorded), 1, "its own write");
    assert_eq!(zone_file_builds(key), 1, "the first key's write stands");
    for (path, recorded) in [(&second, true), (&third, false)] {
        let native = open_mapped(path).await;
        let g = native.table_column_layout(Some("index:posg"), "g").unwrap();
        assert_eq!(records_value_counts(&g), recorded, "{path:?}");
    }

    let (_fresh_dir, fresh) = write_store_file(
        zone_quads(Graphs::Long),
        LayoutStrategy::Dictionary,
        IndexConfig::Copy.types(),
    )
    .await;
    let mut counts = Vec::new();
    for path in [&second, &fresh] {
        let store = VortexRdfStore::from_file(path).await.unwrap();
        let ty = code_of(&store, "<http://example.org/type>").await;
        let taxon = code_of(&store, "\"Taxon\"").await;
        let g1 = code_of(&store, "<http://example.org/g1>").await;
        let pattern = CodePattern::new(None, Some(ty), Some(taxon), Some(g1));
        counts.push(store.count_code_pattern(pattern).await.unwrap());
    }
    assert_eq!(counts[0], counts[1], "the cached file and a fresh write");
    assert_eq!(counts[0], RUN as u64 / 2, "graph 1 holds half the run");
}

/// A read whose answer turns on the zones switch fails the harness.
#[tokio::test]
#[should_panic(expected = "zones on and off")]
async fn zones_agree_catches_a_read_that_differs() {
    zones_agree("the switch", || async {
        crate::zone_stats::rules::zones_enabled()
    })
    .await;
}
