//! The zone tests, and the one fixture layer they share. Zone maps and
//! their statistics, written into store files and kept in memory, and the
//! reads they decide: located runs (`located`), the in-memory residual
//! (`residual`, `pending_residual`, `bounds`), index runs checked inside
//! their component (`component_runs`, `component_zones`), reference
//! indexes' graph column (`reference_graphs`), file counts (`file_counts`)
//! and the recorded graphs (`value_counts`).
//!
//! The layer: the zone-quads stores ([`Graphs`], [`zone_quads`],
//! [`zone_stores`]), the codes and rows their reads answer ([`code_of`],
//! [`streamed`], [`term_rows`], [`probe_answers`]), the stores a residual
//! reads under tombstones and with a tail ([`tombstoned`], [`tailed`]), and
//! what a recorder saw of their zones ([`zone_work`], [`zone_recorded`],
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
    chunks
        .iter()
        .flat_map(|chunk| code_rows::<4>(chunk))
        .collect()
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
