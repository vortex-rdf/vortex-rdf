//! The located cells: graph-bound reads, each of a store opened before
//! timing from a file written once per process under
//! `target/bench_vortex_files`, as the CLI writes one, and opened mapped
//! (`file`) or loaded in memory (`memory`). A cell is a run, where its store
//! lives, and one read: its id is `located[{run}_{file|memory}_{read}]`.
//!
//! **Checked runs** (`copy_*`, `ref_*`) are read as the Java binding reads
//! them, with `countCodes`, `matchCodes` and `matchCodeChunks` (core's
//! `count_code_pattern`, `match_code_pattern` and `match_code_chunks`), each
//! pattern binding the graph after a free slot. On a mapped file a located run
//! answers it: the run of the pattern's longest bound prefix in a sorted
//! table, its rows checked against the graph, a path no other bench reaches.
//! Loaded in memory, where no located run answers, the general match takes
//! the run of the bound predicate or object in an index component and checks
//! the graph inside the component, where the run's rows are contiguous: on
//! the component's own `g` a zone at a time, by its zone map, not at the base
//! rows the run lists. By copy, a window with a limit leaves the graph to the
//! residual compare over the run's ids.
//!
//! Two Dictionary files, one by copy and one by reference, hold the same
//! quads: a run of [`CHECKED_RUN_ROWS`] rows (8 zones of 8,192) for each of
//! two shapes, the quad table's run of subject `hub` and the `(type, Taxon)`
//! run, a row per subject. Along each run the graph cycles over four graphs
//! row by row in the first half; the second half is a stretch of the bound
//! graph, then a stretch of another. The runs:
//!
//! * `copy_pog`: `(? p o g)`, the `(type, Taxon)` run of `index:posg`;
//! * `copy_sg`: `(s ? ? g)`, the quad table's run of `hub`, located the same
//!   way with or without an index;
//! * `copy_pg`: `(? p ? g)`, the `type` run of `index:posg`, its graph left to
//!   check after the free slot;
//! * `ref_pg` and `ref_og`: `(? p ? g)` and `(? ? o g)`, the `type` run of
//!   `index:ref-p` and the `Taxon` run of `index:ref-o`, which list
//!   quad-table rows, the graph checked on the child's own `g` beside them.
//!
//! Their reads are an engine's, paging through a match: the count, the first
//! page, a page from the middle of the matches, and every match streamed a
//! page at a time.
//!
//! **Graph-bound general matches** (`scattered_*`, `whole_*`) are read
//! through `match_pattern`: no index serves them, so on a file the general
//! match is a filtered scan whose zones Vortex prunes with each zone's
//! statistics, and in memory the residual compare over the base. Two
//! Dictionary files without an index each hold 65,536 quads
//! `(s, type, Taxon, g)`, one per subject:
//!
//! * `scattered`: each 8,192-row zone holds 8 of 64 graphs, in turn row by
//!   row, their codes scattered over the zones, so a zone's minimum and
//!   maximum graph span almost the whole range: bounds alone prune almost
//!   nothing, where the graphs a zone records keep only the bound graph's;
//! * `whole`: zones 0 to 3 hold graph A alone, zones 4 and 5 graph B alone,
//!   zone 6 both in turn, and zone 7 graph C: half the file is A's, in whole
//!   zones where every row matches.
//!
//! Their reads: the count of `(? ? ? g)` (`_g`) or of `(? type ? g)` (`_pg`),
//! the rows of `(? ? ? g)` gathered (the bindings' `match_codes`), and a page
//! of its codes from the middle of its matches.

use std::fmt;
use std::hint::black_box;
use std::path::PathBuf;
use std::sync::OnceLock;

use futures::{StreamExt, stream};
use oxrdf::{GraphName, NamedNode, Quad};
use vortex_rdf_core::{CodePattern, LayoutStrategy, RawQuad, VortexRdfStore, io};

use crate::support::{Index, rt};
use crate::{DictResidency, gathered_rows};

/// Rows of each checked run: 8 zones of 8,192 rows.
const CHECKED_RUN_ROWS: usize = 65_536;

/// The graph (0 to 3) a checked-run pattern binds.
const CHECKED_BOUND_GRAPH: usize = 1;

/// The rows of a checked run its bound graph matches: a quarter of the
/// interleaved first half, and the stretch after it.
const CHECKED_RUN_MATCHES: usize = CHECKED_RUN_ROWS / 8 + CHECKED_RUN_ROWS / 4;

/// Rows of each zone of the zone files.
const ZONE_ROWS: usize = 8_192;

/// Rows of each zone file: 8 zones.
const ZONE_FILE_ROWS: usize = 8 * ZONE_ROWS;

/// Graphs each zone of the scattered file holds, in turn row by row.
const GRAPHS_PER_ZONE: usize = 8;

/// The graph slot the scattered runs bind: zone 3's fourth graph.
const BOUND_SLOT: usize = 27;

/// The rows the scattered file's bound graph holds: every 8th row of its
/// zone.
const BOUND_ROWS: usize = ZONE_ROWS / GRAPHS_PER_ZONE;

/// The rows graph A holds in the whole file: zones 0 to 3 whole, and half of
/// zone 6.
const WHOLE_A_ROWS: usize = 4 * ZONE_ROWS + ZONE_ROWS / 2;

/// Where the whole runs' middle page starts: inside zone 2.
const WHOLE_PAGE_OFFSET: usize = 18_000;

/// The first page qEndpoint reads of a match.
const FIRST_PAGE_ROWS: usize = 64;

/// A page qEndpoint reads of a match after the first, and the chunk a stream
/// of one yields.
const PAGE_ROWS: usize = 4_096;

/// The checked-run files' IRI named `name`.
fn checked_iri(name: &str) -> NamedNode {
    NamedNode::new_unchecked(format!("http://example.org/checked/{name}"))
}

/// The graph (0 to 3) of a checked run's row `row`: the four in turn, row by
/// row, through the first half; then the bound graph for a quarter of the run;
/// then the graph after it.
fn checked_run_graph(row: usize) -> usize {
    if row < CHECKED_RUN_ROWS / 2 {
        row % 4
    } else if row < CHECKED_RUN_ROWS / 4 * 3 {
        CHECKED_BOUND_GRAPH
    } else {
        CHECKED_BOUND_GRAPH + 1
    }
}

/// The checked-run files' quads: the run of subject `hub`, then the run of
/// `(type, Taxon)`, each of [`CHECKED_RUN_ROWS`] rows in its table's order,
/// its graphs laid out by [`checked_run_graph`].
fn checked_run_quads() -> Vec<RawQuad> {
    let graph = |row| GraphName::NamedNode(checked_iri(&format!("g{}", checked_run_graph(row))));
    let subject_run = (0..CHECKED_RUN_ROWS).map(|row| {
        let object = checked_iri(&format!("o{row:05}"));
        Quad::new(checked_iri("hub"), checked_iri("link"), object, graph(row))
    });
    let pair_run = (0..CHECKED_RUN_ROWS).map(|row| {
        let subject = checked_iri(&format!("s{row:05}"));
        Quad::new(
            subject,
            checked_iri("type"),
            checked_iri("Taxon"),
            graph(row),
        )
    });
    subject_run
        .chain(pair_run)
        .map(|quad| RawQuad::from_quad(&quad))
        .collect()
}

/// The zone files' IRI named `name`.
fn zones_iri(name: &str) -> NamedNode {
    NamedNode::new_unchecked(format!("http://example.org/zones/{name}"))
}

/// The scattered file's graph of slot `k` (zone `k / 8`'s graph `k % 8`),
/// labelled `(k * 37) % 64`: a permutation of the 64 labels that scatters
/// their codes over the zones.
fn slot_graph(k: usize) -> NamedNode {
    zones_iri(&format!("g{:02}", (k * 37) % 64))
}

/// The scattered file's graph of row `i`: its zone's 8 graphs in turn.
fn scattered_graph(i: usize) -> NamedNode {
    slot_graph(i / ZONE_ROWS * GRAPHS_PER_ZONE + i % GRAPHS_PER_ZONE)
}

/// The whole file's graph labelled `label`.
fn whole_graph_named(label: &str) -> NamedNode {
    zones_iri(&format!("whole/{label}"))
}

/// The whole file's graph of row `i`: A in zones 0 to 3, B in zones 4 and 5,
/// A and B in turn in zone 6, C in zone 7.
fn whole_graph(i: usize) -> NamedNode {
    whole_graph_named(match i / ZONE_ROWS {
        0..=3 => "A",
        4 | 5 => "B",
        6 if i.is_multiple_of(2) => "A",
        6 => "B",
        _ => "C",
    })
}

/// A zone file's quads: [`ZONE_FILE_ROWS`] quads `(s, type, Taxon, g)`, one
/// per subject `{subject}{i:05}`, row `i` in `graph(i)`.
fn zone_quads(subject: &str, graph: impl Fn(usize) -> NamedNode) -> Vec<RawQuad> {
    (0..ZONE_FILE_ROWS)
        .map(|i| {
            let quad = Quad::new(
                zones_iri(&format!("{subject}{i:05}")),
                zones_iri("type"),
                zones_iri("Taxon"),
                GraphName::NamedNode(graph(i)),
            );
            RawQuad::from_quad(&quad)
        })
        .collect()
}

/// A file the located cells read.
#[derive(Copy, Clone)]
enum File {
    /// The checked runs, with the copy index.
    CheckedByCopy,
    /// The checked runs, with the reference index.
    CheckedByReference,
    /// The scattered graphs, without an index.
    Scattered,
    /// The whole zones, without an index.
    Whole,
}

impl File {
    /// The file, written once per process under `target/bench_vortex_files`
    /// by the writer the CLI and the bindings use, Dictionary layout.
    fn path(self) -> PathBuf {
        static PATHS: [OnceLock<PathBuf>; 4] = [const { OnceLock::new() }; 4];
        PATHS[self as usize]
            .get_or_init(|| {
                let (name, index, quads) = match self {
                    Self::CheckedByCopy => ("checked_runs", Index::ByCopy, checked_run_quads()),
                    Self::CheckedByReference => {
                        ("checked_ref_runs", Index::ByReference, checked_run_quads())
                    }
                    Self::Scattered => {
                        ("graph_zones", Index::None, zone_quads("s", scattered_graph))
                    }
                    Self::Whole => (
                        "whole_counts",
                        Index::None,
                        zone_quads("whole/s", whole_graph),
                    ),
                };
                let dir = PathBuf::from("target/bench_vortex_files");
                std::fs::create_dir_all(&dir).unwrap();
                let path = dir.join(format!("{name}.vortex"));
                let quads = stream::iter(quads.into_iter().map(Ok));
                rt().block_on(async {
                    io::quads_stream_to_vortex_file(
                        quads,
                        &path,
                        LayoutStrategy::Dictionary,
                        index.types(),
                    )
                    .await
                    .unwrap_or_else(|e| panic!("write {name}: {e}"));
                });
                path
            })
            .clone()
    }
}

/// A run of matches the located cells read: its file and its pattern.
#[derive(Copy, Clone, PartialEq, Eq)]
pub(crate) enum Run {
    /// `(? p o g)`: the `(type, Taxon)` run of `index:posg`.
    CopyPog,
    /// `(s ? ? g)`: the quad table's run of subject `hub`.
    CopySg,
    /// `(? p ? g)`, by copy: the `type` run of `index:posg`, its graph left to
    /// check.
    CopyPg,
    /// `(? p ? g)`, by reference: the `type` run of `index:ref-p`.
    RefPg,
    /// `(? ? o g)`, by reference: the `Taxon` run of `index:ref-o`.
    RefOg,
    /// `(? ? ? g)` on the scattered graphs.
    ScatteredG,
    /// `(? type ? g)` on the scattered graphs.
    ScatteredPg,
    /// `(? ? ? A)` on the whole zones.
    WholeG,
    /// `(? type ? A)` on the whole zones.
    WholePg,
}

impl Run {
    fn name(self) -> &'static str {
        match self {
            Self::CopyPog => "copy_pog",
            Self::CopySg => "copy_sg",
            Self::CopyPg => "copy_pg",
            Self::RefPg => "ref_pg",
            Self::RefOg => "ref_og",
            Self::ScatteredG => "scattered_g",
            Self::ScatteredPg => "scattered_pg",
            Self::WholeG => "whole_g",
            Self::WholePg => "whole_pg",
        }
    }

    fn file(self) -> File {
        match self {
            Self::CopyPog | Self::CopySg | Self::CopyPg => File::CheckedByCopy,
            Self::RefPg | Self::RefOg => File::CheckedByReference,
            Self::ScatteredG | Self::ScatteredPg => File::Scattered,
            Self::WholeG | Self::WholePg => File::Whole,
        }
    }

    /// Whether a located run answers this run on its mapped file.
    fn located(self) -> bool {
        matches!(
            self,
            Self::CopyPog | Self::CopySg | Self::CopyPg | Self::RefPg | Self::RefOg
        )
    }

    /// The run's pattern as terms, `(s, p, o, g)`.
    fn terms(self) -> [Option<NamedNode>; 4] {
        let checked_graph = || Some(checked_iri(&format!("g{CHECKED_BOUND_GRAPH}")));
        match self {
            Self::CopyPog => [
                None,
                Some(checked_iri("type")),
                Some(checked_iri("Taxon")),
                checked_graph(),
            ],
            Self::CopySg => [Some(checked_iri("hub")), None, None, checked_graph()],
            Self::CopyPg | Self::RefPg => [None, Some(checked_iri("type")), None, checked_graph()],
            Self::RefOg => [None, None, Some(checked_iri("Taxon")), checked_graph()],
            Self::ScatteredG => [None, None, None, Some(slot_graph(BOUND_SLOT))],
            Self::ScatteredPg => [
                None,
                Some(zones_iri("type")),
                None,
                Some(slot_graph(BOUND_SLOT)),
            ],
            Self::WholeG => [None, None, None, Some(whole_graph_named("A"))],
            Self::WholePg => [
                None,
                Some(zones_iri("type")),
                None,
                Some(whole_graph_named("A")),
            ],
        }
    }

    /// The rows the run matches.
    fn matches(self) -> usize {
        match self {
            Self::CopyPog | Self::CopySg | Self::CopyPg | Self::RefPg | Self::RefOg => {
                CHECKED_RUN_MATCHES
            }
            Self::ScatteredG | Self::ScatteredPg => BOUND_ROWS,
            Self::WholeG | Self::WholePg => WHOLE_A_ROWS,
        }
    }

    /// Where a page from the middle of the run's matches starts.
    fn middle(self) -> usize {
        match self {
            Self::WholeG | Self::WholePg => WHOLE_PAGE_OFFSET,
            _ => self.matches() / 2,
        }
    }
}

/// One read of a run.
#[derive(Copy, Clone, PartialEq, Eq)]
pub(crate) enum Read {
    /// `count_code_pattern` (`countCodes`).
    Count,
    /// `match_code_pattern` of the first [`FIRST_PAGE_ROWS`] matches
    /// (`matchCodes`).
    FirstPage,
    /// `match_code_pattern` of [`PAGE_ROWS`] matches from the middle of the
    /// matches ([`Run::middle`]).
    MiddlePage,
    /// Every chunk of `match_code_chunks`, [`PAGE_ROWS`] rows a chunk
    /// (`matchCodeChunks`).
    Stream,
    /// `match_pattern`'s `size`.
    MatchCount,
    /// `match_pattern`'s rows, gathered (`code_columns_gathered`).
    MatchRows,
}

impl Read {
    fn name(self) -> &'static str {
        match self {
            Self::Count => "count",
            Self::FirstPage => "first_page",
            Self::MiddlePage => "middle_page",
            Self::Stream => "stream",
            Self::MatchCount => "match_count",
            Self::MatchRows => "match_rows",
        }
    }
}

/// One located cell: `read` of `run`, its file opened as `residency`.
#[derive(Copy, Clone)]
pub(crate) struct Cell {
    run: Run,
    residency: DictResidency,
    read: Read,
}

impl fmt::Display for Cell {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let residency = match self.residency {
            DictResidency::FileBacked => "file",
            DictResidency::Resident => "memory",
        };
        write!(f, "{}_{residency}_{}", self.run.name(), self.read.name())
    }
}

impl fmt::Debug for Cell {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

/// The located cells: each checked run's four code reads, mapped by copy
/// and by reference and loaded in memory, and the general matches' counts,
/// rows and middle page.
pub(crate) fn cells() -> Vec<Cell> {
    use DictResidency::{FileBacked, Resident};
    const CODE_READS: [Read; 4] = [Read::Count, Read::FirstPage, Read::MiddlePage, Read::Stream];
    let mut cells = Vec::new();
    let mut add = |run, residency, reads: &[Read]| {
        cells.extend(reads.iter().map(|&read| Cell {
            run,
            residency,
            read,
        }));
    };
    for run in [Run::CopyPog, Run::CopySg, Run::RefPg, Run::RefOg] {
        add(run, FileBacked, &CODE_READS);
    }
    for run in [Run::CopyPog, Run::CopyPg, Run::RefPg, Run::RefOg] {
        add(run, Resident, &CODE_READS);
    }
    add(
        Run::ScatteredG,
        FileBacked,
        &[Read::MatchCount, Read::MatchRows],
    );
    add(Run::ScatteredPg, FileBacked, &[Read::MatchCount]);
    for residency in [FileBacked, Resident] {
        add(
            Run::WholeG,
            residency,
            &[Read::MatchCount, Read::MiddlePage],
        );
        add(Run::WholePg, residency, &[Read::MatchCount]);
    }
    cells
}

/// What a cell's read is given: its pattern in the store's codes, or its
/// predicate and graph as terms.
enum Query {
    Codes(CodePattern),
    Terms(Option<NamedNode>, GraphName),
}

impl Cell {
    /// The query of this cell's read on `store`.
    async fn query(self, store: &VortexRdfStore) -> Query {
        let [subject, predicate, object, graph] = self.run.terms();
        match self.read {
            Read::MatchCount | Read::MatchRows => {
                assert!(
                    subject.is_none() && object.is_none(),
                    "{self}: a (? p ? g) match"
                );
                Query::Terms(
                    predicate,
                    GraphName::NamedNode(graph.expect("a bound graph")),
                )
            }
            _ => {
                let dict = store.dict_reader().expect("a Dictionary store");
                let mut codes = [None; 4];
                for (code, term) in codes.iter_mut().zip([subject, predicate, object, graph]) {
                    if let Some(iri) = term {
                        *code = Some(
                            dict.encode(&format!("<{}>", iri.as_str()))
                                .await
                                .expect("encode")
                                .unwrap_or_else(|| panic!("{self}: {iri} is not in the file")),
                        );
                    }
                }
                Query::Codes(CodePattern::new(codes[0], codes[1], codes[2], codes[3]))
            }
        }
    }

    /// This cell's read of `query` on `store`, as the rows it returns (a
    /// count, its value). A code read and a term read each run as their own
    /// future, so neither times the other's state.
    fn rows(self, store: &VortexRdfStore, query: &Query) -> usize {
        match query {
            &Query::Codes(pattern) => rt().block_on(self.code_rows(store, pattern)),
            Query::Terms(predicate, graph) => {
                rt().block_on(self.match_rows(store, predicate.as_ref(), graph))
            }
        }
    }

    /// This cell's code read of `pattern`.
    async fn code_rows(self, store: &VortexRdfStore, pattern: CodePattern) -> usize {
        match self.read {
            Read::Count => {
                let count = store.count_code_pattern(pattern).await.expect("count");
                usize::try_from(count).expect("a count fits usize")
            }
            Read::FirstPage => {
                let columns = store.match_code_pattern(pattern, 0, Some(FIRST_PAGE_ROWS));
                columns.await.expect("window")[0].len()
            }
            Read::MiddlePage => {
                let offset = self.run.middle();
                let columns = store.match_code_pattern(pattern, offset, Some(PAGE_ROWS));
                columns.await.expect("window")[0].len()
            }
            Read::Stream => {
                let chunks = store.match_code_chunks(pattern, PAGE_ROWS).await;
                let mut chunks = chunks.expect("stream");
                let mut rows = 0;
                while let Some(chunk) = chunks.next().await {
                    rows += chunk.expect("chunk")[0].len();
                }
                rows
            }
            Read::MatchCount | Read::MatchRows => unreachable!("{self}: a term read"),
        }
    }

    /// This cell's term read of `(? predicate ? graph)`.
    async fn match_rows(
        self,
        store: &VortexRdfStore,
        predicate: Option<&NamedNode>,
        graph: &GraphName,
    ) -> usize {
        let view = store
            .match_pattern(None, predicate, None, Some(graph))
            .await
            .expect("match");
        match self.read {
            Read::MatchRows => gathered_rows(&view).await,
            _ => view.size().await.expect("size"),
        }
    }

    /// The rows this cell's read returns.
    fn expected_rows(self) -> usize {
        match self.read {
            Read::Count | Read::Stream | Read::MatchCount | Read::MatchRows => self.run.matches(),
            Read::FirstPage => FIRST_PAGE_ROWS,
            Read::MiddlePage => PAGE_ROWS,
        }
    }
}

/// Times `cell`'s read of a store opened before timing, once its query is
/// resolved. The setup panics, naming the cell, unless the read returns the
/// rows it should and, on a mapped file, a located run answers a checked
/// run's pattern.
pub(crate) fn time(bencher: divan::Bencher, cell: Cell) {
    let store = rt().block_on(cell.residency.open(&cell.run.file().path()));
    let query = rt().block_on(cell.query(&store));
    if let (DictResidency::FileBacked, true, Query::Codes(pattern)) =
        (cell.residency, cell.run.located(), &query)
    {
        let located = rt().block_on(store.locate_codes(*pattern, 0, Some(0)));
        assert!(
            located.expect("locate").is_some(),
            "{cell}: no located run answers the pattern"
        );
    }
    let rows = cell.rows(&store, &query);
    assert_eq!(rows, cell.expected_rows(), "{cell}: wrong row count");
    bencher.bench(|| black_box(cell.rows(&store, &query)));
}
