//! Cross-library comparative benchmark for the Rust tab.
//!
//! The Rust counterpart to `js/bench/compare.bench.ts` and `python/bench/run.py`:
//! wall-clock, comparative, **never uploaded to CodSpeed** (`benchmark.rs` is the
//! instrumented library-only suite). It answers the question those two tabs answer
//! for their ecosystems — how does this store compare with what else you would
//! reach for — for the ecosystem the store is actually written in.
//!
//! It reads the *same N-Triples file* the Python suite builds, so a row here and a
//! row on the Python tab measure the same bytes rather than merely the same
//! generator. The file is written once, by whichever suite runs first.
//!
//! # Isolation
//!
//! One adapter per process, re-executing this binary with `--adapter <slug>`. That
//! is not ceremony: in a shared process a cheap pattern is measured in the shadow
//! of another library's scan, and peak RSS is only attributable to a library that
//! had a process to itself. The parent merges the children's JSON.
//!
//! # Libraries
//!
//! oxigraph enters twice — its in-memory store and its RocksDB-backed one, the
//! same library across the residency axis the Vortex (file, memory) pairs cross.
//! sophia holds its data in memory; hdt is a read-only compressed file format,
//! the other entrant with an artifact to open. hdt has no named graphs, so it
//! declines quad patterns rather than answering a different question.

use std::fmt::Write as _;
use std::io::{BufWriter, Write as _};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::Instant;

use sophia::api::MownStr;
use sophia::api::term::{IriRef, SimpleTerm};
use vortex_rdf_core::common::terms::parse_quads_from_reader;
use vortex_rdf_core::{IndexType, LayoutStrategy, VortexRdfStore};

#[path = "support/runtime.rs"]
mod runtime;
use runtime::rt;

// ─── Scale ──────────────────────────────────────────────────────────────────

/// Rows in the comparative dataset — 2^20 by default, the dashboard's
/// indicative-overview scale. The JavaScript and Python suites read the same
/// `BENCH_SIZE` variable, so the three comparative tables share it.
fn bench_size() -> usize {
    std::env::var("BENCH_SIZE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_048_576)
}

/// Repetitions, matching `QUERY_SAMPLES`/`HEAVY_SAMPLES` in `support/mod.rs` and
/// their twins in the other two harnesses.
const QUERY_ITERS: usize = 10;
const HEAVY_ITERS: usize = 3;
const QUERY_WARMUP: usize = 5;

// ─── Dataset ────────────────────────────────────────────────────────────────
//
// The shape lives in `support/dataset.rs`, included directly rather than
// re-derived: the instrumented suite (`benchmark.rs`) generates the very same
// rows in process, and a second port here is how the two would drift apart into
// tables that cannot be read beside each other. Only the *delivery* differs —
// this suite writes an N-Triples file, because every external library ingests
// from one.
//
// Items the file exposes for the in-process generators (oxrdf probe terms, the
// pattern enum) have no use here.
#[path = "support/dataset.rs"]
#[allow(dead_code)]
mod dataset;

use dataset::{dataset_prefix, fresh_quads, graph_iri, predicate_iri, subject_iri};

/// Moduli for the comparative dataset. `WANT_GRAPHS` is nudged to a count
/// coprime with the other roles, so the graph column does not track them; the
/// triples projection uses these same moduli, which is what makes row `i` the
/// same row in both files.
fn moduli(n: usize) -> dataset::Moduli {
    dataset::moduli(n, dataset::WANT_GRAPHS)
}

/// Objects in their N-Triples spelling: angle-bracketed IRI or quoted literal.
fn object_ntriples(i: usize) -> String {
    dataset::object_term(i).to_ntriples()
}

/// The comparative dataset: `n` quads over `WANT_GRAPHS` named graphs (the
/// coprimality nudge lands it at 17 at the dashboard's size), written once and
/// shared by all three comparative suites.
///
/// One dataset, not two: every library that can hold a graph builds from this
/// file and answers every pattern on it, so a row count means the same thing in
/// every cell of every tab.
fn ensure_quads_dataset(n: usize) -> PathBuf {
    write_dataset(&data_dir().join(format!("quads_{n}.nq")), n, true)
}

/// The triples projection of [`ensure_quads_dataset`]: the same rows under the
/// same moduli, with the graph dropped.
///
/// What a library that cannot hold graphs reads. Row `i` is row `i` of the
/// quads file without its fourth term, so those libraries answer the triple
/// patterns over exactly the rows the graph-capable ones do — the counts stay
/// comparable, and what they lack is the graph, not rows. `G`/`SPOG` are
/// unsupported for them, which is the only place the difference shows.
fn ensure_triples_dataset(n: usize) -> PathBuf {
    write_dataset(&data_dir().join(format!("triples_{n}.nt")), n, false)
}

/// Where the dataset files live. Shared with the Python suite, which writes
/// them to the same paths -- whichever runs first pays for them.
fn data_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("python/bench/.data")
}

/// Write the `n`-row dataset to `path` as N-Quads (`with_graphs`) or as its
/// N-Triples projection, unless the file already exists.
fn write_dataset(path: &Path, n: usize, with_graphs: bool) -> PathBuf {
    if path.exists() {
        return path.to_path_buf();
    }
    eprintln!(
        "generating {} {} -> {}",
        n,
        if with_graphs { "quads" } else { "triples" },
        path.display()
    );
    std::fs::create_dir_all(path.parent().unwrap()).expect("create data dir");
    let m = moduli(n);
    let tmp = path.with_extension("partial");
    let mut w = BufWriter::with_capacity(1 << 20, std::fs::File::create(&tmp).expect("create"));
    let mut line = String::with_capacity(256);
    for i in 0..n {
        line.clear();
        write!(
            line,
            "<{}> <{}> {}",
            subject_iri(i % m.n_subj),
            predicate_iri(i % m.n_pred),
            object_ntriples(i % m.n_obj),
        )
        .unwrap();
        if with_graphs {
            write!(line, " <{}>", graph_iri(i % m.n_graph)).unwrap();
        }
        line.push_str(" .\n");
        w.write_all(line.as_bytes()).expect("write");
    }
    w.flush().expect("flush");
    // Never leave a half-written dataset that looks complete.
    std::fs::rename(&tmp, path).expect("rename");
    path.to_path_buf()
}

/// A triple pattern probe. Terms are fixed at index 0, so every probe matches at
/// least row 0 and no pattern silently measures a zero-row query.
struct Probe {
    name: &'static str,
    s: Option<String>,
    p: Option<String>,
    o: Option<String>,
    g: Option<String>,
}

fn probes() -> Vec<Probe> {
    let s0 = format!("<{}>", subject_iri(0));
    let p0 = format!("<{}>", predicate_iri(0));
    let o0 = object_ntriples(0);
    vec![
        Probe {
            name: "S",
            s: Some(s0.clone()),
            p: None,
            o: None,
            g: None,
        },
        Probe {
            name: "P",
            s: None,
            p: Some(p0.clone()),
            o: None,
            g: None,
        },
        Probe {
            name: "O",
            s: None,
            p: None,
            o: Some(o0.clone()),
            g: None,
        },
        Probe {
            name: "SP",
            s: Some(s0.clone()),
            p: Some(p0.clone()),
            o: None,
            g: None,
        },
        Probe {
            name: "PO",
            s: None,
            p: Some(p0.clone()),
            o: Some(o0.clone()),
            g: None,
        },
        Probe {
            name: "SPO",
            s: Some(s0),
            p: Some(p0),
            o: Some(o0),
            g: None,
        },
        Probe {
            name: "full",
            s: None,
            p: None,
            o: None,
            g: None,
        },
    ]
}

/// The two probes that bind a graph. Index-0 terms, like every other probe, so
/// both match at least row 0. Only a library whose model has graphs answers
/// them; the rest report `unsupported` (see `Adapter::handles_graphs`).
fn graph_probes() -> Vec<Probe> {
    let s0 = format!("<{}>", subject_iri(0));
    let p0 = format!("<{}>", predicate_iri(0));
    let o0 = object_ntriples(0);
    let g0 = format!("<{}>", graph_iri(0));
    vec![
        Probe {
            name: "G",
            s: None,
            p: None,
            o: None,
            g: Some(g0.clone()),
        },
        Probe {
            name: "SPOG",
            s: Some(s0),
            p: Some(p0),
            o: Some(o0),
            g: Some(g0),
        },
    ]
}

// ─── Result rows (the dashboard's shape) ────────────────────────────────────

/// One measured cell, in the same JSON shape `js/bench/shared.ts` and
/// `python/bench/worker.py` emit, so `render_bench_dashboard.py` reads all three
/// with one code path.
#[derive(serde::Serialize)]
struct Row {
    group: String,
    variant: Option<String>,
    id: String,
    fastest: String,
    slowest: String,
    median: String,
    mean: String,
    fastest_ns: Option<f64>,
    slowest_ns: Option<f64>,
    median_ns: Option<f64>,
    mean_ns: Option<f64>,
    samples: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    regime: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    unsupported: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<String>,
}

fn fmt_ns(ns: f64) -> String {
    // Matches the JS and Python harnesses' formatting so all three tabs read alike.
    if ns < 1_000.0 {
        format!("{:.4} ns", ns)
    } else if ns < 1_000_000.0 {
        format!("{:.4} µs", ns / 1_000.0)
    } else if ns < 1_000_000_000.0 {
        format!("{:.4} ms", ns / 1_000_000.0)
    } else {
        format!("{:.4} s", ns / 1_000_000_000.0)
    }
}

fn row_from(id: &str, mut samples_ns: Vec<f64>, regime: Option<&'static str>) -> Row {
    samples_ns.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = samples_ns.len();
    let median = samples_ns[n / 2];
    let mean = samples_ns.iter().sum::<f64>() / n as f64;
    let (group, variant) = split_id(id);
    Row {
        group,
        variant,
        id: id.to_string(),
        fastest: fmt_ns(samples_ns[0]),
        slowest: fmt_ns(samples_ns[n - 1]),
        median: fmt_ns(median),
        mean: fmt_ns(mean),
        fastest_ns: Some(samples_ns[0]),
        slowest_ns: Some(samples_ns[n - 1]),
        median_ns: Some(median),
        mean_ns: Some(mean),
        samples: n.to_string(),
        regime,
        unsupported: None,
        reason: None,
    }
}

/// A cell with no measurement to make. Mirrors `unsupported_row` in
/// `python/bench/worker.py` and `unsupportedRow` in `js/bench/shared.ts`: the null
/// median keeps it out of its column's best/ratio arithmetic, and one word covers
/// every kind of absence with the specifics in the tooltip.
fn unsupported_row(id: &str, reason: &str) -> Row {
    let (group, variant) = split_id(id);
    Row {
        group,
        variant,
        id: id.to_string(),
        fastest: "unsupported".into(),
        slowest: "unsupported".into(),
        median: "unsupported".into(),
        mean: "unsupported".into(),
        fastest_ns: None,
        slowest_ns: None,
        median_ns: None,
        mean_ns: None,
        samples: "0".into(),
        regime: None,
        unsupported: Some(true),
        reason: Some(reason.to_string()),
    }
}

fn split_id(id: &str) -> (String, Option<String>) {
    match id.split_once("::") {
        Some((g, v)) => (g.to_string(), Some(v.to_string())),
        None => (id.to_string(), None),
    }
}

/// A phase whose single execution already exceeds this stops after one sample
/// (and skips any remaining warmup); `0` disables the rule.
///
/// Repetition exists to average out noise that is small relative to the
/// reading — on a ten-second bulk phase the extra runs cost real minutes to
/// remove noise orders of magnitude below the number. The reduced count is
/// reported honestly through the row's `samples` field, which the dashboard
/// displays. Same knob and default as the JS and Python workers.
fn slow_phase_cutoff_ns() -> f64 {
    static V: OnceLock<f64> = OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("BENCH_SLOW_PHASE_MS")
            .ok()
            .and_then(|s| s.parse::<f64>().ok())
            .unwrap_or(30_000.0)
            * 1e6
    })
}

/// Time `f` `iters` times after `warmup` untimed runs, and record the median.
/// Both loops honor [`slow_phase_cutoff_ns`].
fn measure<F: FnMut()>(
    id: &str,
    mut f: F,
    rows: &mut Vec<Row>,
    iters: usize,
    warmup: usize,
    regime: Option<&'static str>,
) {
    let cutoff = slow_phase_cutoff_ns();
    for _ in 0..warmup {
        let t = Instant::now();
        f();
        // A warmup run this long says the phase needs no more warming.
        if cutoff > 0.0 && (t.elapsed().as_nanos() as f64) > cutoff {
            break;
        }
    }
    let mut samples = Vec::with_capacity(iters);
    for _ in 0..iters {
        let t = Instant::now();
        f();
        let ns = t.elapsed().as_nanos() as f64;
        samples.push(ns);
        if cutoff > 0.0 && samples.len() == 1 && ns > cutoff {
            break;
        }
    }
    rows.push(row_from(id, samples, regime));
}

/// Bytes the build wrote: a file's length, or a directory's recursive total —
/// a RocksDB store is a directory of SSTs, not a single file.
fn artifact_size(path: &Path) -> Option<u64> {
    let md = std::fs::metadata(path).ok()?;
    if md.is_file() {
        return Some(md.len());
    }
    let mut total = 0;
    let mut stack = vec![path.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(dir).ok()? {
            let entry = entry.ok()?;
            let md = entry.metadata().ok()?;
            if md.is_dir() {
                stack.push(entry.path());
            } else {
                total += md.len();
            }
        }
    }
    Some(total)
}

/// This process's peak resident set size, in MB.
///
/// `VmHWM` is the kernel's own high-water mark, not a point-in-time sample, so it
/// survives the allocator having already returned the peak. It is attributable to
/// one library only because each adapter runs in its own process.
fn peak_rss_mb() -> Option<f64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|l| l.starts_with("VmHWM:"))?;
    let kb: f64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some((kb / 1024.0 * 10.0).round() / 10.0)
}

// ─── Adapters ───────────────────────────────────────────────────────────────

/// What one library under test must be able to do. `build` goes from the shared
/// N-Triples file to whatever that library calls a store; `open` is the separate
/// cost of getting a *fresh process* to a queryable state over an artifact the
/// build already wrote -- which only a library with a persistent form has.
trait Adapter {
    fn slug(&self) -> &'static str;
    fn label(&self) -> &'static str;

    /// True when opening the artifact is a distinct operation from building it.
    /// False for a store that lives in memory: "opening" it is re-parsing the
    /// source, which the Build column already reports.
    fn has_distinct_open(&self) -> bool {
        true
    }

    fn artifact(&self, workdir: &Path) -> PathBuf;
    fn build(&self, src: &Path, artifact: &Path);
    fn open(&self, artifact: &Path, src: &Path) -> Box<dyn Queryable>;

    /// Whether the library's model has graphs at all.
    ///
    /// True (the default) means it builds from the quad dataset and answers
    /// every pattern on it. False means its model is triples-only (hdt), so it
    /// reads the triples projection of the same rows — the same subjects,
    /// predicates and objects, with the graph dropped — and its `G`/`SPOG`
    /// cells read `unsupported`.
    fn handles_graphs(&self) -> bool {
        true
    }

    /// The add and delete phases, for a library that can mutate. Add:
    /// `MUT_BATCH` per-quad inserts into an empty store; `add_batch` is the
    /// one-call path. Delete: `MUT_BATCH` deletes against the full store,
    /// whose reopen/rebuild happens in untimed per-iteration setup. The
    /// default measures nothing.
    fn mutate(&self, _src: &Path, _artifact: &Path, _n: usize, _rows: &mut Vec<Row>) {}
}

/// Which RDF format a source path carries: the dataset is N-Quads, its
/// projection N-Triples, and every adapter parses whichever it was handed.
fn source_is_quads(src: &Path) -> bool {
    src.extension().is_some_and(|e| e == "nq")
}

/// [`source_is_quads`] in oxigraph's format enum.
fn oxigraph_format(src: &Path) -> oxigraph::io::RdfFormat {
    if source_is_quads(src) {
        oxigraph::io::RdfFormat::NQuads
    } else {
        oxigraph::io::RdfFormat::NTriples
    }
}

trait Queryable {
    /// Resolve a pattern and *consume* what it matched: every term of every
    /// matched row is read, and the row count returned.
    ///
    /// Advancing an iterator is not the same measurement across these
    /// libraries. sophia keeps its terms materialized and interned, oxigraph
    /// and hdt keep them encoded, Vortex keeps columns — so a bare `.count()`
    /// lets the first answer from index entries alone while the others decode,
    /// and the column reads "count" while measuring four different things. Each
    /// implementation below therefore reads the lexical bytes of every matched
    /// term into a `black_box`ed accumulator, which is the same rule the
    /// JavaScript tab's `consumeQuads` applies. What is timed is producing the
    /// terms; what is returned is the row count the harness cross-checks.
    fn count(&self, probe: &Probe) -> usize;

    /// Resolve the same pattern and return only how many rows matched — no
    /// term is read. Each library's cheapest correct count path, the
    /// COUNT/ASK shape of the workload.
    fn count_only(&self, probe: &Probe) -> usize;
}

/// The lexical bytes of one oxrdf term — the read that forces a matched row's
/// terms to exist.
fn oxrdf_quad_bytes(q: &oxrdf::Quad) -> usize {
    use oxrdf::{GraphName, NamedOrBlankNode, Term};
    let subject = match &q.subject {
        NamedOrBlankNode::NamedNode(n) => n.as_str().len(),
        NamedOrBlankNode::BlankNode(b) => b.as_str().len(),
    };
    let object = match &q.object {
        Term::NamedNode(n) => n.as_str().len(),
        Term::BlankNode(b) => b.as_str().len(),
        Term::Literal(l) => l.value().len(),
    };
    let graph = match &q.graph_name {
        GraphName::NamedNode(n) => n.as_str().len(),
        GraphName::BlankNode(b) => b.as_str().len(),
        GraphName::DefaultGraph => 0,
    };
    subject + q.predicate.as_str().len() + object + graph
}

/// The lexical bytes of one sophia term. Its terms are already materialized in
/// the graph's interned store, so this is what reading one costs there — the
/// counterpart of a decode in the libraries that keep terms encoded.
fn sophia_term_bytes<T: sophia::api::term::Term>(t: T) -> usize {
    if let Some(iri) = t.iri() {
        iri.len()
    } else if let Some(lex) = t.lexical_form() {
        lex.len()
    } else if let Some(id) = t.bnode_id() {
        id.len()
    } else {
        0
    }
}

// ── Vortex ──

struct VortexAdapter {
    slug: &'static str,
    label: &'static str,
    layout: LayoutStrategy,
    indexes: Vec<IndexType>,
    /// Load the whole store up front (`from_file_in_memory`) instead of opening
    /// it memory-mapped. The same axis the Python tab crosses with the
    /// secondary index, and the configuration that matches how the in-memory
    /// libraries here hold data.
    in_memory: bool,
    /// Whether this variant runs the mutation phase. One Vortex row does: the
    /// add phase starts from an empty store, which no layout/index knob
    /// reaches.
    measures_mutations: bool,
}

impl Adapter for VortexAdapter {
    fn slug(&self) -> &'static str {
        self.slug
    }
    fn label(&self) -> &'static str {
        self.label
    }
    fn artifact(&self, workdir: &Path) -> PathBuf {
        workdir.join(format!("{}.vortex", self.slug))
    }
    fn build(&self, src: &Path, artifact: &Path) {
        let file = std::fs::File::open(src).expect("open source");
        let format = if source_is_quads(src) {
            oxrdfio::RdfFormat::NQuads
        } else {
            oxrdfio::RdfFormat::NTriples
        };
        let quads = parse_quads_from_reader(file, format);
        rt().block_on(vortex_rdf_core::io::quads_stream_to_vortex_file(
            Box::pin(quads),
            artifact,
            self.layout,
            self.indexes.clone(),
        ))
        .expect("build vortex file");
    }
    fn open(&self, artifact: &Path, _src: &Path) -> Box<dyn Queryable> {
        let store = rt().block_on(async {
            if self.in_memory {
                // The load the Python bindings' `in_memory=True` calls, so the
                // two tabs' memory rows mean the same thing.
                VortexRdfStore::from_file_in_memory(artifact)
                    .await
                    .expect("load vortex file into memory")
            } else {
                VortexRdfStore::from_file(artifact)
                    .await
                    .expect("open vortex file")
            }
        });
        Box::new(VortexStore(store))
    }

    fn mutate(&self, _src: &Path, artifact: &Path, n: usize, rows: &mut Vec<Row>) {
        if !self.measures_mutations {
            return;
        }
        let slug = self.slug();
        let batch = mut_batch();
        let log = |m: &str| eprintln!("  [{slug}] {m}");
        let fresh = fresh_quads(batch);
        log("add (per-quad)…");
        measure(
            &format!("add::{slug}"),
            || {
                rt().block_on(async {
                    let mut store = VortexRdfStore::empty();
                    for q in &fresh {
                        store = store.add_quad(q.clone()).await.expect("add_quad");
                    }
                    store
                });
            },
            rows,
            HEAVY_ITERS,
            0,
            None,
        );
        log("add (batch)…");
        measure(
            &format!("add_batch::{slug}"),
            || {
                rt().block_on(async {
                    VortexRdfStore::empty()
                        .add_quads(fresh.iter().cloned())
                        .await
                        .expect("add_quads")
                });
            },
            rows,
            HEAVY_ITERS,
            0,
            None,
        );
        log("delete…");
        let del = dataset_prefix(n, moduli(n), batch);
        measure_with_setup(
            &format!("delete::{slug}"),
            || {
                rt().block_on(VortexRdfStore::from_file(artifact))
                    .expect("reopen")
            },
            |store| {
                rt().block_on(async {
                    let mut s = store;
                    for q in &del {
                        s = s.delete_quad(q).await.expect("delete_quad");
                    }
                    s
                });
            },
            rows,
            HEAVY_ITERS,
            None,
        );
    }
}

struct VortexStore(VortexRdfStore);

/// A probe as oxrdf terms, in `match_pattern`'s argument order.
type OxrdfProbe = (
    Option<oxrdf::NamedOrBlankNode>,
    Option<oxrdf::NamedNode>,
    Option<oxrdf::Term>,
    Option<oxrdf::GraphName>,
);

impl VortexStore {
    fn parse(probe: &Probe) -> OxrdfProbe {
        (
            probe.s.as_ref().map(|t| parse_subject(t)),
            probe.p.as_ref().map(|t| parse_predicate(t)),
            probe.o.as_ref().map(|t| parse_object(t)),
            probe.g.as_ref().map(|t| {
                oxrdf::GraphName::NamedNode(oxrdf::NamedNode::new_unchecked(strip_iri(t)))
            }),
        )
    }
}

impl Queryable for VortexStore {
    fn count(&self, probe: &Probe) -> usize {
        let (s, p, o, g) = Self::parse(probe);
        rt().block_on(async {
            let view = self
                .0
                .match_pattern(s.as_ref(), p.as_ref(), o.as_ref(), g.as_ref())
                .await
                .expect("match");
            // Materialize, don't just read a row count: the view's `size`
            // would answer a full scan from metadata in nanoseconds -- a number
            // that compares nothing. `quads_vec` builds four terms per row, and
            // the fold below reads them, which is what every other adapter here
            // now does too (see `Queryable::count`).
            let quads = view.quads_vec().await.expect("materialize");
            let bytes: usize = quads.iter().map(oxrdf_quad_bytes).sum();
            std::hint::black_box(bytes);
            quads.len()
        })
    }

    fn count_only(&self, probe: &Probe) -> usize {
        let (s, p, o, g) = Self::parse(probe);
        rt().block_on(async {
            let view = self
                .0
                .match_pattern(s.as_ref(), p.as_ref(), o.as_ref(), g.as_ref())
                .await
                .expect("match");
            // The count-only read: the selection's size, evaluating the match
            // where it was deferred but decoding no quad.
            view.size().await.expect("size")
        })
    }
}

fn parse_subject(t: &str) -> oxrdf::NamedOrBlankNode {
    oxrdf::NamedOrBlankNode::NamedNode(oxrdf::NamedNode::new_unchecked(strip_iri(t)))
}
fn parse_predicate(t: &str) -> oxrdf::NamedNode {
    oxrdf::NamedNode::new_unchecked(strip_iri(t))
}
fn parse_object(t: &str) -> oxrdf::Term {
    if let Some(lit) = t.strip_prefix('"').and_then(|r| r.strip_suffix('"')) {
        oxrdf::Term::Literal(oxrdf::Literal::new_simple_literal(lit))
    } else {
        oxrdf::Term::NamedNode(oxrdf::NamedNode::new_unchecked(strip_iri(t)))
    }
}
fn strip_iri(t: &str) -> String {
    t.trim_start_matches('<').trim_end_matches('>').to_string()
}

// ── oxigraph ──

struct OxigraphAdapter;

impl Adapter for OxigraphAdapter {
    fn slug(&self) -> &'static str {
        "oxigraph"
    }
    fn label(&self) -> &'static str {
        "oxigraph (memory)"
    }
    fn has_distinct_open(&self) -> bool {
        false
    }
    fn artifact(&self, workdir: &Path) -> PathBuf {
        // No artifact of its own: the source file is what a fresh process reloads.
        workdir.join("oxigraph.marker")
    }
    fn build(&self, src: &Path, artifact: &Path) {
        let store = load_oxigraph(src, oxigraph_format(src));
        std::fs::write(artifact, format!("{}", store.len().unwrap())).ok();
    }
    fn open(&self, _artifact: &Path, src: &Path) -> Box<dyn Queryable> {
        Box::new(OxStore(load_oxigraph(src, oxigraph_format(src))))
    }

    fn mutate(&self, src: &Path, _artifact: &Path, n: usize, rows: &mut Vec<Row>) {
        let slug = self.slug();
        let batch = mut_batch();
        let log = |m: &str| eprintln!("  [{slug}] {m}");
        let fresh = fresh_quads(batch);
        log("add (per-quad)…");
        measure(
            &format!("add::{slug}"),
            || {
                let store = oxigraph::store::Store::new().expect("new store");
                for q in &fresh {
                    store.insert(q.as_ref()).expect("insert");
                }
            },
            rows,
            HEAVY_ITERS,
            0,
            None,
        );
        log("add (batch)…");
        measure(
            &format!("add_batch::{slug}"),
            || {
                let store = oxigraph::store::Store::new().expect("new store");
                let mut loader = store.bulk_loader();
                loader.load_quads(fresh.iter().cloned()).expect("bulk load");
                loader.commit().expect("commit");
            },
            rows,
            HEAVY_ITERS,
            0,
            None,
        );
        log("delete…");
        let del = dataset_prefix(n, moduli(n), batch);
        measure_with_setup(
            &format!("delete::{slug}"),
            || load_oxigraph(src, oxigraph_format(src)),
            |store| {
                for q in &del {
                    store.remove(q.as_ref()).expect("remove");
                }
            },
            rows,
            HEAVY_ITERS,
            None,
        );
    }
}

fn load_oxigraph(src: &Path, format: oxigraph::io::RdfFormat) -> oxigraph::store::Store {
    let store = oxigraph::store::Store::new().expect("new store");
    let file = std::io::BufReader::new(std::fs::File::open(src).expect("open source"));
    store.load_from_reader(format, file).expect("bulk load");
    store
}

struct OxStore(oxigraph::store::Store);

impl Queryable for OxStore {
    fn count(&self, probe: &Probe) -> usize {
        self.run(probe, true)
    }

    fn count_only(&self, probe: &Probe) -> usize {
        // No count API in oxigraph: the iterator still decodes owned quads,
        // which is this store's floor for answering a count.
        self.run(probe, false)
    }
}

impl OxStore {
    fn run(&self, probe: &Probe, consume: bool) -> usize {
        use oxigraph::model::{GraphNameRef, NamedNodeRef, NamedOrBlankNodeRef, TermRef};
        let s = probe.s.as_ref().map(|t| strip_iri(t));
        let p = probe.p.as_ref().map(|t| strip_iri(t));
        let o = probe.o.clone();
        let g = probe.g.as_ref().map(|t| strip_iri(t));
        let s_ref = s
            .as_ref()
            .map(|v| NamedOrBlankNodeRef::NamedNode(NamedNodeRef::new_unchecked(v)));
        let p_ref = p.as_ref().map(|v| NamedNodeRef::new_unchecked(v));
        let g_ref = g
            .as_ref()
            .map(|v| GraphNameRef::NamedNode(NamedNodeRef::new_unchecked(v)));
        let lit;
        let o_ref = match o.as_ref() {
            None => None,
            Some(t) if t.starts_with('"') => {
                lit = oxigraph::model::Literal::new_simple_literal(
                    t.trim_start_matches('"').trim_end_matches('"'),
                );
                Some(TermRef::Literal(lit.as_ref()))
            }
            Some(t) => {
                let stripped = strip_iri(t);
                // NamedNodeRef borrows, so the owned String must outlive it.
                let o_ref = TermRef::NamedNode(NamedNodeRef::new_unchecked(&stripped));
                return drain_oxigraph(
                    self.0.quads_for_pattern(s_ref, p_ref, Some(o_ref), g_ref),
                    consume,
                );
            }
        };
        drain_oxigraph(
            self.0.quads_for_pattern(s_ref, p_ref, o_ref, g_ref),
            consume,
        )
    }
}

/// Drain an oxigraph result iterator; `consume` reads every term it decodes,
/// the count-only arm just walks it.
fn drain_oxigraph<E: std::fmt::Debug>(
    quads: impl Iterator<Item = Result<oxigraph::model::Quad, E>>,
    consume: bool,
) -> usize {
    let mut rows = 0usize;
    let mut bytes = 0usize;
    for q in quads {
        if consume {
            bytes += oxrdf_quad_bytes(&q.expect("oxigraph quad"));
        }
        rows += 1;
    }
    std::hint::black_box(bytes);
    rows
}

// ── oxigraph (rocksdb) ──

/// oxigraph over its persistent RocksDB backend: the artifact is a database
/// directory, built with the bulk loader (oxigraph's recommended large-ingest
/// path) and reopened read-only. Read-only is what makes the harness's
/// overlapping handles legal — RocksDB allows one read-write instance, but any
/// number of read-only ones, and the warm handle is still alive while the cold
/// arms open fresh ones.
struct OxigraphRocksAdapter;

impl Adapter for OxigraphRocksAdapter {
    fn slug(&self) -> &'static str {
        "oxigraph_rocksdb"
    }
    fn label(&self) -> &'static str {
        "oxigraph (rocksdb)"
    }
    fn artifact(&self, workdir: &Path) -> PathBuf {
        workdir.join("oxigraph.rocksdb")
    }
    fn build(&self, src: &Path, artifact: &Path) {
        build_oxigraph_rocksdb(artifact, src, oxigraph_format(src));
    }
    fn open(&self, artifact: &Path, _src: &Path) -> Box<dyn Queryable> {
        Box::new(OxStore(
            oxigraph::store::Store::open_read_only(artifact).expect("open rocksdb read-only"),
        ))
    }

    fn mutate(&self, src: &Path, artifact: &Path, n: usize, rows: &mut Vec<Row>) {
        let slug = self.slug();
        let batch = mut_batch();
        let log = |m: &str| eprintln!("  [{slug}] {m}");
        let fresh = fresh_quads(batch);
        // A scratch database per iteration; deleting the previous one is
        // untimed setup, but creating the store is part of what an add
        // costs on this backend, so it stays in the timed region — the
        // memory arm's `Store::new` sits inside its timed region too.
        let tmp = artifact.parent().unwrap().join("mut_tmp.rocksdb");
        log("add (per-quad)…");
        measure_with_setup(
            &format!("add::{slug}"),
            || {
                let _ = std::fs::remove_dir_all(&tmp);
                tmp.clone()
            },
            |dir| {
                let store = oxigraph::store::Store::open(&dir).expect("open rocksdb");
                for q in &fresh {
                    store.insert(q.as_ref()).expect("insert");
                }
            },
            rows,
            HEAVY_ITERS,
            None,
        );
        log("add (batch)…");
        measure_with_setup(
            &format!("add_batch::{slug}"),
            || {
                let _ = std::fs::remove_dir_all(&tmp);
                tmp.clone()
            },
            |dir| {
                let store = oxigraph::store::Store::open(&dir).expect("open rocksdb");
                let mut loader = store.bulk_loader();
                loader.load_quads(fresh.iter().cloned()).expect("bulk load");
                loader.commit().expect("commit");
            },
            rows,
            HEAVY_ITERS,
            None,
        );
        log("delete…");
        let del = dataset_prefix(n, moduli(n), batch);
        // Deletes need a read-write store holding the full dataset, and
        // each iteration removes rows the next one must find again — so
        // setup rebuilds the scratch database from the source, the same
        // per-iteration reload the memory arm does.
        measure_with_setup(
            &format!("delete::{slug}"),
            || build_oxigraph_rocksdb(&tmp, src, oxigraph_format(src)),
            |store| {
                for q in &del {
                    store.remove(q.as_ref()).expect("remove");
                }
            },
            rows,
            HEAVY_ITERS,
            None,
        );
        let _ = std::fs::remove_dir_all(&tmp);
    }
}

/// Bulk-load `src` into a fresh RocksDB store at `dir`, flushed so the data is
/// in SSTs rather than the WAL — which is what a read-only open reads. Removes
/// any previous database first: each build iteration must pay the whole cost,
/// not a re-insert of rows RocksDB already holds.
fn build_oxigraph_rocksdb(
    dir: &Path,
    src: &Path,
    format: oxigraph::io::RdfFormat,
) -> oxigraph::store::Store {
    let _ = std::fs::remove_dir_all(dir);
    let store = oxigraph::store::Store::open(dir).expect("open rocksdb read-write");
    let mut loader = store.bulk_loader();
    let file = std::io::BufReader::new(std::fs::File::open(src).expect("open source"));
    loader.load_from_reader(format, file).expect("bulk load");
    loader.commit().expect("commit bulk load");
    store.flush().expect("flush");
    store
}

// ── sophia ──

struct SophiaAdapter;

impl Adapter for SophiaAdapter {
    fn slug(&self) -> &'static str {
        "sophia"
    }
    fn label(&self) -> &'static str {
        "sophia (memory)"
    }
    fn has_distinct_open(&self) -> bool {
        false
    }
    fn artifact(&self, workdir: &Path) -> PathBuf {
        workdir.join("sophia.marker")
    }
    fn build(&self, src: &Path, artifact: &Path) {
        let d = load_sophia_dataset(src);
        use sophia::api::dataset::Dataset;
        std::fs::write(artifact, format!("{}", d.quads().count())).ok();
    }
    fn open(&self, _artifact: &Path, src: &Path) -> Box<dyn Queryable> {
        Box::new(SophiaDataset(load_sophia_dataset(src)))
    }

    fn mutate(&self, src: &Path, _artifact: &Path, n: usize, rows: &mut Vec<Row>) {
        let slug = self.slug();
        let batch = mut_batch();
        let log = |m: &str| eprintln!("  [{slug}] {m}");
        use sophia::api::dataset::MutableDataset;
        let fresh = fresh_quads(batch);
        let fresh_terms: Vec<[SimpleTerm<'static>; 3]> = fresh
            .iter()
            .map(|q| {
                [
                    simple_iri(q.subject.to_string().as_str()),
                    simple_iri(q.predicate.to_string().as_str()),
                    simple_iri(q.object.to_string().as_str()),
                ]
            })
            .collect();
        log("add (per-quad)…");
        measure(
            &format!("add::{slug}"),
            || {
                let mut d = sophia::inmem::dataset::FastDataset::new();
                for [s, p, o] in &fresh_terms {
                    d.insert(s, p, o, None::<&SimpleTerm<'static>>)
                        .expect("insert");
                }
            },
            rows,
            HEAVY_ITERS,
            0,
            None,
        );
        rows.push(unsupported_row(
            &format!("add_batch::{slug}"),
            "no batch-insert API distinct from the per-quad loop",
        ));
        log("delete…");
        let del = dataset_prefix(n, moduli(n), batch);
        let del_terms: Vec<[SimpleTerm<'static>; 4]> = del
            .iter()
            .map(|q| {
                let o = match &q.object {
                    oxrdf::Term::Literal(l) => simple_object(&format!("\"{}\"", l.value())),
                    other => simple_iri(other.to_string().as_str()),
                };
                let g = match &q.graph_name {
                    oxrdf::GraphName::NamedNode(n) => simple_iri(n.as_str()),
                    _ => simple_iri(""),
                };
                [
                    simple_iri(q.subject.to_string().as_str()),
                    simple_iri(q.predicate.to_string().as_str()),
                    o,
                    g,
                ]
            })
            .collect();
        measure_with_setup(
            &format!("delete::{slug}"),
            || load_sophia_dataset(src),
            |mut d| {
                for [s, p, o, g] in &del_terms {
                    d.remove(s, p, o, Some(g)).expect("remove");
                }
            },
            rows,
            HEAVY_ITERS,
            None,
        );
    }
}

fn load_sophia_dataset(src: &Path) -> sophia::inmem::dataset::FastDataset {
    use sophia::api::parser::QuadParser;
    use sophia::api::prelude::QuadSource;
    let file = std::io::BufReader::new(std::fs::File::open(src).expect("open quads source"));
    let parser = sophia::turtle::parser::nq::NQuadsParser::default();
    let mut d = sophia::inmem::dataset::FastDataset::new();
    parser
        .parse(file)
        .add_to_dataset(&mut d)
        .expect("parse into sophia dataset");
    d
}

struct SophiaDataset(sophia::inmem::dataset::FastDataset);

impl SophiaDataset {
    /// The probe as matchers (see `AnyOr`); the graph position needs a
    /// GraphNameMatcher, which `gn()` derives from a TermMatcher at the call.
    fn parse(probe: &Probe) -> (AnyOr, AnyOr, AnyOr, AnyOr) {
        let iri = |t: &Option<String>| {
            t.as_ref()
                .map_or(AnyOr::Any, |t| AnyOr::Exactly(simple_iri(t)))
        };
        (
            iri(&probe.s),
            iri(&probe.p),
            probe
                .o
                .as_ref()
                .map_or(AnyOr::Any, |t| AnyOr::Exactly(simple_object(t))),
            iri(&probe.g),
        )
    }
}

impl Queryable for SophiaDataset {
    fn count(&self, probe: &Probe) -> usize {
        use sophia::api::dataset::Dataset;
        use sophia::api::term::matcher::TermMatcher;
        let (s, p, o, g) = Self::parse(probe);
        use sophia::api::quad::Quad;
        let mut rows = 0usize;
        let mut bytes = 0usize;
        for q in self.0.quads_matching(s, p, o, g.gn()) {
            let q = q.expect("sophia quad");
            bytes += sophia_term_bytes(q.s())
                + sophia_term_bytes(q.p())
                + sophia_term_bytes(q.o())
                + q.g().map_or(0, sophia_term_bytes);
            rows += 1;
        }
        std::hint::black_box(bytes);
        rows
    }

    fn count_only(&self, probe: &Probe) -> usize {
        use sophia::api::dataset::Dataset;
        use sophia::api::term::matcher::TermMatcher;
        let (s, p, o, g) = Self::parse(probe);
        self.0.quads_matching(s, p, o, g.gn()).count()
    }
}

/// "This exact term, or anything" -- the matcher sophia does not ship.
///
/// It reports the constant when there is one and matches everything when there
/// is not. Neither of sophia's two near-fits does that: a **closure** matcher
/// reports no [`TermMatcher::constant`], and that constant is exactly what
/// `FastGraph` uses to look a term up in its index, so every pattern degrades
/// to a full scan; **`Option<T>`** does report a constant, but its documented
/// meaning is "the wrapped term if any, otherwise *nothing*", so every wildcard
/// silently matches zero rows.
enum AnyOr {
    Any,
    Exactly(SimpleTerm<'static>),
}

impl sophia::api::term::matcher::TermMatcher for AnyOr {
    type Term = SimpleTerm<'static>;

    fn matches<T2: sophia::api::term::Term + ?Sized>(&self, term: &T2) -> bool {
        match self {
            Self::Any => true,
            Self::Exactly(t) => sophia::api::term::Term::eq(t, term.borrow_term()),
        }
    }

    fn constant(&self) -> Option<&Self::Term> {
        match self {
            Self::Any => None,
            Self::Exactly(t) => Some(t),
        }
    }
}

fn simple_iri(t: &str) -> SimpleTerm<'static> {
    SimpleTerm::Iri(IriRef::new_unchecked(MownStr::from(strip_iri(t))))
}

/// The dataset's objects are IRIs or plain literals, and a plain literal is an
/// `xsd:string`-typed one -- spelling it any other way would fail to match the
/// terms the parser built, and the count check would catch it as a disagreement.
fn simple_object(t: &str) -> SimpleTerm<'static> {
    if t.starts_with('"') {
        SimpleTerm::LiteralDatatype(
            MownStr::from(t.trim_start_matches('"').trim_end_matches('"').to_string()),
            IriRef::new_unchecked(MownStr::from(
                "http://www.w3.org/2001/XMLSchema#string".to_string(),
            )),
        )
    } else {
        simple_iri(t)
    }
}

// ── hdt ──

struct HdtAdapter;

impl Adapter for HdtAdapter {
    fn slug(&self) -> &'static str {
        "hdt"
    }
    /// The HDT model is triples-only: no graph column exists to hold one, so
    /// this adapter reads the dataset's triples projection.
    fn handles_graphs(&self) -> bool {
        false
    }
    fn label(&self) -> &'static str {
        // `Hdt::read` parses the whole artifact into memory, so this answers
        // from memory on the residency axis — the file is what it was built
        // from, and what its cold regime reopens.
        "hdt (memory)"
    }
    fn artifact(&self, workdir: &Path) -> PathBuf {
        workdir.join("data.hdt")
    }
    fn build(&self, src: &Path, artifact: &Path) {
        let hdt = hdt::Hdt::read_nt(src).expect("build hdt from n-triples");
        let mut out = BufWriter::new(std::fs::File::create(artifact).expect("create hdt"));
        hdt.write(&mut out).expect("write hdt");
    }
    fn open(&self, artifact: &Path, _src: &Path) -> Box<dyn Queryable> {
        let f = std::fs::File::open(artifact).expect("open hdt");
        let hdt = hdt::Hdt::read(std::io::BufReader::new(f)).expect("read hdt");
        Box::new(HdtStore(hdt))
    }

    fn mutate(&self, _src: &Path, _artifact: &Path, _n: usize, rows: &mut Vec<Row>) {
        let slug = self.slug();
        for id in ["add", "add_batch", "delete"] {
            rows.push(unsupported_row(
                &format!("{id}::{slug}"),
                "an HDT file is immutable once built",
            ));
        }
    }
}

struct HdtStore(hdt::Hdt);

impl HdtStore {
    /// The probe in HDT's dictionary spelling: IRIs without angle brackets,
    /// literals with their quotes — what `object_ntriples` already produces
    /// for literals, so only IRIs need unwrapping.
    fn parse(probe: &Probe) -> (Option<String>, Option<String>, Option<String>) {
        (
            probe.s.as_ref().map(|t| strip_iri(t)),
            probe.p.as_ref().map(|t| strip_iri(t)),
            probe.o.as_ref().map(|t| {
                if t.starts_with('"') {
                    t.clone()
                } else {
                    strip_iri(t)
                }
            }),
        )
    }
}

impl Queryable for HdtStore {
    fn count(&self, probe: &Probe) -> usize {
        let (s, p, o) = Self::parse(probe);
        let mut rows = 0usize;
        let mut bytes = 0usize;
        for t in self
            .0
            .triples_with_pattern(s.as_deref(), p.as_deref(), o.as_deref())
        {
            bytes += t[0].len() + t[1].len() + t[2].len();
            rows += 1;
        }
        std::hint::black_box(bytes);
        rows
    }

    fn count_only(&self, probe: &Probe) -> usize {
        // The crate's iterator materializes its term strings internally either
        // way; walking it without reading them is hdt's floor for a count.
        let (s, p, o) = Self::parse(probe);
        self.0
            .triples_with_pattern(s.as_deref(), p.as_deref(), o.as_deref())
            .count()
    }
}

/// Time `f` with a fresh `setup()` product per iteration, setup untimed —
/// the shape of the cold query arms (a per-iteration `open`) and of the
/// mutation phases (a store reopened or rebuilt per iteration). Only
/// `f(target)` is timed; whatever it returns is dropped after the clock stops.
fn measure_with_setup<T, R>(
    id: &str,
    mut setup: impl FnMut() -> T,
    mut f: impl FnMut(T) -> R,
    rows: &mut Vec<Row>,
    iters: usize,
    regime: Option<&'static str>,
) {
    let cutoff = slow_phase_cutoff_ns();
    let mut samples = Vec::with_capacity(iters);
    for _ in 0..iters {
        let target = setup();
        let t = Instant::now();
        let r = f(target);
        let ns = t.elapsed().as_nanos() as f64;
        drop(r);
        samples.push(ns);
        // See `slow_phase_cutoff_ns`: one sample of a multi-ten-second phase.
        if cutoff > 0.0 && samples.len() == 1 && ns > cutoff {
            break;
        }
    }
    rows.push(row_from(id, samples, regime));
}

/// Quads per mutation batch (`MUT_BATCH`), unclamped: the add row includes
/// whatever auto-compaction the batch triggers.
fn mut_batch() -> usize {
    std::env::var("MUT_BATCH")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(10_000)
}

// ─── One adapter's run ──────────────────────────────────────────────────────

fn adapters() -> Vec<Box<dyn Adapter>> {
    /// The same index x residency matrix the Python tab shows, so the two tabs'
    /// Vortex rows line up one for one. A (file, memory) pair builds the same
    /// configuration in its own process and differs only in how it opens the result,
    /// so their Build cells agreeing is a free consistency check on the harness.
    ///
    /// Dictionary is the only layout here: the layout axis belongs to the
    /// instrumented suite's matrix, and an adapter the dashboard does not list
    /// costs a whole process to measure and then discards the rows.
    fn vortex(
        slug: &'static str,
        label: &'static str,
        layout: LayoutStrategy,
        indexes: Vec<IndexType>,
        in_memory: bool,
        measures_mutations: bool,
    ) -> Box<dyn Adapter> {
        Box::new(VortexAdapter {
            slug,
            label,
            layout,
            indexes,
            in_memory,
            measures_mutations,
        })
    }
    use IndexType::{SecondaryByCopy, SecondaryByReference};
    use LayoutStrategy::Dictionary;
    vec![
        vortex(
            "vortex_dict",
            "Vortex Dict",
            Dictionary,
            vec![],
            false,
            true,
        ),
        vortex(
            "vortex_dict_mem",
            "Vortex Dict (in-memory)",
            Dictionary,
            vec![],
            true,
            false,
        ),
        vortex(
            "vortex_dict_bycopy",
            "Vortex Dict+ByCopy",
            Dictionary,
            vec![SecondaryByCopy],
            false,
            false,
        ),
        vortex(
            "vortex_dict_bycopy_mem",
            "Vortex Dict+ByCopy (in-memory)",
            Dictionary,
            vec![SecondaryByCopy],
            true,
            false,
        ),
        vortex(
            "vortex_dict_byref",
            "Vortex Dict+ByRef",
            Dictionary,
            vec![SecondaryByReference],
            false,
            false,
        ),
        vortex(
            "vortex_dict_byref_mem",
            "Vortex Dict+ByRef (in-memory)",
            Dictionary,
            vec![SecondaryByReference],
            true,
            false,
        ),
        Box::new(OxigraphAdapter),
        Box::new(OxigraphRocksAdapter),
        Box::new(SophiaAdapter),
        Box::new(HdtAdapter),
    ]
}

#[derive(serde::Serialize)]
struct WorkerOut {
    slug: String,
    label: String,
    rows: Vec<Row>,
    counts: std::collections::BTreeMap<String, usize>,
    artifact_bytes: Option<u64>,
    peak_rss_mb: Option<f64>,
}

fn run_adapter(a: &dyn Adapter, n: usize) -> WorkerOut {
    // One dataset for the whole run: the quads file for a library whose model
    // has graphs, its triples projection for one whose model does not.
    let src = if a.handles_graphs() {
        ensure_quads_dataset(n)
    } else {
        ensure_triples_dataset(n)
    };
    let workdir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("target/bench_compare")
        .join(a.slug());
    std::fs::create_dir_all(&workdir).expect("create workdir");
    let artifact = a.artifact(&workdir);
    let log = |m: &str| eprintln!("  [{}] {}", a.label(), m);

    let mut rows: Vec<Row> = Vec::new();

    log("build…");
    measure(
        &format!("ingest::{}", a.slug()),
        || a.build(&src, &artifact),
        &mut rows,
        HEAVY_ITERS,
        0,
        None,
    );

    // Only a real persistent form has a size; the in-memory adapters' marker
    // files are bookkeeping, and reporting their few bytes would top the
    // smallest-first size panel with a store that writes no artifact at all.
    let artifact_bytes = if a.has_distinct_open() {
        artifact_size(&artifact)
    } else {
        None
    };

    // Open: only where there is an artifact to open. For an in-memory store,
    // opening is re-parsing the source, which the Build column already reports.
    if a.has_distinct_open() {
        log("open…");
        measure(
            &format!("open::{}", a.slug()),
            || {
                let _ = a.open(&artifact, &src);
            },
            &mut rows,
            QUERY_ITERS,
            QUERY_WARMUP,
            None,
        );
    } else {
        rows.push(unsupported_row(
            &format!("open::{}", a.slug()),
            "no artifact to reopen: a fresh process re-parses the source, which is the Build column",
        ));
    }

    let handle = a.open(&artifact, &src);
    let mut all_probes = probes();
    if a.handles_graphs() {
        all_probes.extend(graph_probes());
    } else {
        // Not a gap in the run: this library has no graph in its model, so the
        // cells say so rather than leaving a hole that reads as unmeasured.
        for probe in graph_probes() {
            for regime in ["warm", "cold"] {
                for suffix in ["", "::count"] {
                    let mut r = unsupported_row(
                        &format!("{}::{}{}", a.slug(), probe.name, suffix),
                        "this library's model has no graphs — it reads the triples projection",
                    );
                    r.regime = Some(regime);
                    rows.push(r);
                }
            }
        }
    }
    let mut counts = std::collections::BTreeMap::new();

    for probe in &all_probes {
        let heavy = probe.name == "full";
        counts.insert(probe.name.to_string(), handle.count(probe));
        log(&format!("match {} (warm)…", probe.name));
        measure(
            &format!("{}::{}", a.slug(), probe.name),
            || {
                handle.count(probe);
            },
            &mut rows,
            if heavy { HEAVY_ITERS } else { QUERY_ITERS },
            if heavy { 0 } else { QUERY_WARMUP },
            Some("warm"),
        );

        // Cold: a fresh handle per iteration, so every probe/segment cache starts
        // empty -- but the open itself is UNTIMED setup. Timing it here would
        // make every cold cell a restatement of the Open column, and would mean
        // something different from the cold columns on every other tab, which
        // all exclude it.
        if a.has_distinct_open() {
            log(&format!("match {} (cold)…", probe.name));
            measure_with_setup(
                &format!("{}::{}", a.slug(), probe.name),
                || a.open(&artifact, &src),
                |h| {
                    h.count(probe);
                    h
                },
                &mut rows,
                if heavy { HEAVY_ITERS } else { QUERY_ITERS },
                Some("cold"),
            );
        }

        // Count-only: the same resolution with no term read — the COUNT/ASK
        // shape. Verified against the consuming count first, so a count path
        // that resolves differently fails loudly instead of timing the wrong
        // work.
        let n_count = handle.count_only(probe);
        assert_eq!(
            n_count,
            counts[probe.name],
            "count_only disagrees with count for {} on {}",
            probe.name,
            a.slug()
        );
        log(&format!("count {} (warm)…", probe.name));
        measure(
            &format!("{}::{}::count", a.slug(), probe.name),
            || {
                handle.count_only(probe);
            },
            &mut rows,
            if heavy { HEAVY_ITERS } else { QUERY_ITERS },
            if heavy { 0 } else { QUERY_WARMUP },
            Some("warm"),
        );
        if a.has_distinct_open() {
            log(&format!("count {} (cold)…", probe.name));
            measure_with_setup(
                &format!("{}::{}::count", a.slug(), probe.name),
                || a.open(&artifact, &src),
                |h| {
                    h.count_only(probe);
                    h
                },
                &mut rows,
                if heavy { HEAVY_ITERS } else { QUERY_ITERS },
                Some("cold"),
            );
        }
    }

    drop(handle);

    a.mutate(&src, &artifact, n, &mut rows);

    WorkerOut {
        slug: a.slug().to_string(),
        label: a.label().to_string(),
        rows,
        counts,
        artifact_bytes,
        peak_rss_mb: peak_rss_mb(),
    }
}

// ─── Driver ─────────────────────────────────────────────────────────────────

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let n = bench_size();

    // Child mode: measure one adapter and print its JSON. One process per adapter,
    // so a cheap pattern is never measured in the shadow of another library's scan.
    if let Some(i) = args.iter().position(|a| a == "--adapter") {
        let slug = args.get(i + 1).expect("--adapter needs a slug");
        let all = adapters();
        let a = all
            .iter()
            .find(|a| a.slug() == slug)
            .unwrap_or_else(|| panic!("unknown adapter {slug}"));
        let out = run_adapter(a.as_ref(), n);
        println!("{}", serde_json::to_string(&out).unwrap());
        return;
    }

    eprintln!(
        "Rust comparative benchmark: {n} quads over {} named graphs, one process per library",
        moduli(n).n_graph
    );
    // Both files up front: a library reads one or the other, and generating them
    // here keeps that cost out of whichever adapter happens to run first.
    ensure_quads_dataset(n);
    ensure_triples_dataset(n);

    let exe = std::env::current_exe().expect("current exe");
    let mut rows: Vec<serde_json::Value> = Vec::new();
    let mut sizes: Vec<serde_json::Value> = Vec::new();
    let mut memory: Vec<serde_json::Value> = Vec::new();
    let mut counts_by_slug: std::collections::BTreeMap<String, serde_json::Value> =
        Default::default();

    for a in adapters() {
        eprintln!("\n== {} ==", a.label());
        let out = std::process::Command::new(&exe)
            .arg("--adapter")
            .arg(a.slug())
            .env("BENCH_SIZE", n.to_string())
            .stderr(std::process::Stdio::inherit())
            .output()
            .expect("spawn adapter child");
        if !out.status.success() {
            // Carry on: a partial tab beats no tab, and the missing rows are
            // visible as absent cells rather than as silently wrong ones.
            eprintln!("!! {} failed ({}), skipping", a.label(), out.status);
            continue;
        }
        let parsed: serde_json::Value =
            serde_json::from_slice(&out.stdout).expect("child emitted invalid JSON");
        if let Some(arr) = parsed["rows"].as_array() {
            rows.extend(arr.iter().cloned());
        }
        sizes.push(serde_json::json!({
            "slug": parsed["slug"], "label": parsed["label"],
            "bytes": parsed["artifact_bytes"],
        }));
        memory.push(serde_json::json!({
            "slug": parsed["slug"], "label": parsed["label"], "role": "query",
            "peakRssMb": parsed["peak_rss_mb"],
        }));
        counts_by_slug.insert(a.slug().to_string(), parsed["counts"].clone());
    }

    // Every adapter queried the same dataset with the same probes, so their counts
    // must agree; a disagreement means one was queried wrongly and its timings
    // measure the wrong work.
    let mut warnings: Vec<String> = Vec::new();
    let mut merged: std::collections::BTreeMap<String, u64> = Default::default();
    for (slug, counts) in &counts_by_slug {
        for (probe, v) in counts.as_object().into_iter().flatten() {
            let v = v.as_u64().unwrap_or(0);
            match merged.get(probe) {
                None => {
                    merged.insert(probe.clone(), v);
                }
                Some(&seen) if seen != v => {
                    warnings.push(format!(
                        "{probe}: {slug} matched {v}, an earlier library matched {seen}"
                    ));
                }
                _ => {}
            }
        }
    }
    for w in &warnings {
        eprintln!("!! count disagreement: {w}");
    }

    // Terms per role plus the default graph, matching `moduli` in
    // python/bench/datasets.py -- the same file, so necessarily the same count.
    let distinct_terms = moduli(n).terms();

    let out = serde_json::json!({
        "results": rows,
        "sizes": sizes,
        "memory": memory,
        "config": {
            "triplesCount": n,
            "quadsCount": n,
            "graphCount": moduli(n).n_graph,
            "mutBatch": mut_batch(),
            "distinctTerms": distinct_terms,
            "queryIterations": QUERY_ITERS,
            "heavyIterations": HEAVY_ITERS,
            "matchedRows": merged,
            "countWarnings": warnings,
        },
    });
    let dest = Path::new(env!("CARGO_MANIFEST_DIR")).join("bench/results.json");
    std::fs::create_dir_all(dest.parent().unwrap()).expect("create bench dir");
    std::fs::write(&dest, serde_json::to_string_pretty(&out).unwrap()).expect("write results");
    eprintln!("\nWrote {} rows -> {}", rows.len(), dest.display());
}
