//! Infrastructure shared by the bench targets (`benchmark.rs`, the CodSpeed
//! suite, and `match_lazy.rs`, the local-only lazy-match matrix): the axis
//! enums, dataset/store/file construction with their caches, and the match
//! pattern probes. Compiled once per target; the caches are per-process, so
//! each target builds its own artifacts.

use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};

use futures::{Stream, StreamExt, stream};
use oxrdf::{GraphName, Literal, NamedNode, NamedOrBlankNode, Quad, Term};
use tokio::runtime::Runtime;

use vortex_rdf_core::error::Result;
use vortex_rdf_core::store::RawQuad;
use vortex_rdf_core::{
    BuiltArray, IndexType, LayoutStrategy, SortedStreamBuilder, VortexArrayBuilder, VortexRdfStore,
};

/// Single dataset size for the whole suite. In simulation mode CodSpeed counts
/// instructions deterministically, so one representative size catches
/// regressions in every path; larger sizes only multiply valgrind cost without
/// adding signal (CodSpeed does not analyse scaling curves). Default matches
/// CodSpeed CI; override locally (e.g. `BENCH_SIZE=2097152 cargo bench`, to
/// match the JS comparative benchmark's default D=128 scale) to see how
/// results shift at a larger size.
pub fn bench_size() -> usize {
    static SIZE: OnceLock<usize> = OnceLock::new();
    *SIZE.get_or_init(|| {
        std::env::var("BENCH_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(100_000)
    })
}

// ── shared tokio runtime ────────────────────────────────────────────────────

static TOKIO_RUNTIME: OnceLock<Runtime> = OnceLock::new();

pub fn rt() -> &'static Runtime {
    TOKIO_RUNTIME.get_or_init(|| Runtime::new().unwrap())
}

// ── configuration axes ──────────────────────────────────────────────────────

#[derive(Copy, Clone, PartialEq, Eq, Hash)]
pub enum Layout {
    Default,
    TypedObject,
    Dictionary,
}

impl Layout {
    pub fn strategy(self) -> LayoutStrategy {
        match self {
            Self::Default => LayoutStrategy::Default,
            Self::TypedObject => LayoutStrategy::TypedObject,
            Self::Dictionary => LayoutStrategy::Dictionary,
        }
    }
    pub fn short(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::TypedObject => "typed_object",
            Self::Dictionary => "dictionary",
        }
    }
}

impl fmt::Debug for Layout {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.short())
    }
}

#[derive(Copy, Clone, PartialEq, Eq, Hash)]
pub enum Index {
    None,
    ByReference,
    ByCopy,
}

impl Index {
    pub fn types(self) -> Vec<IndexType> {
        match self {
            Self::None => vec![],
            Self::ByReference => vec![IndexType::SecondaryByReference],
            Self::ByCopy => vec![IndexType::SecondaryByCopy],
        }
    }
    pub fn short(self) -> &'static str {
        match self {
            Self::None => "no_index",
            Self::ByReference => "by_reference",
            Self::ByCopy => "by_copy",
        }
    }
}

#[derive(Copy, Clone, PartialEq, Eq, Hash)]
pub enum Source {
    File,
    InMemory,
    /// Lean adoption: `from_bytes` over the serialized store, keeping the
    /// base wire-encoded and the index components deferred — the shape js
    /// `fromBytes` and python `from_bytes` hand out.
    Bytes,
}

impl Source {
    pub fn short(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::InMemory => "in_memory",
            Self::Bytes => "from_bytes",
        }
    }
}

impl fmt::Debug for Source {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.short())
    }
}

// ── dataset + artifact construction (all untimed helpers) ────────────────────

/// Materialize the generated quads into an owned `Vec`, eagerly. The generator
/// is a *lazy* stream whose per-quad `format!` allocations would otherwise be
/// polled — and charged — inside the timed serialization region; draining it
/// here keeps those allocations out of the measurement.
pub fn materialize_quads(size: usize) -> Vec<RawQuad> {
    rt().block_on(async move {
        generate_rdf_data_stream(size)
            .map(|q| q.expect("quad generation is infallible"))
            .collect()
            .await
    })
}

/// Run a config's ingest to the builder's `BuiltArray`: the quad array plus
/// whatever travels beside it — under the Dictionary layout the array holds
/// only u32 codes and the term dictionary rides alongside; `from_built` is
/// the one constructor that accepts that pair.
fn build_built(layout: Layout, index: Index, size: usize) -> BuiltArray {
    rt().block_on(async move {
        SortedStreamBuilder::build_vortex_array(
            Box::new(generate_rdf_data_stream(size)),
            layout.strategy(),
            index.types(),
        )
        .await
        .expect("failed to build vortex array")
    })
}

pub type CacheKey = (Layout, Index, usize);

/// Cache of ingest products (`BuiltArray`, cheaply cloneable: Arc-shared
/// buffers). Only the expensive ingest is cached — stores are rebuilt per
/// handout, see [`cached_store`]. Only a handful of distinct configs are ever
/// requested, so this stays naturally bounded (unlike the old full-factorial
/// cache, which held every combination for the process lifetime).
static BUILT_CACHE: OnceLock<Mutex<HashMap<CacheKey, BuiltArray>>> = OnceLock::new();
static FILE_CACHE: OnceLock<Mutex<HashMap<CacheKey, PathBuf>>> = OnceLock::new();

fn cached_built(layout: Layout, index: Index, size: usize) -> BuiltArray {
    let cache = BUILT_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let key = (layout, index, size);
    if let Some(built) = cache.lock().unwrap().get(&key) {
        return built.clone();
    }
    let built = build_built(layout, index, size);
    cache.lock().unwrap().insert(key, built.clone());
    built
}

/// A FRESH store over a config's cached ingest product: `from_built` re-runs
/// component adoption and store construction every call, so no store-level
/// state is shared between the stores this returns. Handing out clones of one
/// cached store instead already produced a documented phantom regression
/// under CodSpeed's single-measured-invocation model (the file bench warming
/// the shared store shifted the in-memory match baselines) — tasks must start
/// cold and order-independent. Known residual: the immutable Arc'd buffers —
/// and vortex's interior-mutable per-array stats riding on them — remain
/// shared with the cache. That was the incident's actual channel (the writer
/// computing file stats on the shared array); today's writer repartitions
/// into copies, but any stat a read lazily computes still lands on the shared
/// array. Closing that too would take a deep copy per handout.
pub fn cached_store(layout: Layout, index: Index, size: usize) -> VortexRdfStore {
    VortexRdfStore::from_built(cached_built(layout, index, size))
        .expect("failed to build vortex store")
}

pub fn cached_file(layout: Layout, index: Index, size: usize) -> PathBuf {
    let cache = FILE_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let key = (layout, index, size);
    if let Some(path) = cache.lock().unwrap().get(&key) {
        return path.clone();
    }
    let store = cached_store(layout, index, size);
    let dir = PathBuf::from("target/bench_vortex_files");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(format!(
        "{}_{}_{}.vortex",
        layout.short(),
        index.short(),
        size
    ));
    rt().block_on(async {
        let bytes = store.to_bytes().await.expect("serialize store");
        std::fs::write(&path, bytes).expect("write file");
    });
    cache.lock().unwrap().insert(key, path.clone());
    path
}

/// Construct a store over a config's data, from the requested source. Both are
/// untimed: `from_file` reads the footer only, and the in-memory arm rebuilds
/// a fresh store from the cached ingest product (see [`cached_store`]).
pub fn make_store(source: Source, layout: Layout, index: Index, size: usize) -> VortexRdfStore {
    match source {
        Source::File => {
            let path = cached_file(layout, index, size);
            rt().block_on(async {
                VortexRdfStore::from_file(path)
                    .await
                    .expect("open file store")
            })
        }
        Source::InMemory => cached_store(layout, index, size),
        Source::Bytes => {
            let path = cached_file(layout, index, size);
            let bytes = std::fs::read(path).expect("read serialized store");
            rt().block_on(async {
                VortexRdfStore::from_bytes_owned(bytes)
                    .await
                    .expect("adopt bytes store")
            })
        }
    }
}

// ── match patterns ──────────────────────────────────────────────────────────

// Each variant names the bound components by letter (Subject/Predicate/
// Object/Graph), so `SPOG` is consistent with its siblings, not a word to
// re-case.
#[allow(clippy::upper_case_acronyms)]
#[derive(Copy, Clone, Debug)]
pub enum Pattern {
    S,
    P,
    O,
    PO,
    G,
    SPOG,
}

pub const PATTERNS: &[Pattern] = &[
    Pattern::S,
    Pattern::P,
    Pattern::O,
    Pattern::PO,
    Pattern::G,
    Pattern::SPOG,
];

/// Probe terms, all chosen to hit rows the generator actually emits (see the
/// module docs on selectivity).
#[allow(clippy::type_complexity)]
pub fn terms_for(
    pattern: Pattern,
) -> (
    Option<NamedOrBlankNode>,
    Option<NamedNode>,
    Option<Term>,
    Option<GraphName>,
) {
    let s =
        || NamedOrBlankNode::NamedNode(NamedNode::new_unchecked("http://example.org/subject/0"));
    let p = || NamedNode::new_unchecked("http://example.org/predicate/0");
    let o = || Term::NamedNode(NamedNode::new_unchecked("http://example.org/object/0"));
    let g = || GraphName::NamedNode(NamedNode::new_unchecked("http://example.org/graph/0"));

    match pattern {
        Pattern::S => (Some(s()), None, None, None),
        Pattern::P => (None, Some(p()), None, None),
        Pattern::O => (None, None, Some(o()), None),
        Pattern::PO => (None, Some(p()), Some(o()), None),
        Pattern::G => (None, None, None, Some(g())),
        Pattern::SPOG => (Some(s()), Some(p()), Some(o()), Some(g())),
    }
}

/// Generate a stream of mock RDF quads for the bench targets.
/// Triples are evenly distributed across 10 named graphs.
pub fn generate_rdf_data_stream(size: usize) -> impl Stream<Item = Result<RawQuad>> {
    const EX: &str = "http://example.org/";
    const NUM_GRAPHS: u64 = 10;

    stream::iter((0..size).map(|i| {
        let subject =
            NamedOrBlankNode::NamedNode(NamedNode::new_unchecked(format!("{}subject/{}", EX, i)));
        let predicate = NamedNode::new_unchecked(format!("{}predicate/{}", EX, i % 100));
        let object = Term::NamedNode(NamedNode::new_unchecked(format!("{}object/{}", EX, i % 50)));
        let graph = GraphName::NamedNode(NamedNode::new_unchecked(format!(
            "{}graph/{}",
            EX,
            (i as u64) % NUM_GRAPHS
        )));

        Ok(RawQuad::from_quad(&Quad::new(
            subject, predicate, object, graph,
        )))
    }))
}

/// Like [`generate_rdf_data_stream`], but with literal objects — plain,
/// language-tagged, typed, and escape-bearing (quotes, backslashes, newlines,
/// and `"@`/`^^` lookalikes inside the value). The star dataset above is
/// named-node-only, so this is what lets a benchmark reach the literal
/// escape/unescape paths at all.
pub fn generate_literal_rdf_data_stream(size: usize) -> impl Stream<Item = Result<RawQuad>> {
    const EX: &str = "http://example.org/";

    stream::iter((0..size).map(|i| {
        let subject =
            NamedOrBlankNode::NamedNode(NamedNode::new_unchecked(format!("{}subject/{}", EX, i)));
        let predicate = NamedNode::new_unchecked(format!("{}predicate/{}", EX, i % 100));
        let object = Term::Literal(match i % 4 {
            0 => Literal::new_simple_literal(format!("plain value {}", i % 50)),
            1 => Literal::new_language_tagged_literal_unchecked(
                format!("tagged value {}", i % 50),
                "en",
            ),
            2 => Literal::new_typed_literal(
                format!("{}", i % 50),
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#integer"),
            ),
            _ => Literal::new_simple_literal(format!(
                "say \"hi\"@home ^^ line\nbreak back\\slash {}",
                i % 50
            )),
        });

        Ok(RawQuad::from_quad(&Quad::new(
            subject,
            predicate,
            object,
            GraphName::DefaultGraph,
        )))
    }))
}

/// The literal-bearing store for `decode_all_literals`: Dictionary layout, no
/// index — the config whose full decode term-decodes every row. Cached and
/// handed out like the star stores: only the ingest product is cached, and
/// each call rebuilds a fresh store from it (see
/// [`cached_store`] for why shared clones are a contamination hazard).
pub fn cached_literal_store(size: usize) -> VortexRdfStore {
    static CACHE: OnceLock<Mutex<HashMap<usize, BuiltArray>>> = OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let built = if let Some(built) = cache.lock().unwrap().get(&size) {
        built.clone()
    } else {
        let built = rt().block_on(async move {
            SortedStreamBuilder::build_vortex_array(
                Box::new(generate_literal_rdf_data_stream(size)),
                LayoutStrategy::Dictionary,
                Vec::new(),
            )
            .await
            .expect("failed to build literal array")
        });
        cache.lock().unwrap().insert(size, built.clone());
        built
    };
    VortexRdfStore::from_built(built).expect("failed to build literal store")
}
