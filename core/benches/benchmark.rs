//! Benchmark suite for `vortex-rdf-core`.
//!
//! # Design: a star (one-factor-at-a-time) layout, not a full factorial
//!
//! The library exposes four independent axes — builder strategy, layout,
//! secondary index, and source (file vs in-memory) — plus a query pattern with
//! 15 shapes. Their full cross product is ~2,400 match instances, most of which
//! measure the *same* code path: at query time both sorted builders emit
//! identically stamped columns (the store reads only the `IsSorted` stat, so it
//! cannot tell `SortedInMemory` from `SortedStream`), a bound subject always
//! declines every secondary index in favour of the primary sorted `s` column,
//! and a bound graph never routes through an index at all. Measuring those
//! combinations three times over buys no signal and bloats CodSpeed.
//!
//! Instead we fix a baseline and vary one axis at a time, adding back only the
//! interactions that genuinely change behaviour (e.g. Dictionary × index, where
//! the index columns hold u32 codes rather than term strings). Each group below
//! documents its baseline and which axis it sweeps.
//!
//! ## Query patterns, reduced to routing classes
//!
//! The 15 pattern shapes collapse to the six the resolver actually branches on:
//! `S` (primary binary search), `P` and `O` (single-column index probes), `PO`
//! (the two-column family prefix probe — `SecondaryByCopy`'s distinguishing
//! capability), `G` (no index covers graph, so the mask-scan / pushdown
//! fallback), and `SPOG` (every component bound — maximum residual filtering).
//!
//! ## Selectivity of the generated data (`generate_rdf_data_stream`)
//!
//! Subjects are unique; predicates repeat every 100 rows; objects every 50;
//! graphs every 10. Probe terms are chosen to hit rows that actually exist, so
//! at `BENCH_SIZE = 100_000` the matched-row counts are: `S`→1, `P`→1,000,
//! `O`→2,000, `PO`→1,000, `G`→10,000, `SPOG`→1. (The previous suite probed a
//! graph term — `.../graph` — that the generator never emits, so every
//! graph-bound benchmark silently matched zero rows.)

use std::collections::HashMap;
use std::fmt;
use std::hint::black_box;
use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};

use futures::{StreamExt, TryStreamExt, stream};
use oxrdf::{GraphName, NamedNode, NamedOrBlankNode, Quad, Term};
use tokio::runtime::Runtime;
use vortex_array::ArrayRef;

use vortex_rdf_core::common::utils::generate_rdf_data_stream;
use vortex_rdf_core::{
    IndexType, LayoutStrategy, SortedInMemoryBuilder, SortedStreamBuilder, UnsortedStreamBuilder,
    VortexRdfError, VortexRdfStore, io,
};

fn main() {
    divan::main();
}

/// Single dataset size for the whole suite. In simulation mode CodSpeed counts
/// instructions deterministically, so one representative size catches
/// regressions in every path; larger sizes only multiply valgrind cost without
/// adding signal (CodSpeed does not analyse scaling curves). Tune here.
const BENCH_SIZE: usize = 100_000;

// ── shared tokio runtime ────────────────────────────────────────────────────

static TOKIO_RUNTIME: OnceLock<Runtime> = OnceLock::new();

fn rt() -> &'static Runtime {
    TOKIO_RUNTIME.get_or_init(|| Runtime::new().unwrap())
}

// ── configuration axes ──────────────────────────────────────────────────────

#[derive(Copy, Clone, PartialEq, Eq, Hash)]
enum Builder {
    Unsorted,
    SortedInMemory,
    SortedStream,
}

impl Builder {
    fn short(self) -> &'static str {
        match self {
            Self::Unsorted => "unsorted",
            Self::SortedInMemory => "sorted_in_memory",
            Self::SortedStream => "sorted_stream",
        }
    }
}

#[derive(Copy, Clone, PartialEq, Eq, Hash)]
enum Layout {
    Default,
    TypedObject,
    Dictionary,
}

impl Layout {
    fn strategy(self) -> LayoutStrategy {
        match self {
            Self::Default => LayoutStrategy::Default,
            Self::TypedObject => LayoutStrategy::TypedObject,
            Self::Dictionary => LayoutStrategy::Dictionary,
        }
    }
    fn short(self) -> &'static str {
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
enum Index {
    None,
    ByReference,
    ByCopy,
}

impl Index {
    fn types(self) -> Vec<IndexType> {
        match self {
            Self::None => vec![],
            Self::ByReference => vec![IndexType::SecondaryByReference],
            Self::ByCopy => vec![IndexType::SecondaryByCopy],
        }
    }
    fn short(self) -> &'static str {
        match self {
            Self::None => "no_index",
            Self::ByReference => "by_reference",
            Self::ByCopy => "by_copy",
        }
    }
}

#[derive(Copy, Clone, PartialEq, Eq, Hash)]
enum Source {
    File,
    InMemory,
}

impl Source {
    fn short(self) -> &'static str {
        match self {
            Self::File => "file",
            Self::InMemory => "in_memory",
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
fn materialize_quads(size: usize) -> Vec<Quad> {
    rt().block_on(async move {
        generate_rdf_data_stream(size)
            .map(|q| q.expect("quad generation is infallible"))
            .collect()
            .await
    })
}

/// Build the in-memory Vortex array for a config, dispatching the generic
/// builder on the runtime `Builder` enum.
fn build_array(builder: Builder, layout: Layout, index: Index, size: usize) -> ArrayRef {
    rt().block_on(async move {
        let stream = generate_rdf_data_stream(size);
        let strategy = layout.strategy();
        let indexes = index.types();
        match builder {
            Builder::Unsorted => {
                VortexRdfStore::build_vortex_array_with_builder::<UnsortedStreamBuilder>(
                    stream, strategy, indexes,
                )
                .await
            }
            Builder::SortedInMemory => {
                VortexRdfStore::build_vortex_array_with_builder::<SortedInMemoryBuilder>(
                    stream, strategy, indexes,
                )
                .await
            }
            Builder::SortedStream => {
                VortexRdfStore::build_vortex_array_with_builder::<SortedStreamBuilder>(
                    stream, strategy, indexes,
                )
                .await
            }
        }
        .expect("failed to build vortex array")
    })
}

type CacheKey = (Builder, Layout, Index, usize);

/// Cache of built in-memory arrays. Under the star design only a handful of
/// distinct configs are ever requested, so this stays naturally bounded (unlike
/// the old full-factorial cache, which held every combination for the process
/// lifetime).
static ARRAY_CACHE: OnceLock<Mutex<HashMap<CacheKey, ArrayRef>>> = OnceLock::new();
static FILE_CACHE: OnceLock<Mutex<HashMap<CacheKey, PathBuf>>> = OnceLock::new();
static IPC_CACHE: OnceLock<Mutex<HashMap<CacheKey, Vec<u8>>>> = OnceLock::new();

fn cached_array(builder: Builder, layout: Layout, index: Index, size: usize) -> ArrayRef {
    let cache = ARRAY_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let key = (builder, layout, index, size);
    if let Some(arr) = cache.lock().unwrap().get(&key) {
        return arr.clone();
    }
    let arr = build_array(builder, layout, index, size);
    cache.lock().unwrap().insert(key, arr.clone());
    arr
}

fn cached_file(builder: Builder, layout: Layout, index: Index, size: usize) -> PathBuf {
    let cache = FILE_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let key = (builder, layout, index, size);
    if let Some(path) = cache.lock().unwrap().get(&key) {
        return path.clone();
    }
    let arr = cached_array(builder, layout, index, size);
    let dir = PathBuf::from("target/bench_vortex_files");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(format!(
        "{}_{}_{}_{}.vortex",
        builder.short(),
        layout.short(),
        index.short(),
        size
    ));
    rt().block_on(async {
        let writer = tokio::fs::File::create(&path).await.expect("create file");
        io::serialize(arr, writer).await.expect("serialize file");
    });
    cache.lock().unwrap().insert(key, path.clone());
    path
}

fn cached_ipc_bytes(builder: Builder, layout: Layout, index: Index, size: usize) -> Vec<u8> {
    let cache = IPC_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let key = (builder, layout, index, size);
    if let Some(bytes) = cache.lock().unwrap().get(&key) {
        return bytes.clone();
    }
    let arr = cached_array(builder, layout, index, size);
    let mut buf = Vec::new();
    io::write_array_to_ipc(arr, &mut buf).expect("write ipc");
    cache.lock().unwrap().insert(key, buf.clone());
    buf
}

/// Construct a store over a config's data, from the requested source. Both are
/// untimed: `from_file` reads the footer only, and `new` wraps a cached
/// (Arc-shared) array.
fn make_store(
    source: Source,
    builder: Builder,
    layout: Layout,
    index: Index,
    size: usize,
) -> VortexRdfStore {
    match source {
        Source::File => {
            let path = cached_file(builder, layout, index, size);
            rt().block_on(async {
                VortexRdfStore::from_file(path)
                    .await
                    .expect("open file store")
            })
        }
        Source::InMemory => VortexRdfStore::new(cached_array(builder, layout, index, size))
            .expect("build in-memory store"),
    }
}

// ══════════════════════════════════════════════════════════════════════════
// Group 1 — SERIALIZE (write path)
//
// The write path is the one place all three axes genuinely differ, so we vary
// them one at a time around a `sorted_stream / default / no_index` baseline and
// add the two real interactions (Dictionary encodes the index as codes; an
// unsorted builder leaves the index columns unstamped, unlike a sorted one).
// ══════════════════════════════════════════════════════════════════════════

#[derive(Copy, Clone)]
struct SerCfg {
    builder: Builder,
    layout: Layout,
    index: Index,
}

impl fmt::Debug for SerCfg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}_{}_{}",
            self.builder.short(),
            self.layout.short(),
            self.index.short()
        )
    }
}

const SERIALIZE_CONFIGS: &[SerCfg] = &[
    // Builder axis (Default layout, no index).
    SerCfg {
        builder: Builder::Unsorted,
        layout: Layout::Default,
        index: Index::None,
    },
    SerCfg {
        builder: Builder::SortedInMemory,
        layout: Layout::Default,
        index: Index::None,
    },
    SerCfg {
        builder: Builder::SortedStream,
        layout: Layout::Default,
        index: Index::None,
    }, // baseline
    // Layout axis (SortedStream, no index).
    SerCfg {
        builder: Builder::SortedStream,
        layout: Layout::TypedObject,
        index: Index::None,
    },
    SerCfg {
        builder: Builder::SortedStream,
        layout: Layout::Dictionary,
        index: Index::None,
    },
    // Index axis (SortedStream, Default layout).
    SerCfg {
        builder: Builder::SortedStream,
        layout: Layout::Default,
        index: Index::ByReference,
    },
    SerCfg {
        builder: Builder::SortedStream,
        layout: Layout::Default,
        index: Index::ByCopy,
    },
    // Interactions worth keeping: index columns as dictionary codes, and an
    // unsorted (per-chunk, unstamped) index vs the sorted global one above.
    SerCfg {
        builder: Builder::SortedStream,
        layout: Layout::Dictionary,
        index: Index::ByCopy,
    },
    SerCfg {
        builder: Builder::Unsorted,
        layout: Layout::Default,
        index: Index::ByCopy,
    },
];

#[divan::bench(args = SERIALIZE_CONFIGS)]
fn serialize(bencher: divan::Bencher, cfg: &SerCfg) {
    let cfg = *cfg;
    bencher
        .with_inputs(|| materialize_quads(BENCH_SIZE))
        .bench_values(|quads| {
            rt().block_on(async move {
                let mut buf = Vec::new();
                let stream = stream::iter(quads.into_iter().map(Ok::<_, VortexRdfError>));
                match cfg.builder {
                    Builder::Unsorted => io::quads_stream_to_vortex_writer_with_builder::<
                        UnsortedStreamBuilder,
                        _,
                        _,
                    >(
                        stream,
                        &mut buf,
                        cfg.layout.strategy(),
                        cfg.index.types(),
                    )
                    .await,
                    Builder::SortedInMemory => io::quads_stream_to_vortex_writer_with_builder::<
                        SortedInMemoryBuilder,
                        _,
                        _,
                    >(
                        stream,
                        &mut buf,
                        cfg.layout.strategy(),
                        cfg.index.types(),
                    )
                    .await,
                    Builder::SortedStream => {
                        io::quads_stream_to_vortex_writer_with_builder::<SortedStreamBuilder, _, _>(
                            stream,
                            &mut buf,
                            cfg.layout.strategy(),
                            cfg.index.types(),
                        )
                        .await
                    }
                }
                .expect("serialize failed");
                black_box(buf.len())
            })
        });
}

// ══════════════════════════════════════════════════════════════════════════
// Group 2 — MATCH (query path)
//
// Baseline: sorted_stream / default / by_copy / file. Each config sweeps the
// six routing patterns. We use SortedStream as the "sorted" representative and
// UnsortedStream as the "unsorted" one; SortedInMemory is omitted because it is
// query-indistinguishable from SortedStream (identical stamped columns).
// ══════════════════════════════════════════════════════════════════════════

// Each variant names the bound components by letter (Subject/Predicate/
// Object/Graph), so `SPOG` is consistent with its siblings, not a word to
// re-case.
#[allow(clippy::upper_case_acronyms)]
#[derive(Copy, Clone, Debug)]
enum Pattern {
    S,
    P,
    O,
    PO,
    G,
    SPOG,
}

const PATTERNS: &[Pattern] = &[
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
fn terms_for(
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

/// Run one match config across a pattern: build the store once (untimed), then
/// time `match_pattern` plus materialization of the matched quads (so the lazy
/// derived view is actually executed).
fn run_match(
    bencher: divan::Bencher,
    builder: Builder,
    layout: Layout,
    index: Index,
    source: Source,
    pattern: Pattern,
) {
    bencher
        .with_inputs(|| make_store(source, builder, layout, index, BENCH_SIZE))
        .bench_refs(|store| {
            let (s, p, o, g) = terms_for(pattern);
            rt().block_on(async {
                let matched = store
                    .match_pattern(s.as_ref(), p.as_ref(), o.as_ref(), g.as_ref())
                    .await
                    .expect("match_pattern failed");
                let quads: Vec<_> = matched
                    .quads()
                    .expect("quad stream")
                    .try_collect()
                    .await
                    .expect("execute match");
                black_box(quads)
            })
        });
}

macro_rules! match_bench {
    ($name:ident, $builder:expr, $layout:expr, $index:expr, $source:expr) => {
        #[divan::bench(args = PATTERNS)]
        fn $name(bencher: divan::Bencher, pattern: &Pattern) {
            run_match(bencher, $builder, $layout, $index, $source, *pattern);
        }
    };
}

// Baseline + source axis.
match_bench!(
    match_sorted_default_bycopy_file,
    Builder::SortedStream,
    Layout::Default,
    Index::ByCopy,
    Source::File
);
match_bench!(
    match_sorted_default_bycopy_mem,
    Builder::SortedStream,
    Layout::Default,
    Index::ByCopy,
    Source::InMemory
);
// Layout axis (file).
match_bench!(
    match_sorted_typedobj_bycopy_file,
    Builder::SortedStream,
    Layout::TypedObject,
    Index::ByCopy,
    Source::File
);
match_bench!(
    match_sorted_dict_bycopy_file,
    Builder::SortedStream,
    Layout::Dictionary,
    Index::ByCopy,
    Source::File
);
// Index axis (file).
match_bench!(
    match_sorted_default_noindex_file,
    Builder::SortedStream,
    Layout::Default,
    Index::None,
    Source::File
);
match_bench!(
    match_sorted_default_byref_file,
    Builder::SortedStream,
    Layout::Default,
    Index::ByReference,
    Source::File
);
// Sortedness axis: unsorted builder leaves nothing stamped, so indexes decline
// and everything falls to the mask scan — the worst case, and the typical
// in-memory (JS bindings) case.
match_bench!(
    match_unsorted_default_bycopy_file,
    Builder::Unsorted,
    Layout::Default,
    Index::ByCopy,
    Source::File
);
match_bench!(
    match_unsorted_default_bycopy_mem,
    Builder::Unsorted,
    Layout::Default,
    Index::ByCopy,
    Source::InMemory
);

/// Chained refinement: `match_pattern(P)` then `match_pattern(O)` on the
/// resulting view — the headline "views narrow the same coordinate space"
/// feature, which no single-pattern benchmark exercises.
#[divan::bench(args = [Source::File, Source::InMemory])]
fn match_chained(bencher: divan::Bencher, source: &Source) {
    let source = *source;
    bencher
        .with_inputs(|| {
            make_store(
                source,
                Builder::SortedStream,
                Layout::Default,
                Index::ByCopy,
                BENCH_SIZE,
            )
        })
        .bench_refs(|store| {
            let p = NamedNode::new_unchecked("http://example.org/predicate/0");
            let o = Term::NamedNode(NamedNode::new_unchecked("http://example.org/object/0"));
            rt().block_on(async {
                let after_p = store
                    .match_pattern(None, Some(&p), None, None)
                    .await
                    .expect("match P");
                let after_po = after_p
                    .match_pattern(None, None, Some(&o), None)
                    .await
                    .expect("match O on view");
                let quads: Vec<_> = after_po
                    .quads()
                    .expect("quad stream")
                    .try_collect()
                    .await
                    .expect("execute chained match");
                black_box(quads)
            })
        });
}

// ══════════════════════════════════════════════════════════════════════════
// Group 3 — DECODE / LOAD (read-back path)
//
// The full-scan decode is the single most fundamental read and was entirely
// unbenchmarked. It is where layouts diverge most: Dictionary decodes codes to
// terms, TypedObject reassembles the object from four columns. Load costs
// (opening a file, decoding IPC) were previously hidden in untimed setup.
// ══════════════════════════════════════════════════════════════════════════

#[derive(Copy, Clone)]
struct DecodeCfg {
    layout: Layout,
    source: Source,
}

impl fmt::Debug for DecodeCfg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}_{}", self.layout.short(), self.source.short())
    }
}

const DECODE_CONFIGS: &[DecodeCfg] = &[
    DecodeCfg {
        layout: Layout::Default,
        source: Source::File,
    }, // baseline full scan
    DecodeCfg {
        layout: Layout::TypedObject,
        source: Source::File,
    }, // object reassembly
    DecodeCfg {
        layout: Layout::Dictionary,
        source: Source::File,
    }, // code → term
    DecodeCfg {
        layout: Layout::Default,
        source: Source::InMemory,
    }, // in-memory decode path
];

/// Decode every quad in the store (`quads()` → `Vec`). Index is irrelevant to a
/// full scan, so it is fixed to `None`.
#[divan::bench(args = DECODE_CONFIGS)]
fn decode_all(bencher: divan::Bencher, cfg: &DecodeCfg) {
    let cfg = *cfg;
    bencher
        .with_inputs(|| {
            make_store(
                cfg.source,
                Builder::SortedStream,
                cfg.layout,
                Index::None,
                BENCH_SIZE,
            )
        })
        .bench_refs(|store| {
            rt().block_on(async {
                let quads: Vec<_> = store
                    .quads()
                    .expect("quad stream")
                    .try_collect()
                    .await
                    .expect("decode all");
                black_box(quads.len())
            })
        });
}

/// Open a file-backed store. Default reads the footer only; Dictionary also
/// reads its term dictionary up front (an extra single-column scan), so the two
/// are worth distinguishing.
#[divan::bench(args = [Layout::Default, Layout::Dictionary])]
fn open_file(bencher: divan::Bencher, layout: &Layout) {
    let layout = *layout;
    bencher
        .with_inputs(|| cached_file(Builder::SortedStream, layout, Index::None, BENCH_SIZE))
        .bench_refs(|path| {
            rt().block_on(async {
                let store = VortexRdfStore::from_file(path).await.expect("open file");
                black_box(store.layout())
            })
        });
}

/// Load a store from IPC bytes (`from_bytes`): full in-memory IPC decode plus
/// layout detection.
#[divan::bench]
fn from_bytes(bencher: divan::Bencher) {
    bencher
        .with_inputs(|| {
            cached_ipc_bytes(
                Builder::SortedStream,
                Layout::Default,
                Index::None,
                BENCH_SIZE,
            )
        })
        .bench_refs(|bytes| {
            rt().block_on(async {
                let store = VortexRdfStore::from_bytes(bytes).await.expect("from_bytes");
                black_box(store)
            })
        });
}
