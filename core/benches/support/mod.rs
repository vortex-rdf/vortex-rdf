//! Infrastructure shared by the bench targets (`benchmark.rs`, the CodSpeed
//! suite, and `match_lazy.rs`, the local-only lazy-match matrix): the axis
//! enums, dataset/store/file construction with their caches, and the match
//! drivers. Compiled once per target; the caches are per-process, so each
//! target builds its own artifacts.

use std::collections::HashMap;
use std::fmt;
use std::hash::Hash;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};

use arrow_array::RecordBatch;
use futures::{Stream, StreamExt, TryStreamExt, stream};
use oxrdf::{GraphName, Literal, NamedNode, NamedOrBlankNode, Quad, Term};

use vortex_rdf_core::{
    BuiltArray, ExportOptions, IndexType, LayoutStrategy, RawQuad, Result, SortedStreamBuilder,
    TermEncoding, VortexArrayBuilder, VortexRdfStore,
};

// The dataset shape — moduli, term spellings, probe patterns — lives on its own
// so `compare.rs` can include just that file (`#[path = "support/dataset.rs"]`)
// without the store-building machinery it has no use for.
pub mod dataset;
pub use dataset::{PATTERNS, Pattern, terms_for};

mod runtime;
pub use runtime::rt;

/// Rows per generated dataset. Default 32,768 = `CODSPEED_BENCH_DIM`³ (32,
/// shared with `js/bench/codspeed.bench.ts` and `python/bench/test_codspeed.py`)
/// and 4 zones of 8,192 rows, the smallest size at which zone pruning has
/// anything to prune. `BENCH_SIZE` overrides it (`BENCH_SIZE=1048576` is the
/// comparative suites' default, which the dashboard workflow runs).
pub fn bench_size() -> usize {
    static SIZE: OnceLock<usize> = OnceLock::new();
    *SIZE.get_or_init(|| {
        std::env::var("BENCH_SIZE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(32_768)
    })
}

/// Samples per query benchmark — the repetition count every comparative suite
/// uses for a query (`QUERY_OPTS.iterations` in `js/bench/shared.ts`,
/// `QUERY_ITERS` in `python/bench/worker.py`), so one number describes a
/// repetition across all three environments. No effect under CodSpeed, which
/// measures one invocation per case.
pub const QUERY_SAMPLES: u32 = 10;

/// Samples per heavy benchmark (ingest, full decode, open, serialize) — the
/// comparative suites' heavy iteration count (`HEAVY_OPTS`, `HEAVY_ITERS`).
pub const HEAVY_SAMPLES: u32 = 3;

// ── configuration axes ──────────────────────────────────────────────────────

/// Bench axis: the store layout the rows are encoded with.
#[derive(Copy, Clone, PartialEq, Eq, Hash)]
pub enum Layout {
    Default,
    TypedObject,
    Dictionary,
}

impl Layout {
    /// The core's strategy for this layout.
    pub fn strategy(self) -> LayoutStrategy {
        match self {
            Self::Default => LayoutStrategy::Default,
            Self::TypedObject => LayoutStrategy::TypedObject,
            Self::Dictionary => LayoutStrategy::Dictionary,
        }
    }
    /// The spelling used in artifact file names and `Debug` output.
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

/// Bench axis: the secondary index the store carries.
#[derive(Copy, Clone, PartialEq, Eq, Hash)]
pub enum Index {
    None,
    ByReference,
    ByCopy,
}

impl Index {
    /// The index types to build for this axis value.
    pub fn types(self) -> Vec<IndexType> {
        match self {
            Self::None => vec![],
            Self::ByReference => vec![IndexType::SecondaryByReference],
            Self::ByCopy => vec![IndexType::SecondaryByCopy],
        }
    }
    /// The spelling used in artifact file names and `Debug` output.
    pub fn short(self) -> &'static str {
        match self {
            Self::None => "no_index",
            Self::ByReference => "by_reference",
            Self::ByCopy => "by_copy",
        }
    }
}

/// Bench axis: where the store's data is served from.
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
    /// The spelling used in artifact file names and `Debug` output.
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

/// Run a quad stream's ingest to the builder's `BuiltArray`: the quad array
/// plus whatever travels beside it — under the Dictionary layout the array
/// holds only u32 codes and the term dictionary rides alongside; `from_built`
/// is the one constructor that accepts that pair.
fn ingest(
    quads: impl Stream<Item = Result<RawQuad>> + Unpin + Send + 'static,
    layout: LayoutStrategy,
    indexes: Vec<IndexType>,
) -> BuiltArray {
    rt().block_on(async move {
        SortedStreamBuilder::build_vortex_array(Box::new(quads), layout, indexes)
            .await
            .expect("failed to build vortex array")
    })
}

/// Get-or-build memo over a process-wide cache: the first call for `key` runs
/// `build` and stores its product, later calls clone the stored product.
pub fn memoized<K: Eq + Hash + Clone, V: Clone>(
    cache: &'static OnceLock<Mutex<HashMap<K, V>>>,
    key: K,
    build: impl FnOnce() -> V,
) -> V {
    let cache = cache.get_or_init(|| Mutex::new(HashMap::new()));
    if let Some(v) = cache.lock().unwrap().get(&key) {
        return v.clone();
    }
    let v = build();
    cache.lock().unwrap().insert(key, v.clone());
    v
}

type CacheKey = (Layout, Index, usize);

/// Cache of ingest products (`BuiltArray`, cheaply cloneable: Arc-shared
/// buffers). Only the expensive ingest is cached — stores are rebuilt per
/// handout, see [`cached_store`]. Only a handful of distinct configs are ever
/// requested, so this stays naturally bounded.
static INGEST_CACHE: OnceLock<Mutex<HashMap<CacheKey, BuiltArray>>> = OnceLock::new();
static FILE_CACHE: OnceLock<Mutex<HashMap<CacheKey, PathBuf>>> = OnceLock::new();
// `Arc`-wrapped so a cache hit hands out a shared buffer.
static FILE_BYTES_CACHE: OnceLock<Mutex<HashMap<CacheKey, Arc<Vec<u8>>>>> = OnceLock::new();

fn cached_ingest(layout: Layout, index: Index, size: usize) -> BuiltArray {
    memoized(&INGEST_CACHE, (layout, index, size), || {
        ingest(
            generate_rdf_data_stream(size),
            layout.strategy(),
            index.types(),
        )
    })
}

/// A fresh store over a config's cached ingest product: `from_built` re-runs
/// component adoption and store construction per call, so every handout starts
/// cold and order-independent — nothing an earlier benchmark warms on one store
/// is visible to the next. The Arc'd buffers, and vortex's interior-mutable
/// per-array stats riding on them, stay shared with the cache.
pub fn cached_store(layout: Layout, index: Index, size: usize) -> VortexRdfStore {
    VortexRdfStore::from_built(cached_ingest(layout, index, size))
        .expect("failed to build vortex store")
}

/// The serialized store file for a config (`to_bytes` of its cached store),
/// written once per process under `target/bench_vortex_files`.
pub fn cached_file(layout: Layout, index: Index, size: usize) -> PathBuf {
    memoized(&FILE_CACHE, (layout, index, size), || {
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
        path
    })
}

/// The bytes of [`cached_file`] for a config, read once per process.
pub fn cached_bytes(layout: Layout, index: Index, size: usize) -> Arc<Vec<u8>> {
    memoized(&FILE_BYTES_CACHE, (layout, index, size), || {
        Arc::new(std::fs::read(cached_file(layout, index, size)).expect("read serialized store"))
    })
}

/// Construct a store over a config's data from the requested source, all
/// untimed: `File` opens the cached file (footer read only), `InMemory`
/// rebuilds a fresh store from the cached ingest product (see
/// [`cached_store`]), `Bytes` copies the cached file's bytes and adopts them
/// through `from_bytes_owned`.
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
            let bytes = Vec::clone(&cached_bytes(layout, index, size));
            rt().block_on(async {
                VortexRdfStore::from_bytes_owned(bytes)
                    .await
                    .expect("adopt bytes store")
            })
        }
    }
}

// ── match drivers ───────────────────────────────────────────────────────────

/// The cache regime a match cell runs in.
#[derive(Copy, Clone)]
pub enum Regime {
    /// Each sample gets a store built fresh (`with_inputs`, untimed) and
    /// answers its first query on it — no chunk probes resolved, no segment
    /// cache, no dictionary memos, and (for `Source::InMemory`) no component
    /// adoption yet run. Process-level cold, not storage-level: the OS page
    /// cache stays warm.
    Cold,
    /// One store, reused across every iteration after one untimed priming
    /// query (match and materialize), so each measurement is a repeat query
    /// against populated caches — the steady state a long-lived process
    /// reaches.
    Warm,
}

/// One match cell: `match_pattern` for `pattern` on a store of the given
/// config, in `regime`. With `materialize` the timed region also decodes the
/// matched quads (`quads_vec`); without it only resolution and selection
/// composition are timed. The product is returned from the timed closure, so
/// its drop is not measured.
///
/// Opening a store is never inside the measurement — it is its own benchmark
/// (`open_file`). The warm priming query also checks the matched row count
/// against the dataset's declared selectivity.
pub fn run_match(
    bencher: divan::Bencher,
    layout: Layout,
    index: Index,
    source: Source,
    pattern: Pattern,
    regime: Regime,
    materialize: bool,
) {
    // Probe terms are built outside the timed closure. Both timed closures
    // return their product so the bencher drops it outside the measurement.
    let (s, p, o, g) = terms_for(pattern);
    let lazy = |store: &VortexRdfStore| {
        rt().block_on(async {
            store
                .match_pattern(s.as_ref(), p.as_ref(), o.as_ref(), g.as_ref())
                .await
                .expect("match_pattern failed")
        })
    };
    let full = |store: &VortexRdfStore| {
        rt().block_on(async {
            store
                .match_pattern(s.as_ref(), p.as_ref(), o.as_ref(), g.as_ref())
                .await
                .expect("match_pattern failed")
                .quads_vec()
                .await
                .expect("execute match")
        })
    };
    let fresh = || make_store(source, layout, index, bench_size());
    match (regime, materialize) {
        (Regime::Cold, true) => bencher.with_inputs(fresh).bench_refs(|store| full(store)),
        (Regime::Cold, false) => bencher.with_inputs(fresh).bench_refs(|store| lazy(store)),
        (Regime::Warm, materialize) => {
            let store = fresh();
            let quads = full(&store);
            bench_moduli().assert_matched(bench_size(), pattern, quads.len());
            drop(quads);
            if materialize {
                bencher.bench(|| full(&store));
            } else {
                bencher.bench(|| lazy(&store));
            }
        }
    }
}

/// One Arrow access cell: `match_pattern` for `pattern`, then the matched view
/// exported as record batches under `encoding`. The warm twin of [`run_match`]
/// — same store config, same probe, same priming and selectivity check — so a
/// cell here and the `match_warm_*` cell beside it differ only in how the
/// matched rows are handed out: per-row quads there, columnar batches here.
///
/// The batches are collected and returned from the timed closure, so the
/// measurement covers producing them and not their drop, exactly as
/// `quads_vec` is measured. Under `Strings` that production decodes every
/// term, which is the same work `quads_vec` does; under `Codes` it hands out
/// the `u32` columns a query engine joins on, which is the point of the
/// comparison.
pub fn run_match_arrow(
    bencher: divan::Bencher,
    layout: Layout,
    index: Index,
    source: Source,
    pattern: Pattern,
    encoding: TermEncoding,
) {
    let (s, p, o, g) = terms_for(pattern);
    let export = |store: &VortexRdfStore| {
        rt().block_on(async {
            let view = store
                .match_pattern(s.as_ref(), p.as_ref(), o.as_ref(), g.as_ref())
                .await
                .expect("match_pattern failed");
            let batches: Vec<RecordBatch> = view
                .to_record_batches(&ExportOptions::new(encoding))
                .await
                .expect("export batches")
                .try_collect()
                .await
                .expect("collect batches");
            batches
        })
    };
    let store = make_store(source, layout, index, bench_size());
    let primed = export(&store);
    bench_moduli().assert_matched(
        bench_size(),
        pattern,
        primed.iter().map(RecordBatch::num_rows).sum(),
    );
    drop(primed);
    bencher.bench(|| export(&store));
}

/// The layout × index × source match matrix in both cache regimes: one
/// `(layout, index, source) => cold / warm` row per cell, each naming the
/// cell's two benchmark functions. The leading flag is [`run_match`]'s
/// `materialize`.
macro_rules! match_matrix {
    ($materialize:expr; $(($layout:expr, $index:expr, $source:expr) => $cold:ident / $warm:ident,)*) => {
        $(
            #[divan::bench(args = PATTERNS, sample_count = QUERY_SAMPLES)]
            fn $cold(bencher: divan::Bencher, pattern: &Pattern) {
                run_match(bencher, $layout, $index, $source, *pattern, Regime::Cold, $materialize);
            }

            #[divan::bench(args = PATTERNS, sample_count = QUERY_SAMPLES)]
            fn $warm(bencher: divan::Bencher, pattern: &Pattern) {
                run_match(bencher, $layout, $index, $source, *pattern, Regime::Warm, $materialize);
            }
        )*
    };
}

// ── dataset generation ──────────────────────────────────────────────────────

/// The moduli this process's dataset was generated with, at the size
/// [`bench_size`] resolved. Every consumer that needs a term index — the
/// distinct-probe walk, the shape line — derives it from here, because the
/// moduli follow from the row count.
pub fn bench_moduli() -> dataset::Moduli {
    static M: OnceLock<dataset::Moduli> = OnceLock::new();
    *M.get_or_init(|| dataset::moduli(bench_size(), dataset::WANT_GRAPHS))
}

/// Generate the bench dataset as a stream of quads — the rows `compare.rs`'s
/// `ensure_quads_dataset` writes, built in process.
pub fn generate_rdf_data_stream(size: usize) -> impl Stream<Item = Result<RawQuad>> {
    let m = dataset::moduli(size, dataset::WANT_GRAPHS);

    stream::iter((0..size).map(move |i| Ok(RawQuad::from_quad(&dataset::dataset_quad(i, m)))))
}

/// Like [`generate_rdf_data_stream`] but with literal objects of every shape —
/// plain, language-tagged, typed, and escape-bearing (quotes, backslashes,
/// newlines, `"@`/`^^` lookalikes) — in the default graph. Subject and
/// predicate cardinality follow the shared moduli.
fn generate_literal_rdf_data_stream(size: usize) -> impl Stream<Item = Result<RawQuad>> {
    let m = dataset::moduli(size, dataset::WANT_GRAPHS);

    stream::iter((0..size).map(move |i| {
        let subject = NamedOrBlankNode::NamedNode(NamedNode::new_unchecked(dataset::subject_iri(
            i % m.n_subj,
        )));
        let predicate = NamedNode::new_unchecked(dataset::predicate_iri(i % m.n_pred));
        let value = i % m.n_obj;
        let object = Term::Literal(match i % 4 {
            0 => Literal::new_simple_literal(format!("plain value {value}")),
            1 => Literal::new_language_tagged_literal_unchecked(
                format!("tagged value {value}"),
                "en",
            ),
            2 => Literal::new_typed_literal(
                format!("{value}"),
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#integer"),
            ),
            _ => Literal::new_simple_literal(format!(
                "say \"hi\"@home ^^ line\nbreak back\\slash {value}"
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

/// The Dictionary/no-index store over [`generate_literal_rdf_data_stream`];
/// the ingest product is cached per size, the store is rebuilt per call as in
/// [`cached_store`].
pub fn cached_literal_store(size: usize) -> VortexRdfStore {
    static CACHE: OnceLock<Mutex<HashMap<usize, BuiltArray>>> = OnceLock::new();
    let built = memoized(&CACHE, size, || {
        ingest(
            generate_literal_rdf_data_stream(size),
            LayoutStrategy::Dictionary,
            Vec::new(),
        )
    });
    VortexRdfStore::from_built(built).expect("failed to build literal store")
}
