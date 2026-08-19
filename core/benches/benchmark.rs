//! Benchmark suite for `vortex-rdf-core`.
//!
//! # Design: a star write path, a deliberately factorial match matrix
//!
//! The library exposes three independent axes — layout, secondary index, and
//! source (file vs in-memory) — plus a query pattern with 15 shapes. Their
//! full cross product is ~360 match instances; the suite spends its upload
//! budget differently per group:
//!
//! * **Serialize (Group 1)** is a star (one-factor-at-a-time) sweep: most
//!   write-path cross-products measure the same code, so we fix a baseline and
//!   vary one axis at a time, adding back only the interactions that genuinely
//!   change behaviour (e.g. Dictionary × index, where the index columns hold
//!   u32 codes rather than term strings).
//! * **Match (Group 2)** is a full 18-cell layout × index × source factorial
//!   (× 6 routing patterns), plus a chained-view pair. Some cells are
//!   redundant on paper — a bound subject
//!   declines every secondary index in favour of the primary sorted `s`
//!   column, and a bound graph never routes through an index at all — but
//!   index-decline routing is exactly where regressions have hidden, and the
//!   cell names are long-lived CodSpeed baselines, so the matrix is kept
//!   whole.
//! * **Decode/load (Group 3) and dictionary residency (Group 4)** sweep only
//!   the axis each path actually branches on.
//!
//! Each group below documents its baseline and what it sweeps.
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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use futures::stream;
use oxrdf::{GraphName, NamedNode, NamedOrBlankNode, Term};

use vortex_rdf_core::{LayoutStrategy, VortexRdfError, VortexRdfStore, io};

// The module is shared with `match_lazy.rs` and compiled per-target; items
// only the other target uses (the `Bytes` source axis) are dead here by
// design.
#[allow(dead_code)]
mod support;
use support::*;

fn main() {
    divan::main();
}

// The payload is `Arc`-wrapped so a cache hit is a pointer bump, not a
// multi-MB buffer copy per `with_inputs` call around the measured region.
static BYTES_CACHE: OnceLock<Mutex<HashMap<CacheKey, Arc<Vec<u8>>>>> = OnceLock::new();

fn cached_store_bytes(layout: Layout, index: Index, size: usize) -> Arc<Vec<u8>> {
    let cache = BYTES_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let key = (layout, index, size);
    if let Some(bytes) = cache.lock().unwrap().get(&key) {
        return Arc::clone(bytes);
    }
    let store = cached_store(layout, index, size);
    let buf = Arc::new(rt().block_on(store.to_bytes()).expect("store bytes"));
    cache.lock().unwrap().insert(key, Arc::clone(&buf));
    buf
}

// ══════════════════════════════════════════════════════════════════════════
// Group 1 — SERIALIZE (write path)
//
// The write path is the one place all three axes genuinely differ, so we vary
// them one at a time around a `default / no_index` baseline and
// add the one real interaction (Dictionary encodes the index as codes).
// ══════════════════════════════════════════════════════════════════════════

#[derive(Copy, Clone)]
struct SerCfg {
    layout: Layout,
    index: Index,
}

impl fmt::Debug for SerCfg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}_{}", self.layout.short(), self.index.short())
    }
}

const SERIALIZE_CONFIGS: &[SerCfg] = &[
    SerCfg {
        layout: Layout::Default,
        index: Index::None,
    }, // baseline
    // Layout axis (no index).
    SerCfg {
        layout: Layout::TypedObject,
        index: Index::None,
    },
    SerCfg {
        layout: Layout::Dictionary,
        index: Index::None,
    },
    // Index axis (Default layout).
    SerCfg {
        layout: Layout::Default,
        index: Index::ByReference,
    },
    SerCfg {
        layout: Layout::Default,
        index: Index::ByCopy,
    },
    // Interaction worth keeping: index columns as dictionary codes.
    SerCfg {
        layout: Layout::Dictionary,
        index: Index::ByCopy,
    },
];

#[divan::bench(args = SERIALIZE_CONFIGS)]
fn serialize(bencher: divan::Bencher, cfg: &SerCfg) {
    let cfg = *cfg;
    bencher
        .with_inputs(|| materialize_quads(bench_size()))
        .bench_values(|quads| {
            rt().block_on(async move {
                let mut buf = Vec::new();
                let stream = stream::iter(quads.into_iter().map(Ok::<_, VortexRdfError>));
                io::quads_stream_to_vortex_writer(
                    stream,
                    &mut buf,
                    cfg.layout.strategy(),
                    cfg.index.types(),
                )
                .await
                .expect("serialize failed");
                black_box(buf.len())
            })
        });
}

// ══════════════════════════════════════════════════════════════════════════
// Group 2 — MATCH (query path)
//
// Baseline: default / by_copy / file. Each config sweeps the
// six routing patterns. SortedInMemory is omitted because it is
// query-indistinguishable from SortedStream (identical stamped columns).
// ══════════════════════════════════════════════════════════════════════════

/// Run one match config across a pattern: build the store once (untimed), then
/// time `match_pattern` plus materialization of the matched quads (so the lazy
/// derived view is actually executed).
fn run_match(
    bencher: divan::Bencher,
    layout: Layout,
    index: Index,
    source: Source,
    pattern: Pattern,
) {
    // Probe construction stays OUTSIDE the timed closure (as dict_probe_warm's
    // does): its ~4 String allocations per iteration are fixed setup, and at
    // the lazy suite's ~31 µs match floor they are visible noise.
    let (s, p, o, g) = terms_for(pattern);
    bencher
        .with_inputs(|| make_store(source, layout, index, bench_size()))
        .bench_refs(|store| {
            rt().block_on(async {
                let matched = store
                    .match_pattern(s.as_ref(), p.as_ref(), o.as_ref(), g.as_ref())
                    .await
                    .expect("match_pattern failed");
                let quads = matched.quads_vec().await.expect("execute match");
                black_box(quads)
            })
        });
}

macro_rules! match_bench {
    ($name:ident, $layout:expr, $index:expr, $source:expr) => {
        #[divan::bench(args = PATTERNS)]
        fn $name(bencher: divan::Bencher, pattern: &Pattern) {
            run_match(bencher, $layout, $index, $source, *pattern);
        }
    };
}

/// The full layout × source × index match matrix. One group per cell, named
/// `match_{layout}_{index}_{source}`.
macro_rules! match_matrix {
    ($(($layout:expr, $index:expr, $source:expr) => $name:ident,)*) => {
        $(match_bench!($name, $layout, $index, $source);)*
    };
}
match_matrix!(
    // No secondary index.
    (Layout::Default, Index::None, Source::InMemory) => match_default_noindex_mem,
    (Layout::Default, Index::None, Source::File) => match_default_noindex_file,
    (Layout::TypedObject, Index::None, Source::InMemory) => match_typedobj_noindex_mem,
    (Layout::TypedObject, Index::None, Source::File) => match_typedobj_noindex_file,
    (Layout::Dictionary, Index::None, Source::InMemory) => match_dict_noindex_mem,
    (Layout::Dictionary, Index::None, Source::File) => match_dict_noindex_file,
    // Secondary by reference.
    (Layout::Default, Index::ByReference, Source::InMemory) => match_default_byref_mem,
    (Layout::Default, Index::ByReference, Source::File) => match_default_byref_file,
    (Layout::TypedObject, Index::ByReference, Source::InMemory) => match_typedobj_byref_mem,
    (Layout::TypedObject, Index::ByReference, Source::File) => match_typedobj_byref_file,
    (Layout::Dictionary, Index::ByReference, Source::InMemory) => match_dict_byref_mem,
    (Layout::Dictionary, Index::ByReference, Source::File) => match_dict_byref_file,
    // Secondary by copy.
    (Layout::Default, Index::ByCopy, Source::InMemory) => match_default_bycopy_mem,
    (Layout::Default, Index::ByCopy, Source::File) => match_default_bycopy_file,
    (Layout::TypedObject, Index::ByCopy, Source::InMemory) => match_typedobj_bycopy_mem,
    (Layout::TypedObject, Index::ByCopy, Source::File) => match_typedobj_bycopy_file,
    (Layout::Dictionary, Index::ByCopy, Source::InMemory) => match_dict_bycopy_mem,
    (Layout::Dictionary, Index::ByCopy, Source::File) => match_dict_bycopy_file,
);
/// Chained refinement: `match_pattern(P)` then `match_pattern(O)` on the
/// resulting view — the headline "views narrow the same coordinate space"
/// feature, which no single-pattern benchmark exercises.
#[divan::bench(args = [Source::File, Source::InMemory])]
fn match_chained(bencher: divan::Bencher, source: &Source) {
    let source = *source;
    let p = NamedNode::new_unchecked("http://example.org/predicate/0");
    let o = Term::NamedNode(NamedNode::new_unchecked("http://example.org/object/0"));
    bencher
        .with_inputs(|| make_store(source, Layout::Default, Index::ByCopy, bench_size()))
        .bench_refs(|store| {
            rt().block_on(async {
                let after_p = store
                    .match_pattern(None, Some(&p), None, None)
                    .await
                    .expect("match P");
                let after_po = after_p
                    .match_pattern(None, None, Some(&o), None)
                    .await
                    .expect("match O on view");
                let quads = after_po.quads_vec().await.expect("execute chained match");
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
    DecodeCfg {
        layout: Layout::TypedObject,
        source: Source::InMemory,
    }, // in-memory object reassembly
    DecodeCfg {
        layout: Layout::Dictionary,
        source: Source::InMemory,
    }, // in-memory code → term
];

/// Decode every quad in the store (`quads()` → `Vec`). Index is irrelevant to a
/// full scan, so it is fixed to `None`.
#[divan::bench(args = DECODE_CONFIGS)]
fn decode_all(bencher: divan::Bencher, cfg: &DecodeCfg) {
    let cfg = *cfg;
    bencher
        .with_inputs(|| make_store(cfg.source, cfg.layout, Index::None, bench_size()))
        .bench_refs(|store| {
            rt().block_on(async {
                let quads = store.quads_vec().await.expect("decode all");
                black_box(quads.len())
            })
        });
}

/// [`decode_all`] over a literal-bearing dataset (plain, language-tagged,
/// typed, and escape-carrying values — see the generator's doc): the star
/// dataset is named-node-only, so this is the one benchmark whose decode
/// reaches the literal unescape path.
#[divan::bench]
fn decode_all_literals(bencher: divan::Bencher) {
    bencher
        .with_inputs(|| cached_literal_store(bench_size()))
        .bench_refs(|store| {
            rt().block_on(async {
                let quads = store.quads_vec().await.expect("decode literals");
                black_box(quads.len())
            })
        });
}

/// Open a file-backed store. Default and TypedObject read the footer only;
/// Dictionary also reads its term dictionary up front (an extra single-column
/// scan), so the layouts are worth distinguishing.
#[divan::bench(args = [Layout::Default, Layout::TypedObject, Layout::Dictionary])]
fn open_file(bencher: divan::Bencher, layout: &Layout) {
    let layout = *layout;
    bencher
        .with_inputs(|| cached_file(layout, Index::None, bench_size()))
        .bench_refs(|path| {
            rt().block_on(async {
                let store = VortexRdfStore::from_file(path).await.expect("open file");
                black_box(store.layout())
            })
        });
}

/// Load a store from file bytes (`from_bytes`): root-layout validation plus a
/// full in-memory materialization off the buffer-backed file.
#[divan::bench]
fn from_bytes(bencher: divan::Bencher) {
    bencher
        .with_inputs(|| cached_store_bytes(Layout::Default, Index::None, bench_size()))
        .bench_refs(|bytes| {
            rt().block_on(async {
                let store = VortexRdfStore::from_bytes(bytes).await.expect("from_bytes");
                black_box(store)
            })
        });
}

// ══════════════════════════════════════════════════════════════════════════
// Group 4 — DICTIONARY RESIDENCY (file-backed vs resident term dictionary)
//
// A Dictionary file's term dictionary can be lifted resident at open or left
// in its dictionary child and reached by scans through the bounded reader
// (`from_file_with_dict_residency`, byte threshold). The residency axis moves
// cost between phases: resident pays one contiguous child read at open and
// then probes/decodes from memory; file-backed opens on footer metadata alone
// but pays a pruned child scan per cold term probe and a row-index scan per
// decoded chunk.
// ══════════════════════════════════════════════════════════════════════════

#[derive(Copy, Clone, PartialEq, Eq, Hash)]
enum DictResidency {
    Resident,
    FileBacked,
}

impl DictResidency {
    /// The `max_resident_bytes` value that forces this residency.
    fn threshold(self) -> u64 {
        match self {
            Self::Resident => u64::MAX,
            Self::FileBacked => 0,
        }
    }

    fn short(self) -> &'static str {
        match self {
            Self::Resident => "resident",
            Self::FileBacked => "file_backed",
        }
    }
}

impl fmt::Debug for DictResidency {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.short())
    }
}

const DICT_CONFIGS: &[DictResidency] = &[DictResidency::Resident, DictResidency::FileBacked];

static DICT_FILE_CACHE: OnceLock<Mutex<HashMap<usize, PathBuf>>> = OnceLock::new();

/// A Dictionary-layout file (no indexes), built once per size.
fn cached_dict_file(size: usize) -> PathBuf {
    let cache = DICT_FILE_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    if let Some(path) = cache.lock().unwrap().get(&size) {
        return path.clone();
    }
    let dir = PathBuf::from("target/bench_vortex_files");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(format!("dict_{size}.vortex"));
    rt().block_on(async {
        io::quads_stream_to_vortex_file(
            generate_rdf_data_stream(size),
            &path,
            LayoutStrategy::Dictionary,
            Vec::new(),
        )
        .await
        .expect("write dictionary bench file");
    });
    cache.lock().unwrap().insert(size, path.clone());
    path
}

fn open_dict_store(residency: DictResidency, size: usize) -> VortexRdfStore {
    let path = cached_dict_file(size);
    rt().block_on(async {
        VortexRdfStore::from_file_with_dict_residency(&path, residency.threshold())
            .await
            .expect("open dictionary store")
    })
}

/// Open cost across the residency axis: resident pays the child read and
/// dictionary lift, file-backed only the footer reads.
#[divan::bench(args = DICT_CONFIGS)]
fn dict_open(bencher: divan::Bencher, residency: &DictResidency) {
    let residency = *residency;
    bencher
        .with_inputs(|| (cached_dict_file(bench_size()), residency.threshold()))
        .bench_refs(|(path, threshold)| {
            rt().block_on(async {
                let store = VortexRdfStore::from_file_with_dict_residency(&*path, *threshold)
                    .await
                    .expect("open");
                black_box(store.layout())
            })
        });
}

/// Cold term→ID probes: a fully bound pattern (four dictionary probes) on a
/// store opened fresh each iteration, so neither the probe memo nor the
/// file-backed dictionary's chunk cache carries anything over. Resident
/// probes are in-memory binary searches; file-backed ones binary-search the
/// term column through chunk leaves fetched on demand, so this cell prices
/// those first fetches rather than the search over them.
#[divan::bench(args = DICT_CONFIGS)]
fn dict_probe_cold(bencher: divan::Bencher, residency: &DictResidency) {
    let residency = *residency;
    let s = NamedOrBlankNode::NamedNode(NamedNode::new_unchecked("http://example.org/subject/0"));
    let p = NamedNode::new_unchecked("http://example.org/predicate/0");
    let o = Term::NamedNode(NamedNode::new_unchecked("http://example.org/object/0"));
    let g = GraphName::NamedNode(NamedNode::new_unchecked("http://example.org/graph/0"));
    bencher
        .with_inputs(|| open_dict_store(residency, bench_size()))
        .bench_refs(|store| {
            rt().block_on(async {
                let matched = store
                    .match_pattern(Some(&s), Some(&p), Some(&o), Some(&g))
                    .await
                    .expect("match SPOG");
                black_box(matched)
            })
        });
}

/// The same fully bound pattern on one shared store — the steady state of
/// repeated lookups for the *same* terms. After the first iteration the probe
/// memo answers every term on both arms, so this cell prices the match
/// machinery around the dictionary rather than the dictionary itself. The
/// residency axis shows in [`dict_probe_cold`], which pays the chunk fetches,
/// and to a much smaller degree in [`dict_probe_distinct`] — not here.
#[divan::bench(args = DICT_CONFIGS)]
fn dict_probe_warm(bencher: divan::Bencher, residency: &DictResidency) {
    let store = open_dict_store(*residency, bench_size());
    let s = NamedOrBlankNode::NamedNode(NamedNode::new_unchecked("http://example.org/subject/0"));
    let p = NamedNode::new_unchecked("http://example.org/predicate/0");
    let o = Term::NamedNode(NamedNode::new_unchecked("http://example.org/object/0"));
    let g = GraphName::NamedNode(NamedNode::new_unchecked("http://example.org/graph/0"));
    bencher.bench(|| {
        rt().block_on(async {
            let matched = store
                .match_pattern(Some(&s), Some(&p), Some(&o), Some(&g))
                .await
                .expect("match SPOG");
            black_box(matched)
        })
    });
}

/// Term→ID probes that always miss the memo: one shared store, so its chunk
/// cache stays warm, probed with a different subject every iteration. This is
/// the steady state of a query workload over a large term set — distinct
/// lookups against a store that has been open a while — and the cell that
/// prices the search itself: the memo cannot answer it and the chunk fetches
/// are already paid, so what is left is what residency costs a warm binary
/// search. The term is built outside the timed closure, as the other probe
/// cells do.
#[divan::bench(args = DICT_CONFIGS)]
fn dict_probe_distinct(bencher: divan::Bencher, residency: &DictResidency) {
    let store = open_dict_store(*residency, bench_size());
    let subjects = bench_size();
    let next = AtomicUsize::new(0);
    bencher
        .with_inputs(|| {
            // One row per subject in the generated data, so cycling the
            // counter walks distinct terms that all exist.
            let i = next.fetch_add(1, Ordering::Relaxed) % subjects;
            NamedOrBlankNode::NamedNode(NamedNode::new_unchecked(format!(
                "http://example.org/subject/{i}"
            )))
        })
        .bench_refs(|s| {
            rt().block_on(async {
                let matched = store
                    .match_pattern(Some(s), None, None, None)
                    .await
                    .expect("match S");
                black_box(matched)
            })
        });
}

/// Reconstruction of a point result (subject-bound, one row): the chunk's
/// handful of distinct codes stays under the point-read cap, so a file-backed
/// dictionary resolves them by reading exactly those rows out of its cached
/// wire chunks instead of scanning. The bound term is memoized after the
/// first iteration, so this cell prices the decode, not the probe.
#[divan::bench(args = DICT_CONFIGS)]
fn dict_decode_point(bencher: divan::Bencher, residency: &DictResidency) {
    let store = open_dict_store(*residency, bench_size());
    let s = NamedOrBlankNode::NamedNode(NamedNode::new_unchecked("http://example.org/subject/0"));
    bencher.bench(|| {
        rt().block_on(async {
            let matched = store
                .match_pattern(Some(&s), None, None, None)
                .await
                .expect("match S");
            let quads = matched.quads_vec().await.expect("decode point");
            black_box(quads.len())
        })
    });
}

/// Reconstruction of a wide matched subset (predicate-bound, 1% of rows at
/// the default size): resident decodes codes against the in-memory
/// dictionary. The matched chunk holds far more distinct codes than the
/// point-read cap admits, so a file-backed dictionary resolves them with one
/// row-index scan — the bulk path, whose whole-leaf decode is what wins at
/// this width. [`dict_decode_point`] covers the other side of the cap.
#[divan::bench(args = DICT_CONFIGS)]
fn dict_decode_matched(bencher: divan::Bencher, residency: &DictResidency) {
    let store = open_dict_store(*residency, bench_size());
    let p = NamedNode::new_unchecked("http://example.org/predicate/0");
    bencher.bench(|| {
        rt().block_on(async {
            let matched = store
                .match_pattern(None, Some(&p), None, None)
                .await
                .expect("match P");
            let quads = matched.quads_vec().await.expect("decode matched");
            black_box(quads.len())
        })
    });
}
