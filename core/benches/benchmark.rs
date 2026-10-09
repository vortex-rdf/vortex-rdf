//! Benchmark suite for `vortex-rdf-core`.
//!
//! # Design: a star write path, a deliberately factorial match matrix
//!
//! The library exposes three independent axes — layout, secondary index, and
//! source (file vs in-memory) — plus a query pattern with 15 shapes. Their
//! full cross product is far more match instances than an upload budget
//! allows; the suite spends that budget differently per group:
//!
//! * **Serialize (Group 1)** is a star (one-factor-at-a-time) sweep: most
//!   write-path cross-products measure the same code, so we fix a baseline and
//!   vary one axis at a time, adding back only the interactions that genuinely
//!   change behaviour (e.g. Dictionary × index, where the index columns hold
//!   term codes rather than term strings).
//! * **Match (Group 2)** is a full 18-cell layout × index × source factorial
//!   (× 8 routing patterns × 2 cache regimes), plus a chained-view pair. Some
//!   cells are redundant on paper — a bound subject
//!   declines every secondary index in favour of the primary sorted `s`
//!   column, and a bound graph never routes through an index at all — but
//!   index-decline routing is exactly where a regression would go unnoticed,
//!   so the matrix is kept whole.
//!
//!   The regime axis is `match_cold_*` (each sample answers the first query on a
//!   freshly opened store) against `match_warm_*` (one store, reused). Both are
//!   needed: a cold-only suite cannot see caching work at all, and reports an
//!   improvement that only a resolved probe cache delivers as noise. Opening is
//!   never inside either measurement — it is its own benchmark (`open_file`).
//! * **Decode/load (Group 3) and dictionary access (Group 4)** sweep only
//!   the axis each path actually branches on.
//! * **Mutate (Group 5)** sweeps a star of the same three axes, because an
//!   append's presence check and a delete's pattern resolution both route
//!   through `match_pattern` — and separates the three costs a mutation can
//!   carry (accretion, tombstoning, and the compaction that folds a tail back
//!   into the base) into cells of their own.
//!
//! Each group below documents its baseline and what it sweeps.
//!
//! ## Query patterns, reduced to routing classes
//!
//! The 15 pattern shapes collapse to the eight the resolver actually branches
//! on: `S` (primary binary search), `SP` and `SPO` (the prefix probe over the
//! `(s, p, o, g)` order — each further bound role narrows the subject range in
//! place), `P` and `O` (single-column index probes), `PO` (the two-column
//! family prefix probe — `SecondaryByCopy`'s distinguishing capability), `G`
//! (no index covers graph, so the mask-scan / pushdown fallback), and `SPOG`
//! (every component bound — the prefix probe's full-width case in memory,
//! maximum residual filtering on file).
//!
//! ## Selectivity of the generated data
//!
//! The dataset is `support::dataset`, the same term shape the comparative
//! suites read from an N-Triples file — ten triples per subject, a 32-term
//! predicate vocabulary, one distinct object per two rows — generated in
//! process so this target stays uploadable to CodSpeed. Probes bind index 0 of
//! each role, which every role has, so no pattern can silently match zero rows.
//!
//! Selectivity therefore follows the row count rather than a fixed period, and
//! nothing here restates it: each run prints its own moduli and matched-row
//! counts as a `#dataset` line (see `dataset::shape_line`), which is where the
//! dashboard's figures come from.

use std::collections::HashMap;
use std::fmt;
use std::hint::black_box;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};

use futures::stream;
use oxrdf::{NamedNode, NamedOrBlankNode};

use vortex_rdf_core::{LayoutStrategy, VortexRdfError, VortexRdfStore, io};

// The module is shared with `match_lazy.rs` and compiled per-target; items
// only the other target uses are dead here by design.
#[allow(dead_code)]
#[macro_use]
mod support;
use support::*;

fn main() {
    // Stamp what this run generated; the dashboard reads the line, divan
    // ignores it.
    println!(
        "{}",
        dataset::shape_line(bench_size(), dataset::WANT_GRAPHS)
    );
    divan::main();
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

#[divan::bench(args = SERIALIZE_CONFIGS, sample_count = HEAVY_SAMPLES)]
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
// Every layout × index × source cell sweeps the eight routing patterns in
// both cache regimes.
// ══════════════════════════════════════════════════════════════════════════

// The full layout × source × index match matrix, materializing, in both cache
// regimes: two groups per cell, named `match_{cold,warm}_{layout}_{index}_{source}`.
match_matrix!(
    true;
    // No secondary index.
    (Layout::Default, Index::None, Source::InMemory) => match_cold_default_noindex_mem / match_warm_default_noindex_mem,
    (Layout::Default, Index::None, Source::File) => match_cold_default_noindex_file / match_warm_default_noindex_file,
    (Layout::TypedObject, Index::None, Source::InMemory) => match_cold_typedobj_noindex_mem / match_warm_typedobj_noindex_mem,
    (Layout::TypedObject, Index::None, Source::File) => match_cold_typedobj_noindex_file / match_warm_typedobj_noindex_file,
    (Layout::Dictionary, Index::None, Source::InMemory) => match_cold_dict_noindex_mem / match_warm_dict_noindex_mem,
    (Layout::Dictionary, Index::None, Source::File) => match_cold_dict_noindex_file / match_warm_dict_noindex_file,
    // Secondary by reference.
    (Layout::Default, Index::ByReference, Source::InMemory) => match_cold_default_byref_mem / match_warm_default_byref_mem,
    (Layout::Default, Index::ByReference, Source::File) => match_cold_default_byref_file / match_warm_default_byref_file,
    (Layout::TypedObject, Index::ByReference, Source::InMemory) => match_cold_typedobj_byref_mem / match_warm_typedobj_byref_mem,
    (Layout::TypedObject, Index::ByReference, Source::File) => match_cold_typedobj_byref_file / match_warm_typedobj_byref_file,
    (Layout::Dictionary, Index::ByReference, Source::InMemory) => match_cold_dict_byref_mem / match_warm_dict_byref_mem,
    (Layout::Dictionary, Index::ByReference, Source::File) => match_cold_dict_byref_file / match_warm_dict_byref_file,
    // Secondary by copy.
    (Layout::Default, Index::ByCopy, Source::InMemory) => match_cold_default_bycopy_mem / match_warm_default_bycopy_mem,
    (Layout::Default, Index::ByCopy, Source::File) => match_cold_default_bycopy_file / match_warm_default_bycopy_file,
    (Layout::TypedObject, Index::ByCopy, Source::InMemory) => match_cold_typedobj_bycopy_mem / match_warm_typedobj_bycopy_mem,
    (Layout::TypedObject, Index::ByCopy, Source::File) => match_cold_typedobj_bycopy_file / match_warm_typedobj_bycopy_file,
    (Layout::Dictionary, Index::ByCopy, Source::InMemory) => match_cold_dict_bycopy_mem / match_warm_dict_bycopy_mem,
    (Layout::Dictionary, Index::ByCopy, Source::File) => match_cold_dict_bycopy_file / match_warm_dict_bycopy_file,
);
/// Chained refinement: `match_pattern(P)` then `match_pattern(O)` on the
/// resulting view — the headline "views narrow the same coordinate space"
/// feature, which no single-pattern benchmark exercises.
#[divan::bench(args = [Source::File, Source::InMemory], sample_count = QUERY_SAMPLES)]
fn match_chained(bencher: divan::Bencher, source: &Source) {
    let source = *source;
    let (_, p, o, _) = terms_for(Pattern::PO);
    let (p, o) = (p.unwrap(), o.unwrap());
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
// The full-scan decode is the single most fundamental read, and where layouts
// diverge most: Dictionary decodes codes to terms, TypedObject reassembles the
// object from four columns. Load costs (opening a file, decoding IPC) are
// benchmarked in their own cells rather than sitting in untimed setup.
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
#[divan::bench(args = DECODE_CONFIGS, sample_count = HEAVY_SAMPLES)]
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

/// [`decode_all`] over a literal-bearing dataset. The main dataset's literals
/// are plain and escape-free, so this is the one benchmark whose decode reaches
/// the language-tag, datatype and unescape paths.
#[divan::bench(sample_count = HEAVY_SAMPLES)]
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

/// Open a file-backed store: the footer (and, under Dictionary, the
/// dictionary's window bounds), memory-mapped.
#[divan::bench(args = [Layout::Default, Layout::TypedObject, Layout::Dictionary], sample_count = HEAVY_SAMPLES)]
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
#[divan::bench(sample_count = HEAVY_SAMPLES)]
fn from_bytes(bencher: divan::Bencher) {
    bencher
        .with_inputs(|| cached_bytes(Layout::Default, Index::None, bench_size()))
        .bench_refs(|bytes| {
            rt().block_on(async {
                let store = VortexRdfStore::from_bytes(bytes).await.expect("from_bytes");
                black_box(store)
            })
        });
}

// ══════════════════════════════════════════════════════════════════════════
// Group 4 — DICTIONARY ACCESS (memory-mapped file vs whole-store load)
//
// A Dictionary file opened with `from_file` is memory-mapped: the term
// dictionary stays in its child and every probe or decode reads the mapped
// leaves it touches. `from_file_in_memory` loads the whole store, the
// dictionary lifted into memory (still FSST). The bench ids keep their
// `resident`/`file_backed` names so the dashboard's history lines up.
// ══════════════════════════════════════════════════════════════════════════

#[derive(Copy, Clone, PartialEq, Eq, Hash)]
enum DictResidency {
    Resident,
    FileBacked,
}

impl DictResidency {
    /// Open the store at `path` under this access mode.
    async fn open(self, path: &std::path::Path) -> VortexRdfStore {
        match self {
            Self::Resident => VortexRdfStore::from_file_in_memory(path).await,
            Self::FileBacked => VortexRdfStore::from_file(path).await,
        }
        .expect("open dictionary store")
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

static WRITER_FILE_CACHE: OnceLock<Mutex<HashMap<usize, PathBuf>>> = OnceLock::new();

/// A Dictionary-layout file (no indexes), built once per size as the
/// `quads_stream_to_vortex_file` product — the on-disk file the CLI and the
/// bindings write, with the writer's own column encodings. The Group 2/3 file
/// cells open a different artifact: `cached_file`, the `to_bytes`
/// re-serialization of a built store. Every Group 4 cell opens this one.
fn cached_writer_file(size: usize) -> PathBuf {
    memoized(&WRITER_FILE_CACHE, size, || {
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
        path
    })
}

fn open_dict_store(residency: DictResidency, size: usize) -> VortexRdfStore {
    let path = cached_writer_file(size);
    rt().block_on(residency.open(&path))
}

/// Open cost across the access axis: the load pays every read at open, the
/// mapped open only the footer and the window bounds.
#[divan::bench(args = DICT_CONFIGS, sample_count = HEAVY_SAMPLES)]
fn dict_open(bencher: divan::Bencher, residency: &DictResidency) {
    let residency = *residency;
    bencher
        .with_inputs(|| cached_writer_file(bench_size()))
        .bench_refs(|path| {
            rt().block_on(async {
                let store = residency.open(path).await;
                black_box(store.layout())
            })
        });
}

/// Cold term → code probes: a fully bound pattern (four dictionary probes) on a
/// store opened fresh each iteration, so neither the probe memo nor the
/// file-backed dictionary's chunk cache carries anything over. Resident
/// probes are in-memory binary searches; file-backed ones binary-search the
/// term column through chunk leaves fetched on demand, so this cell prices
/// those first fetches rather than the search over them.
#[divan::bench(args = DICT_CONFIGS, sample_count = QUERY_SAMPLES)]
fn dict_probe_cold(bencher: divan::Bencher, residency: &DictResidency) {
    let residency = *residency;
    let (s, p, o, g) = terms_for(Pattern::SPOG);
    let (s, p, o, g) = (s.unwrap(), p.unwrap(), o.unwrap(), g.unwrap());
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
#[divan::bench(args = DICT_CONFIGS, sample_count = QUERY_SAMPLES)]
fn dict_probe_warm(bencher: divan::Bencher, residency: &DictResidency) {
    let store = open_dict_store(*residency, bench_size());
    let (s, p, o, g) = terms_for(Pattern::SPOG);
    let (s, p, o, g) = (s.unwrap(), p.unwrap(), o.unwrap(), g.unwrap());
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
#[divan::bench(args = DICT_CONFIGS, sample_count = QUERY_SAMPLES)]
fn dict_probe_distinct(bencher: divan::Bencher, residency: &DictResidency) {
    let store = open_dict_store(*residency, bench_size());
    let subjects = bench_moduli().n_subj;
    let next = AtomicUsize::new(0);
    bencher
        .with_inputs(|| {
            // Cycle the *distinct* subjects the generator emits, not the row
            // count: past `n_subj` the terms stop existing, and a probe that
            // misses measures the dictionary's absent-term path instead of the
            // search this cell is about.
            let i = next.fetch_add(1, Ordering::Relaxed) % subjects;
            NamedOrBlankNode::NamedNode(NamedNode::new_unchecked(dataset::subject_iri(i)))
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

/// Reconstruction of a point result (subject-bound, the ten-odd rows describing
/// one resource): the chunk's handful of distinct codes stays under the
/// point-read cap, so a file-backed dictionary resolves them by reading exactly
/// those rows out of its cached wire chunks instead of scanning. The bound term
/// is memoized after the first iteration, so this cell prices the decode, not
/// the probe.
#[divan::bench(args = DICT_CONFIGS, sample_count = QUERY_SAMPLES)]
fn dict_decode_point(bencher: divan::Bencher, residency: &DictResidency) {
    let store = open_dict_store(*residency, bench_size());
    let (s, ..) = terms_for(Pattern::S);
    let s = s.unwrap();
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

/// Reconstruction of a wide matched subset (predicate-bound, one 32nd of the
/// rows — the predicate vocabulary is 32 terms): resident decodes codes against
/// the in-memory dictionary. The matched chunk holds far more distinct codes
/// than the point-read cap admits, so a file-backed dictionary resolves them
/// with one row-index scan — the bulk path, whose whole-leaf decode is what
/// wins at this width. [`dict_decode_point`] covers the other side of the cap.
#[divan::bench(args = DICT_CONFIGS, sample_count = QUERY_SAMPLES)]
fn dict_decode_matched(bencher: divan::Bencher, residency: &DictResidency) {
    let store = open_dict_store(*residency, bench_size());
    let (_, p, ..) = terms_for(Pattern::P);
    let p = p.unwrap();
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

// ══════════════════════════════════════════════════════════════════════════
// Group 4b — NARROWING (keep / window / predicates / batches)
//
// The restrictions a query engine pushes below a pattern, on the Group 4
// file (both dictionary residencies where the dictionary is what answers):
// a `keep` applied inside a predicate-bound file view, a `LIMIT` window and
// an `ASK` over the same filtered view, a term predicate partitioning the
// dictionary, and a batch of subject probes.
// ══════════════════════════════════════════════════════════════════════════

/// A predicate-bound file view narrowed by a `keep` on its subject column:
/// the namespace range of the first half of the subjects — on this sorted
/// file one run, located in the mapped subject column and intersected with
/// the view's rows, with the predicate filter left pending beside it — the
/// shape a `VALUES`/`FILTER` pushdown takes on a file. Priced with the gather
/// of the surviving code columns, which is where the pending filter is
/// evaluated, so the number is what a consumer waits for.
#[divan::bench(args = DICT_CONFIGS, sample_count = QUERY_SAMPLES)]
fn narrow_keep_range(bencher: divan::Bencher, residency: &DictResidency) {
    use vortex_rdf_core::{Keep, QuadColumn};
    let store = open_dict_store(*residency, bench_size());
    let (_, p, ..) = terms_for(Pattern::P);
    let p = p.unwrap();
    let keep = rt().block_on(async {
        let dict = store.dict_reader().expect("dictionary handle");
        let half = bench_moduli().n_subj / 2;
        let lo = dict
            .lower_bound(&format!("<{}", dataset::subject_iri(0)))
            .await
            .unwrap();
        let hi = dict
            .lower_bound(&format!("<{}", dataset::subject_iri(half)))
            .await
            .unwrap();
        Keep::range(lo..hi)
    });
    bencher.bench(|| {
        rt().block_on(async {
            let narrowed = store
                .match_pattern(None, Some(&p), None, None)
                .await
                .expect("match P")
                .keep(QuadColumn::S, &keep)
                .await
                .expect("keep");
            let columns = narrowed.code_columns_gathered().await.expect("gather");
            black_box(columns.map(|c| c[0].len()))
        })
    });
}

/// `LIMIT 64` over a predicate-bound file view: the filter's splits are
/// evaluated in file order and the window stops at the first that fills —
/// against the full match's gather, this is what `LIMIT` saves.
#[divan::bench(args = DICT_CONFIGS, sample_count = QUERY_SAMPLES)]
fn narrow_window(bencher: divan::Bencher, residency: &DictResidency) {
    let store = open_dict_store(*residency, bench_size());
    let (_, p, ..) = terms_for(Pattern::P);
    let p = p.unwrap();
    bencher.bench(|| {
        rt().block_on(async {
            let windowed = store
                .match_pattern(None, Some(&p), None, None)
                .await
                .expect("match P")
                .window(0, 64)
                .await
                .expect("window");
            let columns = windowed.code_columns_gathered().await.expect("gather");
            black_box(columns.map(|c| c[0].len()))
        })
    });
}

/// `ASK` over a predicate-bound file view: `exists` reads the filter up to
/// its first matching split.
#[divan::bench(args = DICT_CONFIGS, sample_count = QUERY_SAMPLES)]
fn narrow_exists(bencher: divan::Bencher, residency: &DictResidency) {
    let store = open_dict_store(*residency, bench_size());
    let (_, p, ..) = terms_for(Pattern::P);
    let p = p.unwrap();
    bencher.bench(|| {
        rt().block_on(async {
            let matched = store
                .match_pattern(None, Some(&p), None, None)
                .await
                .expect("match P");
            black_box(matched.exists().await.expect("exists"))
        })
    });
}

/// A term predicate over 4,096 candidate codes spread across the dictionary:
/// the per-query FILTER evaluation (resident: a cursor read per code;
/// file-backed: the windows holding them).
#[divan::bench(args = DICT_CONFIGS, sample_count = QUERY_SAMPLES)]
fn narrow_filter_codes(bencher: divan::Bencher, residency: &DictResidency) {
    use vortex_rdf_core::{TermPredicate, TextOptions};
    let store = open_dict_store(*residency, bench_size());
    // Over `STR()`: an IRI is a text only there, so its candidates are read.
    let options = TextOptions {
        as_str: true,
        ..TextOptions::default()
    };
    let predicate = TermPredicate::parse_with("str_prefix", "http://", &options).unwrap();
    let dict = store.dict_reader().expect("dictionary handle");
    let step = (dict.len() / 4_096).max(1);
    let codes: Vec<vortex_rdf_core::TermCode> = (0..dict.len() as vortex_rdf_core::TermCode)
        .step_by(step)
        .take(4_096)
        .collect();
    bencher.bench(|| {
        rt().block_on(async {
            let (passed, undecided) = dict.filter_codes(&predicate, &codes).await.expect("filter");
            black_box((passed.len(), undecided.len()))
        })
    });
}

/// A batch of 64 subject probes in one `match_many`, gathered: the
/// nested-loop join's shape, where a file-backed store overlaps the probes'
/// reads.
#[divan::bench(args = DICT_CONFIGS, sample_count = QUERY_SAMPLES)]
fn narrow_match_many(bencher: divan::Bencher, residency: &DictResidency) {
    use vortex_rdf_core::Probe;
    let store = open_dict_store(*residency, bench_size());
    let subjects = bench_moduli().n_subj;
    let probes: Vec<Probe> = (0..64)
        .map(|i| {
            let s = NamedOrBlankNode::NamedNode(NamedNode::new_unchecked(dataset::subject_iri(
                (i * 7919) % subjects,
            )));
            Probe::new(Some(s), None, None, None)
        })
        .collect();
    bencher.bench(|| {
        rt().block_on(async {
            let views = store.match_many(&probes).await.expect("match_many");
            let mut rows = 0usize;
            for view in &views {
                rows += view
                    .code_columns_gathered()
                    .await
                    .expect("gather")
                    .map_or(0, |c| c[0].len());
            }
            black_box(rows)
        })
    });
}

// ══════════════════════════════════════════════════════════════════════════
// Group 5 — MUTATE (append / delete / compact)
//
// Mutations are copy-on-write: appends accrete in an in-memory tail, deletes
// tombstone, and neither rewrites the base — so what each arm costs is a
// different thing, and they are timed as three.
//
// * The `add_*` pair starts from an empty store, which no layout or index knob
//   reaches, so it is one cell per call shape (per-quad loop vs one batched
//   call) rather than a sweep.
// * The `append_*` and `delete_*` arms run against a populated store, where the
//   axes do matter: every add checks presence and every delete resolves a fully
//   bound pattern, both through `match_pattern` — so layout, index and source
//   decide the routing they pay for.
// * `mutate_compact` times the fold of a tail back into the base on its own.
//   The batch is kept under the auto-compaction thresholds (see [`mut_batch`]),
//   so the append arms measure accretion only and the rebuild they would
//   eventually trigger is a cell of its own instead of landing in whichever
//   iteration crossed the line.
// ══════════════════════════════════════════════════════════════════════════

/// Quads per mutation batch (`MUT_BATCH`), clamped below the store's
/// auto-compaction floor.
///
/// The ceiling is what keeps the arms separable: an append that crosses the
/// floor folds the tail into the base, which is a rebuild rather than an append
/// — `mutate_compact` times that — and for a file-backed store the fold rewrites
/// its source file in place, which is the shared artifact every other file
/// benchmark here opens. The comparative suite's `MUT_BATCH` (`compare.rs`) is
/// not clamped: its add row deliberately includes the auto-compaction.
fn mut_batch() -> usize {
    /// One below `AUTO_COMPACT_TAIL_FLOOR`, the smallest tail the store will
    /// fold (the ratio trigger only raises that bar).
    const MAX: usize = 4_095;
    static N: OnceLock<usize> = OnceLock::new();
    *N.get_or_init(|| {
        std::env::var("MUT_BATCH")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(512)
            .clamp(1, MAX)
    })
}

/// Deletes per [`mutate_delete_loop`], capped well below [`mut_batch`]. Nothing
/// accumulates across a delete loop the way an append's tail does — each delete
/// resolves its pattern and unions a mask, so the count only averages — and a
/// file-backed delete costs a materialization, which at the append batch's size
/// would make that one cell dominate the suite's instrumented runtime.
fn delete_batch() -> usize {
    const CAP: usize = 64;
    mut_batch().min(CAP)
}

#[derive(Copy, Clone)]
struct MutCfg {
    layout: Layout,
    index: Index,
    source: Source,
}

impl fmt::Debug for MutCfg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}_{}_{}",
            self.layout.short(),
            self.index.short(),
            self.source.short()
        )
    }
}

/// A star around `default / no_index / in_memory`, one factor at a time — the
/// three ways the presence check and the delete's pattern resolution route:
/// against a secondary index, against dictionary codes (where a fresh quad's
/// terms have no code at all), and against a file.
const MUT_CONFIGS: &[MutCfg] = &[
    MutCfg {
        layout: Layout::Default,
        index: Index::None,
        source: Source::InMemory,
    }, // baseline
    MutCfg {
        layout: Layout::Default,
        index: Index::ByCopy,
        source: Source::InMemory,
    },
    MutCfg {
        layout: Layout::Dictionary,
        index: Index::None,
        source: Source::InMemory,
    },
    MutCfg {
        layout: Layout::Default,
        index: Index::None,
        source: Source::File,
    },
];

/// `add_quad` in a loop, into an empty store: one tail rebuild and one presence
/// check per quad, the shape a per-quad ingest API takes.
#[divan::bench(sample_count = HEAVY_SAMPLES)]
fn mutate_add_loop(bencher: divan::Bencher) {
    let fresh = dataset::fresh_quads(mut_batch());
    bencher.bench(|| {
        rt().block_on(async {
            let mut store = VortexRdfStore::empty();
            for quad in &fresh {
                store = store.add_quad(quad.clone()).await.expect("add_quad");
            }
            black_box(store)
        })
    });
}

/// The same batch through one `add_quads` call: the presence checks are still
/// per quad, but the tail is built once.
#[divan::bench(sample_count = HEAVY_SAMPLES)]
fn mutate_add_batch(bencher: divan::Bencher) {
    let fresh = dataset::fresh_quads(mut_batch());
    bencher.bench(|| {
        rt().block_on(async {
            let store = VortexRdfStore::empty()
                .add_quads(fresh.iter().cloned())
                .await
                .expect("add_quads");
            black_box(store)
        })
    });
}

/// `add_quad` in a loop against a *populated* store — the append the empty-store
/// arms cannot show: each quad's presence check resolves a fully bound pattern
/// against the base, which is where layout, index and source diverge. The store
/// is rebuilt per iteration (untimed), so every iteration appends to the same
/// base with the same cold caches.
#[divan::bench(args = MUT_CONFIGS, sample_count = HEAVY_SAMPLES)]
fn mutate_append_loop(bencher: divan::Bencher, cfg: &MutCfg) {
    let cfg = *cfg;
    let fresh = dataset::fresh_quads(mut_batch());
    bencher
        .with_inputs(|| make_store(cfg.source, cfg.layout, cfg.index, bench_size()))
        .bench_values(|store| {
            rt().block_on(async {
                let mut store = store;
                for quad in &fresh {
                    store = store.add_quad(quad.clone()).await.expect("add_quad");
                }
                black_box(store)
            })
        });
}

/// [`mutate_append_loop`]'s batch counterpart: one `add_quads` call, so the
/// difference against the loop is the tail rebuilds it avoids.
#[divan::bench(args = MUT_CONFIGS, sample_count = HEAVY_SAMPLES)]
fn mutate_append_batch(bencher: divan::Bencher, cfg: &MutCfg) {
    let cfg = *cfg;
    let fresh = dataset::fresh_quads(mut_batch());
    bencher
        .with_inputs(|| make_store(cfg.source, cfg.layout, cfg.index, bench_size()))
        .bench_values(|store| {
            rt().block_on(async {
                let store = store
                    .add_quads(fresh.iter().cloned())
                    .await
                    .expect("add_quads");
                black_box(store)
            })
        });
}

/// `delete_quad` in a loop over rows the store holds: each one resolves a fully
/// bound pattern and unions its rows into the tombstone mask, leaving the base
/// and its indexes untouched. Rebuilding (or reopening) the store is untimed
/// per-iteration setup — otherwise the cell measures the setup, since each
/// iteration deletes the rows the next one has to find again.
#[divan::bench(args = MUT_CONFIGS, sample_count = HEAVY_SAMPLES)]
fn mutate_delete_loop(bencher: divan::Bencher, cfg: &MutCfg) {
    let cfg = *cfg;
    let doomed = dataset::dataset_prefix(bench_size(), bench_moduli(), delete_batch());
    bencher
        .with_inputs(|| make_store(cfg.source, cfg.layout, cfg.index, bench_size()))
        .bench_values(|store| {
            rt().block_on(async {
                let mut store = store;
                for quad in &doomed {
                    store = store.delete_quad(quad).await.expect("delete_quad");
                }
                black_box(store)
            })
        });
}

/// Fold a tail into the base: gather the live rows, re-sort by (s, p, o, g) and
/// rebuild — the O(n log n) step `add_quads` reaches for on its own once a tail
/// outgrows the thresholds, and the amortized half of an append's cost. Only the
/// in-memory source: a file-backed compaction rewrites its source file, which
/// the rest of the suite reads. Dictionary is swept beside Default because
/// compaction re-encodes the rows against a fresh term dictionary.
#[divan::bench(args = [Layout::Default, Layout::Dictionary], sample_count = HEAVY_SAMPLES)]
fn mutate_compact(bencher: divan::Bencher, layout: &Layout) {
    let layout = *layout;
    let fresh = dataset::fresh_quads(mut_batch());
    bencher
        .with_inputs(|| {
            let store = make_store(Source::InMemory, layout, Index::None, bench_size());
            rt().block_on(store.add_quads(fresh.iter().cloned()))
                .expect("build tail")
        })
        .bench_refs(|store| {
            rt().block_on(async { black_box(store.compact().await.expect("compact")) })
        });
}
