//! Match benchmarks *without* materialization: the cost of `match_pattern`
//! alone — index resolution + row-selection composition into a narrowed view,
//! before any quad is decoded. Same layout × source × index matrix and probe
//! patterns as `benchmark.rs`'s match groups; the difference between the two
//! suites is materialization's share of a match, which is what an iterative
//! query plan defers by refining views and decoding only the final result.
//!
//! Deliberately a separate bench target: CodSpeed CI runs
//! `cargo codspeed run --bench benchmark`, so these groups never upload — they
//! exist for the local dashboard's lazy-match table only.

use std::hint::black_box;

// The module is shared with `benchmark.rs` and compiled per-target; items
// only the other target uses (the serialize helpers) are dead
// here by design.
#[allow(dead_code)]
mod support;
use support::*;

fn main() {
    divan::main();
}

/// Time `match_pattern` only: the returned view (a store sharing Arc'd
/// internals with the base) is black-boxed and dropped per iteration, never
/// executed.
fn run_lazy_match(
    bencher: divan::Bencher,
    layout: Layout,
    index: Index,
    source: Source,
    pattern: Pattern,
) {
    // Probe construction stays OUTSIDE the timed closure: this suite times
    // ~31 µs of pure match setup, so terms_for's ~4 String allocations per
    // iteration would be a visible fraction of the measurement.
    let (s, p, o, g) = terms_for(pattern);
    bencher
        .with_inputs(|| make_store(source, layout, index, bench_size()))
        .bench_refs(|store| {
            rt().block_on(async {
                let matched = store
                    .match_pattern(s.as_ref(), p.as_ref(), o.as_ref(), g.as_ref())
                    .await
                    .expect("match_pattern failed");
                black_box(matched)
            })
        });
}

macro_rules! lazy_match_bench {
    ($name:ident, $layout:expr, $index:expr, $source:expr) => {
        #[divan::bench(args = PATTERNS)]
        fn $name(bencher: divan::Bencher, pattern: &Pattern) {
            run_lazy_match(bencher, $layout, $index, $source, *pattern);
        }
    };
}

// The full matrix, named `lazy_` + the materializing twin's group name so the
// dashboard derives one set of ids from the other.
// No secondary index.
lazy_match_bench!(
    lazy_match_default_noindex_mem,
    Layout::Default,
    Index::None,
    Source::InMemory
);
lazy_match_bench!(
    lazy_match_default_noindex_file,
    Layout::Default,
    Index::None,
    Source::File
);
lazy_match_bench!(
    lazy_match_typedobj_noindex_mem,
    Layout::TypedObject,
    Index::None,
    Source::InMemory
);
lazy_match_bench!(
    lazy_match_typedobj_noindex_file,
    Layout::TypedObject,
    Index::None,
    Source::File
);
lazy_match_bench!(
    lazy_match_dict_noindex_mem,
    Layout::Dictionary,
    Index::None,
    Source::InMemory
);
lazy_match_bench!(
    lazy_match_dict_noindex_file,
    Layout::Dictionary,
    Index::None,
    Source::File
);
// Secondary by reference.
lazy_match_bench!(
    lazy_match_default_byref_mem,
    Layout::Default,
    Index::ByReference,
    Source::InMemory
);
lazy_match_bench!(
    lazy_match_default_byref_file,
    Layout::Default,
    Index::ByReference,
    Source::File
);
lazy_match_bench!(
    lazy_match_typedobj_byref_mem,
    Layout::TypedObject,
    Index::ByReference,
    Source::InMemory
);
lazy_match_bench!(
    lazy_match_typedobj_byref_file,
    Layout::TypedObject,
    Index::ByReference,
    Source::File
);
lazy_match_bench!(
    lazy_match_dict_byref_mem,
    Layout::Dictionary,
    Index::ByReference,
    Source::InMemory
);
lazy_match_bench!(
    lazy_match_dict_byref_file,
    Layout::Dictionary,
    Index::ByReference,
    Source::File
);
// Lean from_bytes adoption (wire-encoded base, deferred components) on the
// Dictionary layout: the encoded-probe counterpart of the `_mem` rows.
lazy_match_bench!(
    lazy_match_dict_noindex_bytes,
    Layout::Dictionary,
    Index::None,
    Source::Bytes
);
lazy_match_bench!(
    lazy_match_dict_byref_bytes,
    Layout::Dictionary,
    Index::ByReference,
    Source::Bytes
);
lazy_match_bench!(
    lazy_match_dict_bycopy_bytes,
    Layout::Dictionary,
    Index::ByCopy,
    Source::Bytes
);
// Secondary by copy.
lazy_match_bench!(
    lazy_match_default_bycopy_mem,
    Layout::Default,
    Index::ByCopy,
    Source::InMemory
);
lazy_match_bench!(
    lazy_match_default_bycopy_file,
    Layout::Default,
    Index::ByCopy,
    Source::File
);
lazy_match_bench!(
    lazy_match_typedobj_bycopy_mem,
    Layout::TypedObject,
    Index::ByCopy,
    Source::InMemory
);
lazy_match_bench!(
    lazy_match_typedobj_bycopy_file,
    Layout::TypedObject,
    Index::ByCopy,
    Source::File
);
lazy_match_bench!(
    lazy_match_dict_bycopy_mem,
    Layout::Dictionary,
    Index::ByCopy,
    Source::InMemory
);
lazy_match_bench!(
    lazy_match_dict_bycopy_file,
    Layout::Dictionary,
    Index::ByCopy,
    Source::File
);
