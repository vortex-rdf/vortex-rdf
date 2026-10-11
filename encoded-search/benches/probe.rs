//! Probe mechanism benchmarks: resolve cost, bounds vs the generic vortex
//! kernel and the canonical floor, windowed bounds, and point access.
//!
//! Deliberately outside the CodSpeed surface: CI uploads only core's
//! `benchmark` target, so these groups exist for local A/B runs. The
//! user-visible effect of encoded probing is measured end-to-end by core's
//! `benchmark` and `match_lazy` targets.

#[path = "../tests/common/mod.rs"]
mod common;

use std::sync::LazyLock;

use divan::Bencher;
use divan::counter::ItemsCount;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::scalar::Scalar;
use vortex_array::search_sorted::{SearchSorted, SearchSortedSide};
use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute};
use vortex_btrblocks::schemes::integer;
use vortex_btrblocks::{BtrBlocksCompressor, BtrBlocksCompressorBuilder, Scheme};
use vortex_rdf_encoded_search::{NodeKind, SortedProbe};

const N: usize = 2_097_152;
const REPEATS: usize = 11;
/// Run length of the dictionary fixture: few enough distinct values for the
/// dictionary scheme to accept the column.
const DICT_RUN: usize = 1024;
const SHAPES: [&str; 3] = ["runend_2m", "for_bitpacked_2m", "dict_2m"];

struct Fixture {
    name: &'static str,
    data: Vec<u32>,
    encoded: ArrayRef,
}

static FIXTURES: LazyLock<Vec<Fixture>> = LazyLock::new(|| {
    let session = common::session();
    let mut ctx = session.create_execution_ctx();
    let default = BtrBlocksCompressorBuilder::from_session(&session).build();
    let dict = BtrBlocksCompressorBuilder::empty()
        .with_new_scheme(&integer::IntDictScheme as &'static dyn Scheme)
        .build();
    let shapes: Vec<(&'static str, Vec<u32>, &BtrBlocksCompressor, NodeKind)> = vec![
        (
            "runend_2m",
            (0..N).map(|i| (i / REPEATS) as u32).collect(),
            &default,
            NodeKind::RunEnd,
        ),
        (
            "for_bitpacked_2m",
            (0..N).map(|i| 1_000_000_000 + (i / 3) as u32).collect(),
            &default,
            NodeKind::FoR,
        ),
        (
            "dict_2m",
            (0..N).map(|i| (i / DICT_RUN) as u32).collect(),
            &dict,
            NodeKind::Dict,
        ),
    ];
    shapes
        .into_iter()
        .map(|(name, data, compressor, kind)| {
            let canonical = PrimitiveArray::from_iter(data.iter().copied()).into_array();
            let encoded = compressor.compress(&canonical, &mut ctx).unwrap();
            let probe = SortedProbe::resolve(&encoded)
                .unwrap_or_else(|| panic!("{name}: resolve declined ({})", encoded.encoding_id()));
            assert!(
                probe.node_kinds().contains(&kind),
                "{name}: expected a {kind:?} node, got {:?}",
                probe.node_kinds()
            );
            Fixture {
                name,
                data,
                encoded,
            }
        })
        .collect()
});

fn fixture(shape: &str) -> &'static Fixture {
    FIXTURES.iter().find(|f| f.name == shape).unwrap()
}

/// Probe values drawn from the fixture's own domain, strided across it.
fn needles(data: &[u32]) -> Vec<u32> {
    (0..1000usize)
        .map(|i| data[(i * 2099) % data.len()])
        .collect()
}

fn main() {
    divan::main();
}

#[divan::bench(args = SHAPES)]
fn resolve(bencher: Bencher, shape: &str) {
    let fixture = fixture(shape);
    bencher.bench(|| SortedProbe::resolve(divan::black_box(&fixture.encoded)).is_some());
}

#[divan::bench(args = SHAPES)]
fn bounds_encoded(bencher: Bencher, shape: &str) {
    let fixture = fixture(shape);
    let probe = SortedProbe::resolve(&fixture.encoded).unwrap();
    let probes = needles(&fixture.data);
    bencher.counter(ItemsCount::new(probes.len())).bench(|| {
        probes
            .iter()
            .map(|&c| probe.bounds(u64::from(c)).1)
            .sum::<usize>()
    });
}

#[divan::bench(args = SHAPES)]
fn bounds_generic_search_sorted(bencher: Bencher, shape: &str) {
    let fixture = fixture(shape);
    let probes: Vec<u32> = needles(&fixture.data).into_iter().take(20).collect();
    bencher.counter(ItemsCount::new(probes.len())).bench(|| {
        probes
            .iter()
            .map(|&c| {
                let scalar = Scalar::from(c);
                let lo = fixture
                    .encoded
                    .search_sorted(&scalar, SearchSortedSide::Left)
                    .unwrap()
                    .to_index();
                let hi = fixture
                    .encoded
                    .search_sorted(&scalar, SearchSortedSide::Right)
                    .unwrap()
                    .to_index();
                hi - lo
            })
            .sum::<usize>()
    });
}

#[divan::bench(args = SHAPES)]
fn bounds_canonical_partition_point(bencher: Bencher, shape: &str) {
    let fixture = fixture(shape);
    let probes = needles(&fixture.data);
    bencher.counter(ItemsCount::new(probes.len())).bench(|| {
        probes
            .iter()
            .map(|&c| {
                let lo = fixture.data.partition_point(|&v| v < c);
                let hi = fixture.data.partition_point(|&v| v <= c);
                hi - lo
            })
            .sum::<usize>()
    });
}

/// Windowed bounds: 1000 windows of `1000 * REPEATS` rows strided across the
/// column, each probed for the value at its midpoint.
#[divan::bench(args = SHAPES)]
fn bounds_in(bencher: Bencher, shape: &str) {
    let fixture = fixture(shape);
    let probe = SortedProbe::resolve(&fixture.encoded).unwrap();
    let window_len = 1000 * REPEATS;
    let windows: Vec<(std::ops::Range<usize>, u64)> = (0..1000usize)
        .map(|i| {
            let start = (i * 2099 * REPEATS) % (N - window_len);
            let needle = u64::from(fixture.data[start + window_len / 2]);
            (start..start + window_len, needle)
        })
        .collect();
    bencher.counter(ItemsCount::new(windows.len())).bench(|| {
        windows
            .iter()
            .map(|(window, needle)| probe.bounds_in(window.clone(), *needle).1)
            .sum::<usize>()
    });
}

#[divan::bench(args = SHAPES)]
fn value_at(bencher: Bencher, shape: &str) {
    let fixture = fixture(shape);
    let probe = SortedProbe::resolve(&fixture.encoded).unwrap();
    let indices: Vec<usize> = (0..1000usize).map(|i| (i * 2099) % N).collect();
    bencher
        .counter(ItemsCount::new(indices.len()))
        .bench(|| indices.iter().map(|&i| probe.value_at(i)).sum::<u64>());
}
