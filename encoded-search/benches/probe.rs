//! Probe mechanism benchmarks: resolve cost, bounds vs the generic vortex
//! kernel and the canonical floor, and point access.
//!
//! Deliberately outside the CodSpeed surface: CI uploads only core's
//! `benchmark` target, so these groups exist for local A/B runs. The
//! user-visible effect of encoded probing is measured end-to-end by core's
//! `benchmark` and `match_lazy` targets.

use std::sync::LazyLock;

use divan::Bencher;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::scalar::Scalar;
use vortex_array::scalar_fn::session::ScalarFnSession;
use vortex_array::search_sorted::{SearchSorted, SearchSortedSide};
use vortex_array::session::ArraySession;
use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute};
use vortex_btrblocks::BtrBlocksCompressorBuilder;
use vortex_encoded_search::SortedProbe;
use vortex_io::session::RuntimeSession;
use vortex_layout::session::LayoutSession;
use vortex_session::VortexSession;

const N: usize = 2_097_152;
const REPEATS: usize = 11;

struct Fixture {
    name: &'static str,
    data: Vec<u32>,
    encoded: ArrayRef,
}

static FIXTURES: LazyLock<Vec<Fixture>> = LazyLock::new(|| {
    let session = VortexSession::empty()
        .with::<ArraySession>()
        .with::<LayoutSession>()
        .with::<ScalarFnSession>()
        .with::<RuntimeSession>();
    vortex_file::register_default_encodings(&session);
    let mut ctx = session.create_execution_ctx();
    let compressor = BtrBlocksCompressorBuilder::default().build();
    let shapes: Vec<(&'static str, Vec<u32>)> = vec![
        ("runend_2m", (0..N).map(|i| (i / REPEATS) as u32).collect()),
        (
            "for_bitpacked_2m",
            (0..N).map(|i| 1_000_000_000 + (i / 3) as u32).collect(),
        ),
    ];
    shapes
        .into_iter()
        .map(|(name, data)| {
            let canonical = PrimitiveArray::from_iter(data.iter().copied()).into_array();
            let encoded = compressor.compress(&canonical, &mut ctx).unwrap();
            Fixture {
                name,
                data,
                encoded,
            }
        })
        .collect()
});

/// Probe values drawn from the fixture's own domain, strided across it.
fn needles(data: &[u32]) -> Vec<u32> {
    (0..1000usize)
        .map(|i| data[(i * 2099) % data.len()])
        .collect()
}

fn main() {
    divan::main();
}

#[divan::bench(args = ["runend_2m", "for_bitpacked_2m"])]
fn resolve(bencher: Bencher, shape: &str) {
    let fixture = FIXTURES.iter().find(|f| f.name == shape).unwrap();
    bencher.bench(|| SortedProbe::resolve(divan::black_box(&fixture.encoded)).is_some());
}

#[divan::bench(args = ["runend_2m", "for_bitpacked_2m"])]
fn bounds_probe(bencher: Bencher, shape: &str) {
    let fixture = FIXTURES.iter().find(|f| f.name == shape).unwrap();
    let probes = needles(&fixture.data);
    bencher.bench(|| {
        let probe = SortedProbe::resolve(&fixture.encoded).unwrap();
        probes
            .iter()
            .map(|&c| probe.bounds(u64::from(c)).1)
            .sum::<usize>()
    });
}

#[divan::bench(args = ["runend_2m", "for_bitpacked_2m"])]
fn bounds_generic_search_sorted(bencher: Bencher, shape: &str) {
    let fixture = FIXTURES.iter().find(|f| f.name == shape).unwrap();
    let probes: Vec<u32> = needles(&fixture.data).into_iter().take(20).collect();
    bencher.bench(|| {
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

#[divan::bench(args = ["runend_2m", "for_bitpacked_2m"])]
fn bounds_canonical_partition_point(bencher: Bencher, shape: &str) {
    let fixture = FIXTURES.iter().find(|f| f.name == shape).unwrap();
    let probes = needles(&fixture.data);
    bencher.bench(|| {
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

#[divan::bench(args = ["runend_2m", "for_bitpacked_2m"])]
fn value_at(bencher: Bencher, shape: &str) {
    let fixture = FIXTURES.iter().find(|f| f.name == shape).unwrap();
    let probe = SortedProbe::resolve(&fixture.encoded).unwrap();
    let indices: Vec<usize> = (0..1000usize).map(|i| (i * 2099) % N).collect();
    bencher.bench(|| indices.iter().map(|&i| probe.value_at(i)).sum::<u64>());
}
