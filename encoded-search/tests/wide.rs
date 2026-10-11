//! Probes over u64 columns whose values lie past `u32::MAX` — the term-code
//! columns of a dictionary of more than 2^32 terms — against the canonical
//! `partition_point` floor: every forced encoding the store's columns take
//! (bit-packed, frame-of-reference over bit-packed, run-end over sequences,
//! dictionary, constant) and the default cascade. Needles include each
//! value's 32-bit truncation, which a probe narrowing to u32 would confuse
//! with a real value.

mod common;

use common::session;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute};
use vortex_btrblocks::schemes::integer;
use vortex_btrblocks::{BtrBlocksCompressorBuilder, CompressionSessionExt as _, Scheme, SchemeExt};
use vortex_rdf_encoded_search::{NodeKind, SortedProbe};

/// Past `u32::MAX`, so a value narrowed to 32 bits loses its high bits.
const BASE: u64 = (1 << 32) + 3;

/// The frame-of-reference scheme; the session's edition refines it to a
/// single reference.
static FOR: integer::FoRScheme = integer::FoRScheme::v1();

fn compress_with(schemes: &[&'static dyn Scheme], data: &[u64]) -> ArrayRef {
    let session = session();
    let stock: Vec<_> = session
        .compression()
        .schemes()
        .iter()
        .map(|s| s.id())
        .collect();
    let mut builder = BtrBlocksCompressorBuilder::from_session(&session).exclude_schemes(stock);
    for scheme in schemes {
        builder = builder.with_new_scheme(*scheme);
    }
    let canonical = PrimitiveArray::from_iter(data.iter().copied()).into_array();
    builder
        .build()
        .compress(&canonical, &mut session.create_execution_ctx())
        .unwrap()
}

fn compress_default(data: &[u64]) -> ArrayRef {
    let session = session();
    let canonical = PrimitiveArray::from_iter(data.iter().copied()).into_array();
    BtrBlocksCompressorBuilder::from_session(&session)
        .build()
        .compress(&canonical, &mut session.create_execution_ctx())
        .unwrap()
}

/// The half-open run of `needle` in sorted `data`.
fn canonical_bounds(data: &[u64], needle: u64) -> (usize, usize) {
    let lo = data.partition_point(|&v| v < needle);
    let hi = data.partition_point(|&v| v <= needle);
    (lo, hi.max(lo))
}

/// Every value and its neighbours, each value's 32-bit truncation, and the
/// domain edges.
fn needles_for(data: &[u64]) -> Vec<u64> {
    let mut needles = vec![0, 1, u64::from(u32::MAX), u64::from(u32::MAX) + 1, u64::MAX];
    for &v in data {
        needles.extend([v - 1, v, v + 1, u64::from(v as u32)]);
    }
    needles.sort_unstable();
    needles.dedup();
    needles
}

/// Exact bounds on every needle and exact point reads, with the resolved
/// tree holding each of `expect_kinds`.
fn assert_probe(arr: &ArrayRef, data: &[u64], expect_kinds: &[NodeKind]) {
    let probe = SortedProbe::resolve(arr)
        .unwrap_or_else(|| panic!("resolve declined ({})", arr.encoding_id()));
    let kinds = probe.node_kinds();
    for expected in expect_kinds {
        assert!(
            kinds.contains(expected),
            "kinds {kinds:?} missing {expected:?}"
        );
    }
    assert_eq!(probe.len(), data.len());
    for needle in needles_for(data) {
        assert_eq!(
            probe.bounds(needle),
            canonical_bounds(data, needle),
            "needle {needle}"
        );
    }
    for (i, &v) in data.iter().enumerate() {
        assert_eq!(probe.value_at(i), v, "value_at({i})");
    }
}

/// Codes of a sorted column: runs of `run` rows over consecutive codes from
/// [`BASE`].
fn sorted_codes(len: u64, run: u64) -> Vec<u64> {
    (0..len).map(|i| BASE + i / run).collect()
}

#[test]
fn probes_bitpacked_codes_past_u32() {
    // Raw bit-packing at 33 bits: the values themselves are past u32::MAX.
    let data = sorted_codes(4096, 7);
    let arr = compress_with(&[&integer::BitPackingScheme], &data);
    assert_probe(&arr, &data, &[NodeKind::BitPacked]);
}

#[test]
fn probes_for_bitpacked_codes_past_u32() {
    let data = sorted_codes(4096, 3);
    let arr = compress_with(&[&FOR, &integer::BitPackingScheme], &data);
    assert_probe(&arr, &data, &[NodeKind::FoR, NodeKind::BitPacked]);
}

#[test]
fn probes_runend_sequence_codes_past_u32() {
    let data = sorted_codes(22_000, 11);
    let arr = compress_with(&[&integer::RunEndScheme, &integer::SequenceScheme], &data);
    assert_probe(&arr, &data, &[NodeKind::RunEnd]);
}

#[test]
fn probes_dict_codes_past_u32() {
    // The twin of `differential.rs`'s dictionary probe, over twenty codes
    // past u32::MAX and over twenty that straddle it: the dictionary's
    // values child holds the wide codes.
    for start in [BASE, u64::from(u32::MAX) - 95] {
        let data: Vec<u64> = (0..10_000u64).map(|i| start + (i / 500) * 10).collect();
        let arr = compress_with(&[&integer::IntDictScheme], &data);
        assert_eq!(
            arr.encoding_id().as_str(),
            "vortex.dict",
            "fixture must produce a dict array"
        );
        assert_probe(&arr, &data, &[NodeKind::Dict]);
    }
}

#[test]
fn probes_constant_code_past_u32() {
    let data = vec![BASE; 1000];
    let arr = compress_default(&data);
    assert_probe(&arr, &data, &[NodeKind::Constant]);
}

#[test]
fn probes_default_cascade_codes_past_u32() {
    // Sorted codes with varied run lengths and gaps, from just past
    // u32::MAX to past 2^40.
    for (len, run, step) in [(1_000u64, 1u64, 1u64), (10_000, 5, 3), (50_000, 2, 1 << 20)] {
        let data: Vec<u64> = (0..len).map(|i| BASE + (i / run) * step).collect();
        let arr = compress_default(&data);
        assert_probe(&arr, &data, &[]);
    }
}
