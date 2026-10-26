//! Probes over u64 columns whose values lie past `u32::MAX` — the term-code
//! columns of a dictionary of more than 2^32 terms — against the canonical
//! `partition_point` floor: every forced encoding the store's columns take
//! (bit-packed, frame-of-reference over bit-packed, run-end over sequences,
//! dictionary, constant) and the default cascade. Needles include each
//! value's 32-bit truncation, which a probe narrowing to u32 would confuse
//! with a real value.

mod common;

use common::{BITPACKING, FOR, canonical_index_bounds, compress_default, compress_with, session};
use vortex_array::arrays::PrimitiveArray;
use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute as _};
use vortex_btrblocks::schemes::integer;
use vortex_rdf_encoded_search::{NodeKind, SortedProbe};

/// Past `u32::MAX`, so a value narrowed to 32 bits loses its high bits.
const BASE: u64 = (1 << 32) + 3;

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
            canonical_index_bounds(data, needle),
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
    let arr = compress_with(&[&BITPACKING], &data);
    assert_probe(&arr, &data, &[NodeKind::BitPacked]);
}

#[test]
fn probes_for_bitpacked_codes_past_u32() {
    let data = sorted_codes(4096, 3);
    let arr = compress_with(&[&FOR, &BITPACKING], &data);
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

#[test]
fn probes_sparse_codes_past_u32() {
    use vortex_array::dtype::Nullability::NonNullable;
    use vortex_array::scalar::Scalar;
    use vortex_sequence::Sequence;
    use vortex_sparse::Sparse;

    // Sorted: the fill below every patch, the patches ascending to the end.
    let (len, first) = (5_000usize, 4_000usize);
    let data: Vec<u64> = (0..len as u64)
        .map(|i| {
            if (i as usize) < first {
                BASE
            } else {
                BASE + 1 + (i - first as u64) / 7
            }
        })
        .collect();
    let indices: Vec<u64> = (first as u64..len as u64).collect();
    let values: Vec<u64> = indices.iter().map(|&i| data[i as usize]).collect();
    let arr = Sparse::try_new(
        PrimitiveArray::from_iter(indices).into_array(),
        PrimitiveArray::from_iter(values).into_array(),
        len,
        Scalar::from(BASE),
        &mut session().create_execution_ctx(),
    )
    .unwrap()
    .into_array();
    assert_eq!(arr.encoding_id().as_str(), "vortex.sparse");
    assert_probe(&arr, &data, &[NodeKind::Sparse]);

    // Patch positions and values both arithmetic sequences, the shape a
    // sorted object column of an index child takes.
    let data: Vec<u64> = (0..len as u64)
        .map(|i| {
            if (i as usize) < first {
                BASE
            } else {
                BASE + 2 * (i - first as u64 + 1)
            }
        })
        .collect();
    let arr = Sparse::try_new(
        Sequence::try_new_typed::<u64>(first as u64, 1, NonNullable, len - first)
            .unwrap()
            .into_array(),
        Sequence::try_new_typed::<u64>(BASE + 2, 2, NonNullable, len - first)
            .unwrap()
            .into_array(),
        len,
        Scalar::from(BASE),
        &mut session().create_execution_ctx(),
    )
    .unwrap()
    .into_array();
    assert_probe(&arr, &data, &[NodeKind::Sparse, NodeKind::Sequence]);
}

#[test]
fn probes_chunked_frame_of_reference_past_u32() {
    use vortex_fastlanes::FoR;

    // One reference per 1024-value chunk, rising faster than the encoded
    // values, so the composed values ascend; with and without an offset into
    // the first chunk.
    for (offset, len) in [(0u16, 3_500usize), (100, 3_000)] {
        let chunks = (usize::from(offset) + len).div_ceil(1024);
        let references: Vec<u64> = (0..chunks as u64).map(|k| BASE + k * 100_000).collect();
        let encoded: Vec<u64> = (0..len)
            .map(|i| ((usize::from(offset) + i) % 1024) as u64 / 3)
            .collect();
        let data: Vec<u64> = (0..len)
            .map(|i| references[(usize::from(offset) + i) / 1024] + encoded[i])
            .collect();
        let arr = FoR::try_new_chunked(
            PrimitiveArray::from_iter(encoded).into_array(),
            PrimitiveArray::from_iter(references).into_array(),
            offset,
        )
        .unwrap()
        .into_array();
        assert_probe(&arr, &data, &[NodeKind::FoR, NodeKind::Primitive]);
    }
}
