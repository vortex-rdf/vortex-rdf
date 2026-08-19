//! Differential suite: probe bounds and point access against two oracles —
//! the canonical `partition_point` floor and vortex's generic `search_sorted`
//! kernel — over forced-encoding fixtures and a randomized sweep through the
//! default BtrBlocks cascade.
//!
//! Forced fixtures assert the resolved `node_kinds`, so a compressor change
//! that silently reroutes a fixture to a different encoding fails the test
//! instead of shrinking coverage.

use vortex_array::arrays::{ChunkedArray, ConstantArray, PrimitiveArray, SliceArray};
use vortex_array::scalar::Scalar;
use vortex_array::scalar_fn::session::ScalarFnSession;
use vortex_array::search_sorted::{SearchSorted, SearchSortedSide};
use vortex_array::session::ArraySession;
use vortex_array::{ArrayRef, ExecutionCtx, IntoArray, VortexSessionExecute};
use vortex_btrblocks::schemes::integer;
use vortex_btrblocks::{BtrBlocksCompressorBuilder, Scheme};
use vortex_encoded_search::{NodeKind, SortedProbe};
use vortex_io::session::RuntimeSession;
use vortex_layout::session::LayoutSession;
use vortex_sequence::Sequence;
use vortex_session::VortexSession;

fn session() -> VortexSession {
    let session = VortexSession::empty()
        .with::<ArraySession>()
        .with::<LayoutSession>()
        .with::<ScalarFnSession>()
        .with::<RuntimeSession>();
    vortex_file::register_default_encodings(&session);
    session
}

fn ctx() -> ExecutionCtx {
    session().create_execution_ctx()
}

fn compress_with(schemes: &[&'static dyn Scheme], data: &[u32]) -> ArrayRef {
    let mut builder = BtrBlocksCompressorBuilder::empty();
    for scheme in schemes {
        builder = builder.with_new_scheme(*scheme);
    }
    let canonical = PrimitiveArray::from_iter(data.iter().copied()).into_array();
    builder.build().compress(&canonical, &mut ctx()).unwrap()
}

fn compress_default(data: &[u32]) -> ArrayRef {
    let canonical = PrimitiveArray::from_iter(data.iter().copied()).into_array();
    BtrBlocksCompressorBuilder::default()
        .build()
        .compress(&canonical, &mut ctx())
        .unwrap()
}

/// Needle set exercising every boundary of `data`: each distinct value and its
/// neighbors, domain extremes, and values beyond the array's ptype.
fn needles_for(data: &[u32]) -> Vec<u64> {
    let mut needles = vec![0, 1, u64::from(u32::MAX), u64::from(u32::MAX) + 1, u64::MAX];
    for &v in data {
        let v = u64::from(v);
        needles.extend([v.saturating_sub(1), v, v + 1]);
    }
    needles.sort_unstable();
    needles.dedup();
    needles
}

fn canonical_bounds(data: &[u32], needle: u64) -> (usize, usize) {
    let lo = data.partition_point(|&v| u64::from(v) < needle);
    let hi = data.partition_point(|&v| u64::from(v) <= needle);
    (lo, hi)
}

fn generic_bounds(arr: &ArrayRef, needle: u32) -> (usize, usize) {
    let probe = Scalar::from(needle);
    let lo = arr
        .search_sorted(&probe, SearchSortedSide::Left)
        .unwrap()
        .to_index();
    let hi = arr
        .search_sorted(&probe, SearchSortedSide::Right)
        .unwrap()
        .to_index();
    (lo, hi)
}

/// Three-way agreement on bounds plus exact point access, with an optional
/// coverage assertion on the resolved probe tree.
fn assert_probe(arr: &ArrayRef, data: &[u32], expect_kinds: &[NodeKind]) {
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
        let got = probe.bounds(needle);
        assert_eq!(
            got,
            canonical_bounds(data, needle),
            "canonical oracle, needle {needle}"
        );
        if let Ok(narrow) = u32::try_from(needle) {
            assert_eq!(
                got,
                generic_bounds(arr, narrow),
                "generic oracle, needle {needle}"
            );
        }
    }

    let stride = (data.len() / 1000).max(1);
    for i in (0..data.len()).step_by(stride) {
        assert_eq!(probe.value_at(i), u64::from(data[i]), "value_at({i})");
    }
    if let Some(last) = data.len().checked_sub(1) {
        assert_eq!(probe.value_at(last), u64::from(data[last]));
    }
}

/// Deterministic generator for the randomized sweeps.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 33
    }
}

fn sorted_random(seed: u64, len: usize, max_step: u64) -> Vec<u32> {
    let mut rng = Lcg(seed);
    let mut value = 0u64;
    (0..len)
        .map(|_| {
            value = (value + rng.next() % (max_step + 1)).min(u64::from(u32::MAX));
            value as u32
        })
        .collect()
}

#[test]
fn probes_bitpacked() {
    let data: Vec<u32> = (0..4096u32).map(|i| i / 7).collect();
    let arr = compress_with(&[&integer::BitPackingScheme], &data);
    assert_probe(&arr, &data, &[NodeKind::BitPacked]);
}

#[test]
fn probes_bitpacked_with_patches() {
    // Mostly narrow values with a sorted tail of outliers above the packed
    // width, so patches carry the tail.
    let mut data: Vec<u32> = (0..3000u32).map(|i| i % 500).collect();
    data.sort_unstable();
    data.extend([100_000, 100_001, 200_000, u32::MAX - 1, u32::MAX]);
    let parr = PrimitiveArray::from_iter(data.iter().copied());
    let packed = vortex_fastlanes::bitpack_compress::bitpack_encode(&parr, 9, None, &mut ctx())
        .unwrap()
        .into_array();
    assert_probe(&packed, &data, &[NodeKind::BitPacked, NodeKind::Patches]);
}

#[test]
fn probes_for_bitpacked() {
    let data: Vec<u32> = (0..4096u32).map(|i| 1_000_000_000 + i / 3).collect();
    let arr = compress_with(&[&integer::FoRScheme, &integer::BitPackingScheme], &data);
    assert_probe(&arr, &data, &[NodeKind::FoR, NodeKind::BitPacked]);
}

#[test]
fn probes_runend_uniform_runs() {
    // Uniform 11-row runs: both ends and values are arithmetic, so the
    // recursive children land on Sequence.
    let data: Vec<u32> = (0..22_000u32).map(|i| i / 11).collect();
    let arr = compress_with(&[&integer::RunEndScheme, &integer::SequenceScheme], &data);
    assert_probe(&arr, &data, &[NodeKind::RunEnd, NodeKind::Sequence]);
}

#[test]
fn probes_runend_bitpacked_children() {
    // Zipf-ish run lengths with both RunEnd children bit-packed, the shape a
    // sorted subject column takes on disk. Built directly: the cascade's
    // cost model may keep small children canonical.
    let mut rng = Lcg(7);
    let mut data = Vec::new();
    let mut value = 0u32;
    while data.len() < 20_000 {
        let run = 1 + (rng.next() % 64) as usize * (rng.next().is_multiple_of(4) as usize * 40 + 1);
        data.extend(std::iter::repeat_n(value, run));
        value += 1 + (rng.next() % 3) as u32;
    }
    let mut ends = Vec::new();
    let mut values = Vec::new();
    for (i, &v) in data.iter().enumerate() {
        if i + 1 == data.len() || data[i + 1] != v {
            ends.push((i + 1) as u32);
            values.push(v);
        }
    }
    let width = |max: u32| u8::try_from(32 - max.leading_zeros()).unwrap();
    let mut exec = ctx();
    let packed_ends = vortex_fastlanes::bitpack_compress::bitpack_encode(
        &PrimitiveArray::from_iter(ends.iter().copied()),
        width(*ends.last().unwrap()),
        None,
        &mut exec,
    )
    .unwrap()
    .into_array();
    let packed_values = vortex_fastlanes::bitpack_compress::bitpack_encode(
        &PrimitiveArray::from_iter(values.iter().copied()),
        width(*values.last().unwrap()),
        None,
        &mut exec,
    )
    .unwrap()
    .into_array();
    let arr = vortex_runend::RunEnd::try_new(packed_ends, packed_values, &mut exec)
        .unwrap()
        .into_array();
    assert_probe(&arr, &data, &[NodeKind::RunEnd, NodeKind::BitPacked]);
}

#[test]
fn probes_sequence() {
    let data: Vec<u32> = (0..5000u32).map(|i| 40 + i * 3).collect();
    let arr = compress_with(&[&integer::SequenceScheme], &data);
    assert_probe(&arr, &data, &[NodeKind::Sequence]);

    let direct =
        Sequence::try_new_typed::<u32>(40, 3, vortex_array::dtype::Nullability::NonNullable, 5000)
            .unwrap()
            .into_array();
    assert_probe(&direct, &data, &[NodeKind::Sequence]);
}

#[test]
fn probes_sequence_zero_multiplier() {
    let data = vec![9u32; 64];
    let arr =
        Sequence::try_new_typed::<u32>(9, 0, vortex_array::dtype::Nullability::NonNullable, 64)
            .unwrap()
            .into_array();
    // A zero multiplier resolves to the constant node.
    assert_probe(&arr, &data, &[NodeKind::Constant]);
}

#[test]
fn probes_constant() {
    let data = vec![7u32; 100];
    let arr = ConstantArray::new(Scalar::from(7u32), 100).into_array();
    assert_probe(&arr, &data, &[NodeKind::Constant]);
}

#[test]
fn probes_slice_wrapper() {
    let data: Vec<u32> = (0..22_000u32).map(|i| i / 11).collect();
    let arr = compress_with(&[&integer::RunEndScheme, &integer::SequenceScheme], &data);
    // Run boundary, mid-run, and non-1024-aligned windows.
    for range in [11..22_000, 5..1000, 1030..2050, 21_990..22_000, 0..0] {
        let sliced = SliceArray::new(arr.clone(), range.clone()).into_array();
        assert_probe(&sliced, &data[range], &[NodeKind::Slice]);
    }
}

#[test]
fn probes_optimizer_sliced() {
    let data: Vec<u32> = (0..4096u32).map(|i| 1_000_000_000 + i / 3).collect();
    let arr = compress_with(&[&integer::FoRScheme, &integer::BitPackingScheme], &data);
    for range in [0..4096usize, 7..4000, 1024..2048, 100..101] {
        // `ArrayRef::slice` may keep a Slice wrapper or push the window into
        // the child; both shapes must probe identically, so no kind assert.
        let sliced = arr.slice(range.clone()).unwrap();
        assert_probe(&sliced, &data[range], &[]);
    }
}

#[test]
fn probes_sliced_runend_offset() {
    let data: Vec<u32> = (0..22_000u32).map(|i| i / 11).collect();
    let arr = compress_with(&[&integer::RunEndScheme, &integer::SequenceScheme], &data);
    for range in [3..15_000usize, 11..22, 12_345..12_346] {
        let sliced = arr.slice(range.clone()).unwrap();
        assert_probe(&sliced, &data[range], &[]);
    }
}

#[test]
fn probes_chunked_mixed_encodings() {
    // Differently-encoded uneven chunks, an empty chunk, and an equal run
    // spanning the second chunk boundary.
    let c1: Vec<u32> = (0..1500u32).map(|i| i / 4).collect(); // ends at 374
    let c2: Vec<u32> = std::iter::repeat_n(374u32, 300).chain(375..2000).collect();
    let c3: Vec<u32> = (0..900u32).map(|i| 2000 + i * 2).collect();
    let chunks: Vec<ArrayRef> = vec![
        compress_with(&[&integer::RunEndScheme, &integer::SequenceScheme], &c1),
        PrimitiveArray::from_iter(std::iter::empty::<u32>()).into_array(),
        compress_with(&[&integer::BitPackingScheme], &c2),
        compress_with(&[&integer::FoRScheme, &integer::SequenceScheme], &c3),
    ];
    let dtype = chunks[0].dtype().clone();
    let arr = ChunkedArray::try_new(chunks, dtype).unwrap().into_array();
    let data: Vec<u32> = c1.into_iter().chain(c2).chain(c3).collect();
    assert_probe(
        &arr,
        &data,
        &[NodeKind::Chunked, NodeKind::RunEnd, NodeKind::BitPacked],
    );
}

#[test]
fn probes_default_cascade_random() {
    let shapes: &[(u64, usize, u64)] = &[
        (1, 5000, 0),      // constant
        (2, 5000, 1),      // slow ramp, long runs
        (3, 5000, 3),      // short runs
        (4, 5000, 1000),   // wide spread
        (5, 20_000, 7),    // longer, moderate runs
        (6, 333, 1 << 20), // short, huge steps
        (7, 5000, 65_536), // u16-crossing steps
        (8, 1, 5),         // single element
    ];
    for &(seed_base, len, max_step) in shapes {
        for salt in 0..8u64 {
            let data = sorted_random(seed_base * 1000 + salt, len, max_step);
            let arr = compress_default(&data);
            // Encodings outside the supported set decline cleanly.
            if SortedProbe::resolve(&arr).is_some() {
                assert_probe(&arr, &data, &[]);
            }
        }
    }
}

#[test]
fn declines_nullable() {
    let arr = PrimitiveArray::from_option_iter([Some(1u32), None, Some(3)]).into_array();
    assert!(SortedProbe::resolve(&arr).is_none());
}

#[test]
fn declines_signed_dtype() {
    let arr = PrimitiveArray::from_iter([1i32, 2, 3]).into_array();
    assert!(SortedProbe::resolve(&arr).is_none());
}

#[test]
fn declines_float_dtype() {
    let arr = PrimitiveArray::from_iter([1.0f32, 2.0]).into_array();
    assert!(SortedProbe::resolve(&arr).is_none());
}

#[test]
fn probes_dict_encoding() {
    let data: Vec<u32> = (0..10_000u32).map(|i| (i / 500) * 10).collect();
    let arr = compress_with(&[&integer::IntDictScheme], &data);
    assert_eq!(
        arr.encoding_id().as_str(),
        "vortex.dict",
        "fixture must produce a dict array"
    );
    assert_probe(&arr, &data, &[NodeKind::Dict]);
}

#[test]
fn probes_dict_permuted_values() {
    // Sorted logical sequence through an unsorted dictionary — the shape a
    // non-order-preserving dict encoder produces on a sorted column.
    let values: Vec<u32> = vec![50, 10, 70, 20, 40];
    let codes: Vec<u16> = vec![1, 1, 1, 3, 3, 4, 4, 4, 0, 2];
    let data: Vec<u32> = codes.iter().map(|&c| values[c as usize]).collect();
    let arr = vortex_array::arrays::DictArray::try_new(
        PrimitiveArray::from_iter(codes).into_array(),
        PrimitiveArray::from_iter(values).into_array(),
    )
    .unwrap()
    .into_array();
    assert_probe(&arr, &data, &[NodeKind::Dict, NodeKind::Primitive]);
}

#[test]
fn probes_through_shared_wrapper() {
    // The writer wraps a values child shared between chunks in a `Shared`
    // lazy wrapper; it is transparent, so the tree above it must still
    // resolve rather than declining wholesale.
    let values: Vec<u32> = vec![10, 20, 30, 40, 50];
    let codes: Vec<u16> = vec![0, 0, 1, 2, 2, 2, 3, 4];
    let data: Vec<u32> = codes.iter().map(|&c| values[c as usize]).collect();
    let shared =
        vortex_array::arrays::SharedArray::new(PrimitiveArray::from_iter(values).into_array())
            .into_array();
    assert_eq!(shared.encoding_id().as_str(), "vortex.shared");
    let arr = vortex_array::arrays::DictArray::try_new(
        PrimitiveArray::from_iter(codes).into_array(),
        shared,
    )
    .unwrap()
    .into_array();
    assert_probe(&arr, &data, &[NodeKind::Dict, NodeKind::Primitive]);
}

#[test]
fn bounds_on_empty_array() {
    let arr = PrimitiveArray::from_iter(std::iter::empty::<u32>()).into_array();
    let probe = SortedProbe::resolve(&arr).unwrap();
    assert!(probe.is_empty());
    assert_eq!(probe.bounds(0), (0, 0));
    assert_eq!(probe.bounds(u64::MAX), (0, 0));
}

#[test]
fn bounds_all_equal() {
    let data = vec![42u32; 9000];
    let arr = compress_default(&data);
    if SortedProbe::resolve(&arr).is_some() {
        assert_probe(&arr, &data, &[]);
    }
}

#[test]
fn bounds_domain_extremes() {
    let data = vec![0u32, 0, 5, u32::MAX, u32::MAX];
    let arr = PrimitiveArray::from_iter(data.iter().copied()).into_array();
    assert_probe(&arr, &data, &[NodeKind::Primitive]);
}

#[test]
fn probes_slice_of_piecewise_sorted() {
    // Sawtooth: each 1000-row run ascends, the column as a whole does not —
    // the shape a prefix probe slices when it searches a second key inside a
    // lead run. Only the window is sorted; the probe must never consult the
    // child's out-of-window order.
    let run_len = 1000usize;
    let mut data: Vec<u32> = Vec::new();
    for run in 0..24u32 {
        data.extend((0..run_len as u32).map(|i| i / 3 + (run % 5) * 40));
    }
    let encodings = [
        compress_with(&[&integer::RunEndScheme, &integer::BitPackingScheme], &data),
        compress_with(&[&integer::BitPackingScheme], &data),
        compress_with(&[&integer::IntDictScheme], &data),
    ];
    for arr in &encodings {
        for wstart in [0usize, 1000, 5000, 23_000] {
            let window = wstart..wstart + run_len;
            let wdata = &data[window.clone()];
            let wrapper = SliceArray::new(arr.clone(), window.clone()).into_array();
            assert_probe(&wrapper, wdata, &[NodeKind::Slice]);
            let pushed = arr.slice(window.clone()).unwrap();
            assert_probe(&pushed, wdata, &[]);

            // The windowed search over the WHOLE (unsorted) column must agree
            // with slicing the window out first — window-only reads.
            let probe = SortedProbe::resolve(arr).unwrap();
            for needle in needles_for(wdata) {
                let (lo, hi) = probe.bounds_in(window.clone(), needle);
                let (clo, chi) = canonical_bounds(wdata, needle);
                assert_eq!(
                    (lo - wstart, hi - wstart),
                    (clo, chi),
                    "bounds_in vs canonical, window {wstart}, needle {needle}"
                );
            }
        }
    }
}

#[test]
fn bounds_in_edge_windows() {
    let data: Vec<u32> = (0..4096u32).map(|i| i / 7).collect();
    let arr = compress_with(&[&integer::BitPackingScheme], &data);
    let probe = SortedProbe::resolve(&arr).unwrap();
    // Empty window, single-row window, and the full array.
    assert_eq!(probe.bounds_in(100..100, 5), (100, 100));
    assert_eq!(probe.bounds_in(70..71, 10), (70, 71));
    assert_eq!(probe.bounds_in(70..71, 11), (71, 71));
    assert_eq!(
        probe.bounds_in(0..data.len(), 100),
        probe.bounds(100),
        "full-window bounds_in must equal bounds"
    );
}
