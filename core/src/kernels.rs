//! Kernels the store session adds to Vortex's own, each for a case Vortex
//! 0.90 executes the slow way. The executor looks a fused execution up by
//! its `(parent, child)` encoding pair; the store's kernels are registered
//! before the default encodings register theirs ([`register`]), so that a
//! kernel for a pair Vortex also covers is tried first, and declines
//! whatever it does not do better, which Vortex's own kernel then takes.
//! Each goes once Vortex covers its case.

use vortex_array::arrays::scalar_fn::{ExactScalarFn, ScalarFnArrayView};
use vortex_array::arrays::slice::{SliceArraySlotsExt as _, SliceKernel as _};
use vortex_array::arrays::{Filter, PrimitiveArray, Slice};
use vortex_array::kernel::ExecuteParentKernel;
use vortex_array::optimizer::kernels::ArrayKernelsExt as _;
use vortex_array::scalar_fn::ScalarFnVTable as _;
use vortex_array::scalar_fn::fns::binary::{Binary, CompareExecuteAdaptor};
use vortex_array::validity::Validity;
use vortex_array::{
    ArrayRef, ArrayVTable as _, ArrayView, ExecutionCtx, IntoArray as _,
    match_each_unsigned_integer_ptype,
};
use vortex_buffer::{BitBuffer, BufferMut};
use vortex_error::VortexResult;
use vortex_fastlanes::{BitPacked, BitPackedArrayExt as _};
use vortex_mask::Mask;
use vortex_runend::{RunEnd, RunEndArrayExt as _, RunEndArraySlotsExt as _};
use vortex_session::VortexSession;

/// Registers the store's kernels with `session`, before the default
/// encodings register theirs: [`RunEndFilter`] is then tried ahead of
/// vortex-runend's own filter kernel.
pub(crate) fn register(session: &VortexSession) {
    let kernels = session.kernels();
    kernels.register_execute_parent_kernel(Binary.id(), Slice, CompareSlicedPatches);
    kernels.register_execute_parent_kernel(Filter.id(), RunEnd, RunEndFilter);
}

/// A comparison against a constant of a slice of a bit-packed array with
/// patches: the slice is taken, patches and all, and compared by the
/// bit-packed comparison, which reads the packed values a block at a time
/// and applies the patches after.
///
/// Vortex 0.90 cannot slice such an array without reading its patches'
/// buffers, so the slice stays lazy, and a comparison finds no kernel for a
/// slice: it decodes the slice and compares it value by value, at about 10
/// instructions a row against 3 to 5. A file's leaf is sliced whenever a
/// filter evaluates part of it (a zone-pruned range, a split that cuts it,
/// an early-exit piece), and a column of dictionary codes bit-packs with
/// patches when a few of its codes pass a power of two: the object column,
/// whose codes start at 0, so that no frame of reference takes it.
#[derive(Debug)]
struct CompareSlicedPatches;

impl ExecuteParentKernel<Slice> for CompareSlicedPatches {
    type Parent = ExactScalarFn<Binary>;

    fn execute_parent(
        &self,
        array: ArrayView<'_, Slice>,
        parent: ScalarFnArrayView<'_, Binary>,
        child_idx: usize,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Option<ArrayRef>> {
        let Some(packed) = array.child().as_opt::<BitPacked>() else {
            return Ok(None);
        };
        let range = array.slice_range().clone();
        // Without patches the slice is taken before execution, and an empty
        // one is the comparison's own case.
        if packed.patches().is_none() || range.is_empty() {
            return Ok(None);
        }
        // Only a comparison against a constant has a bit-packed kernel.
        let against_constant = parent.options.to_compare_operator().is_some()
            && parent
                .slots()
                .get(1 - child_idx.min(1))
                .and_then(Option::as_ref)
                .is_some_and(|other| other.as_constant().is_some());
        if !against_constant {
            return Ok(None);
        }
        let Some(sliced) = BitPacked::slice(packed, range, ctx)? else {
            return Ok(None);
        };
        let Some(sliced) = sliced.as_opt::<BitPacked>() else {
            return Ok(None);
        };
        let compared =
            CompareExecuteAdaptor(BitPacked).execute_parent(sliced, parent, child_idx, ctx)?;
        #[cfg(test)]
        if compared.is_some() {
            hooks::note(hooks::Kernel::CompareSlicedPatches);
        }
        Ok(compared)
    }
}

/// Below this many kept rows vortex-runend's filter takes them directly,
/// which [`RunEndFilter`] leaves to it.
const RUN_FILTER_MIN_KEPT: usize = 25;
/// Below this many kept rows a run vortex-runend's filter takes the kept
/// rows directly, which [`RunEndFilter`] leaves to it.
const RUN_FILTER_MIN_KEPT_PER_RUN: f64 = 0.1;

/// A filter of a run-end array that keeps enough rows a run to filter the
/// runs: each run's kept rows counted as the rank of its end in the mask (the
/// mask's set bits before it), the ranks read in one pass over the mask's
/// 64-bit words as the ends rise.
///
/// Vortex 0.90's run-end filter counts each run's kept rows with a population
/// count of the run's own range of the mask, some 30 instructions a run, more
/// than expanding the runs costs: on the subject column of a sorted store
/// file, runs of a few rows filtered at a dense pattern's masks, a fifth of
/// the read. This kernel declines the filters vortex-runend takes directly
/// (fewer than 25 rows kept, or fewer than one a ten runs), and the masks the
/// filter adaptor answers without a kernel (every row kept, or none).
#[derive(Debug)]
struct RunEndFilter;

impl ExecuteParentKernel<RunEnd> for RunEndFilter {
    type Parent = Filter;

    fn execute_parent(
        &self,
        array: ArrayView<'_, RunEnd>,
        parent: ArrayView<'_, Filter>,
        _child_idx: usize,
        ctx: &mut ExecutionCtx,
    ) -> VortexResult<Option<ArrayRef>> {
        let Some(mask) = parent.filter_mask().values() else {
            return Ok(None);
        };
        let kept = mask.true_count();
        let runs = array.ends().len();
        if kept < RUN_FILTER_MIN_KEPT || (kept as f64 / runs as f64) < RUN_FILTER_MIN_KEPT_PER_RUN {
            return Ok(None);
        }
        let ends = array.ends().clone().execute::<PrimitiveArray>(ctx)?;
        let (offset, len) = (array.offset(), array.len());
        let (kept_ends, kept_runs) = match_each_unsigned_integer_ptype!(ends.ptype(), |P| {
            let ends = ends.as_slice::<P>();
            let mut kept_ends = BufferMut::<P>::with_capacity(ends.len());
            let kept_runs = kept_run_ends(
                ends.len(),
                |run| ends[run] as usize,
                offset,
                len,
                mask.bit_buffer(),
                |rank| kept_ends.push(rank as P),
            );
            (
                PrimitiveArray::new(kept_ends.freeze(), Validity::NonNullable).into_array(),
                kept_runs,
            )
        });
        let values = array.values().filter(kept_runs)?;
        #[cfg(test)]
        hooks::note(hooks::Kernel::RunEndFilter);
        // SAFETY: `kept_run_ends` pushes one end per run that keeps a row,
        // each past the one before and the last `kept`, and `values` holds
        // those runs' values.
        Ok(Some(
            unsafe { RunEnd::new_unchecked(kept_ends, values, 0, kept) }.into_array(),
        ))
    }
}

/// Which of `runs` runs, ending at `end(run)` (in rows from `offset`, over
/// `len` rows), keep a row of `mask`, each run that does pushing its end
/// among the kept rows: the rank of its end in `mask`. One pass over the
/// mask's 64-bit words, as the ends rise.
fn kept_run_ends(
    runs: usize,
    end: impl Fn(usize) -> usize,
    offset: usize,
    len: usize,
    mask: &BitBuffer,
    mut push: impl FnMut(usize),
) -> Mask {
    let mut words = mask.chunks().iter_padded();
    // The word holding bit `64 * at`, and the set bits of the words before.
    let (mut at, mut word, mut before) = (0usize, words.next().unwrap_or(0), 0usize);
    let mut ranked = 0usize;
    BitBuffer::collect_bool(runs, |run| {
        let end = end(run).saturating_sub(offset).min(len);
        while (at + 1) * 64 <= end {
            before += word.count_ones() as usize;
            word = words.next().unwrap_or(0);
            at += 1;
        }
        let rank = before + (word & ((1u64 << (end - at * 64)) - 1)).count_ones() as usize;
        let keeps = rank > ranked;
        if keeps {
            push(rank);
        }
        ranked = rank;
        keeps
    })
    .into()
}

/// Test hook: which of the store's kernels ran on this thread.
#[cfg(test)]
pub(crate) mod hooks {
    use std::cell::RefCell;

    /// One of the store's kernels.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(crate) enum Kernel {
        CompareSlicedPatches,
        RunEndFilter,
    }

    thread_local! {
        static RAN: RefCell<Vec<Kernel>> = const { RefCell::new(Vec::new()) };
    }

    /// Record that `kernel` answered.
    pub(super) fn note(kernel: Kernel) {
        RAN.with_borrow_mut(|ran| ran.push(kernel));
    }

    /// The kernels that answered on this thread since the last call, in
    /// order.
    pub(crate) fn take() -> Vec<Kernel> {
        RAN.with_borrow_mut(std::mem::take)
    }
}

#[cfg(test)]
mod tests {
    use vortex_array::arrays::bool::BoolArrayExt as _;
    use vortex_array::arrays::{BoolArray, ConstantArray, PrimitiveArray, Slice};
    use vortex_array::builtins::ArrayBuiltins as _;
    use vortex_array::scalar_fn::fns::operators::Operator;
    use vortex_array::{ArrayRef, IntoArray as _, VortexSessionExecute as _};
    use vortex_buffer::BitBuffer;
    use vortex_fastlanes::BitPackedArrayExt as _;
    use vortex_fastlanes::bitpack_compress::bitpack_encode;
    use vortex_mask::Mask;
    use vortex_runend::RunEnd;

    use super::hooks::{self, Kernel};
    use super::kept_run_ends;
    use crate::session::VORTEX_SESSION;

    /// A comparison of a slice of a bit-packed array with patches against a
    /// constant answers as the sliced values do, for every comparison, the
    /// slice on either side, over slices in a block and across blocks,
    /// holding patches or not, of one row and of thousands, and the store's
    /// kernel answers it. Vortex leaves such a slice lazy: once it no longer
    /// does, the kernel is left with nothing to do.
    #[test]
    fn a_patched_bit_packed_slice_compares_as_its_values() {
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        // 5,000 codes under 1,000, every 97th one past 5,000: 10-bit packed
        // with patches.
        let values: Vec<u64> = (0..5_000u64)
            .map(|i| {
                if i % 97 == 3 {
                    5_000 + i
                } else {
                    (i * 7) % 1_000
                }
            })
            .collect();
        let packed = bitpack_encode(
            &PrimitiveArray::from_iter(values.iter().copied()),
            10,
            None,
            &mut ctx,
        )
        .unwrap();
        assert!(
            packed.patches().is_some(),
            "the codes past 10 bits are patched"
        );
        let packed = packed.into_array();
        hooks::take();
        for range in [
            3..1_500,
            1_000..2_049,
            1_024..4_096,
            4_000..4_999,
            17..18,
            100..101,
        ] {
            let slice = packed.slice(range.clone()).unwrap();
            assert!(slice.is::<Slice>(), "{range:?}: a patched slice stays lazy");
            for (op, constant) in [
                (Operator::Eq, 7u64),
                (Operator::Eq, 5_100),
                (Operator::NotEq, 7),
                (Operator::Lt, 500),
                (Operator::Lte, 0),
                (Operator::Gt, 4_000),
                (Operator::Gte, 900),
                (Operator::Eq, 123_456),
            ] {
                let constants = ConstantArray::new(constant, range.len()).into_array();
                // The slice on either side of the comparison.
                for slice_first in [true, false] {
                    let label = format!("{range:?} {op:?} {constant}, slice first {slice_first}");
                    let compared = if slice_first {
                        slice.binary(constants.clone(), op)
                    } else {
                        constants.binary(slice.clone(), op)
                    };
                    let compared = compared.unwrap().execute::<BoolArray>(&mut ctx).unwrap();
                    let want: Vec<bool> = values[range.clone()]
                        .iter()
                        .map(|&v| {
                            let (l, r) = if slice_first {
                                (v, constant)
                            } else {
                                (constant, v)
                            };
                            match op {
                                Operator::Eq => l == r,
                                Operator::NotEq => l != r,
                                Operator::Lt => l < r,
                                Operator::Lte => l <= r,
                                Operator::Gt => l > r,
                                Operator::Gte => l >= r,
                                _ => unreachable!("a comparison"),
                            }
                        })
                        .collect();
                    let got: Vec<bool> = compared.to_bit_buffer().iter().collect();
                    assert_eq!(got, want, "{label}: the comparison");
                    assert_eq!(
                        hooks::take(),
                        [Kernel::CompareSlicedPatches],
                        "{label}: the kernel"
                    );
                }
            }
        }
    }

    /// A deterministic stream of pseudo-random numbers (xorshift).
    struct Rng(u64);

    impl Rng {
        fn below(&mut self, n: u64) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0 % n
        }
    }

    /// The runs a filter keeps, and their ends among the kept rows, are
    /// vortex-runend's: runs of one row to dozens, from an offset into the
    /// first and cut short of the last, under masks of every density, of
    /// lengths off a word's multiple and starting off a byte.
    #[test]
    fn kept_run_ends_are_vortexs() {
        let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
        for case in 0..400 {
            let runs = 1 + rng.below(300) as usize;
            let longest = 1 + rng.below(40);
            let mut end = 0u32;
            let ends: Vec<u32> = (0..runs)
                .map(|_| {
                    end += 1 + rng.below(longest) as u32;
                    end
                })
                .collect();
            let offset = rng.below(u64::from(ends[0])) as usize;
            let len = end as usize
                - offset
                - rng
                    .below(u64::from(*ends.last().unwrap()) - offset as u64)
                    .min(5) as usize;
            let density = rng.below(101);
            let skip = rng.below(13) as usize;
            let bits = BitBuffer::from_iter((0..skip + len).map(|_| rng.below(100) < density));
            let mask = bits.slice(skip..skip + len);
            let (want_ends, want_runs) = vortex_runend::_benchmarking::filter_run_end_primitive(
                &ends,
                offset as u64,
                len as u64,
                &mask,
            )
            .unwrap();
            let mut got_ends = Vec::new();
            let got_runs = kept_run_ends(
                ends.len(),
                |run| ends[run] as usize,
                offset,
                len,
                &mask,
                |rank| got_ends.push(rank as u32),
            );
            assert_eq!(
                got_ends,
                want_ends.as_slice::<u32>(),
                "case {case}: the kept ends"
            );
            assert_eq!(got_runs, want_runs, "case {case}: the kept runs");
        }
    }

    /// A filter of a run-end array keeping enough rows a run is answered by
    /// the store's kernel, ahead of vortex-runend's; one keeping fewer than
    /// 25 rows, or fewer than one a ten runs, is left to vortex-runend's
    /// take. Either keeps the rows the decoded values keep.
    #[test]
    fn a_dense_run_end_filter_runs_the_store_kernel() {
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let values: Vec<u64> = (0..10_000u64).map(|i| i / 10).collect();
        let runs: ArrayRef = RunEnd::encode(
            PrimitiveArray::from_iter(values.iter().copied()).into_array(),
            &mut ctx,
        )
        .unwrap()
        .into_array();
        hooks::take();
        for (every, kernel) in [(9, true), (3, true), (150, false), (1_000, false)] {
            let mask = Mask::from_iter((0..values.len()).map(|i| i % every == 2));
            let kept = runs
                .filter(mask)
                .unwrap()
                .execute::<PrimitiveArray>(&mut ctx)
                .unwrap();
            let want: Vec<u64> = values.iter().copied().skip(2).step_by(every).collect();
            assert_eq!(
                kept.as_slice::<u64>(),
                want,
                "every {every}th row: the kept rows"
            );
            assert_eq!(
                hooks::take().contains(&Kernel::RunEndFilter),
                kernel,
                "every {every}th row: the kernel"
            );
        }
    }
}
