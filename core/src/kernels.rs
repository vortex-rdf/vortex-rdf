//! Kernels the store session adds to Vortex's own, each for a case Vortex
//! 0.90 executes the slow way. The executor looks a fused execution up by
//! its `(parent, child)` encoding pair; the store's kernels are registered
//! before the default encodings register theirs ([`register`]), so that a
//! kernel for a pair Vortex also covers is tried first, and declines
//! whatever it does not do better, which Vortex's own kernel then takes.
//! Each goes once Vortex covers its case.

use vortex_array::arrays::Slice;
use vortex_array::arrays::scalar_fn::{ExactScalarFn, ScalarFnArrayView};
use vortex_array::arrays::slice::{SliceArraySlotsExt as _, SliceKernel as _};
use vortex_array::kernel::ExecuteParentKernel;
use vortex_array::optimizer::kernels::ArrayKernelsExt as _;
use vortex_array::scalar_fn::ScalarFnVTable as _;
use vortex_array::scalar_fn::fns::binary::{Binary, CompareExecuteAdaptor};
use vortex_array::{ArrayRef, ArrayView, ExecutionCtx};
use vortex_error::VortexResult;
use vortex_fastlanes::{BitPacked, BitPackedArrayExt as _};
use vortex_session::VortexSession;

/// Registers the store's kernels with `session`, before the default
/// encodings register theirs.
pub(crate) fn register(session: &VortexSession) {
    let kernels = session.kernels();
    kernels.register_execute_parent_kernel(Binary.id(), Slice, CompareSlicedPatches);
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

/// Test hook: which of the store's kernels ran on this thread.
#[cfg(test)]
pub(crate) mod hooks {
    use std::cell::RefCell;

    /// One of the store's kernels.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub(crate) enum Kernel {
        CompareSlicedPatches,
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
    use vortex_array::{IntoArray as _, VortexSessionExecute as _};
    use vortex_fastlanes::BitPackedArrayExt as _;
    use vortex_fastlanes::bitpack_compress::bitpack_encode;

    use super::hooks::{self, Kernel};
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
}
