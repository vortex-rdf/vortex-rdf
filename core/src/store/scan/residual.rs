//! The residual compare over an in-memory base: the rows of a selection
//! where every equality a pattern left after its fast paths holds
//! ([`residual_rows`]: the zones whose code bounds cannot hold a code
//! skipped, then the typed fast path or the mask scan over the rows left),
//! and the mask of an array's rows holding them ([`mask_for`]), which the
//! tail's match compares with too. Functions of the base, with no store
//! state: the match narrows a selection with them, and a residual compare
//! left pending runs them piece by piece
//! ([`PendingResidual`](crate::store::view::pending::PendingResidual)).

use vortex_array::arrays::StructArray;
use vortex_array::arrays::constant::ConstantArray;
use vortex_array::arrays::struct_::StructArrayExt;
use vortex_array::builtins::ArrayBuiltins;
use vortex_array::scalar::Scalar;
use vortex_array::scalar_fn::fns::operators::Operator;
use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute};
use web_time::Instant;

use crate::debug;
use crate::error::Result;
use crate::io::zone_map::ZONE_ROWS;
use crate::session::VORTEX_SESSION;
use crate::store::MemoryBase;
use crate::store::array::{bool_array_to_mask, into_struct_array};
use crate::store::scan::typed_eq::typed_residual_ids;
use crate::store::view::selection::RowSelection;

/// The rows of `selection` (of `memory`'s base) where every equality of
/// `eqs` holds: the zones whose code bounds cannot hold a constrained
/// code skipped, then the typed fast path or the mask scan over the rows
/// left.
pub(crate) fn residual_rows(
    memory: &MemoryBase,
    selection: RowSelection,
    eqs: &[(&'static str, Scalar)],
    t: Option<Instant>,
) -> Result<RowSelection> {
    let MemoryBase {
        base,
        probes,
        zones,
        ..
    } = memory;
    let struct_arr = into_struct_array(base.clone())?;
    // The zones whose code bounds cannot hold a constrained code are
    // skipped, as a file's zone maps skip them. Every row or a range has
    // the zone runs left inside it compared run by run, each a slice of
    // the base, with no id list of the rows left (those runs are many
    // where a code's zones scatter); an id list keeps only its ids in them.
    let mut selection = selection;
    if selection.len(base.len()) > ZONE_ROWS
        && let Some(runs) = zones.runs_holding(eqs, base.len())
    {
        #[cfg(test)]
        zone_hooks::note_narrowed();
        debug::step!(t, "[match_pattern] In-memory narrowed by zone bounds");
        match selection.clip_runs(&runs) {
            Some(runs) => {
                let ids = runs_matching(base, &runs, eqs)?;
                return Ok(RowSelection::from_ids(vortex_buffer::Buffer::from(ids)));
            }
            None => selection = selection.intersect_runs(&runs),
        }
        if selection.is_empty(base.len()) {
            return Ok(selection);
        }
    }
    // Typed fast path: residual equalities over canonical code columns
    // (Dictionary layout) become slice loops yielding exact base ids,
    // with no slice/compare/mask pipeline, and a probe-bound column reads
    // through the store's own probe. Only the selection's rows are
    // tested, so the ids are already the intersection.
    Ok(
        match typed_residual_ids(
            &struct_arr,
            &selection,
            base.len(),
            eqs,
            Some((base, probes)),
        ) {
            Some(ids) => {
                debug::step!(
                    t,
                    "[match_pattern] In-memory narrowed by typed residual scan"
                );
                RowSelection::Ids(ids)
            }
            None => {
                let mask = mask_for(&selection.apply(base)?, eqs)?;
                let narrowed = selection.refine(&bool_array_to_mask(mask)?);
                debug::step!(t, "[match_pattern] In-memory narrowed by mask scan");
                narrowed
            }
        },
    )
}

/// The base rows of `runs` that satisfy `eqs`, ascending: each run's
/// slice of the base compared on the constrained columns alone
/// ([`mask_for`]).
fn runs_matching(
    base: &ArrayRef,
    runs: &[std::ops::Range<u64>],
    eqs: &[(&'static str, Scalar)],
) -> Result<Vec<u64>> {
    let mut ids = Vec::new();
    for run in runs {
        let rows = base.slice(run.start as usize..run.end as usize)?;
        match bool_array_to_mask(mask_for(&rows, eqs)?)?.bit_buffer() {
            vortex_mask::AllOr::All => ids.extend(run.clone()),
            vortex_mask::AllOr::None => {}
            vortex_mask::AllOr::Some(bits) => {
                bits.for_each_set_index(|i| ids.push(run.start + i as u64));
            }
        }
    }
    Ok(ids)
}

/// A boolean mask, one bit per row of `array` in its own order, of the
/// rows that satisfy `eqs`: the per-column equalities a pattern compiled
/// to under the layout of `array`'s rows (the store's for the base, the
/// tail's for the tail; see [`Tail::layout_for`](crate::store::Tail::layout_for)). The mask is
/// positional, so a caller holding a view maps it back to base row ids
/// with [`RowSelection::refine`]. The fallback of the typed comparison
/// paths: callers pass in the constraints they compiled to try those.
pub(crate) fn mask_for(array: &ArrayRef, eqs: &[(&'static str, Scalar)]) -> Result<ArrayRef> {
    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let struct_arr = array.clone().execute::<StructArray>(&mut ctx)?;

    // One equality comparison per constrained column, ANDed.
    let mut mask: Option<ArrayRef> = None;
    for (field, value) in eqs {
        let col = struct_arr.unmasked_field_by_name(field)?;
        // Cast to the column's dtype, so a numeric column such as
        // `o_kind` compares against a scalar of its type and nullability.
        let scalar = value.cast(col.dtype())?;
        let rhs = ConstantArray::new(scalar, col.len()).into_array();
        let m = col.binary(rhs, Operator::Eq)?;
        mask = Some(match mask.take() {
            Some(prev) => prev.binary(m, Operator::And)?,
            None => m,
        });
    }
    Ok(match mask {
        Some(mask) => mask,
        // An unconstrained pattern matches every row. Both callers gate on
        // a non-empty constraint set, so the fold always starts.
        None => ConstantArray::new(Scalar::from(true), struct_arr.len()).into_array(),
    })
}

/// Test hook: how many in-memory residual filters this thread narrowed to
/// the zones their codes' bounds hold.
#[cfg(test)]
pub(crate) mod zone_hooks {
    use std::cell::Cell;

    thread_local! {
        static NARROWED: Cell<usize> = const { Cell::new(0) };
    }

    /// Record one residual filter narrowed by zone bounds.
    pub(super) fn note_narrowed() {
        NARROWED.set(NARROWED.get() + 1);
    }

    /// The residual filters narrowed by zone bounds on this thread so far.
    pub(crate) fn narrowed() -> usize {
        NARROWED.get()
    }
}
