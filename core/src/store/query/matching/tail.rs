//! The append tail's match: a compare over its selected rows, or over the
//! rows its keys name for a fully bound pattern.

use vortex_array::ArrayRef;
use vortex_array::scalar::Scalar;
use vortex_mask::Mask;

use crate::debug;
use crate::error::Result;
use crate::observe::observe;
use crate::store::array::bool_array_to_mask;
use crate::store::layouts::{Constraints, Prepared, QuadPattern, ResolvedLayout};
use crate::store::scan::residual;
use crate::store::scan::typed_eq::typed_positions;
use crate::store::schema;
use crate::store::view::selection::RowSelection;
use crate::store::{Tail, VortexRdfStore};

impl VortexRdfStore {
    /// A tail narrowed to the rows matching the pattern, under the tail's own
    /// layout: a compare over its selected rows (a tail is small, unsorted
    /// and unindexed), whose surviving positions refine the tail-local
    /// selection as base matches refine the base's. A fully bound pattern
    /// compares only the rows its key names.
    pub(super) async fn match_tail(tail: &Tail, pattern: QuadPattern<'_>) -> Result<Tail> {
        let t = debug::timer();
        let carry = |selection: RowSelection| tail.with_selection(selection);
        if tail.selection.is_empty(tail.rows.len()) {
            return Ok(carry(tail.selection.clone()));
        }
        // The tail's layouts store plain strings (see `Tail::layout_for`), so
        // this prelude resolves nothing, never suspends and is never empty;
        // it still mints the witness, as for every probe surface.
        let layout = ResolvedLayout::from(tail.layout);
        let Prepared::Codes(mut codes) = layout.prepare_pattern(pattern).await? else {
            return Ok(carry(RowSelection::empty()));
        };
        let eqs = match codes.constraints(pattern)? {
            // The tail's string layouts never compile to AlwaysFalse (see
            // `Tail::layout_for`); the arm stays for totality.
            Constraints::AlwaysFalse => return Ok(carry(RowSelection::empty())),
            Constraints::Eq(eqs) => eqs,
        };
        if eqs.is_empty() {
            // Unconstrained pattern: every selected tail row matches.
            return Ok(carry(tail.selection.clone()));
        }
        // A fully bound pattern (every `contains`, so every presence check
        // `add_quads` makes) compares only the selected rows the tail's keys
        // name for it ([`TailKeys`](crate::store::view::tail_keys::TailKeys)),
        // instead of every selected row; a row that only shares the key is
        // dropped by the compare.
        let compared = match tail.keys.candidates(&eqs) {
            Some(candidates) => {
                observe!(TailMatch { keyed: true });
                let named: Vec<schema::RowId> = candidates
                    .into_iter()
                    .filter(|&row| tail.selection.contains(row))
                    .collect();
                RowSelection::from_ids(vortex_buffer::Buffer::from(named))
            }
            None => {
                observe!(TailMatch { keyed: false });
                tail.selection.clone()
            }
        };
        let matched = Self::tail_rows_holding(&tail.rows, compared, &eqs)?;
        debug::step!(t, "[match_pattern] Tail matched");
        Ok(carry(matched))
    }

    /// The rows of `selection` (tail-local ids into `rows`) that hold every
    /// equality of `eqs`: the tail's string columns compared as raw bytes
    /// where they can be ([`typed_positions`]), without the per-call
    /// compare/canonicalize pipeline, else by the mask scan.
    pub(super) fn tail_rows_holding(
        rows: &ArrayRef,
        selection: RowSelection,
        eqs: &[(&'static str, Scalar)],
    ) -> Result<RowSelection> {
        if selection.is_empty(rows.len()) {
            return Ok(selection);
        }
        let applied = selection.apply(rows)?;
        if let Some(positions) = typed_positions(&applied, eqs) {
            let mask = Mask::from_indices(applied.len(), positions);
            return Ok(selection.refine(&mask));
        }
        let mask = residual::mask_for(&applied, eqs)?;
        Ok(selection.refine(&bool_array_to_mask(mask)?))
    }
}
