//! A pattern chained on a narrowed served view: a window, keep or partition
//! of a by-copy index's match, or such a view chained before. The view's
//! rows are compared with the pattern's bound terms on the copy's own
//! columns (codes or strings) at those rows, in the index's order, and the
//! matching ones kept on the serve plan, so the result reads off the index
//! in its order too ("index order, filtered"), as a keep does.

use vortex_array::scalar::Scalar;
use vortex_buffer::Buffer;
use vortex_mask::Mask;

use crate::error::Result;
use crate::store::VortexRdfStore;
use crate::store::array::bool_array_to_mask;
use crate::store::indexes::{LazyRowIds, ServedRows};
use crate::store::layouts::{Prepared, QuadPattern};
use crate::store::scan::residual::mask_for;
use crate::store::schema::RowId;
use crate::store::view::selection::ViewSelection;

impl VortexRdfStore {
    /// `pattern` matched over the base of a view a by-copy index serves
    /// narrowed (a window, keep or partition of a served match): each bound
    /// role's probe value, as the copy's columns spell it, compared with the
    /// copy's column at the view's live rows in the index's order (the
    /// component's columns in memory; on file the child's, point by point or
    /// over the rows they span), the matching rows kept on the plan with
    /// their row ids unsorted. A term the layout proves absent, or no row
    /// matching, gives the empty view; a pattern binding nothing, the view as
    /// it is. `None` for any other view (no serve plan, or one covering the
    /// match's whole run), which matches as before. The tail is left as it
    /// is: [`match_pattern_for`](Self::match_pattern_for) matches it.
    pub(super) async fn served_chain(&self, pattern: QuadPattern<'_>) -> Result<Option<Self>> {
        if !self.is_narrowed_served() {
            return Ok(None);
        }
        let Prepared::Codes(mut codes) = self.layout.prepare_pattern(pattern).await? else {
            return Ok(Some(self.empty_view()));
        };
        let mut eqs: Vec<(&'static str, Scalar)> = Vec::new();
        for term in pattern.bound_roles() {
            let Some(value) = codes.probe_scalar(term)? else {
                return Ok(Some(self.empty_view()));
            };
            eqs.push((term.role().name(), value));
        }
        if eqs.is_empty() {
            return Ok(Some(self.clone()));
        }
        #[cfg(feature = "file-io")]
        if let Some(chained) = self.served_chain_file(&eqs).await? {
            return Ok(Some(chained));
        }
        self.served_chain_in_memory(&eqs)
    }

    /// Whether a serve plan narrowed below its match's run serves this view:
    /// what [`served_chain`](Self::served_chain) takes, known without I/O.
    pub(super) fn is_narrowed_served(&self) -> bool {
        #[cfg(feature = "file-io")]
        if let Some(base) = self.quads.file() {
            return base.serve.as_ref().is_some_and(|plan| plan.is_narrowed());
        }
        self.quads
            .in_memory()
            .and_then(|memory| memory.serve.as_ref())
            .is_some_and(|plan| plan.is_narrowed())
    }

    /// [`served_chain`](Self::served_chain) of an in-memory view, whose plan
    /// is narrowed: `eqs` compared on the component's columns at the view's
    /// live rows.
    fn served_chain_in_memory(&self, eqs: &[(&'static str, Scalar)]) -> Result<Option<Self>> {
        let Some(memory) = self.quads.in_memory() else {
            return Ok(None);
        };
        let Some(plan) = &memory.serve else {
            return Ok(None);
        };
        let (positions, rids) = plan.live_rows(self.quads.deleted.as_ref())?;
        if positions.is_empty() {
            return Ok(Some(self.empty_view()));
        }
        let positions: Buffer<u64> = positions.iter().map(|&p| p as u64).collect();
        let names: Vec<&'static str> = eqs.iter().map(|(name, _)| *name).collect();
        let read = plan.columns_at(&names, &positions)?;
        let kept = rows_holding(
            &bool_array_to_mask(mask_for(&read, eqs)?)?,
            &positions,
            &rids,
        );
        if kept.len() == 0 {
            return Ok(Some(self.empty_view()));
        }
        // The kept ids leave out every tombstone: the chain skipped them.
        let ids = LazyRowIds::from_unsorted_ids(kept.rids.clone(), true);
        let quads = self.quads.in_memory_with(
            memory,
            ViewSelection::pending(ids),
            Some(plan.with_rows(kept)),
        );
        Ok(Some(self.derived(quads, self.tail.clone())))
    }

    /// [`served_chain`](Self::served_chain) of a file view, whose plan is
    /// narrowed: `eqs` compared on the child's columns at the view's live
    /// rows. `None` off file.
    #[cfg(feature = "file-io")]
    async fn served_chain_file(&self, eqs: &[(&'static str, Scalar)]) -> Result<Option<Self>> {
        let Some(base) = self.quads.file() else {
            return Ok(None);
        };
        let Some(plan) = &base.serve else {
            return Ok(None);
        };
        let rows = plan
            .live_rows(&base.file, self.quads.deleted.as_ref())
            .await?;
        if rows.len() == 0 {
            return Ok(Some(self.empty_view()));
        }
        let names: Vec<&'static str> = eqs.iter().map(|(name, _)| *name).collect();
        let read = plan.columns_at(&base.file, &names, &rows.positions).await?;
        let kept = rows_holding(
            &bool_array_to_mask(mask_for(&read, eqs)?)?,
            &rows.positions,
            &rows.rids,
        );
        if kept.len() == 0 {
            return Ok(Some(self.empty_view()));
        }
        // The kept ids leave out every tombstone: the chain skipped them.
        let ids = LazyRowIds::from_unsorted_ids(kept.rids.clone(), true);
        let quads = self.quads.file_with(
            base,
            base.filter.clone(),
            ViewSelection::pending(ids),
            Some(plan.with_rows(kept)),
        );
        Ok(Some(self.derived(quads, self.tail.clone())))
    }
}

/// The rows `holding` sets, of the rows at `positions` with row ids `rids`,
/// in their order.
fn rows_holding(holding: &Mask, positions: &[u64], rids: &[RowId]) -> ServedRows {
    let (positions, rids): (Vec<u64>, Vec<RowId>) = (0..positions.len())
        .filter(|&row| holding.value(row))
        .map(|row| (positions[row], rids[row]))
        .unzip();
    ServedRows {
        positions: Buffer::from(positions),
        rids: Buffer::from(rids),
    }
}
