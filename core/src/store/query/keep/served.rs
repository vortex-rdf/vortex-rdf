//! Keeps of a served view: the view's rows tested in its index's order, the
//! admitted ones kept on its serve plan, so the kept view reads off the
//! index in the index's order too ("index order, filtered").

use super::Keep;
use crate::error::Result;
use crate::store::VortexRdfStore;
use crate::store::indexes::LazyRowIds;
use crate::store::schema::{QuadColumn, TermCode};
use crate::store::view::selection::ViewSelection;

impl VortexRdfStore {
    /// [`keep`](Self::keep) of a view an in-memory plan serves, served too:
    /// the view's live rows tested in its index's order against `column`'s
    /// codes (the base's code column at their row ids, else the serving
    /// component's at their positions), the admitted ones kept on the plan
    /// with their row ids unsorted. None admitted is the empty view. `None`
    /// for a view no in-memory plan serves, or whose column neither the
    /// base's code cache nor the component's probes read.
    pub(super) fn served_keep(&self, column: QuadColumn, keep: &Keep) -> Result<Option<Self>> {
        let Some(memory) = self.quads.in_memory() else {
            return Ok(None);
        };
        let Some(plan) = &memory.serve else {
            return Ok(None);
        };
        let deleted = self.quads.deleted.as_ref();
        let rows = plan.kept().map_or(plan.range().len(), |kept| kept.len());
        let test = keep.test(rows);
        let kept = match self.base_code_primitive(column) {
            Some(codes) => {
                let codes = codes.as_slice::<TermCode>();
                plan.kept_rows(deleted, |_, rid| test.admits(codes[rid as usize]))?
            }
            None => match plan.component_probe(column.name()) {
                Some(codes) => {
                    plan.kept_rows(deleted, |position, _| test.admits(codes.value_at(position)))?
                }
                None => return Ok(None),
            },
        };
        if kept.len() == 0 {
            return Ok(Some(self.empty_view()));
        }
        // The kept ids leave out every tombstone: the keep skipped them.
        let ids = LazyRowIds::from_unsorted_ids(kept.rids.clone(), true);
        let quads = self.quads.in_memory_with(
            memory,
            ViewSelection::pending(ids),
            Some(plan.with_rows(kept)),
        );
        Ok(Some(self.derived(quads, self.tail.clone())))
    }
}
