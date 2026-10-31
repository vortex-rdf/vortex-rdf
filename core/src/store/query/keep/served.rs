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
    /// [`keep`](Self::keep) of a served view, served too. In memory:
    /// the view's live rows tested in its index's order against `column`'s
    /// codes (the base's code column at their row ids, else the serving
    /// component's at their positions), the admitted ones kept on the plan
    /// with their row ids unsorted. None admitted is the empty view. `None`
    /// for a view no in-memory plan serves, or whose column neither the
    /// base's code cache nor the component's probes read.
    pub(super) async fn served_keep(
        &self,
        column: QuadColumn,
        keep: &Keep,
    ) -> Result<Option<Self>> {
        #[cfg(feature = "file-io")]
        if self.quads.file().is_some() {
            return self.served_keep_file(column, keep).await;
        }
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

    /// [`keep`](Self::keep) of a view a file plan serves, served too: the
    /// view's live rows tested in the child's order against the child's
    /// `column` (read over its located range, or at its kept rows'
    /// positions), the admitted ones kept on the plan with their row ids
    /// unsorted (an unlocated run located first, read once by row index).
    /// None admitted is the empty view. `None` for a view no file plan
    /// serves.
    #[cfg(feature = "file-io")]
    async fn served_keep_file(&self, column: QuadColumn, keep: &Keep) -> Result<Option<Self>> {
        use vortex_buffer::Buffer;

        use crate::store::indexes::{COL_RID, ServedRows, base_row};
        use crate::store::read::chunks::extract_codes;
        use crate::store::schema::RowId;

        let Some(base) = self.quads.file() else {
            return Ok(None);
        };
        let Some(plan) = &base.serve else {
            return Ok(None);
        };
        let plan = &plan.located().await?;
        let deleted = self.quads.deleted.as_ref();
        let name = column.name();
        // The rows' codes in `column` beside their positions and row ids: the
        // kept rows', or the located range's.
        let (positions, codes, rids): (Option<Buffer<u64>>, Buffer<u64>, Buffer<RowId>) =
            match plan.kept() {
                None => {
                    let mut columns = plan.range_columns(&base.file, &[name, COL_RID]).await?;
                    let rids = columns.remove(1).iter().map(|&rid| base_row(rid)).collect();
                    (None, columns.remove(0), rids)
                }
                Some(kept) => {
                    if kept.len() == 0 {
                        return Ok(Some(self.empty_view()));
                    }
                    let read = plan
                        .columns_at(&base.file, &[name], &kept.positions)
                        .await?;
                    let codes = extract_codes(read, &[name])?.remove(0);
                    (Some(kept.positions.clone()), codes, kept.rids.clone())
                }
            };
        let start = plan.located_range().map_or(0, |range| range.start);
        let position = |row: usize| positions.as_ref().map_or(start + row as u64, |p| p[row]);
        let test = keep.test(codes.len());
        let (kept_positions, kept_rids): (Vec<u64>, Vec<RowId>) = (0..codes.len())
            .filter(|&row| {
                deleted.is_none_or(|d| !d.value(rids[row] as usize)) && test.admits(codes[row])
            })
            .map(|row| (position(row), rids[row]))
            .unzip();
        if kept_rids.is_empty() {
            return Ok(Some(self.empty_view()));
        }
        let rows = ServedRows {
            positions: Buffer::from(kept_positions),
            rids: Buffer::from(kept_rids),
        };
        // The kept ids leave out every tombstone: the keep skipped them.
        let ids = LazyRowIds::from_unsorted_ids(rows.rids.clone(), true);
        let quads = self.quads.file_with(
            base,
            base.filter.clone(),
            ViewSelection::pending(ids),
            Some(plan.with_rows(rows)),
        );
        Ok(Some(self.derived(quads, self.tail.clone())))
    }
}
