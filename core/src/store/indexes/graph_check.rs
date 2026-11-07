//! The graph check of an in-memory index run bound to a graph
//! ([`GraphCheck`]): the run's rows are contiguous in its component, so the
//! component's own graph column checks them zone by zone, as a file's located
//! run checks its rows, in place of the graph compared at the base rows the
//! run lists, scattered over the base.
//!
//! The check is the residual compare over the component's rows
//! ([`ResidualCompare`]), with the graph its one equality and the run its
//! selection: each zone the run touches is decided by the component's zone
//! map of its graph column, a one-check [`ZoneSet`]. A mixed zone is counted
//! from its recorded graphs when the run holds the whole zone, else on the
//! graph column's encoding (`count_eq`), else by comparing its graphs; its
//! matching rows are found on the encoding (`eq_offsets`). A window of a run
//! whose ids ascend skips the zones before it by their counts and reads only
//! the zones it spans. The matches' positions map to row ids through the
//! run's slice of the `rid` column, each zone holding matches decoded once.

use std::ops::Range;
use std::sync::Arc;

use vortex_array::ArrayRef;
use vortex_array::scalar::Scalar;
use vortex_buffer::{Buffer, BufferMut};

use super::components::{IndexComponent, run_order_row_ids};
use crate::error::Result;
use crate::store::probes::StructProbes;
use crate::store::read::live::Live;
use crate::store::scan::residual::{Classes, ResidualCompare, ResidualTable};
use crate::store::schema::{COL_G, RowId};
use crate::store::view::selection::{RowSelection, SelectedRows};
use crate::zone_stats::zone_map::{ColumnZones, ZONE_ROWS};
use crate::zone_stats::zone_set::{ZoneCheck, ZoneSet};

/// A run's check of its bound graph inside its component: the residual
/// compare of the component's graph column against the graph's code over
/// the run's rows of the component.
pub(crate) struct GraphCheck {
    /// The run's rows of the component.
    rows: Range<usize>,
    /// The component's rows, whose address its probes are resolved for.
    table: ArrayRef,
    /// The component's encoded-search probes over its columns.
    probes: Arc<StructProbes>,
    /// The run's rows, as the compare's selection.
    selection: RowSelection,
    /// The one equality: the graph column against the bound graph's code.
    eqs: [(&'static str, Scalar); 1],
    /// The graph checked against the zone map of the component's graph
    /// column, with each zone's recorded graphs, as bit 0.
    classes: Classes,
}

impl GraphCheck {
    /// The check of the graph `code` over `component`'s rows `rows` (a run,
    /// not empty), whose graph column's zone map is `zones`.
    pub(crate) fn new(
        component: &IndexComponent,
        rows: Range<usize>,
        code: u64,
        zones: Arc<ColumnZones>,
    ) -> Result<Self> {
        let set = ZoneSet::new(
            vec![ZoneCheck {
                bit: 0,
                code,
                zones,
            }],
            0,
        )
        .expect("one check is of one geometry");
        Ok(Self {
            selection: RowSelection::Range(rows.start as u64..rows.end as u64),
            rows,
            table: component.rows()?.as_ref().clone(),
            probes: component.probes_arc(),
            eqs: [(COL_G, Scalar::from(code))],
            classes: Classes::Zoned(set),
        })
    }

    /// The compare of the run's graph.
    fn compare(&self) -> ResidualCompare<'_> {
        ResidualCompare {
            table: ResidualTable {
                rows: &self.table,
                probes: &self.probes,
                component: true,
            },
            selection: &self.selection,
            eqs: &self.eqs,
            classes: &self.classes,
            deleted: None,
            t: None,
        }
    }

    /// How many of the run's rows hold the graph.
    pub(crate) fn count(&self) -> Result<usize> {
        self.compare().count(None)
    }

    /// `min(matches, limit)` at least, and never more than the matches: the
    /// zones counted in run order only until `limit` matches are, so a
    /// capped count stops at the zone that meets its cap.
    pub(crate) fn count_capped(&self, limit: usize) -> Result<usize> {
        self.compare().count(Some(limit))
    }

    /// The ids of the run's rows that hold the graph, in run order, `rids`
    /// being the run's slice of the rid column: the zones of the graph alone
    /// decoded whole, the mixed ones' matching rows picked, and a zone
    /// holding none of the graph's rows neither read nor decoded.
    pub(crate) fn ids(&self, rids: &ArrayRef) -> Result<Buffer<RowId>> {
        let matched = self.compare().rows()?;
        self.ids_at(rids, &matched)
    }

    /// The ids of the run's matches `offset..offset + limit`, in run order,
    /// `rids` being the run's slice of the rid column, beside the run's
    /// matches as far as they are known ([`Live`]). The zones are counted in
    /// run order only until the window fills, those before it skipped by
    /// their counts, and only the zones it spans read; a window of no rows
    /// reads nothing.
    pub(crate) fn window(
        &self,
        rids: &ArrayRef,
        offset: usize,
        limit: usize,
    ) -> Result<(Buffer<RowId>, Live)> {
        let (positions, live) = self.compare().window(offset, limit)?;
        Ok((self.ids_at(rids, &RowSelection::Ids(positions))?, live))
    }

    /// The row ids `rids` (the run's slice of the rid column) lists at the
    /// component rows `matched`, in order: a run's decoded at once, an id
    /// list's a zone at a time, each zone's span of them decoded once.
    fn ids_at(&self, rids: &ArrayRef, matched: &RowSelection) -> Result<Buffer<RowId>> {
        let start = self.rows.start as u64;
        let in_run = |rows: Range<u64>| (rows.start - start) as usize..(rows.end - start) as usize;
        let positions = match matched.as_rows(self.table.len()) {
            SelectedRows::Run(run) if run.is_empty() => return Ok(Buffer::empty()),
            SelectedRows::Run(run) => {
                return run_order_row_ids(rids.slice(in_run(run.start as u64..run.end as u64))?);
            }
            SelectedRows::Ids(positions) => positions,
        };
        let zone = ZONE_ROWS as u64;
        let mut ids = BufferMut::with_capacity(positions.len());
        let mut at = 0;
        while at < positions.len() {
            let first = positions[at];
            let zone_end = (first / zone + 1) * zone;
            let end = at + positions[at..].partition_point(|&row| row < zone_end);
            let decoded = run_order_row_ids(rids.slice(in_run(first..positions[end - 1] + 1))?)?;
            ids.extend(
                positions[at..end]
                    .iter()
                    .map(|&row| decoded[(row - first) as usize]),
            );
            at = end;
        }
        Ok(ids.freeze())
    }
}
