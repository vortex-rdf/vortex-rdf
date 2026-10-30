//! The zone maps a store file carries, read back exactly: a zoned column
//! whose zone table records each zone's exact bounds
//! ([`exact_bound_aggregates`]). A quad child's code columns are zoned every
//! [`ZONE_ROWS`] rows, a dictionary's term column once per window.

use std::sync::Arc;

use vortex_array::aggregate_fn::AggregateFnRef;
use vortex_array::arrays::StructArray;
use vortex_array::arrays::struct_::StructArrayExt as _;
use vortex_array::{Executable, ExecutionCtx, VortexSessionExecute as _};
use vortex_layout::LayoutRef;
use vortex_layout::layouts::zoned::Zoned;
use vortex_layout::segments::SegmentSource;

use crate::error::Result;
use crate::session::VORTEX_SESSION;

/// Rows per zone of a quad child's zone maps, and the length every block the
/// writer repartitions a quad child into is a multiple of: the
/// `WriteStrategyBuilder` default the store keeps.
pub(crate) const ZONE_ROWS: usize = 8_192;

/// The zone aggregates recording a zone's exact bounds, in the order a zone
/// table holds them: `vortex.max()`, then `vortex.min()`. A primitive
/// column's default zone map records these; a dictionary's term column names
/// them in place of the bounded string aggregates Vortex writes by default,
/// which truncate long terms.
pub(crate) fn exact_bound_aggregates() -> Arc<[AggregateFnRef]> {
    use vortex_array::aggregate_fn::fns::{max::Max, min::Min};
    use vortex_array::aggregate_fn::{AggregateFnVTableExt as _, NumericalAggregateOpts};
    Arc::from([
        Max.bind(NumericalAggregateOpts::skip_nans()),
        Min.bind(NumericalAggregateOpts::skip_nans()),
    ])
}

/// A zoned column's zone table that records each zone's exact bounds.
pub(crate) struct ExactZoneMap {
    /// The zone table: one row per zone, a column per aggregate.
    pub(crate) table: LayoutRef,
    /// Rows per zone, the last zone's excepted.
    pub(crate) zone_len: usize,
}

impl ExactZoneMap {
    /// `column`'s zone map, when `column` is zoned with every aggregate of
    /// [`exact_bound_aggregates`].
    pub(crate) fn of(column: &LayoutRef) -> Option<Self> {
        let zoned = column.as_opt::<Zoned>()?;
        let present = zoned.present_aggregates();
        let exact = exact_bound_aggregates()
            .iter()
            .all(|aggregate| present.contains(&aggregate.to_string()));
        // A zoned layout's zone table is its second slot, after the data.
        let table = exact.then(|| column.slot(1).ok().flatten()).flatten()?;
        Some(Self {
            table,
            zone_len: zoned.zone_len(),
        })
    }

    /// Each zone's `(max, min)` columns, read through `source` and executed
    /// to `A`: `None` when the table lacks either or either holds a null.
    pub(crate) async fn read_bounds<A: Executable>(
        &self,
        source: &Arc<dyn SegmentSource>,
    ) -> Result<Option<(A, A)>> {
        let reader = self.table.new_reader(
            "zone-table".into(),
            Arc::clone(source),
            &VORTEX_SESSION,
            &Default::default(),
        )?;
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let table = crate::io::read::scan_all_reader(reader)
            .await?
            .execute::<StructArray>(&mut ctx)?;
        let aggregates = exact_bound_aggregates();
        let max = bound_column(&table, &aggregates[0].to_string(), &mut ctx)?;
        let min = bound_column(&table, &aggregates[1].to_string(), &mut ctx)?;
        Ok(max.zip(min))
    }
}

/// Column `name` of a zone table executed to `A`, `None` when the table lacks
/// it or it holds a null.
fn bound_column<A: Executable>(
    table: &StructArray,
    name: &str,
    ctx: &mut ExecutionCtx,
) -> Result<Option<A>> {
    let Ok(column) = table.unmasked_field_by_name(name) else {
        return Ok(None);
    };
    if !column.all_valid(ctx)? {
        return Ok(None);
    }
    Ok(Some(column.clone().execute::<A>(ctx)?))
}
