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

/// A non-nullable `u64` column's zone map as Vortex's own [`ZoneMap`] takes
/// it: the zone table's exact bounds ([`exact_bound_aggregates`], `max` then
/// `min`), one row per zone. A stats proof (a falsifier or a satisfier) is
/// evaluated over a range of its zones by [`prune`](Self::prune), Vortex's
/// [`ZoneMap::prune`] on those zones' rows of the table.
///
/// [`ZoneMap`]: vortex_layout::layouts::zoned::zone_map::ZoneMap
/// [`ZoneMap::prune`]: vortex_layout::layouts::zoned::zone_map::ZoneMap::prune
#[cfg(feature = "file-io")]
pub(crate) struct ColumnZones {
    /// The zone table: `max`, then `min`, each a nullable `u64` column.
    table: StructArray,
    /// Rows per zone, the last zone's excepted.
    zone_len: u64,
    /// The rows the zones cover.
    row_count: u64,
}

#[cfg(feature = "file-io")]
impl ColumnZones {
    /// The zone map of a column whose zone `z` holds codes `mins[z]..=maxes[z]`,
    /// `zone_len` rows each but the last, `row_count` rows in all.
    pub(crate) fn from_bounds(
        mins: vortex_buffer::Buffer<u64>,
        maxes: vortex_buffer::Buffer<u64>,
        zone_len: u64,
        row_count: u64,
    ) -> Result<Self> {
        use vortex_array::IntoArray as _;
        use vortex_array::arrays::PrimitiveArray;
        use vortex_array::validity::Validity;

        let zones = mins.len();
        if maxes.len() != zones || zone_len == 0 || row_count.div_ceil(zone_len) != zones as u64 {
            return Err(crate::error::VortexRdfError::InvalidInput(format!(
                "{zones} minimums and {} maximums for {row_count} rows in zones of {zone_len}",
                maxes.len()
            )));
        }
        let aggregates = exact_bound_aggregates();
        let names: Vec<String> = aggregates.iter().map(|a| a.to_string()).collect();
        let table = StructArray::try_new(
            names.iter().map(String::as_str).collect::<Vec<_>>().into(),
            vec![
                PrimitiveArray::new(maxes, Validity::AllValid).into_array(),
                PrimitiveArray::new(mins, Validity::AllValid).into_array(),
            ],
            zones,
            Validity::NonNullable,
        )?;
        Ok(Self {
            table,
            zone_len,
            row_count,
        })
    }

    /// `column`'s zone map, read through `source`: `None` when `column` is
    /// not a non-nullable `u64` column zoned with exact bounds
    /// ([`ExactZoneMap`]), or a bound is null.
    pub(crate) async fn read(
        column: &LayoutRef,
        source: &Arc<dyn SegmentSource>,
    ) -> Result<Option<Self>> {
        use vortex_array::arrays::PrimitiveArray;
        use vortex_array::dtype::{DType, Nullability, PType};

        if column.dtype() != &DType::Primitive(PType::U64, Nullability::NonNullable) {
            return Ok(None);
        }
        let Some(map) = ExactZoneMap::of(column) else {
            return Ok(None);
        };
        let Some((maxes, mins)) = map.read_bounds::<PrimitiveArray>(source).await? else {
            return Ok(None);
        };
        if maxes.ptype() != PType::U64 || mins.ptype() != PType::U64 {
            return Ok(None);
        }
        Self::from_bounds(
            mins.into_buffer::<u64>(),
            maxes.into_buffer::<u64>(),
            map.zone_len as u64,
            column.row_count(),
        )
        .map(Some)
    }

    /// Rows per zone, the last zone's excepted.
    pub(crate) fn zone_len(&self) -> u64 {
        self.zone_len
    }

    /// How many zones the column has.
    pub(crate) fn zones(&self) -> u64 {
        self.table.len() as u64
    }

    /// Where `proof` holds over the zones `zones`, one bit per zone in
    /// order: Vortex's [`ZoneMap::prune`] on those zones' rows of the
    /// table.
    ///
    /// [`ZoneMap::prune`]: vortex_layout::layouts::zoned::zone_map::ZoneMap::prune
    pub(crate) fn prune(
        &self,
        zones: std::ops::Range<u64>,
        proof: &vortex_array::expr::BoundExpression,
    ) -> Result<vortex_mask::Mask> {
        use vortex_array::IntoArray as _;
        use vortex_array::dtype::{DType, Nullability, PType};
        use vortex_layout::layouts::zoned::zone_map::ZoneMap;

        if zones.is_empty() {
            return Ok(vortex_mask::Mask::new_false(0));
        }
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let table = self
            .table
            .clone()
            .into_array()
            .slice(zones.start as usize..zones.end as usize)?
            .execute::<StructArray>(&mut ctx)?;
        let first_row = zones.start * self.zone_len;
        let rows = (zones.end * self.zone_len).min(self.row_count) - first_row;
        let map = ZoneMap::try_new(
            DType::Primitive(PType::U64, Nullability::NonNullable),
            table,
            exact_bound_aggregates(),
            self.zone_len,
            rows,
        )?;
        Ok(map.prune(proof, &VORTEX_SESSION)?)
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

#[cfg(all(test, feature = "file-io"))]
mod tests {
    use super::*;
    use vortex_array::dtype::{DType, Nullability, PType};
    use vortex_array::expr::{eq, lit, root};

    /// A zone map pruned with Vortex's own falsifier of `x = 5`: the zones
    /// whose bounds leave 5 out, over the whole table and over a range of
    /// its zones.
    #[test]
    fn column_zones_prune_with_vortex_falsifier() {
        let zones = ColumnZones::from_bounds(
            vortex_buffer::buffer![5u64, 4, 5, 6, 1],
            vortex_buffer::buffer![5u64, 5, 6, 7, 3],
            8_192,
            36_000,
        )
        .unwrap();
        assert_eq!((zones.zones(), zones.zone_len()), (5, 8_192));
        let dtype = DType::Primitive(PType::U64, Nullability::NonNullable);
        let none = eq(root(), lit(5u64))
            .bind(&dtype)
            .unwrap()
            .falsify(&VORTEX_SESSION)
            .unwrap()
            .expect("Vortex falsifies an equality");
        let bits =
            |mask: vortex_mask::Mask| (0..mask.len()).map(|i| mask.value(i)).collect::<Vec<_>>();
        assert_eq!(
            bits(zones.prune(0..5, &none).unwrap()),
            [false, false, false, true, true]
        );
        assert_eq!(
            bits(zones.prune(1..4, &none).unwrap()),
            [false, false, true]
        );
        assert_eq!(zones.prune(2..2, &none).unwrap().len(), 0);
    }
}
