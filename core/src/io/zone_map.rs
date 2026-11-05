//! The zone maps a store file carries, read back exactly: a zoned column
//! whose zone table records each zone's exact bounds
//! ([`exact_bound_aggregates`]), and for a graph column the values each zone
//! holds (`vortexrdf.value_counts`). A quad child's code columns are zoned
//! every [`ZONE_ROWS`] rows, a dictionary's term column once per window.

use std::sync::Arc;

#[cfg(feature = "file-io")]
use vortex_array::ArrayRef;
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

    /// The zone table, read through `source`: one row per zone, a column
    /// per aggregate.
    pub(crate) async fn read_table(&self, source: &Arc<dyn SegmentSource>) -> Result<StructArray> {
        let reader = self.table.new_reader(
            "zone-table".into(),
            Arc::clone(source),
            &VORTEX_SESSION,
            &Default::default(),
        )?;
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        Ok(crate::io::read::scan_all_reader(reader)
            .await?
            .execute::<StructArray>(&mut ctx)?)
    }

    /// Each zone's `(max, min)` columns, read through `source` and executed
    /// to `A`: `None` when the table lacks either or either holds a null.
    pub(crate) async fn read_bounds<A: Executable>(
        &self,
        source: &Arc<dyn SegmentSource>,
    ) -> Result<Option<(A, A)>> {
        let table = self.read_table(source).await?;
        bounds_of(&table, &mut VORTEX_SESSION.create_execution_ctx())
    }
}

/// A non-nullable `u64` column's zone map as Vortex's own [`ZoneMap`] takes
/// it: the zone table's exact bounds ([`exact_bound_aggregates`], `max` then
/// `min`), one row per zone, then the values each zone holds with their rows
/// where the column records them (`vortexrdf.value_counts`, a graph column
/// of a file written since the `vortexrdf 2026.11.0` edition). A stats proof
/// (a falsifier or a satisfier) is evaluated over a range of its zones by
/// [`prune`](Self::prune), Vortex's [`ZoneMap::prune`] on those zones' rows
/// of the table; a zone's recorded rows of a code are
/// [`recorded_count`](Self::recorded_count).
///
/// [`ZoneMap`]: vortex_layout::layouts::zoned::zone_map::ZoneMap
/// [`ZoneMap::prune`]: vortex_layout::layouts::zoned::zone_map::ZoneMap::prune
#[cfg(feature = "file-io")]
pub(crate) struct ColumnZones {
    /// The zone table: `max`, then `min`, each a nullable `u64` column, then
    /// each zone's recorded values where the column records them.
    table: StructArray,
    /// The aggregates of the table's columns, in order, as [`ZoneMap`]
    /// takes them.
    ///
    /// [`ZoneMap`]: vortex_layout::layouts::zoned::zone_map::ZoneMap
    aggregates: Arc<[AggregateFnRef]>,
    /// Each zone's recorded values, `None` for every zone of a column
    /// without the statistic.
    recorded: Option<RecordedZones>,
    /// Rows per zone, the last zone's excepted.
    zone_len: u64,
    /// The rows the zones cover.
    row_count: u64,
}

/// Each zone's recorded `(code, rows)` pairs, ascending by code: `None` for a
/// zone that held more values than the statistic records.
#[cfg(feature = "file-io")]
type RecordedZones = Vec<Option<Box<[(u64, u32)]>>>;

#[cfg(feature = "file-io")]
impl ColumnZones {
    /// The zone map of a column whose zone `z` holds codes `mins[z]..=maxes[z]`,
    /// `zone_len` rows each but the last, `row_count` rows in all. It records
    /// no zone's values.
    pub(crate) fn from_bounds(
        mins: vortex_buffer::Buffer<u64>,
        maxes: vortex_buffer::Buffer<u64>,
        zone_len: u64,
        row_count: u64,
    ) -> Result<Self> {
        Self::new(mins, maxes, None, zone_len, row_count)
    }

    /// The zone map of [`from_bounds`](Self::from_bounds), recording the
    /// values `states` holds for each zone (the zone table's
    /// `vortexrdf.value_counts` column) where given.
    fn new(
        mins: vortex_buffer::Buffer<u64>,
        maxes: vortex_buffer::Buffer<u64>,
        states: Option<ArrayRef>,
        zone_len: u64,
        row_count: u64,
    ) -> Result<Self> {
        use vortex_array::IntoArray as _;
        use vortex_array::arrays::PrimitiveArray;
        use vortex_array::validity::Validity;

        let zones = mins.len();
        let recorded_zones = states.as_ref().map_or(zones, ArrayRef::len);
        if maxes.len() != zones
            || recorded_zones != zones
            || zone_len == 0
            || row_count.div_ceil(zone_len) != zones as u64
        {
            return Err(crate::error::VortexRdfError::InvalidInput(format!(
                "{zones} minimums, {} maximums and {recorded_zones} recorded zones for \
                 {row_count} rows in zones of {zone_len}",
                maxes.len()
            )));
        }
        let mut aggregates = exact_bound_aggregates().to_vec();
        let mut columns = vec![
            PrimitiveArray::new(maxes, Validity::AllValid).into_array(),
            PrimitiveArray::new(mins, Validity::AllValid).into_array(),
        ];
        let recorded = match states {
            Some(states) => {
                let (states, recorded) = decode_states(states)?;
                aggregates.push(crate::store::value_counts::value_counts());
                columns.push(states);
                Some(recorded)
            }
            None => None,
        };
        let names: Vec<String> = aggregates.iter().map(ToString::to_string).collect();
        let table = StructArray::try_new(
            names.iter().map(String::as_str).collect::<Vec<_>>().into(),
            columns,
            zones,
            Validity::NonNullable,
        )?;
        Ok(Self {
            table,
            aggregates: aggregates.into(),
            recorded,
            zone_len,
            row_count,
        })
    }

    /// `column`'s zone map, read through `source`: `None` when `column` is
    /// not a non-nullable `u64` column zoned with exact bounds
    /// ([`ExactZoneMap`]), or a bound is null. The values each zone holds
    /// come with it where the zone table records them.
    pub(crate) async fn read(
        column: &LayoutRef,
        source: &Arc<dyn SegmentSource>,
    ) -> Result<Option<Self>> {
        use crate::store::value_counts::{state_dtype, value_counts};
        use vortex_array::arrays::PrimitiveArray;
        use vortex_array::dtype::{DType, Nullability, PType};

        if column.dtype() != &DType::Primitive(PType::U64, Nullability::NonNullable) {
            return Ok(None);
        }
        let Some(map) = ExactZoneMap::of(column) else {
            return Ok(None);
        };
        let table = map.read_table(source).await?;
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let Some((maxes, mins)) = bounds_of::<PrimitiveArray>(&table, &mut ctx)? else {
            return Ok(None);
        };
        if maxes.ptype() != PType::U64 || mins.ptype() != PType::U64 {
            return Ok(None);
        }
        // A column of another dtype under the statistic's name is not one
        // this store wrote: its zones are left to their bounds.
        let states = table
            .unmasked_field_by_name_opt(value_counts().to_string())
            .filter(|states| states.dtype() == &state_dtype())
            .cloned();
        Self::new(
            mins.into_buffer::<u64>(),
            maxes.into_buffer::<u64>(),
            states,
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

    /// The column's rows in zone `zone`: [`zone_len`](Self::zone_len) of
    /// them from its first, or to the column's end.
    pub(crate) fn zone_rows(&self, zone: u64) -> std::ops::Range<u64> {
        zone * self.zone_len..((zone + 1) * self.zone_len).min(self.row_count)
    }

    /// The rows of zone `zone` holding `code`, as the zone recorded them:
    /// `Some(0)` for a recorded zone without `code`; `None` for a zone that
    /// held more values than the statistic records, and for every zone of a
    /// column without it.
    pub(crate) fn recorded_count(&self, zone: u64, code: u64) -> Option<u64> {
        let zone = usize::try_from(zone).ok()?;
        let pairs = self.recorded.as_ref()?.get(zone)?.as_deref()?;
        Some(
            pairs
                .binary_search_by_key(&code, |&(value, _)| value)
                .map_or(0, |at| u64::from(pairs[at].1)),
        )
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
            Arc::clone(&self.aggregates),
            self.zone_len,
            rows,
        )?;
        Ok(map.prune(proof, &VORTEX_SESSION)?)
    }
}

/// A zone table's exact `(max, min)` columns executed to `A`: `None` when
/// the table lacks either or either holds a null.
fn bounds_of<A: Executable>(table: &StructArray, ctx: &mut ExecutionCtx) -> Result<Option<(A, A)>> {
    let aggregates = exact_bound_aggregates();
    let max = bound_column(table, &aggregates[0].to_string(), ctx)?;
    let min = bound_column(table, &aggregates[1].to_string(), ctx)?;
    Ok(max.zip(min))
}

/// A zone table's `vortexrdf.value_counts` column executed once to canonical
/// arrays all through, and each zone's `(code, rows)` pairs decoded from it:
/// `None` for a zone past the statistic's limit (a null state).
#[cfg(feature = "file-io")]
fn decode_states(states: ArrayRef) -> Result<(ArrayRef, RecordedZones)> {
    use crate::store::value_counts::decode_state;
    use vortex_array::{IntoArray as _, RecursiveCanonical};

    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let states = states
        .execute::<RecursiveCanonical>(&mut ctx)?
        .0
        .into_array();
    let recorded = (0..states.len())
        .map(|zone| {
            Ok(decode_state(&states.execute_scalar(zone, &mut ctx)?).map(Vec::into_boxed_slice))
        })
        .collect::<Result<_>>()?;
    Ok((states, recorded))
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
