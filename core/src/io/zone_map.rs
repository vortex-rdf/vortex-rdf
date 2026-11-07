//! The zone maps a store file carries, read back exactly: a zoned column
//! whose zone table records each zone's exact bounds
//! ([`exact_bound_aggregates`]), and for a graph column the values each zone
//! holds (`vortexrdf.value_counts`). A quad child's code columns are zoned
//! every [`ZONE_ROWS`] rows, a dictionary's term column once per window.

use std::sync::Arc;

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
        // A zoned layout's zone table is its `zones` auxiliary child.
        let table = exact
            .then(|| {
                (0..column.nslots()).find_map(|slot| match column.slot_type(slot) {
                    Some(vortex_layout::LayoutChildType::Auxiliary(name))
                        if name.as_ref() == "zones" =>
                    {
                        column.slot(slot).ok().flatten()
                    }
                    _ => None,
                })
            })
            .flatten()?;
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

    #[cfg(feature = "file-io")]
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

/// A non-nullable `u64` column's zone map: each zone's exact bounds (the
/// zone table's [`exact_bound_aggregates`]); and beside them, where the
/// column records them (`vortexrdf.value_counts`, a graph column of a file
/// written since the `vortexrdf 2026.11.0` edition), the values each zone
/// holds with their rows, decoded once. How a zone stands against `x = c`
/// is [`eq_holds`](Self::eq_holds); its recorded rows of a code are
/// [`recorded_count`](Self::recorded_count).
pub(crate) struct ColumnZones {
    /// Each zone's exact minimum and maximum, in zone order.
    mins: vortex_buffer::Buffer<u64>,
    maxes: vortex_buffer::Buffer<u64>,
    /// The whole column's exact `(min, max)`, the zones' folded; `None` for
    /// a column of no rows.
    bounds: Option<(u64, u64)>,
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
pub(crate) type RecordedZones = Vec<Option<Box<[(u64, u32)]>>>;

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
        let zones = mins.len();
        if maxes.len() != zones || zone_len == 0 || row_count.div_ceil(zone_len) != zones as u64 {
            return Err(crate::error::VortexRdfError::InvalidInput(format!(
                "{zones} minimums and {} maximums for {row_count} rows in zones of {zone_len}",
                maxes.len()
            )));
        }
        let bounds = mins.iter().min().zip(maxes.iter().max());
        Ok(Self {
            bounds: bounds.map(|(&min, &max)| (min, max)),
            mins,
            maxes,
            recorded: None,
            zone_len,
            row_count,
        })
    }

    /// This zone map with each zone's recorded `(code, rows)` pairs, as a
    /// build counts them (`vortexrdf.value_counts` over each zone).
    pub(crate) fn with_records(mut self, records: RecordedZones) -> Result<Self> {
        if records.len() as u64 != self.zones() {
            return Err(crate::error::VortexRdfError::InvalidInput(format!(
                "{} recorded zones for {} zone bounds",
                records.len(),
                self.zones()
            )));
        }
        self.recorded = Some(records);
        Ok(self)
    }

    /// The whole column's exact `(min, max)`: the smallest of its zones'
    /// minimums and the largest of their maximums, folded once when the map
    /// is made. A residual asks it before the zones ([`eq_on_bounds`]).
    /// `None` for a column of no rows.
    pub(crate) fn column_bounds(&self) -> Option<(u64, u64)> {
        self.bounds
    }

    /// Each zone's `(min, max)`, in zone order.
    #[cfg(test)]
    pub(crate) fn zone_bounds_list(&self) -> Vec<(u64, u64)> {
        self.mins
            .iter()
            .copied()
            .zip(self.maxes.iter().copied())
            .collect()
    }

    /// Zone `zone`'s exact `(min, max)`.
    pub(crate) fn zone_bounds(&self, zone: u64) -> (u64, u64) {
        (self.mins[zone as usize], self.maxes[zone as usize])
    }

    /// How zone `zone` stands against `x = code`, by its exact bounds
    /// ([`eq_on_bounds`]) and then the values it recorded: `Some(false)` also
    /// where the zone recorded its values without `code`.
    pub(crate) fn eq_holds(&self, zone: u64, code: u64) -> Option<bool> {
        let (min, max) = self.zone_bounds(zone);
        match eq_on_bounds(min, max, code) {
            None if self.recorded_count(zone, code) == Some(0) => Some(false),
            held => held,
        }
    }

    /// Each zone's recorded `(code, rows)` pairs, `None` without them.
    #[cfg(test)]
    pub(crate) fn recorded(&self) -> Option<&RecordedZones> {
        self.recorded.as_ref()
    }

    /// This zone map with the values `states` records for each zone (the
    /// zone table's `vortexrdf.value_counts` column), decoded once
    /// ([`decode_states`]).
    fn with_recorded(mut self, states: ArrayRef) -> Result<Self> {
        if states.len() as u64 != self.zones() {
            return Err(crate::error::VortexRdfError::InvalidInput(format!(
                "{} recorded zones for {} zone bounds",
                states.len(),
                self.zones()
            )));
        }
        let rows = |zone| {
            let rows = self.zone_rows(zone);
            rows.end - rows.start
        };
        self.recorded = Some(decode_states(states, rows)?);
        Ok(self)
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
        let zones = Self::from_bounds(
            mins.into_buffer::<u64>(),
            maxes.into_buffer::<u64>(),
            map.zone_len as u64,
            column.row_count(),
        )?;
        match states {
            Some(states) => zones.with_recorded(states).map(Some),
            None => Ok(Some(zones)),
        }
    }

    /// Rows per zone, the last zone's excepted.
    pub(crate) fn zone_len(&self) -> u64 {
        self.zone_len
    }

    /// How many zones the column has.
    pub(crate) fn zones(&self) -> u64 {
        self.mins.len() as u64
    }

    /// The rows the zones cover.
    pub(crate) fn row_count(&self) -> u64 {
        self.row_count
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
}

/// How rows of exact bounds `min..=max` stand against `x = code`:
/// `Some(false)` where none can hold `code` (it lies outside the bounds),
/// `Some(true)` where every one does (`min = max = code`), `None` where the
/// bounds settle neither. The store's one rule for `x = c` on exact bounds,
/// a file's footer and zones and an in-memory column's statistics alike: it
/// refutes where Vortex's own falsifier of `x = c` does, and proves what
/// Vortex has no satisfier of an equality for. Read off the bounds directly,
/// for a read asks it of a column and of every zone it touches on every
/// call, where building and evaluating Vortex's proofs cost tens of µs a
/// column, more than many reads' whole compare.
pub(crate) fn eq_on_bounds(min: u64, max: u64, code: u64) -> Option<bool> {
    if code < min || code > max {
        Some(false)
    } else if min == code && max == code {
        Some(true)
    } else {
        None
    }
}

/// The footer's exact `(min, max)` of `file`'s top-level `u64` column
/// `column` (a store file's quad table: its footer statistics); `None`
/// when the file keeps no exact bounds for it.
#[cfg(feature = "file-io")]
pub(crate) fn footer_bounds(file: &vortex_file::VortexFile, column: &str) -> Option<(u64, u64)> {
    use vortex_array::expr::stats::Stat;

    let stats = file.file_stats()?;
    let index = file.dtype().as_struct_fields_opt()?.find(column)?;
    let (set, dtype) = stats.get(index);
    if !dtype.is_unsigned_int() {
        return None;
    }
    let min = set.get_as::<u64>(Stat::Min, dtype).as_exact()?;
    let max = set.get_as::<u64>(Stat::Max, dtype).as_exact()?;
    Some((min, max))
}

/// A zone table's exact `(max, min)` columns executed to `A`: `None` when
/// the table lacks either or either holds a null.
fn bounds_of<A: Executable>(table: &StructArray, ctx: &mut ExecutionCtx) -> Result<Option<(A, A)>> {
    let aggregates = exact_bound_aggregates();
    let max = bound_column(table, &aggregates[0].to_string(), ctx)?;
    let min = bound_column(table, &aggregates[1].to_string(), ctx)?;
    Ok(max.zip(min))
}

/// Each zone's `(code, rows)` pairs, read from a zone table's
/// `vortexrdf.value_counts` column (of the statistic's dtype) executed once
/// to canonical arrays all through, straight from their buffers. `None` for
/// a zone past the statistic's limit (a null state), and for a record no
/// store writes, whose zone then stands on its bounds alone: lists of
/// unequal lengths, codes not strictly ascending ([`pairs`]), or counts that
/// do not sum to the zone's `zone_rows`.
///
/// [`pairs`]: crate::store::value_counts::pairs
fn decode_states(states: ArrayRef, zone_rows: impl Fn(u64) -> u64) -> Result<RecordedZones> {
    use crate::store::value_counts::pairs;
    use vortex_array::arrays::listview::{ListViewArrayExt as _, ListViewArraySlotsExt as _};
    use vortex_array::arrays::struct_::StructArrayExt as _;
    use vortex_array::arrays::{ListViewArray, PrimitiveArray};
    use vortex_array::{IntoArray as _, RecursiveCanonical};

    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let states = states
        .execute::<RecursiveCanonical>(&mut ctx)?
        .0
        .into_array()
        .execute::<StructArray>(&mut ctx)?;
    let recorded = states
        .struct_validity()
        .execute_mask(states.len(), &mut ctx)?;
    let mut list = |name: &str| -> Result<(ListViewArray, PrimitiveArray)> {
        let list = states
            .unmasked_field_by_name(name)?
            .clone()
            .execute::<ListViewArray>(&mut ctx)?;
        let elements = list
            .elements()
            .clone()
            .execute::<PrimitiveArray>(&mut ctx)?;
        Ok((list, elements))
    };
    let (codes, code_items) = list("values")?;
    let (counts, count_items) = list("counts")?;
    let (code_items, count_items) = (code_items.as_slice::<u64>(), count_items.as_slice::<u32>());
    let mut zones = Vec::with_capacity(states.len());
    for zone in 0..states.len() {
        let mut items = |list: &ListViewArray| {
            let start = list.offset_at(zone, &mut ctx);
            start..start.saturating_add(list.size_at(zone, &mut ctx))
        };
        let (codes, counts) = (items(&codes), items(&counts));
        let record = recorded
            .value(zone)
            .then(|| pairs(code_items.get(codes)?, count_items.get(counts)?))
            .flatten()
            .filter(|pairs| {
                pairs.iter().map(|&(_, n)| u64::from(n)).sum::<u64>() == zone_rows(zone as u64)
            });
        zones.push(record.map(Vec::into_boxed_slice));
    }
    Ok(zones)
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

#[cfg(test)]
mod tests {
    use super::*;
    use vortex_array::dtype::{DType, Nullability, PType};
    use vortex_array::expr::{eq, lit, root};

    /// Vortex's own zone map of zones with exact bounds `bounds`, `zone_len`
    /// rows each but the last, `row_count` rows in all: the map its scans
    /// prune with.
    fn vortex_zone_map(
        bounds: &[(u64, u64)],
        zone_len: u64,
        row_count: u64,
    ) -> vortex_layout::layouts::zoned::zone_map::ZoneMap {
        use vortex_array::IntoArray as _;
        use vortex_array::arrays::PrimitiveArray;
        use vortex_array::validity::Validity;

        let column = |bound: fn(&(u64, u64)) -> u64| {
            let values: vortex_buffer::Buffer<u64> = bounds.iter().map(bound).collect();
            PrimitiveArray::new(values, Validity::AllValid).into_array()
        };
        let names: Vec<String> = exact_bound_aggregates()
            .iter()
            .map(|aggregate| aggregate.to_string())
            .collect();
        let table = StructArray::try_new(
            names.iter().map(String::as_str).collect::<Vec<_>>().into(),
            vec![column(|b| b.1), column(|b| b.0)],
            bounds.len(),
            Validity::NonNullable,
        )
        .unwrap();
        vortex_layout::layouts::zoned::zone_map::ZoneMap::try_new(
            DType::Primitive(PType::U64, Nullability::NonNullable),
            table,
            exact_bound_aggregates(),
            zone_len,
            row_count,
        )
        .unwrap()
    }

    /// The bounds' rule of `x = c` ([`ColumnZones::eq_holds`]) refutes a
    /// zone exactly where Vortex's own falsifier of `x = c` holds on it, and
    /// proves it exactly where the zone's bounds are the code alone (Vortex
    /// has no satisfier of an equality), for codes below, inside, on and
    /// above every zone's bounds and at the top of the `u64` range; the
    /// recorded values refute a zone whose bounds hold the code but whose
    /// records lack it.
    #[test]
    fn eq_holds_matches_vortex_falsifier() {
        let bounds = [(5u64, 5u64), (4, 5), (5, 6), (6, 7), (1, 3), (7, 7)];
        let (zone_len, row_count) = (8_192, 6 * 8_192 - 100);
        let zones = ColumnZones::from_bounds(
            bounds.iter().map(|b| b.0).collect(),
            bounds.iter().map(|b| b.1).collect(),
            zone_len,
            row_count,
        )
        .unwrap();
        let map = vortex_zone_map(&bounds, zone_len, row_count);
        let u64 = DType::Primitive(PType::U64, Nullability::NonNullable);
        for code in 0..=9u64 {
            let falsifier = eq(root(), lit(code))
                .bind(&u64)
                .unwrap()
                .falsify(&vortex_session::VortexSession::empty())
                .unwrap()
                .expect("Vortex falsifies an equality");
            let refuted = map.prune(&falsifier, &VORTEX_SESSION).unwrap();
            for (zone, &(min, max)) in bounds.iter().enumerate() {
                let held = zones.eq_holds(zone as u64, code);
                assert_eq!(
                    held == Some(false),
                    refuted.value(zone),
                    "zone {zone}, code {code}: refuted"
                );
                assert_eq!(
                    held == Some(true),
                    min == code && max == code,
                    "zone {zone}, code {code}: proved"
                );
            }
        }
        let top = u64::MAX;
        assert_eq!(eq_on_bounds(top, top, top), Some(true));
        assert_eq!(eq_on_bounds(top - 1, top, top), None);
        assert_eq!(eq_on_bounds(top - 1, top - 1, top), Some(false));
        let recorded = zones
            .with_records(vec![
                Some(vec![(5, 8_192)].into()),
                Some(vec![(4, 8_000), (5, 192)].into()),
                Some(vec![(6, 8_192)].into()),
                None,
                Some(vec![(1, 4_000), (3, 4_192)].into()),
                Some(vec![(7, 8_092)].into()),
            ])
            .unwrap();
        assert_eq!(
            recorded.eq_holds(2, 5),
            Some(false),
            "5 in the bounds, not the records"
        );
        assert_eq!(recorded.eq_holds(4, 2), Some(false));
        assert_eq!(
            recorded.eq_holds(3, 6),
            None,
            "a zone that recorded nothing"
        );
        assert_eq!(recorded.eq_holds(1, 5), None);
        assert_eq!(recorded.eq_holds(0, 5), Some(true));
    }

    /// A zone table's recorded states as another writer of the statistic
    /// could leave them: a zone whose lists differ in length, whose codes do
    /// not ascend strictly, or whose counts do not sum to its rows reads as
    /// unrecorded, as a zone past the limit does; well-formed zones read as
    /// written, the last one by its own rows.
    #[test]
    fn malformed_records_read_as_unrecorded() {
        use crate::store::value_counts::{state_dtype, state_of as state};
        use vortex_array::builders::builder_with_capacity_in;
        use vortex_array::memory::BufferAllocatorRef;
        use vortex_array::scalar::Scalar;

        let written = [
            state(&[1, 3], &[5_000, 3_192]),
            state(&[1, 3], &[8_192]),
            state(&[3, 1], &[5_000, 3_192]),
            state(&[2, 2], &[5_000, 3_192]),
            state(&[1, 3], &[5_000, 3_000]),
            Scalar::null(state_dtype()),
            state(&[7], &[100]),
        ];
        let mut states = builder_with_capacity_in(
            &state_dtype(),
            written.len(),
            BufferAllocatorRef::static_ref(),
        );
        for zone in &written {
            states.append_scalar(zone).unwrap();
        }
        let zones = ColumnZones::from_bounds(
            vortex_buffer::buffer![1u64, 1, 1, 2, 1, 0, 7],
            vortex_buffer::buffer![3u64, 3, 3, 2, 3, 9, 7],
            8_192,
            6 * 8_192 + 100,
        )
        .unwrap()
        .with_recorded(states.finish())
        .unwrap();
        let recorded: Vec<Option<Vec<(u64, u32)>>> = zones
            .recorded()
            .unwrap()
            .iter()
            .map(|zone| zone.as_deref().map(<[_]>::to_vec))
            .collect();
        assert_eq!(
            recorded,
            [
                Some(vec![(1, 5_000), (3, 3_192)]),
                None,
                None,
                None,
                None,
                None,
                Some(vec![(7, 100)]),
            ]
        );
        assert_eq!(zones.recorded_count(1, 3), None, "unequal lists");
        assert_eq!(zones.eq_holds(2, 2), None, "codes out of order");
    }
}
