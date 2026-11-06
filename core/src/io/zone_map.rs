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

/// A non-nullable `u64` column's zone map as Vortex's own [`ZoneMap`] takes
/// it: the zone table's exact bounds ([`exact_bound_aggregates`], `max` then
/// `min`), one row per zone; and beside it, where the column records them
/// (`vortexrdf.value_counts`, a graph column of a file written since the
/// `vortexrdf 2026.11.0` edition), the values each zone holds with their
/// rows, decoded once. A stats proof (a falsifier or a satisfier) is
/// evaluated over a range of its zones by [`prune`](Self::prune), Vortex's
/// [`ZoneMap::prune`] on those zones' rows of the table; a zone's recorded
/// rows of a code are [`recorded_count`](Self::recorded_count).
///
/// [`ZoneMap`]: vortex_layout::layouts::zoned::zone_map::ZoneMap
/// [`ZoneMap::prune`]: vortex_layout::layouts::zoned::zone_map::ZoneMap::prune
pub(crate) struct ColumnZones {
    /// The zone table: `max`, then `min`, each a nullable `u64` column.
    table: StructArray,
    /// The table's `min` and `max` buffers, shared with its columns: the
    /// bounds [`eq_holds`](Self::eq_holds) reads.
    mins: vortex_buffer::Buffer<u64>,
    maxes: vortex_buffer::Buffer<u64>,
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
                PrimitiveArray::new(maxes.clone(), Validity::AllValid).into_array(),
                PrimitiveArray::new(mins.clone(), Validity::AllValid).into_array(),
            ],
            zones,
            Validity::NonNullable,
        )?;
        Ok(Self {
            table,
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

    /// Each zone's `(min, max)`, in zone order.
    #[cfg(test)]
    pub(crate) fn bounds(&self) -> Vec<(u64, u64)> {
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
    /// zone table's `vortexrdf.value_counts` column), decoded once.
    fn with_recorded(mut self, states: ArrayRef) -> Result<Self> {
        if states.len() as u64 != self.zones() {
            return Err(crate::error::VortexRdfError::InvalidInput(format!(
                "{} recorded zones for {} zone bounds",
                states.len(),
                self.zones()
            )));
        }
        self.recorded = Some(decode_states(states)?);
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
        self.table.len() as u64
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

    /// Where `proof` holds over the zones `zones`, one bit per zone in
    /// order: Vortex's [`ZoneMap::prune`] on those zones' rows of the
    /// table.
    ///
    /// [`ZoneMap::prune`]: vortex_layout::layouts::zoned::zone_map::ZoneMap::prune
    #[cfg(feature = "file-io")]
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

/// How rows of exact bounds `min..=max` stand against `x = code`:
/// `Some(false)` where none can hold `code` (it lies outside the bounds),
/// `Some(true)` where every one does (`min = max = code`), `None` where the
/// bounds settle neither. What Vortex's falsifier of `x = c` and the store's
/// satisfier ([`eq_proofs`]) prove on exact bounds, read off the bounds
/// directly: the in-memory residual asks it of a column and of every zone
/// it touches on every call, where building and evaluating the proofs cost
/// tens of µs a column, more than many residuals' whole compare.
///
/// [`eq_proofs`]: crate::stats_rules::eq_proofs
pub(crate) fn eq_on_bounds(min: u64, max: u64, code: u64) -> Option<bool> {
    if code < min || code > max {
        Some(false)
    } else if min == code && max == code {
        Some(true)
    } else {
        None
    }
}

/// Whether `proof`, one of Vortex's proofs of `x = code` ([`eq_proofs`]),
/// holds on bounds `(min, max)` of a column of `row_count` rows, a one-zone
/// map of them: for the falsifier no row holds the code, for the satisfier
/// every row does. `false` without bounds or a proof.
///
/// [`eq_proofs`]: crate::stats_rules::eq_proofs
#[cfg(feature = "file-io")]
pub(crate) fn bounds_prove(
    bounds: Option<(u64, u64)>,
    proof: Option<&vortex_array::expr::BoundExpression>,
    row_count: u64,
) -> Result<bool> {
    let (Some((min, max)), Some(proof)) = (bounds, proof) else {
        return Ok(false);
    };
    if row_count == 0 {
        return Ok(false);
    }
    let zones = ColumnZones::from_bounds(
        vortex_buffer::buffer![min],
        vortex_buffer::buffer![max],
        row_count,
        row_count,
    )?;
    Ok(zones.prune(0..1, proof)?.value(0))
}

/// The footer's exact `(min, max)` of `file`'s top-level `u64` column
/// `column` (a store file's quad table: its footer statistics); `None`
/// when the file keeps no exact bounds for it.
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

/// Each zone's `(code, rows)` pairs, decoded from a zone table's
/// `vortexrdf.value_counts` column executed once to canonical arrays all
/// through: `None` for a zone past the statistic's limit (a null state).
fn decode_states(states: ArrayRef) -> Result<RecordedZones> {
    use crate::store::value_counts::decode_state;
    use vortex_array::{IntoArray as _, RecursiveCanonical};

    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let states = states
        .execute::<RecursiveCanonical>(&mut ctx)?
        .0
        .into_array();
    (0..states.len())
        .map(|zone| {
            Ok(decode_state(&states.execute_scalar(zone, &mut ctx)?).map(Vec::into_boxed_slice))
        })
        .collect()
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

    /// The bounds' rule of `x = c` ([`ColumnZones::eq_holds`]) is what
    /// Vortex's falsifier and the store's satisfier prove on each zone
    /// ([`ColumnZones::prune`] with [`eq_proofs`]), for codes below, inside,
    /// on and above every zone's bounds, and the recorded values rule out a
    /// zone whose bounds hold the code but whose records lack it.
    ///
    /// [`eq_proofs`]: crate::stats_rules::eq_proofs
    #[cfg(feature = "file-io")]
    #[test]
    fn eq_holds_proves_what_the_vortex_proofs_prove() {
        let zones = ColumnZones::from_bounds(
            vortex_buffer::buffer![5u64, 4, 5, 6, 1, 7],
            vortex_buffer::buffer![5u64, 5, 6, 7, 3, 7],
            8_192,
            6 * 8_192 - 100,
        )
        .unwrap();
        let u64 = DType::Primitive(PType::U64, Nullability::NonNullable);
        for code in 0..=9 {
            let proofs = crate::stats_rules::eq_proofs(&u64, code).unwrap();
            let none = zones.prune(0..6, proofs.none.as_ref().unwrap()).unwrap();
            let all = zones.prune(0..6, proofs.all.as_ref().unwrap()).unwrap();
            for zone in 0..6 {
                let proved = match (none.value(zone), all.value(zone)) {
                    (true, _) => Some(false),
                    (false, true) => Some(true),
                    (false, false) => None,
                };
                assert_eq!(
                    zones.eq_holds(zone as u64, code),
                    proved,
                    "zone {zone}, code {code}"
                );
            }
        }
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

    /// A zone map pruned with Vortex's own falsifier of `x = 5`: the zones
    /// whose bounds leave 5 out, over the whole table and over a range of
    /// its zones.
    #[cfg(feature = "file-io")]
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

    #[cfg(feature = "file-io")]
    /// Bounds prove every row holds a code only where both are the
    /// code, and no row does where they leave it out; a file without them
    /// proves nothing.
    #[test]
    fn bounds_prove_the_code() {
        use vortex_array::dtype::{DType, Nullability, PType};

        let dtype = DType::Primitive(PType::U64, Nullability::NonNullable);
        let proofs = |code| crate::stats_rules::eq_proofs(&dtype, code).unwrap();
        let (five, top) = (proofs(5), proofs(u64::MAX));
        let (all, none) = (five.all.as_ref(), five.none.as_ref());
        assert!(!bounds_prove(None, all, 100).unwrap());
        assert!(!bounds_prove(Some((4, 5)), all, 100).unwrap());
        assert!(bounds_prove(Some((5, 5)), all, 100).unwrap());
        assert!(!bounds_prove(Some((5, 5)), None, 100).unwrap());
        let max = (u64::MAX, u64::MAX);
        assert!(bounds_prove(Some(max), top.all.as_ref(), 100).unwrap());
        for (bounds, held_by_none) in [
            ((6, 9), true),
            ((1, 4), true),
            ((4, 6), false),
            ((5, 5), false),
        ] {
            assert_eq!(
                bounds_prove(Some(bounds), none, 100).unwrap(),
                held_by_none,
                "{bounds:?}"
            );
        }
        assert!(!bounds_prove(None, none, 100).unwrap());
    }
}
