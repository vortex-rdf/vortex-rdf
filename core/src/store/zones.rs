//! The zone maps of an in-memory base's code columns: P1's [`ColumnZones`]
//! (a Vortex `ZoneMap` of each 8,192-row zone's exact bounds) per quad code
//! column, `g`'s with the graphs each zone recorded (P2's
//! `vortexrdf.value_counts`), as a file store's located runs read them. A
//! residual equality over a base that no index serves compares only the
//! zones Vortex's falsifier of `x = c` and the records leave
//! ([`QuadZones::kept_runs`]), as the file path's zone-map pruning reads only
//! those. A build takes the bounds in the pass that picks each code column's
//! compression, which reads every code anyway ([`ZoneBuilder`]), and `g`'s
//! records with Vortex's accumulator; a store loaded from a file or from
//! bytes reads both from the file's zone tables ([`QuadZones::read`]). A base
//! with neither has none, and its residual compares every row.

use std::ops::Range;
use std::sync::Arc;

use vortex_array::dtype::{DType, Nullability, PType};
use vortex_array::scalar::Scalar;

use crate::error::Result;
use crate::io::zone_map::{ColumnZones, ExactZoneMap, RecordedZones, ZONE_ROWS};
use crate::store::array::unsigned_value;
use crate::store::schema::{PRIMARY_COLUMNS, TermCode};

/// The dtype of a code column that carries a zone map.
const CODE_DTYPE: DType = DType::Primitive(PType::U64, Nullability::NonNullable);

/// The graph column's slot in `(s, p, o, g)`.
const GRAPH_SLOT: usize = 3;

/// A base's quad code columns' zone maps, in `(s, p, o, g)` slot order; a
/// column without one is `None`.
#[derive(Default)]
pub(crate) struct QuadZones([Option<Arc<ColumnZones>>; 4]);

impl QuadZones {
    /// Slot `slot`'s zone map, if it has one.
    pub(crate) fn get(&self, slot: usize) -> Option<&Arc<ColumnZones>> {
        self.0.get(slot).and_then(Option::as_ref)
    }

    /// Column `name`'s zone map (`s`, `p`, `o` or `g`), if it has one.
    pub(crate) fn of_column(&self, name: &str) -> Option<&Arc<ColumnZones>> {
        let slot = PRIMARY_COLUMNS.iter().position(|column| *column == name)?;
        self.get(slot)
    }

    /// Record `zones` as slot `slot`'s.
    pub(crate) fn set(&mut self, slot: usize, zones: ColumnZones) {
        self.0[slot] = Some(Arc::new(zones));
    }

    /// The columns whose zones cover `rows` rows in zones of [`ZONE_ROWS`]: a
    /// column with another number of zones is dropped, since its zones do
    /// not line up with a base of `rows` rows.
    pub(crate) fn covering(mut self, rows: usize) -> Self {
        for column in &mut self.0 {
            if column.as_ref().is_some_and(|zones| {
                zones.zone_len() != ZONE_ROWS as u64
                    || zones.zones() != rows.div_ceil(ZONE_ROWS) as u64
            }) {
                *column = None;
            }
        }
        self
    }

    /// The zone maps of a store file's quad code columns, read from their
    /// zone tables with `g`'s recorded graphs ([`ColumnZones::read`]): each of
    /// `s`, `p`, `o` and `g` that is a non-nullable `u64` column zoned in
    /// [`ZONE_ROWS`]-row zones with exact bounds. `quads` is the quad child's
    /// struct layout, its segments read from `source`; a column without such
    /// a map has none. The columns' tables are read concurrently: over a file
    /// read through I/O, each read waits.
    pub(crate) async fn read(
        quads: &vortex_layout::LayoutRef,
        source: &Arc<dyn vortex_layout::segments::SegmentSource>,
    ) -> Result<Self> {
        use vortex_layout::LayoutChildType;

        if quads
            .as_opt::<vortex_layout::layouts::struct_::Struct>()
            .is_none()
        {
            return Ok(Self::default());
        }
        let mut columns = Vec::new();
        for slot in 0..quads.nslots() {
            let Some(LayoutChildType::Field(name)) = quads.slot_type(slot) else {
                continue;
            };
            let Some(quad_slot) = PRIMARY_COLUMNS
                .iter()
                .position(|column| *column == name.as_ref())
            else {
                continue;
            };
            let Some(column) = quads.slot(slot)? else {
                continue;
            };
            if column.dtype() != &CODE_DTYPE {
                continue;
            }
            // A zone table of other zones than this base's would not line up
            // with its rows: such a column has none.
            let Some(map) = ExactZoneMap::of(&column) else {
                continue;
            };
            let zones = column.row_count().div_ceil(ZONE_ROWS as u64);
            if map.zone_len == ZONE_ROWS && map.table.row_count() == zones {
                columns.push((quad_slot, column));
            }
        }
        let read = columns.into_iter().map(|(slot, column)| async move {
            Result::Ok((slot, ColumnZones::read(&column, source).await?))
        });
        let mut zones = Self::default();
        for (slot, column) in futures::future::try_join_all(read).await? {
            if let Some(column) = column {
                zones.0[slot] = Some(Arc::new(column));
            }
        }
        Ok(zones)
    }

    /// The rows of the zones that may hold every code `eqs` binds, as
    /// ascending runs over a base of `rows` rows, adjacent zones joined: a
    /// zone is left out where Vortex's falsifier of a bound column's `x = c`
    /// holds on its zone map, or where `g`'s records lack its code. `None`
    /// when no bound column has zones, or when the zones left hold more than
    /// half the base: comparing a whole column then costs less than
    /// comparing its runs.
    pub(crate) fn kept_runs(
        &self,
        eqs: &[(&str, Scalar)],
        rows: usize,
    ) -> Result<Option<Vec<Range<u64>>>> {
        let mut keep: Option<Vec<bool>> = None;
        for (field, scalar) in eqs {
            let (Some(code), Some(zones)) = (unsigned_value(scalar), self.of_column(field)) else {
                continue;
            };
            let proofs = crate::stats_rules::eq_proofs(&CODE_DTYPE, code)?;
            let none = match &proofs.none {
                Some(proof) => Some(zones.prune(0..zones.zones(), proof)?),
                None => None,
            };
            let keep = keep.get_or_insert_with(|| vec![true; zones.zones() as usize]);
            for (zone, kept) in keep.iter_mut().enumerate() {
                let ruled_out = none.as_ref().is_some_and(|mask| mask.value(zone))
                    || zones.recorded_count(zone as u64, code) == Some(0);
                *kept &= !ruled_out;
            }
        }
        let Some(keep) = keep else {
            return Ok(None);
        };
        let kept = keep.iter().filter(|&&kept| kept).count();
        if kept.saturating_mul(2) > keep.len() {
            return Ok(None);
        }
        let mut runs: Vec<Range<u64>> = Vec::new();
        for zone in (0..keep.len()).filter(|&zone| keep[zone]) {
            let start = (zone * ZONE_ROWS) as u64;
            let end = ((zone + 1) * ZONE_ROWS).min(rows) as u64;
            match runs.last_mut() {
                Some(run) if run.end == start => run.end = end,
                _ => runs.push(start..end),
            }
        }
        Ok(Some(runs))
    }

    /// The columns that have zones, in `(s, p, o, g)` order.
    #[cfg(test)]
    pub(crate) fn columns(&self) -> Vec<String> {
        PRIMARY_COLUMNS
            .iter()
            .zip(&self.0)
            .filter(|(_, zones)| zones.is_some())
            .map(|(name, _)| name.to_string())
            .collect()
    }
}

/// A column's zone bounds gathered in row order, a run of rows at a time,
/// each run within one zone ([`room`](Self::room)): the pass that compresses
/// a build's codes reads each zone's smallest and largest code as it goes,
/// so the bounds cost a few instructions a zone.
pub(crate) struct ZoneBuilder {
    mins: Vec<TermCode>,
    maxes: Vec<TermCode>,
    lo: TermCode,
    hi: TermCode,
    /// Rows in the zone being filled.
    filled: usize,
    /// Rows seen in all.
    rows: usize,
}

impl ZoneBuilder {
    pub(crate) fn new() -> Self {
        Self {
            mins: Vec::new(),
            maxes: Vec::new(),
            lo: TermCode::MAX,
            hi: TermCode::MIN,
            filled: 0,
            rows: 0,
        }
    }

    /// How many rows the zone being filled still takes.
    pub(crate) fn room(&self) -> usize {
        ZONE_ROWS - self.filled
    }

    /// Fold in the next `rows` rows, at most [`room`](Self::room), whose
    /// codes lie within `lo..=hi`.
    pub(crate) fn push(&mut self, rows: usize, lo: TermCode, hi: TermCode) {
        debug_assert!(rows <= self.room(), "a run must end within its zone");
        if rows == 0 {
            return;
        }
        self.lo = self.lo.min(lo);
        self.hi = self.hi.max(hi);
        self.filled += rows;
        self.rows += rows;
        if self.filled == ZONE_ROWS {
            self.close();
        }
    }

    fn close(&mut self) {
        self.mins.push(self.lo);
        self.maxes.push(self.hi);
        (self.lo, self.hi, self.filled) = (TermCode::MAX, TermCode::MIN, 0);
    }

    /// The column's zone map, if the runs folded in cover exactly `rows`
    /// rows (a column whose codes were only partly seen has none); the last
    /// zone is as short as the rows left.
    pub(crate) fn finish(mut self, rows: usize) -> Result<Option<ColumnZones>> {
        if self.rows != rows || rows == 0 {
            return Ok(None);
        }
        if self.filled > 0 {
            self.close();
        }
        ColumnZones::from_bounds(
            self.mins.into(),
            self.maxes.into(),
            ZONE_ROWS as u64,
            rows as u64,
        )
        .map(Some)
    }
}

/// The graphs each [`ZONE_ROWS`]-row zone of a built graph column holds, with
/// their rows, as Vortex's accumulator of `vortexrdf.value_counts` records
/// them in a written zone table: `None` for a zone past its 16 values.
pub(crate) fn graph_records(codes: &[TermCode]) -> Result<RecordedZones> {
    use crate::session::VORTEX_SESSION;
    use crate::store::value_counts::{MAX_VALUES, ValueCounts, ValueCountsOptions, decode_state};
    use vortex_array::aggregate_fn::{Accumulator, DynAccumulator as _};
    use vortex_array::{IntoArray as _, VortexSessionExecute as _};

    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let options = ValueCountsOptions {
        max_values: MAX_VALUES,
    };
    codes
        .chunks(ZONE_ROWS)
        .map(|zone| {
            let mut acc = Accumulator::try_new(ValueCounts, options, CODE_DTYPE)?;
            let array =
                vortex_array::arrays::PrimitiveArray::from_iter(zone.iter().copied()).into_array();
            acc.accumulate(&array, &mut ctx)?;
            Ok(decode_state(&acc.finish()?).map(Vec::into_boxed_slice))
        })
        .collect()
}

/// Whether slot `slot` is the graph column, whose zones record their graphs.
pub(crate) fn records_graphs(slot: usize) -> bool {
    slot == GRAPH_SLOT
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A zone's bounds are its own codes' smallest and largest, whether the
    /// codes arrive in one slice or across slices cut anywhere; the last zone
    /// is as short as the rows left, and a column seen only in part has none.
    #[test]
    fn zones_bound_their_own_codes_however_the_slices_cut_them() {
        let codes: Vec<TermCode> = (0..2 * ZONE_ROWS as u64 + 5)
            .map(|i| (i * 7919) % 100_003)
            .collect();
        let want: Vec<(TermCode, TermCode)> = codes
            .chunks(ZONE_ROWS)
            .map(|zone| (*zone.iter().min().unwrap(), *zone.iter().max().unwrap()))
            .collect();
        assert_eq!(want.len(), 3);
        let built = |cut: usize| {
            let mut builder = ZoneBuilder::new();
            for slice in codes.chunks(cut) {
                let mut rest = slice;
                while !rest.is_empty() {
                    let (run, next) = rest.split_at(builder.room().min(rest.len()));
                    let lo = run.iter().copied().min().unwrap();
                    let hi = run.iter().copied().max().unwrap();
                    builder.push(run.len(), lo, hi);
                    rest = next;
                }
            }
            builder
                .finish(codes.len())
                .unwrap()
                .expect("every row seen")
        };
        for cut in [1, 100, ZONE_ROWS - 1, ZONE_ROWS, ZONE_ROWS + 3, codes.len()] {
            assert_eq!(built(cut).bounds(), want, "slices of {cut}");
        }
        assert!(ZoneBuilder::new().finish(codes.len()).unwrap().is_none());
        // A table keeps only the columns zoned for the base's row count.
        let mut zones = QuadZones::default();
        zones.set(0, built(codes.len()));
        let short = &codes[..ZONE_ROWS];
        let mut builder = ZoneBuilder::new();
        builder.push(short.len(), 0, 1);
        zones.set(2, builder.finish(short.len()).unwrap().unwrap());
        assert_eq!(zones.covering(codes.len()).columns(), ["s"]);
    }

    /// A built graph column's records are each zone's graphs with their rows,
    /// and none for a zone of more than 16.
    #[test]
    fn graph_records_count_each_zones_graphs() {
        let mut codes: Vec<TermCode> = (0..ZONE_ROWS as u64).map(|i| 1 + i % 3).collect();
        codes.extend((0..ZONE_ROWS as u64).map(|i| 1 + i % 17));
        let records = graph_records(&codes).unwrap();
        let third = (ZONE_ROWS / 3) as u32;
        let first = vec![(1, third + 1), (2, third + 1), (3, third)];
        assert_eq!(records, vec![Some(first.into_boxed_slice()), None]);
    }
}
