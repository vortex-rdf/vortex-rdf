//! Per-zone code bounds of an in-memory base's code columns: the in-memory
//! counterpart of a written file's zone maps. A residual equality over a base
//! that no index serves compares only the zones whose bounds hold its code
//! ([`ZoneTable::runs_holding`]), as the file path's zone-map pruning reads
//! only those. A build takes the bounds in the pass that picks each code
//! column's compression, which reads every code anyway ([`ZoneBuilder`]); a
//! store loaded from a file or from bytes reads them from the file's zone maps
//! ([`ZoneTable::read_from_file`]). A base with neither has none, and its
//! residual filter compares every row.

use std::ops::Range;
use std::sync::Arc;

use vortex_array::dtype::{DType, FieldName, Nullability, PType};
use vortex_array::scalar::Scalar;

use crate::error::Result;
use crate::io::zone_map::{ExactZoneMap, ZONE_ROWS};
use crate::store::array::unsigned_value;
use crate::store::schema::TermCode;

/// The dtype of a code column that carries zone bounds.
const CODE_DTYPE: DType = DType::Primitive(PType::U64, Nullability::NonNullable);

/// The smallest and largest code of each zone of one code column, in row
/// order: zone `z` covers rows `z * ZONE_ROWS` up to the next zone's first.
#[derive(Debug, PartialEq)]
pub(crate) struct CodeZones {
    bounds: Vec<(TermCode, TermCode)>,
}

impl CodeZones {
    /// The zones of a column's codes, given as consecutive slices in row
    /// order (a zone may span slices), through a [`ZoneBuilder`].
    #[cfg(test)]
    pub(crate) fn of_slices<'a>(slices: impl IntoIterator<Item = &'a [TermCode]>) -> Self {
        let mut builder = ZoneBuilder::new();
        for slice in slices {
            let mut rest = slice;
            while !rest.is_empty() {
                let (run, next) = rest.split_at(builder.room().min(rest.len()));
                let lo = run.iter().copied().min().unwrap_or(TermCode::MAX);
                let hi = run.iter().copied().max().unwrap_or(TermCode::MIN);
                builder.push(run.len(), lo, hi);
                rest = next;
            }
        }
        let rows = builder.rows;
        builder.finish(rows).expect("the builder saw every row")
    }

    /// Zones from their `(min, max)` bounds, in row order.
    pub(crate) fn from_bounds(bounds: Vec<(TermCode, TermCode)>) -> Self {
        Self { bounds }
    }

    /// How many zones the column has.
    pub(crate) fn zones(&self) -> usize {
        self.bounds.len()
    }

    /// Each zone's `(min, max)`, in row order.
    #[cfg(test)]
    pub(crate) fn bounds(&self) -> &[(TermCode, TermCode)] {
        &self.bounds
    }

    /// Whether zone `zone`'s bounds hold `code`.
    pub(crate) fn may_hold(&self, zone: usize, code: TermCode) -> bool {
        let (lo, hi) = self.bounds[zone];
        lo <= code && code <= hi
    }
}

/// A column's zones gathered in row order, a run of rows at a time, each run
/// within one zone ([`room`](Self::room)): the pass that compresses a build's
/// codes reads each zone's smallest and largest code as it goes, so the
/// bounds cost a few instructions a zone.
pub(crate) struct ZoneBuilder {
    bounds: Vec<(TermCode, TermCode)>,
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
            bounds: Vec::new(),
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
            self.bounds.push((self.lo, self.hi));
            (self.lo, self.hi, self.filled) = (TermCode::MAX, TermCode::MIN, 0);
        }
    }

    /// The zones, if the runs folded in cover exactly `rows` rows (a column
    /// whose codes were only partly seen has none); the last zone is as
    /// short as the rows left.
    pub(crate) fn finish(mut self, rows: usize) -> Option<CodeZones> {
        if self.rows != rows {
            return None;
        }
        if self.filled > 0 {
            self.bounds.push((self.lo, self.hi));
        }
        Some(CodeZones {
            bounds: self.bounds,
        })
    }
}

/// A base's code columns' zones, by column name; columns without bounds are
/// absent.
#[derive(Debug, Default)]
pub(crate) struct ZoneTable(Vec<(FieldName, CodeZones)>);

impl ZoneTable {
    /// The zones of a store file's quad code columns, read from their zone
    /// maps: each non-nullable `u64` column zoned in [`ZONE_ROWS`]-row zones
    /// with exact `max` and `min` ([`ExactZoneMap`], every column this crate
    /// writes). `quads` is the quad child's struct layout, its segments read
    /// from `source`; a column without such a map has no zones. The columns'
    /// tables are read concurrently: over a file read through I/O, each read
    /// waits.
    pub(crate) async fn read_from_file(
        quads: &vortex_layout::LayoutRef,
        source: &Arc<dyn vortex_layout::segments::SegmentSource>,
    ) -> Result<Self> {
        use vortex_array::arrays::PrimitiveArray;
        use vortex_layout::LayoutChildType;

        if quads
            .as_opt::<vortex_layout::layouts::struct_::Struct>()
            .is_none()
        {
            return Ok(Self::default());
        }
        let mut maps = Vec::new();
        for slot in 0..quads.nslots() {
            let Some(LayoutChildType::Field(name)) = quads.slot_type(slot) else {
                continue;
            };
            let Some(column) = quads.slot(slot)? else {
                continue;
            };
            if column.dtype() != &CODE_DTYPE {
                continue;
            }
            let Some(map) = ExactZoneMap::of(&column) else {
                continue;
            };
            let zones = column.row_count().div_ceil(ZONE_ROWS as u64);
            if map.zone_len == ZONE_ROWS && map.table.row_count() == zones {
                maps.push((name, map));
            }
        }
        let read = maps.into_iter().map(|(name, map)| async move {
            let bounds = map.read_bounds::<PrimitiveArray>(source).await?;
            let zones = bounds
                .filter(|(maxes, mins)| maxes.ptype() == PType::U64 && mins.ptype() == PType::U64);
            Result::Ok(zones.map(|(maxes, mins)| {
                let (maxes, mins) = (maxes.as_slice::<TermCode>(), mins.as_slice::<TermCode>());
                let bounds = mins.iter().copied().zip(maxes.iter().copied()).collect();
                (name, CodeZones::from_bounds(bounds))
            }))
        });
        let mut table = Self::default();
        for (name, zones) in futures::future::try_join_all(read)
            .await?
            .into_iter()
            .flatten()
        {
            table.insert(name, zones);
        }
        Ok(table)
    }

    /// Record `zones` as column `name`'s.
    pub(crate) fn insert(&mut self, name: FieldName, zones: CodeZones) {
        self.0.push((name, zones));
    }

    /// The columns whose zones cover `rows` rows: a column with another
    /// number of zones is dropped, since its zones do not line up with a
    /// base of `rows` rows.
    pub(crate) fn covering(mut self, rows: usize) -> Self {
        let zones = rows.div_ceil(ZONE_ROWS);
        self.0.retain(|(_, column)| column.zones() == zones);
        self
    }

    /// Column `name`'s zones, if it has any.
    pub(crate) fn get(&self, name: &str) -> Option<&CodeZones> {
        self.0
            .iter()
            .find(|(column, _)| column.as_ref() == name)
            .map(|(_, zones)| zones)
    }

    /// The rows of the zones whose bounds may hold every code `eqs` binds,
    /// as ascending runs over a base of `rows` rows, adjacent zones joined.
    /// `None` when no bound column has zones, or when the zones left hold
    /// more than half the base: comparing a whole column then costs less than
    /// comparing its runs.
    pub(crate) fn runs_holding(
        &self,
        eqs: &[(&str, Scalar)],
        rows: usize,
    ) -> Option<Vec<Range<u64>>> {
        let mut keep: Option<Vec<bool>> = None;
        for (field, scalar) in eqs {
            let (Some(code), Some(zones)) = (unsigned_value(scalar), self.get(field)) else {
                continue;
            };
            let keep = keep.get_or_insert_with(|| vec![true; zones.zones()]);
            for (zone, kept) in keep.iter_mut().enumerate() {
                *kept &= zones.may_hold(zone, code);
            }
        }
        let keep = keep?;
        let kept = keep.iter().filter(|&&kept| kept).count();
        if kept.saturating_mul(2) > keep.len() {
            return None;
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
        Some(runs)
    }

    /// The columns that have zones, in the order they were recorded.
    #[cfg(test)]
    pub(crate) fn columns(&self) -> Vec<String> {
        self.0.iter().map(|(name, _)| name.to_string()).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A zone's bounds are its own codes' smallest and largest, whether the
    /// codes arrive in one slice or across slices cut anywhere; the last zone
    /// is as short as the rows left.
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
        let whole = CodeZones::of_slices([codes.as_slice()]);
        assert_eq!(whole, CodeZones::from_bounds(want.clone()));
        for cut in [1, 100, ZONE_ROWS - 1, ZONE_ROWS, ZONE_ROWS + 3] {
            let slices = codes.chunks(cut);
            assert_eq!(CodeZones::of_slices(slices), whole, "slices of {cut}");
        }
        assert_eq!(CodeZones::of_slices(std::iter::empty()).zones(), 0);
        // A table keeps only the columns zoned for the base's row count.
        let mut table = ZoneTable::default();
        table.insert("s".into(), CodeZones::of_slices([codes.as_slice()]));
        table.insert("o".into(), CodeZones::of_slices([&codes[..ZONE_ROWS]]));
        assert_eq!(table.covering(codes.len()).columns(), ["s"]);
        for (z, &(lo, hi)) in want.iter().enumerate() {
            assert!(whole.may_hold(z, lo) && whole.may_hold(z, hi));
            assert!(!whole.may_hold(z, hi + 1));
            assert!(lo == 0 || !whole.may_hold(z, lo - 1));
        }
    }
}
