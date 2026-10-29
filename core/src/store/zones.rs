//! Per-zone code bounds of an in-memory base's code columns: the in-memory
//! counterpart of a written file's zone maps. A residual equality over a base
//! that no index serves compares only the zones whose bounds hold its code,
//! as the file path's zone-map pruning reads only those. The bounds are taken
//! where they cost next to nothing: in the pass over a build's codes that
//! picks their compression ([`ZoneBuilder`]), or from the zone maps of the
//! file a store is loaded from ([`ZoneTable::read_from_file`]). A base with
//! neither has none, and its residual filter compares every row as before.

use std::sync::Arc;

use vortex_array::dtype::FieldName;

use crate::store::schema::TermCode;

/// Rows per zone: a written file's zone-map block, so a loaded store's bounds
/// are the file's own.
pub(crate) const ZONE_ROWS: usize = 8_192;

/// The smallest and largest code of each zone of one code column, in row
/// order: zone `z` covers rows `z * ZONE_ROWS` up to the next zone's first.
#[derive(Debug, PartialEq)]
pub(crate) struct CodeZones {
    bounds: Vec<(TermCode, TermCode)>,
}

impl CodeZones {
    /// The zones of a column's codes, given as consecutive slices in row
    /// order (a zone may span slices).
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
pub(crate) struct ZoneTable(Vec<(FieldName, Arc<CodeZones>)>);

impl ZoneTable {
    /// The zones of a store file's quad code columns, read from their zone
    /// maps: each non-nullable `u64` column zoned in [`ZONE_ROWS`]-row zones
    /// with exact `max` and `min` (every column this crate writes). `quads`
    /// is the quad child's struct layout, its segments read from `source`; a
    /// column without such a map has no zones. The columns' tables are read
    /// concurrently: over a file read through I/O, each read waits.
    pub(crate) async fn read_from_file(
        quads: &vortex_layout::LayoutRef,
        source: &Arc<dyn vortex_layout::segments::SegmentSource>,
    ) -> crate::error::Result<Self> {
        use vortex_array::dtype::{DType, PType};
        use vortex_layout::LayoutChildType;
        use vortex_layout::layouts::zoned::Zoned;

        if quads
            .as_opt::<vortex_layout::layouts::struct_::Struct>()
            .is_none()
        {
            return Ok(Self::default());
        }
        let [max, min] = zone_bound_names();
        let mut tables = Vec::new();
        for slot in 0..quads.nslots() {
            let Some(LayoutChildType::Field(name)) = quads.slot_type(slot) else {
                continue;
            };
            let Some(column) = quads.slot(slot)? else {
                continue;
            };
            let Some(zoned) = column.as_opt::<Zoned>() else {
                continue;
            };
            let present = zoned.present_aggregates();
            if !matches!(column.dtype(), DType::Primitive(PType::U64, n) if !n.is_nullable())
                || zoned.zone_len() != ZONE_ROWS
                || !present.contains(&max)
                || !present.contains(&min)
            {
                continue;
            }
            let Some(bounds) = column.slot(1)? else {
                continue;
            };
            if bounds.row_count() == column.row_count().div_ceil(ZONE_ROWS as u64) {
                tables.push((name, bounds));
            }
        }
        let read = tables.into_iter().map(|(name, bounds)| {
            let (source, max, min) = (Arc::clone(source), &max, &min);
            async move {
                let zones = read_bounds(bounds, source, max, min).await?;
                crate::error::Result::Ok(zones.map(|zones| (name, zones)))
            }
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
        self.0.push((name, Arc::new(zones)));
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
            .map(|(_, zones)| &**zones)
    }

    /// The columns that have zones, in the order they were recorded.
    #[cfg(test)]
    pub(crate) fn columns(&self) -> Vec<String> {
        self.0.iter().map(|(name, _)| name.to_string()).collect()
    }
}

/// One column's zones from its zone table, `bounds` (one row per zone, a
/// column per aggregate): `None` unless the `max` and `min` columns are
/// `u64` and valid in every zone.
async fn read_bounds(
    bounds: vortex_layout::LayoutRef,
    source: Arc<dyn vortex_layout::segments::SegmentSource>,
    max: &str,
    min: &str,
) -> crate::error::Result<Option<CodeZones>> {
    use vortex_array::VortexSessionExecute as _;
    use vortex_array::arrays::PrimitiveArray;
    use vortex_array::arrays::struct_::{StructArray, StructArrayExt as _};
    use vortex_array::dtype::PType;

    let reader = bounds.new_reader(
        "quad-zones".into(),
        source,
        &crate::session::VORTEX_SESSION,
        &Default::default(),
    )?;
    let mut ctx = crate::session::VORTEX_SESSION.create_execution_ctx();
    let table = crate::io::read::scan_all_reader(reader)
        .await?
        .execute::<StructArray>(&mut ctx)?;
    let mut column_of = |aggregate: &str| -> crate::error::Result<Option<Vec<TermCode>>> {
        let Ok(field) = table.unmasked_field_by_name(aggregate) else {
            return Ok(None);
        };
        if !field.all_valid(&mut ctx)? {
            return Ok(None);
        }
        let values = field.clone().execute::<PrimitiveArray>(&mut ctx)?;
        Ok((values.ptype() == PType::U64).then(|| values.as_slice::<TermCode>().to_vec()))
    };
    let (Some(maxes), Some(mins)) = (column_of(max)?, column_of(min)?) else {
        return Ok(None);
    };
    Ok(Some(CodeZones::from_bounds(
        mins.into_iter().zip(maxes).collect(),
    )))
}

/// The zone-map aggregate names of exact `max` and `min`, as a written
/// file's zone table names its columns.
fn zone_bound_names() -> [String; 2] {
    let aggregates = crate::store::layouts::dictionary::storage::window_bound_aggregates();
    [aggregates[0].to_string(), aggregates[1].to_string()]
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
