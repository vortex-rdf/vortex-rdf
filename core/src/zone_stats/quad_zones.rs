//! The zone maps of an in-memory base's code columns: P1's [`ColumnZones`]
//! (each 8,192-row zone's exact bounds) per quad code column, `g`'s with the
//! graphs each zone recorded (P2's `vortexrdf.value_counts`), as a file store's
//! located runs read them. A residual equality over a base that no index serves
//! classes the zones its selection touches against them
//! ([`ResidualCompare`](crate::store::scan::residual::ResidualCompare)): it
//! skips the zones a bound column's bounds or `g`'s records rule out and
//! compares only the rest, as the file path's zone-map pruning reads only the
//! zones it leaves. With zones off it skips the zones the bounds alone rule
//! out, compared in pieces. A build takes the bounds, and `g`'s records, in
//! the pass that picks each code column's compression, which reads every
//! code anyway ([`ZoneBuilder`]); a store loaded from a file or from bytes
//! reads both from the file's zone tables ([`QuadZones::read`]). A zone map
//! also keeps its column's whole bounds, the zones' folded
//! ([`ColumnZones::column_bounds`]), which a residual asks first. A base with
//! neither has none, and its residual compares every row.

use std::sync::Arc;

use crate::error::Result;
use crate::store::schema::{CODE_DTYPE, PRIMARY_COLUMNS, TermCode};
use crate::zone_stats::value_counts::{Counts, MAX_VALUES};
use crate::zone_stats::zone_map::{ColumnZones, ExactZoneMap, RecordedZones, ZONE_ROWS};

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
            if zoned_code_column(&column) {
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

/// A column's zone map gathered in row order, a run of rows at a time, each
/// run within one zone ([`room`](Self::room)): the pass that compresses a
/// build's codes reads each zone's smallest and largest code as it goes, so
/// the bounds cost a few instructions a zone. A graph column's builder
/// ([`recording`](Self::recording)) counts each zone's graphs with their rows
/// in the same pass, as the file writer's `vortexrdf.value_counts` records
/// them ([`Counts`]).
pub(crate) struct ZoneBuilder {
    mins: Vec<TermCode>,
    maxes: Vec<TermCode>,
    lo: TermCode,
    hi: TermCode,
    /// Rows in the zone being filled.
    filled: usize,
    /// Rows seen in all.
    rows: usize,
    /// The closed zones' recorded values, beside the counts of the zone being
    /// filled; `None` for a column that records none.
    records: Option<(RecordedZones, Counts)>,
}

impl ZoneBuilder {
    /// A builder of a column's zone bounds alone.
    pub(crate) fn new() -> Self {
        Self {
            mins: Vec::new(),
            maxes: Vec::new(),
            lo: TermCode::MAX,
            hi: TermCode::MIN,
            filled: 0,
            rows: 0,
            records: None,
        }
    }

    /// A builder of a column's zone bounds and of each zone's values with
    /// their rows: a graph column's.
    pub(crate) fn recording() -> Self {
        Self {
            records: Some((Vec::new(), Counts::new())),
            ..Self::new()
        }
    }

    /// How many rows the zone being filled still takes.
    pub(crate) fn room(&self) -> usize {
        ZONE_ROWS - self.filled
    }

    /// Fold in the next rows' codes `part`, at most [`room`](Self::room) of
    /// them, which lie within `lo..=hi`.
    pub(crate) fn push(&mut self, part: &[TermCode], lo: TermCode, hi: TermCode) {
        debug_assert!(part.len() <= self.room(), "a run must end within its zone");
        if part.is_empty() {
            return;
        }
        if let Some((_, counts)) = &mut self.records {
            counts.add_all(part, MAX_VALUES);
        }
        self.lo = self.lo.min(lo);
        self.hi = self.hi.max(hi);
        self.filled += part.len();
        self.rows += part.len();
        if self.filled == ZONE_ROWS {
            self.close();
        }
    }

    fn close(&mut self) {
        self.mins.push(self.lo);
        self.maxes.push(self.hi);
        if let Some((zones, counts)) = &mut self.records {
            zones.push(counts.recorded());
            *counts = Counts::new();
        }
        (self.lo, self.hi, self.filled) = (TermCode::MAX, TermCode::MIN, 0);
    }

    /// The column's zone map, if the runs folded in cover exactly `rows`
    /// rows (a column whose codes were only partly seen has none), with each
    /// zone's recorded values where the builder records them; the last zone
    /// is as short as the rows left.
    pub(crate) fn finish(mut self, rows: usize) -> Result<Option<ColumnZones>> {
        if self.rows != rows || rows == 0 {
            return Ok(None);
        }
        if self.filled > 0 {
            self.close();
        }
        let zones = ColumnZones::from_bounds(
            self.mins.into(),
            self.maxes.into(),
            ZONE_ROWS as u64,
            rows as u64,
        )?;
        match self.records {
            Some((records, _)) => zones.with_records(records).map(Some),
            None => Ok(Some(zones)),
        }
    }
}

/// Whether `column` is a code column zoned in [`ZONE_ROWS`]-row zones with
/// exact bounds, a zone per [`ZONE_ROWS`] of its rows: a zone table of other
/// zones would not line up with an in-memory copy of its rows, so such a
/// column has none.
fn zoned_code_column(column: &vortex_layout::LayoutRef) -> bool {
    column.dtype() == &CODE_DTYPE
        && ExactZoneMap::of(column).is_some_and(|map| {
            map.zone_len == ZONE_ROWS
                && map.table.row_count() == column.row_count().div_ceil(ZONE_ROWS as u64)
        })
}

/// The zone map an in-memory index component keeps of its child's graph
/// column `column`, read from the column's zone table with each zone's
/// recorded graphs ([`ColumnZones::read`]): `None` without the column, or
/// when it is not a code column zoned as a base's are
/// ([`QuadZones::read`]), as a string column or a child written without the
/// zones.
pub(crate) async fn read_graph_zones(
    column: Option<vortex_layout::LayoutRef>,
    source: &Arc<dyn vortex_layout::segments::SegmentSource>,
) -> Result<Option<Arc<ColumnZones>>> {
    match column.filter(zoned_code_column) {
        Some(column) => Ok(ColumnZones::read(&column, source).await?.map(Arc::new)),
        None => Ok(None),
    }
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
                    builder.push(run, lo, hi);
                    rest = next;
                }
            }
            builder
                .finish(codes.len())
                .unwrap()
                .expect("every row seen")
        };
        for cut in [1, 100, ZONE_ROWS - 1, ZONE_ROWS, ZONE_ROWS + 3, codes.len()] {
            let zones = built(cut);
            assert_eq!(zones.zone_bounds_list(), want, "slices of {cut}");
            let folded = (
                want.iter().map(|z| z.0).min(),
                want.iter().map(|z| z.1).max(),
            );
            assert_eq!(
                zones.column_bounds(),
                folded.0.zip(folded.1),
                "the column's, slices of {cut}"
            );
            assert!(zones.recorded().is_none(), "bounds alone");
        }
        assert!(ZoneBuilder::new().finish(codes.len()).unwrap().is_none());
        // A table keeps only the columns zoned for the base's row count.
        let mut zones = QuadZones::default();
        zones.set(0, built(codes.len()));
        let short = &codes[..ZONE_ROWS];
        let mut builder = ZoneBuilder::new();
        builder.push(short, 0, 1);
        zones.set(2, builder.finish(short.len()).unwrap().unwrap());
        assert_eq!(zones.covering(codes.len()).columns(), ["s"]);
    }

    /// Each [`ZONE_ROWS`]-row zone's graphs with their rows, as Vortex's
    /// accumulator of `vortexrdf.value_counts` records them in a written zone
    /// table (the file writer's records): `None` for a zone past its 16
    /// values. The oracle the compress pass's records are held to.
    fn accumulated_records(
        column: &vortex_array::ArrayRef,
        ctx: &mut vortex_array::ExecutionCtx,
    ) -> RecordedZones {
        use crate::zone_stats::value_counts::{
            MAX_VALUES, ValueCounts, ValueCountsOptions, decode_state,
        };
        use vortex_array::aggregate_fn::{Accumulator, DynAccumulator as _};

        let options = ValueCountsOptions {
            max_values: MAX_VALUES,
        };
        let mut records = Vec::new();
        for start in (0..column.len()).step_by(ZONE_ROWS) {
            let end = (start + ZONE_ROWS).min(column.len());
            let mut acc = Accumulator::try_new(ValueCounts, options, CODE_DTYPE).unwrap();
            acc.accumulate(&column.slice(start..end).unwrap(), ctx)
                .unwrap();
            records.push(decode_state(&acc.finish().unwrap()).map(Vec::into_boxed_slice));
        }
        records
    }

    /// The pass that compresses a graph column records each zone's graphs
    /// with their rows as the file writer's accumulator does over the same
    /// rows: a zone of 3 graphs, one of 17 (past the 16 the statistic
    /// records, so none), one of exactly 16, and a short last zone; the same
    /// whether the column is one array or chunks of 7, 8,192 or 20,000 rows,
    /// in a base or an index component.
    #[test]
    fn one_pass_records_each_zones_graphs() {
        use crate::session::VORTEX_SESSION;
        use crate::store::resident::{CodeTable, with_compressed_int_children};
        use vortex_array::arrays::{ChunkedArray, PrimitiveArray, StructArray};
        use vortex_array::validity::Validity;
        use vortex_array::{IntoArray as _, VortexSessionExecute as _};

        let mut codes: Vec<TermCode> = (0..ZONE_ROWS as u64).map(|i| 1 + i % 3).collect();
        codes.extend((0..ZONE_ROWS as u64).map(|i| 1 + i % 17));
        codes.extend((0..ZONE_ROWS as u64).map(|i| 100 + i / 512));
        codes.extend([5, 5, 6]);
        let column = |rows: usize| -> vortex_array::ArrayRef {
            let chunks: Vec<vortex_array::ArrayRef> = codes
                .chunks(rows)
                .map(|chunk| PrimitiveArray::from_iter(chunk.iter().copied()).into_array())
                .collect();
            match chunks.len() {
                1 => chunks[0].clone(),
                _ => ChunkedArray::try_new(chunks, CODE_DTYPE)
                    .unwrap()
                    .into_array(),
            }
        };
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let want = accumulated_records(&column(codes.len()), &mut ctx);
        let third = (ZONE_ROWS / 3) as u32;
        let sixteen: Vec<(u64, u32)> = (100..116).map(|graph| (graph, 512)).collect();
        assert_eq!(
            want,
            vec![
                Some(vec![(1, third + 1), (2, third + 1), (3, third)].into_boxed_slice()),
                None,
                Some(sixteen.into_boxed_slice()),
                Some(vec![(5, 2), (6, 1)].into_boxed_slice()),
            ],
            "the oracle"
        );
        for rows in [7, ZONE_ROWS, 20_000, codes.len()] {
            let quads = StructArray::try_new(
                ["g"].into(),
                vec![column(rows)],
                codes.len(),
                Validity::NonNullable,
            )
            .unwrap()
            .into_array();
            for table in [CodeTable::Base, CodeTable::Component] {
                let (_, zones) = with_compressed_int_children(quads.clone(), table).unwrap();
                let graphs = zones.get(GRAPH_SLOT).expect("g's zone map");
                assert_eq!(
                    graphs.recorded(),
                    Some(&want),
                    "chunks of {rows}, {table:?}"
                );
            }
        }
    }
}
