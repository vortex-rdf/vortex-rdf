//! The zone level of the statistics ladder: how each zone of a table stands
//! against the bound slots a located run checks, decided by each zone's
//! exact bounds and the values a graph column's zones record
//! ([`ColumnZones::eq_holds`]): no row of the zone holds every checked code,
//! every row does, or the zone is mixed.
//!
//! The ladder is the order a located run asks the statistics in, each level
//! only for what the one before left open: the file's footer settles a
//! checked slot for the whole file (level 1), the zone maps class each zone
//! (level 2, this module), and a graph column's recorded values count a
//! mixed zone without reading its rows (level 3).

use std::ops::Range;
use std::sync::Arc;

use crate::io::zone_map::ColumnZones;

/// How a zone stands against a run's checked slots.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ZoneClass {
    /// No row of the zone holds every checked code.
    None,
    /// Every row of the zone holds every checked code.
    All,
    /// Neither: `unsettled` has bit `1 << slot` for each checked slot whose
    /// column is neither none nor all in the zone, the slots left to check.
    Mixed { unsettled: u8 },
}

/// A checked slot of a run: the code it is checked for and its column's
/// zone map in the run's table.
pub(crate) struct CheckedColumn {
    pub(crate) slot: usize,
    pub(crate) code: u64,
    pub(crate) zones: Arc<ColumnZones>,
}

/// The classes of a table's zones against a run's checked slots.
pub(crate) struct ZoneDecider {
    columns: Vec<CheckedColumn>,
}

impl ZoneDecider {
    /// The decider of `columns`; `None` without columns, or for columns of
    /// different zones (lengths or counts).
    pub(crate) fn new(columns: Vec<CheckedColumn>) -> Option<Self> {
        let first = columns.first()?;
        let (zone_len, zones) = (first.zones.zone_len(), first.zones.zones());
        columns
            .iter()
            .all(|c| c.zones.zone_len() == zone_len && c.zones.zones() == zones)
            .then_some(Self { columns })
    }

    /// Rows per zone, the last zone's excepted.
    pub(crate) fn zone_len(&self) -> u64 {
        self.columns[0].zones.zone_len()
    }

    /// The table's rows in zone `zone`.
    pub(crate) fn zone_rows(&self, zone: u64) -> Range<u64> {
        self.columns[0].zones.zone_rows(zone)
    }

    /// The rows of zone `zone` holding the code of checked slot `slot`, as
    /// the slot's column recorded them
    /// ([`ColumnZones::recorded_count`]): `None` where the column records
    /// none for the zone, or `slot` is not checked.
    pub(crate) fn recorded_count(&self, slot: usize, zone: u64) -> Option<u64> {
        let column = self.columns.iter().find(|column| column.slot == slot)?;
        column.zones.recorded_count(zone, column.code)
    }

    /// The class of each zone of `zones`, in order, by how each column's
    /// zone stands against its code ([`ColumnZones::eq_holds`]):
    /// [`ZoneClass::None`] where any column's zone holds it in no row,
    /// [`ZoneClass::All`] where every column's holds it in every row,
    /// [`ZoneClass::Mixed`] otherwise, naming the columns neither settles.
    pub(crate) fn classify(&self, zones: Range<u64>) -> Vec<ZoneClass> {
        crate::observe::observe!(ZonesClassed(zones.end - zones.start));
        zones
            .map(|zone| {
                let mut unsettled = 0u8;
                for column in &self.columns {
                    match column.zones.eq_holds(zone, column.code) {
                        Some(false) => return ZoneClass::None,
                        Some(true) => {}
                        None => unsettled |= 1 << column.slot,
                    }
                }
                match unsettled {
                    0 => ZoneClass::All,
                    _ => ZoneClass::Mixed { unsettled },
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::schema::TermCode;

    const Z: u64 = 8_192;

    /// A column at `slot`, checked for `code`, with zones of these bounds.
    fn checked(slot: usize, code: TermCode, bounds: &[(u64, u64)]) -> CheckedColumn {
        let zones = ColumnZones::from_bounds(
            bounds.iter().map(|b| b.0).collect(),
            bounds.iter().map(|b| b.1).collect(),
            Z,
            Z * bounds.len() as u64,
        )
        .unwrap();
        CheckedColumn {
            slot,
            code,
            zones: Arc::new(zones),
        }
    }

    fn graph_bounds() -> Vec<(u64, u64)> {
        vec![(5, 5), (1, 4), (6, 9), (5, 6), (4, 5)]
    }

    /// One checked column: all where its bounds are the code alone, none
    /// where they leave it out, mixed where they hold it among others, its
    /// bounds' ends included.
    #[test]
    fn one_column_classes() {
        let decider = ZoneDecider::new(vec![checked(3, 5, &graph_bounds())]).unwrap();
        assert_eq!(decider.zone_len(), Z);
        let mixed = ZoneClass::Mixed { unsettled: 1 << 3 };
        assert_eq!(
            decider.classify(0..5),
            [
                ZoneClass::All,
                ZoneClass::None,
                ZoneClass::None,
                mixed,
                mixed
            ]
        );
    }

    /// Two checked columns: none where either says none, all where both say
    /// all, and a mixed zone names the columns still unsettled in it.
    #[test]
    fn two_columns_combine() {
        let o = checked(2, 7, &[(7, 7), (7, 7), (1, 9), (8, 9), (6, 8), (7, 7)]);
        let g = checked(3, 5, &[(5, 5), (1, 4), (6, 9), (5, 6), (4, 6), (5, 6)]);
        let decider = ZoneDecider::new(vec![o, g]).unwrap();
        assert_eq!(
            decider.classify(0..6),
            [
                ZoneClass::All,
                ZoneClass::None,
                ZoneClass::None,
                ZoneClass::None,
                ZoneClass::Mixed {
                    unsettled: 1 << 2 | 1 << 3
                },
                ZoneClass::Mixed { unsettled: 1 << 3 },
            ]
        );
    }

    /// Exact bounds settle a zone before its recorded values do: a zone
    /// whose bounds are the code alone is all though its record lacks the
    /// code (a record no store writes, contradicting the bounds), as
    /// [`ColumnZones::eq_holds`] has it; a zone whose bounds hold the code
    /// among others is none where its record lacks it.
    #[test]
    fn exact_bounds_settle_before_records() {
        let mut column = checked(3, 5, &[(5, 5), (4, 6)]);
        column.zones = Arc::new(
            ColumnZones::from_bounds(
                vortex_buffer::buffer![5u64, 4],
                vortex_buffer::buffer![5u64, 6],
                Z,
                2 * Z,
            )
            .unwrap()
            .with_records(vec![
                Some(vec![(6, Z as u32)].into()),
                Some(vec![(4, 8_000), (6, 192)].into()),
            ])
            .unwrap(),
        );
        let decider = ZoneDecider::new(vec![column]).unwrap();
        assert_eq!(decider.classify(0..2), [ZoneClass::All, ZoneClass::None]);
    }

    /// A range of zones is decided alone, and an empty one is nothing.
    #[test]
    fn partial_and_empty_ranges() {
        let decider = ZoneDecider::new(vec![checked(3, 5, &graph_bounds())]).unwrap();
        assert_eq!(decider.classify(1..3), [ZoneClass::None, ZoneClass::None]);
        assert!(decider.classify(2..2).is_empty());
    }

    /// No decider without columns, or over columns zoned differently.
    #[test]
    fn new_declines() {
        assert!(ZoneDecider::new(Vec::new()).is_none());
        let other = ColumnZones::from_bounds(
            vortex_buffer::buffer![5u64; 10],
            vortex_buffer::buffer![5u64; 10],
            Z / 2,
            Z * 5,
        )
        .unwrap();
        let other = CheckedColumn {
            slot: 2,
            code: 5,
            zones: Arc::new(other),
        };
        assert!(ZoneDecider::new(vec![checked(3, 5, &graph_bounds()), other]).is_none());
    }
}
