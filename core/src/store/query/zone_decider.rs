//! The zone level of the statistics ladder (spec §3, level 2): how each zone
//! of a table stands against the bound slots a located run checks, decided
//! by Vortex's own proofs of `x = c` ([`eq_proofs`]) on the checked columns'
//! zone maps ([`ColumnZones`]) and by the values a graph column's zones
//! record: no row of the zone holds every checked code, every row does, or
//! the zone is mixed.

use std::ops::Range;
use std::sync::Arc;

use crate::error::Result;
use crate::io::zone_map::ColumnZones;
use crate::stats_rules::EqProofs;
#[cfg(test)]
use crate::stats_rules::eq_proofs;

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

/// A checked slot of a run: the code it is checked for, its column's zone
/// map in the run's table, and Vortex's proofs of `x = code`
/// ([`eq_proofs`](crate::stats_rules::eq_proofs)).
pub(crate) struct CheckedColumn {
    pub(crate) slot: usize,
    pub(crate) code: u64,
    pub(crate) zones: Arc<ColumnZones>,
    pub(crate) proofs: EqProofs,
}

/// The classes of a table's zones against a run's checked slots, each
/// column's zones pruned once per [`classify`](Self::classify) with
/// Vortex's proofs of `x = code`.
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

    /// The class of each zone of `zones`, in order: [`ZoneClass::None`]
    /// where any column's falsifier holds or the values its zone recorded
    /// lack the column's code, [`ZoneClass::All`] where every column's
    /// satisfier does, [`ZoneClass::Mixed`] otherwise, naming the columns
    /// none of these settle.
    pub(crate) fn classify(&self, zones: Range<u64>) -> Result<Vec<ZoneClass>> {
        let n = (zones.end - zones.start) as usize;
        let (mut none, mut all, mut unsettled) = (vec![false; n], vec![true; n], vec![0u8; n]);
        for column in &self.columns {
            let EqProofs {
                none: falsifier,
                all: satisfier,
            } = &column.proofs;
            let holds = |proof: &Option<vortex_array::expr::BoundExpression>| -> Result<Option<vortex_mask::Mask>> {
                proof
                    .as_ref()
                    .map(|proof| column.zones.prune(zones.clone(), proof))
                    .transpose()
            };
            let (no_row, every_row) = (holds(falsifier)?, holds(satisfier)?);
            for z in 0..n {
                let zone = zones.start + z as u64;
                let zone_none = no_row.as_ref().is_some_and(|mask| mask.value(z))
                    || column.zones.recorded_count(zone, column.code) == Some(0);
                let zone_all = every_row.as_ref().is_some_and(|mask| mask.value(z));
                none[z] |= zone_none;
                all[z] &= zone_all;
                if !zone_none && !zone_all {
                    unsettled[z] |= 1 << column.slot;
                }
            }
        }
        Ok((0..n)
            .map(|z| match (none[z], all[z]) {
                (true, _) => ZoneClass::None,
                (false, true) => ZoneClass::All,
                (false, false) => ZoneClass::Mixed {
                    unsettled: unsettled[z],
                },
            })
            .collect())
    }
}

/// Whether located runs decide their checked rows zone by zone:
/// `VORTEX_RDF_ZONES=0` (an ablation switch, read once) puts every run on
/// its row-by-row route, as before zones were asked. Unset is on.
pub(crate) fn zones_enabled() -> bool {
    #[cfg(test)]
    if let Some(on) = crate::store::test_hooks::forced_zones() {
        return on;
    }
    crate::stats_rules::zones_switch()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::schema::TermCode;
    use crate::store::test_hooks::ForcedZones;

    const Z: u64 = 8_192;
    const U64: vortex_array::dtype::DType = vortex_array::dtype::DType::Primitive(
        vortex_array::dtype::PType::U64,
        vortex_array::dtype::Nullability::NonNullable,
    );

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
            proofs: eq_proofs(&U64, code).unwrap(),
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
            decider.classify(0..5).unwrap(),
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
            decider.classify(0..6).unwrap(),
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

    /// A range of zones is decided alone, and an empty one is nothing.
    #[test]
    fn partial_and_empty_ranges() {
        let decider = ZoneDecider::new(vec![checked(3, 5, &graph_bounds())]).unwrap();
        assert_eq!(
            decider.classify(1..3).unwrap(),
            [ZoneClass::None, ZoneClass::None]
        );
        assert!(decider.classify(2..2).unwrap().is_empty());
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
            proofs: eq_proofs(&U64, 5).unwrap(),
        };
        assert!(ZoneDecider::new(vec![checked(3, 5, &graph_bounds()), other]).is_none());
    }

    /// A test forces the switch for its thread; the guard's drop restores it.
    #[test]
    fn forced_zones_override() {
        let production = zones_enabled();
        {
            let _off = ForcedZones::set(false);
            assert!(!zones_enabled());
            let _on = ForcedZones::set(true);
            assert!(zones_enabled());
        }
        assert_eq!(zones_enabled(), production);
    }
}
