//! The zone checks of one table ([`ZoneSet`]): how each zone stands against
//! a set of `x = c` checks, read off their columns' zone maps
//! ([`ColumnZones::eq_holds`]). Every zone walk asks it: a file's located
//! runs, the in-memory residual, an index component's graph check and the
//! file filter's every-row step.

use std::ops::Range;
use std::sync::Arc;

use crate::zone_stats::zone_map::{ColumnZones, eq_on_bounds};

/// How a zone stands against a set's checks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ZoneClass {
    /// No row of the zone passes every check.
    None,
    /// Every row of the zone passes every check.
    All,
    /// Neither: `unsettled` has the bit of each check the zone leaves to
    /// its rows, those the zone maps do not settle and those no zone map
    /// speaks for.
    Mixed { unsettled: u32 },
}

/// A check of `x = code` against a column's zone map, named by `bit` in a
/// class's `unsettled` bits: a located run's slot, a residual equality's
/// place.
pub(crate) struct ZoneCheck {
    pub(crate) bit: u32,
    pub(crate) code: u64,
    pub(crate) zones: Arc<ColumnZones>,
}

/// A table's rows within one zone: zone `zone`'s rows of a range.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ZonePart {
    pub(crate) zone: u64,
    pub(crate) rows: Range<u64>,
}

/// The checks of one table whose columns have zone maps, of one geometry
/// (checked once, [`new`](Self::new)), and the bits of those no zone map
/// speaks for: the classes of the table's zones against all of them
/// ([`class`](Self::class)), a range of its rows cut at its zones
/// ([`parts`](Self::parts)), and a zone's recorded rows of a check's code
/// ([`recorded_in`](Self::recorded_in)).
pub(crate) struct ZoneSet {
    checks: Vec<ZoneCheck>,
    /// The bits of the checks no zone map speaks for, unsettled in every
    /// zone.
    unzoned: u32,
    /// Whether classes read the bounds alone ([`bounds_only`](Self::bounds_only)).
    bounds_only: bool,
}

impl ZoneSet {
    /// The set of `checks`, `unzoned` naming the checks without a zone map;
    /// `None` without checks, for maps of different zone lengths, zone
    /// counts or row counts, or for a bit of 32 or more.
    pub(crate) fn new(checks: Vec<ZoneCheck>, unzoned: u32) -> Option<Self> {
        let first = &checks.first()?.zones;
        let geometry = (first.zone_len(), first.zones(), first.row_count());
        checks
            .iter()
            .all(|check| {
                check.bit < u32::BITS
                    && (
                        check.zones.zone_len(),
                        check.zones.zones(),
                        check.zones.row_count(),
                    ) == geometry
            })
            .then_some(Self {
                checks,
                unzoned,
                bounds_only: false,
            })
    }

    /// This set classing by the bounds alone, as before the zone classes:
    /// [`ZoneClass::None`] where a check's bounds rule its code out, else
    /// [`ZoneClass::Mixed`] on every check, so no zone is taken whole and
    /// no record is read. The route with zones off.
    pub(crate) fn bounds_only(self) -> Self {
        Self {
            bounds_only: true,
            ..self
        }
    }

    /// Rows per zone, the last zone's excepted.
    pub(crate) fn zone_len(&self) -> u64 {
        self.checks[0].zones.zone_len()
    }

    /// How many zones the table has.
    #[cfg(test)]
    pub(crate) fn zones(&self) -> u64 {
        self.checks[0].zones.zones()
    }

    /// The table's rows in zone `zone`.
    pub(crate) fn zone_rows(&self, zone: u64) -> Range<u64> {
        self.checks[0].zones.zone_rows(zone)
    }

    /// The class of zone `zone` against every check
    /// ([`ColumnZones::eq_holds`]): [`ZoneClass::None`] where any check's
    /// zone holds its code in no row, [`ZoneClass::All`] where every check's
    /// holds it in every row and no check is unzoned, [`ZoneClass::Mixed`]
    /// otherwise.
    pub(crate) fn class(&self, zone: u64) -> ZoneClass {
        if self.bounds_only {
            let ruled_out = self.checks.iter().any(|check| {
                let (min, max) = check.zones.zone_bounds(zone);
                eq_on_bounds(min, max, check.code) == Some(false)
            });
            let every = self
                .checks
                .iter()
                .fold(self.unzoned, |bits, check| bits | 1 << check.bit);
            return match ruled_out {
                true => ZoneClass::None,
                false => ZoneClass::Mixed { unsettled: every },
            };
        }
        let mut unsettled = self.unzoned;
        for check in &self.checks {
            match check.zones.eq_holds(zone, check.code) {
                Some(false) => return ZoneClass::None,
                Some(true) => {}
                None => unsettled |= 1 << check.bit,
            }
        }
        match unsettled {
            0 => ZoneClass::All,
            _ => ZoneClass::Mixed { unsettled },
        }
    }

    /// The zones `rows` touches, in order, each with its rows within `rows`
    /// and the table's: none for an empty range.
    pub(crate) fn parts(&self, rows: Range<u64>) -> impl Iterator<Item = ZonePart> + '_ {
        let rows = rows.start..rows.end.min(self.checks[0].zones.row_count());
        let zone_len = self.zone_len();
        let zones = match rows.is_empty() {
            true => 0..0,
            false => rows.start / zone_len..(rows.end - 1) / zone_len + 1,
        };
        zones.map(move |zone| {
            let whole = self.zone_rows(zone);
            ZonePart {
                zone,
                rows: whole.start.max(rows.start)..whole.end.min(rows.end),
            }
        })
    }

    /// The rows of `part` holding the code of the check `bit`, as its
    /// column's zone recorded them: only for a part that is its whole zone,
    /// as a record counts the zone's every row; `None` too where the zone
    /// records none, or no check has the bit.
    pub(crate) fn recorded_in(&self, bit: u32, part: &ZonePart) -> Option<u64> {
        if part.rows != self.zone_rows(part.zone) {
            return None;
        }
        let check = self.checks.iter().find(|check| check.bit == bit)?;
        check.zones.recorded_count(part.zone, check.code)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const Z: u64 = 8_192;

    /// The zone map of a column of `rows` rows whose zones hold `bounds`.
    fn zones(bounds: &[(u64, u64)], rows: u64) -> Arc<ColumnZones> {
        Arc::new(
            ColumnZones::from_bounds(
                bounds.iter().map(|b| b.0).collect(),
                bounds.iter().map(|b| b.1).collect(),
                Z,
                rows,
            )
            .unwrap(),
        )
    }

    /// A check of `code` under bit `bit` over zones of these bounds, full.
    fn check(bit: u32, code: u64, bounds: &[(u64, u64)]) -> ZoneCheck {
        ZoneCheck {
            bit,
            code,
            zones: zones(bounds, Z * bounds.len() as u64),
        }
    }

    /// One check: all where its bounds are the code alone, none where they
    /// leave it out, mixed where they hold it among others, their ends
    /// included.
    #[test]
    fn one_check_classes() {
        let set = ZoneSet::new(
            vec![check(3, 5, &[(5, 5), (1, 4), (6, 9), (5, 6), (4, 5)])],
            0,
        )
        .unwrap();
        let mixed = ZoneClass::Mixed { unsettled: 1 << 3 };
        assert_eq!(
            (0..5).map(|zone| set.class(zone)).collect::<Vec<_>>(),
            [
                ZoneClass::All,
                ZoneClass::None,
                ZoneClass::None,
                mixed,
                mixed
            ]
        );
        assert_eq!((set.zone_len(), set.zones()), (Z, 5));
    }

    /// Two checks: none where either says none, all where both say all,
    /// and a mixed zone names the checks still unsettled in it.
    #[test]
    fn checks_combine() {
        let o = check(2, 7, &[(7, 7), (7, 7), (1, 9), (8, 9), (6, 8), (7, 7)]);
        let g = check(3, 5, &[(5, 5), (1, 4), (6, 9), (5, 6), (4, 6), (5, 6)]);
        let set = ZoneSet::new(vec![o, g], 0).unwrap();
        assert_eq!(
            (0..6).map(|zone| set.class(zone)).collect::<Vec<_>>(),
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

    /// Checks no zone map speaks for leave every zone they reach unsettled:
    /// an all zone is mixed on them, a none zone stays none, and a mixed
    /// zone adds them.
    #[test]
    fn unzoned_bits_leave_every_zone_mixed() {
        let set = ZoneSet::new(vec![check(0, 5, &[(5, 5), (1, 4), (4, 6)])], 1 << 5).unwrap();
        assert_eq!(
            (0..3).map(|zone| set.class(zone)).collect::<Vec<_>>(),
            [
                ZoneClass::Mixed { unsettled: 1 << 5 },
                ZoneClass::None,
                ZoneClass::Mixed {
                    unsettled: 1 | 1 << 5
                },
            ]
        );
    }

    /// Exact bounds settle a zone before its recorded values do: a zone
    /// whose bounds are the code alone is all though its record lacks the
    /// code (a record no store writes, contradicting the bounds); a zone
    /// whose bounds hold the code among others is none where its record
    /// lacks it.
    #[test]
    fn bounds_settle_before_records() {
        let recorded = ColumnZones::from_bounds(
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
        .unwrap();
        let set = ZoneSet::new(
            vec![ZoneCheck {
                bit: 3,
                code: 5,
                zones: Arc::new(recorded),
            }],
            0,
        )
        .unwrap();
        assert_eq!(
            [set.class(0), set.class(1)],
            [ZoneClass::All, ZoneClass::None]
        );
    }

    /// By the bounds alone, a zone is none where they rule the code out and
    /// mixed on every check otherwise: a zone of the code alone, and one
    /// whose record lacks the code, are both compared.
    #[test]
    fn bounds_only_skips_by_bounds_alone() {
        let recorded = ColumnZones::from_bounds(
            vortex_buffer::buffer![5u64, 1, 4],
            vortex_buffer::buffer![5u64, 4, 6],
            Z,
            3 * Z,
        )
        .unwrap()
        .with_records(vec![
            Some(vec![(5, Z as u32)].into()),
            Some(vec![(1, Z as u32)].into()),
            Some(vec![(4, 8_000), (6, 192)].into()),
        ])
        .unwrap();
        let set = ZoneSet::new(
            vec![ZoneCheck {
                bit: 1,
                code: 5,
                zones: Arc::new(recorded),
            }],
            1 << 4,
        )
        .unwrap()
        .bounds_only();
        let every = ZoneClass::Mixed {
            unsettled: 1 << 1 | 1 << 4,
        };
        assert_eq!(
            (0..3).map(|zone| set.class(zone)).collect::<Vec<_>>(),
            [every, ZoneClass::None, every]
        );
    }

    /// No set without checks, over maps of other zone lengths, zone counts
    /// or row counts, or with a bit past `u32`'s.
    #[test]
    fn geometry_is_checked_once() {
        let five = |rows: u64| ZoneCheck {
            bit: 0,
            code: 5,
            zones: zones(&[(5, 5), (5, 5), (5, 5)], rows),
        };
        assert!(ZoneSet::new(Vec::new(), 0).is_none());
        let half = ZoneCheck {
            bit: 1,
            code: 5,
            zones: Arc::new(
                ColumnZones::from_bounds(
                    vortex_buffer::buffer![5u64; 6],
                    vortex_buffer::buffer![5u64; 6],
                    Z / 2,
                    3 * Z,
                )
                .unwrap(),
            ),
        };
        assert!(
            ZoneSet::new(vec![five(3 * Z), half], 0).is_none(),
            "zone length"
        );
        let two = check(1, 5, &[(5, 5), (5, 5)]);
        assert!(ZoneSet::new(vec![five(3 * Z), two], 0).is_none(), "zones");
        assert!(
            ZoneSet::new(
                vec![
                    five(3 * Z),
                    ZoneCheck {
                        bit: 1,
                        ..five(3 * Z - 7)
                    }
                ],
                0
            )
            .is_none(),
            "rows"
        );
        assert!(
            ZoneSet::new(
                vec![ZoneCheck {
                    bit: 32,
                    ..five(3 * Z)
                }],
                0
            )
            .is_none()
        );
        assert!(ZoneSet::new(vec![five(3 * Z)], 1 << 31).is_some());
    }

    /// A range of rows cut at the zones it touches, each part clipped to the
    /// range and to the table's rows; an empty range touches none.
    #[test]
    fn parts_clip_rows_to_zones() {
        let set = ZoneSet::new(
            vec![ZoneCheck {
                bit: 0,
                code: 5,
                zones: zones(&[(5, 5), (5, 5), (5, 5)], 20_000),
            }],
            0,
        )
        .unwrap();
        let parts = |rows: std::ops::Range<u64>| {
            set.parts(rows)
                .map(|part| (part.zone, part.rows))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            parts(100..20_000),
            [(0, 100..8_192), (1, 8_192..16_384), (2, 16_384..20_000)]
        );
        assert_eq!(parts(9_000..9_001), [(1, 9_000..9_001)]);
        assert!(parts(5..5).is_empty());
        assert_eq!(set.zone_rows(2), 16_384..20_000);
    }

    /// A check's recorded rows of its code count a part only when the part
    /// is its whole zone, the last short zone included.
    #[test]
    fn records_count_whole_zones_only() {
        let recorded = ColumnZones::from_bounds(
            vortex_buffer::buffer![5u64, 5, 6],
            vortex_buffer::buffer![5u64, 6, 6],
            Z,
            2 * Z + 3_616,
        )
        .unwrap()
        .with_records(vec![
            Some(vec![(5, 8_192)].into()),
            Some(vec![(5, 100), (6, 8_092)].into()),
            Some(vec![(6, 3_616)].into()),
        ])
        .unwrap();
        let set = ZoneSet::new(
            vec![ZoneCheck {
                bit: 2,
                code: 5,
                zones: Arc::new(recorded),
            }],
            0,
        )
        .unwrap();
        let part = |zone: u64, rows: std::ops::Range<u64>| ZonePart { zone, rows };
        assert_eq!(set.recorded_in(2, &part(0, 0..Z)), Some(8_192));
        assert_eq!(set.recorded_in(2, &part(0, 5..Z)), None, "part of a zone");
        assert_eq!(set.recorded_in(2, &part(1, Z..2 * Z)), Some(100));
        assert_eq!(set.recorded_in(2, &part(2, 2 * Z..2 * Z + 3_616)), Some(0));
        assert_eq!(set.recorded_in(1, &part(0, 0..Z)), None, "no such check");
    }
}
