//! The pieces a fold that may stop at its first matches cuts its rows into:
//! one zone's rows first ([`FIRST_PIECE_ROWS`]), then each piece
//! [`PIECE_GROWTH`] times the one before. Matches in the first rows end the
//! fold after a piece of about their size, and a fold that runs to the end
//! takes a handful of pieces. A file filter's early-exit fold evaluates its
//! splits in them (`file_filter`), and a residual compare left pending
//! compares its selection in them (`view::pending`).

use std::ops::Range;

use crate::io::zone_map::ZONE_ROWS;

/// The rows of the first piece: a zone's.
pub(crate) const FIRST_PIECE_ROWS: usize = ZONE_ROWS;

/// How many times the rows of the piece before each later piece holds.
const PIECE_GROWTH: u64 = 4;

/// `ranges` (ascending, disjoint) cut into pieces, the first of `first`
/// rows and each later one [`PIECE_GROWTH`] times the one before, the growth
/// carried from range to range and no piece crossing a range's end: so the
/// pieces soon reach whole ranges. Each piece comes beside the index of the
/// range it was cut from, in order.
pub(crate) fn growing_pieces(
    ranges: impl IntoIterator<Item = Range<u64>>,
    first: u64,
) -> impl Iterator<Item = (usize, Range<u64>)> {
    let mut ranges = ranges.into_iter().enumerate();
    let mut current: Option<(usize, Range<u64>)> = None;
    // A piece of no rows would never end its range.
    let mut size = first.max(1);
    std::iter::from_fn(move || {
        loop {
            if let Some((index, rest)) = &mut current
                && rest.start < rest.end
            {
                let end = rest.start.saturating_add(size).min(rest.end);
                let piece = rest.start..end;
                rest.start = end;
                size = size.saturating_mul(PIECE_GROWTH);
                return Some((*index, piece));
            }
            current = Some(ranges.next()?);
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The pieces start at the first piece's rows and grow fourfold, none
    /// crossing a range's end, the growth carried from range to range, so
    /// ranges past it stay whole, each piece beside the index of its range;
    /// an empty range gives none.
    #[test]
    #[allow(
        clippy::single_range_in_vec_init,
        reason = "the pieces are cut from a list of ranges, and some cases hold just one"
    )]
    fn pieces_grow_fourfold_from_the_first() {
        let z = FIRST_PIECE_ROWS as u64;
        let pieces =
            |ranges: &[Range<u64>]| growing_pieces(ranges.iter().cloned(), z).collect::<Vec<_>>();
        assert_eq!(pieces(&[]), []);
        assert_eq!(pieces(&[0..100]), [(0, 0..100)]);
        assert_eq!(
            pieces(&[0..262_144, 262_144..524_288]),
            [
                (0, 0..z),
                (0, z..5 * z),
                (0, 5 * z..21 * z),
                (0, 21 * z..262_144),
                (1, 262_144..524_288)
            ]
        );
        // A selection's clamped ranges, the first shorter than a zone, the
        // second empty.
        assert_eq!(
            pieces(&[10..20, 50..50, 100..100_000, 200_000..300_000]),
            [
                (0, 10..20),
                (2, 100..100 + 4 * z),
                (2, 100 + 4 * z..100_000),
                (3, 200_000..300_000)
            ]
        );
        // A first piece of every row is the one piece.
        assert_eq!(
            growing_pieces([0..100], 100).collect::<Vec<_>>(),
            [(0, 0..100)]
        );
    }
}
