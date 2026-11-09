//! The distinct codes of a run of codes, found cheaply ([`distinct_codes`]):
//! what a binding decodes once per distinct code and spells per position.

use crate::store::schema::TermCode;

/// Buckets in each column's cache of recent codes: a power of two, so a
/// code's bucket is its low bits; 256 entries keep a column's table at 4 KiB,
/// small enough to stay cache-resident and to clear cheaply for a short run.
const BUCKETS: usize = 256;

/// The slot an empty bucket holds: code 0 is a code like any other, so
/// emptiness is carried by the slot rather than the code.
const NO_SLOT: usize = usize::MAX;

/// The distinct codes of `codes`, in the order first met, beside each
/// position's slot among them: what a binding decodes once per distinct code
/// (as [`DictReader::decode_many_with`](super::DictReader::decode_many_with)
/// does) and spells per position. They are found through a direct-mapped
/// cache of each column's recent codes, `W` columns interleaved (the code at
/// position `p` is column `p % W`'s), 256 buckets a column, a code's bucket
/// its low bits: a hit shares its slot, a miss (a new code, or one its bucket
/// lost) makes a new one. So a code repeated far apart may take two slots,
/// but every slot holds the code its positions hold, slots are made in
/// position order, and a prefix of the codes gets the same slots however
/// many follow it. A slot is an index into the distinct codes.
pub fn distinct_codes<const W: usize>(
    codes: impl IntoIterator<Item = TermCode>,
) -> (Vec<TermCode>, Vec<usize>) {
    let codes = codes.into_iter();
    let mut recent = [[(0 as TermCode, NO_SLOT); BUCKETS]; W];
    let mut distinct = Vec::new();
    let mut slots = Vec::with_capacity(codes.size_hint().0);
    for (position, code) in codes.enumerate() {
        // The low bits pick the bucket: masked before the narrowing, so they
        // are the code's own.
        let entry = &mut recent[position % W][(code & (BUCKETS as TermCode - 1)) as usize];
        if entry.1 == NO_SLOT || entry.0 != code {
            // Overwrites whatever shared the bucket: a hit is gated on the
            // stored code, so a lost entry only costs a second slot.
            *entry = (code, distinct.len());
            distinct.push(code);
        }
        slots.push(entry.1);
    }
    (distinct, slots)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repeated_codes_share_a_slot_and_every_slot_holds_its_code() {
        let codes = [7, 0, 7, 263, 0, 7, 263];
        let (distinct, slots) = distinct_codes::<1>(codes);
        // 263 shares 7's bucket: each evicts the other, so their repeats
        // after that take new slots.
        assert_eq!(distinct, [7, 0, 263, 7, 263]);
        assert_eq!(slots, [0, 1, 0, 2, 1, 3, 4]);
        for (position, &slot) in slots.iter().enumerate() {
            assert_eq!(distinct[slot], codes[position]);
        }
    }

    #[test]
    fn interleaved_columns_keep_their_own_caches() {
        // Two columns, rows (5, 6) (6, 5) (5, 6): each column caches only its
        // own codes, so a code first met in the other column takes a slot of
        // its own.
        let (distinct, slots) = distinct_codes::<2>([5, 6, 6, 5, 5, 6]);
        assert_eq!(distinct, [5, 6, 6, 5]);
        assert_eq!(slots, [0, 1, 2, 3, 0, 1]);
    }

    #[test]
    fn a_prefix_gets_the_same_slots() {
        let codes: Vec<TermCode> = (0..2_000).map(|i| (i * 37) % 300).collect();
        let (_, whole) = distinct_codes::<4>(codes.iter().copied());
        let (_, prefix) = distinct_codes::<4>(codes[..1_000].iter().copied());
        assert_eq!(whole[..1_000], prefix[..]);
    }
}
