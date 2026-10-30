//! Kernels over [`TermCode`] columns: distinct values and value counts, the
//! starts of runs of equal rows, an index gather, and an equi-join (its pair
//! count, its row pairs, refused past a cap before either output is
//! allocated, and its build side [`CodeGroups`], which joins a left column
//! slice by slice and can gather the joined columns). All work on plain
//! slices and preserve order: distinct values come out in first-seen order,
//! join pairs in nested-loop order (left rows in order, each with its right
//! matches in their order). Row indices
//! and counts are `u64`, the codes' width, so one column type carries all
//! three. Each kernel uses a direct-mapped table when the code range is dense
//! relative to the column (`dense_table_fits`), else a hash map under the fast
//! hasher of the code-keyed maps (the join's too).
//!
//! The kernels over one column, and the table rule they share with the join,
//! are here; the equi-join and its build side are in `join.rs`.

mod join;

pub use join::{CodeGroups, equi_join_indices, join_count};

use rustc_hash::{FxHashMap, FxHashSet};
use vortex_buffer::Buffer;

use crate::store::schema::TermCode;

/// Whether a direct-mapped table over the codes `[lo, lo + span]` (`span + 1`
/// slots) is used for a column of `len` values: while it stays within a small
/// multiple of the column's length. Compared as codes, so a `true` answer
/// guarantees `span` fits a `usize`; a wider span (on a 32-bit target) takes
/// the hashed path, never a wrapped table index.
fn dense_table_fits(span: TermCode, len: usize) -> bool {
    let cutoff = len.saturating_mul(4).max(1 << 16);
    span < TermCode::try_from(cutoff).unwrap_or(TermCode::MAX)
}

/// The largest code in `codes`, or `None` for an empty column.
fn max_code(codes: &[TermCode]) -> Option<TermCode> {
    codes.iter().copied().max()
}

/// The distinct codes of a column, each at its first occurrence, in that
/// order.
pub fn distinct_first_seen(codes: &[TermCode]) -> Buffer<TermCode> {
    let Some(max) = max_code(codes) else {
        return Buffer::empty();
    };
    let mut out = Vec::new();
    if dense_table_fits(max, codes.len()) {
        // Every code is at most `max`, which fits a `usize` here.
        let mut seen = vec![false; max as usize + 1];
        for &c in codes {
            let slot = &mut seen[c as usize];
            if !*slot {
                *slot = true;
                out.push(c);
            }
        }
    } else {
        let mut seen =
            FxHashSet::with_capacity_and_hasher(codes.len().min(1 << 20), Default::default());
        for &c in codes {
            if seen.insert(c) {
                out.push(c);
            }
        }
    }
    Buffer::from(out)
}

/// The distinct codes of a column in first-seen order, paired with how many
/// times each occurs: `(codes, counts)` of equal length.
pub fn value_counts(codes: &[TermCode]) -> (Buffer<TermCode>, Buffer<u64>) {
    let Some(max) = max_code(codes) else {
        return (Buffer::empty(), Buffer::empty());
    };
    let mut order = Vec::new();
    let mut counts: Vec<u64> = Vec::new();
    if dense_table_fits(max, codes.len()) {
        // `slot[c]` is the 1-based position of `c` in `order`, 0 for unseen.
        // Every code is at most `max`, which fits a `usize` here.
        let mut slot = vec![0usize; max as usize + 1];
        for &c in codes {
            let s = &mut slot[c as usize];
            if *s == 0 {
                order.push(c);
                counts.push(1);
                *s = order.len();
            } else {
                counts[*s - 1] += 1;
            }
        }
    } else {
        let mut slot: FxHashMap<TermCode, usize> =
            FxHashMap::with_capacity_and_hasher(codes.len().min(1 << 20), Default::default());
        for &c in codes {
            match slot.get(&c) {
                Some(&i) => counts[i] += 1,
                None => {
                    slot.insert(c, order.len());
                    order.push(c);
                    counts.push(1);
                }
            }
        }
    }
    (Buffer::from(order), Buffer::from(counts))
}

/// Gather `codes` at `indices`: `out[i] = codes[indices[i]]`. An index past
/// the column is an error carrying its position in `indices` (the pair-index
/// columns of [`equi_join_indices`] never produce one).
pub fn take(codes: &[TermCode], indices: &[u64]) -> Result<Buffer<TermCode>, usize> {
    let len = codes.len();
    // Checked, never cast: an index past what a `usize` holds (on a 32-bit
    // target) is out of range rather than wrapped onto another row.
    let in_range = |i: u64| usize::try_from(i).ok().filter(|&i| i < len);
    if let Some(bad) = indices.iter().position(|&i| in_range(i).is_none()) {
        return Err(bad);
    }
    Ok(Buffer::from_iter(
        indices.iter().map(|&i| codes[i as usize]),
    ))
}

/// The rows of equally long columns where a run of equal rows starts: row 0,
/// and every row whose code in some column differs from the row before.
/// Gathered with [`take`], they keep one row of each run, which is the
/// distinct rows when equal rows are adjacent — as the rows of one match are
/// on the columns its sort order puts before the graph (every order a store
/// holds ends in `g`), so the triples of a union over graphs. `None` when the
/// columns differ in length; no column, no rows.
pub fn run_starts(columns: &[&[TermCode]]) -> Option<Buffer<u64>> {
    let Some(first) = columns.first() else {
        return Some(Buffer::empty());
    };
    let len = first.len();
    if columns.iter().any(|c| c.len() != len) {
        return None;
    }
    Some(Buffer::from_iter((0..len).filter_map(|i| {
        (i == 0 || columns.iter().any(|c| c[i] != c[i - 1])).then_some(i as u64)
    })))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Past `u32::MAX`, so a kernel narrowing a code to 32 bits would read
    /// it as 7. The join's tests use it too.
    pub(super) const WIDE: TermCode = (1 << 32) + 7;

    #[test]
    fn run_starts_keeps_one_row_per_run() {
        let s = [1, 1, 1, 2, 2, WIDE];
        let p = [5, 5, 6, 6, 6, 6];
        let starts = run_starts(&[&s, &p]).unwrap();
        assert_eq!(starts.as_slice(), &[0, 2, 3, 5]);
        assert_eq!(run_starts(&[&s]).unwrap().as_slice(), &[0, 3, 5]);
        assert!(run_starts(&[&s, &p[..2]]).is_none());
        assert!(run_starts(&[]).unwrap().is_empty());
        assert!(run_starts(&[&[]]).unwrap().is_empty());
    }

    #[test]
    fn distinct_keeps_first_seen_order() {
        assert_eq!(
            distinct_first_seen(&[5, 3, 5, 9, 3, 1]).as_slice(),
            &[5, 3, 9, 1]
        );
        assert!(distinct_first_seen(&[]).is_empty());
        // Sparse strategy: a huge code forces the hash path.
        assert_eq!(
            distinct_first_seen(&[TermCode::MAX, 0, TermCode::MAX, 7]).as_slice(),
            &[TermCode::MAX, 0, 7]
        );
        // A wide code stays distinct from the narrow code it would truncate to.
        assert_eq!(distinct_first_seen(&[WIDE, 7, WIDE]).as_slice(), &[WIDE, 7]);
    }

    #[test]
    fn value_counts_align_with_distinct() {
        let col = [5, 3, 5, 9, 3, 5];
        let (codes, counts) = value_counts(&col);
        assert_eq!(codes.as_slice(), &[5, 3, 9]);
        assert_eq!(counts.as_slice(), &[3, 2, 1]);
        let sparse = [
            TermCode::MAX - 1,
            2,
            2,
            TermCode::MAX - 1,
            TermCode::MAX - 1,
        ];
        let (codes, counts) = value_counts(&sparse);
        assert_eq!(codes.as_slice(), &[TermCode::MAX - 1, 2]);
        assert_eq!(counts.as_slice(), &[3, 2]);
        let (codes, counts) = value_counts(&[7, WIDE, WIDE]);
        assert_eq!(codes.as_slice(), &[7, WIDE]);
        assert_eq!(counts.as_slice(), &[1, 2]);
        assert!(value_counts(&[]).0.is_empty());
    }

    #[test]
    fn take_gathers_and_checks_bounds() {
        assert_eq!(
            take(&[10, 20, 30], &[2, 0, 2]).unwrap().as_slice(),
            &[30, 10, 30]
        );
        assert_eq!(take(&[10, 20, 30], &[1, 3]), Err(1));
        assert!(take(&[], &[]).unwrap().is_empty());
        assert_eq!(take(&[WIDE, 1], &[0, 0]).unwrap().as_slice(), &[WIDE, WIDE]);
        // An index past `u32::MAX` that would truncate to a valid row is out
        // of range, not row 1.
        assert_eq!(take(&[10, 20], &[0, (1 << 32) + 1]), Err(1));
        assert_eq!(take(&[10, 20], &[u64::MAX]), Err(0));
    }

    #[test]
    fn join_indices_feed_take() {
        let left_key = [1, 2, 2, 3];
        let left_val = [10, 20, 21, 30];
        let right_key = [2, 3, 2];
        let right_val = [200, 300, 201];
        let (li, ri) = equi_join_indices(&left_key, &right_key, None).unwrap();
        let lv = take(&left_val, li.as_slice()).unwrap();
        let rv = take(&right_val, ri.as_slice()).unwrap();
        assert_eq!(lv.as_slice(), &[20, 20, 21, 21, 30]);
        assert_eq!(rv.as_slice(), &[200, 201, 200, 201, 300]);
    }
}
