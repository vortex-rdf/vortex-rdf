//! Kernels over [`TermCode`] columns — the column-at-a-time operations a
//! query layer runs between matches: distinct values and value counts (for
//! `DISTINCT`, `GROUP BY`, `COUNT DISTINCT`), an index gather, and the
//! matching row pairs of an equi-join. They work on plain slices so every
//! binding can hand them its zero-copy columns, and they are deliberately
//! order-preserving, because the orders are observable: distinct values come
//! out in first-seen order, and a join's pairs come out in nested-loop order
//! (left rows in order, each with its right matches in their original order).
//! Row indices and counts are `u64`, the width of the codes, so one column
//! type carries all three.
//!
//! Term codes are dictionary ranks, so a column's values are dense in a
//! known range; when that range is small enough relative to the column, a
//! direct-mapped table beats hashing. Each kernel picks between the two by
//! [`dense_table_fits`].

use std::collections::HashMap;

use vortex_buffer::Buffer;

use crate::store::schema::TermCode;

/// Whether a direct-mapped table over `[0, max_code]` is the better strategy
/// for a column of `len` values: the table costs `max_code + 1` slots, so it
/// wins while it stays within a small multiple of the column's length.
/// Compared as codes, so a code past what a `usize` holds (on a 32-bit
/// target) takes the hashed path rather than a wrapped table index: a `true`
/// answer guarantees `max_code` fits a `usize`.
fn dense_table_fits(max_code: TermCode, len: usize) -> bool {
    let cutoff = len.saturating_mul(4).max(1 << 16);
    max_code < TermCode::try_from(cutoff).unwrap_or(TermCode::MAX)
}

/// The largest code in `codes`, or `None` for an empty column.
fn max_code(codes: &[TermCode]) -> Option<TermCode> {
    codes.iter().copied().max()
}

/// A row index as the `u64` the index columns carry. Lossless: a `usize` is
/// at most 64 bits on every target.
#[inline]
fn row(i: usize) -> u64 {
    i as u64
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
        let mut seen = HashMap::with_capacity(codes.len().min(1 << 20));
        for &c in codes {
            if seen.insert(c, ()).is_none() {
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
        let mut slot: HashMap<TermCode, usize> = HashMap::with_capacity(codes.len().min(1 << 20));
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

/// The matching row pairs of two key columns, as parallel index columns
/// `(left_idx, right_idx)`: for every left row in order, each right row with
/// the same code, in the right column's order — the order a nested loop
/// produces, which is what makes the result stable across strategies.
///
/// The right column is indexed once (a CSR layout: every right row grouped
/// under its code), then the left column is probed in order. When both
/// columns are already ascending the pairs come from one merge pass instead.
pub fn equi_join_indices(left: &[TermCode], right: &[TermCode]) -> (Buffer<u64>, Buffer<u64>) {
    if left.is_empty() || right.is_empty() {
        return (Buffer::empty(), Buffer::empty());
    }
    if is_ascending(left) && is_ascending(right) {
        return merge_join(left, right);
    }
    let index = RightIndex::build(right);
    let mut out_l = Vec::new();
    let mut out_r = Vec::new();
    for (i, &code) in left.iter().enumerate() {
        let rows = index.rows_for(code);
        if rows.is_empty() {
            continue;
        }
        out_l.extend(std::iter::repeat_n(row(i), rows.len()));
        out_r.extend_from_slice(rows);
    }
    (Buffer::from(out_l), Buffer::from(out_r))
}

fn is_ascending(codes: &[TermCode]) -> bool {
    codes.windows(2).all(|w| w[0] <= w[1])
}

/// The join of two ascending key columns: walk both, and for each run of
/// equal codes emit the cross product in nested-loop order.
fn merge_join(left: &[TermCode], right: &[TermCode]) -> (Buffer<u64>, Buffer<u64>) {
    let mut out_l = Vec::new();
    let mut out_r = Vec::new();
    let (mut i, mut j) = (0usize, 0usize);
    while i < left.len() && j < right.len() {
        let (a, b) = (left[i], right[j]);
        if a < b {
            i += 1;
        } else if a > b {
            j += 1;
        } else {
            let run_l = i + left[i..].iter().take_while(|&&c| c == a).count();
            let run_r = j + right[j..].iter().take_while(|&&c| c == a).count();
            for li in i..run_l {
                out_l.extend(std::iter::repeat_n(row(li), run_r - j));
                out_r.extend((j..run_r).map(row));
            }
            i = run_l;
            j = run_r;
        }
    }
    (Buffer::from(out_l), Buffer::from(out_r))
}

/// The right column grouped by code: `rows[starts[c]..starts[c+1]]` are the
/// right row indices holding code `c`, ascending — a CSR layout built with
/// two counting passes when the code range is dense, or a hash map of
/// per-code vectors otherwise.
enum RightIndex {
    Dense { starts: Vec<usize>, rows: Vec<u64> },
    Sparse(HashMap<TermCode, Vec<u64>>),
}

impl RightIndex {
    fn build(right: &[TermCode]) -> Self {
        let max = max_code(right).unwrap_or(0);
        if dense_table_fits(max, right.len()) {
            // Every code is at most `max`, which fits a `usize` here.
            let slots = max as usize + 1;
            let mut starts = vec![0usize; slots + 1];
            for &c in right {
                starts[c as usize + 1] += 1;
            }
            for i in 0..slots {
                starts[i + 1] += starts[i];
            }
            let mut fill = starts.clone();
            let mut rows = vec![0u64; right.len()];
            for (r, &c) in right.iter().enumerate() {
                let at = &mut fill[c as usize];
                rows[*at] = row(r);
                *at += 1;
            }
            RightIndex::Dense { starts, rows }
        } else {
            let mut map: HashMap<TermCode, Vec<u64>> = HashMap::new();
            for (r, &c) in right.iter().enumerate() {
                map.entry(c).or_default().push(row(r));
            }
            RightIndex::Sparse(map)
        }
    }

    fn rows_for(&self, code: TermCode) -> &[u64] {
        match self {
            RightIndex::Dense { starts, rows } => {
                // A code past the table — or past what a `usize` holds — has
                // no rows. `starts` has one entry per code plus one.
                let Some(c) = usize::try_from(code).ok().filter(|&c| c < starts.len() - 1) else {
                    return &[];
                };
                &rows[starts[c]..starts[c + 1]]
            }
            RightIndex::Sparse(map) => map.get(&code).map_or(&[], Vec::as_slice),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Past `u32::MAX`, so a kernel narrowing a code to 32 bits would read
    /// it as 7.
    const WIDE: TermCode = (1 << 32) + 7;

    /// The nested-loop oracle every join strategy must reproduce.
    fn nested_loop(left: &[TermCode], right: &[TermCode]) -> (Vec<u64>, Vec<u64>) {
        let mut l = Vec::new();
        let mut r = Vec::new();
        for (i, a) in left.iter().enumerate() {
            for (j, b) in right.iter().enumerate() {
                if a == b {
                    l.push(i as u64);
                    r.push(j as u64);
                }
            }
        }
        (l, r)
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
    fn join_matches_the_nested_loop_in_every_strategy() {
        let cases: Vec<(Vec<TermCode>, Vec<TermCode>)> = vec![
            (vec![1, 2, 2, 3, 7], vec![2, 2, 7, 1, 9]), // hashed, dense
            (vec![1, 2, 2, 3, 7], vec![1, 2, 2, 2, 7, 7]), // merge path (both ascending)
            (
                vec![3, 1, TermCode::MAX],
                vec![TermCode::MAX, 1, TermCode::MAX],
            ), // sparse right
            (vec![7, WIDE, 7], vec![WIDE, 7, WIDE]),    // wide codes, hashed
            (vec![7, WIDE], vec![7, 7, WIDE]),          // wide codes, merged
            (vec![], vec![1]),
            (vec![1, 1, 1], vec![1, 1]),
            (vec![4, 5], vec![6]),
        ];
        for (l, r) in cases {
            let (li, ri) = equi_join_indices(&l, &r);
            let (ol, or) = nested_loop(&l, &r);
            assert_eq!(
                li.as_slice(),
                ol.as_slice(),
                "left indices for {l:?} ⋈ {r:?}"
            );
            assert_eq!(
                ri.as_slice(),
                or.as_slice(),
                "right indices for {l:?} ⋈ {r:?}"
            );
        }
    }

    /// A dense right index probed with a code past its table, or past what
    /// a `usize` holds, finds no rows.
    #[test]
    fn dense_index_misses_codes_past_its_table() {
        let index = RightIndex::build(&[0, 1, 2]);
        assert!(matches!(index, RightIndex::Dense { .. }));
        assert_eq!(index.rows_for(1), &[1]);
        assert!(index.rows_for(3).is_empty());
        assert!(index.rows_for(WIDE).is_empty());
        assert!(index.rows_for(TermCode::MAX).is_empty());
        assert!(dense_table_fits(65_535, 1));
        assert!(!dense_table_fits(WIDE, 1));
    }

    #[test]
    fn join_indices_feed_take() {
        let left_key = [1, 2, 2, 3];
        let left_val = [10, 20, 21, 30];
        let right_key = [2, 3, 2];
        let right_val = [200, 300, 201];
        let (li, ri) = equi_join_indices(&left_key, &right_key);
        let lv = take(&left_val, li.as_slice()).unwrap();
        let rv = take(&right_val, ri.as_slice()).unwrap();
        assert_eq!(lv.as_slice(), &[20, 20, 21, 21, 30]);
        assert_eq!(rv.as_slice(), &[200, 201, 200, 201, 300]);
    }
}
