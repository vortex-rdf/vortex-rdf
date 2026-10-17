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
//! `dense_table_fits`.

use std::collections::HashMap;
use std::ops::Range;

use rustc_hash::FxHashMap;
use vortex_buffer::Buffer;

use crate::store::schema::TermCode;

/// Whether a direct-mapped table over the codes `[lo, lo + span]` is the
/// better strategy for a column of `len` values: the table costs `span + 1`
/// slots, so it wins while it stays within a small multiple of the column's
/// length. Compared as codes, so a span past what a `usize` holds (on a
/// 32-bit target) takes the hashed path rather than a wrapped table index: a
/// `true` answer guarantees `span` fits a `usize`.
fn dense_table_fits(span: TermCode, len: usize) -> bool {
    let cutoff = len.saturating_mul(4).max(1 << 16);
    span < TermCode::try_from(cutoff).unwrap_or(TermCode::MAX)
}

/// The largest code in `codes`, or `None` for an empty column.
fn max_code(codes: &[TermCode]) -> Option<TermCode> {
    codes.iter().copied().max()
}

/// The smallest and the largest code in `codes`, or `None` for an empty
/// column.
fn code_range(codes: &[TermCode]) -> Option<(TermCode, TermCode)> {
    let (&first, rest) = codes.split_first()?;
    Some(
        rest.iter()
            .fold((first, first), |(lo, hi), &c| (lo.min(c), hi.max(c))),
    )
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
/// Either way a counting pass finds the number of pairs first, and each
/// output is allocated once at that length.
pub fn equi_join_indices(left: &[TermCode], right: &[TermCode]) -> (Buffer<u64>, Buffer<u64>) {
    let (out_l, out_r) = join_pairs(left, right);
    (Buffer::from(out_l), Buffer::from(out_r))
}

/// The pair columns of [`equi_join_indices`], each allocated once at its
/// final length.
fn join_pairs(left: &[TermCode], right: &[TermCode]) -> (Vec<u64>, Vec<u64>) {
    if left.is_empty() || right.is_empty() {
        return (Vec::new(), Vec::new());
    }
    if is_ascending(left) && is_ascending(right) {
        merge_join(left, right)
    } else {
        hash_join(left, right)
    }
}

fn is_ascending(codes: &[TermCode]) -> bool {
    codes.windows(2).all(|w| w[0] <= w[1])
}

/// The join of a left column against a [`RightIndex`] of the right one: for
/// each left row in order, its right matches. The index is a table over the
/// right column's code [`Span`] when [`dense_table_fits`] it, else a
/// [`Hashed`] map of its codes. The kind is picked once, here, and
/// [`RightIndex::join`] is compiled for each, so its lookup is inlined into
/// both of the join's passes.
fn hash_join(left: &[TermCode], right: &[TermCode]) -> (Vec<u64>, Vec<u64>) {
    match Span::of(right) {
        Some(span) => RightIndex::dense(right, span).join(left),
        None => RightIndex::hashed(right).join(left),
    }
}

/// Calls `visit(left_run, right_run)`, in order, for each pair of equal-code
/// runs of two ascending columns.
fn for_each_run_pair(
    left: &[TermCode],
    right: &[TermCode],
    mut visit: impl FnMut(Range<usize>, Range<usize>),
) {
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
            visit(i..run_l, j..run_r);
            i = run_l;
            j = run_r;
        }
    }
}

/// The number of pairs [`merge_join`] produces: the product of the two run
/// lengths, summed over the equal runs.
fn merge_join_len(left: &[TermCode], right: &[TermCode]) -> usize {
    let mut total = 0;
    for_each_run_pair(left, right, |run_l, run_r| {
        total += run_l.len() * run_r.len()
    });
    total
}

/// The join of two ascending key columns: walk both, and for each run of
/// equal codes emit the cross product in nested-loop order. A counting walk
/// sizes both outputs before they are filled.
fn merge_join(left: &[TermCode], right: &[TermCode]) -> (Vec<u64>, Vec<u64>) {
    let total = merge_join_len(left, right);
    let mut out_l = Vec::with_capacity(total);
    let mut out_r = Vec::with_capacity(total);
    for_each_run_pair(left, right, |run_l, run_r| {
        for li in run_l {
            out_l.extend(std::iter::repeat_n(row(li), run_r.len()));
            out_r.extend(run_r.clone().map(row));
        }
    });
    (out_l, out_r)
}

/// The right column grouped by code: [`Groups`] holds each code's right rows
/// in a slot of their own, and `slots` finds a code's slot — a table over
/// the column's code [`Span`], or a [`Hashed`] map of its codes.
struct RightIndex<S> {
    slots: S,
    groups: Groups,
}

/// How a [`RightIndex`] finds a code's group: the slot holding the right
/// rows with that code, or the empty slot for a code the right column lacks.
trait Slots {
    fn slot(&self, code: TermCode) -> usize;
}

/// A slot per code of a right column's span `[min, min + width)`: code
/// `min + k` is in slot `k`. Every other code is clamped to the empty slot
/// `width` (a code below `min` wraps past the span), so a miss takes no
/// branch. The table costs the span, not the largest code: one code, however
/// large, gets one slot.
struct Span {
    min: TermCode,
    width: usize,
}

impl Span {
    /// The span of `codes`, when a table over it fits
    /// ([`dense_table_fits`]); `None` for a span too wide for one, or an
    /// empty column.
    fn of(codes: &[TermCode]) -> Option<Span> {
        let (min, max) = code_range(codes)?;
        // `max - min` fits a `usize` when the table fits.
        dense_table_fits(max - min, codes.len()).then(|| Span {
            min,
            width: (max - min) as usize + 1,
        })
    }
}

impl Slots for Span {
    #[inline]
    fn slot(&self, code: TermCode) -> usize {
        // At most `width`, which is a `usize`.
        code.wrapping_sub(self.min).min(self.width as TermCode) as usize
    }
}

/// A slot per distinct code of a right column, numbered in first-seen order
/// and found through an FxHash map; every other code takes the empty slot,
/// numbered after them.
struct Hashed(FxHashMap<TermCode, usize>);

impl Slots for Hashed {
    #[inline]
    fn slot(&self, code: TermCode) -> usize {
        self.0.get(&code).copied().unwrap_or(self.0.len())
    }
}

/// Right rows grouped by slot, CSR style: the rows in slot `k` are
/// `rows[starts[k]..starts[k + 1]]`, ascending. The last slot is the empty
/// one, where a code the right column lacks is looked up.
struct Groups {
    starts: Vec<usize>,
    rows: Vec<u64>,
}

impl Groups {
    /// Groups a column's rows by `row_slots`, the slot of each row in order
    /// (each below `slots`), with two counting passes; slot `slots` is left
    /// empty.
    fn build(slots: usize, row_slots: impl Iterator<Item = usize> + Clone) -> Self {
        let mut starts = vec![0usize; slots + 2];
        for k in row_slots.clone() {
            starts[k + 1] += 1;
        }
        for k in 0..slots {
            starts[k + 1] += starts[k];
        }
        starts[slots + 1] = starts[slots];
        let mut fill = starts[..slots].to_vec();
        let mut rows = vec![0u64; starts[slots]];
        for (r, k) in row_slots.enumerate() {
            let at = &mut fill[k];
            rows[*at] = row(r);
            *at += 1;
        }
        Groups { starts, rows }
    }

    /// The number of rows in slot `k`.
    #[inline]
    fn count(&self, k: usize) -> usize {
        self.starts[k + 1] - self.starts[k]
    }

    /// The rows in slot `k`, ascending.
    #[inline]
    fn rows(&self, k: usize) -> &[u64] {
        &self.rows[self.starts[k]..self.starts[k + 1]]
    }
}

impl RightIndex<Span> {
    /// A table over `right`'s codes, which `span` covers.
    fn dense(right: &[TermCode], span: Span) -> Self {
        // A code's offset from `min` is below `width`, which is a `usize`.
        let groups = Groups::build(span.width, right.iter().map(|&c| (c - span.min) as usize));
        RightIndex {
            slots: span,
            groups,
        }
    }
}

impl RightIndex<Hashed> {
    /// A map over `right`'s distinct codes.
    fn hashed(right: &[TermCode]) -> Self {
        let mut map = FxHashMap::default();
        let row_slots: Vec<usize> = right
            .iter()
            .map(|&code| {
                let next = map.len();
                *map.entry(code).or_insert(next)
            })
            .collect();
        let groups = Groups::build(map.len(), row_slots.iter().copied());
        RightIndex {
            slots: Hashed(map),
            groups,
        }
    }
}

impl<S: Slots> RightIndex<S> {
    /// The number of pairs `left` forms with the indexed column: the rows
    /// holding each left code, summed over the left column.
    fn match_count(&self, left: &[TermCode]) -> usize {
        left.iter()
            .map(|&code| self.groups.count(self.slots.slot(code)))
            .sum()
    }

    /// The right rows holding `code`, ascending.
    #[inline]
    fn rows_for(&self, code: TermCode) -> &[u64] {
        self.groups.rows(self.slots.slot(code))
    }

    /// For each left row in order, its right matches. A counting pass sizes
    /// both outputs before they are filled.
    fn join(&self, left: &[TermCode]) -> (Vec<u64>, Vec<u64>) {
        let total = self.match_count(left);
        let mut out_l = Vec::with_capacity(total);
        let mut out_r = Vec::with_capacity(total);
        for (i, &code) in left.iter().enumerate() {
            let rows = self.rows_for(code);
            if rows.is_empty() {
                continue;
            }
            out_l.extend(std::iter::repeat_n(row(i), rows.len()));
            out_r.extend_from_slice(rows);
        }
        (out_l, out_r)
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

    /// A join's pair columns, `(left_idx, right_idx)`.
    type Pairs = (Vec<u64>, Vec<u64>);

    /// `left` joined against each right index `right` can take: the hashed
    /// one, which any column can, and the dense one when `right`'s span fits
    /// a table. [`hash_join`] picks one of them; both must agree with the
    /// nested loop.
    fn index_joins(left: &[TermCode], right: &[TermCode]) -> Vec<(&'static str, Pairs)> {
        let mut joins = vec![("hashed", RightIndex::hashed(right).join(left))];
        if let Some(span) = Span::of(right) {
            joins.push(("dense", RightIndex::dense(right, span).join(left)));
        }
        joins
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
            // A dense span starting past 0, probed below it, inside it, in
            // its gaps (11, 13, 14) and above it.
            (
                vec![0, 9, 10, 11, 12, 13, 14, 15, 16, WIDE, TermCode::MAX, 10],
                vec![12, 10, 15, 10],
            ),
            // One wide code: dense, with a one-slot span. The narrow code it
            // would truncate to (7) and its neighbours miss.
            (vec![7, WIDE, WIDE - 1, WIDE + 1, 0, WIDE], vec![WIDE]),
            // Wide codes close together: dense over their span.
            (
                vec![WIDE + 2, 7, WIDE, WIDE + 9, 2],
                vec![WIDE + 2, WIDE, WIDE + 9, WIDE + 2],
            ),
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
            for (kind, (jl, jr)) in index_joins(&l, &r) {
                assert_eq!(jl, ol, "{kind} left indices for {l:?} ⋈ {r:?}");
                assert_eq!(jr, or, "{kind} right indices for {l:?} ⋈ {r:?}");
            }
        }
    }

    /// Column pairs that exercise the sizing of both join paths: empty
    /// inputs, all-equal codes, disjoint codes, and repeats on one or both
    /// sides, ascending and not.
    fn sizing_cases() -> Vec<(Vec<TermCode>, Vec<TermCode>)> {
        vec![
            (vec![], vec![]),
            (vec![], vec![1, 2]),
            (vec![1, 2], vec![]),
            (vec![5; 7], vec![5; 3]),
            (vec![1, 2, 3], vec![4, 5, 6]),
            (vec![4, 5, 6], vec![1, 2, 3]),
            (vec![9, 8, 7], vec![1, 2, 3]),
            (vec![1, 1, 2, 2, 2, 9], vec![1, 2, 2, 3, 9, 9]),
            (vec![9, 2, 2, 1, 2, 9, 1], vec![2, 9, 1, 2, 9, 9]),
            (vec![WIDE, 7, WIDE], vec![7, WIDE, WIDE]),
            (vec![7, 7, WIDE], vec![7, WIDE, WIDE, WIDE]),
            (vec![9, 12, 0, 12, 16], vec![12, 10, 12]),
            (vec![WIDE, 7, WIDE], vec![WIDE]),
        ]
    }

    /// The counting pass of each path equals the number of pairs the join
    /// produces.
    #[test]
    fn join_counts_equal_the_produced_lengths() {
        for (l, r) in sizing_cases() {
            let produced = nested_loop(&l, &r).0.len();
            assert_eq!(
                RightIndex::hashed(&r).match_count(&l),
                produced,
                "hashed count for {l:?} ⋈ {r:?}"
            );
            if let Some(span) = Span::of(&r) {
                assert_eq!(
                    RightIndex::dense(&r, span).match_count(&l),
                    produced,
                    "dense count for {l:?} ⋈ {r:?}"
                );
            }
            if is_ascending(&l) && is_ascending(&r) {
                assert_eq!(
                    merge_join_len(&l, &r),
                    produced,
                    "merge count for {l:?} ⋈ {r:?}"
                );
            }
        }
    }

    /// Every path fills vectors whose capacity is their length, so none
    /// grows its output by reallocation.
    #[test]
    fn join_outputs_are_allocated_at_their_final_length() {
        for (l, r) in sizing_cases() {
            let (want_l, want_r) = nested_loop(&l, &r);
            let mut paths = vec![("hash", hash_join(&l, &r))];
            paths.extend(index_joins(&l, &r));
            if is_ascending(&l) && is_ascending(&r) {
                paths.push(("merge", merge_join(&l, &r)));
            }
            for (path, (out_l, out_r)) in paths {
                assert_eq!(out_l, want_l, "{path} left for {l:?} ⋈ {r:?}");
                assert_eq!(out_r, want_r, "{path} right for {l:?} ⋈ {r:?}");
                assert_eq!(out_l.capacity(), out_l.len(), "{path} left capacity");
                assert_eq!(out_r.capacity(), out_r.len(), "{path} right capacity");
            }
        }
    }

    /// A dense right index spans its column's codes: a code below the
    /// smallest, above the largest or in a gap between them finds no rows,
    /// as does a code past what a `usize` holds.
    #[test]
    fn dense_index_misses_codes_outside_its_column() {
        let right = [12, 10, 15, 10];
        let span = Span::of(&right).expect("a narrow span fits a table");
        assert_eq!((span.min, span.width), (10, 6));
        let index = RightIndex::dense(&right, span);
        assert_eq!(index.rows_for(10), &[1, 3]);
        assert_eq!(index.rows_for(12), &[0]);
        assert_eq!(index.rows_for(15), &[2]);
        for code in [0, 9, 11, 13, 14, 16, WIDE, TermCode::MAX] {
            assert!(index.rows_for(code).is_empty(), "code {code}");
        }
        assert!(dense_table_fits(65_535, 1));
        assert!(!dense_table_fits(WIDE, 1));
    }

    /// A span sends every code outside it to its empty slot, including a
    /// code below its start (which wraps) at either end of the code range.
    #[test]
    fn span_clamps_every_miss_to_the_empty_slot() {
        let span = Span { min: 10, width: 6 };
        assert_eq!(span.slot(10), 0);
        assert_eq!(span.slot(15), 5);
        for code in [0, 9, 16, WIDE, TermCode::MAX] {
            assert_eq!(span.slot(code), 6, "code {code}");
        }
        let top = Span {
            min: TermCode::MAX,
            width: 1,
        };
        assert_eq!(top.slot(TermCode::MAX), 0);
        assert_eq!(top.slot(0), 1);
        assert_eq!(top.slot(TermCode::MAX - 1), 1);
        let bottom = Span { min: 0, width: 1 };
        assert_eq!(bottom.slot(0), 0);
        assert_eq!(bottom.slot(TermCode::MAX), 1);
    }

    /// Wide codes close together are dense over their span, so a single
    /// wide code gets a one-slot table; codes far apart are hashed.
    #[test]
    fn wide_codes_close_together_are_dense() {
        let close = Span::of(&[WIDE + 9, WIDE, WIDE + 2]).expect("close codes fit a table");
        assert_eq!((close.min, close.width), (WIDE, 10));
        assert!(Span::of(&[7, WIDE]).is_none());
        assert!(Span::of(&[]).is_none());
        let one = Span::of(&[WIDE]).expect("one code fits a table");
        assert_eq!((one.min, one.width), (WIDE, 1));
        let index = RightIndex::dense(&[WIDE], one);
        assert_eq!(index.rows_for(WIDE), &[0]);
        // The narrow code `WIDE` would truncate to, and its neighbours.
        for code in [7, WIDE - 1, WIDE + 1] {
            assert!(index.rows_for(code).is_empty(), "code {code}");
        }
    }

    /// A hashed right index finds each distinct code's rows, in the right
    /// column's order, and no rows for a code the column lacks.
    #[test]
    fn hashed_index_groups_rows_by_code() {
        let right = [TermCode::MAX, 1, WIDE, TermCode::MAX, 1];
        assert!(Span::of(&right).is_none());
        let index = RightIndex::hashed(&right);
        assert_eq!(index.rows_for(TermCode::MAX), &[0, 3]);
        assert_eq!(index.rows_for(1), &[1, 4]);
        assert_eq!(index.rows_for(WIDE), &[2]);
        for code in [0, 7, 2, TermCode::MAX - 1] {
            assert!(index.rows_for(code).is_empty(), "code {code}");
        }
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
