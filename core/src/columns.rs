//! Kernels over [`TermCode`] columns: distinct values and value counts, an
//! index gather, and an equi-join (its pair count, its row pairs, refused past
//! a cap before either output is allocated, and its build side
//! [`CodeGroups`], which joins a left column slice by slice and can gather the
//! joined columns). All work on plain slices and preserve order: distinct
//! values come out in first-seen order, join pairs in nested-loop order (left
//! rows in order, each with its right matches in their order). Row indices
//! and counts are `u64`, the codes' width, so one column type carries all
//! three. Each kernel uses a direct-mapped table when the code range is dense
//! relative to the column (`dense_table_fits`), else a hash map.

use std::collections::HashMap;
use std::ops::Range;

use rustc_hash::FxHashMap;
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

/// The matching row pairs of two key columns as parallel index columns
/// `(left_idx, right_idx)`: for every left row in order, each right row with
/// the same code in the right column's order (nested-loop order, whichever
/// strategy runs). The right column is grouped once ([`CodeGroups`]) and the
/// left probed in order; two ascending columns take one merge pass. A
/// counting pass ([`join_count`]) sizes each output, allocated once; a count
/// past `max_pairs` is refused before either is allocated, as `Err(pairs)`,
/// and `None` admits any count.
pub fn equi_join_indices(
    left: &[TermCode],
    right: &[TermCode],
    max_pairs: Option<usize>,
) -> Result<(Buffer<u64>, Buffer<u64>), usize> {
    let (out_l, out_r) = join_pairs(left, right, max_pairs)?;
    Ok((Buffer::from(out_l), Buffer::from(out_r)))
}

/// The number of pairs [`equi_join_indices`] produces for the same columns,
/// saturating at `usize::MAX`, from its counting pass alone: no pair column
/// is allocated.
pub fn join_count(left: &[TermCode], right: &[TermCode]) -> usize {
    if left.is_empty() || right.is_empty() {
        return 0;
    }
    if is_ascending(left) && is_ascending(right) {
        merge_join_len(left, right)
    } else {
        CodeGroups::build(right).count(left)
    }
}

/// The pair columns of [`equi_join_indices`], each allocated once at its
/// final length, or `Err(pairs)` for a count past `max_pairs`.
fn join_pairs(
    left: &[TermCode],
    right: &[TermCode],
    max_pairs: Option<usize>,
) -> Result<(Vec<u64>, Vec<u64>), usize> {
    if left.is_empty() || right.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }
    if is_ascending(left) && is_ascending(right) {
        merge_join(left, right, max_pairs)
    } else {
        hash_join(left, right, max_pairs)
    }
}

/// `Err(pairs)` when a join of `pairs` pairs exceeds `max_pairs`; `None`
/// admits any count.
fn within_cap(pairs: usize, max_pairs: Option<usize>) -> Result<(), usize> {
    match max_pairs {
        Some(max) if pairs > max => Err(pairs),
        _ => Ok(()),
    }
}

fn is_ascending(codes: &[TermCode]) -> bool {
    codes.windows(2).all(|w| w[0] <= w[1])
}

/// The join of a left column against the right one's [`CodeGroups`]: for
/// each left row in order, its right matches, or `Err(pairs)` for a count
/// past `max_pairs`.
fn hash_join(
    left: &[TermCode],
    right: &[TermCode],
    max_pairs: Option<usize>,
) -> Result<(Vec<u64>, Vec<u64>), usize> {
    CodeGroups::build(right).pairs(left, max_pairs)
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
/// lengths, summed over the equal runs, saturating at `usize::MAX`.
fn merge_join_len(left: &[TermCode], right: &[TermCode]) -> usize {
    let mut total: usize = 0;
    for_each_run_pair(left, right, |run_l, run_r| {
        total = total.saturating_add(run_l.len().saturating_mul(run_r.len()));
    });
    total
}

/// The join of two ascending key columns: the cross product of each pair of
/// equal-code runs, in nested-loop order. A counting walk sizes both outputs
/// and refuses a count past `max_pairs` with `Err(pairs)` before either is
/// allocated.
fn merge_join(
    left: &[TermCode],
    right: &[TermCode],
    max_pairs: Option<usize>,
) -> Result<(Vec<u64>, Vec<u64>), usize> {
    let total = merge_join_len(left, right);
    within_cap(total, max_pairs)?;
    let mut out_l = Vec::with_capacity(total);
    let mut out_r = Vec::with_capacity(total);
    for_each_run_pair(left, right, |run_l, run_r| {
        for li in run_l {
            out_l.extend(std::iter::repeat_n(row(li), run_r.len()));
            out_r.extend(run_r.clone().map(row));
        }
    });
    Ok((out_l, out_r))
}

/// The right column grouped by code: [`Groups`] holds each code's right rows
/// in a slot of their own, and `slots` finds a code's slot, through a table
/// over the column's code [`Span`] or a [`Hashed`] map of its codes.
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
    /// holding each left code, summed over the left column, saturating at
    /// `usize::MAX`.
    fn match_count(&self, left: &[TermCode]) -> usize {
        let counts = left
            .iter()
            .map(|&code| self.groups.count(self.slots.slot(code)));
        // A left row matches at most every right row, so the sum fits a
        // `usize` whenever `left.len()` times the right rows does; only past
        // that bound does the sum need to saturate.
        if left.len().checked_mul(self.groups.rows.len()).is_some() {
            counts.sum()
        } else {
            counts.fold(0, usize::saturating_add)
        }
    }

    /// The right rows holding `code`, ascending.
    #[inline]
    fn rows_for(&self, code: TermCode) -> &[u64] {
        self.groups.rows(self.slots.slot(code))
    }

    /// For each left row in order, its right matches. A counting pass sizes
    /// both outputs before they are filled, and refuses a count past
    /// `max_pairs` with `Err(pairs)` before either is allocated.
    fn join(
        &self,
        left: &[TermCode],
        max_pairs: Option<usize>,
    ) -> Result<(Vec<u64>, Vec<u64>), usize> {
        let total = self.match_count(left);
        within_cap(total, max_pairs)?;
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
        Ok((out_l, out_r))
    }

    /// For each left row in order, its right matches as values rather than
    /// row indices: one output per column, each of `left_columns` at the left
    /// row, then each of `right_columns` at the right row. A counting pass
    /// sizes every output and refuses a count past `max_pairs` with
    /// `Err(pairs)` before any is allocated; each output is then filled in a
    /// pass of its own over the left rows.
    fn gather(
        &self,
        left: &[TermCode],
        left_columns: &[&[TermCode]],
        right_columns: &[&[TermCode]],
        max_pairs: Option<usize>,
    ) -> Result<Vec<Vec<TermCode>>, usize> {
        let total = self.match_count(left);
        within_cap(total, max_pairs)?;
        let mut outputs = Vec::with_capacity(left_columns.len() + right_columns.len());
        for column in left_columns {
            let mut out = Vec::with_capacity(total);
            for (&code, &value) in left.iter().zip(column.iter()) {
                let matches = self.groups.count(self.slots.slot(code));
                out.extend(std::iter::repeat_n(value, matches));
            }
            outputs.push(out);
        }
        for column in right_columns {
            let mut out = Vec::with_capacity(total);
            for &code in left {
                // A grouped row is a `usize` row of the right column, widened.
                out.extend(self.rows_for(code).iter().map(|&r| column[r as usize]));
            }
            outputs.push(out);
        }
        Ok(outputs)
    }
}

/// A right key column's rows grouped by code: the build side of
/// [`equi_join_indices`], built once and probed by any number of left
/// columns, so a join too large for one call runs as a loop over slices of
/// its left column. It holds one `u64` per right row and one slot per code
/// of the column's span (a table, when the span is narrow enough) or per
/// distinct code (a hash map otherwise).
pub struct CodeGroups(Grouping);

/// The [`RightIndex`] behind a [`CodeGroups`], of the kind [`Span::of`]
/// picks. Each kind's methods are compiled separately, so a lookup inlines
/// into both of a join's passes and the kind is matched once per call.
enum Grouping {
    Dense(RightIndex<Span>),
    Hashed(RightIndex<Hashed>),
}

impl CodeGroups {
    /// The rows of `right` grouped by code: under a table over its codes'
    /// span when one fits, else under a hashed map of its distinct codes.
    pub fn build(right: &[TermCode]) -> Self {
        CodeGroups(match Span::of(right) {
            Some(span) => Grouping::Dense(RightIndex::dense(right, span)),
            None => Grouping::Hashed(RightIndex::hashed(right)),
        })
    }

    /// The number of right rows grouped.
    pub fn len(&self) -> usize {
        match &self.0 {
            Grouping::Dense(index) => index.groups.rows.len(),
            Grouping::Hashed(index) => index.groups.rows.len(),
        }
    }

    /// Whether no right row is grouped.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The number of pairs `left` forms with the grouped rows, saturating at
    /// `usize::MAX`: what [`join`](Self::join) produces, without allocating
    /// it.
    pub fn count(&self, left: &[TermCode]) -> usize {
        match &self.0 {
            Grouping::Dense(index) => index.match_count(left),
            Grouping::Hashed(index) => index.match_count(left),
        }
    }

    /// The pairs `left` forms with the grouped rows, as parallel index
    /// columns `(left_idx, right_idx)` in nested-loop order (left rows in
    /// order, each with its right rows in their column's order), the left
    /// indices counted from `left`'s first row. A count past `max_pairs` is
    /// refused before either output is allocated: `Err(pairs)` carries it,
    /// and `None` admits any count.
    ///
    /// For consecutive slices of a left column, the concatenation of their
    /// joins, each slice's left indices offset by the slice's start, equals
    /// [`equi_join_indices`] of the whole column against the grouped one with
    /// no cap.
    pub fn join(
        &self,
        left: &[TermCode],
        max_pairs: Option<usize>,
    ) -> Result<(Buffer<u64>, Buffer<u64>), usize> {
        let (out_l, out_r) = self.pairs(left, max_pairs)?;
        Ok((Buffer::from(out_l), Buffer::from(out_r)))
    }

    /// The columns [`join`](Self::join)'s pairs gather, filled while the
    /// pairs are found: one output per column, left columns first, each of
    /// `left_columns` read at a pair's left row and each of `right_columns` at
    /// its right row, in nested-loop order. Each output equals [`take`] of its
    /// column at the matching index column of [`join`](Self::join). A count
    /// past `max_pairs` is refused before any output is allocated:
    /// `Err(pairs)` carries it, and `None` admits any count.
    ///
    /// # Panics
    ///
    /// When a left column is not as long as `left`, or a right column not as
    /// long as the grouped column.
    pub fn gather(
        &self,
        left: &[TermCode],
        left_columns: &[&[TermCode]],
        right_columns: &[&[TermCode]],
        max_pairs: Option<usize>,
    ) -> Result<Vec<Buffer<TermCode>>, usize> {
        assert!(
            left_columns.iter().all(|column| column.len() == left.len()),
            "a left column runs parallel to the left keys"
        );
        assert!(
            right_columns
                .iter()
                .all(|column| column.len() == self.len()),
            "a right column runs parallel to the grouped column"
        );
        let outputs = match &self.0 {
            Grouping::Dense(index) => index.gather(left, left_columns, right_columns, max_pairs),
            Grouping::Hashed(index) => index.gather(left, left_columns, right_columns, max_pairs),
        }?;
        Ok(outputs.into_iter().map(Buffer::from).collect())
    }

    /// The pair columns of [`join`](Self::join), each allocated once at its
    /// final length.
    fn pairs(
        &self,
        left: &[TermCode],
        max_pairs: Option<usize>,
    ) -> Result<(Vec<u64>, Vec<u64>), usize> {
        match &self.0 {
            Grouping::Dense(index) => index.join(left, max_pairs),
            Grouping::Hashed(index) => index.join(left, max_pairs),
        }
    }

    /// Whether the groups sit under a table over the right column's span.
    #[cfg(test)]
    fn is_dense(&self) -> bool {
        matches!(self.0, Grouping::Dense(_))
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
        let mut joins = vec![(
            "hashed",
            RightIndex::hashed(right).join(left, None).unwrap(),
        )];
        if let Some(span) = Span::of(right) {
            joins.push((
                "dense",
                RightIndex::dense(right, span).join(left, None).unwrap(),
            ));
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

    /// Column pairs that exercise every join strategy: the hashed and the
    /// dense right index, the merge path, sparse and wide codes, a dense span
    /// probed outside it and in its gaps, and an empty side.
    fn join_cases() -> Vec<(Vec<TermCode>, Vec<TermCode>)> {
        vec![
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
        ]
    }

    #[test]
    fn join_matches_the_nested_loop_in_every_strategy() {
        for (l, r) in join_cases() {
            let (li, ri) = equi_join_indices(&l, &r, None).unwrap();
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

    /// The counting pass of each path, and [`join_count`], equal the number
    /// of pairs the join produces.
    #[test]
    fn join_counts_equal_the_produced_lengths() {
        for (l, r) in sizing_cases() {
            let produced = nested_loop(&l, &r).0.len();
            assert_eq!(join_count(&l, &r), produced, "join_count for {l:?} ⋈ {r:?}");
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

    /// A cap admits a join of at most its pairs, whole, and refuses one past
    /// it with the count; an empty join passes a cap of zero. The build side
    /// applies the same cap.
    #[test]
    fn join_cap_refuses_only_past_the_count() {
        for (l, r) in sizing_cases() {
            let produced = nested_loop(&l, &r).0.len();
            let uncapped = equi_join_indices(&l, &r, None).unwrap();
            let groups = CodeGroups::build(&r);
            assert_eq!(
                equi_join_indices(&l, &r, Some(produced)),
                Ok(uncapped.clone()),
                "a cap at the count admits {l:?} ⋈ {r:?}"
            );
            assert_eq!(
                equi_join_indices(&l, &r, Some(produced + 1)),
                Ok(uncapped.clone()),
                "a cap above the count admits {l:?} ⋈ {r:?}"
            );
            assert_eq!(
                groups.join(&l, Some(produced)),
                Ok(uncapped),
                "the build side's cap at the count admits {l:?} ⋈ {r:?}"
            );
            if produced > 0 {
                assert_eq!(
                    equi_join_indices(&l, &r, Some(produced.saturating_sub(1))),
                    Err(produced),
                    "a cap below the count refuses {l:?} ⋈ {r:?}"
                );
                assert_eq!(
                    groups.join(&l, Some(produced.saturating_sub(1))),
                    Err(produced),
                    "the build side's cap below the count refuses {l:?} ⋈ {r:?}"
                );
            }
        }
        let empty = (Buffer::empty(), Buffer::empty());
        assert_eq!(
            equi_join_indices(&[], &[1], Some(0)),
            Ok(empty.clone()),
            "an empty left side under a cap of zero"
        );
        assert_eq!(
            CodeGroups::build(&[1]).join(&[], Some(0)),
            Ok(empty),
            "an empty left side against the build side under a cap of zero"
        );
    }

    /// A left column joined against a right column's [`CodeGroups`] slice by
    /// slice, each slice's left indices offset by its start, is the one-shot
    /// join, for every strategy that join picks and every slice width; each
    /// slice's count is its pairs, and the groups hold every right row.
    #[test]
    fn groups_join_slices_concatenate_to_the_one_shot_result() {
        for (l, r) in join_cases() {
            let groups = CodeGroups::build(&r);
            assert_eq!(groups.len(), r.len(), "right rows of {r:?}");
            assert_eq!(groups.is_empty(), r.is_empty(), "right rows of {r:?}");
            let (want_l, want_r) = equi_join_indices(&l, &r, None).unwrap();
            for width in [1, 2, 3, l.len().max(1)] {
                let (mut got_l, mut got_r) = (Vec::new(), Vec::new());
                for (k, slice) in l.chunks(width).enumerate() {
                    let start = row(k * width);
                    let (slice_l, slice_r) = groups.join(slice, None).unwrap();
                    assert_eq!(
                        groups.count(slice),
                        slice_l.len(),
                        "count of slice {k} at width {width} for {l:?} ⋈ {r:?}"
                    );
                    got_l.extend(slice_l.iter().map(|&i| i + start));
                    got_r.extend_from_slice(slice_r.as_slice());
                    // Gathering the row numbers gives the pair indices back,
                    // and the keys gathered from either side agree.
                    let rows_l: Vec<u64> = (0..slice.len()).map(|i| row(i) + start).collect();
                    let rows_r: Vec<u64> = (0..r.len()).map(row).collect();
                    let gathered = groups
                        .gather(slice, &[slice, &rows_l], &[&r, &rows_r], None)
                        .unwrap();
                    assert_eq!(
                        gathered[1].iter().map(|&i| i - start).collect::<Vec<_>>(),
                        slice_l.as_slice(),
                        "gathered left rows of slice {k} at width {width} for {l:?} ⋈ {r:?}"
                    );
                    assert_eq!(
                        gathered[3], slice_r,
                        "gathered right rows of slice {k} at width {width} for {l:?} ⋈ {r:?}"
                    );
                    assert_eq!(
                        gathered[0], gathered[2],
                        "gathered keys of slice {k} at width {width} for {l:?} ⋈ {r:?}"
                    );
                    if !slice_l.is_empty() {
                        assert_eq!(
                            groups.gather(slice, &[], &[], Some(slice_l.len() - 1)),
                            Err(slice_l.len()),
                            "a gather past its cap at width {width} for {l:?} ⋈ {r:?}"
                        );
                    }
                }
                assert_eq!(
                    got_l,
                    want_l.as_slice(),
                    "left indices at width {width} for {l:?} ⋈ {r:?}"
                );
                assert_eq!(
                    got_r,
                    want_r.as_slice(),
                    "right indices at width {width} for {l:?} ⋈ {r:?}"
                );
            }
        }
        let empty = CodeGroups::build(&[]);
        assert!(empty.is_empty(), "an empty right column groups no row");
        assert_eq!(
            empty.count(&[1, 2]),
            0,
            "an empty right column matches nothing"
        );
    }

    /// The build side picks the right index the one-shot join picks: a table
    /// over a narrow span, a hashed map for codes too far apart for one.
    #[test]
    fn groups_pick_the_dense_arm_when_the_span_fits() {
        assert!(
            CodeGroups::build(&[12, 10, 15, 10]).is_dense(),
            "a narrow span is a table"
        );
        assert!(
            !CodeGroups::build(&[7, WIDE]).is_dense(),
            "codes far apart are hashed"
        );
    }

    /// Every path fills vectors whose capacity is their length, so none
    /// grows its output by reallocation.
    #[test]
    fn join_outputs_are_allocated_at_their_final_length() {
        for (l, r) in sizing_cases() {
            let (want_l, want_r) = nested_loop(&l, &r);
            let mut paths = vec![("hash", hash_join(&l, &r, None).unwrap())];
            paths.extend(index_joins(&l, &r));
            if is_ascending(&l) && is_ascending(&r) {
                paths.push(("merge", merge_join(&l, &r, None).unwrap()));
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
        let (li, ri) = equi_join_indices(&left_key, &right_key, None).unwrap();
        let lv = take(&left_val, li.as_slice()).unwrap();
        let rv = take(&right_val, ri.as_slice()).unwrap();
        assert_eq!(lv.as_slice(), &[20, 20, 21, 21, 30]);
        assert_eq!(rv.as_slice(), &[200, 201, 200, 201, 300]);
    }
}
