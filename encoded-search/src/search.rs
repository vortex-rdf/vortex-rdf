//! The searches over a virtual index range that every probe node runs:
//! bisection, and the interpolating search over ascending values.

/// Split points [`search_ascending`] may place by interpolation.
const INTERPOLATION_STEPS: usize = 4;

/// Windows up to this many values [`search_ascending`] splits at their middle:
/// a split by value saves little on so few.
const INTERPOLATION_MIN: usize = 16;

/// The first index of `0..n` whose value is `>= needle`, over values
/// ascending in that range: what `partition(n, |i| value(i) < needle)`
/// returns, with fewer reads where the values are spread evenly. It bisects
/// until a probe has landed on each side of the needle, then, while the
/// window is wider than [`INTERPOLATION_MIN`] and the two values bracketing it
/// span at least a quarter of its width, splits it where an even spread
/// between them puts the needle, for up to [`INTERPOLATION_STEPS`] splits, and
/// bisects the rest. Every split lies inside the window, so the answer is the
/// bisection's; the bracketing values are ones the probes read anyway, so long
/// plateaus and small windows cost what bisection does.
pub(crate) fn search_ascending(n: usize, value: impl Fn(usize) -> u64, needle: u64) -> usize {
    search_ascending_capped(n, value, needle).0
}

/// [`search_ascending`], with the first position its probes found holding a
/// value `> needle` (`n` when none did): the end of the needle's rows lies
/// between the answer and it, so the rows are bounded with no further read.
pub(crate) fn search_ascending_capped(
    n: usize,
    value: impl Fn(usize) -> u64,
    needle: u64,
) -> (usize, usize) {
    // The answer is in [lo, hi]. Bisect until a probe has landed on each side
    // of the needle: below = value(lo - 1) < needle <= value(hi) = at.
    let (mut lo, mut hi, mut past) = (0usize, n, n);
    let mut probe = |i: usize| {
        let v = value(i);
        if v > needle && i < past {
            past = i;
        }
        v
    };
    let (mut below, mut at) = (None, None);
    while lo < hi && (below.is_none() || at.is_none()) {
        let mid = lo + (hi - lo) / 2;
        let v = probe(mid);
        if v < needle {
            (lo, below) = (mid + 1, Some(v));
        } else {
            (hi, at) = (mid, Some(v));
        }
    }
    // Interpolate between them while the window is wide and its values are
    // spread; bisect what is left.
    if let (Some(mut b), Some(mut a)) = (below, at) {
        for _ in 0..INTERPOLATION_STEPS {
            let width = hi - lo;
            if width <= INTERPOLATION_MIN || a - b < width as u64 / 4 {
                break;
            }
            // Between positions lo - 1 and hi, which hold b and a.
            let ahead = u128::from(needle - b) * (width as u128 + 1) / u128::from(a - b);
            let split = (lo - 1 + ahead as usize).clamp(lo, hi - 1);
            let v = probe(split);
            if v < needle {
                (lo, b) = (split + 1, v);
            } else {
                (hi, a) = (split, v);
            }
        }
    }
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if probe(mid) < needle {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    (lo, past)
}

/// `partition_point` over a virtual index range, probing through a closure.
pub(crate) fn partition(n: usize, mut below: impl FnMut(usize) -> bool) -> usize {
    let (mut lo, mut hi) = (0usize, n);
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if below(mid) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `search_ascending` returns what bisection does, and its cap bounds the
    /// needle's rows, whatever the spread:
    /// even, skewed (a few huge gaps), plateaus of repeated values, values
    /// near `u64::MAX`, and lengths around the bisect-outright threshold; for
    /// needles at, between and past every value.
    #[test]
    fn ascending_searches_match_bisection() {
        let mut series: Vec<Vec<u64>> = vec![
            (0..10_000).map(|i| 7 + 3 * i).collect(),
            (0..5_000u64).map(|i| i * i * i).collect(),
            (0..4_000u64).map(|i| (i / 250) * 1_000_000).collect(),
            (0..3_000u64).map(|i| u64::MAX - 3_000 + i).collect(),
            (0..2_000u64)
                .map(|i| if i < 1_990 { i } else { 1 << 60 })
                .collect(),
        ];
        for n in [0usize, 1, 2, 15, 16, 17, 18, 40] {
            series.push((0..n as u64).map(|i| 100 + 2 * i).collect());
        }
        for values in &series {
            let mut needles = vec![0, 1, u64::MAX];
            for &v in values.iter().step_by((values.len() / 300).max(1)) {
                needles.extend([v.saturating_sub(1), v, v.saturating_add(1)]);
            }
            for needle in needles {
                let want = partition(values.len(), |i| values[i] < needle);
                let len = values.len();
                assert_eq!(
                    search_ascending(len, |i| values[i], needle),
                    want,
                    "len {len} needle {needle}"
                );
                let (at, past) = search_ascending_capped(len, |i| values[i], needle);
                assert_eq!(at, want, "capped: len {len} needle {needle}");
                let end = partition(len, |i| values[i] <= needle);
                assert!(
                    end <= past && (past == len || values[past] > needle),
                    "len {len} needle {needle}: cap {past}, rows end {end}"
                );
            }
        }
    }
}
