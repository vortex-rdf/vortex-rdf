//! A window's walk over matches found part by part ([`WindowCursor`]), and
//! the live total it hands back ([`Live`]).

/// The live matches a window's walk counted: every one when the walk reached
/// the last ([`Exact`](Live::Exact)), else at least so many
/// ([`AtLeast`](Live::AtLeast)): a window that filled before the matches
/// ended counted no further, so its total is at least `offset + limit` when
/// its limit is not zero. A tailed view's window hands its tail what the
/// base did not consume by it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Live {
    Exact(usize),
    AtLeast(usize),
}

impl Live {
    /// The matches the total counts at least: all of them when exact.
    pub(crate) fn at_least(self) -> usize {
        match self {
            Live::Exact(n) | Live::AtLeast(n) => n,
        }
    }

    /// Every match, when the walk counted them all.
    #[cfg(test)]
    pub(crate) fn exact(self) -> Option<usize> {
        match self {
            Live::Exact(n) => Some(n),
            Live::AtLeast(_) => None,
        }
    }
}

/// The window `offset..offset + limit` (`None`: to the last) of matches a
/// walk finds part by part, in order: a part whose matches all lie before
/// the window is skipped by its count ([`skips`](Self::skips)), any other
/// part's matches are taken ([`take`](Self::take)), and the walk stops once
/// the window is full ([`is_full`](Self::is_full)). A window of no rows is
/// full from the start, so its walk reads nothing.
pub(crate) struct WindowCursor<T> {
    offset: usize,
    limit: Option<usize>,
    /// Matches counted so far, skipped or taken.
    seen: usize,
    rows: Vec<T>,
}

impl<T: Copy> WindowCursor<T> {
    pub(crate) fn new(offset: usize, limit: Option<usize>) -> Self {
        Self {
            offset,
            limit,
            seen: 0,
            rows: Vec::new(),
        }
    }

    /// Whether a part of `count` matches lies wholly before the window,
    /// so the walk skips it unread; its matches are then counted. False once
    /// the window has started, where a part's matches are taken instead.
    pub(crate) fn skips(&mut self, count: usize) -> bool {
        if self.seen.saturating_add(count) <= self.offset {
            self.seen += count;
            true
        } else {
            false
        }
    }

    /// Whether the walk has reached the window's first match, after which
    /// every part is read.
    pub(crate) fn started(&self) -> bool {
        self.seen >= self.offset
    }

    /// A part's matches, in order: those in the window kept, all counted.
    pub(crate) fn take(&mut self, matches: &[T]) {
        let from = self.offset.saturating_sub(self.seen).min(matches.len());
        let room = self
            .limit
            .map_or(usize::MAX, |limit| limit - self.rows.len());
        let to = from.saturating_add(room).min(matches.len());
        self.rows.extend_from_slice(&matches[from..to]);
        self.seen += matches.len();
    }

    /// Whether the window holds its limit's rows, so the walk stops.
    pub(crate) fn is_full(&self) -> bool {
        self.limit.is_some_and(|limit| self.rows.len() >= limit)
    }

    /// The window's rows, beside the total: exact when the walk `ended` at
    /// the last match, else at least what it counted.
    pub(crate) fn finish(self, ended: bool) -> (Vec<T>, Live) {
        let live = match ended {
            true => Live::Exact(self.seen),
            false => Live::AtLeast(self.seen),
        };
        (self.rows, live)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Window `3..5` over parts of 2, 2 and 4 matches: the first part lies
    /// before the window and is skipped by its count, the second gives its
    /// last match and the third its first; the total is exact when the walk
    /// ended and at least so many when it stopped short.
    #[test]
    fn cursor_skips_takes_and_finishes() {
        let walk = |ended: bool| {
            let mut cursor = WindowCursor::new(3, Some(2));
            assert!(!cursor.is_full());
            assert!(cursor.skips(2), "two matches before the window");
            assert!(!cursor.skips(2), "the window starts in the second part");
            cursor.take(&[10u64, 11]);
            assert!(!cursor.is_full());
            cursor.take(&[20, 21, 22, 23]);
            assert!(cursor.is_full());
            cursor.finish(ended)
        };
        assert_eq!(walk(true), (vec![11, 20], Live::Exact(8)));
        assert_eq!(walk(false), (vec![11, 20], Live::AtLeast(8)));
        assert_eq!(Live::Exact(8).at_least(), 8);
        assert_eq!(Live::AtLeast(8).exact(), None);
        assert_eq!(Live::Exact(8).exact(), Some(8));
    }

    /// A window of no rows is full from the start: a walk reads nothing and
    /// knows nothing of the total.
    #[test]
    fn cursor_limit_zero_reads_nothing() {
        let cursor = WindowCursor::<u64>::new(5, Some(0));
        assert!(cursor.is_full());
        assert_eq!(cursor.finish(false), (vec![], Live::AtLeast(0)));
    }

    /// A window with no limit takes every match from its offset on, and is
    /// never full.
    #[test]
    fn cursor_without_limit_takes_to_the_end() {
        let mut cursor = WindowCursor::new(1, None);
        cursor.take(&[1u64, 2, 3]);
        assert!(!cursor.is_full());
        assert!(
            !cursor.skips(5),
            "a window with no limit reads every part past its offset"
        );
        cursor.take(&[4, 5]);
        assert_eq!(cursor.finish(true), (vec![2, 3, 4, 5], Live::Exact(5)));
    }
}
