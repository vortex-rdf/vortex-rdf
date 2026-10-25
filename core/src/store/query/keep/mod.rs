//! [`Keep`]: a view narrowed to the rows whose code in one column is in a
//! set or range, the restriction a `VALUES` block or a `FILTER` on a term
//! predicate pushes below a pattern. Windows (`LIMIT`/`OFFSET`) are in
//! `query/window.rs`, the capped counts behind `ASK` in `read/count.rs`.
//!
//! A keep is a derived view that composes with
//! [`match_pattern`](VortexRdfStore::match_pattern) in either order and reads
//! through the same paths (`size`, `code_columns_gathered`, `quads`). Keeps
//! apply after the pattern and never as scan expressions: in memory
//! (`memory.rs`) as a binary search inside a sorted run or a pass over the
//! column's selected rows, on file (`file.rs`) as the located runs of a sorted
//! subject column or else a pass over the column streamed for the view's rows.

#[cfg(feature = "file-io")]
mod file;
mod memory;

#[cfg(test)]
pub(crate) use memory::keep_hooks;

use std::ops::Range;

use vortex_buffer::Buffer;

use crate::error::Result;
use crate::store::Backend;
use crate::store::VortexRdfStore;
use crate::store::schema::{QuadColumn, TermCode};

/// Which term codes a [`keep`](VortexRdfStore::keep) admits in a column.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Keep {
    /// Any of these codes; ascending and unique, as [`Keep::set`] builds
    /// them.
    Set(Buffer<TermCode>),
    /// Any code in the half-open range `lo..hi`. Codes rank the dictionary's
    /// spellings in byte order, so a term kind or an IRI namespace is one
    /// such range (see `DictReader::prefix_range`).
    Range(TermCode, TermCode),
}

impl Keep {
    /// The keep admitting exactly `codes` (any order, repeats folded).
    pub fn set(codes: impl IntoIterator<Item = TermCode>) -> Self {
        let mut codes: Vec<TermCode> = codes.into_iter().collect();
        codes.sort_unstable();
        codes.dedup();
        Keep::Set(Buffer::from(codes))
    }

    /// The keep admitting every code in `range`.
    pub fn range(range: Range<TermCode>) -> Self {
        Keep::Range(range.start, range.end.max(range.start))
    }

    /// Whether the keep admits no code at all.
    pub fn is_empty(&self) -> bool {
        match self {
            Keep::Set(codes) => codes.is_empty(),
            Keep::Range(lo, hi) => hi <= lo,
        }
    }

    /// Whether `code` is admitted.
    pub fn admits(&self, code: TermCode) -> bool {
        match self {
            Keep::Set(codes) => codes.as_slice().binary_search(&code).is_ok(),
            Keep::Range(lo, hi) => (*lo..*hi).contains(&code),
        }
    }

    /// The keep compiled for a row loop.
    fn test(&self) -> KeepTest<'_> {
        match self {
            Keep::Range(lo, hi) => KeepTest::Range(*lo, *hi),
            Keep::Set(codes) => {
                let codes = codes.as_slice();
                let (Some(&lo), Some(&hi)) = (codes.first(), codes.last()) else {
                    return KeepTest::Range(0, 0);
                };
                // A bitmap, a bit per code of the span, while the span is
                // within 8 bits per member. A span wider than a `usize` (on a
                // 32-bit target) takes the sorted test, not a wrapped bitmap.
                let span = usize::try_from(hi - lo)
                    .ok()
                    .filter(|&span| span < codes.len().saturating_mul(8));
                match span {
                    Some(span) => {
                        let mut bits = vec![0u64; (span + 1).div_ceil(64)];
                        for &code in codes {
                            // At most `span`, so it fits a `usize`.
                            let bit = (code - lo) as usize;
                            bits[bit / 64] |= 1u64 << (bit % 64);
                        }
                        KeepTest::Bitmap { lo, hi, bits }
                    }
                    None => KeepTest::Sorted(codes),
                }
            }
        }
    }
}

impl From<Buffer<TermCode>> for Keep {
    /// The keep admitting exactly `codes`: a buffer that is already ascending
    /// and unique becomes the set as it stands, shared rather than copied;
    /// any other goes through [`Keep::set`].
    fn from(codes: Buffer<TermCode>) -> Self {
        if codes.windows(2).all(|pair| pair[0] < pair[1]) {
            return Keep::Set(codes);
        }
        Keep::set(codes.iter().copied())
    }
}

/// A [`Keep`] as a per-row test: a range compare, a bitmap over a dense set's
/// span, or a binary search of a sparse one.
enum KeepTest<'a> {
    Range(TermCode, TermCode),
    Bitmap {
        lo: TermCode,
        hi: TermCode,
        bits: Vec<u64>,
    },
    Sorted(&'a [TermCode]),
}

impl KeepTest<'_> {
    #[inline]
    fn admits(&self, code: TermCode) -> bool {
        match self {
            KeepTest::Range(lo, hi) => (*lo..*hi).contains(&code),
            KeepTest::Bitmap { lo, hi, bits } => {
                if code < *lo || code > *hi {
                    return false;
                }
                // `lo <= code <= hi`, a span the bitmap was sized for.
                let bit = (code - lo) as usize;
                bits[bit / 64] & (1u64 << (bit % 64)) != 0
            }
            KeepTest::Sorted(codes) => codes.binary_search(&code).is_ok(),
        }
    }
}

impl VortexRdfStore {
    /// Narrow this view to the rows whose code in `column` the keep admits
    /// (the rows a `VALUES` block or a term predicate selects; see
    /// `DictReader::filter_codes`), inside the store rather than over
    /// gathered columns. Composes with [`match_pattern`] in either order and
    /// reads through the same paths; an empty keep is the empty view.
    ///
    /// Requires the Dictionary layout with an empty append tail (codes are
    /// the store's vocabulary only then, as for
    /// [`dict_reader`](Self::dict_reader)); anything else is an
    /// `InvalidOperation` error.
    ///
    /// In memory the keep is a binary search when the selected rows are a
    /// run of the sorted base that `column` orders (a bound prefix of the
    /// `(s, p, o, g)` order, as a subject-bound match leaves, or every row of
    /// a base sorted by subject for a keep on `s`), else one pass over the
    /// column's selected rows: read in place from a canonical column, else a
    /// piece of a chunk at a time, decoded and compared where the selected
    /// rows are dense and read row by row through the column's probe where
    /// they are sparse or point-sized. On file, a keep on
    /// the subject column of a store sorted by it is located: the rows it
    /// admits are runs of that column, found in the mapped leaf metadata and
    /// intersected with the view's rows, so nothing is read and a pending
    /// filter stays pending (a set of more than one code per sixteen rows of
    /// the view streams instead). Any other keep resolves the view's rows,
    /// streams the column for them and tests the keep in memory, never as a
    /// scan expression. A pending view's deferred row ids (a served match's,
    /// or a run held for a count) materialize first.
    ///
    /// [`match_pattern`]: Self::match_pattern
    pub async fn keep(&self, column: QuadColumn, keep: &Keep) -> Result<Self> {
        self.ensure_code_view("keep")?;
        if keep.is_empty() {
            return Ok(self.empty_view());
        }
        match &self.quads.backend {
            Backend::InMemory(memory) => self.keep_in_memory(memory, column, keep),
            #[cfg(feature = "file-io")]
            Backend::File(base) => self.keep_file(base, column, keep).await,
        }
    }

    /// [`keep`](Self::keep) for several columns at once, applied in the order
    /// given (each over the previous one's result).
    pub async fn keep_many(&self, keeps: &[(QuadColumn, Keep)]) -> Result<Self> {
        let mut view = self.clone();
        for (column, keep) in keeps {
            view = view.keep(*column, keep).await?;
        }
        Ok(view)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keep_set_sorts_and_dedups() {
        let keep = Keep::set([5, 1, 3, 3, 1]);
        assert_eq!(keep, Keep::Set(Buffer::from(vec![1u64, 3, 5])));
        assert!(keep.admits(3) && !keep.admits(4));
        assert!(Keep::set([]).is_empty());
        assert!(Keep::range(4..4).is_empty());
        let (lo, hi) = (9, 4);
        assert!(Keep::range(lo..hi).is_empty());
        assert!(Keep::range(2..5).admits(4) && !Keep::range(2..5).admits(5));
    }

    #[test]
    fn keep_from_a_buffer_shares_a_sorted_one_and_sorts_any_other() {
        let sorted = Buffer::from(vec![1u64, 3, 5]);
        let Keep::Set(shared) = Keep::from(sorted.clone()) else {
            panic!("a code buffer is a set keep");
        };
        assert_eq!(
            shared.as_ptr(),
            sorted.as_ptr(),
            "the sorted buffer was copied"
        );
        // Out of order, or ascending with a repeat: sorted and folded.
        for codes in [vec![5u64, 1, 3, 3, 1], vec![1, 3, 3, 5]] {
            assert_eq!(Keep::from(Buffer::from(codes)), Keep::Set(sorted.clone()));
        }
        assert!(Keep::from(Buffer::<TermCode>::empty()).is_empty());
    }

    #[test]
    fn keep_test_matches_admits() {
        let dense = Keep::set((100..140).step_by(3));
        let sparse = Keep::set([1, 1_000, 1_000_000]);
        let range = Keep::range(10..20);
        for keep in [&dense, &sparse, &range] {
            let test = keep.test();
            for code in (0..1_100).chain([999_999, 1_000_000, 1_000_001]) {
                assert_eq!(test.admits(code), keep.admits(code), "{keep:?} at {code}");
            }
        }
        assert!(matches!(dense.test(), KeepTest::Bitmap { .. }));
        assert!(matches!(sparse.test(), KeepTest::Sorted(_)));
    }
}
