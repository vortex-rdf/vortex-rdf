//! A canonical buffer of unsigned words, borrowed at its own width: the
//! leaves of the probe tree, and the packed buffer a bit-packed node unpacks.

use crate::search::EqSink;

/// Typed borrow of a canonical primitive buffer, one variant per unsigned width.
pub(crate) enum Words<'a> {
    U8(&'a [u8]),
    U16(&'a [u16]),
    U32(&'a [u32]),
    U64(&'a [u64]),
}

impl Words<'_> {
    #[inline]
    pub(crate) fn len(&self) -> usize {
        match self {
            Words::U8(s) => s.len(),
            Words::U16(s) => s.len(),
            Words::U32(s) => s.len(),
            Words::U64(s) => s.len(),
        }
    }

    /// Bytes per word.
    pub(crate) fn word_bytes(&self) -> usize {
        match self {
            Words::U8(_) => 1,
            Words::U16(_) => 2,
            Words::U32(_) => 4,
            Words::U64(_) => 8,
        }
    }

    #[inline]
    pub(crate) fn get(&self, i: usize) -> u64 {
        match self {
            Words::U8(s) => u64::from(s[i]),
            Words::U16(s) => u64::from(s[i]),
            Words::U32(s) => u64::from(s[i]),
            Words::U64(s) => s[i],
        }
    }

    /// The words at `start..start + out.len()`, widened, into `out`.
    #[inline]
    pub(crate) fn read_into(&self, start: usize, out: &mut [u64]) {
        fn widen<T: Copy + Into<u64>>(words: &[T], out: &mut [u64]) {
            for (o, &w) in out.iter_mut().zip(words) {
                *o = w.into();
            }
        }
        let end = start + out.len();
        match self {
            Words::U8(s) => widen(&s[start..end], out),
            Words::U16(s) => widen(&s[start..end], out),
            Words::U32(s) => widen(&s[start..end], out),
            Words::U64(s) => out.copy_from_slice(&s[start..end]),
        }
    }

    /// The words at `start..start + len` equal to `needle`, into `sink` as
    /// the offsets `base + k` of the words at `start + k`. A needle wider
    /// than the words matches none of them.
    pub(crate) fn eq_into<S: EqSink>(
        &self,
        start: usize,
        len: usize,
        needle: u64,
        base: usize,
        sink: &mut S,
    ) {
        fn find<T: Copy + PartialEq + TryFrom<u64>, S: EqSink>(
            words: &[T],
            needle: u64,
            base: usize,
            sink: &mut S,
        ) {
            if let Ok(needle) = T::try_from(needle) {
                sink.matching(base, words, needle);
            }
        }
        let end = start + len;
        match self {
            Words::U8(s) => find(&s[start..end], needle, base, sink),
            Words::U16(s) => find(&s[start..end], needle, base, sink),
            Words::U32(s) => find(&s[start..end], needle, base, sink),
            Words::U64(s) => find(&s[start..end], needle, base, sink),
        }
    }

    /// First index whose value is `>= needle`, by the native monomorphized
    /// `partition_point`; the length for a needle past the width's maximum.
    #[inline]
    pub(crate) fn lower_bound(&self, needle: u64) -> usize {
        fn bound<T: Copy + Ord + TryFrom<u64>>(s: &[T], needle: u64) -> usize {
            match T::try_from(needle) {
                Ok(n) => s.partition_point(|&v| v < n),
                Err(_) => s.len(),
            }
        }
        match self {
            Words::U8(s) => bound(s, needle),
            Words::U16(s) => bound(s, needle),
            Words::U32(s) => bound(s, needle),
            Words::U64(s) => bound(s, needle),
        }
    }
}
