//! Bit-packed nodes: FastLanes words of `bit_width` bits per value, with
//! exception values (patches) written over them. A point read unpacks one
//! value; a bulk read, count or find takes each part of a 1024-value chunk it
//! touches one value at a time, at its positions, or as the whole chunk, by
//! one read rule ([`unpack_tier`]). Every unchecked unpack relies on what
//! [`PackedNode::new`] checks.

use crate::node::{FL_CHUNK, PatchProbe};
use crate::search::EqSink;
use crate::words::Words;
use std::ops::Range;

#[cfg(test)]
thread_local! {
    /// The values this thread's unpacks unpacked: what the read rule costs,
    /// for its tests.
    static UNPACKED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Adds `values` to [`UNPACKED`] in tests; nothing otherwise.
#[inline(always)]
fn unpacked(values: usize) {
    #[cfg(test)]
    UNPACKED.with(|unpacked| unpacked.set(unpacked.get() + values));
    #[cfg(not(test))]
    let _ = values;
}

/// Bit-packed words plus the packing parameters needed for point extraction.
/// Every field is fixed at [`PackedNode::new`], which checks what the
/// unchecked unpacks below rely on.
pub(crate) struct PackedNode<'a> {
    packed: Words<'a>,
    bit_width: usize,
    offset: usize,
    len: usize,
    patches: Option<PatchProbe<'a>>,
}

impl<'a> PackedNode<'a> {
    /// The node of `len` values from physical position `offset` of `packed`,
    /// `bit_width` bits each, with `patches` written over them; `None` unless
    /// the width fits the words and `packed` holds every FastLanes chunk the
    /// values reach, `128 * bit_width / word bytes` words apiece.
    pub(crate) fn new(
        packed: Words<'a>,
        bit_width: usize,
        offset: usize,
        len: usize,
        patches: Option<PatchProbe<'a>>,
    ) -> Option<Self> {
        let bytes = packed.word_bytes();
        if bit_width > 8 * bytes {
            return None;
        }
        let chunks = offset.checked_add(len)?.div_ceil(FL_CHUNK);
        let words = chunks.checked_mul(128 * bit_width / bytes)?;
        (words <= packed.len()).then_some(Self {
            packed,
            bit_width,
            offset,
            len,
            patches,
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    /// The exception values written over the packed ones, if any.
    pub(crate) fn patches(&self) -> Option<&PatchProbe<'a>> {
        self.patches.as_ref()
    }
}

impl PackedNode<'_> {
    /// Patch-aware logical value at `i`. The sorted invariant of the parent
    /// column holds only for patched values, so bounds search must go through
    /// this accessor, never the raw packed words.
    pub(crate) fn value_at(&self, i: usize) -> u64 {
        if let Some(p) = &self.patches
            && let Some(v) = p.lookup(i)
        {
            return v;
        }
        if self.bit_width == 0 {
            return 0;
        }
        let index = i + self.offset;
        match &self.packed {
            Words::U8(s) => self.unpack(s, index).into(),
            Words::U16(s) => self.unpack(s, index).into(),
            Words::U32(s) => self.unpack(s, index).into(),
            Words::U64(s) => self.unpack(s, index),
        }
    }

    /// The patch-aware values at logical `start..start + out.len()`, a chunk
    /// part at a time by the read rule ([`unpack_tier`]), widened; the
    /// patches in range are then written over them
    /// ([`PatchProbe::overwrite`]), where [`Self::value_at`] searches the
    /// patches once per value.
    pub(crate) fn read_into(&self, start: usize, out: &mut [u64]) {
        fn read<T>(node: &PackedNode<'_>, packed: &[T], start: usize, out: &mut [u64])
        where
            T: vortex_array::dtype::NativePType + fastlanes::BitPacking + Into<u64> + Default,
        {
            let first = node.offset + start;
            // A read the rule takes one by one in all goes straight into place.
            if unpack_tier::<T>(out.len()) == Tier::OneByOne {
                for (o, i) in out.iter_mut().zip(first..) {
                    *o = node.unpack(packed, i).into();
                }
                return;
            }
            node.read_parts(packed, first, out.len(), |at, values| {
                for (o, &v) in out[at..at + values.len()].iter_mut().zip(values) {
                    *o = v.into();
                }
            });
        }
        if self.bit_width == 0 {
            out.fill(0);
        } else {
            match &self.packed {
                Words::U8(s) => read(self, s, start, out),
                Words::U16(s) => read(self, s, start, out),
                Words::U32(s) => read(self, s, start, out),
                Words::U64(s) => read(self, s, start, out),
            }
        }
        if let Some(patches) = &self.patches {
            patches.overwrite(start, out);
        }
    }

    /// The values at `start..start + len` equal to `needle`, into `sink` as
    /// the offsets `base + k` of the values at `start + k`: each chunk part
    /// unpacked as a read takes it ([`unpack_tier`]) and compared in the
    /// stored words. Requires no patches.
    pub(crate) fn eq_into<S: EqSink>(
        &self,
        start: usize,
        len: usize,
        needle: u64,
        base: usize,
        sink: &mut S,
    ) {
        fn find<T, S: EqSink>(
            node: &PackedNode<'_>,
            packed: &[T],
            (start, len, needle, base): (usize, usize, u64, usize),
            sink: &mut S,
        ) where
            T: vortex_array::dtype::NativePType + fastlanes::BitPacking + TryFrom<u64> + Default,
        {
            // A needle wider than the words matches none of them.
            let Ok(needle) = T::try_from(needle) else {
                return;
            };
            node.read_parts(packed, node.offset + start, len, |at, values| {
                sink.matching(base + at, values, needle);
            });
        }
        debug_assert!(self.patches.is_none(), "a patched node searches by reading");
        if self.bit_width == 0 {
            if needle == 0 {
                sink.run(base..base + len);
            }
            return;
        }
        let window = (start, len, needle, base);
        match &self.packed {
            Words::U8(s) => find(self, s, window, sink),
            Words::U16(s) => find(self, s, window, sink),
            Words::U32(s) => find(self, s, window, sink),
            Words::U64(s) => find(self, s, window, sink),
        }
    }

    /// Hands `f` the values at physical positions `first..first + n`, a
    /// chunk part at a time, each with its offset in the window, unpacked as
    /// the read rule takes it ([`unpack_tier`]). A read the rule takes one by
    /// one in all is read so across its chunks, in one part.
    fn read_parts<T>(&self, packed: &[T], first: usize, n: usize, mut f: impl FnMut(usize, &[T]))
    where
        T: vortex_array::dtype::NativePType + fastlanes::BitPacking + Default,
    {
        match unpack_tier::<T>(n) {
            Tier::OneByOne => {
                let mut values = [T::default(); UNPACK_ONE_BY_ONE];
                for (v, i) in values.iter_mut().zip(first..first + n) {
                    *v = self.unpack(packed, i);
                }
                f(0, &values[..n]);
            }
            Tier::Positions => {
                for (c, part) in chunk_parts(first, first + n) {
                    let at = c * FL_CHUNK + part.start - first;
                    self.read_short_part(packed, c, part, |values| f(at, values));
                }
            }
            Tier::Chunk => self.read_long_parts(packed, first, n, f),
        }
    }

    /// [`Self::read_parts`] of more values than the crossover: the only reads
    /// whose parts may take a whole chunk. Out of line, so the chunk's buffer
    /// is reserved only by the reads that may fill it.
    #[inline(never)]
    fn read_long_parts<T>(
        &self,
        packed: &[T],
        first: usize,
        n: usize,
        mut f: impl FnMut(usize, &[T]),
    ) where
        T: vortex_array::dtype::NativePType + fastlanes::BitPacking + Default,
    {
        // Filled when a part first takes its whole chunk.
        let mut chunk: Option<[T; FL_CHUNK]> = None;
        for (c, part) in chunk_parts(first, first + n) {
            let at = c * FL_CHUNK + part.start - first;
            if unpack_tier::<T>(part.len()) == Tier::Chunk {
                let chunk = chunk.get_or_insert_with(|| [T::default(); FL_CHUNK]);
                self.unpack_chunk(packed, c, chunk);
                f(at, &chunk[part]);
            } else {
                self.read_short_part(packed, c, part, |values| f(at, values));
            }
        }
    }

    /// The values at positions `part` of FastLanes chunk `c`, at most the
    /// crossover of them, handed to `f`: one by one or at their positions,
    /// by the read rule.
    fn read_short_part<T>(&self, packed: &[T], c: usize, part: Range<usize>, f: impl FnOnce(&[T]))
    where
        T: vortex_array::dtype::NativePType + fastlanes::BitPacking + Default,
    {
        match unpack_tier::<T>(part.len()) {
            Tier::OneByOne => {
                let mut values = [T::default(); UNPACK_ONE_BY_ONE];
                for (v, i) in values.iter_mut().zip(part.clone()) {
                    *v = self.unpack(packed, c * FL_CHUNK + i);
                }
                f(&values[..part.len()]);
            }
            Tier::Positions => self.unpack_positions(packed, c, part, f),
            Tier::Chunk => unreachable!("a short part takes at most the crossover"),
        }
    }

    /// Unpack FastLanes chunk `c` of `packed` whole into `chunk`.
    fn unpack_chunk<T>(&self, packed: &[T], c: usize, chunk: &mut [T; FL_CHUNK])
    where
        T: vortex_array::dtype::NativePType + fastlanes::BitPacking,
    {
        unpacked(FL_CHUNK);
        // A chunk of 1024 values packs into `bit_width` bits apiece.
        let words = 128 * self.bit_width / std::mem::size_of::<T>();
        // SAFETY: the slice is exactly one chunk's words (its bounds are
        // checked), `chunk` holds 1024 values, and `bit_width` fits `T`, as
        // `new` checked.
        unsafe {
            T::unchecked_unpack(self.bit_width, &packed[c * words..(c + 1) * words], chunk);
        }
    }

    /// The values at positions `part` of FastLanes chunk `c`, at most
    /// [`PART_MAX`] of them, handed to `f`: fastlanes' batch unpack, which
    /// dispatches on the bit width once for the part, where [`Self::unpack`]
    /// dispatches once per value.
    fn unpack_positions<T>(&self, packed: &[T], c: usize, part: Range<usize>, f: impl FnOnce(&[T]))
    where
        T: vortex_array::dtype::NativePType + fastlanes::BitPacking,
    {
        use std::mem::MaybeUninit;
        let n = part.len();
        assert!(n <= PART_MAX, "a part of at most PART_MAX values");
        unpacked(n);
        let mut values = [const { MaybeUninit::<T>::uninit() }; PART_MAX];
        let words = 128 * self.bit_width / std::mem::size_of::<T>();
        // SAFETY: as in `unpack_chunk`, the input is exactly one chunk's
        // words and `bit_width` fits `T`; every position is below 1024, and
        // the output holds one slot per position.
        unsafe {
            T::unchecked_unpack_indices(
                self.bit_width,
                &packed[c * words..(c + 1) * words],
                &POSITIONS[part],
                &mut values[..n],
            );
        }
        // SAFETY: the unpack wrote each of the first `n` slots, and a
        // `MaybeUninit<T>` is laid out as a `T`.
        f(unsafe { std::slice::from_raw_parts(values.as_ptr().cast::<T>(), n) });
    }

    /// The packed value at physical `index` of `packed`.
    fn unpack<T>(&self, packed: &[T], index: usize) -> T
    where
        T: vortex_array::dtype::NativePType + fastlanes::BitPacking,
    {
        unpacked(1);
        // SAFETY: `new` checked that the buffer holds every chunk up to
        // `(offset + len).div_ceil(1024)`, and callers keep `i < len`, so
        // `index = i + offset` lies in a chunk the buffer holds.
        unsafe {
            vortex_fastlanes::bitpack_decompress::unpack_single_primitive::<T>(
                packed,
                self.bit_width,
                index,
            )
        }
    }
}

/// The FastLanes chunks that physical positions `first..last` touch: each
/// chunk's index and the part of its 1024 positions in range.
fn chunk_parts(first: usize, last: usize) -> impl Iterator<Item = (usize, std::ops::Range<usize>)> {
    (first / FL_CHUNK..last.div_ceil(FL_CHUNK)).map(move |c| {
        let at = c * FL_CHUNK;
        (c, first.max(at) - at..last.min(at + FL_CHUNK) - at)
    })
}

/// The most values of one FastLanes chunk that a bulk read unpacks at their
/// positions, per stored word width: past it, unpacking the whole chunk
/// costs no more per value. Measured with the probe bench's `read_into_*`
/// arms, on columns packed at 7, 13, 20 and 20 bits.
const fn unpack_crossover<T>() -> usize {
    match std::mem::size_of::<T>() {
        1 => 16,
        2 => 32,
        4 => 96,
        _ => 128,
    }
}

/// The most values of one chunk that a bulk read unpacks one by one: below
/// about seven, fastlanes' batch unpack
/// ([`PackedNode::unpack_positions`]) costs more to set up than its single
/// bit-width dispatch saves. Measured with the probe bench's `read_into_*`
/// arms at 2, 4 and 8 values.
const UNPACK_ONE_BY_ONE: usize = 6;

/// The largest [`unpack_crossover`]: the most positions a bulk read unpacks
/// from one chunk.
const PART_MAX: usize = unpack_crossover::<u64>();

/// How a bulk read, count or find takes the values it wants from one
/// FastLanes chunk.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Tier {
    /// One value at a time ([`PackedNode::unpack`]).
    OneByOne,
    /// At their positions, in one batch ([`PackedNode::unpack_positions`]).
    Positions,
    /// The whole chunk ([`PackedNode::unpack_chunk`]).
    Chunk,
}

/// The read rule: the [`Tier`] that takes `values` values from one chunk of
/// `T` words. One by one up to [`UNPACK_ONE_BY_ONE`], at their positions up
/// to the crossover ([`unpack_crossover`]), and the whole chunk past it.
const fn unpack_tier<T>(values: usize) -> Tier {
    if values <= UNPACK_ONE_BY_ONE {
        Tier::OneByOne
    } else if values <= unpack_crossover::<T>() {
        Tier::Positions
    } else {
        Tier::Chunk
    }
}

/// Every position of a FastLanes chunk, in order: a part's positions are a
/// slice of it, for fastlanes' unpack at positions.
static POSITIONS: [usize; FL_CHUNK] = {
    let mut positions = [0; FL_CHUNK];
    let mut i = 0;
    while i < FL_CHUNK {
        positions[i] = i;
        i += 1;
    }
    positions
};

const _: () = assert!(
    unpack_crossover::<u8>() <= PART_MAX
        && unpack_crossover::<u16>() <= PART_MAX
        && unpack_crossover::<u32>() <= PART_MAX
);

#[cfg(test)]
mod tests {
    use super::*;

    /// 2,048 values `i % 100` packed at 7 bits into `u32` words: 224 words
    /// per 1,024-value chunk.
    fn packed_7() -> Vec<u32> {
        use fastlanes::BitPacking;
        let values: Vec<u32> = (0..2_048u32).map(|i| i % 100).collect();
        let mut words = vec![0u32; 448];
        let chunks = values.as_chunks::<1_024>().0;
        for (chunk, out) in chunks.iter().zip(words.as_chunks_mut::<224>().0) {
            u32::pack::<7, 224>(chunk, out);
        }
        words
    }

    /// A packed node owns what its unchecked unpacks rely on: its bit width
    /// fits the word, and its buffer holds every chunk its rows reach.
    #[test]
    fn new_checks_the_packing() {
        let words = packed_7();
        let new = |words: &[u32], bit_width, offset, len| {
            PackedNode::new(Words::U32(words), bit_width, offset, len, None).is_some()
        };
        assert!(new(&words, 7, 0, 2_048));
        assert!(!new(&words[..447], 7, 0, 2_048), "a word short");
        assert!(!new(&words, 33, 0, 2_048), "wider than the word");
        assert!(new(&words, 7, 1, 2_047));
        assert!(!new(&words, 7, 1, 2_048), "a third chunk");
        assert!(new(&[], 0, 0, 2_048), "a zero width packs no words");
        let node = PackedNode::new(Words::U32(&words), 7, 0, 2_048, None).unwrap();
        assert_eq!(node.value_at(1_500), 1_500 % 100);
    }

    /// The values this thread's unpacks unpacked while `f` ran.
    fn unpacked_by(f: impl FnOnce()) -> usize {
        let before = UNPACKED.with(std::cell::Cell::get);
        f();
        UNPACKED.with(std::cell::Cell::get) - before
    }

    /// Counts and finds take each chunk part as a read does, by the read
    /// rule: a few values one by one, a part up to the crossover (96 values
    /// of `u32` words) at its positions, and only a longer part as its whole
    /// chunk. Each answers as a plain scan.
    #[test]
    fn counts_and_finds_unpack_as_reads_do() {
        use crate::search::{Count, Offsets};
        let words = packed_7();
        let node = PackedNode::new(Words::U32(&words), 7, 0, 2_048, None).unwrap();
        let at = |i: usize| (i % 100) as u64;
        let windows = [
            (5, 3, 3),
            (1_022, 3, 3),
            (100, 40, 40),
            (0, 100, 1_024),
            (0, 2_048, 2_048),
        ];
        for (start, len, unpacked) in windows {
            let needle = at(start + 1);
            let want: Vec<usize> = (0..len).filter(|&k| at(start + k) == needle).collect();
            let mut count = Count(0);
            let counted = unpacked_by(|| node.eq_into(start, len, needle, 0, &mut count));
            assert_eq!(counted, unpacked, "count {start}..+{len}");
            assert_eq!(count.0, want.len(), "count {start}..+{len}");
            let mut offsets = Vec::new();
            let found =
                unpacked_by(|| node.eq_into(start, len, needle, 0, &mut Offsets(&mut offsets)));
            assert_eq!(found, unpacked, "find {start}..+{len}");
            assert_eq!(offsets, want, "find {start}..+{len}");
            let mut out = vec![0; len];
            let read = unpacked_by(|| node.read_into(start, &mut out));
            assert_eq!(read, unpacked, "read {start}..+{len}");
            assert_eq!(out, (start..start + len).map(at).collect::<Vec<_>>());
        }
    }

    /// The read rule's tiers at their edges, per word width.
    #[test]
    fn the_read_rule_by_word_width() {
        assert_eq!(unpack_tier::<u32>(1), Tier::OneByOne);
        assert_eq!(unpack_tier::<u32>(6), Tier::OneByOne);
        assert_eq!(unpack_tier::<u32>(7), Tier::Positions);
        assert_eq!(unpack_tier::<u32>(96), Tier::Positions);
        assert_eq!(unpack_tier::<u32>(97), Tier::Chunk);
        assert_eq!(unpack_tier::<u8>(16), Tier::Positions);
        assert_eq!(unpack_tier::<u8>(17), Tier::Chunk);
        assert_eq!(unpack_tier::<u64>(128), Tier::Positions);
        assert_eq!(unpack_tier::<u64>(129), Tier::Chunk);
    }
}
