//! Bit-packed nodes: FastLanes words of `bit_width` bits per value, with
//! exception values (patches) written over them. A point read unpacks one
//! value; a bulk read unpacks each 1024-value chunk it touches one value at a
//! time, at its positions, or whole, by the read rule below. Every unchecked
//! unpack relies on what [`PackedNode::new`] checks.

use crate::node::{FL_CHUNK, PatchProbe};
use crate::search::EqSink;
use crate::words::Words;

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
            Words::U8(s) => self.unpack(s, index),
            Words::U16(s) => self.unpack(s, index),
            Words::U32(s) => self.unpack(s, index),
            Words::U64(s) => self.unpack(s, index),
        }
    }

    /// The patch-aware values at logical `start..start + out.len()`, a
    /// FastLanes chunk at a time: a chunk the read takes more than
    /// [`unpack_crossover`] values of is unpacked whole and widened, a
    /// smaller part unpacked at its positions
    /// ([`unpack_positions`](Self::unpack_positions)), and a read or part
    /// of at most [`UNPACK_ONE_BY_ONE`] values one by one. The patches in
    /// range are then written over them ([`PatchProbe::overwrite`]), where
    /// [`Self::value_at`] searches the patches once per value.
    pub(crate) fn read_into(&self, start: usize, out: &mut [u64]) {
        fn unpack_all<T>(node: &PackedNode<'_>, packed: &[T], start: usize, out: &mut [u64])
        where
            T: vortex_array::dtype::NativePType + fastlanes::BitPacking + Into<u64> + Default,
        {
            let first = node.offset + start;
            if out.len() <= UNPACK_ONE_BY_ONE {
                for (k, o) in out.iter_mut().enumerate() {
                    *o = node.unpack(packed, first + k);
                }
            } else if out.len() > unpack_crossover::<T>() {
                node.unpack_long(packed, first, out);
            } else {
                for (c, part) in chunk_parts(first, first + out.len()) {
                    let dst = &mut out[c * FL_CHUNK + part.start - first..][..part.len()];
                    node.unpack_part(packed, c, part, dst);
                }
            }
        }
        if self.bit_width == 0 {
            out.fill(0);
        } else {
            match &self.packed {
                Words::U8(s) => unpack_all(self, s, start, out),
                Words::U16(s) => unpack_all(self, s, start, out),
                Words::U32(s) => unpack_all(self, s, start, out),
                Words::U64(s) => unpack_all(self, s, start, out),
            }
        }
        if let Some(patches) = &self.patches {
            patches.overwrite(start, out);
        }
    }

    /// The values at `start..start + len` equal to `needle`, into `sink` as
    /// the offsets `base + k` of the values at `start + k`: the packed words
    /// unpacked a 1024-value FastLanes chunk at a time. Requires no patches.
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
            let first = node.offset + start;
            let mut chunk = [T::default(); FL_CHUNK];
            for (c, part) in chunk_parts(first, first + len) {
                node.unpack_chunk(packed, c, &mut chunk);
                // The chunk's position `part.start + j` is the window's
                // offset `c * 1024 + part.start + j - first`.
                let at = base + c * FL_CHUNK + part.start - first;
                sink.matching(at, &chunk[part], needle);
            }
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

    /// [`read_into`](Self::read_into)'s values at physical positions
    /// `first..first + out.len()`, more than [`unpack_crossover`] of them: a
    /// chunk the read takes more than the crossover of is unpacked whole and
    /// widened, a smaller part unpacked at its positions. Out of line, so
    /// the chunk's buffer is reserved only by reads that may fill it.
    #[inline(never)]
    fn unpack_long<T>(&self, packed: &[T], first: usize, out: &mut [u64])
    where
        T: vortex_array::dtype::NativePType + fastlanes::BitPacking + Into<u64> + Default,
    {
        // Filled only when a part passes the crossover.
        let mut chunk: Option<[T; FL_CHUNK]> = None;
        for (c, part) in chunk_parts(first, first + out.len()) {
            let dst = &mut out[c * FL_CHUNK + part.start - first..][..part.len()];
            if part.len() > unpack_crossover::<T>() {
                let chunk = chunk.get_or_insert_with(|| [T::default(); FL_CHUNK]);
                self.unpack_chunk(packed, c, chunk);
                for (o, &v) in dst.iter_mut().zip(&chunk[part]) {
                    *o = v.into();
                }
            } else {
                self.unpack_part(packed, c, part, dst);
            }
        }
    }

    /// The values at positions `part` of FastLanes chunk `c`, at most
    /// [`unpack_crossover`] of them, widened into `dst`: one by one for at
    /// most [`UNPACK_ONE_BY_ONE`], else at their positions in one batch.
    fn unpack_part<T>(&self, packed: &[T], c: usize, part: std::ops::Range<usize>, dst: &mut [u64])
    where
        T: vortex_array::dtype::NativePType + fastlanes::BitPacking + Into<u64>,
    {
        if part.len() <= UNPACK_ONE_BY_ONE {
            for (o, i) in dst.iter_mut().zip(part) {
                *o = self.unpack(packed, c * FL_CHUNK + i);
            }
        } else {
            self.unpack_positions(packed, c, part, dst);
        }
    }

    /// Unpack FastLanes chunk `c` of `packed` whole into `chunk`.
    fn unpack_chunk<T>(&self, packed: &[T], c: usize, chunk: &mut [T; FL_CHUNK])
    where
        T: vortex_array::dtype::NativePType + fastlanes::BitPacking,
    {
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
    /// [`PART_MAX`] of them, widened into `dst`: fastlanes' batch unpack,
    /// which dispatches on the bit width once for the part, where
    /// [`Self::unpack`] dispatches once per value.
    fn unpack_positions<T>(
        &self,
        packed: &[T],
        c: usize,
        part: std::ops::Range<usize>,
        dst: &mut [u64],
    ) where
        T: vortex_array::dtype::NativePType + fastlanes::BitPacking + Into<u64>,
    {
        use std::mem::MaybeUninit;
        let n = part.len();
        assert!(
            n <= PART_MAX && dst.len() == n,
            "a part of at most PART_MAX values"
        );
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
        for (o, v) in dst.iter_mut().zip(&values[..n]) {
            // SAFETY: the unpack wrote each of the first `n` slots.
            *o = unsafe { v.assume_init() }.into();
        }
    }

    /// The packed value at physical `index` of `packed`, widened.
    fn unpack<T>(&self, packed: &[T], index: usize) -> u64
    where
        T: vortex_array::dtype::NativePType + fastlanes::BitPacking + Into<u64>,
    {
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
        .into()
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

/// The most values a bulk read unpacks one by one, from a chunk or in all:
/// below about seven, fastlanes' batch unpack
/// ([`PackedNode::unpack_positions`]) costs more to set up than its single
/// bit-width dispatch saves. Measured with the probe bench's `read_into_*`
/// arms at 2, 4 and 8 values.
const UNPACK_ONE_BY_ONE: usize = 6;

/// The largest [`unpack_crossover`]: the most positions a bulk read unpacks
/// from one chunk.
const PART_MAX: usize = unpack_crossover::<u64>();

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
}
