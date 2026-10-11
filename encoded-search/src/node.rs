//! The resolved probe tree and its search/access operations.
//!
//! A [`Node`] mirrors one encoded array node, borrowing its buffers; every
//! operation is a slice read, integer arithmetic, or a single bit-packed word
//! extraction — no `ExecutionCtx`, no decoding, no allocation. The one
//! exception is [`DeltaNode`], whose values are running sums: it decodes a
//! 1024-value block the first time one of its values is read, and keeps it.

use std::sync::OnceLock;

/// Typed borrow of a canonical primitive buffer, one variant per unsigned width.
pub(crate) enum Words<'a> {
    U8(&'a [u8]),
    U16(&'a [u16]),
    U32(&'a [u32]),
    U64(&'a [u64]),
}

impl Words<'_> {
    pub(crate) fn len(&self) -> usize {
        match self {
            Words::U8(s) => s.len(),
            Words::U16(s) => s.len(),
            Words::U32(s) => s.len(),
            Words::U64(s) => s.len(),
        }
    }

    pub(crate) fn get(&self, i: usize) -> u64 {
        match self {
            Words::U8(s) => u64::from(s[i]),
            Words::U16(s) => u64::from(s[i]),
            Words::U32(s) => u64::from(s[i]),
            Words::U64(s) => s[i],
        }
    }

    /// First index whose value is `>= needle`, via the native monomorphized
    /// `partition_point`. A needle above the width's maximum finds nothing.
    fn lower_bound(&self, needle: u64) -> usize {
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

    /// First index whose value is `> needle`.
    fn upper_bound(&self, needle: u64) -> usize {
        fn bound<T: Copy + Ord + TryFrom<u64>>(s: &[T], needle: u64) -> usize {
            match T::try_from(needle) {
                Ok(n) => s.partition_point(|&v| v <= n),
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

/// One resolved chunk of a [`Node::Chunked`] tree, with its logical start
/// row. Chunk extremes are read on demand during bounds searches and then
/// memoized — resolving stays downcast-only (a borrowed probe is resolved
/// per call), while a long-lived owned probe pays each extreme once.
pub(crate) struct Chunk<'a> {
    pub(crate) start: usize,
    pub(crate) node: Node<'a>,
    first: std::sync::OnceLock<u64>,
    last: std::sync::OnceLock<u64>,
}

impl<'a> Chunk<'a> {
    pub(crate) fn new(start: usize, node: Node<'a>) -> Self {
        Self {
            start,
            node,
            first: std::sync::OnceLock::new(),
            last: std::sync::OnceLock::new(),
        }
    }

    /// The chunk's first value, read once.
    fn first(&self) -> u64 {
        *self.first.get_or_init(|| self.node.value_at(0))
    }

    /// The chunk's last value, read once.
    fn last(&self) -> u64 {
        *self
            .last
            .get_or_init(|| self.node.value_at(self.node.len() - 1))
    }
}

/// Exception values of a bit-packed array, resolved as probe nodes: `indices`
/// holds strictly increasing patched positions (searchable), `values` the
/// replacement values (point access only, so its sort order never matters).
pub(crate) struct PatchProbe<'a> {
    pub(crate) indices: Box<Node<'a>>,
    pub(crate) values: Box<Node<'a>>,
    pub(crate) offset: usize,
}

impl PatchProbe<'_> {
    /// Replacement value for logical index `i`, if that position is patched.
    pub(crate) fn lookup(&self, i: usize) -> Option<u64> {
        let key = (i + self.offset) as u64;
        let at = self.indices.lower_bound(key);
        (at < self.indices.len() && self.indices.value_at(at) == key)
            .then(|| self.values.value_at(at))
    }
}

/// Bit-packed words plus the packing parameters needed for point extraction.
pub(crate) struct PackedNode<'a> {
    pub(crate) packed: Words<'a>,
    pub(crate) bit_width: usize,
    pub(crate) offset: usize,
    pub(crate) len: usize,
    pub(crate) patches: Option<PatchProbe<'a>>,
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
        // SAFETY: the packed buffer length is pinned to
        // `(len + offset).div_ceil(1024) * 128 * bit_width` words at array
        // construction, and callers keep `i < len`, so `i + offset` is within
        // the length the buffer was packed for.
        match &self.packed {
            Words::U8(s) => u64::from(unsafe {
                vortex_fastlanes::bitpack_decompress::unpack_single_primitive::<u8>(
                    s,
                    self.bit_width,
                    i + self.offset,
                )
            }),
            Words::U16(s) => u64::from(unsafe {
                vortex_fastlanes::bitpack_decompress::unpack_single_primitive::<u16>(
                    s,
                    self.bit_width,
                    i + self.offset,
                )
            }),
            Words::U32(s) => u64::from(unsafe {
                vortex_fastlanes::bitpack_decompress::unpack_single_primitive::<u32>(
                    s,
                    self.bit_width,
                    i + self.offset,
                )
            }),
            Words::U64(s) => unsafe {
                vortex_fastlanes::bitpack_decompress::unpack_single_primitive::<u64>(
                    s,
                    self.bit_width,
                    i + self.offset,
                )
            },
        }
    }
}

/// One node of the resolved probe tree. Variants mirror the supported
/// encodings; children are resolved recursively at [`crate::SortedProbe::resolve`]
/// time so every operation below is non-fallible.
pub(crate) enum Node<'a> {
    Primitive(Words<'a>),
    Constant {
        value: u64,
        len: usize,
    },
    /// `value_at(i) = base + i * multiplier`, `multiplier > 0` (a zero
    /// multiplier resolves to `Constant`).
    Sequence {
        base: u64,
        multiplier: u64,
        len: usize,
    },
    RunEnd {
        ends: Box<Node<'a>>,
        values: Box<Node<'a>>,
        offset: usize,
        len: usize,
    },
    FoR {
        reference: u64,
        child: Box<Node<'a>>,
    },
    BitPacked(PackedNode<'a>),
    Slice {
        child: Box<Node<'a>>,
        start: usize,
        len: usize,
    },
    Chunked {
        chunks: Vec<Chunk<'a>>,
        len: usize,
    },
    /// `value_at(i) = values.value_at(codes.value_at(i))`. Dictionary values
    /// are not order-preserving, so bounds searches probe the composed
    /// logical values rather than the values child.
    Dict {
        codes: Box<Node<'a>>,
        values: Box<Node<'a>>,
    },
    /// FastLanes delta encoding, decoded one 1024-value block at a time on
    /// first touch (see [`DeltaNode`]).
    Delta(DeltaNode<'a>),
}

/// The integer width a FastLanes delta block is decoded in: the wrapping
/// arithmetic that undoes the deltas runs in the stored type, so the width
/// is part of the node, not of the `u64` values it answers.
#[derive(Clone, Copy)]
pub(crate) enum DeltaWidth {
    U8,
    U16,
    U32,
    U64,
}

impl DeltaWidth {
    /// Lanes per 1024-value block: how many bases a block carries.
    fn lanes(self) -> usize {
        match self {
            DeltaWidth::U8 => 128,
            DeltaWidth::U16 => 64,
            DeltaWidth::U32 => 32,
            DeltaWidth::U64 => 16,
        }
    }
}

/// A FastLanes delta-encoded node: every 1024-value block stores one base
/// per lane and the transposed, delta-encoded values (typically bit-packed
/// beneath). A value cannot be read in place — it is a running sum down its
/// lane — so a block is decoded whole the first time any of its values is
/// asked for, through the same `undelta` and `untranspose` kernels the
/// encoding's own decompressor runs, and kept for the node's lifetime. A
/// bounds search therefore decodes the handful of blocks its bisection
/// crosses, never the chunk.
pub(crate) struct DeltaNode<'a> {
    bases: Box<Node<'a>>,
    deltas: Box<Node<'a>>,
    width: DeltaWidth,
    /// Physical offset of the first logical value inside the first block
    /// (`< 1024`, a sliced array's leading rows).
    offset: usize,
    len: usize,
    /// Decoded blocks, filled on first touch; an untouched slot costs one
    /// pointer.
    blocks: Vec<OnceLock<Box<[u64; 1024]>>>,
}

impl<'a> DeltaNode<'a> {
    /// A node over resolved `bases` and `deltas` children; `None` when the
    /// children are too short for the blocks `offset + len` spans.
    pub(crate) fn new(
        bases: Node<'a>,
        deltas: Node<'a>,
        width: DeltaWidth,
        offset: usize,
        len: usize,
    ) -> Option<Self> {
        if offset >= 1024 {
            return None;
        }
        let blocks = (offset + len).div_ceil(1024);
        if deltas.len() < blocks * 1024 || bases.len() < blocks * width.lanes() {
            return None;
        }
        Some(Self {
            bases: Box::new(bases),
            deltas: Box::new(deltas),
            width,
            offset,
            len,
            blocks: (0..blocks).map(|_| OnceLock::new()).collect(),
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    /// Exact logical value at `i`; requires `i < self.len()`.
    pub(crate) fn value_at(&self, i: usize) -> u64 {
        let physical = self.offset + i;
        self.block(physical / 1024)[physical % 1024]
    }

    fn block(&self, b: usize) -> &[u64; 1024] {
        self.blocks[b].get_or_init(|| {
            Box::new(match self.width {
                DeltaWidth::U8 => decode_block::<u8, 128>(&self.bases, &self.deltas, b),
                DeltaWidth::U16 => decode_block::<u16, 64>(&self.bases, &self.deltas, b),
                DeltaWidth::U32 => decode_block::<u32, 32>(&self.bases, &self.deltas, b),
                DeltaWidth::U64 => decode_block::<u64, 16>(&self.bases, &self.deltas, b),
            })
        })
    }

    pub(crate) fn collect_kinds(&self, out: &mut Vec<crate::NodeKind>) {
        out.push(crate::NodeKind::Delta);
        self.bases.collect_kinds(out);
        self.deltas.collect_kinds(out);
    }
}

/// Decode block `b` of a delta node in the stored width `T`: gather the
/// block's transposed deltas and its lane bases through the children's
/// point reads, run the FastLanes kernels, and widen the result.
fn decode_block<T, const LANES: usize>(bases: &Node<'_>, deltas: &Node<'_>, b: usize) -> [u64; 1024]
where
    T: fastlanes::Delta + fastlanes::Transpose + Copy + Default + Into<u64> + TryFrom<u64>,
{
    // The children hold values of width `T` by construction (the array's
    // dtype), so the narrowing conversions cannot fail; a child that lied
    // about its width would already have declined resolution.
    let narrow = |v: u64| T::try_from(v).unwrap_or_default();
    let mut chunk = [T::default(); 1024];
    for (j, slot) in chunk.iter_mut().enumerate() {
        *slot = narrow(deltas.value_at(b * 1024 + j));
    }
    let mut lane_bases = [T::default(); LANES];
    for (l, slot) in lane_bases.iter_mut().enumerate() {
        *slot = narrow(bases.value_at(b * LANES + l));
    }
    let mut transposed = [T::default(); 1024];
    T::undelta::<LANES>(&chunk, &lane_bases, &mut transposed);
    let mut decoded = [T::default(); 1024];
    T::untranspose(&transposed, &mut decoded);
    let mut wide = [0u64; 1024];
    for (w, v) in wide.iter_mut().zip(decoded) {
        *w = v.into();
    }
    wide
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

impl Node<'_> {
    pub(crate) fn len(&self) -> usize {
        match self {
            Node::Primitive(w) => w.len(),
            Node::Constant { len, .. }
            | Node::Sequence { len, .. }
            | Node::RunEnd { len, .. }
            | Node::Slice { len, .. }
            | Node::Chunked { len, .. } => *len,
            Node::FoR { child, .. } => child.len(),
            Node::BitPacked(p) => p.len,
            Node::Dict { codes, .. } => codes.len(),
            Node::Delta(d) => d.len(),
        }
    }

    /// Exact logical value at `i`; requires `i < self.len()` but no sortedness.
    pub(crate) fn value_at(&self, i: usize) -> u64 {
        match self {
            Node::Primitive(w) => w.get(i),
            Node::Constant { value, .. } => *value,
            Node::Sequence {
                base, multiplier, ..
            } => (u128::from(*base) + i as u128 * u128::from(*multiplier)) as u64,
            Node::RunEnd {
                ends,
                values,
                offset,
                ..
            } => {
                let pos = (i + offset) as u64;
                let run = partition(ends.len(), |r| ends.value_at(r) <= pos);
                values.value_at(run)
            }
            Node::FoR { reference, child } => reference.saturating_add(child.value_at(i)),
            Node::BitPacked(p) => p.value_at(i),
            Node::Slice { child, start, .. } => child.value_at(start + i),
            Node::Chunked { chunks, .. } => {
                let c = partition(chunks.len(), |c| chunks[c].start <= i) - 1;
                chunks[c].node.value_at(i - chunks[c].start)
            }
            Node::Dict { codes, values } => values.value_at(codes.value_at(i) as usize),
            Node::Delta(d) => d.value_at(i),
        }
    }

    /// First index whose value is `>= needle`; requires ascending order.
    pub(crate) fn lower_bound(&self, needle: u64) -> usize {
        match self {
            Node::Primitive(w) => w.lower_bound(needle),
            Node::Constant { value, len } => {
                if *value < needle {
                    *len
                } else {
                    0
                }
            }
            Node::Sequence {
                base,
                multiplier,
                len,
            } => {
                if needle <= *base {
                    0
                } else {
                    clamp_to_len((needle - *base).div_ceil(*multiplier), *len)
                }
            }
            Node::RunEnd {
                ends,
                values,
                offset,
                len,
            } => {
                let (first, span) = window_runs(ends, *offset, *len);
                let k = partition(span, |k| values.value_at(first + k) < needle);
                run_start(ends, first + k, *offset, *len)
            }
            Node::FoR { reference, child } => {
                // Every stored value is `reference + encoded >= reference`.
                if needle <= *reference {
                    0
                } else {
                    child.lower_bound(needle - *reference)
                }
            }
            Node::BitPacked(p) => partition(p.len, |i| p.value_at(i) < needle),
            // Sortedness is asserted for the window only, so the search reads
            // through the window, never the child's own order.
            Node::Slice { child, start, len } => {
                partition(*len, |i| child.value_at(*start + i) < needle)
            }
            Node::Chunked { chunks, len } => {
                // First chunk whose last value reaches the needle holds the
                // boundary.
                let c = partition(chunks.len(), |c| chunks[c].last() < needle);
                if c == chunks.len() {
                    *len
                } else {
                    chunks[c].start + chunks[c].node.lower_bound(needle)
                }
            }
            Node::Dict { codes, values } => partition(codes.len(), |i| {
                values.value_at(codes.value_at(i) as usize) < needle
            }),
            // A bisection touches O(log blocks) blocks, each decoded once.
            Node::Delta(d) => partition(d.len(), |i| d.value_at(i) < needle),
        }
    }

    /// First index whose value is `> needle`; requires ascending order.
    pub(crate) fn upper_bound(&self, needle: u64) -> usize {
        match self {
            Node::Primitive(w) => w.upper_bound(needle),
            Node::Constant { value, len } => {
                if *value <= needle {
                    *len
                } else {
                    0
                }
            }
            Node::Sequence {
                base,
                multiplier,
                len,
            } => {
                if needle < *base {
                    0
                } else {
                    // `q + 1` values are `<= needle`; saturating keeps a
                    // quotient of `u64::MAX` past every position.
                    clamp_to_len(((needle - *base) / *multiplier).saturating_add(1), *len)
                }
            }
            Node::RunEnd {
                ends,
                values,
                offset,
                len,
            } => {
                let (first, span) = window_runs(ends, *offset, *len);
                let k = partition(span, |k| values.value_at(first + k) <= needle);
                run_start(ends, first + k, *offset, *len)
            }
            Node::FoR { reference, child } => {
                if needle < *reference {
                    0
                } else {
                    child.upper_bound(needle - *reference)
                }
            }
            Node::BitPacked(p) => partition(p.len, |i| p.value_at(i) <= needle),
            // Window-only search; see `lower_bound`.
            Node::Slice { child, start, len } => {
                partition(*len, |i| child.value_at(*start + i) <= needle)
            }
            Node::Chunked { chunks, .. } => {
                let c = partition(chunks.len(), |c| chunks[c].first() <= needle);
                if c == 0 {
                    0
                } else {
                    chunks[c - 1].start + chunks[c - 1].node.upper_bound(needle)
                }
            }
            Node::Dict { codes, values } => partition(codes.len(), |i| {
                values.value_at(codes.value_at(i) as usize) <= needle
            }),
            Node::Delta(d) => partition(d.len(), |i| d.value_at(i) <= needle),
        }
    }

    pub(crate) fn collect_kinds(&self, out: &mut Vec<crate::NodeKind>) {
        use crate::NodeKind as K;
        match self {
            Node::Primitive(_) => out.push(K::Primitive),
            Node::Constant { .. } => out.push(K::Constant),
            Node::Sequence { .. } => out.push(K::Sequence),
            Node::RunEnd { ends, values, .. } => {
                out.push(K::RunEnd);
                ends.collect_kinds(out);
                values.collect_kinds(out);
            }
            Node::FoR { child, .. } => {
                out.push(K::FoR);
                child.collect_kinds(out);
            }
            Node::BitPacked(p) => {
                out.push(K::BitPacked);
                if let Some(patches) = &p.patches {
                    out.push(K::Patches);
                    patches.indices.collect_kinds(out);
                    patches.values.collect_kinds(out);
                }
            }
            Node::Slice { child, .. } => {
                out.push(K::Slice);
                child.collect_kinds(out);
            }
            Node::Chunked { chunks, .. } => {
                out.push(K::Chunked);
                for c in chunks {
                    c.node.collect_kinds(out);
                }
            }
            Node::Dict { codes, values } => {
                out.push(K::Dict);
                codes.collect_kinds(out);
                values.collect_kinds(out);
            }
            Node::Delta(d) => d.collect_kinds(out),
        }
    }
}

/// Position `q` among `len` positions, clamped to `len`. A quotient the index
/// type cannot hold lies past every position: on a 32-bit target a `u64`
/// quotient above `u32::MAX` clamps rather than wrapping to its low bits.
/// Generic over the index type so a 64-bit host can test the narrow case.
fn clamp_to_len<I: TryFrom<u64> + Ord + Copy>(q: u64, len: I) -> I {
    I::try_from(q).map_or(len, |n| n.min(len))
}

/// The runs overlapping the window `[offset, offset + len)`, as
/// `(first_run, run_count)`. Sortedness is asserted for the window only, so
/// run searches must stay inside these runs — a sliced RunEnd keeps its full
/// children, whose out-of-window values are in unspecified order.
fn window_runs(ends: &Node<'_>, offset: usize, len: usize) -> (usize, usize) {
    if len == 0 {
        return (0, 0);
    }
    let nruns = ends.len();
    let first = partition(nruns, |r| ends.value_at(r) <= offset as u64);
    let last = partition(nruns, |r| ends.value_at(r) <= (offset + len - 1) as u64);
    (first, (last + 1).min(nruns) - first)
}

/// First row of run `run` in the sliced coordinate space: runs before it end at
/// `ends[run - 1] - offset`, clamped into `[0, len]` so a moved offset or a
/// shortened length (a sliced RunEnd keeps its full ends child) stays in range.
fn run_start(ends: &Node<'_>, run: usize, offset: usize, len: usize) -> usize {
    if run == 0 {
        0
    } else {
        (ends.value_at(run - 1) as usize)
            .saturating_sub(offset)
            .min(len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seq(base: u64, multiplier: u64, len: usize) -> Node<'static> {
        Node::Sequence {
            base,
            multiplier,
            len,
        }
    }

    #[test]
    fn sequence_bounds_closed_form() {
        // 5, 8, 11, 14
        let n = seq(5, 3, 4);
        for (needle, lo, hi) in [
            (0, 0, 0),
            (4, 0, 0),
            (5, 0, 1),
            (6, 1, 1),
            (8, 1, 2),
            (13, 3, 3),
            (14, 3, 4),
            (15, 4, 4),
            (u64::MAX, 4, 4),
        ] {
            assert_eq!(n.lower_bound(needle), lo, "lower({needle})");
            assert_eq!(n.upper_bound(needle), hi, "upper({needle})");
        }
        assert_eq!(n.value_at(2), 11);
    }

    #[test]
    fn quotients_past_the_index_width_clamp_to_len() {
        // u32 stands in for a 32-bit target's usize: a quotient past
        // u32::MAX is past every position, never its low bits.
        let len = 10u32;
        for q in [1u64 << 32, (1 << 32) + 3, (1 << 33) + 9, u64::MAX] {
            assert_eq!(clamp_to_len(q, len), len, "q = {q}");
        }
        assert_eq!(clamp_to_len(3, len), 3);
        assert_eq!(clamp_to_len(u64::from(u32::MAX), len), len);
        // The upper bound's `q + 1` past u32::MAX clamps too.
        assert_eq!(
            clamp_to_len(u64::from(u32::MAX).saturating_add(1), len),
            len
        );
        // The native width.
        assert_eq!(clamp_to_len(u64::MAX, 7usize), 7);
        assert_eq!(clamp_to_len(4, 7usize), 4);
    }

    #[test]
    fn sequence_value_at_wide_domain() {
        let n = seq(u64::MAX - 10, 2, 6);
        assert_eq!(n.value_at(5), u64::MAX);
    }

    #[test]
    fn constant_bounds() {
        let n = Node::Constant { value: 7, len: 3 };
        assert_eq!((n.lower_bound(6), n.upper_bound(6)), (0, 0));
        assert_eq!((n.lower_bound(7), n.upper_bound(7)), (0, 3));
        assert_eq!((n.lower_bound(8), n.upper_bound(8)), (3, 3));
    }

    #[test]
    fn for_underflow_guard() {
        // reference 100 over [0, 0, 2] -> logical [100, 100, 102]
        let child = Node::Primitive(Words::U64(&[0, 0, 2]));
        let n = Node::FoR {
            reference: 100,
            child: Box::new(child),
        };
        assert_eq!((n.lower_bound(50), n.upper_bound(50)), (0, 0));
        assert_eq!((n.lower_bound(100), n.upper_bound(100)), (0, 2));
        assert_eq!((n.lower_bound(101), n.upper_bound(101)), (2, 2));
        assert_eq!((n.lower_bound(102), n.upper_bound(102)), (2, 3));
        assert_eq!(n.value_at(2), 102);
    }

    #[test]
    fn slice_clamp_rebase() {
        // child [1, 3, 3, 5, 7], window [1, 4) -> [3, 3, 5]
        let child = Node::Primitive(Words::U32(&[1, 3, 3, 5, 7]));
        let n = Node::Slice {
            child: Box::new(child),
            start: 1,
            len: 3,
        };
        assert_eq!((n.lower_bound(0), n.upper_bound(0)), (0, 0));
        assert_eq!((n.lower_bound(3), n.upper_bound(3)), (0, 2));
        assert_eq!((n.lower_bound(5), n.upper_bound(5)), (2, 3));
        assert_eq!((n.lower_bound(9), n.upper_bound(9)), (3, 3));
        assert_eq!(n.value_at(2), 5);
    }

    #[test]
    fn runend_offset_clamp() {
        // ends [4, 8, 10] over values [2, 5, 9]; sliced view offset=2 len=6
        // logical rows: positions 2..8 -> [2, 2, 5, 5, 5, 5]
        let ends = Node::Primitive(Words::U64(&[4, 8, 10]));
        let values = Node::Primitive(Words::U64(&[2, 5, 9]));
        let n = Node::RunEnd {
            ends: Box::new(ends),
            values: Box::new(values),
            offset: 2,
            len: 6,
        };
        assert_eq!((n.lower_bound(2), n.upper_bound(2)), (0, 2));
        assert_eq!((n.lower_bound(5), n.upper_bound(5)), (2, 6));
        assert_eq!((n.lower_bound(9), n.upper_bound(9)), (6, 6));
        assert_eq!(n.value_at(0), 2);
        assert_eq!(n.value_at(2), 5);
        assert_eq!(n.value_at(5), 5);
    }

    #[test]
    fn chunked_boundary_selection() {
        // chunks [1, 2, 2] and [2, 2, 3]: an equal run spans the boundary
        let a = Node::Primitive(Words::U32(&[1, 2, 2]));
        let b = Node::Primitive(Words::U32(&[2, 2, 3]));
        let n = Node::Chunked {
            chunks: vec![Chunk::new(0, a), Chunk::new(3, b)],
            len: 6,
        };
        assert_eq!((n.lower_bound(2), n.upper_bound(2)), (1, 5));
        assert_eq!((n.lower_bound(1), n.upper_bound(1)), (0, 1));
        assert_eq!((n.lower_bound(3), n.upper_bound(3)), (5, 6));
        assert_eq!((n.lower_bound(4), n.upper_bound(4)), (6, 6));
        assert_eq!(n.value_at(4), 2);
        assert_eq!(n.value_at(5), 3);
    }

    #[test]
    fn words_needle_above_width_max() {
        let n = Node::Primitive(Words::U8(&[1, 2, 3]));
        assert_eq!(n.lower_bound(300), 3);
        assert_eq!(n.upper_bound(300), 3);
    }

    #[test]
    fn dict_bounds_through_permuted_values() {
        // codes [1, 1, 3, 3, 0, 2, 2] over values [5, 1, 7, 2]
        // -> logical [1, 1, 2, 2, 5, 7, 7] (sorted; values are not)
        let codes = Node::Primitive(Words::U8(&[1, 1, 3, 3, 0, 2, 2]));
        let values = Node::Primitive(Words::U32(&[5, 1, 7, 2]));
        let n = Node::Dict {
            codes: Box::new(codes),
            values: Box::new(values),
        };
        assert_eq!(n.len(), 7);
        assert_eq!((n.lower_bound(0), n.upper_bound(0)), (0, 0));
        assert_eq!((n.lower_bound(1), n.upper_bound(1)), (0, 2));
        assert_eq!((n.lower_bound(2), n.upper_bound(2)), (2, 4));
        assert_eq!((n.lower_bound(3), n.upper_bound(3)), (4, 4));
        assert_eq!((n.lower_bound(5), n.upper_bound(5)), (4, 5));
        assert_eq!((n.lower_bound(7), n.upper_bound(7)), (5, 7));
        assert_eq!((n.lower_bound(8), n.upper_bound(8)), (7, 7));
        assert_eq!(n.value_at(4), 5);
        assert_eq!(n.value_at(6), 7);
    }
}
