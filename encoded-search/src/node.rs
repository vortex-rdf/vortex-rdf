//! The resolved probe tree. A [`Node`] mirrors one encoded array node,
//! borrowing its buffers; every operation is a slice read, integer
//! arithmetic or one bit-packed word extraction, with no `ExecutionCtx`, no
//! decode and no allocation. The exception is [`DeltaNode`], whose values
//! are running sums: it decodes a 1024-value block the first time one of its
//! values is read, and keeps it.

use std::sync::OnceLock;

/// Values per FastLanes chunk: the stride of a chunked FoR's references and
/// of a run-length array's value offsets.
const FL_CHUNK: usize = vortex_fastlanes::FL_CHUNK_SIZE;

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

    /// First index whose value is `>= needle`, by the native monomorphized
    /// `partition_point`; the length for a needle past the width's maximum.
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
}

/// One resolved chunk of a [`Node::Chunked`] tree, never empty, with its
/// logical start row. Its last value is read when a bounds search first
/// probes the chunk, then kept, so resolution stays downcast-only and a
/// long-lived owned probe pays the read once.
pub(crate) struct Chunk<'a> {
    pub(crate) start: usize,
    pub(crate) node: Node<'a>,
    last: std::sync::OnceLock<u64>,
}

impl<'a> Chunk<'a> {
    pub(crate) fn new(start: usize, node: Node<'a>) -> Self {
        Self {
            start,
            node,
            last: std::sync::OnceLock::new(),
        }
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

/// One node of the resolved probe tree, a variant per supported encoding.
/// Children are resolved with their parent
/// ([`crate::SortedProbe::resolve`]), so every operation below is
/// infallible.
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
    /// Frame of reference with one reference per 1024-value FastLanes chunk
    /// (`fastlanes.for.v2`): `value_at(i) = child.value_at(i) +
    /// references.value_at((offset + i) / 1024)`, wrapping. A needle maps to
    /// a different encoded value in every chunk, so searches probe the
    /// composed values.
    ChunkedFoR {
        references: Box<Node<'a>>,
        offset: usize,
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
    /// `vortex.sparse`: `fill` everywhere except the patched positions.
    Sparse {
        fill: u64,
        patches: PatchProbe<'a>,
        len: usize,
    },
    /// FastLanes run-length (`fastlanes.rle`): each 1024-value chunk keeps
    /// its run values in `values`, from `value_offsets.value_at(chunk) -
    /// base`, and one index per position into them, so `value_at(i) =
    /// values.value_at(value_offsets.value_at(c) - base +
    /// indices.value_at(offset + i))` with `c = (offset + i) / 1024`.
    Rle {
        values: Box<Node<'a>>,
        indices: Box<Node<'a>>,
        value_offsets: Box<Node<'a>>,
        base: u64,
        offset: usize,
        len: usize,
    },
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
/// per lane and the transposed deltas (typically bit-packed beneath). A
/// value is a running sum down its lane, so a block is decoded whole the
/// first time any of its values is read, by the encoding's own `undelta`
/// and `untranspose` kernels, and kept for the node's lifetime: a bounds
/// search decodes only the blocks its bisection crosses.
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
    /// A node over resolved `bases` and `deltas` children; `None` for an
    /// `offset` of a whole block or more, or children too short for the
    /// blocks `offset + len` spans.
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
    // dtype), so the narrowing conversions cannot fail.
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

impl Node<'_> {
    pub(crate) fn len(&self) -> usize {
        match self {
            Node::Primitive(w) => w.len(),
            Node::Constant { len, .. }
            | Node::Sequence { len, .. }
            | Node::RunEnd { len, .. }
            | Node::Slice { len, .. }
            | Node::Chunked { len, .. }
            | Node::Sparse { len, .. }
            | Node::Rle { len, .. } => *len,
            Node::FoR { child, .. } | Node::ChunkedFoR { child, .. } => child.len(),
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
            Node::ChunkedFoR {
                references,
                offset,
                child,
            } => child
                .value_at(i)
                .wrapping_add(references.value_at((offset + i) / FL_CHUNK)),
            Node::BitPacked(p) => p.value_at(i),
            Node::Slice { child, start, .. } => child.value_at(start + i),
            Node::Chunked { chunks, .. } => {
                let c = partition(chunks.len(), |c| chunks[c].start <= i) - 1;
                chunks[c].node.value_at(i - chunks[c].start)
            }
            // A code is below the values' length (a construction invariant),
            // so it fits a `usize`.
            Node::Dict { codes, values } => values.value_at(codes.value_at(i) as usize),
            Node::Delta(d) => d.value_at(i),
            Node::Sparse { fill, patches, .. } => patches.lookup(i).unwrap_or(*fill),
            // Offsets and indices point inside `values` (a construction
            // invariant, as for a dictionary's codes).
            Node::Rle {
                values,
                indices,
                value_offsets,
                base,
                offset,
                ..
            } => {
                let pos = offset + i;
                let first = value_offsets.value_at(pos / FL_CHUNK) - base;
                values.value_at((first + indices.value_at(pos)) as usize)
            }
        }
    }

    /// First index whose value is `>= needle`, the length when every value is
    /// below it; requires ascending order.
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
                let k = search_ascending(span, |k| values.value_at(first + k), needle);
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
            Node::ChunkedFoR { .. } | Node::Sparse { .. } | Node::Rle { .. } => {
                partition(self.len(), |i| self.value_at(i) < needle)
            }
        }
    }

    /// First index whose value is `> needle`: values are integers, so it is
    /// the lower bound of `needle + 1`, and the length when `needle` is
    /// `u64::MAX`. Requires ascending order.
    pub(crate) fn upper_bound(&self, needle: u64) -> usize {
        needle
            .checked_add(1)
            .map_or(self.len(), |next| self.lower_bound(next))
    }

    /// `(lower_bound, upper_bound)` of `needle`; requires ascending order. A
    /// RunEnd node finds the needle's run once and reads both ends of it from
    /// `ends`, where the two bounds would each search the runs again.
    #[inline]
    pub(crate) fn bounds(&self, needle: u64) -> (usize, usize) {
        match self {
            Node::RunEnd {
                ends,
                values,
                offset,
                len,
            } => runend_bounds(ends, values, *offset, *len, needle),
            _ => (self.lower_bound(needle), self.upper_bound(needle)),
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
            Node::ChunkedFoR {
                references, child, ..
            } => {
                out.push(K::FoR);
                child.collect_kinds(out);
                references.collect_kinds(out);
            }
            Node::Sparse { patches, .. } => {
                out.push(K::Sparse);
                patches.indices.collect_kinds(out);
                patches.values.collect_kinds(out);
            }
            Node::Rle {
                values,
                indices,
                value_offsets,
                ..
            } => {
                out.push(K::Rle);
                values.collect_kinds(out);
                indices.collect_kinds(out);
                value_offsets.collect_kinds(out);
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

/// [`Node::bounds`] of a RunEnd node: the needle's first run by one search of
/// the window's run values, both bounds from `ends`.
fn runend_bounds(
    ends: &Node<'_>,
    values: &Node<'_>,
    offset: usize,
    len: usize,
    needle: u64,
) -> (usize, usize) {
    let (first, span) = window_runs(ends, offset, len);
    let k = search_ascending(span, |k| values.value_at(first + k), needle);
    // Adjacent runs may repeat a value, so the run past the needle's is
    // searched for unless the next run already holds a larger value.
    let past = if k == span || values.value_at(first + k) != needle {
        k
    } else if k + 1 == span || values.value_at(first + k + 1) > needle {
        k + 1
    } else {
        k + 1
            + partition(span - k - 1, |j| {
                values.value_at(first + k + 1 + j) <= needle
            })
    };
    (
        run_start(ends, first + k, offset, len),
        run_start(ends, first + past, offset, len),
    )
}

/// The runs overlapping the window `[offset, offset + len)`, as
/// `(first_run, run_count)`. Sortedness is asserted for the window only, so
/// run searches must stay inside these runs: a sliced RunEnd keeps its full
/// children, whose out-of-window values are in unspecified order.
fn window_runs(ends: &Node<'_>, offset: usize, len: usize) -> (usize, usize) {
    if len == 0 {
        return (0, 0);
    }
    let nruns = ends.len();
    // An unsliced node: its last run ends at its length, so every run lies in
    // the window (the ends ascend).
    if offset == 0 && nruns > 0 && ends.value_at(nruns - 1) == len as u64 {
        return (0, nruns);
    }
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
    fn runend_bounds_search_once_and_cross_repeated_values() {
        // ends [2, 3, 6, 7, 9, 12] over values [1, 4, 4, 4, 7, 9]: adjacent
        // runs repeat 4, which a RunEnd may hold although a writer merges
        // them. Logical rows: [1, 1, 4, 4, 4, 4, 4, 7, 7, 9, 9, 9].
        let ends = [2u64, 3, 6, 7, 9, 12];
        let values = [1u64, 4, 4, 4, 7, 9];
        let rows: Vec<u64> = values
            .iter()
            .zip(ends.iter())
            .scan(0u64, |start, (&v, &end)| {
                let run = (*start..end).map(move |_| v);
                *start = end;
                Some(run)
            })
            .flatten()
            .collect();
        // Unsliced, then three windows (one a single run, one ending mid-run).
        for (offset, len) in [(0usize, 12usize), (2, 6), (3, 4), (8, 1), (5, 0)] {
            let n = Node::RunEnd {
                ends: Box::new(Node::Primitive(Words::U64(&ends))),
                values: Box::new(Node::Primitive(Words::U64(&values))),
                offset,
                len,
            };
            let window = &rows[offset..offset + len];
            for needle in (0..=10).chain([u64::MAX]) {
                let want = (
                    window.partition_point(|&v| v < needle),
                    window.partition_point(|&v| v <= needle),
                );
                assert_eq!(
                    n.bounds(needle),
                    want,
                    "offset {offset} len {len} needle {needle}"
                );
                assert_eq!(
                    (n.lower_bound(needle), n.upper_bound(needle)),
                    want,
                    "offset {offset} len {len} needle {needle}"
                );
            }
        }
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
