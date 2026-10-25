//! FastLanes delta nodes: a value is a running sum down its lane, so a
//! 1024-value block is decoded whole on first touch and kept.

use std::sync::OnceLock;

use crate::node::Node;

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
