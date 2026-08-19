//! Resolution of an encoded array into a probe tree.
//!
//! Resolution is downcasts and metadata reads only — no child is decoded and
//! no `ExecutionCtx` is created. Any node outside the supported set, any
//! nullable or non-unsigned-integer dtype, and any non-host buffer declines
//! the whole tree.

use vortex_array::ArrayRef;
use vortex_array::arrays::chunked::ChunkedArrayExt as _;
use vortex_array::arrays::dict::DictSlots;
use vortex_array::arrays::shared::SharedSlots;
use vortex_array::arrays::slice::SliceSlots;
use vortex_array::arrays::{Chunked, Constant, Dict, Primitive, Shared, Slice};
use vortex_array::dtype::PType;
use vortex_array::scalar::PValue;
use vortex_fastlanes::{
    BitPacked, BitPackedArrayExt as _, BitPackedSlots, FoR, FoRArrayExt as _, FoRSlots,
};
use vortex_runend::{RunEnd, RunEndArrayExt as _, RunEndSlots};
use vortex_sequence::Sequence;

use crate::node::{Chunk, Node, PackedNode, Words};
use crate::patches::PatchProbe;

/// Child array at a fixed slot index, `'a`-borrowed from the erased slots.
fn slot(slots: &[Option<ArrayRef>], idx: usize) -> Option<&ArrayRef> {
    slots.get(idx).and_then(|s| s.as_ref())
}

pub(crate) fn resolve_node<'a>(arr: &'a ArrayRef) -> Option<Node<'a>> {
    let dtype = arr.dtype();
    if !dtype.is_unsigned_int() || dtype.is_nullable() {
        return None;
    }

    if let Some(view) = arr.as_opt::<Primitive>() {
        let data = view.data();
        let words = match data.ptype() {
            PType::U8 => Words::U8(data.as_slice::<u8>()),
            PType::U16 => Words::U16(data.as_slice::<u16>()),
            PType::U32 => Words::U32(data.as_slice::<u32>()),
            PType::U64 => Words::U64(data.as_slice::<u64>()),
            _ => return None,
        };
        return Some(Node::Primitive(words));
    }

    if let Some(view) = arr.as_opt::<Constant>() {
        let value = u64::try_from(view.data().scalar()).ok()?;
        return Some(Node::Constant {
            value,
            len: arr.len(),
        });
    }

    if let Some(view) = arr.as_opt::<Sequence>() {
        let base = pvalue_u64(view.data().base())?;
        let multiplier = pvalue_u64(view.data().multiplier())?;
        let len = arr.len();
        return Some(if multiplier == 0 {
            Node::Constant { value: base, len }
        } else {
            Node::Sequence {
                base,
                multiplier,
                len,
            }
        });
    }

    if let Some(view) = arr.as_opt::<RunEnd>() {
        let slots = view.slots();
        let ends = resolve_node(slot(slots, RunEndSlots::ENDS)?)?;
        let values = resolve_node(slot(slots, RunEndSlots::VALUES)?)?;
        if ends.len() != values.len() {
            return None;
        }
        return Some(Node::RunEnd {
            ends: Box::new(ends),
            values: Box::new(values),
            offset: view.offset(),
            len: arr.len(),
        });
    }

    if let Some(view) = arr.as_opt::<FoR>() {
        let reference = u64::try_from(view.reference_scalar()).ok()?;
        let child = resolve_node(slot(view.slots(), FoRSlots::ENCODED)?)?;
        return Some(Node::FoR {
            reference,
            child: Box::new(child),
        });
    }

    if let Some(view) = arr.as_opt::<BitPacked>() {
        let data = view.data();
        let packed = match dtype.as_ptype() {
            PType::U8 => Words::U8(data.packed_slice::<u8>()),
            PType::U16 => Words::U16(data.packed_slice::<u16>()),
            PType::U32 => Words::U32(data.packed_slice::<u32>()),
            PType::U64 => Words::U64(data.packed_slice::<u64>()),
            _ => return None,
        };
        let slots = view.slots();
        if slot(slots, BitPackedSlots::VALIDITY_CHILD).is_some() {
            return None;
        }
        let patch_indices = slot(slots, BitPackedSlots::PATCH_INDICES);
        let patch_values = slot(slots, BitPackedSlots::PATCH_VALUES);
        let patches = match (patch_indices, patch_values) {
            (None, None) => None,
            (Some(indices), Some(values)) => Some(PatchProbe {
                indices: Box::new(resolve_node(indices)?),
                values: Box::new(resolve_node(values)?),
                // Patch indices hold absolute positions; the chunk-offsets
                // slot is only a lookup acceleration and needs no handling.
                offset: view.patches()?.offset(),
            }),
            _ => return None,
        };
        return Some(Node::BitPacked(PackedNode {
            packed,
            bit_width: usize::from(data.bit_width()),
            offset: usize::from(data.offset()),
            len: arr.len(),
            patches,
        }));
    }

    if let Some(view) = arr.as_opt::<Slice>() {
        let child = resolve_node(slot(view.slots(), SliceSlots::CHILD)?)?;
        let range = view.data().slice_range();
        return Some(Node::Slice {
            child: Box::new(child),
            start: range.start,
            len: range.end - range.start,
        });
    }

    if let Some(view) = arr.as_opt::<Dict>() {
        let slots = view.slots();
        // Codes resolve recursively (their own gate declines signed or
        // nullable codes); the construction invariant pins every code below
        // the values length.
        let codes = resolve_node(slot(slots, DictSlots::CODES)?)?;
        let values = resolve_node(slot(slots, DictSlots::VALUES)?)?;
        if codes.len() != arr.len() {
            return None;
        }
        return Some(Node::Dict {
            codes: Box::new(codes),
            values: Box::new(values),
        });
    }

    if let Some(view) = arr.as_opt::<Shared>() {
        // A transparent lazy wrapper, not an encoding: it delegates to its
        // source until something materializes the cached canonical of the
        // same logical values, so resolving the source answers either way.
        // The writer emits one around values children shared between chunks,
        // and declining it would decline the whole tree above it.
        return resolve_node(slot(view.slots(), SharedSlots::SOURCE)?);
    }

    if let Some(view) = arr.as_opt::<Chunked>() {
        let nchunks = view.nchunks();
        let offsets: Vec<usize> = view.chunk_offsets().to_vec();
        let slots = view.slots();
        let first_chunk_slot = slots.len() - nchunks;
        let mut chunks = Vec::with_capacity(nchunks);
        let mut i = 0usize;
        while i < nchunks {
            let child = slot(slots, first_chunk_slot + i)?;
            if child.is_empty() {
                i += 1;
                continue;
            }
            // Consecutive chunks that are contiguous slices of one shared
            // parent (a coalesced block read back split-by-split) merge into
            // a single window over that parent — fewer nodes to resolve and
            // one search instead of a per-chunk cascade.
            if let Some((parent, mut range)) = as_slice_parts(child) {
                let start = offsets[i];
                let mut j = i + 1;
                while j < nchunks {
                    let Some(next) = slot(slots, first_chunk_slot + j) else {
                        break;
                    };
                    let Some((next_parent, next_range)) = as_slice_parts(next) else {
                        break;
                    };
                    if next_parent.addr() != parent.addr() || next_range.start != range.end {
                        break;
                    }
                    range.end = next_range.end;
                    j += 1;
                }
                chunks.push(Chunk::new(
                    start,
                    Node::Slice {
                        child: Box::new(resolve_node(parent)?),
                        start: range.start,
                        len: range.end - range.start,
                    },
                ));
                i = j;
                continue;
            }
            chunks.push(Chunk::new(offsets[i], resolve_node(child)?));
            i += 1;
        }
        return Some(Node::Chunked {
            chunks,
            len: arr.len(),
        });
    }

    None
}

/// A slice child's `(parent, range)`, when `arr` is a slice wrapper.
fn as_slice_parts(arr: &ArrayRef) -> Option<(&ArrayRef, std::ops::Range<usize>)> {
    let view = arr.as_opt::<Slice>()?;
    let parent = slot(view.slots(), SliceSlots::CHILD)?;
    Some((parent, view.data().slice_range().clone()))
}

fn pvalue_u64(value: PValue) -> Option<u64> {
    match value {
        PValue::U8(v) => Some(u64::from(v)),
        PValue::U16(v) => Some(u64::from(v)),
        PValue::U32(v) => Some(u64::from(v)),
        PValue::U64(v) => Some(v),
        _ => None,
    }
}
