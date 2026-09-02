//! The live canonical form of an encoded base's code columns.
//!
//! A base adopted from bytes or a file keeps the writer's encodings, so a
//! contiguous wide code read has no canonical buffer to hand out. This cache
//! decodes a column on demand and holds it *weakly*: every buffer it hands
//! out keeps the decoded column alive, readers that arrive while one is alive
//! share it zero-copy, and the column is freed with its last holder. A base
//! that is never read wide, or whose readers have all dropped, holds nothing
//! here — the cache is bounded by what is live, never by what was read.

use std::sync::{Arc, Mutex, PoisonError, Weak};

use bytes::Bytes;
use vortex_array::arrays::PrimitiveArray;
use vortex_array::{ArrayRef, VortexSessionExecute};
use vortex_buffer::{Alignment, Buffer};

use crate::error::{Result, VortexRdfError};
use crate::session::VORTEX_SESSION;
use crate::store::schema;

/// Per-base weak slots, one per primary column in
/// [`PRIMARY_COLUMNS`](schema::PRIMARY_COLUMNS) order; shared by every view
/// over the base (`Arc` in `QuadsSource::InMemory`), like the probes.
pub(crate) struct LiveCanonical {
    slots: [Mutex<Weak<Buffer<u32>>>; schema::PRIMARY_COLUMNS.len()],
}

/// The owner a handed-out buffer keeps alive: the decoded column behind an
/// `Arc`, exposed as bytes for `Bytes::from_owner`.
struct Held(Arc<Buffer<u32>>);

impl AsRef<[u8]> for Held {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl LiveCanonical {
    /// A fresh cache holding nothing, shared (`Arc`) because every
    /// `QuadsSource::InMemory` holds one by `Arc`.
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            slots: std::array::from_fn(|_| Mutex::new(Weak::new())),
        })
    }

    /// Column `idx`'s canonical `u32` buffer: shared with every holder
    /// currently alive, else decoded from `col` now. The returned buffer, and
    /// every slice taken from it, keeps the decoded column alive.
    pub(crate) fn column(&self, idx: usize, col: &ArrayRef) -> Result<Buffer<u32>> {
        let mut slot = self.slots[idx].lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(owner) = slot.upgrade() {
            return Ok(Self::handle(owner));
        }
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let prim = col
            .clone()
            .execute::<PrimitiveArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?;
        if prim.ptype() != vortex_array::dtype::PType::U32 {
            return Err(VortexRdfError::InvalidOperation(format!(
                "code column {} is {}, not u32",
                schema::PRIMARY_COLUMNS[idx],
                prim.ptype()
            )));
        }
        let owner = Arc::new(prim.into_buffer::<u32>());
        *slot = Arc::downgrade(&owner);
        Ok(Self::handle(owner))
    }

    /// Column `idx`'s canonical buffer only if some holder keeps it alive;
    /// never decodes.
    pub(crate) fn column_if_alive(&self, idx: usize) -> Option<Buffer<u32>> {
        let slot = self.slots[idx].lock().unwrap_or_else(PoisonError::into_inner);
        slot.upgrade().map(Self::handle)
    }

    /// Whether column `idx`'s decoded form is currently held by some reader.
    #[cfg(test)]
    pub(crate) fn is_alive(&self, idx: usize) -> bool {
        let slot = self.slots[idx].lock().unwrap_or_else(PoisonError::into_inner);
        slot.strong_count() > 0
    }

    /// A buffer over the owner's bytes that holds the owner: `Bytes::from_owner`
    /// keeps the `Arc` inside the buffer's shared data, so clones and slices
    /// all count as holders.
    fn handle(owner: Arc<Buffer<u32>>) -> Buffer<u32> {
        let bytes = Bytes::from_owner(Held(owner));
        Buffer::from_bytes_aligned(bytes, Alignment::of::<u32>())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vortex_array::IntoArray;

    fn column(n: u32) -> ArrayRef {
        let prim = PrimitiveArray::from_iter(0..n);
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        vortex::encodings::fastlanes::bitpack_compress::bitpack_encode(&prim, 10, None, &mut ctx)
            .unwrap()
            .into_array()
    }

    /// Two holders share one decode; the column is freed with the last of
    /// them, and a later read decodes afresh.
    #[test]
    fn shared_while_held_freed_after() {
        let live = LiveCanonical::new();
        let col = column(1000);
        assert!(!live.is_alive(0));
        let a = live.column(0, &col).unwrap();
        assert!(live.is_alive(0));
        let b = live.column(0, &col).unwrap();
        assert_eq!(a.as_ptr(), b.as_ptr());
        assert_eq!(a.as_slice(), (0..1000).collect::<Vec<u32>>());
        let tail = b.slice(990..1000);
        drop(a);
        drop(b);
        assert!(live.is_alive(0), "a slice is a holder too");
        drop(tail);
        assert!(!live.is_alive(0));
        assert!(live.column_if_alive(0).is_none());
        let c = live.column(0, &col).unwrap();
        assert_eq!(c.as_slice()[123], 123);
    }
}
