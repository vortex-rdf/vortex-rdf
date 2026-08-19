//! Bounds search and point access over compressed Vortex arrays without
//! decoding.
//!
//! A [`SortedProbe`] resolves an encoded array into a borrowed probe tree and
//! answers two-sided bounds queries and point reads directly against the
//! compressed representation. Resolution walks the encoding tree with typed
//! downcasts only; probing is slice reads, integer arithmetic, and single
//! bit-packed word extraction — no `ExecutionCtx`, no canonicalization.
//!
//! Supported encoding nodes: Primitive, Constant, Sequence, RunEnd, FoR,
//! BitPacked (with patches), Slice, Chunked, and Dict, composed arbitrarily;
//! the transparent Shared wrapper resolves to whatever it wraps. Anything
//! else — including nullable or non-unsigned-integer dtypes and non-host
//! buffers — declines, and the caller falls back to its generic search path.

mod node;
mod owned;
mod patches;
mod resolve;

pub use owned::OwnedSortedProbe;

#[cfg(feature = "layout")]
mod layout;
#[cfg(feature = "layout")]
pub use layout::ColumnChunks;

use vortex_array::ArrayRef;

/// A resolved, borrowed probe over one encoded (or canonical) array.
///
/// Bounds queries require the array to be sorted ascending — sortedness is a
/// caller contract, exactly like `slice::partition_point`; an unsorted array
/// yields an unspecified (but never panicking) bound. [`Self::value_at`] is
/// exact regardless of sort order.
pub struct SortedProbe<'a> {
    root: node::Node<'a>,
}

impl<'a> SortedProbe<'a> {
    /// Resolves a non-nullable unsigned-integer array whose encoding tree is
    /// drawn from the supported set; returns `None` otherwise.
    pub fn resolve(array: &'a ArrayRef) -> Option<Self> {
        if !array.is_host() {
            return None;
        }
        resolve::resolve_node(array).map(|root| Self { root })
    }

    /// Number of rows in the resolved array.
    pub fn len(&self) -> usize {
        self.root.len()
    }

    /// Whether the resolved array is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Index of the first element `>= needle`.
    pub fn lower_bound(&self, needle: u64) -> usize {
        self.root.lower_bound(needle)
    }

    /// Index of the first element `> needle`.
    pub fn upper_bound(&self, needle: u64) -> usize {
        self.root.upper_bound(needle)
    }

    /// `(lower_bound, upper_bound)` — the half-open run of elements equal to
    /// `needle`.
    pub fn bounds(&self, needle: u64) -> (usize, usize) {
        (self.lower_bound(needle), self.upper_bound(needle))
    }

    /// [`Self::bounds`] restricted to `range`, in absolute indices. Only the
    /// window must be sorted ascending — rows outside it are never read (a
    /// prefix probe searches a second key inside a lead run of a column that
    /// is not sorted as a whole). Probes point-read through [`Self::value_at`]
    /// rather than descending the encoding structure, so the window's order is
    /// the only order consulted.
    ///
    /// # Panics
    /// Panics if `range.end > self.len()`.
    pub fn bounds_in(&self, range: std::ops::Range<usize>, needle: u64) -> (usize, usize) {
        assert!(range.end <= self.len(), "window out of bounds");
        let width = range.end.saturating_sub(range.start);
        let lo = node::partition(width, |i| self.value_at(range.start + i) < needle);
        let hi = lo
            + node::partition(width - lo, |i| {
                self.value_at(range.start + lo + i) <= needle
            });
        (range.start + lo, range.start + hi)
    }

    /// Exact value at `index`, widened to `u64`.
    ///
    /// # Panics
    /// Panics if `index >= self.len()`.
    pub fn value_at(&self, index: usize) -> u64 {
        assert!(index < self.len(), "index {index} out of bounds");
        self.root.value_at(index)
    }

    /// Pre-order encoding kinds of the resolved tree, for tests and
    /// diagnostics.
    pub fn node_kinds(&self) -> Vec<NodeKind> {
        let mut kinds = Vec::new();
        self.root.collect_kinds(&mut kinds);
        kinds
    }
}

/// The set of probe-node shapes a [`SortedProbe`] resolves.
///
/// This is deliberately not a vortex type: vortex identifies encodings by an
/// open, registry-backed string id, while probing supports a fixed set of
/// node shapes (including [`NodeKind::Patches`], which is a component of
/// bit-packed arrays rather than an encoding of its own). Non-exhaustive so
/// new probe nodes extend the set without breaking downstream matches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum NodeKind {
    Primitive,
    Constant,
    Sequence,
    RunEnd,
    FoR,
    BitPacked,
    Patches,
    Slice,
    Chunked,
    Dict,
}
