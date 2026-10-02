#![doc = include_str!("../README.md")]
#![deny(missing_docs)]

mod node;
mod owned;
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
    /// window must be sorted ascending; rows outside it are never read.
    /// Intended for columns that are sorted piecewise, e.g. per run of a
    /// leading key. Probes point-read through [`Self::value_at`], so the
    /// window's order is the only order consulted.
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

/// Reports what the probe resolved to, not the data it reads: the row count
/// and the pre-order encoding kinds of the tree.
impl std::fmt::Debug for SortedProbe<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SortedProbe")
            .field("len", &self.len())
            .field("nodes", &self.node_kinds())
            .finish()
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
    /// A decoded buffer of unsigned integers, read directly.
    Primitive,
    /// One value repeated for the whole array.
    Constant,
    /// An arithmetic sequence, evaluated from its start and step.
    Sequence,
    /// Run-end encoding: a search over run ends, then the run's value.
    RunEnd,
    /// Frame of reference: a reference value added to an encoded child.
    FoR,
    /// Bit-packed words, unpacked one value at a time.
    BitPacked,
    /// The exception values riding beside a [`NodeKind::BitPacked`] node.
    Patches,
    /// A window over a child, offsetting every index into it.
    Slice,
    /// Chunks concatenated end to end, addressed by chunk extremes.
    Chunked,
    /// Dictionary encoding: codes indexing a values child. Dictionary values
    /// are not order-preserving, so bounds search probes the composed
    /// logical values rather than the values child.
    Dict,
    /// FastLanes delta: per 1024-value block, a lane of bases plus transposed
    /// deltas, decoded one block at a time on first touch.
    Delta,
}
