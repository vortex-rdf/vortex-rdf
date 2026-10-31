#![doc = include_str!("../README.md")]
#![deny(missing_docs)]

mod delta;
mod node;
mod owned;
mod resolve;
mod search;

pub use owned::OwnedSortedProbe;

#[cfg(feature = "layout")]
mod layout;
#[cfg(feature = "layout")]
pub use layout::{ColumnChunks, FlatLeaf, LeafRows, fetch_flat, flat_leaves};

use std::sync::OnceLock;

use vortex_array::ArrayRef;

/// Ablation switch `VORTEX_RDF_ENCODED_SEARCH`, read once per process: `0` makes every place that searches or
/// point-reads an encoded column decode it first (to a primitive array) and search the decoded values; the chunk
/// bisection over chunk probes and the zone-map pruning stay as they are. Unset or any other value: the encoded
/// search as usual.
pub fn encoded_search_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| std::env::var("VORTEX_RDF_ENCODED_SEARCH").as_deref() != Ok("0"))
}

/// Work counters of the encoded search, for the ablation study: `VORTEX_RDF_COUNTERS=1`, read once per process.
/// Off, a note costs one relaxed load. Each site has a pair: arrays (leaves, columns) and their values.
pub mod counters {
    use std::sync::OnceLock;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Names in [`snapshot`] order.
    pub const NAMES: [&str; 18] = [
        "leaf_encoded",
        "leaf_encoded_values",
        "leaf_decoded",
        "leaf_decoded_values",
        "probe_encoded",
        "probe_encoded_values",
        "probe_decoded",
        "probe_decoded_values",
        "gather_encoded",
        "gather_encoded_values",
        "gather_decoded",
        "gather_decoded_values",
        "shared_decoded",
        "shared_decoded_values",
        "typed_eq_probe",
        "typed_eq_probe_values",
        "typed_eq_primitive",
        "typed_eq_primitive_values",
    ];
    /// A chunk leaf loaded and resolved as it is encoded.
    pub const LEAF_ENCODED: usize = 0;
    /// A chunk leaf loaded and decoded before its probe resolves (`VORTEX_RDF_ENCODED_SEARCH=0`).
    pub const LEAF_DECODED: usize = 2;
    /// A resident column probe resolved on the encoded column.
    pub const PROBE_ENCODED: usize = 4;
    /// A resident column probe resolved on the decoded column.
    pub const PROBE_DECODED: usize = 6;
    /// A point gather read through the encoded column (no cached probe).
    pub const GATHER_ENCODED: usize = 8;
    /// A point gather that decoded the column first.
    pub const GATHER_DECODED: usize = 10;
    /// A shared column's canonical form computed (decoded once, then cached).
    pub const SHARED_DECODED: usize = 12;
    /// An equality scan offered an encoded-search probe (it falls back to the generic scan if the probe declines).
    pub const TYPED_EQ_PROBE: usize = 14;
    /// An equality scan bound to a primitive (decoded) column.
    pub const TYPED_EQ_PRIMITIVE: usize = 16;

    static C: [AtomicU64; 18] = [const { AtomicU64::new(0) }; 18];

    /// Whether the counters are on.
    pub fn on() -> bool {
        static ON: OnceLock<bool> = OnceLock::new();
        *ON.get_or_init(|| std::env::var("VORTEX_RDF_COUNTERS").as_deref() == Ok("1"))
    }

    /// One array of `values` values at `site`.
    pub fn note(site: usize, values: usize) {
        if on() {
            C[site].fetch_add(1, Ordering::Relaxed);
            C[site + 1].fetch_add(values as u64, Ordering::Relaxed);
        }
    }

    /// The counters, in [`NAMES`] order.
    pub fn snapshot() -> [u64; 18] {
        std::array::from_fn(|i| C[i].load(Ordering::Relaxed))
    }
}

/// A resolved, borrowed probe over one encoded (or canonical) array.
///
/// Bounds queries require the array to be sorted ascending, a caller
/// contract as for `slice::partition_point`: an unsorted array yields an
/// unspecified bound, never a panic. [`Self::value_at`] is exact regardless
/// of sort order.
pub struct SortedProbe<'a> {
    root: node::Node<'a>,
}

impl<'a> SortedProbe<'a> {
    /// Resolves a host-resident, non-nullable unsigned-integer array whose
    /// encoding tree is drawn from the supported set ([`NodeKind`]); `None`
    /// otherwise. Resolution reads metadata only: nothing is decoded.
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

    /// Index of the first element `>= needle`; the length when every element
    /// is below it.
    pub fn lower_bound(&self, needle: u64) -> usize {
        self.root.lower_bound(needle)
    }

    /// Index of the first element `> needle`: [`Self::lower_bound`] of
    /// `needle + 1`, and the length when `needle` is `u64::MAX`.
    pub fn upper_bound(&self, needle: u64) -> usize {
        self.root.upper_bound(needle)
    }

    /// `(lower_bound, upper_bound)`: the half-open run of elements equal to
    /// `needle`, empty at its insertion point when there is none.
    #[inline]
    pub fn bounds(&self, needle: u64) -> (usize, usize) {
        self.root.bounds(needle)
    }

    /// [`Self::bounds`] restricted to `range`, in absolute indices: only the
    /// window must be sorted ascending (as in a column sorted per run of a
    /// leading key), and rows outside it are never read, because every probe
    /// point-reads through [`Self::value_at`].
    ///
    /// # Panics
    /// Panics if `range.end > self.len()`.
    pub fn bounds_in(&self, range: std::ops::Range<usize>, needle: u64) -> (usize, usize) {
        assert!(range.end <= self.len(), "window out of bounds");
        let width = range.end.saturating_sub(range.start);
        let value = |i: usize| self.value_at(range.start + i);
        // The lower bound's probes cap the needle's rows: the first row past
        // them lies between the bound and the nearest probe above the needle.
        let (lo, past) = search::search_ascending_capped(width, value, needle);
        let hi = lo + search::partition(past - lo, |i| value(lo + i) <= needle);
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

    /// The exact values at `start..start + out.len()`, widened to `u64`, into
    /// `out`: what [`Self::value_at`] reads at each index, with the work the
    /// indices share done once (a run-end column's first run found once and
    /// the runs walked, a patched column's patches searched once, a chunked
    /// column's chunks found once each). Needs no sort order.
    ///
    /// # Panics
    /// Panics if `start + out.len() > self.len()`.
    pub fn read_into(&self, start: usize, out: &mut [u64]) {
        assert!(
            start
                .checked_add(out.len())
                .is_some_and(|end| end <= self.len()),
            "{} values from {start} out of bounds",
            out.len()
        );
        self.root.read_into(start, out);
    }

    /// The exact values at `indices`, in their order, into `out`: each run of
    /// consecutive indices read in one pass ([`Self::read_into`]), a lone
    /// index point-read.
    ///
    /// # Panics
    /// Panics if an index is `>= self.len()` or `out` is not as long as
    /// `indices`.
    pub fn read_at(&self, indices: &[usize], out: &mut [u64]) {
        assert_eq!(indices.len(), out.len(), "one value per index");
        let mut i = 0;
        while i < indices.len() {
            let run = consecutive_run(&indices[i..], |index| index + 1);
            if run == 1 {
                out[i] = self.value_at(indices[i]);
            } else {
                self.read_into(indices[i], &mut out[i..i + run]);
            }
            i += run;
        }
    }

    /// Pre-order encoding kinds of the resolved tree, for tests and
    /// diagnostics.
    pub fn node_kinds(&self) -> Vec<NodeKind> {
        let mut kinds = Vec::new();
        self.root.collect_kinds(&mut kinds);
        kinds
    }
}

/// The length of the run of consecutive values `values` starts with, at least
/// one: each value is `next` of the one before it.
pub(crate) fn consecutive_run<T: Copy + PartialEq>(values: &[T], next: impl Fn(T) -> T) -> usize {
    1 + values
        .windows(2)
        .take_while(|pair| pair[1] == next(pair[0]))
        .count()
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

/// The probe-node shapes a [`SortedProbe`] resolves: a fixed set, not
/// vortex's open registry of encoding ids, and one that includes
/// [`NodeKind::Patches`], a component of bit-packed arrays rather than an
/// encoding. Non-exhaustive, so a new node shape breaks no downstream match.
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
    /// Chunks concatenated end to end, searched by each chunk's last value.
    Chunked,
    /// Dictionary encoding: codes indexing a values child. Dictionary values
    /// are not order-preserving, so bounds search probes the composed
    /// logical values rather than the values child.
    Dict,
    /// FastLanes delta: per 1024-value block, a lane of bases plus transposed
    /// deltas, decoded one block at a time on first touch.
    Delta,
    /// Sparse: one fill value, with exception values at patched positions.
    Sparse,
    /// FastLanes run-length: per 1024-value chunk, run values and an index
    /// per position into them.
    Rle,
}
