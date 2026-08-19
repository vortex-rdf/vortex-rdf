//! An owning probe: the array and its resolved [`SortedProbe`] in one value,
//! so callers can resolve once and reuse the probe across calls (typically
//! behind an `Arc` in a per-column cache) instead of re-walking the encoding
//! tree on every search.

use vortex_array::ArrayRef;

use crate::SortedProbe;

self_cell::self_cell!(
    struct ProbeCell {
        owner: ArrayRef,
        #[covariant]
        dependent: SortedProbe,
    }
);

/// A [`SortedProbe`] that owns the array it probes.
///
/// Same contract as [`SortedProbe`]: bounds queries require the array to be
/// sorted ascending (caller contract); [`Self::value_at`] is exact regardless
/// of sort order.
pub struct OwnedSortedProbe {
    cell: ProbeCell,
}

impl OwnedSortedProbe {
    /// Resolves `array` exactly as [`SortedProbe::resolve`] does, keeping the
    /// array alive alongside the probe; returns `None` on decline.
    pub fn resolve(array: ArrayRef) -> Option<Self> {
        ProbeCell::try_new(array, |a| SortedProbe::resolve(a).ok_or(()))
            .ok()
            .map(|cell| Self { cell })
    }

    /// The resolved probe, borrowed from this owner.
    pub fn probe(&self) -> &SortedProbe<'_> {
        self.cell.borrow_dependent()
    }

    /// The owned array the probe reads.
    pub fn array(&self) -> &ArrayRef {
        self.cell.borrow_owner()
    }

    /// Number of rows in the resolved array.
    pub fn len(&self) -> usize {
        self.probe().len()
    }

    /// Whether the resolved array is empty.
    pub fn is_empty(&self) -> bool {
        self.probe().is_empty()
    }

    /// Index of the first element `>= needle`.
    pub fn lower_bound(&self, needle: u64) -> usize {
        self.probe().lower_bound(needle)
    }

    /// Index of the first element `> needle`.
    pub fn upper_bound(&self, needle: u64) -> usize {
        self.probe().upper_bound(needle)
    }

    /// `(lower_bound, upper_bound)` — the half-open run of elements equal to
    /// `needle`.
    pub fn bounds(&self, needle: u64) -> (usize, usize) {
        self.probe().bounds(needle)
    }

    /// [`SortedProbe::bounds_in`]: bounds restricted to a window that is
    /// itself sorted, in absolute indices.
    ///
    /// # Panics
    /// Panics if `range.end > self.len()`.
    pub fn bounds_in(&self, range: std::ops::Range<usize>, needle: u64) -> (usize, usize) {
        self.probe().bounds_in(range, needle)
    }

    /// Exact value at `index`, widened to `u64`.
    ///
    /// # Panics
    /// Panics if `index >= self.len()`.
    pub fn value_at(&self, index: usize) -> u64 {
        self.probe().value_at(index)
    }
}

#[cfg(test)]
mod tests {
    use vortex_array::IntoArray;
    use vortex_array::arrays::PrimitiveArray;

    use super::*;

    #[test]
    fn owned_probe_outlives_local_array_binding() {
        let owned = {
            let arr = PrimitiveArray::from_iter([1u32, 3, 3, 7]).into_array();
            OwnedSortedProbe::resolve(arr).unwrap()
        };
        assert_eq!(owned.len(), 4);
        assert_eq!(owned.bounds(3), (1, 3));
        assert_eq!(owned.value_at(3), 7);
    }

    #[test]
    fn owned_probe_declines_like_borrowed() {
        let arr = PrimitiveArray::from_iter([1i32, 2]).into_array();
        assert!(OwnedSortedProbe::resolve(arr).is_none());
    }

    #[test]
    fn owned_probe_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<OwnedSortedProbe>();
    }
}
