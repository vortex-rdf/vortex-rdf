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
/// Queries go through the borrowed probe, [`Self::probe`], under its
/// contract: bounds queries require the array to be sorted ascending (caller
/// contract); [`SortedProbe::value_at`] is exact regardless of sort order.
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
}

/// Forwards to the borrowed [`SortedProbe`]'s own [`std::fmt::Debug`].
impl std::fmt::Debug for OwnedSortedProbe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("OwnedSortedProbe")
            .field(self.probe())
            .finish()
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
        let probe = owned.probe();
        assert_eq!(probe.len(), 4);
        assert_eq!(probe.bounds(3), (1, 3));
        assert_eq!(probe.bounds_in(1..4, 3), (1, 3));
        assert_eq!((probe.lower_bound(7), probe.upper_bound(7)), (3, 4));
        assert_eq!(probe.value_at(3), 7);
        assert_eq!(owned.array().len(), 4);
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
