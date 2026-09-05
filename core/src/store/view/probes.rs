//! Per-base cache of resolved encoded-search probes over a struct array's
//! children. A base is immutable for a store's lifetime (mutation constructs
//! a new source), so probes are resolved once per base and shared by every
//! derived view.

use std::sync::{Arc, OnceLock};

use vortex_array::ArrayRef;
use vortex_array::dtype::FieldName;
use vortex_rdf_encoded_search::OwnedSortedProbe;

/// Lazily-resolved probes for one struct array's children, shared across the
/// views over that base (`Arc` in `QuadsSource::InMemory`) and by the index
/// components and serve plans keyed on their own struct arrays.
///
/// The cells are keyed by the address of the array they were resolved for;
/// every lookup against a different base returns `None` (the uncached path).
#[derive(Default)]
pub(crate) struct StructProbes {
    cells: OnceLock<(usize, ProbeCells)>,
}

/// One resolved probe (or a decline) per base child, in child order.
type ProbeCells = Vec<(FieldName, Option<Arc<OwnedSortedProbe>>)>;

impl StructProbes {
    /// A fresh, unresolved cache, shared (`Arc`) because every
    /// `QuadsSource::InMemory` and index component holds one by `Arc`.
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// The resolved probe for `base`'s `idx`-th struct child, resolving all
    /// children on first use. `None` for a child whose encoding declines, a
    /// non-struct base, or a base other than the one first resolved.
    pub(crate) fn child(&self, base: &ArrayRef, idx: usize) -> Option<&Arc<OwnedSortedProbe>> {
        Some(&self.cells(base)?.get(idx)?.1).and_then(Option::as_ref)
    }

    /// Resolve `base`'s children now; later lookups against the same base
    /// are cache hits. Called by every in-memory construction after its
    /// encoding pass.
    pub(crate) fn warm(&self, base: &ArrayRef) {
        let _ = self.cells(base);
    }

    /// The resolved probe for `base`'s child named `name`; `None` exactly as
    /// for [`Self::child`].
    pub(crate) fn by_name(&self, base: &ArrayRef, name: &str) -> Option<&Arc<OwnedSortedProbe>> {
        self.cells(base)?
            .iter()
            .find(|(n, _)| n.as_ref() == name)
            .and_then(|(_, probe)| probe.as_ref())
    }

    fn cells(&self, base: &ArrayRef) -> Option<&ProbeCells> {
        let (addr, cells) = self
            .cells
            .get_or_init(|| (base.addr(), Self::resolve_children(base)));
        (*addr == base.addr()).then_some(cells)
    }

    fn resolve_children(base: &ArrayRef) -> ProbeCells {
        use vortex_array::arrays::Struct;
        use vortex_array::arrays::struct_::StructArrayExt;

        let Ok(struct_arr) = base.clone().try_downcast::<Struct>() else {
            return Vec::new();
        };
        let names = struct_arr.names().clone();
        names
            .iter()
            .map(|name| {
                let probe = struct_arr
                    .unmasked_field_by_name(name.as_ref())
                    .ok()
                    .and_then(|c| OwnedSortedProbe::resolve(c.clone()).map(Arc::new));
                (name.clone(), probe)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vortex_array::IntoArray;
    use vortex_array::arrays::StructArray;
    use vortex_array::validity::Validity;
    use vortex_buffer::Buffer;

    fn u32_struct(values: impl IntoIterator<Item = u32>) -> ArrayRef {
        let s = Buffer::from_iter(values).into_array();
        let len = s.len();
        StructArray::try_new(["s"].into(), vec![s], len, Validity::NonNullable)
            .unwrap()
            .into_array()
    }

    /// The cells resolve for the first base only; every other base looks up
    /// as a miss.
    #[test]
    fn cells_decline_a_different_base() {
        let a = u32_struct(0..4);
        let b = u32_struct(0..4);
        let probes = StructProbes::new();
        probes.warm(&a);
        assert!(probes.cells(&a).is_some());
        assert!(probes.child(&a, 0).is_some());
        assert!(probes.by_name(&a, "s").is_some());
        assert!(probes.cells(&b).is_none());
        assert!(probes.child(&b, 0).is_none());
        assert!(probes.by_name(&b, "s").is_none());
    }
}
