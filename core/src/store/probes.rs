//! Per-base cache of resolved encoded-search probes over a struct array's
//! children. A base is immutable for a store's lifetime (mutation constructs
//! a new source), so probes are resolved once per base and shared by every
//! derived view.

use std::sync::{Arc, OnceLock};

use vortex_array::ArrayRef;
use vortex_array::dtype::FieldName;
use vortex_rdf_encoded_search::OwnedSortedProbe;

use crate::store::zones::{CodeZones, ZoneTable};

/// Lazily resolved probes for one struct array's children, shared across the
/// views over that base (`Arc` in `MemoryBase`) and held by the index
/// components and serve plans for their own struct arrays.
///
/// The cells are keyed by the address of the array they were resolved for;
/// every lookup against a different base returns `None` (the uncached path).
#[derive(Default)]
pub(crate) struct StructProbes {
    cells: OnceLock<(usize, ProbeCells)>,
    /// The base's code columns' zone bounds, set once at construction
    /// ([`set_zones`](Self::set_zones)).
    zones: OnceLock<(usize, ZoneTable)>,
}

/// One resolved probe (or a decline) per base child, in child order.
type ProbeCells = Vec<(FieldName, Option<Arc<OwnedSortedProbe>>)>;

impl StructProbes {
    /// A fresh, unresolved cache, by `Arc` as every `MemoryBase` and index
    /// component holds one.
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Resolve `base`'s children now; later lookups against the same base
    /// are cache hits. Called by every in-memory construction after its
    /// encoding pass.
    pub(crate) fn warm(&self, base: &ArrayRef) {
        let _ = self.cells(base);
    }

    /// Whether the cache has resolved a base's children yet (test hook).
    #[cfg(test)]
    pub(crate) fn is_resolved(&self) -> bool {
        self.cells.get().is_some()
    }

    /// The cache's answer for `base`'s child named `name`, resolving all
    /// children on first use: `Some(Some(probe))` for a resolved probe,
    /// `Some(None)` for a child whose encoding declines (or a name `base`
    /// lacks), and `None` for a base other than the one first resolved: the
    /// cache cannot answer for that base's children.
    pub(crate) fn by_name(
        &self,
        base: &ArrayRef,
        name: &str,
    ) -> Option<Option<&Arc<OwnedSortedProbe>>> {
        Some(
            self.cells(base)?
                .iter()
                .find(|(n, _)| n.as_ref() == name)
                .and_then(|(_, probe)| probe.as_ref()),
        )
    }

    /// Record `zones` as `base`'s code columns' zone bounds, each column's
    /// only if its zones cover the base's rows; a cache that already holds a
    /// base's keeps them.
    pub(crate) fn set_zones(&self, base: &ArrayRef, zones: ZoneTable) {
        let _ = self.zones.set((base.addr(), zones.covering(base.len())));
    }

    /// `base`'s child `name`'s zone bounds ([`CodeZones`]); `None` for a base
    /// other than the one they were set for, or a child without them.
    pub(crate) fn zones(&self, base: &ArrayRef, name: &str) -> Option<&CodeZones> {
        let (addr, table) = self.zones.get()?;
        (*addr == base.addr()).then(|| table.get(name)).flatten()
    }

    /// The columns of `base` with zone bounds (test hook).
    #[cfg(test)]
    pub(crate) fn zone_columns(&self, base: &ArrayRef) -> Vec<String> {
        match self.zones.get() {
            Some((addr, table)) if *addr == base.addr() => table.columns(),
            _ => Vec::new(),
        }
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
                    .and_then(|c| {
                        // VORTEX_RDF_ENCODED_SEARCH=0: the probe searches the decoded column.
                        use vortex_rdf_encoded_search::counters::{
                            PROBE_DECODED, PROBE_ENCODED, note,
                        };
                        let c = if vortex_rdf_encoded_search::encoded_search_enabled() {
                            note(PROBE_ENCODED, c.len());
                            c.clone()
                        } else {
                            note(PROBE_DECODED, c.len());
                            crate::store::array::decoded_for_search(c).ok()?
                        };
                        OwnedSortedProbe::resolve(c).map(Arc::new)
                    });
                (name.clone(), probe)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vortex_array::IntoArray;
    use vortex_array::arrays::{StructArray, VarBinViewArray};
    use vortex_array::validity::Validity;
    use vortex_buffer::Buffer;

    /// A four-row struct of a u32 `s`, whose encoding resolves a probe, and a
    /// Utf8 `o`, whose encoding resolves none.
    fn mixed_struct() -> ArrayRef {
        let s = Buffer::from_iter(0..4u32).into_array();
        let o = VarBinViewArray::from_iter_str(["a", "b", "c", "d"]).into_array();
        StructArray::try_new(["s", "o"].into(), vec![s, o], 4, Validity::NonNullable)
            .unwrap()
            .into_array()
    }

    /// The cells resolve for the first base only — a probe for a child whose
    /// encoding resolves one, the cache's own decline (`Some(None)`) for one
    /// whose encoding does not — and every other base looks up as a miss
    /// (`None`).
    #[test]
    fn cells_decline_a_different_base() {
        let a = mixed_struct();
        let b = mixed_struct();
        let probes = StructProbes::new();
        probes.warm(&a);
        assert!(probes.cells(&a).is_some());
        assert!(probes.by_name(&a, "s").flatten().is_some());
        assert!(matches!(probes.by_name(&a, "o"), Some(None)));
        assert!(matches!(probes.by_name(&a, "no-such-child"), Some(None)));
        assert!(probes.cells(&b).is_none());
        assert!(probes.by_name(&b, "s").is_none());

        // A struct without children, and a base that is no struct, resolve
        // no probe.
        let childless =
            StructArray::try_new(Vec::<&str>::new().into(), vec![], 4, Validity::NonNullable)
                .unwrap()
                .into_array();
        let flat = Buffer::from_iter(0..4u32).into_array();
        for base in [childless, flat] {
            let probes = StructProbes::new();
            probes.warm(&base);
            assert!(matches!(probes.by_name(&base, "s"), Some(None)));
        }
    }
}
