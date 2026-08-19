//! Per-store cache of resolved encoded-search probes over the base's
//! columns.
//!
//! Resolving a probe walks the column's encoding tree (per-chunk on a
//! chunked base), which measured as the dominant fixed cost of point reads
//! on a compressed-resident store when paid per call. The base is immutable
//! for a store's lifetime — mutation constructs a new source — so its
//! column probes are resolved once and shared by every derived view.

use std::sync::{Arc, OnceLock};

use vortex_array::ArrayRef;
use vortex_array::dtype::FieldName;
use vortex_encoded_search::OwnedSortedProbe;

/// Lazily-resolved probes for one base array's struct children, shared
/// across the views over that base (`Arc` in `QuadsSource::InMemory`).
///
/// The cells record which array they were resolved for; a lookup against a
/// different base declines rather than answering with the wrong probes, so
/// a construction site that wrongly carries the cache across a base swap
/// degrades to the uncached path instead of corrupting reads.
#[derive(Default)]
pub(crate) struct BaseProbes {
    cells: OnceLock<(usize, ProbeCells)>,
}

/// One resolved probe (or a decline) per base child, in child order.
type ProbeCells = Vec<(FieldName, Option<Arc<OwnedSortedProbe>>)>;

impl BaseProbes {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// The resolved probe for `base`'s `idx`-th struct child, resolving all
    /// children on first use. `None` for a child whose encoding declines, a
    /// non-struct base, or a base other than the one first resolved.
    pub(crate) fn child(&self, base: &ArrayRef, idx: usize) -> Option<&Arc<OwnedSortedProbe>> {
        Some(&self.cells(base)?.get(idx)?.1).and_then(Option::as_ref)
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
