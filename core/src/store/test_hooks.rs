//! Test hooks on [`VortexRdfStore`]: read-only accessors over the store's
//! internal state, so tests assert which mechanism answered a query (a
//! serve plan, a deferred selection, a prefix-probe range, a retained wire
//! encoding) rather than only the result.

use std::ops::Range;

use vortex_array::ArrayRef;
use vortex_array::arrays::StructArray;

#[cfg(feature = "file-io")]
use crate::error::Result;
use crate::store::array;
#[cfg(feature = "file-io")]
use crate::store::layouts::QuadPattern;
#[cfg(feature = "file-io")]
use crate::store::layouts::DictAccess;
use crate::store::layouts::ResolvedLayout;
use crate::store::selection::{RowSelection, ViewSelection};
use crate::store::{QuadsSource, VortexRdfStore};

pub(crate) use crate::store::mutation::{TAIL_FLATTEN_FLOOR, TAIL_MAX_CHUNKS};

#[cfg(feature = "file-io")]
use oxrdf::NamedOrBlankNode;

impl VortexRdfStore {
    /// Whether this view carries an index serving plan for `quads()`.
    pub(crate) fn debug_has_serve_plan(&self) -> bool {
        match &self.quads {
            QuadsSource::InMemory { serve, .. } => serve.is_some(),
            #[cfg(feature = "file-io")]
            QuadsSource::File { serve, .. } => serve.is_some(),
        }
    }

    /// Whether this view's base selection is still pending — a served match
    /// whose exact row ids no consumer has needed yet.
    pub(crate) fn debug_selection_pending(&self) -> bool {
        matches!(self.quads.view_selection(), ViewSelection::Pending(_))
    }

    /// Whether a pending selection's row ids have been computed: `false` for
    /// a still-deferred resolution, `None` when the selection is not pending
    /// at all.
    pub(crate) fn debug_row_ids_materialized(&self) -> Option<bool> {
        match self.quads.view_selection() {
            ViewSelection::Exact(_) => None,
            ViewSelection::Pending(lazy) => Some(lazy.debug_materialized()),
        }
    }

    /// The exact row range this view's base selection is, when it is one
    /// (what the prefix probe leaves behind); `None` for every other shape.
    pub(crate) fn debug_selection_range(&self) -> Option<Range<u64>> {
        match self.quads.view_selection() {
            ViewSelection::Exact(RowSelection::Range(range)) => Some(range.clone()),
            _ => None,
        }
    }

    /// The append tail's physical rows, `None` when nothing has been
    /// appended.
    pub(crate) fn debug_tail_rows(&self) -> Option<&ArrayRef> {
        self.tail.as_ref().map(|tail| &tail.rows)
    }

    /// Whether every non-nullable integer child of an in-memory base is a
    /// canonical primitive (adoption decoded everything, or the children
    /// were built canonical). Vacuously true for a file-backed store, whose
    /// base is not held in memory.
    pub(crate) fn debug_base_int_children_canonical(&self) -> bool {
        use vortex_array::arrays::Struct;
        match &self.quads {
            QuadsSource::InMemory { base, .. } => match base.clone().try_downcast::<Struct>() {
                Ok(struct_arr) => debug_int_children_canonical(&struct_arr),
                Err(_) => false,
            },
            #[cfg(feature = "file-io")]
            QuadsSource::File { .. } => true,
        }
    }

    /// Whether the named in-memory component's rows hold canonical integer
    /// children. `None` when this store holds no such in-memory component.
    pub(crate) fn debug_index_component_int_children_canonical(&self, name: &str) -> Option<bool> {
        match &self.quads {
            QuadsSource::InMemory { components, .. } => {
                let component = components.iter().find(|c| c.name == name)?;
                Some(debug_int_children_canonical(component.rows().ok()?))
            }
            #[cfg(feature = "file-io")]
            QuadsSource::File { .. } => None,
        }
    }

    /// Whether every sorted-stamped child of an in-memory base resolves an
    /// encoded search probe (see [`debug_sorted_children_probe_resolvable`]):
    /// false only when a bounds search on some child would fall through to
    /// the generic kernel. Vacuously true for a file-backed store.
    pub(crate) fn debug_base_probe_resolvable(&self) -> bool {
        use vortex_array::arrays::Struct;
        match &self.quads {
            QuadsSource::InMemory { base, .. } => match base.clone().try_downcast::<Struct>() {
                Ok(struct_arr) => debug_sorted_children_probe_resolvable(&struct_arr),
                Err(_) => false,
            },
            #[cfg(feature = "file-io")]
            QuadsSource::File { .. } => true,
        }
    }

    /// Whether an in-memory base's `s` column carries the sorted stamp;
    /// false for a file-backed store.
    pub(crate) fn debug_base_subject_sorted(&self) -> bool {
        match &self.quads {
            QuadsSource::InMemory { base, .. } => array::subject_sorted(base),
            #[cfg(feature = "file-io")]
            QuadsSource::File { .. } => false,
        }
    }
}

#[cfg(feature = "file-io")]
impl VortexRdfStore {
    /// Whether the dictionary was left in its file child (a file-backed
    /// dictionary is only built around a point-readable wire-chunk handle).
    pub(crate) fn debug_dict_file_backed(&self) -> bool {
        matches!(
            &self.layout,
            ResolvedLayout::Dictionary(DictAccess::FileBacked(_))
        )
    }

    /// Whether one named integer child of an in-memory base is a canonical
    /// primitive. `None` when the base has no such child or is not in memory.
    pub(crate) fn debug_base_child_int_canonical(&self, name: &str) -> Option<bool> {
        use vortex_array::arrays::struct_::StructArrayExt;
        use vortex_array::arrays::{Primitive, Struct};
        match &self.quads {
            QuadsSource::InMemory { base, .. } => {
                let struct_arr = base.clone().try_downcast::<Struct>().ok()?;
                let child = struct_arr.unmasked_field_by_name(name).ok()?;
                child.dtype().is_int().then(|| child.is::<Primitive>())
            }
            QuadsSource::File { .. } => None,
        }
    }

    /// Whether the named in-memory index component has canonicalized its
    /// rows yet; `None` when this store holds no such component.
    pub(crate) fn debug_index_component_materialized(&self, name: &str) -> Option<bool> {
        match &self.quads {
            QuadsSource::InMemory { components, .. } => components
                .iter()
                .find(|c| c.name == name)
                .map(|c| c.is_materialized()),
            QuadsSource::File { .. } => None,
        }
    }

    /// The index-child row range a file view's serve plan located for its
    /// run. `None` without a plan, or when the plan's run is unlocated.
    pub(crate) fn debug_serve_row_range(&self) -> Option<Range<u64>> {
        match &self.quads {
            QuadsSource::InMemory { .. } => None,
            QuadsSource::File { serve, .. } => serve.as_ref().and_then(|plan| plan.row_range()),
        }
    }

    /// The zone-map row range a bound subject prunes to on a file view:
    /// `Some(0..0)` when the layout or the statistics prove it absent, `None`
    /// off-file or when the statistics exclude nothing.
    pub(crate) async fn debug_subject_pruning_envelope(
        &self,
        subject: &NamedOrBlankNode,
    ) -> Result<Option<Range<u64>>> {
        use crate::store::scan::file_scan;
        let QuadsSource::File { file, .. } = &self.quads else {
            return Ok(None);
        };
        let pattern = QuadPattern::new(Some(subject), None, None, None);
        let Some(mut codes) = self.prepared_codes(pattern).await? else {
            return Ok(Some(0..0));
        };
        match file_scan::build_file_filter(pattern, &mut codes)? {
            Some(filter) => file_scan::row_range_from_pruning(file, &filter).await,
            None => Ok(None),
        }
    }

    /// The exact row range the encoded chunk-probe fast path computes for a
    /// bound subject; `None` when the fast path would not engage (off-file,
    /// unsorted file, unsupported layout, unknown term).
    pub(crate) async fn debug_subject_chunk_probe_range(
        &self,
        subject: &NamedOrBlankNode,
    ) -> Result<Option<Range<u64>>> {
        use crate::store::scan::file_scan;
        let QuadsSource::File { file, .. } = &self.quads else {
            return Ok(None);
        };
        let Some(mut codes) = self
            .prepared_codes(QuadPattern::new(Some(subject), None, None, None))
            .await?
        else {
            return Ok(None);
        };
        file_scan::locate_subject_run(file, &mut codes, subject).await
    }

    /// The index-child run the reference index's file resolution locates for
    /// a predicate/object pattern; `None` when the location declines and the
    /// resolution falls back to its pushed-down scan.
    pub(crate) async fn debug_reference_index_located_run(
        &self,
        predicate: Option<&oxrdf::NamedNode>,
        object: Option<&oxrdf::Term>,
    ) -> Result<Option<Range<u64>>> {
        let QuadsSource::File { file, .. } = &self.quads else {
            return Ok(None);
        };
        let pattern = QuadPattern::new(None, predicate, object, None);
        let Some(mut codes) = self.prepared_codes(pattern).await? else {
            return Ok(None);
        };
        crate::store::indexes::secondary_by_reference::debug_located_run(file, pattern, &mut codes)
            .await
    }
}

/// Whether every non-nullable integer child of `struct_arr` is a canonical
/// primitive — the shared predicate behind the resident-adoption hooks.
fn debug_int_children_canonical(struct_arr: &StructArray) -> bool {
    use vortex_array::arrays::Primitive;
    use vortex_array::arrays::struct_::StructArrayExt;
    struct_arr.names().iter().all(|name| {
        let Ok(child) = struct_arr.unmasked_field_by_name(name.as_ref()) else {
            return false;
        };
        let int = child.dtype().is_int() && !child.dtype().is_nullable();
        !int || child.clone().try_downcast::<Primitive>().is_ok()
    })
}

/// Whether every sorted-stamped child of `struct_arr` binds an encoded
/// search probe — the property that keeps every bounds search off the
/// generic per-scalar kernel, whether the child is canonical or
/// wire-encoded. Unsorted children never take bounds searches, so they are
/// not constrained.
fn debug_sorted_children_probe_resolvable(struct_arr: &StructArray) -> bool {
    use vortex_array::arrays::struct_::StructArrayExt;
    struct_arr.names().iter().all(|name| {
        let Ok(child) = struct_arr.unmasked_field_by_name(name.as_ref()) else {
            return false;
        };
        !array::column_is_sorted(child)
            || vortex_rdf_encoded_search::SortedProbe::resolve(child).is_some()
    })
}

impl VortexRdfStore {
    /// Whether some reader currently holds the live canonical form of
    /// in-memory base column `idx` (in `PRIMARY_COLUMNS` order); `None` for
    /// a file-backed store.
    pub(crate) fn debug_live_canonical_alive(&self, idx: usize) -> Option<bool> {
        match &self.quads {
            QuadsSource::InMemory { canonical, .. } => Some(canonical.is_alive(idx)),
            #[cfg(feature = "file-io")]
            QuadsSource::File { .. } => None,
        }
    }

    /// Whether some consumer currently holds the resident dictionary's Arrow
    /// values array; `None` without a resident dictionary.
    pub(crate) fn debug_dict_arrow_values_alive(&self) -> Option<bool> {
        match &self.layout {
            ResolvedLayout::Dictionary(access) => {
                Some(access.resident()?.debug_arrow_values_alive())
            }
            _ => None,
        }
    }
}
