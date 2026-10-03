//! The backend-independent probe an index chooses for a pattern, and its
//! resolution against the in-memory components.

use std::ops::Range;

use vortex_array::IntoArray;
use vortex_array::arrays::struct_::StructArrayExt;

use super::components::{ComponentIdentity, IndexComponent, sorted_row_ids};
use super::serve::{InMemoryServePlan, ServeDecode};
use super::{COL_RID, IndexResolution, LazyRowIds, ResolvedRoles, ResolvedRowIds};
use crate::error::{Result, VortexRdfError};
use crate::store::layouts::{PatternCodes, TermRef};

/// What an index probes for a pattern.
///
/// `keys` are `(column, term)` equalities on the child's leading sort keys,
/// located in order, each inside the previous key's run; they define
/// `resolves` and the resolution's row ids. `residual` holds equalities a
/// file serve plan also filters on without locating them (a bound graph);
/// they keep a located range off the plan. `serve` is how a serving index
/// decodes its child, `None` for an index that stores no whole quads.
pub(crate) struct IndexProbe<'a> {
    pub(crate) identity: &'static ComponentIdentity,
    pub(crate) keys: Vec<(&'static str, TermRef<'a>)>,
    #[cfg_attr(not(feature = "file-io"), allow(dead_code))]
    pub(crate) residual: Vec<(&'static str, TermRef<'a>)>,
    pub(crate) resolves: ResolvedRoles,
    pub(crate) serve: Option<ServeDecode>,
}

/// Resolve `probe` against the in-memory components: one binary search per
/// key, each inside the previous key's run. `Declined` when the component is
/// absent or not globally sorted, or a key's column declines the search;
/// `Empty` when a key's term has no code or its run is empty. A serving probe
/// answers lazy ids beside its plan, a non-serving one eager ids.
pub(crate) fn resolve_in_memory(
    probe: IndexProbe<'_>,
    components: &[IndexComponent],
    codes: &mut PatternCodes,
) -> Result<IndexResolution<InMemoryServePlan>> {
    let Some(component) = IndexComponent::find_sorted(components, probe.identity.name) else {
        return Ok(IndexResolution::Declined);
    };
    let mut run: Option<Range<usize>> = None;
    for (column, term) in &probe.keys {
        let Some(native) = codes.probe_scalar(*term)? else {
            return Ok(IndexResolution::Empty);
        };
        let Some(narrowed) = component.probe_run(column, &native, run)? else {
            return Ok(IndexResolution::Declined);
        };
        if narrowed.is_empty() {
            return Ok(IndexResolution::Empty);
        }
        run = Some(narrowed);
    }
    let Some(run) = run else {
        return Ok(IndexResolution::Declined);
    };
    let rows = component.rows()?;
    let rids = rows
        .unmasked_field_by_name(COL_RID)
        .map_err(VortexRdfError::Vortex)?
        .slice(run.clone())
        .map_err(VortexRdfError::Vortex)?;
    Ok(match probe.serve {
        Some(decode) => IndexResolution::Resolved {
            row_ids: ResolvedRowIds::Lazy(LazyRowIds::from_component_run(rids)),
            resolves: probe.resolves,
            serve: Some(InMemoryServePlan::new(
                decode,
                rows.clone().into_array(),
                run,
                component.probes_arc(),
            )),
        },
        None => IndexResolution::Resolved {
            row_ids: ResolvedRowIds::Eager(sorted_row_ids(rids)?),
            resolves: probe.resolves,
            serve: None,
        },
    })
}
