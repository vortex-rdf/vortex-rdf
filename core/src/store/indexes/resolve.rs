//! The probe an index runs for a pattern ([`IndexProbe`], which each index
//! chooses for the shapes it covers, independent of any backend) and its
//! execution against an in-memory store's index components: the one
//! in-memory resolver every index shares, as `file::resolve_file` is the one
//! file resolver.

use std::ops::Range;

use vortex_array::IntoArray;
use vortex_array::arrays::struct_::StructArrayExt;

use super::components::{ComponentIdentity, IndexComponent};
use super::serve::{InMemoryServePlan, ServeDecode};
use super::{COL_RID, IndexResolution, LazyRowIds, ResolvedRoles, ResolvedRowIds};
use crate::error::Result;
use crate::store::layouts::{PatternCodes, TermRef};

/// What an index probes for a pattern shape it covers: the shared front half
/// of resolution, chosen by the index's leaf module (`copy::choose`,
/// `reference::choose`) and executed by either backend's resolver.
pub(super) struct IndexProbe<'a> {
    /// The probed child's identity: the component name both backends find it
    /// by.
    pub(super) identity: &'static ComponentIdentity,
    /// `(column, term)` equalities on the child's leading sort keys, in sort
    /// order. Each key is sorted within the run of the keys before it (the
    /// child's full comparator orders it so), which lets each search be
    /// windowed to the previous key's run. Together the keys select exactly
    /// the rows `resolves` names. At least one: a probe without keys bounds
    /// no run, and `IndexType::choose` declines it for both backends.
    pub(super) keys: Vec<(&'static str, TermRef<'a>)>,
    /// Equalities a file serve plan filters on beyond the keys (a bound
    /// graph, never one of the searched keys); a residual term keeps a
    /// located run off the plan, whose range would over-cover it.
    // Read by the file resolver, which is compiled in under `file-io`.
    #[cfg_attr(not(feature = "file-io"), allow(dead_code))]
    pub(super) residual: Vec<(&'static str, TermRef<'a>)>,
    /// Which pattern roles the keys resolve.
    pub(super) resolves: ResolvedRoles,
    /// How the child's columns decode as the primary quads, for an index
    /// whose children hold whole quads and serve reads from them; `None` for
    /// one that stores only back-references.
    pub(super) serve: Option<ServeDecode>,
    /// Whether a run's rids ascend: true for a `{val, rid}` child, whose rows
    /// are ordered by `(val, rid)`, so that row `k` of a run holds the run's
    /// `k`-th smallest rid. A window of the run's rows is then the same
    /// window of its ids in base order, cut before anything decodes in
    /// memory. On file it is what lets a non-serving probe keep a located
    /// run's ids deferred: a count takes the run's width and a window reads
    /// only its own rows.
    pub(super) rids_ascend: bool,
}

/// `probe` resolved against an in-memory store's index components: one
/// binary search per key over the probed component, each windowed to the run
/// of the keys before it, then the matched run's paired row ids.
///
/// Declines (so the store falls back to a mask scan) when the probed
/// component is absent, probe-incompatible, or not globally sorted
/// (`IndexComponent::sorted`: per-chunk-sorted data is not
/// binary-searchable); `Empty` when a key's term is absent from the
/// dictionary or its run is empty. The row ids are always lazy (the run's
/// rid slice, decoded and sorted into base row order when a consumer needs
/// the selection, and windowed before decoding when its rids ascend), and a
/// serving probe hands back an [`InMemoryServePlan`] over the run beside
/// them, so reads slice the matched quads straight from the component.
pub(super) fn resolve_in_memory(
    probe: IndexProbe<'_>,
    components: &[IndexComponent],
    codes: &mut PatternCodes,
) -> Result<IndexResolution<InMemoryServePlan>> {
    // Route through the index only when the probed component exists and its
    // sort keys are globally sorted: the writer's provenance, not a stamp
    // inspection.
    let Some(component) = IndexComponent::find_sorted(components, probe.identity.name) else {
        return Ok(IndexResolution::Declined);
    };
    let mut run: Option<Range<usize>> = None;
    for &(column, term) in &probe.keys {
        // The term's native probe value (a string, or a dictionary code),
        // read off the match's resolution cache, as the terms are the
        // pattern's own. A term absent from the dictionary matches nothing.
        let Some(native) = codes.probe_scalar(term)? else {
            return Ok(IndexResolution::Empty);
        };
        // The run of rows equal to the term within the previous key's run, by
        // binary search (through the component's cached probe when the column
        // resolves one). The first search is a `from_bytes`-adopted
        // component's first use, where a deferred child canonicalizes.
        let Some(narrowed) = component.probe_run(column, &native, run)? else {
            return Ok(IndexResolution::Declined);
        };
        // An empty run: no row the keys before it selected holds the term.
        if narrowed.is_empty() {
            return Ok(IndexResolution::Empty);
        }
        run = Some(narrowed);
    }
    let run = run.expect("`IndexType::choose` hands out only probes with keys");
    let rows = component.rows()?;
    let rids = rows.unmasked_field_by_name(COL_RID)?.slice(run.clone())?;
    let ids = LazyRowIds::from_component_run(rids.clone(), probe.rids_ascend);
    // The matched quads are the contiguous matched run of the serving
    // component, so a read can slice them straight from it instead of
    // gathering the primary columns at the row ids. The plan shares the ids'
    // caches: the run's ids decode once for both.
    let serve = probe.serve.map(|decode| {
        InMemoryServePlan::new(
            decode,
            component.identity.name,
            rows.clone().into_array(),
            run,
            rids,
            component.probes_arc(),
            ids.cells(),
        )
    });
    Ok(IndexResolution::Resolved {
        row_ids: ResolvedRowIds::Lazy(ids),
        resolves: probe.resolves,
        serve,
    })
}
