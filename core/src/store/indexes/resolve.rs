//! The probe an index runs for a pattern ([`IndexProbe`], which each index
//! chooses for the shapes it covers, independent of any backend) and its
//! execution against an in-memory store's index components: the one
//! in-memory resolver every index shares, as `file::resolve_file` is the one
//! file resolver.

use std::ops::Range;
use std::sync::Arc;

use vortex_array::IntoArray;
use vortex_array::arrays::struct_::StructArrayExt;

use super::components::{ComponentIdentity, IndexComponent};
use super::graph_check::GraphCheck;
use super::serve::{InMemoryServePlan, ServeDecode};
use super::{
    COL_RID, CheckedRun, ComponentRun, IndexResolution, LazyRowIds, ResolvedRoles, ResolvedRowIds,
};
use crate::error::Result;
use crate::store::array::unsigned_value;
use crate::store::layouts::{PatternCodes, TermRef};
use crate::zone_stats::rules::zones_enabled;

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
    /// The pattern's bound graph, which the in-memory resolver checks inside
    /// the run where the probed component keeps its graph column's zone map
    /// ([`IndexComponent::graph_zones`]). The file resolver reads a bound
    /// graph from `residual` instead (a copy child's), or leaves it to its
    /// located runs (a reference child's).
    pub(super) graph: Option<TermRef<'a>>,
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
///
/// A bound graph is checked inside the run where the component keeps its
/// graph column's zone map and zones are on ([`GraphCheck`]), except on a
/// view only `windowed` whose run's ids do not ascend (a copy family's): a
/// window takes its matches in base order, which would find every match of
/// such a run first, while the residual over the run's ids in base order
/// reaches the window through the base's zones.
pub(super) fn resolve_in_memory(
    probe: IndexProbe<'_>,
    components: &[IndexComponent],
    codes: &mut PatternCodes,
    windowed: bool,
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
    // A bound graph is checked inside the run where the component keeps its
    // graph column's zone map: the run's rows are contiguous there, so its
    // zones count and read them, where the residual would compare the graph
    // at the base rows the run lists.
    let check = match (probe.graph, component.graph_zones()) {
        (Some(graph), Some(zones)) if zones_enabled() && (probe.rids_ascend || !windowed) => {
            // A graph absent from the dictionary matches nothing, as a key's
            // term does.
            let Some(native) = codes.probe_scalar(graph)? else {
                return Ok(IndexResolution::Empty);
            };
            match unsigned_value(&native) {
                Some(code) => Some(GraphCheck::new(
                    component,
                    run.clone(),
                    code,
                    Arc::clone(zones),
                )?),
                None => None,
            }
        }
        _ => None,
    };
    let resolves = match check {
        Some(_) => probe.resolves.with_graph(),
        None => probe.resolves,
    };
    // The matched quads are the contiguous matched run of the serving
    // component, so a read can slice them straight from it instead of
    // gathering the primary columns at the row ids. The plan shares the run
    // with the lazy ids: its ids decode once for both. A checked run keeps
    // only some of the run's rows, which a plan would over-cover.
    let (ids, serve) = match check {
        Some(check) => (
            LazyRowIds::Checked(Arc::new(CheckedRun::new(rids, probe.rids_ascend, check))),
            None,
        ),
        None => {
            let ids = Arc::new(ComponentRun::new(rids, probe.rids_ascend));
            let serve = probe.serve.map(|decode| {
                InMemoryServePlan::new(
                    decode,
                    component.identity.name,
                    rows.clone().into_array(),
                    run,
                    Arc::clone(&ids),
                    component.probes_arc(),
                )
            });
            (LazyRowIds::Component(ids), serve)
        }
    };
    Ok(IndexResolution::Resolved {
        row_ids: ResolvedRowIds::Lazy(ids),
        resolves,
        serve,
    })
}
