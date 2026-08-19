//! The [`IndexType::SecondaryByCopy`] index: two complete extra copies of the
//! quad columns, one per sort order, each paired with the primary row IDs it
//! permutes — the classic triple-store permutation indexes (POS/OSP) adapted
//! to quads. This module owns both halves of the index's lifecycle — building
//! the copy columns at write time, and executing lookups against them at query
//! time (`resolve_in_memory` / `resolve_file`).
//!
//! The two families are:
//!
//! - **`_idx_posg_*`** — quads sorted by (p, o, s, g). Serves predicate-bound
//!   patterns by binary search on `_idx_posg_p`, and predicate+object patterns
//!   by a two-key *prefix* search: within a predicate's run the object column
//!   is itself sorted, so a second binary search inside the run resolves both
//!   components at once (`IndexedComponent::PredicateObject`).
//! - **`_idx_ospg_*`** — quads sorted by (o, s, p, g). Serves object-bound
//!   patterns by binary search on `_idx_ospg_o`.
//!
//! Like [`secondary_by_reference`], resolutions answer in *base row ids* (via
//! the `_idx_*_rid` columns), so they compose with row selections, tombstones
//! and chained matches unchanged. What the full copies add over the reference
//! index is locality: the rows matching a bound predicate/object are a
//! *contiguous* run of the copy columns, which both backends exploit by
//! reading `quads()` straight from the copy family — this index hands back a
//! serve plan (`InMemoryServePlan` / `FileServePlan`) during resolution to
//! describe that read — instead of scattering row-id reads across the primary
//! columns.
//!
//! The copies come in two encodings — term strings (Default and TypedObject
//! layouts, the object as its full N-Triples term string), or u32 dictionary
//! codes under the Dictionary layout — and in the same two scopes as the
//! reference index: per-chunk (chunk-local sort, `IsSorted` stamped only when
//! the chunk spans the dataset) and global (`GlobalCopyArrays` and the
//! `append_sorted_*_keys` helpers, always stamped). The in-memory resolver
//! requires the lead value column's `IsSorted` stamp; the file resolver pushes
//! equality predicates down and only prunes better when the columns are
//! sorted.
//!
//! [`IndexType::SecondaryByCopy`]: super::IndexType::SecondaryByCopy
//! [`secondary_by_reference`]: super::secondary_by_reference

use std::cmp::Ordering;
use std::ops::Range;
use std::sync::Arc;

use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::struct_::StructArrayExt;
use vortex_array::dtype::DType;
use vortex_array::{ArrayRef, IntoArray};

use super::components::{child_struct, child_struct_dtype};
use super::{IndexResolution, IndexedComponent, LazyRowIds, ResolvedRowIds};
use crate::error::{Result, VortexRdfError};
use crate::store::RawQuad;
use crate::store::array::{make_string_array, stamp_is_sorted};
use crate::store::layouts::dictionary::QuadCodes;
use crate::store::layouts::{PatternCodes, QuadPattern, ResolvedLayout, TermRef};

#[cfg(feature = "file-io")]
use super::FileServePlan;
use super::InMemoryServePlan;
#[cfg(feature = "file-io")]
use vortex_array::scalar::Scalar;

/// One of the two sorted copy families this index maintains, named after its
/// sort order. Each family owns five columns: the four quad components plus
/// the primary row id each copy row came from.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum Family {
    /// Quads sorted by (p, o, s, g).
    Posg,
    /// Quads sorted by (o, s, p, g).
    Ospg,
}

/// Column names inside a copy family's persisted child: the plain primaries
/// plus the primary row id — no `_idx_*` prefixes; the child's identity
/// carries the family.
pub(crate) const CHILD_COLUMNS: [&str; 5] = ["s", "p", "o", "g", "rid"];
pub(crate) const CHILD_RID_COL: &str = "rid";
pub(crate) const POSG_IMPLEMENTATION: &str = "secondary-by-copy/posg";
pub(crate) const OSPG_IMPLEMENTATION: &str = "secondary-by-copy/ospg";

const POSG_SOURCE_COLUMNS: [&str; 5] = Family::Posg.column_names();
const OSPG_SOURCE_COLUMNS: [&str; 5] = Family::Ospg.column_names();

/// This index's persisted-child role table — one differently-sorted quad
/// table per family — feeding every generic loop in the hub (spec push,
/// row-space split, schema detection, the slug registry); see
/// [`IndexType::component_roles`](super::IndexType::component_roles). Built
/// from the [`Family`] accessors so each name keeps exactly one spelling.
pub(crate) const ROLES: [super::ComponentRole; 2] = [
    super::ComponentRole {
        name: Family::Posg.component_name(),
        slug: Family::Posg.component_slug(),
        source_columns: &POSG_SOURCE_COLUMNS,
        child_columns: &CHILD_COLUMNS,
        lead_source: Family::Posg.lead_col(),
    },
    super::ComponentRole {
        name: Family::Ospg.component_name(),
        slug: Family::Ospg.component_slug(),
        source_columns: &OSPG_SOURCE_COLUMNS,
        child_columns: &CHILD_COLUMNS,
        lead_source: Family::Ospg.lead_col(),
    },
];

/// The persisted child's struct dtype: quad components as strings (or u32
/// codes under the Dictionary layout) plus the u32 primary row id.
// This and the child-chunk builders below are consumed only by the
// external-sort builder, compiled out on wasm (see the module gate in
// `store::builders`).
#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
pub(crate) fn copy_child_dtype(encoded: bool) -> DType {
    use vortex_array::dtype::{Nullability, PType};
    let term = if encoded {
        DType::Primitive(PType::U32, Nullability::NonNullable)
    } else {
        DType::Utf8(Nullability::NonNullable)
    };
    child_struct_dtype(
        &CHILD_COLUMNS,
        vec![
            term.clone(),
            term.clone(),
            term.clone(),
            term,
            DType::Primitive(PType::U32, Nullability::NonNullable),
        ],
    )
}

/// One chunk of a copy family's persisted child from a window of its merged
/// `(sort key, row id)` entries — plain child column names, lead stamped.
#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
pub(crate) fn copy_child_chunk_strings(
    family: Family,
    keys: &[(CopyKey<String>, u32)],
) -> Result<ArrayRef> {
    let [s_ix, p_ix, o_ix, g_ix] = family.key_positions();
    let col = |ix: usize| make_string_array(keys.iter().map(|(key, _)| key.0[ix].as_str()));
    let columns = vec![
        col(s_ix),
        col(p_ix),
        col(o_ix),
        col(g_ix),
        PrimitiveArray::from_iter(keys.iter().map(|(_, rid)| *rid)).into_array(),
    ];
    stamp_is_sorted(&columns[family.lead_ix()]);
    child_struct(&CHILD_COLUMNS, columns, keys.len()).map(|a| a.into_array())
}

/// Code-column variant of [`copy_child_chunk_strings`].
#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
pub(crate) fn copy_child_chunk_codes(
    family: Family,
    keys: &[(CopyKey<u32>, u32)],
) -> Result<ArrayRef> {
    let [s_ix, p_ix, o_ix, g_ix] = family.key_positions();
    let col = |ix: usize| -> ArrayRef {
        PrimitiveArray::from_iter(keys.iter().map(|(key, _)| key.0[ix])).into_array()
    };
    let columns = vec![
        col(s_ix),
        col(p_ix),
        col(o_ix),
        col(g_ix),
        PrimitiveArray::from_iter(keys.iter().map(|(_, rid)| *rid)).into_array(),
    ];
    stamp_is_sorted(&columns[family.lead_ix()]);
    child_struct(&CHILD_COLUMNS, columns, keys.len()).map(|a| a.into_array())
}

impl Family {
    pub(crate) const ALL: [Family; 2] = [Family::Posg, Family::Ospg];

    /// The persisted child's component name.
    pub(crate) const fn component_name(self) -> &'static str {
        match self {
            Family::Posg => "index:posg",
            Family::Ospg => "index:ospg",
        }
    }

    /// The persisted child's implementation slug.
    pub(crate) const fn component_slug(self) -> &'static str {
        match self {
            Family::Posg => POSG_IMPLEMENTATION,
            Family::Ospg => OSPG_IMPLEMENTATION,
        }
    }

    /// The leading sort-key column inside the persisted child (plain names).
    pub(crate) fn child_lead_col(self) -> &'static str {
        match self {
            Family::Posg => "p",
            Family::Ospg => "o",
        }
    }

    /// The second sort-key column inside the persisted child.
    pub(crate) fn child_second_col(self) -> &'static str {
        match self {
            Family::Posg => "o",
            Family::Ospg => "s",
        }
    }

    pub(crate) const fn s_col(self) -> &'static str {
        match self {
            Family::Posg => "_idx_posg_s",
            Family::Ospg => "_idx_ospg_s",
        }
    }

    pub(crate) const fn p_col(self) -> &'static str {
        match self {
            Family::Posg => "_idx_posg_p",
            Family::Ospg => "_idx_ospg_p",
        }
    }

    pub(crate) const fn o_col(self) -> &'static str {
        match self {
            Family::Posg => "_idx_posg_o",
            Family::Ospg => "_idx_ospg_o",
        }
    }

    pub(crate) const fn g_col(self) -> &'static str {
        match self {
            Family::Posg => "_idx_posg_g",
            Family::Ospg => "_idx_ospg_g",
        }
    }

    pub(crate) const fn rid_col(self) -> &'static str {
        match self {
            Family::Posg => "_idx_posg_rid",
            Family::Ospg => "_idx_ospg_rid",
        }
    }

    /// The five column names in the order the builders emit them (s, p, o, g,
    /// rid).
    // `const` (with the accessors above) so [`ROLES`] can assemble its
    // `&'static` rosters from these instead of re-spelling the names.
    pub(crate) const fn column_names(self) -> [&'static str; 5] {
        [
            self.s_col(),
            self.p_col(),
            self.o_col(),
            self.g_col(),
            self.rid_col(),
        ]
    }

    /// The column holding this family's leading sort key — the one binary
    /// searches probe and builders stamp `IsSorted`.
    pub(crate) const fn lead_col(self) -> &'static str {
        match self {
            Family::Posg => self.p_col(),
            Family::Ospg => self.o_col(),
        }
    }

    /// Index of the lead value column within [`Self::column_names`] order.
    fn lead_ix(self) -> usize {
        match self {
            Family::Posg => 1,
            Family::Ospg => 2,
        }
    }

    /// This family's quad comparator over term strings.
    fn cmp_quads(self, a: &RawQuad, b: &RawQuad) -> Ordering {
        match self {
            Family::Posg => {
                a.p.cmp(&b.p)
                    .then_with(|| a.o.cmp(&b.o))
                    .then_with(|| a.s.cmp(&b.s))
                    .then_with(|| a.g.cmp(&b.g))
            }
            Family::Ospg => {
                a.o.cmp(&b.o)
                    .then_with(|| a.s.cmp(&b.s))
                    .then_with(|| a.p.cmp(&b.p))
                    .then_with(|| a.g.cmp(&b.g))
            }
        }
    }

    /// Row `i`'s sort key as a code tuple — order-equivalent to
    /// [`Self::cmp_quads`] because sorted-dictionary codes are lexicographic
    /// ranks.
    fn code_key(self, codes: &QuadCodes, i: usize) -> [u32; 4] {
        match self {
            Family::Posg => [codes.p[i], codes.o[i], codes.s[i], codes.g[i]],
            Family::Ospg => [codes.o[i], codes.s[i], codes.p[i], codes.g[i]],
        }
    }

    /// Where each quad component (s, p, o, g) sits inside this family's
    /// [`CopyKey`] tuple, which stores the components in sort-key order.
    // Read only by the external-sort emission (wasm-gated) and its tests.
    #[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
    fn key_positions(self) -> [usize; 4] {
        match self {
            Family::Posg => [2, 0, 1, 3],
            Family::Ospg => [1, 2, 0, 3],
        }
    }
}

/// The family, probe terms, and resolved component(s) this index would use for
/// a pattern shape, independent of any backend — the shared front half of both
/// resolvers.
///
/// A bound subject declines the index: the primary `s` column (binary-searched
/// or zone-pruned) is the better access path there. A bound predicate *and*
/// object take the POSG family's (p, o) prefix, resolving both components in
/// one probe. `None` when nothing this index covers is bound.
struct CopyProbe<'a> {
    family: Family,
    lead: TermRef<'a>,
    second: Option<TermRef<'a>>,
    resolves: IndexedComponent,
}

fn choose<'a>(pattern: QuadPattern<'a>) -> Option<CopyProbe<'a>> {
    if pattern.subject.is_some() {
        return None;
    }
    match (pattern.predicate, pattern.object) {
        (Some(predicate), Some(object)) => Some(CopyProbe {
            family: Family::Posg,
            lead: TermRef::Predicate(predicate),
            second: Some(TermRef::Object(object)),
            resolves: IndexedComponent::PredicateObject,
        }),
        (Some(predicate), None) => Some(CopyProbe {
            family: Family::Posg,
            lead: TermRef::Predicate(predicate),
            second: None,
            resolves: IndexedComponent::Predicate,
        }),
        (None, Some(object)) => Some(CopyProbe {
            family: Family::Ospg,
            lead: TermRef::Object(object),
            second: None,
            resolves: IndexedComponent::Object,
        }),
        (None, None) => None,
    }
}

/// Resolve a pattern against this index's in-memory component.
///
/// Binary-searches the chosen family's lead column for the probe term — and,
/// for a (p, o) prefix probe, the object column within the resulting run — and
/// slices out the paired row ids. Declines (so the store falls back to a mask
/// scan) when the family's component is absent, probe-incompatible, or not
/// globally sorted (`IndexComponent::sorted` — per-chunk sorted data is not
/// binary-searchable). Global sortedness by the family's full comparator is
/// also what makes the second column sorted within each lead run and the
/// prefix search valid.
pub(crate) fn resolve_in_memory(
    components: &[super::IndexComponent],
    layout: &ResolvedLayout,
    pattern: QuadPattern<'_>,
    codes: &mut PatternCodes,
) -> Result<IndexResolution<InMemoryServePlan>> {
    // Pick the family and probe(s) for this pattern shape, or decline it.
    let Some(probe) = choose(pattern) else {
        return Ok(IndexResolution::Declined);
    };
    // Route through the index only when the family's component exists and its
    // sort keys are globally sorted — the writer's provenance, not a stamp
    // inspection.
    let Some(component) =
        super::IndexComponent::find_sorted(components, probe.family.component_name())
    else {
        return Ok(IndexResolution::Declined);
    };
    // Translate the term to the value columns' native probe value (a string,
    // or a dictionary code). Absent from the dictionary ⇒ nothing can match.
    // The probe terms are the pattern's own predicate/object, so this shares
    // the match's resolution cache rather than searching the dictionary again.
    let Some(lead_native) = codes.probe_scalar(probe.lead)? else {
        return Ok(IndexResolution::Empty);
    };
    // First genuine use of a `from_bytes`-adopted component: this is where a
    // deferred child canonicalizes.
    let rows = component.rows()?;
    // Binary search bounds the run of rows whose lead component equals the
    // probe — through the component's cached probe when the column resolves
    // one.
    let Some(mut run) =
        super::component_probe_run(component, probe.family.child_lead_col(), &lead_native, None)?
    else {
        return Ok(IndexResolution::Declined);
    };
    if run.is_empty() {
        return Ok(IndexResolution::Empty);
    }
    // Prefix probe: narrow the run by the second sort key, which is sorted
    // within the run by the family's comparator.
    if let Some(second_term) = probe.second {
        let Some(second_native) = codes.probe_scalar(second_term)? else {
            return Ok(IndexResolution::Empty);
        };
        let Some(narrowed) = super::component_probe_run(
            component,
            probe.family.child_second_col(),
            &second_native,
            Some(run),
        )?
        else {
            return Ok(IndexResolution::Declined);
        };
        if narrowed.is_empty() {
            return Ok(IndexResolution::Empty);
        }
        run = narrowed;
    }
    // Row ids of every quad in the matched run — the rid slice comes out in
    // the family's order, so materializing decodes and re-sorts it into base
    // row order. Handed back lazily: the serving plan below answers reads
    // without the ids, so the decode+sort runs only if a consumer needs the
    // selection itself.
    let rids = rows
        .unmasked_field_by_name(CHILD_RID_COL)
        .map_err(VortexRdfError::Vortex)?
        .slice(run.clone())
        .map_err(VortexRdfError::Vortex)?;
    Ok(IndexResolution::Resolved {
        row_ids: ResolvedRowIds::Lazy(LazyRowIds::from_component_run(rids)),
        resolves: probe.resolves,
        // The matched quads are the contiguous matched run of this family's
        // component, so a read can slice them straight from it instead of
        // gathering the primary columns at the row ids (see
        // `InMemoryServePlan`).
        serve: Some(InMemoryServePlan::new(
            ["s", "p", "o", "g"],
            CHILD_RID_COL,
            copy_decode_layout(layout),
            rows.clone().into_array(),
            run,
            component.probes_arc(),
        )),
    })
}

/// The layout a copy family's columns decode through: the copies always store
/// each component as one full term — dictionary codes under the Dictionary
/// layout, N-Triples strings otherwise, so even a TypedObject store's copies
/// decode as Default.
fn copy_decode_layout(layout: &ResolvedLayout) -> ResolvedLayout {
    match layout {
        ResolvedLayout::Dictionary(dict) => ResolvedLayout::Dictionary(dict.clone()),
        _ => ResolvedLayout::Default,
    }
}

/// Resolve a pattern against this index's copy columns in a file-backed store
/// — the file counterpart of [`resolve_in_memory`]. On a globally sorted
/// family the matched run is located first by binary search over the child's
/// cached chunk probes (the in-memory search's file mirror, including the
/// windowed second-key probe); a small run's row ids then come from rid point
/// reads instead of a deferred child scan, and the serve plan carries the
/// range so reads point-read it too. Anything the probes decline falls back
/// to the pushed-down scan, whose filter answers regardless of sortedness.
#[cfg(feature = "file-io")]
pub(crate) async fn resolve_file(
    file: &crate::store::native_file::NativeStoreFile,
    layout: &ResolvedLayout,
    pattern: QuadPattern<'_>,
    codes: &mut PatternCodes,
) -> Result<IndexResolution<FileServePlan>> {
    let Some(probe) = choose(pattern) else {
        return Ok(IndexResolution::Declined);
    };
    // The store's index set says this index exists, but be graceful when the
    // family's child is absent (a foreign writer could omit one family).
    let Some((descriptor, reader)) = file
        .component_reader(probe.family.component_name())
        .map_err(VortexRdfError::Vortex)?
    else {
        return Ok(IndexResolution::Declined);
    };
    let sorted = descriptor.sorted;
    // Term absent from the dictionary ⇒ the pattern provably matches nothing.
    let Some(lead_native) = codes.probe_scalar(probe.lead)? else {
        return Ok(IndexResolution::Empty);
    };
    let mut constraints: Vec<(&'static str, Scalar)> =
        vec![(probe.family.child_lead_col(), lead_native)];
    if let Some(second_term) = probe.second {
        let Some(second_native) = codes.probe_scalar(second_term)? else {
            return Ok(IndexResolution::Empty);
        };
        constraints.push((probe.family.child_second_col(), second_native));
    }

    // Locate the matched run through the child's cached chunk probes: the
    // lead search over the whole child, then — for a prefix probe — the
    // windowed second-key search inside the lead run. Integer probe values
    // only (string copies decline through `u64::try_from`); any probe
    // decline abandons the location wholesale.
    let name = probe.family.component_name();
    let mut located: Option<std::ops::Range<u64>> = None;
    if sorted && let Ok(lead_needle) = u64::try_from(&constraints[0].1) {
        let source = file.segment_source();
        let session = file.session();
        located = match file.component_column_chunks(name, probe.family.child_lead_col()) {
            Some(chunks) => chunks
                .bounds(lead_needle, &source, session)
                .await
                .map_err(VortexRdfError::Vortex)?,
            None => None,
        };
        if let Some(range) = located.clone()
            && !range.is_empty()
            && let Some((_, second_native)) = constraints.get(1)
        {
            located = match (
                file.component_column_chunks(name, probe.family.child_second_col()),
                u64::try_from(second_native),
            ) {
                (Some(chunks), Ok(needle)) => chunks
                    .bounds_in(range, needle, &source, session)
                    .await
                    .map_err(VortexRdfError::Vortex)?,
                _ => None,
            };
        }
    }
    // A located empty run proves the combination absent — the short-circuit
    // the deferred path gives up.
    if let Some(range) = &located
        && range.is_empty()
    {
        return Ok(IndexResolution::Empty);
    }
    // A small located run resolves its row ids NOW by rid point reads — a
    // handful of cached-chunk accesses — instead of deferring a whole child
    // scan (which a count or chained match would then pay).
    let row_ids = match &located {
        Some(range)
            if (range.end - range.start) as usize
                <= crate::store::selection::POINT_GATHER_MAX_ROWS =>
        {
            match super::rid_point_reads(file, name, CHILD_RID_COL, range.clone()).await? {
                Some(ids) => ResolvedRowIds::Eager(ids),
                None => ResolvedRowIds::Lazy(LazyRowIds::from_index_child_scan(
                    reader.clone(),
                    constraints.clone(),
                    CHILD_RID_COL,
                )),
            }
        }
        _ => ResolvedRowIds::Lazy(LazyRowIds::from_index_child_scan(
            reader.clone(),
            constraints.clone(),
            CHILD_RID_COL,
        )),
    };
    match build_serve_plan(reader.clone(), layout, pattern, codes, name, located)? {
        // A serving resolution reads the matched quads straight from the
        // copy columns — point reads over a located run, or the pushed-down
        // filter scan.
        Some(plan) => Ok(IndexResolution::Resolved {
            row_ids,
            resolves: probe.resolves,
            serve: Some(plan),
        }),
        // No plan (a bound residual term with no dictionary code — see
        // `build_serve_plan`): fall back to the eager scan, whose ids the
        // store will actually need.
        None => {
            super::resolve_eager_from_scan(reader, &constraints, CHILD_RID_COL, probe.resolves)
                .await
        }
    }
}

/// Build the [`FileServePlan`] letting the store stream a resolved pattern's
/// quads from this index's own copy columns, or `None` when a bound residual
/// term has no dictionary code (the pattern matches nothing — a case
/// `match_pattern` already short-circuits before resolving, so this is only a
/// safety fallback to the row-id path).
///
/// Every bound non-subject component (predicate, object, graph) becomes a term
/// equality on the family's matching column: the copies store each component as
/// one full term, so — unlike the primary layout's split TypedObject columns —
/// even the object probes as a single equality. The copy index declines
/// subject-bound patterns, so the subject never appears here.
#[cfg(feature = "file-io")]
fn build_serve_plan(
    reader: vortex_layout::LayoutReaderRef,
    layout: &ResolvedLayout,
    pattern: QuadPattern<'_>,
    codes: &mut PatternCodes,
    component: &'static str,
    row_range: Option<std::ops::Range<u64>>,
) -> Result<Option<FileServePlan>> {
    let mut constraints: Vec<(&'static str, Scalar)> = Vec::new();
    for (column, term) in [
        ("p", pattern.predicate.map(TermRef::Predicate)),
        ("o", pattern.object.map(TermRef::Object)),
        ("g", pattern.graph.map(TermRef::Graph)),
    ] {
        let Some(term) = term else { continue };
        let Some(scalar) = codes.probe_scalar(term)? else {
            return Ok(None);
        };
        constraints.push((column, scalar));
    }
    // A located range is exactly the constrained rows only when the probes
    // covered every constraint: the location searched the sort keys (lead,
    // then second), so a bound graph — never a sort key here — demotes the
    // range back to the filter scan.
    let row_range = if pattern.graph.is_none() {
        row_range
    } else {
        None
    };
    Ok(Some(FileServePlan::new(
        ["s", "p", "o", "g"],
        CHILD_RID_COL,
        copy_decode_layout(layout),
        reader,
        constraints,
        component,
        row_range,
    )))
}

// ── build side ───────────────────────────────────────────────────────────────

/// The permutation putting `quads` in `family` order.
fn string_perm(quads: &[RawQuad], family: Family) -> Vec<u32> {
    let mut perm: Vec<u32> = (0..quads.len() as u32).collect();
    perm.sort_unstable_by(|&a, &b| family.cmp_quads(&quads[a as usize], &quads[b as usize]));
    perm
}

/// The permutation putting the encoded dataset in `family` order.
fn code_perm(codes: &QuadCodes, family: Family) -> Vec<u32> {
    let mut perm: Vec<u32> = (0..codes.s.len() as u32).collect();
    perm.sort_unstable_by_key(|&i| family.code_key(codes, i as usize));
    perm
}

/// One family's five columns (s, p, o, g, rid) over `perm` order, term-string
/// encoding. `start_row` offsets the row ids so they address the assembled
/// array.
fn family_string_columns(quads: &[RawQuad], perm: &[u32], start_row: u32) -> [ArrayRef; 5] {
    let col = |term_of: fn(&RawQuad) -> &str| -> ArrayRef {
        make_string_array(perm.iter().map(|&i| term_of(&quads[i as usize])))
    };
    [
        col(|q| &q.s),
        col(|q| &q.p),
        col(|q| &q.o),
        col(|q| &q.g),
        PrimitiveArray::from_iter(perm.iter().map(|&i| start_row + i)).into_array(),
    ]
}

/// Code-column variant of [`family_string_columns`].
fn family_code_columns(codes: &QuadCodes, perm: &[u32], start_row: u32) -> [ArrayRef; 5] {
    let col = |column: &[u32]| -> ArrayRef {
        PrimitiveArray::from_iter(perm.iter().map(|&i| column[i as usize])).into_array()
    };
    [
        col(&codes.s),
        col(&codes.p),
        col(&codes.o),
        col(&codes.g),
        PrimitiveArray::from_iter(perm.iter().map(|&i| start_row + i)).into_array(),
    ]
}

fn push_family(
    field_names: &mut Vec<Arc<str>>,
    field_arrays: &mut Vec<ArrayRef>,
    family: Family,
    columns: [ArrayRef; 5],
) {
    field_names.extend(
        family
            .column_names()
            .iter()
            .map(|name| Arc::<str>::from(*name)),
    );
    field_arrays.extend(columns);
}

/// Append the ten copy columns for one chunk, sorting the chunk's own quads
/// into each family's order.
///
/// `start_row` is the global row ID of the first quad in `quads`, so per-chunk
/// builders emit row IDs that address the fully assembled array. An empty
/// `quads` slice yields empty columns with the correct dtypes.
///
/// `whole_dataset` must be `true` only when `quads` is the entire dataset
/// (single-chunk builds): the chunk-local sort is then the global order and
/// the lead value columns are stamped `IsSorted` for binary-search routing.
pub(crate) fn append_columns(
    field_names: &mut Vec<Arc<str>>,
    field_arrays: &mut Vec<ArrayRef>,
    quads: &[RawQuad],
    start_row: u32,
    whole_dataset: bool,
) {
    for family in Family::ALL {
        let perm = string_perm(quads, family);
        let columns = family_string_columns(quads, &perm, start_row);
        if whole_dataset {
            stamp_is_sorted(&columns[family.lead_ix()]);
        }
        push_family(field_names, field_arrays, family, columns);
    }
}

/// Dictionary-layout variant of [`append_columns`]: the copy columns hold u32
/// dictionary codes instead of strings. Sorting codes is order-equivalent to
/// sorting the term strings, so the families stay binary-searchable — queries
/// translate the pattern terms to codes first.
pub(crate) fn append_encoded_columns(
    field_names: &mut Vec<Arc<str>>,
    field_arrays: &mut Vec<ArrayRef>,
    codes: &QuadCodes,
    start_row: u32,
    whole_dataset: bool,
) {
    for family in Family::ALL {
        let perm = code_perm(codes, family);
        let columns = family_code_columns(codes, &perm, start_row);
        if whole_dataset {
            stamp_is_sorted(&columns[family.lead_ix()]);
        }
        push_family(field_names, field_arrays, family, columns);
    }
}

/// A quad's terms rearranged into one family's sort-key order, so deriving
/// `Ord` (and the spill machinery's pair sort) compares by exactly that
/// family's comparator. `V` is the term encoding: `String`, or `u32` codes
/// under the Dictionary layout.
///
/// Built via [`Self::posg`] / [`Self::ospg`] from an `[s, p, o, g]` tuple;
/// [`Family::key_positions`] maps the components back out when the sorted
/// keys are turned into columns.
#[derive(
    Clone, Debug, PartialEq, Eq, PartialOrd, Ord, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
pub(crate) struct CopyKey<V>(pub(crate) [V; 4]);

#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
impl<V: Clone> CopyKey<V> {
    /// The POSG key of a quad given as `[s, p, o, g]`.
    pub(crate) fn posg(spog: &[V; 4]) -> Self {
        Self([
            spog[1].clone(),
            spog[2].clone(),
            spog[0].clone(),
            spog[3].clone(),
        ])
    }

    /// The OSPG key of a quad given as `[s, p, o, g]`, consuming the tuple —
    /// the merge path constructs it last, so the rearrangement needs no
    /// clones (which are String allocations on the non-Dictionary layouts).
    pub(crate) fn ospg(spog: [V; 4]) -> Self {
        let [s, p, o, g] = spog;
        Self([o, s, p, g])
    }
}

/// Append both families' globally sorted code-key windows as row-space
/// `_idx_*` columns (the in-memory builders' emission path).
#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
pub(crate) fn append_sorted_code_keys(
    field_names: &mut Vec<Arc<str>>,
    field_arrays: &mut Vec<ArrayRef>,
    posg: &[(CopyKey<u32>, u32)],
    ospg: &[(CopyKey<u32>, u32)],
    stamp_sorted: bool,
) {
    append_family_code_keys(field_names, field_arrays, Family::Posg, posg, stamp_sorted);
    append_family_code_keys(field_names, field_arrays, Family::Ospg, ospg, stamp_sorted);
}

#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
fn append_family_code_keys(
    field_names: &mut Vec<Arc<str>>,
    field_arrays: &mut Vec<ArrayRef>,
    family: Family,
    keys: &[(CopyKey<u32>, u32)],
    stamp_sorted: bool,
) {
    let [s_ix, p_ix, o_ix, g_ix] = family.key_positions();
    let col = |ix: usize| -> ArrayRef {
        PrimitiveArray::from_iter(keys.iter().map(|(key, _)| key.0[ix])).into_array()
    };
    let columns = [
        col(s_ix),
        col(p_ix),
        col(o_ix),
        col(g_ix),
        PrimitiveArray::from_iter(keys.iter().map(|(_, rid)| *rid)).into_array(),
    ];
    if stamp_sorted {
        stamp_is_sorted(&columns[family.lead_ix()]);
    }
    push_family(field_names, field_arrays, family, columns);
}

/// The complete dataset's copy columns in global family order, built once by
/// in-memory builders and sliced per chunk: chunk `i` carries window
/// `[i·C, (i+1)·C)` of the same order, so the concatenation across chunks is
/// itself the globally sorted copy.
pub(crate) struct GlobalCopyArrays {
    posg: [ArrayRef; 5],
    ospg: [ArrayRef; 5],
}

impl GlobalCopyArrays {
    /// Sort by term strings. Row IDs are the quads' positions in `quads` (the
    /// builder must pass the dataset in final row order), so each family is
    /// just a u32 permutation — no per-term string copies beyond the columns.
    pub(crate) fn from_quads(quads: &[RawQuad]) -> Self {
        let build = |family: Family| {
            let perm = string_perm(quads, family);
            let columns = family_string_columns(quads, &perm, 0);
            stamp_is_sorted(&columns[family.lead_ix()]);
            columns
        };
        Self {
            posg: build(Family::Posg),
            ospg: build(Family::Ospg),
        }
    }

    /// Dictionary-layout variant: sort the u32 codes.
    pub(crate) fn from_codes(codes: &QuadCodes) -> Self {
        let build = |family: Family| {
            let perm = code_perm(codes, family);
            let columns = family_code_columns(codes, &perm, 0);
            stamp_is_sorted(&columns[family.lead_ix()]);
            columns
        };
        Self {
            posg: build(Family::Posg),
            ospg: build(Family::Ospg),
        }
    }

    /// Append window `range` of the global order as one chunk's copy columns.
    /// Lead value slices are re-stamped `IsSorted` (a slice of a sorted array
    /// is sorted, but slicing does not propagate the stat).
    pub(crate) fn append_slice(
        &self,
        field_names: &mut Vec<Arc<str>>,
        field_arrays: &mut Vec<ArrayRef>,
        range: Range<usize>,
    ) -> Result<()> {
        for (family, columns) in [(Family::Posg, &self.posg), (Family::Ospg, &self.ospg)] {
            for (ix, (name, arr)) in family.column_names().iter().zip(columns).enumerate() {
                let sliced = arr.slice(range.clone()).map_err(VortexRdfError::Vortex)?;
                if ix == family.lead_ix() {
                    stamp_is_sorted(&sliced);
                }
                field_names.push((*name).into());
                field_arrays.push(sliced);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxrdf::{Literal, NamedNode, NamedOrBlankNode, Term};

    fn raw(s: &str, p: &str, o: &str, g: &str) -> RawQuad {
        RawQuad {
            s: s.to_string(),
            p: p.to_string(),
            o: o.to_string(),
            g: g.to_string(),
        }
    }

    #[test]
    fn choose_family_and_component() {
        let s = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s").unwrap());
        let p = NamedNode::new("http://example.org/p").unwrap();
        let o = Term::Literal(Literal::new_simple_literal("o"));

        // A bound subject declines: the primary sorted `s` column is the
        // better access path than this index.
        assert!(choose(QuadPattern::new(Some(&s), Some(&p), Some(&o), None)).is_none());

        // Predicate and object bound: (p, o) prefix probe on the POSG family,
        // resolving both components.
        let probe = choose(QuadPattern::new(None, Some(&p), Some(&o), None)).unwrap();
        assert_eq!(probe.family, Family::Posg);
        assert_eq!(probe.resolves, IndexedComponent::PredicateObject);
        assert_eq!(probe.lead.to_string(), p.to_string());
        assert_eq!(probe.second.map(|t| t.to_string()), Some(o.to_string()));

        // Predicate-only patterns probe the POSG lead alone.
        let probe = choose(QuadPattern::new(None, Some(&p), None, None)).unwrap();
        assert_eq!(probe.family, Family::Posg);
        assert_eq!(probe.resolves, IndexedComponent::Predicate);
        assert!(probe.second.is_none());

        // Object-only patterns probe the OSPG lead.
        let probe = choose(QuadPattern::new(None, None, Some(&o), None)).unwrap();
        assert_eq!(probe.family, Family::Ospg);
        assert_eq!(probe.resolves, IndexedComponent::Object);
        assert!(probe.second.is_none());

        // Nothing this index covers is bound: declines.
        assert!(choose(QuadPattern::new(None, None, None, None)).is_none());
    }

    #[test]
    fn family_permutations_follow_comparators() {
        // Rows chosen so every family produces a distinct order.
        let quads = vec![
            raw("s2", "p1", "o2", ""), // 0
            raw("s0", "p2", "o0", ""), // 1
            raw("s1", "p1", "o0", ""), // 2
        ];
        // (p, o, s, g): (p1,o0) < (p1,o2) < (p2,o0) → rows 2, 0, 1.
        assert_eq!(string_perm(&quads, Family::Posg), vec![2, 0, 1]);
        // (o, s, p, g): (o0,s0) < (o0,s1) < (o2,s2) → rows 1, 2, 0.
        assert_eq!(string_perm(&quads, Family::Ospg), vec![1, 2, 0]);

        // The code comparator agrees with the string one when codes are
        // lexicographic ranks of the terms.
        let codes = QuadCodes {
            s: vec![2, 0, 1],
            p: vec![0, 1, 0],
            o: vec![1, 0, 0],
            g: vec![0, 0, 0],
        };
        assert_eq!(code_perm(&codes, Family::Posg), vec![2, 0, 1]);
        assert_eq!(code_perm(&codes, Family::Ospg), vec![1, 2, 0]);
    }

    #[test]
    fn copy_key_positions_roundtrip() {
        // Rearranging [s, p, o, g] into a key and reading it back through
        // key_positions must return the original components.
        let spog = [
            "s".to_string(),
            "p".to_string(),
            "o".to_string(),
            "g".to_string(),
        ];

        let posg = CopyKey::posg(&spog);
        let [s_ix, p_ix, o_ix, g_ix] = Family::Posg.key_positions();
        assert_eq!(
            [&posg.0[s_ix], &posg.0[p_ix], &posg.0[o_ix], &posg.0[g_ix]],
            [&spog[0], &spog[1], &spog[2], &spog[3]]
        );

        let ospg = CopyKey::ospg(spog.clone());
        let [s_ix, p_ix, o_ix, g_ix] = Family::Ospg.key_positions();
        assert_eq!(
            [&ospg.0[s_ix], &ospg.0[p_ix], &ospg.0[o_ix], &ospg.0[g_ix]],
            [&spog[0], &spog[1], &spog[2], &spog[3]]
        );

        // Derived Ord on the key compares by the family's comparator: POSG
        // keys order by predicate first.
        let key = |s: &str, p: &str, o: &str| {
            CopyKey::posg(&[s.to_string(), p.to_string(), o.to_string(), String::new()])
        };
        assert!(key("s9", "p1", "o9") < key("s0", "p2", "o0"));
    }
}
