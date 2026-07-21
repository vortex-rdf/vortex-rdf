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
//! *contiguous* run of the copy columns, which file-backed stores exploit by
//! streaming `quads()` straight from the copy family — this index hands back a
//! `ServePlan` during resolution to describe that read —
//! instead of scattering row-id reads across the primary columns.
//!
//! The copies come in two encodings — term strings (Default and TypedObject
//! layouts, the object as its full N-Triples term string), or u32 dictionary
//! codes under the Dictionary layout — and in the same two scopes as the
//! reference index: per-chunk (chunk-local sort, `IsSorted` stamped only when
//! the chunk spans the dataset) and global (`GlobalCopyArrays` and the
//! `append_sorted_*_keys` helpers, always stamped). The in-memory resolver
//! requires the lead value column's `IsSorted` stamp; the file resolver pushes
//! range predicates down and only prunes better when the columns are sorted.
//!
//! [`IndexType::SecondaryByCopy`]: super::IndexType::SecondaryByCopy
//! [`secondary_by_reference`]: super::secondary_by_reference

use std::cmp::Ordering;
use std::ops::Range;
use std::sync::Arc;

use oxrdf::{GraphName, NamedNode, NamedOrBlankNode, Term};
use vortex_array::arrays::struct_::StructArrayExt;
use vortex_array::arrays::{PrimitiveArray, StructArray};
use vortex_array::dtype::DType;
use vortex_array::{ArrayRef, IntoArray};

use super::{IndexResolution, IndexedComponent, sorted_row_ids};
use crate::common::utils::{
    column_is_sorted, make_string_array, search_sorted_bounds, stamp_is_sorted,
};
use crate::error::{Result, VortexRdfError};
use crate::store::layouts::ResolvedLayout;
use crate::store::{QuadCodes, RawQuad};

use super::ServePlan;
#[cfg(feature = "file-io")]
use crate::common::utils::graph_name_str;
#[cfg(feature = "file-io")]
use vortex_array::scalar::Scalar;
#[cfg(feature = "file-io")]
use vortex_file::VortexFile;

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

impl Family {
    pub(crate) const ALL: [Family; 2] = [Family::Posg, Family::Ospg];

    pub(crate) fn s_col(self) -> &'static str {
        match self {
            Family::Posg => "_idx_posg_s",
            Family::Ospg => "_idx_ospg_s",
        }
    }

    pub(crate) fn p_col(self) -> &'static str {
        match self {
            Family::Posg => "_idx_posg_p",
            Family::Ospg => "_idx_ospg_p",
        }
    }

    pub(crate) fn o_col(self) -> &'static str {
        match self {
            Family::Posg => "_idx_posg_o",
            Family::Ospg => "_idx_ospg_o",
        }
    }

    pub(crate) fn g_col(self) -> &'static str {
        match self {
            Family::Posg => "_idx_posg_g",
            Family::Ospg => "_idx_ospg_g",
        }
    }

    pub(crate) fn rid_col(self) -> &'static str {
        match self {
            Family::Posg => "_idx_posg_rid",
            Family::Ospg => "_idx_ospg_rid",
        }
    }

    /// The five column names in the order the builders emit them (s, p, o, g,
    /// rid).
    pub(crate) fn column_names(self) -> [&'static str; 5] {
        [
            self.s_col(),
            self.p_col(),
            self.o_col(),
            self.g_col(),
            self.rid_col(),
        ]
    }

    /// This family's four quad-component columns, in primary `(s, p, o, g)`
    /// order — the columns a serve reads and relabels as the primaries.
    fn primary_columns(self) -> [&'static str; 4] {
        [self.s_col(), self.p_col(), self.o_col(), self.g_col()]
    }

    /// The column holding this family's leading sort key — the one binary
    /// searches probe and builders stamp `IsSorted`.
    fn lead_col(self) -> &'static str {
        match self {
            Family::Posg => self.p_col(),
            Family::Ospg => self.o_col(),
        }
    }

    /// The column holding the second sort key, sorted within each lead run —
    /// what a prefix search probes after bounding the lead.
    fn second_col(self) -> &'static str {
        match self {
            Family::Posg => self.o_col(),
            Family::Ospg => self.s_col(),
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
    fn key_positions(self) -> [usize; 4] {
        match self {
            Family::Posg => [2, 0, 1, 3],
            Family::Ospg => [1, 2, 0, 3],
        }
    }
}

/// Whether a struct dtype carries this index's ten columns — how stores detect
/// the index in an array or file schema without reading any data.
pub(crate) fn is_present(dtype: &DType) -> bool {
    match dtype {
        DType::Struct(fields, _) => {
            let has = |name: &str| fields.names().iter().any(|n| n.as_ref() == name);
            Family::ALL
                .iter()
                .all(|family| family.column_names().iter().all(|name| has(name)))
        }
        _ => false,
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
struct CopyProbe {
    family: Family,
    lead_term: String,
    second_term: Option<String>,
    resolves: IndexedComponent,
}

fn choose(
    subject: Option<&NamedOrBlankNode>,
    predicate: Option<&NamedNode>,
    object: Option<&Term>,
) -> Option<CopyProbe> {
    if subject.is_some() {
        return None;
    }
    match (predicate, object) {
        (Some(predicate), Some(object)) => Some(CopyProbe {
            family: Family::Posg,
            lead_term: predicate.to_string(),
            second_term: Some(object.to_string()),
            resolves: IndexedComponent::PredicateObject,
        }),
        (Some(predicate), None) => Some(CopyProbe {
            family: Family::Posg,
            lead_term: predicate.to_string(),
            second_term: None,
            resolves: IndexedComponent::Predicate,
        }),
        (None, Some(object)) => Some(CopyProbe {
            family: Family::Ospg,
            lead_term: object.to_string(),
            second_term: None,
            resolves: IndexedComponent::Object,
        }),
        (None, None) => None,
    }
}

/// Resolve a pattern against this index's copy columns in an in-memory base
/// array.
///
/// Binary-searches the chosen family's lead column for the probe term — and,
/// for a (p, o) prefix probe, the object column within the resulting run — and
/// slices out the paired row ids. Declines (so the store falls back to a mask
/// scan) when a needed column is absent, probe-incompatible, or the lead
/// column isn't stamped `IsSorted`: a per-chunk copy over a multi-chunk array
/// is not globally binary-searchable. The stamp is only ever set by builds
/// that sorted the family by its full comparator, which is also what makes the
/// second column sorted within each lead run and the prefix search valid.
pub(crate) fn resolve_in_memory(
    struct_arr: &StructArray,
    layout: &ResolvedLayout,
    subject: Option<&NamedOrBlankNode>,
    predicate: Option<&NamedNode>,
    object: Option<&Term>,
    _graph: Option<&GraphName>,
) -> Result<IndexResolution> {
    // Pick the family and probe(s) for this pattern shape, or decline it.
    let Some(probe) = choose(subject, predicate, object) else {
        return Ok(IndexResolution::Declined);
    };
    // Translate the term to the value columns' native probe value (a string,
    // or a dictionary code). Absent from the dictionary ⇒ nothing can match.
    let Some(lead_native) = layout.probe_scalar(&probe.lead_term) else {
        return Ok(IndexResolution::Empty);
    };
    // Route through the index only when its lead column is actually usable:
    // present, probe-castable, and stamped sorted for binary search.
    let Ok(lead_col) = struct_arr.unmasked_field_by_name(probe.family.lead_col()) else {
        return Ok(IndexResolution::Declined);
    };
    let Ok(lead_scalar) = lead_native.cast(lead_col.dtype()) else {
        return Ok(IndexResolution::Declined);
    };
    if !column_is_sorted(lead_col) {
        return Ok(IndexResolution::Declined);
    }
    // Left/right binary search bounds the [lo, hi) run of rows whose lead
    // component equals the probe.
    let (mut lo, mut hi) = search_sorted_bounds(lead_col, &lead_scalar)?;
    if lo == hi {
        return Ok(IndexResolution::Empty);
    }
    // Prefix probe: narrow the run by the second sort key, which is sorted
    // within the run by the family's comparator.
    if let Some(second_term) = &probe.second_term {
        let Some(second_native) = layout.probe_scalar(second_term) else {
            return Ok(IndexResolution::Empty);
        };
        let Ok(second_col) = struct_arr.unmasked_field_by_name(probe.family.second_col()) else {
            return Ok(IndexResolution::Declined);
        };
        let Ok(second_scalar) = second_native.cast(second_col.dtype()) else {
            return Ok(IndexResolution::Declined);
        };
        let run = second_col.slice(lo..hi).map_err(VortexRdfError::Vortex)?;
        let (run_lo, run_hi) = search_sorted_bounds(&run, &second_scalar)?;
        (lo, hi) = (lo + run_lo, lo + run_hi);
        if lo == hi {
            return Ok(IndexResolution::Empty);
        }
    }
    // Row ids of every quad in the matched run. They come out in the family's
    // order, so `sorted_row_ids` puts them back in base row order.
    let row_ids = sorted_row_ids(
        struct_arr
            .unmasked_field_by_name(probe.family.rid_col())
            .map_err(VortexRdfError::Vortex)?
            .slice(lo..hi)
            .map_err(VortexRdfError::Vortex)?,
    )?;
    Ok(IndexResolution::Resolved {
        row_ids,
        resolves: probe.resolves,
        // The matched quads are the contiguous `[lo, hi)` run of this family's
        // copy columns, so a read can slice them straight from the base instead
        // of gathering the primary columns at the row ids (see `ServePlan`).
        serve: Some(ServePlan::in_memory(
            probe.family.primary_columns(),
            probe.family.rid_col(),
            copy_decode_layout(layout),
            lo..hi,
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
/// — the file counterpart of [`resolve_in_memory`], reaching the columns
/// through a pushed-down scan instead of an in-memory binary search. A prefix
/// probe simply becomes two value constraints on the same scan.
#[cfg(feature = "file-io")]
pub(crate) async fn resolve_file(
    file: &VortexFile,
    layout: &ResolvedLayout,
    subject: Option<&NamedOrBlankNode>,
    predicate: Option<&NamedNode>,
    object: Option<&Term>,
    graph: Option<&GraphName>,
) -> Result<IndexResolution> {
    let Some(probe) = choose(subject, predicate, object) else {
        return Ok(IndexResolution::Declined);
    };
    // Term absent from the dictionary ⇒ the pattern provably matches nothing.
    let Some(lead_native) = layout.probe_scalar(&probe.lead_term) else {
        return Ok(IndexResolution::Empty);
    };
    let mut constraints: Vec<(&'static str, Scalar)> = vec![(probe.family.lead_col(), lead_native)];
    if let Some(second_term) = &probe.second_term {
        let Some(second_native) = layout.probe_scalar(second_term) else {
            return Ok(IndexResolution::Empty);
        };
        constraints.push((probe.family.second_col(), second_native));
    }
    let row_ids = super::scan_index_row_ids(file, &constraints, probe.family.rid_col()).await?;
    if row_ids.is_empty() {
        return Ok(IndexResolution::Empty);
    }
    Ok(IndexResolution::Resolved {
        row_ids,
        resolves: probe.resolves,
        // The copies hold the whole quad in family order, so the matched rows
        // are a contiguous run the store can stream directly (see `ServePlan`).
        serve: build_serve_plan(probe.family, layout, predicate, object, graph),
    })
}

/// Build the [`ServePlan`] letting the store stream a resolved pattern's quads
/// from this index's own copy columns, or `None` when a bound residual term has
/// no dictionary code (the pattern matches nothing — a case `match_pattern`
/// already short-circuits before resolving, so this is only a safety fallback
/// to the row-id path).
///
/// Every bound non-subject component (predicate, object, graph) becomes a term
/// equality on the family's matching column: the copies store each component as
/// one full term, so — unlike the primary layout's split TypedObject columns —
/// even the object probes as a single equality. The copy index declines
/// subject-bound patterns, so the subject never appears here.
#[cfg(feature = "file-io")]
fn build_serve_plan(
    family: Family,
    layout: &ResolvedLayout,
    predicate: Option<&NamedNode>,
    object: Option<&Term>,
    graph: Option<&GraphName>,
) -> Option<ServePlan> {
    let mut constraints: Vec<(&'static str, Scalar)> = Vec::new();
    for (column, term) in [
        (family.p_col(), predicate.map(|p| p.to_string())),
        (family.o_col(), object.map(|o| o.to_string())),
        (family.g_col(), graph.map(graph_name_str)),
    ] {
        let Some(term) = term else { continue };
        constraints.push((column, layout.probe_scalar(&term)?));
    }
    Some(ServePlan::file(
        family.primary_columns(),
        family.rid_col(),
        copy_decode_layout(layout),
        constraints,
    ))
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
pub(crate) struct CopyKey<V>(pub(crate) [V; 4]);

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

    /// The OSPG key of a quad given as `[s, p, o, g]`.
    pub(crate) fn ospg(spog: &[V; 4]) -> Self {
        Self([
            spog[2].clone(),
            spog[0].clone(),
            spog[1].clone(),
            spog[3].clone(),
        ])
    }
}

/// Append the ten copy columns from already-sorted `(key, row ID)` pairs.
/// Out-of-core builders call this directly with keys merged from disk runs in
/// global family order (`stamp_sorted = true`).
pub(crate) fn append_sorted_string_keys(
    field_names: &mut Vec<Arc<str>>,
    field_arrays: &mut Vec<ArrayRef>,
    posg: &[(CopyKey<String>, u32)],
    ospg: &[(CopyKey<String>, u32)],
    stamp_sorted: bool,
) {
    append_family_string_keys(field_names, field_arrays, Family::Posg, posg, stamp_sorted);
    append_family_string_keys(field_names, field_arrays, Family::Ospg, ospg, stamp_sorted);
}

fn append_family_string_keys(
    field_names: &mut Vec<Arc<str>>,
    field_arrays: &mut Vec<ArrayRef>,
    family: Family,
    keys: &[(CopyKey<String>, u32)],
    stamp_sorted: bool,
) {
    let [s_ix, p_ix, o_ix, g_ix] = family.key_positions();
    let col = |ix: usize| make_string_array(keys.iter().map(|(key, _)| key.0[ix].as_str()));
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

/// Code-column variant of [`append_sorted_string_keys`].
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
    use oxrdf::Literal;

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
        assert!(choose(Some(&s), Some(&p), Some(&o)).is_none());

        // Predicate and object bound: (p, o) prefix probe on the POSG family,
        // resolving both components.
        let probe = choose(None, Some(&p), Some(&o)).unwrap();
        assert_eq!(probe.family, Family::Posg);
        assert_eq!(probe.resolves, IndexedComponent::PredicateObject);
        assert_eq!(probe.lead_term, p.to_string());
        assert_eq!(probe.second_term.as_deref(), Some(o.to_string().as_str()));

        // Predicate-only patterns probe the POSG lead alone.
        let probe = choose(None, Some(&p), None).unwrap();
        assert_eq!(probe.family, Family::Posg);
        assert_eq!(probe.resolves, IndexedComponent::Predicate);
        assert!(probe.second_term.is_none());

        // Object-only patterns probe the OSPG lead.
        let probe = choose(None, None, Some(&o)).unwrap();
        assert_eq!(probe.family, Family::Ospg);
        assert_eq!(probe.resolves, IndexedComponent::Object);
        assert!(probe.second_term.is_none());

        // Nothing this index covers is bound: declines.
        assert!(choose(None, None, None).is_none());
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

        let ospg = CopyKey::ospg(&spog);
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
