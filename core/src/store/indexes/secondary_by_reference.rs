//! The [`IndexType::SecondaryByReference`] index: sorted object and predicate
//! value columns, each paired with the primary row IDs they point at. This
//! module owns both halves of the index's lifecycle — building the columns at
//! write time, and executing lookups against them at query time
//! (`resolve_in_memory` / `resolve_file`, which produce primary row ids
//! directly for each backend).
//!
//! The value columns come in two encodings — term strings, or u32 dictionary
//! codes under the Dictionary layout — and in two scopes:
//!
//! - **Per-chunk** (`append_columns` / `append_encoded_columns`): each
//!   chunk sorts its own quads. Cheap and single-pass, but the concatenation
//!   of several chunks is *not* globally sorted, so the `IsSorted` stat is
//!   stamped only when the chunk spans the whole dataset. The chunk-local sort
//!   still pays off in a file-backed store: `resolve_file` pushes the probe
//!   down as an equality predicate (falsified against each zone's min/max),
//!   and clustering the values shrinks each zone's min/max so the scan prunes
//!   to the few zones that can hold the probe. Zones are smaller than a chunk
//!   (8192 rows), so this holds within a chunk even though the whole column is
//!   unsorted.
//! - **Global** (`GlobalIndexArrays` and the `append_sorted_*` helpers):
//!   the complete dataset's sorted order, emitted per chunk as consecutive
//!   windows. Every value column is stamped `IsSorted`, and the concatenated
//!   columns stay globally binary-searchable.
//!
//! Both backends need global sortedness to binary-search, and neither depends
//! on it to be correct — they differ only in what they do without it.
//! `resolve_in_memory` declines an unsorted column outright (searching a
//! concatenation of per-chunk orders would be wrong) and `match_pattern` falls
//! back to a mask scan; `resolve_file` falls back on its own, to the
//! pushed-down equality, which answers whatever the order — there, sortedness
//! decides only whether the matched run can be *located* (and, failing that,
//! how much of the scan prunes).
//!
//! [`IndexType::SecondaryByReference`]: super::IndexType::SecondaryByReference
// Inspired by https://clickhouse.com/blog/projections-secondary-indices

use std::ops::Range;
use std::sync::Arc;

use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::struct_::StructArrayExt;
use vortex_array::dtype::DType;
use vortex_array::{ArrayRef, IntoArray};

#[cfg(feature = "file-io")]
use super::FileServePlan;
use super::components::{child_struct, child_struct_dtype};
use super::{InMemoryServePlan, IndexResolution, IndexedComponent, ResolvedRowIds, sorted_row_ids};
use crate::error::{Result, VortexRdfError};
use crate::store::RawQuad;
use crate::store::array::{make_string_array, stamp_is_sorted};
use crate::store::layouts::dictionary::QuadCodes;
use crate::store::layouts::{PatternCodes, QuadPattern, TermRef};

pub(crate) const REF_O_COMPONENT: &str = "index:ref-o";
pub(crate) const REF_P_COMPONENT: &str = "index:ref-p";

/// The persisted child's struct dtype: sorted values (strings, or u32 codes
/// under the Dictionary layout) plus the u32 primary row id.
// This and the child-chunk builders below are consumed only by the
// external-sort builder, compiled out on wasm (see the module gate in
// `store::builders`).
#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
pub(crate) fn ref_child_dtype(encoded: bool) -> DType {
    use vortex_array::dtype::{Nullability, PType};
    let val = if encoded {
        DType::Primitive(PType::U32, Nullability::NonNullable)
    } else {
        DType::Utf8(Nullability::NonNullable)
    };
    child_struct_dtype(
        &CHILD_COLUMNS,
        vec![val, DType::Primitive(PType::U32, Nullability::NonNullable)],
    )
}

/// One chunk of a reference component's persisted child from a window of its
/// merged `(value, row id)` pairs.
#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
pub(crate) fn ref_child_chunk_strings(pairs: &[(String, u32)]) -> Result<ArrayRef> {
    let val = make_string_array(pairs.iter().map(|(v, _)| v.as_str()));
    stamp_is_sorted(&val);
    let rid = PrimitiveArray::from_iter(pairs.iter().map(|(_, rid)| *rid)).into_array();
    child_struct(&CHILD_COLUMNS, vec![val, rid], pairs.len()).map(|a| a.into_array())
}

/// Code-column variant of [`ref_child_chunk_strings`].
#[cfg_attr(all(target_arch = "wasm32", target_os = "unknown"), allow(dead_code))]
pub(crate) fn ref_child_chunk_codes(pairs: &[(u32, u32)]) -> Result<ArrayRef> {
    let val = PrimitiveArray::from_iter(pairs.iter().map(|(code, _)| *code)).into_array();
    stamp_is_sorted(&val);
    let rid = PrimitiveArray::from_iter(pairs.iter().map(|(_, rid)| *rid)).into_array();
    child_struct(&CHILD_COLUMNS, vec![val, rid], pairs.len()).map(|a| a.into_array())
}

/// Column names inside a reference component's persisted child.
pub(crate) const CHILD_COLUMNS: [&str; 2] = ["val", "rid"];
pub(crate) const CHILD_VAL_COL: &str = "val";
pub(crate) const CHILD_RID_COL: &str = "rid";
pub(crate) const O_IMPLEMENTATION: &str = "secondary-by-reference/o";
pub(crate) const P_IMPLEMENTATION: &str = "secondary-by-reference/p";

const O_SOURCE_COLUMNS: [&str; 2] = [RefRole::O.val_col(), RefRole::O.rid_col()];
const P_SOURCE_COLUMNS: [&str; 2] = [RefRole::P.val_col(), RefRole::P.rid_col()];

/// This index's persisted-child role table — one `{val, rid}` table per
/// covered role — feeding every generic loop in the hub (spec push,
/// row-space split, schema detection, the slug registry); see
/// [`IndexType::component_roles`](super::IndexType::component_roles). Built
/// from the [`RefRole`] accessors so each name keeps exactly one spelling.
pub(crate) const ROLES: [super::ComponentRole; 2] = [
    super::ComponentRole {
        name: RefRole::O.component_name(),
        slug: RefRole::O.component_slug(),
        source_columns: &O_SOURCE_COLUMNS,
        child_columns: &CHILD_COLUMNS,
        lead_source: RefRole::O.val_col(),
    },
    super::ComponentRole {
        name: RefRole::P.component_name(),
        slug: RefRole::P.component_slug(),
        source_columns: &P_SOURCE_COLUMNS,
        child_columns: &CHILD_COLUMNS,
        lead_source: RefRole::P.val_col(),
    },
];

/// This index's four columns: a sorted copy of a component's values paired
/// with the primary row id each value came from.
pub(crate) const O_VAL_COL: &str = "_idx_o_val";
pub(crate) const O_RID_COL: &str = "_idx_o_rid";
pub(crate) const P_VAL_COL: &str = "_idx_p_val";
pub(crate) const P_RID_COL: &str = "_idx_p_rid";

/// One of the two quad components this index covers, named after it. Each
/// role owns one `{val, rid}` component — its name, implementation slug, and
/// the row-space column pair it is assembled from — so the association is
/// stated once here (and in the [`ROLES`] rows built from these accessors)
/// instead of at every roster site.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum RefRole {
    /// Object values.
    O,
    /// Predicate values.
    P,
}

impl RefRole {
    /// The persisted child's component name.
    pub(crate) const fn component_name(self) -> &'static str {
        match self {
            RefRole::O => REF_O_COMPONENT,
            RefRole::P => REF_P_COMPONENT,
        }
    }

    /// The persisted child's implementation slug.
    pub(crate) const fn component_slug(self) -> &'static str {
        match self {
            RefRole::O => O_IMPLEMENTATION,
            RefRole::P => P_IMPLEMENTATION,
        }
    }

    /// The row-space column holding this role's sorted values.
    pub(crate) const fn val_col(self) -> &'static str {
        match self {
            RefRole::O => O_VAL_COL,
            RefRole::P => P_VAL_COL,
        }
    }

    /// The row-space column holding the primary row id paired with each value.
    pub(crate) const fn rid_col(self) -> &'static str {
        match self {
            RefRole::O => O_RID_COL,
            RefRole::P => P_RID_COL,
        }
    }
}

/// The covered role to probe (which names its component and columns), the
/// term to probe for, and which pattern component a hit resolves.
struct ColumnProbe<'a> {
    role: RefRole,
    probe_term: TermRef<'a>,
    resolves: IndexedComponent,
}

/// The column pair and component this index would use for a pattern shape,
/// independent of any backend — the shared front half of both resolvers.
///
/// A bound subject declines the index: the primary `s` column (binary-searched
/// or zone-pruned) is the better access path there. When both object and
/// predicate are bound, the object side is chosen — object equality is usually
/// the more selective constraint. `None` when nothing this index covers is
/// bound.
fn choose<'a>(pattern: QuadPattern<'a>) -> Option<ColumnProbe<'a>> {
    if pattern.subject.is_some() {
        return None;
    }
    if let Some(object) = pattern.object {
        return Some(ColumnProbe {
            role: RefRole::O,
            probe_term: TermRef::Object(object),
            resolves: IndexedComponent::Object,
        });
    }
    if let Some(predicate) = pattern.predicate {
        return Some(ColumnProbe {
            role: RefRole::P,
            probe_term: TermRef::Predicate(predicate),
            resolves: IndexedComponent::Predicate,
        });
    }
    None
}

/// Resolve a pattern against this index's in-memory component.
///
/// Binary-searches the sorted value column for the probe term and slices out
/// the paired row ids — the base rows whose indexed component equals the
/// term. Declines (so the store falls back to a mask scan) when the covered
/// role's component is absent, probe-incompatible, or not globally sorted
/// (`IndexComponent::sorted` — per-chunk sorted data is not
/// binary-searchable).
pub(crate) fn resolve_in_memory(
    components: &[super::IndexComponent],
    pattern: QuadPattern<'_>,
    codes: &mut PatternCodes,
) -> Result<IndexResolution<InMemoryServePlan>> {
    // Pick the column pair for this pattern shape, or decline it entirely.
    let Some(probe) = choose(pattern) else {
        return Ok(IndexResolution::Declined);
    };
    // Route through the index only when the role's component exists and is
    // globally sorted — the writer's provenance, not a stamp inspection.
    let Some(component) =
        super::IndexComponent::find_sorted(components, probe.role.component_name())
    else {
        return Ok(IndexResolution::Declined);
    };
    // Translate the term to the value column's native probe value (a string, or
    // a dictionary code). Absent from the dictionary ⇒ nothing can match. The
    // probe term is the pattern's own predicate or object, so this shares the
    // match's resolution cache rather than searching the dictionary again.
    let Some(native) = codes.probe_scalar(probe.probe_term)? else {
        return Ok(IndexResolution::Empty);
    };
    // First genuine use of a `from_bytes`-adopted component: this is where a
    // deferred child canonicalizes.
    let rows = component.rows()?;
    // Binary search bounds the run of rows equal to the probe — through the
    // component's cached probe when the column resolves one; an empty run
    // means the term is present in the schema but absent from the data.
    let Some(run) = super::component_probe_run(component, CHILD_VAL_COL, &native, None)? else {
        return Ok(IndexResolution::Declined);
    };
    if run.is_empty() {
        return Ok(IndexResolution::Empty);
    }
    // Row ids of every quad whose indexed component equals the probe term.
    // They come out in the index's order (the rid column is ordered by value,
    // not by row), so `sorted_row_ids` puts them back in base row order.
    let row_ids = sorted_row_ids(
        rows.unmasked_field_by_name(CHILD_RID_COL)
            .map_err(VortexRdfError::Vortex)?
            .slice(run)
            .map_err(VortexRdfError::Vortex)?,
    )?;
    Ok(IndexResolution::Resolved {
        row_ids: ResolvedRowIds::Eager(row_ids),
        resolves: probe.resolves,
        // A back-reference index stores no whole quads to serve from.
        serve: None,
    })
}

/// Resolve a pattern against this index's columns in a file-backed store — the
/// file counterpart of [`resolve_in_memory`], reaching the columns through the
/// child's cached chunk probes (the file mirror of the in-memory binary
/// search) or, failing that, a pushed-down scan.
///
/// On a globally sorted child the matched rows are one contiguous run of the
/// value column, so a binary search over the chunk probes bounds it without
/// reading the column: a small run's row ids then come from rid point reads, a
/// wide one from a rid scan restricted to the located range — either way
/// without the filter evaluation (and the pruning it depends on) a scan of the
/// whole child pays. Anything the probes decline — an unsorted child, a string
/// value column, an encoding resolving no probe — falls back to the pushed-down
/// scan, whose filter answers regardless of order.
///
/// This index stores no whole quads, so every outcome here is an eager row-id
/// resolution with no serving plan; the store gathers the matched quads from
/// the primary columns (point reads of their own, for a small id set).
#[cfg(feature = "file-io")]
pub(crate) async fn resolve_file(
    file: &crate::store::native_file::NativeStoreFile,
    pattern: QuadPattern<'_>,
    codes: &mut PatternCodes,
) -> Result<IndexResolution<FileServePlan>> {
    let Some(probe) = choose(pattern) else {
        return Ok(IndexResolution::Declined);
    };
    // Map the probed role onto its persisted child; be graceful when the
    // child is absent (a foreign writer could omit one role).
    let Some((descriptor, reader)) = file
        .component_reader(probe.role.component_name())
        .map_err(VortexRdfError::Vortex)?
    else {
        return Ok(IndexResolution::Declined);
    };
    // Term absent from the dictionary ⇒ the pattern provably matches nothing.
    let Some(native) = codes.probe_scalar(probe.probe_term)? else {
        return Ok(IndexResolution::Empty);
    };

    let name = probe.role.component_name();
    if let Some(range) = locate_run(file, probe.role, &native, descriptor.sorted).await? {
        // A located empty run proves the term absent from the data — the
        // short-circuit an empty scan reaches only after reading it.
        if range.is_empty() {
            return Ok(IndexResolution::Empty);
        }
        // The located range is exactly this index's matched rows: the value
        // column is the one and only constraint.
        let row_ids = if (range.end - range.start) as usize
            <= crate::store::selection::POINT_GATHER_MAX_ROWS
        {
            super::rid_point_reads(file, name, CHILD_RID_COL, range.clone()).await?
        } else {
            Some(super::scan_located_row_ids(reader.clone(), CHILD_RID_COL, range).await?)
        };
        // `None` is a mid-read decline (an unprobeable rid chunk); the scan
        // below reads the same rows the long way.
        if let Some(row_ids) = row_ids {
            return Ok(IndexResolution::Resolved {
                row_ids: ResolvedRowIds::Eager(row_ids),
                resolves: probe.resolves,
                // A back-reference index stores no whole quads to serve from.
                serve: None,
            });
        }
    }
    super::resolve_eager_from_scan(
        reader,
        &[(CHILD_VAL_COL, native)],
        CHILD_RID_COL,
        probe.resolves,
    )
    .await
}

/// The child rows whose indexed value equals `native`, bounded by binary-
/// searching the value column's cached chunk probes — the file mirror of
/// [`resolve_in_memory`]'s search, reading only the chunks the bisection
/// touches.
///
/// `None` declines the location (the caller falls back to its pushed-down
/// scan): a child that is not globally sorted, a probe value that is not an
/// integer (the string value columns), or a chunk whose encoding resolves no
/// probe.
#[cfg(feature = "file-io")]
async fn locate_run(
    file: &crate::store::native_file::NativeStoreFile,
    role: RefRole,
    native: &vortex_array::scalar::Scalar,
    sorted: bool,
) -> Result<Option<Range<u64>>> {
    if !sorted {
        return Ok(None);
    }
    let Ok(needle) = u64::try_from(native) else {
        return Ok(None);
    };
    let Some(chunks) = file.component_column_chunks(role.component_name(), CHILD_VAL_COL) else {
        return Ok(None);
    };
    chunks
        .bounds(needle, &file.segment_source(), file.session())
        .await
        .map_err(VortexRdfError::Vortex)
}

/// Test-only hook exposing the run [`resolve_file`] locates for a pattern —
/// `None` when the location declines and the resolution falls back to its
/// pushed-down scan — so tests can assert engagement instead of inferring it
/// from results.
#[cfg(all(test, feature = "file-io"))]
pub(crate) async fn debug_located_run(
    file: &crate::store::native_file::NativeStoreFile,
    pattern: QuadPattern<'_>,
    codes: &mut PatternCodes,
) -> Result<Option<Range<u64>>> {
    let Some(probe) = choose(pattern) else {
        return Ok(None);
    };
    let Some((descriptor, _)) = file
        .component_reader(probe.role.component_name())
        .map_err(VortexRdfError::Vortex)?
    else {
        return Ok(None);
    };
    let Some(native) = codes.probe_scalar(probe.probe_term)? else {
        return Ok(None);
    };
    locate_run(file, probe.role, &native, descriptor.sorted).await
}

/// Append the four reference secondary-index columns for one chunk, sorting
/// the chunk's own quads: `_idx_o_val`/`_idx_o_rid` (sorted objects) and
/// `_idx_p_val`/`_idx_p_rid` (sorted predicates).
///
/// `start_row` is the global row ID of the first quad in `quads`, so per-chunk
/// index builders can emit row IDs that address the fully assembled array.
/// An empty `quads` slice yields empty columns with the correct dtypes.
///
/// `whole_dataset` must be `true` only when `quads` is the entire dataset
/// (single-chunk builds): the chunk-local sort is then the global order and
/// the value columns are stamped `IsSorted` for binary-search routing.
pub(crate) fn append_columns(
    field_names: &mut Vec<Arc<str>>,
    field_arrays: &mut Vec<ArrayRef>,
    quads: &[RawQuad],
    start_row: u32,
    whole_dataset: bool,
) {
    let sorted_pairs = |term_of: fn(&RawQuad) -> &str| -> Vec<(&str, u32)> {
        let mut pairs: Vec<(&str, u32)> = quads
            .iter()
            .enumerate()
            .map(|(i, q)| (term_of(q), start_row + i as u32))
            .collect();
        pairs.sort_unstable();
        pairs
    };
    append_sorted_string_pairs(
        field_names,
        field_arrays,
        &sorted_pairs(|q| &q.o),
        &sorted_pairs(|q| &q.p),
        whole_dataset,
    );
}

/// Dictionary-layout variant of [`append_columns`]: `_idx_o_val`/`_idx_p_val`
/// hold the terms' u32 dictionary codes instead of strings. Sorting codes is
/// order-equivalent to sorting the term strings (sorted-dictionary codes are
/// lexicographic ranks), so the index stays binary-searchable — queries
/// translate the pattern term to its code first.
pub(crate) fn append_encoded_columns(
    field_names: &mut Vec<Arc<str>>,
    field_arrays: &mut Vec<ArrayRef>,
    codes: &QuadCodes,
    start_row: u32,
    whole_dataset: bool,
) {
    let sorted_pairs = |column: &[u32]| -> Vec<(u32, u32)> {
        let mut pairs: Vec<(u32, u32)> = column
            .iter()
            .enumerate()
            .map(|(i, &code)| (code, start_row + i as u32))
            .collect();
        pairs.sort_unstable();
        pairs
    };
    append_sorted_code_pairs(
        field_names,
        field_arrays,
        &sorted_pairs(&codes.o),
        &sorted_pairs(&codes.p),
        whole_dataset,
    );
}

/// Append the four index columns from already-sorted (term, row ID) pairs.
/// Out-of-core builders call this directly with pairs merged from disk runs
/// in global order (`stamp_sorted = true`).
pub(crate) fn append_sorted_string_pairs(
    field_names: &mut Vec<Arc<str>>,
    field_arrays: &mut Vec<ArrayRef>,
    o_pairs: &[(impl AsRef<str>, u32)],
    p_pairs: &[(impl AsRef<str>, u32)],
    stamp_sorted: bool,
) {
    let o_val = make_string_array(o_pairs.iter().map(|(s, _)| s.as_ref()));
    let p_val = make_string_array(p_pairs.iter().map(|(s, _)| s.as_ref()));
    if stamp_sorted {
        stamp_is_sorted(&o_val);
        stamp_is_sorted(&p_val);
    }
    field_names.extend_from_slice(&[
        O_VAL_COL.into(),
        O_RID_COL.into(),
        P_VAL_COL.into(),
        P_RID_COL.into(),
    ]);
    field_arrays.extend([
        o_val,
        PrimitiveArray::from_iter(o_pairs.iter().map(|(_, rid)| *rid)).into_array(),
        p_val,
        PrimitiveArray::from_iter(p_pairs.iter().map(|(_, rid)| *rid)).into_array(),
    ]);
}

/// Code-column variant of [`append_sorted_string_pairs`].
pub(crate) fn append_sorted_code_pairs(
    field_names: &mut Vec<Arc<str>>,
    field_arrays: &mut Vec<ArrayRef>,
    o_pairs: &[(u32, u32)],
    p_pairs: &[(u32, u32)],
    stamp_sorted: bool,
) {
    let o_val = PrimitiveArray::from_iter(o_pairs.iter().map(|(code, _)| *code)).into_array();
    let p_val = PrimitiveArray::from_iter(p_pairs.iter().map(|(code, _)| *code)).into_array();
    if stamp_sorted {
        stamp_is_sorted(&o_val);
        stamp_is_sorted(&p_val);
    }
    field_names.extend_from_slice(&[
        O_VAL_COL.into(),
        O_RID_COL.into(),
        P_VAL_COL.into(),
        P_RID_COL.into(),
    ]);
    field_arrays.extend([
        o_val,
        PrimitiveArray::from_iter(o_pairs.iter().map(|(_, rid)| *rid)).into_array(),
        p_val,
        PrimitiveArray::from_iter(p_pairs.iter().map(|(_, rid)| *rid)).into_array(),
    ]);
}

/// The complete dataset's secondary-index columns in global sorted order,
/// built once by in-memory builders and sliced per chunk: chunk `i` carries
/// window `[i·C, (i+1)·C)` of the same order, so the concatenation across
/// chunks is itself the globally sorted index.
pub(crate) struct GlobalIndexArrays {
    o_val: ArrayRef,
    o_rid: ArrayRef,
    p_val: ArrayRef,
    p_rid: ArrayRef,
}

impl GlobalIndexArrays {
    /// Sort by term strings. Row IDs are the quads' positions in `quads`
    /// (the builder must pass the dataset in final row order), so the sort is
    /// just a u32 permutation — no per-term string copies.
    pub(crate) fn from_quads(quads: &[RawQuad]) -> Self {
        let perm_by = |term_of: fn(&RawQuad) -> &str| -> Vec<u32> {
            let mut perm: Vec<u32> = (0..quads.len() as u32).collect();
            perm.sort_unstable_by(|&a, &b| {
                term_of(&quads[a as usize]).cmp(term_of(&quads[b as usize]))
            });
            perm
        };
        let o_perm = perm_by(|q| &q.o);
        let p_perm = perm_by(|q| &q.p);
        Self::from_arrays(
            make_string_array(o_perm.iter().map(|&i| quads[i as usize].o.as_str())),
            o_perm,
            make_string_array(p_perm.iter().map(|&i| quads[i as usize].p.as_str())),
            p_perm,
        )
    }

    /// Dictionary-layout variant: sort the u32 codes.
    pub(crate) fn from_codes(codes: &QuadCodes) -> Self {
        let sorted = |column: &[u32]| -> (ArrayRef, Vec<u32>) {
            let mut pairs: Vec<(u32, u32)> = column
                .iter()
                .enumerate()
                .map(|(i, &code)| (code, i as u32))
                .collect();
            pairs.sort_unstable();
            (
                PrimitiveArray::from_iter(pairs.iter().map(|(code, _)| *code)).into_array(),
                pairs.into_iter().map(|(_, rid)| rid).collect(),
            )
        };
        let (o_val, o_perm) = sorted(&codes.o);
        let (p_val, p_perm) = sorted(&codes.p);
        Self::from_arrays(o_val, o_perm, p_val, p_perm)
    }

    fn from_arrays(o_val: ArrayRef, o_perm: Vec<u32>, p_val: ArrayRef, p_perm: Vec<u32>) -> Self {
        stamp_is_sorted(&o_val);
        stamp_is_sorted(&p_val);
        Self {
            o_val,
            o_rid: PrimitiveArray::from_iter(o_perm).into_array(),
            p_val,
            p_rid: PrimitiveArray::from_iter(p_perm).into_array(),
        }
    }

    /// Append window `range` of the global order as one chunk's index columns.
    /// Value slices are re-stamped `IsSorted` (a slice of a sorted array is
    /// sorted, but slicing does not propagate the stat).
    pub(crate) fn append_slice(
        &self,
        field_names: &mut Vec<Arc<str>>,
        field_arrays: &mut Vec<ArrayRef>,
        range: Range<usize>,
    ) -> Result<()> {
        for (name, arr, is_val) in [
            (O_VAL_COL, &self.o_val, true),
            (O_RID_COL, &self.o_rid, false),
            (P_VAL_COL, &self.p_val, true),
            (P_RID_COL, &self.p_rid, false),
        ] {
            let sliced = arr.slice(range.clone()).map_err(VortexRdfError::Vortex)?;
            if is_val {
                stamp_is_sorted(&sliced);
            }
            field_names.push(name.into());
            field_arrays.push(sliced);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use oxrdf::{Literal, NamedNode, NamedOrBlankNode, Term};

    #[test]
    fn choose_component_selection() {
        let s = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s").unwrap());
        let p = NamedNode::new("http://example.org/p").unwrap();
        let o = Term::Literal(Literal::new_simple_literal("o"));

        // A bound subject declines: the primary sorted `s` column is the
        // better access path than this index.
        assert!(choose(QuadPattern::new(Some(&s), Some(&p), Some(&o), None)).is_none());

        // Object preferred over predicate when both are bound.
        let probe = choose(QuadPattern::new(None, Some(&p), Some(&o), None)).unwrap();
        assert_eq!(probe.resolves, IndexedComponent::Object);
        assert_eq!(probe.role.val_col(), "_idx_o_val");
        assert_eq!(probe.probe_term.to_string(), o.to_string());

        // Predicate-only patterns use the predicate side.
        let probe = choose(QuadPattern::new(None, Some(&p), None, None)).unwrap();
        assert_eq!(probe.resolves, IndexedComponent::Predicate);
        assert_eq!(probe.role.val_col(), "_idx_p_val");
        assert_eq!(probe.probe_term.to_string(), p.to_string());

        // Nothing this index covers is bound: declines.
        assert!(choose(QuadPattern::new(None, None, None, None)).is_none());
    }
}
