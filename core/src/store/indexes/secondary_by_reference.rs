//! The [`IndexType::SecondaryByReference`] index: sorted object and predicate
//! value columns, each paired with the primary row IDs they point at. This
//! module owns both halves of the index's lifecycle — building the columns at
//! write time, and executing lookups against them at query time
//! ([`resolve_in_memory`] / [`resolve_file`], which produce primary row ids
//! directly for each backend).
//!
//! The value columns come in two encodings — term strings, or u32 dictionary
//! codes under the Dictionary layout — and in two scopes:
//!
//! - **Per-chunk** ([`append_columns`] / [`append_encoded_columns`]): each
//!   chunk sorts its own quads. Cheap and single-pass, but the concatenation
//!   of several chunks is *not* globally sorted, so the `IsSorted` stat is
//!   stamped only when the chunk spans the whole dataset. The chunk-local sort
//!   still pays off in a file-backed store: [`resolve_file`] pushes the probe
//!   down as a range predicate, and clustering the values shrinks each zone's
//!   min/max so the scan prunes to the few zones that can hold the probe.
//!   Zones are smaller than a chunk (8192 rows), so this holds within a chunk
//!   even though the whole column is unsorted.
//! - **Global** ([`GlobalIndexArrays`] and the `append_sorted_*` helpers):
//!   the complete dataset's sorted order, emitted per chunk as consecutive
//!   windows. Every value column is stamped `IsSorted`, and the concatenated
//!   columns stay globally binary-searchable.
//!
//! The two backends read that stamp differently. [`resolve_in_memory`] needs
//! it: binary search over a concatenation of per-chunk orders would be wrong,
//! so an unstamped column makes it decline and `match_pattern` falls back to a
//! mask scan. [`resolve_file`] never consults it — the range predicate is
//! correct whatever the order, and sortedness only decides how much prunes.
//!
//! [`IndexType::SecondaryByReference`]: super::IndexType::SecondaryByReference

use std::ops::Range;
use std::sync::Arc;

use oxrdf::{GraphName, NamedNode, NamedOrBlankNode, Term};
use vortex_array::{ArrayRef, IntoArray};
use vortex_array::arrays::{PrimitiveArray, StructArray};
use vortex_array::arrays::struct_::StructArrayExt;
use vortex_array::dtype::DType;

use crate::common::utils::{
    column_is_sorted, make_string_array, search_sorted_bounds, stamp_is_sorted,
};
use crate::error::{Result, VortexRdfError};
use crate::store::{QuadCodes, RawQuad};
use crate::store::layouts::ResolvedLayout;
use super::{sorted_row_ids, IndexResolution, IndexedComponent};

#[cfg(feature = "file-io")]
use vortex_file::VortexFile;

/// Whether a struct dtype carries this index's four columns — how stores
/// detect the index in an array or file schema without reading any data.
pub(crate) fn is_present(dtype: &DType) -> bool {
    match dtype {
        DType::Struct(fields, _) => {
            let has = |name: &str| fields.names().iter().any(|n| n.as_ref() == name);
            has("_idx_o_val") && has("_idx_o_rid") && has("_idx_p_val") && has("_idx_p_rid")
        }
        _ => false,
    }
}

/// The sorted value column to probe, its paired row-id column, the term to
/// probe for, and which pattern component a hit resolves.
struct ColumnProbe {
    value_column: &'static str,
    row_id_column: &'static str,
    probe_term: String,
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
fn choose(
    subject: Option<&NamedOrBlankNode>,
    predicate: Option<&NamedNode>,
    object: Option<&Term>,
) -> Option<ColumnProbe> {
    if subject.is_some() {
        return None;
    }
    if let Some(object) = object {
        return Some(ColumnProbe {
            value_column: "_idx_o_val",
            row_id_column: "_idx_o_rid",
            probe_term: object.to_string(),
            resolves: IndexedComponent::Object,
        });
    }
    if let Some(predicate) = predicate {
        return Some(ColumnProbe {
            value_column: "_idx_p_val",
            row_id_column: "_idx_p_rid",
            probe_term: predicate.to_string(),
            resolves: IndexedComponent::Predicate,
        });
    }
    None
}

/// Resolve a pattern against this index's columns in an in-memory base array.
///
/// Binary-searches the chosen sorted value column for the probe term and slices
/// out the paired row ids — the base rows whose indexed component equals the
/// term. Declines (so the store falls back to a mask scan) when the value
/// column is absent, probe-incompatible, or not stamped `IsSorted`: a per-chunk
/// index over a multi-chunk array is not globally binary-searchable.
pub(crate) fn resolve_in_memory(
    struct_arr: &StructArray,
    layout: &ResolvedLayout,
    subject: Option<&NamedOrBlankNode>,
    predicate: Option<&NamedNode>,
    object: Option<&Term>,
    _graph: Option<&GraphName>,
) -> Result<IndexResolution> {
    // Pick the column pair for this pattern shape, or decline it entirely.
    let Some(probe) = choose(subject, predicate, object) else {
        return Ok(IndexResolution::Declined);
    };
    // Translate the term to the value column's native probe value (a string, or
    // a dictionary code). Absent from the dictionary ⇒ nothing can match.
    let Some(native) = layout.probe_scalar(&probe.probe_term) else {
        return Ok(IndexResolution::Empty);
    };
    // Route through the index only when its value column is actually usable:
    // present, probe-castable, and stamped sorted for binary search.
    let Ok(val_col) = struct_arr.unmasked_field_by_name(probe.value_column) else {
        return Ok(IndexResolution::Declined);
    };
    let Ok(scalar) = native.cast(val_col.dtype()) else {
        return Ok(IndexResolution::Declined);
    };
    if !column_is_sorted(val_col) {
        return Ok(IndexResolution::Declined);
    }
    // Left/right binary search bounds the [lo, hi) run of rows equal to the
    // probe; an empty run means the term is present in the schema but absent
    // from the data.
    let (lo, hi) = search_sorted_bounds(val_col, &scalar)?;
    if lo == hi {
        return Ok(IndexResolution::Empty);
    }
    // Row ids of every quad whose indexed component equals the probe term.
    // They come out in the index's order (`_idx_*_rid` is ordered by value, not
    // by row), so `sorted_row_ids` puts them back in base row order.
    let row_ids = sorted_row_ids(
        struct_arr
            .unmasked_field_by_name(probe.row_id_column)
            .map_err(VortexRdfError::Vortex)?
            .slice(lo..hi)
            .map_err(VortexRdfError::Vortex)?,
    )?;
    Ok(IndexResolution::Resolved {
        row_ids,
        resolves: probe.resolves,
    })
}

/// Resolve a pattern against this index's columns in a file-backed store — the
/// file counterpart of [`resolve_in_memory`], reaching the columns through a
/// pushed-down scan instead of an in-memory binary search.
#[cfg(feature = "file-io")]
pub(crate) async fn resolve_file(
    file: &VortexFile,
    layout: &ResolvedLayout,
    subject: Option<&NamedOrBlankNode>,
    predicate: Option<&NamedNode>,
    object: Option<&Term>,
    _graph: Option<&GraphName>,
) -> Result<IndexResolution> {
    let Some(probe) = choose(subject, predicate, object) else {
        return Ok(IndexResolution::Declined);
    };
    // Term absent from the dictionary ⇒ the pattern provably matches nothing.
    let Some(native) = layout.probe_scalar(&probe.probe_term) else {
        return Ok(IndexResolution::Empty);
    };
    let row_ids =
        super::scan_index_row_ids(file, &[(probe.value_column, native)], probe.row_id_column)
            .await?;
    if row_ids.is_empty() {
        return Ok(IndexResolution::Empty);
    }
    Ok(IndexResolution::Resolved {
        row_ids,
        resolves: probe.resolves,
    })
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
        let mut pairs: Vec<(&str, u32)> = quads.iter()
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
        let mut pairs: Vec<(u32, u32)> = column.iter()
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
        "_idx_o_val".into(), "_idx_o_rid".into(),
        "_idx_p_val".into(), "_idx_p_rid".into(),
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
        "_idx_o_val".into(), "_idx_o_rid".into(),
        "_idx_p_val".into(), "_idx_p_rid".into(),
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
            let mut pairs: Vec<(u32, u32)> = column.iter()
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
            ("_idx_o_val", &self.o_val, true),
            ("_idx_o_rid", &self.o_rid, false),
            ("_idx_p_val", &self.p_val, true),
            ("_idx_p_rid", &self.p_rid, false),
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
    use oxrdf::Literal;

    #[test]
    fn choose_component_selection() {
        let s = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s").unwrap());
        let p = NamedNode::new("http://example.org/p").unwrap();
        let o = Term::Literal(Literal::new_simple_literal("o"));

        // A bound subject declines: the primary sorted `s` column is the
        // better access path than this index.
        assert!(choose(Some(&s), Some(&p), Some(&o)).is_none());

        // Object preferred over predicate when both are bound.
        let probe = choose(None, Some(&p), Some(&o)).unwrap();
        assert_eq!(probe.resolves, IndexedComponent::Object);
        assert_eq!(probe.value_column, "_idx_o_val");
        assert_eq!(probe.row_id_column, "_idx_o_rid");
        assert_eq!(probe.probe_term, o.to_string());

        // Predicate-only patterns use the predicate side.
        let probe = choose(None, Some(&p), None).unwrap();
        assert_eq!(probe.resolves, IndexedComponent::Predicate);
        assert_eq!(probe.value_column, "_idx_p_val");
        assert_eq!(probe.row_id_column, "_idx_p_rid");
        assert_eq!(probe.probe_term, p.to_string());

        // Nothing this index covers is bound: declines.
        assert!(choose(None, None, None).is_none());
    }
}
