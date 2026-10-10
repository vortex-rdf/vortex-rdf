//! The [`IndexType::SecondaryByReference`] index: sorted object and predicate
//! value columns, each paired with the primary row IDs they point at. This
//! module owns both halves of the index's lifecycle — building the columns at
//! write time, and executing lookups against them at query time
//! (`resolve_in_memory` / `resolve_file`, which produce primary row ids
//! directly for each backend).
//!
//! The value columns come in two encodings — term strings, or dictionary
//! codes ([`TermCode`]) under the Dictionary layout — and are always built over the complete
//! dataset in one global sort: [`GlobalReferenceArrays`] for the in-memory
//! builders, merged `(value, row id)` spill runs for the out-of-core one.
//! Both hand the columns over as this index's two persisted children
//! (`{val, rid}` per covered family), value column stamped `IsSorted`.
//!
//! Both backends need global sortedness to binary-search, and neither depends
//! on it to be correct — they differ only in what they do without it, which
//! is what a child declaring itself unsorted (this crate never writes one,
//! but the wire format can carry one) falls back to. `resolve_in_memory`
//! declines outright and `match_pattern` mask-scans; `resolve_file` falls
//! back to the pushed-down equality, which answers whatever the order —
//! there, sortedness decides only whether the matched run can be *located*
//! (and, failing that, how much of the scan prunes).
//!
//! [`IndexType::SecondaryByReference`]: super::IndexType::SecondaryByReference

#[cfg(feature = "file-io")]
use std::ops::Range;
#[cfg(feature = "file-io")]
use std::sync::Arc;

use vortex_array::arrays::PrimitiveArray;
use vortex_array::arrays::struct_::StructArrayExt;
use vortex_array::{ArrayRef, IntoArray};

#[cfg(feature = "file-io")]
use super::FileServePlan;
use super::components::child_struct;
use super::{
    COL_RID, InMemoryServePlan, IndexResolution, LazyRowIds, ResolvedRoles, ResolvedRowIds,
};
use crate::error::{Result, VortexRdfError};
use crate::store::RawQuad;
use crate::store::array::{make_string_array, stamp_is_sorted};
use crate::store::layouts::dictionary::QuadCodes;
use crate::store::layouts::{PatternCodes, QuadPattern, TermRef};
use crate::store::schema::{RowId, TermCode};

/// The value column of a reference component's persisted child; the row id
/// beside it is the name every index child shares ([`COL_RID`]).
pub(crate) const COL_VAL: &str = "val";
const CHILD_COLUMNS: [&str; 2] = [COL_VAL, COL_RID];

/// This index's persisted-child identity table — one `{val, rid}` table per
/// covered family — feeding every generic loop in the hub (the slug registry
/// and the roster it builds); see
/// [`IndexType::component_identities`](super::IndexType::component_identities).
/// Built from the [`RefFamily`] accessors so each name keeps exactly one
/// spelling.
pub(crate) const IDENTITIES: [super::ComponentIdentity; 2] = [
    super::ComponentIdentity {
        name: RefFamily::Object.component_name(),
        slug: RefFamily::Object.component_slug(),
    },
    super::ComponentIdentity {
        name: RefFamily::Predicate.component_name(),
        slug: RefFamily::Predicate.component_slug(),
    },
];

/// One of the two quad components this index covers, named after it. Each
/// family owns one `{val, rid}` component — its name and implementation slug
/// — so the association is stated once here (and in the [`IDENTITIES`] rows
/// built from these accessors) instead of at every roster site.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum RefFamily {
    /// Object values.
    Object,
    /// Predicate values.
    Predicate,
}

impl RefFamily {
    /// The persisted child's component name.
    pub(crate) const fn component_name(self) -> &'static str {
        match self {
            RefFamily::Object => "index:ref-o",
            RefFamily::Predicate => "index:ref-p",
        }
    }

    /// The persisted child's implementation slug.
    pub(crate) const fn component_slug(self) -> &'static str {
        match self {
            RefFamily::Object => "secondary-by-reference/o",
            RefFamily::Predicate => "secondary-by-reference/p",
        }
    }
}

/// The covered family to probe (which names its component and columns), the
/// term to probe for, and which pattern component a hit resolves.
struct RefProbe<'a> {
    family: RefFamily,
    lead: TermRef<'a>,
    resolves: ResolvedRoles,
}

/// The column pair and component this index would use for a pattern shape,
/// independent of any backend — the shared front half of both resolvers.
///
/// A bound subject declines the index: the primary `s` column (binary-searched
/// or zone-pruned) is the better access path there. When both object and
/// predicate are bound, the object side is chosen — object equality is usually
/// the more selective constraint. `None` when nothing this index covers is
/// bound.
fn choose<'a>(pattern: QuadPattern<'a>) -> Option<RefProbe<'a>> {
    if pattern.subject.is_some() {
        return None;
    }
    if let Some(object) = pattern.object {
        return Some(RefProbe {
            family: RefFamily::Object,
            lead: TermRef::Object(object),
            resolves: ResolvedRoles::Object,
        });
    }
    if let Some(predicate) = pattern.predicate {
        return Some(RefProbe {
            family: RefFamily::Predicate,
            lead: TermRef::Predicate(predicate),
            resolves: ResolvedRoles::Predicate,
        });
    }
    None
}

/// Resolve a pattern against this index's in-memory component.
///
/// Binary-searches the sorted value column for the probe term and slices out
/// the paired row ids — the base rows whose indexed component equals the
/// term. Declines (so the store falls back to a mask scan) when the covered
/// family's component is absent, probe-incompatible, or not globally sorted
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
    // Route through the index only when the family's component exists and is
    // globally sorted — the writer's provenance, not a stamp inspection.
    let Some(component) =
        super::IndexComponent::find_sorted(components, probe.family.component_name())
    else {
        return Ok(IndexResolution::Declined);
    };
    // Translate the term to the value column's native probe value (a string, or
    // a dictionary code). Absent from the dictionary ⇒ nothing can match. The
    // probe term is the pattern's own predicate or object, so this reads the
    // match's resolution cache (no second dictionary search).
    let Some(native) = codes.probe_scalar(probe.lead)? else {
        return Ok(IndexResolution::Empty);
    };
    // First genuine use of a `from_bytes`-adopted component: this is where a
    // deferred child canonicalizes.
    let rows = component.rows()?;
    // Binary search bounds the run of rows equal to the probe — through the
    // component's cached probe when the column resolves one; an empty run
    // means the term is present in the schema but absent from the data.
    let Some(run) = super::component_probe_run(component, COL_VAL, &native, None)? else {
        return Ok(IndexResolution::Declined);
    };
    if run.is_empty() {
        return Ok(IndexResolution::Empty);
    }
    // The run's rids, decoded and sorted only when a consumer needs them —
    // a count takes the run's width.
    let rids = rows
        .unmasked_field_by_name(COL_RID)
        .map_err(VortexRdfError::Vortex)?
        .slice(run)
        .map_err(VortexRdfError::Vortex)?;
    Ok(IndexResolution::Resolved {
        row_ids: ResolvedRowIds::Lazy(LazyRowIds::from_component_run(rids)),
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
/// This index stores no whole quads, so every outcome here is a row-id
/// resolution with no serving plan; the store gathers the matched quads from
/// the primary columns (point reads of their own, for a small id set). The
/// ids are eager, except a located run's: those stay deferred
/// ([`LazyRowIds::from_located_run`]) so that a count takes the run's width
/// and a window reads only its own rows.
#[cfg(feature = "file-io")]
pub(crate) async fn resolve_file(
    file: &Arc<crate::store::native_file::NativeStoreFile>,
    pattern: QuadPattern<'_>,
    codes: &mut PatternCodes,
) -> Result<IndexResolution<FileServePlan>> {
    let (probe, reader, name, native, located) = match locate(file, pattern, codes).await? {
        Located::Declined => return Ok(IndexResolution::Declined),
        Located::Absent => return Ok(IndexResolution::Empty),
        Located::Run {
            probe,
            reader,
            name,
            native,
            range,
        } => (probe, reader, name, native, range),
    };
    if let Some(range) = located {
        // A located empty run proves the term absent from the data — the
        // short-circuit an empty scan reaches only after reading it.
        if range.is_empty() {
            return Ok(IndexResolution::Empty);
        }
        // The located range is exactly this index's matched rows: the value
        // column is the one and only constraint. Its ids are read only when
        // a consumer needs them.
        return Ok(IndexResolution::Resolved {
            row_ids: ResolvedRowIds::Lazy(LazyRowIds::from_located_run(
                Arc::clone(file),
                name,
                reader,
                COL_RID,
                range,
                name,
            )),
            resolves: probe.resolves,
            // A back-reference index stores no whole quads to serve from.
            serve: None,
        });
    }
    super::resolve_eager_from_scan(
        reader,
        &[(COL_VAL, native)],
        COL_RID,
        probe.resolves,
        file.bound_exprs(),
        name,
    )
    .await
}

/// The file resolver's prelude: the probe a pattern shape chooses, its
/// persisted child, its native probe value, and the run of matching child
/// rows when the value column's cached chunk probes locate it.
#[cfg(feature = "file-io")]
enum Located<'a> {
    /// The index does not cover the pattern, or the probed family's child is
    /// absent (a foreign writer could omit one).
    Declined,
    /// The probed term is absent from the dictionary: nothing can match.
    Absent,
    /// The probe applies; `range` is the matched run when located, `None`
    /// when the location declined (an unsorted child, a string value column,
    /// a chunk resolving no probe) and the pushed-down scan answers instead.
    Run {
        probe: RefProbe<'a>,
        reader: vortex_layout::LayoutReaderRef,
        name: &'static str,
        native: vortex_array::scalar::Scalar,
        range: Option<Range<u64>>,
    },
}

/// Choose the probe for `pattern`, open its child, translate the term and
/// locate the matched run through the value column's chunk probes — the
/// shared front half of [`resolve_file`] and the test hook `debug_located_run`.
#[cfg(feature = "file-io")]
async fn locate<'a>(
    file: &crate::store::native_file::NativeStoreFile,
    pattern: QuadPattern<'a>,
    codes: &mut PatternCodes,
) -> Result<Located<'a>> {
    let Some(probe) = choose(pattern) else {
        return Ok(Located::Declined);
    };
    let name = probe.family.component_name();
    let Some((descriptor, reader)) = file
        .component_reader(name)
        .map_err(VortexRdfError::Vortex)?
    else {
        return Ok(Located::Declined);
    };
    let Some(native) = codes.probe_scalar(probe.lead)? else {
        return Ok(Located::Absent);
    };
    let range =
        super::row_ids::locate_component_run(file, name, COL_VAL, &native, None, descriptor.sorted)
            .await?;
    Ok(Located::Run {
        probe,
        reader,
        name,
        native,
        range,
    })
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
    Ok(match locate(file, pattern, codes).await? {
        Located::Run { range, .. } => range,
        Located::Declined | Located::Absent => None,
    })
}

/// The emission surface of the out-of-core builder (compiled out on
/// wasm32-unknown-unknown with it): the persisted child dtype and the child
/// chunks built from windows of merged `(value, row id)` pairs.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) mod out_of_core {
    use vortex_array::arrays::PrimitiveArray;
    use vortex_array::dtype::DType;
    use vortex_array::{ArrayRef, IntoArray};

    use super::super::components::{child_struct, child_struct_dtype};
    use super::CHILD_COLUMNS;
    use crate::error::Result;
    use crate::store::array::{make_string_array, stamp_is_sorted};
    use crate::store::schema::{CODE_PTYPE, ROW_ID_PTYPE, RowId, TermCode};

    /// The persisted child's struct dtype: sorted values (strings, or codes
    /// under the Dictionary layout) plus the u64 primary row id.
    pub(crate) fn ref_child_dtype(encoded: bool) -> DType {
        use vortex_array::dtype::Nullability;
        let val = if encoded {
            DType::Primitive(CODE_PTYPE, Nullability::NonNullable)
        } else {
            DType::Utf8(Nullability::NonNullable)
        };
        child_struct_dtype(
            &CHILD_COLUMNS,
            vec![
                val,
                DType::Primitive(ROW_ID_PTYPE, Nullability::NonNullable),
            ],
        )
    }

    /// One chunk of a reference component's persisted child from a window of its
    /// merged `(value, row id)` pairs.
    pub(crate) fn ref_child_chunk_strings(pairs: &[(String, RowId)]) -> Result<ArrayRef> {
        let val = make_string_array(pairs.iter().map(|(v, _)| v.as_str()));
        stamp_is_sorted(&val);
        let rid = PrimitiveArray::from_iter(pairs.iter().map(|(_, rid)| *rid)).into_array();
        child_struct(&CHILD_COLUMNS, vec![val, rid], pairs.len()).map(|a| a.into_array())
    }

    /// Code-column variant of [`ref_child_chunk_strings`].
    pub(crate) fn ref_child_chunk_codes(pairs: &[(TermCode, RowId)]) -> Result<ArrayRef> {
        let val = PrimitiveArray::from_iter(pairs.iter().map(|(code, _)| *code)).into_array();
        stamp_is_sorted(&val);
        let rid = PrimitiveArray::from_iter(pairs.iter().map(|(_, rid)| *rid)).into_array();
        child_struct(&CHILD_COLUMNS, vec![val, rid], pairs.len()).map(|a| a.into_array())
    }
}

/// The `rid` column of a permutation of the dataset: the row ids of the
/// positions `perm` lists, in that order.
fn rid_column(perm: &[usize]) -> ArrayRef {
    PrimitiveArray::from_iter(perm.iter().map(|&i| super::row_id(i))).into_array()
}

/// The complete dataset's secondary-index columns in global sorted order —
/// the in-memory builders' emission, handed on as this index's two persisted
/// children by [`into_components`](Self::into_components).
pub(crate) struct GlobalReferenceArrays {
    o_val: ArrayRef,
    o_rid: ArrayRef,
    p_val: ArrayRef,
    p_rid: ArrayRef,
}

impl GlobalReferenceArrays {
    /// Sort by term strings. Row ids number the quads' positions in `quads`
    /// (the builder must pass the dataset in final row order), so the sort is
    /// just a permutation of positions — no per-term string copies — whose
    /// ties break by position, which is row-id order.
    pub(crate) fn from_quads(quads: &[RawQuad]) -> Self {
        let perm_by = |term_of: fn(&RawQuad) -> &str| -> Vec<usize> {
            let mut perm: Vec<usize> = (0..quads.len()).collect();
            perm.sort_unstable_by(|&a, &b| {
                term_of(&quads[a]).cmp(term_of(&quads[b])).then(a.cmp(&b))
            });
            perm
        };
        let o_perm = perm_by(|q| &q.o);
        let (o_val, o_rid) = (
            make_string_array(o_perm.iter().map(|&i| quads[i].o.as_str())),
            rid_column(&o_perm),
        );
        drop(o_perm);
        let p_perm = perm_by(|q| &q.p);
        let (p_val, p_rid) = (
            make_string_array(p_perm.iter().map(|&i| quads[i].p.as_str())),
            rid_column(&p_perm),
        );
        Self::from_arrays(o_val, o_rid, p_val, p_rid)
    }

    /// Dictionary-layout variant: sort the codes.
    pub(crate) fn from_codes(codes: &QuadCodes) -> Self {
        let sorted = |column: &[TermCode]| -> (ArrayRef, ArrayRef) {
            let mut pairs: Vec<(TermCode, RowId)> = column
                .iter()
                .enumerate()
                .map(|(i, &code)| (code, super::row_id(i)))
                .collect();
            pairs.sort_unstable();
            (
                PrimitiveArray::from_iter(pairs.iter().map(|(code, _)| *code)).into_array(),
                PrimitiveArray::from_iter(pairs.iter().map(|(_, rid)| *rid)).into_array(),
            )
        };
        let (o_val, o_rid) = sorted(&codes.o);
        let (p_val, p_rid) = sorted(&codes.p);
        Self::from_arrays(o_val, o_rid, p_val, p_rid)
    }

    fn from_arrays(o_val: ArrayRef, o_rid: ArrayRef, p_val: ArrayRef, p_rid: ArrayRef) -> Self {
        stamp_is_sorted(&o_val);
        stamp_is_sorted(&p_val);
        Self {
            o_val,
            o_rid,
            p_val,
            p_rid,
        }
    }

    /// This index's two persisted children, each a `{val, rid}` table over the
    /// whole dataset. Globally sorted by construction, so both are handed over
    /// with that provenance.
    pub(crate) fn into_components(self) -> Result<Vec<super::IndexComponent>> {
        let Self {
            o_val,
            o_rid,
            p_val,
            p_rid,
        } = self;
        [
            (RefFamily::Object, o_val, o_rid),
            (RefFamily::Predicate, p_val, p_rid),
        ]
        .into_iter()
        .map(|(family, val, rid)| {
            let len = val.len();
            let rows = child_struct(&CHILD_COLUMNS, vec![val, rid], len)?;
            Ok(super::IndexComponent::built(
                family.component_name(),
                family.component_slug(),
                rows,
                true,
            ))
        })
        .collect()
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
        assert_eq!(probe.resolves, ResolvedRoles::Object);
        assert_eq!(probe.family.component_name(), "index:ref-o");
        assert_eq!(probe.lead.to_string(), o.to_string());

        // Predicate-only patterns use the predicate side.
        let probe = choose(QuadPattern::new(None, Some(&p), None, None)).unwrap();
        assert_eq!(probe.resolves, ResolvedRoles::Predicate);
        assert_eq!(probe.family.component_name(), "index:ref-p");
        assert_eq!(probe.lead.to_string(), p.to_string());

        // Nothing this index covers is bound: declines.
        assert!(choose(QuadPattern::new(None, None, None, None)).is_none());
    }

    /// Every `{val, rid}` child orders its rows by `(value, row id)`, the
    /// string-valued ones too: the rids inside one value's run ascend, which
    /// is what lets a window take the first rows of a located run as the
    /// first rows in base order.
    #[test]
    fn from_quads_breaks_value_ties_by_row_id() {
        use crate::session::VORTEX_SESSION;
        use crate::store::array::StrColReader;
        use vortex_array::VortexSessionExecute;
        use vortex_array::arrays::VarBinViewArray;

        // Few distinct values over many rows, so every value repeats widely.
        let quads: Vec<RawQuad> = (0..900u32)
            .map(|i| RawQuad {
                s: format!("<http://example.org/s{i:04}>"),
                p: format!("<http://example.org/p{}>", i % 7),
                o: format!("\"o{}\"", (i * 5) % 11),
                g: String::new(),
            })
            .collect();
        let arrays = GlobalReferenceArrays::from_quads(&quads);
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        for (name, val, rid) in [
            ("o", &arrays.o_val, &arrays.o_rid),
            ("p", &arrays.p_val, &arrays.p_rid),
        ] {
            let val = val.clone().execute::<VarBinViewArray>(&mut ctx).unwrap();
            let rid = rid.clone().execute::<PrimitiveArray>(&mut ctx).unwrap();
            let (vals, rids) = (StrColReader::new(&val), rid.as_slice::<RowId>());
            assert_eq!(rids.len(), quads.len());
            let mut repeats = 0;
            for row in 1..rids.len() {
                let (before, at) = (vals.str_at(row - 1).unwrap(), vals.str_at(row).unwrap());
                assert!(before <= at, "{name}: values ascend at row {row}");
                if before == at {
                    repeats += 1;
                    assert!(
                        rids[row - 1] < rids[row],
                        "{name}: rids ascend within {at} at row {row}"
                    );
                }
            }
            assert!(repeats > quads.len() / 2, "{name}: values repeat");
        }
    }
}
