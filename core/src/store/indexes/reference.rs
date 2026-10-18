//! The [`IndexType::SecondaryByReference`] index: sorted object and predicate
//! value columns, each paired with the primary row IDs they point at. This
//! module owns both halves of the index's lifecycle — building the columns at
//! write time, and choosing the probe a pattern runs against them at query
//! time (`choose`), which one shared resolver per backend executes, producing
//! primary row ids directly.
//!
//! The value columns come in two encodings — term strings, or dictionary
//! codes ([`TermCode`]) under the Dictionary layout — and are always built over the complete
//! dataset in one global sort: [`components`] for the in-memory builders,
//! merged `(value, row id)` spill runs for the out-of-core one. Both hand
//! the columns over as this index's two persisted children (`{val, rid}` per
//! covered family), value column stamped `IsSorted`.
//!
//! Both backends need global sortedness to binary-search, and neither depends
//! on it to be correct — they differ only in what they do without it, which
//! is what a child declaring itself unsorted (this crate never writes one,
//! but the wire format can carry one) falls back to. The in-memory resolver
//! declines outright and `match_pattern` mask-scans; the file resolver falls
//! back to the pushed-down equality, which answers whatever the order —
//! there, sortedness decides only whether the matched run can be *located*
//! (and, failing that, how much of the scan prunes).
//!
//! [`IndexType::SecondaryByReference`]: super::IndexType::SecondaryByReference

use vortex_array::arrays::PrimitiveArray;
use vortex_array::{ArrayRef, IntoArray};

use super::components::{ComponentIdentity, IndexComponent, components_from};
use super::resolve::IndexProbe;
use super::{COL_RID, IndexInput, IndexType, ResolvedRoles};
use crate::error::Result;
use crate::store::RawQuad;
use crate::store::array::{make_string_array, stamp_is_sorted};
use crate::store::layouts::{QuadPattern, TermRef};
use crate::store::schema::{RowId, TermCode};

/// The value column of a reference component's persisted child; the row id
/// beside it is the name every index child shares ([`COL_RID`]).
pub(crate) const COL_VAL: &str = "val";
const CHILD_COLUMNS: [&str; 2] = [COL_VAL, COL_RID];

/// This index's persisted-child identity table — one `{val, rid}` table per
/// covered family, in [`RefFamily`] order — feeding every generic loop in the
/// hub (the slug registry and the roster it builds); see
/// [`IndexType::component_identities`](super::IndexType::component_identities).
/// Each family reaches its row through [`RefFamily::identity`], so each name
/// keeps exactly one spelling.
pub(crate) static IDENTITIES: [ComponentIdentity; 2] = [
    ComponentIdentity {
        name: "index:ref-o",
        slug: "secondary-by-reference/o",
        index: IndexType::SecondaryByReference,
    },
    ComponentIdentity {
        name: "index:ref-p",
        slug: "secondary-by-reference/p",
        index: IndexType::SecondaryByReference,
    },
];

/// One of the two quad components this index covers, named after it. Each
/// family owns one `{val, rid}` component — its [`IDENTITIES`] row — so the
/// association is stated once there instead of at every roster site.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum RefFamily {
    /// Object values.
    Object,
    /// Predicate values.
    Predicate,
}

impl RefFamily {
    /// The persisted child's identity: its component name, implementation
    /// slug and owning index.
    pub(crate) const fn identity(self) -> &'static ComponentIdentity {
        match self {
            RefFamily::Object => &IDENTITIES[0],
            RefFamily::Predicate => &IDENTITIES[1],
        }
    }
}

/// The probe this index runs for a pattern shape — the covered family's
/// `{val, rid}` child, the term to search its value column for, and which
/// pattern component a hit resolves — independent of any backend: the shared
/// front half of both resolvers.
///
/// A bound subject declines the index: the primary `s` column (binary-searched
/// or zone-pruned) is the better access path there. When both object and
/// predicate are bound, the object side is chosen — object equality is usually
/// the more selective constraint. The children store no whole quads, so the
/// probe serves no reads; their rows are ordered by `(val, rid)`, so a run's
/// rids ascend. `None` when nothing this index covers is bound.
pub(super) fn choose<'a>(pattern: QuadPattern<'a>) -> Option<IndexProbe<'a>> {
    if pattern.subject.is_some() {
        return None;
    }
    let probe = |family: RefFamily, term: TermRef<'a>, resolves| IndexProbe {
        identity: family.identity(),
        keys: vec![(COL_VAL, term)],
        residual: Vec::new(),
        resolves,
        serve: None,
        rids_ascend: true,
    };
    if let Some(object) = pattern.object {
        return Some(probe(
            RefFamily::Object,
            TermRef::Object(object),
            ResolvedRoles::Object,
        ));
    }
    if let Some(predicate) = pattern.predicate {
        return Some(probe(
            RefFamily::Predicate,
            TermRef::Predicate(predicate),
            ResolvedRoles::Predicate,
        ));
    }
    None
}

/// The emission surface of the out-of-core builder (compiled out on
/// wasm32-unknown-unknown with it): the persisted child dtype and the child
/// chunks built from windows of merged `(value, row id)` pairs.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) mod out_of_core {
    use vortex_array::arrays::PrimitiveArray;
    use vortex_array::dtype::DType;
    use vortex_array::{ArrayRef, IntoArray};

    use super::super::components::{TermColumn, child_struct, child_struct_dtype};
    use super::CHILD_COLUMNS;
    use crate::error::Result;
    use crate::store::array::stamp_is_sorted;
    use crate::store::schema::{ROW_ID_PTYPE, RowId};

    /// The persisted child's struct dtype: sorted `V` values (strings, or
    /// codes under the Dictionary layout) plus the u64 primary row id.
    pub(crate) fn ref_child_dtype<V: TermColumn>() -> DType {
        use vortex_array::dtype::Nullability;
        child_struct_dtype(
            &CHILD_COLUMNS,
            vec![
                V::dtype(),
                DType::Primitive(ROW_ID_PTYPE, Nullability::NonNullable),
            ],
        )
    }

    /// One chunk of a reference component's persisted child from a window of its
    /// merged `(value, row id)` pairs, value column stamped sorted. `V` is the
    /// value encoding: `String`, or [`TermCode`]s under the Dictionary layout.
    ///
    /// [`TermCode`]: crate::store::TermCode
    pub(crate) fn ref_child_chunk<V: TermColumn>(pairs: &[(V, RowId)]) -> Result<ArrayRef> {
        let val = V::column(pairs.iter().map(|(v, _)| v));
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

/// This index's two persisted children over `input`, each a `{val, rid}`
/// table over the whole dataset in [`IDENTITIES`] order, sorted by
/// `(value, row id)` with the value column stamped sorted — the in-memory
/// builders' emission. Row ids number the input's positions (the builder
/// passes the dataset in final row order): term strings sort as a
/// permutation of positions — no per-term string copies — whose ties break
/// by position, which is row-id order; codes sort as `(code, row id)` pairs.
pub(super) fn components(input: IndexInput<'_>) -> Result<Vec<IndexComponent>> {
    let [(o_val, o_rid), (p_val, p_rid)] = match input {
        IndexInput::Quads(quads) => {
            let perm_by = |term_of: fn(&RawQuad) -> &str| -> Vec<usize> {
                let mut perm: Vec<usize> = (0..quads.len()).collect();
                perm.sort_unstable_by(|&a, &b| {
                    term_of(&quads[a]).cmp(term_of(&quads[b])).then(a.cmp(&b))
                });
                perm
            };
            let o_perm = perm_by(|q| &q.o);
            let o = (
                make_string_array(o_perm.iter().map(|&i| quads[i].o.as_str())),
                rid_column(&o_perm),
            );
            drop(o_perm);
            let p_perm = perm_by(|q| &q.p);
            let p = (
                make_string_array(p_perm.iter().map(|&i| quads[i].p.as_str())),
                rid_column(&p_perm),
            );
            [o, p]
        }
        IndexInput::Codes(codes) => {
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
            [sorted(&codes.o), sorted(&codes.p)]
        }
    };
    stamp_is_sorted(&o_val);
    stamp_is_sorted(&p_val);
    components_from(
        &CHILD_COLUMNS,
        [
            (RefFamily::Object.identity(), vec![o_val, o_rid]),
            (RefFamily::Predicate.identity(), vec![p_val, p_rid]),
        ],
    )
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

        // Object preferred over predicate when both are bound. A probe
        // searches the value column alone — a bound graph stays the store's
        // residual — and serves no reads, and its runs' rids ascend.
        let g = oxrdf::GraphName::NamedNode(NamedNode::new("http://example.org/g").unwrap());
        let probe = choose(QuadPattern::new(None, Some(&p), Some(&o), Some(&g))).unwrap();
        assert_eq!(probe.resolves, ResolvedRoles::Object);
        assert_eq!(probe.identity.name, "index:ref-o");
        let [(column, term)] = probe.keys[..] else {
            panic!("one key")
        };
        assert_eq!((column, term.to_string()), (COL_VAL, o.to_string()));
        assert!(probe.residual.is_empty() && probe.serve.is_none() && probe.rids_ascend);

        // Predicate-only patterns use the predicate side.
        let probe = choose(QuadPattern::new(None, Some(&p), None, None)).unwrap();
        assert_eq!(probe.resolves, ResolvedRoles::Predicate);
        assert_eq!(probe.identity.name, "index:ref-p");
        let [(column, term)] = probe.keys[..] else {
            panic!("one key")
        };
        assert_eq!((column, term.to_string()), (COL_VAL, p.to_string()));

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
        use crate::store::array::{StrColReader, field_as};
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
        let children = components(IndexInput::Quads(&quads)).unwrap();
        assert_eq!(children.len(), 2, "one child per covered family");
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        for component in &children {
            let name = component.identity.name;
            let rows = component.rows().unwrap();
            let val = field_as::<VarBinViewArray>(rows, COL_VAL, &mut ctx).unwrap();
            let rid = field_as::<PrimitiveArray>(rows, COL_RID, &mut ctx).unwrap();
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
