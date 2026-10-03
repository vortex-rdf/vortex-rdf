//! The `SecondaryByReference` index: one sorted `{val, rid}` child per
//! covered role, `index:ref-o` for objects and `index:ref-p` for predicates.
//! Holds the family vocabulary, the probe a pattern chooses, and the
//! builders' column emission.

use vortex_array::arrays::PrimitiveArray;
use vortex_array::{ArrayRef, IntoArray};

use super::components::{ComponentIdentity, IndexComponent, TermColumn, components_from};
use super::resolve::IndexProbe;
use super::{COL_RID, ResolvedRoles};
use crate::error::Result;
use crate::store::RawQuad;
use crate::store::array::stamp_is_sorted;
use crate::store::layouts::dictionary::QuadCodes;
use crate::store::layouts::{QuadPattern, ResolvedLayout, TermRef};

/// The value column of a reference child; the row id beside it is
/// [`COL_RID`].
const COL_VAL: &str = "val";
const CHILD_COLUMNS: [&str; 2] = [COL_VAL, COL_RID];

/// This index's child identities, in [`RefFamily`] order.
pub(crate) static IDENTITIES: [ComponentIdentity; 2] = [
    ComponentIdentity {
        name: "index:ref-o",
        slug: "secondary-by-reference/o",
    },
    ComponentIdentity {
        name: "index:ref-p",
        slug: "secondary-by-reference/p",
    },
];

/// One of the two quad roles this index covers.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum RefFamily {
    /// Object values.
    Object,
    /// Predicate values.
    Predicate,
}

impl RefFamily {
    /// The persisted child's identity.
    pub(crate) const fn identity(self) -> &'static ComponentIdentity {
        match self {
            RefFamily::Object => &IDENTITIES[0],
            RefFamily::Predicate => &IDENTITIES[1],
        }
    }
}

/// The probe this index runs for `pattern`: the object side when an object is
/// bound, else the predicate side. `None` when a subject is bound or neither
/// predicate nor object is.
pub(crate) fn choose<'a>(
    pattern: QuadPattern<'a>,
    _layout: &ResolvedLayout,
) -> Option<IndexProbe<'a>> {
    if pattern.subject.is_some() {
        return None;
    }
    let (family, term, resolves) = match (pattern.object, pattern.predicate) {
        (Some(object), _) => (
            RefFamily::Object,
            TermRef::Object(object),
            ResolvedRoles::Object,
        ),
        (None, Some(predicate)) => (
            RefFamily::Predicate,
            TermRef::Predicate(predicate),
            ResolvedRoles::Predicate,
        ),
        (None, None) => return None,
    };
    Some(IndexProbe {
        identity: family.identity(),
        keys: vec![(COL_VAL, term)],
        residual: Vec::new(),
        resolves,
        serve: None,
    })
}

/// The out-of-core builder's emission surface: the child dtype and child
/// chunks from windows of merged `(value, row id)` pairs.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) mod out_of_core {
    use vortex_array::arrays::PrimitiveArray;
    use vortex_array::dtype::DType;
    use vortex_array::{ArrayRef, IntoArray};

    use super::super::components::{TermColumn, child_struct, child_struct_dtype};
    use super::CHILD_COLUMNS;
    use crate::error::Result;
    use crate::store::array::stamp_is_sorted;

    /// The child's struct dtype: values as strings, or u32 codes when
    /// `encoded`, plus the u32 primary row id.
    pub(crate) fn ref_child_dtype(encoded: bool) -> DType {
        child_struct_dtype(&CHILD_COLUMNS, encoded)
    }

    /// One child chunk from a window of merged `(value, row id)` pairs,
    /// value column stamped sorted.
    pub(crate) fn ref_child_chunk<V: TermColumn>(pairs: &[(V, u32)]) -> Result<ArrayRef> {
        let val = V::column(pairs.iter().map(|(v, _)| v));
        stamp_is_sorted(&val);
        let rid = PrimitiveArray::from_iter(pairs.iter().map(|(_, rid)| *rid)).into_array();
        child_struct(&CHILD_COLUMNS, vec![val, rid], pairs.len()).map(|a| a.into_array())
    }
}

/// The complete dataset's reference columns in global sorted order, the
/// in-memory builders' emission.
pub(crate) struct GlobalReferenceArrays {
    object: [ArrayRef; 2],
    predicate: [ArrayRef; 2],
}

impl GlobalReferenceArrays {
    /// Sorted by term strings. Row ids are the quads' positions in `quads`,
    /// which must be the dataset in final row order.
    pub(crate) fn from_quads(quads: &[RawQuad]) -> Self {
        let term = |family: RefFamily, i: usize| -> &String {
            match family {
                RefFamily::Object => &quads[i].o,
                RefFamily::Predicate => &quads[i].p,
            }
        };
        Self::build(
            |family| {
                let mut perm: Vec<u32> = (0..quads.len() as u32).collect();
                perm.sort_unstable_by(|&a, &b| {
                    term(family, a as usize).cmp(term(family, b as usize))
                });
                perm
            },
            term,
        )
    }

    /// Sorted by u32 codes, ties by row id; same row-order precondition as
    /// [`Self::from_quads`].
    pub(crate) fn from_codes(codes: &QuadCodes) -> Self {
        let term = |family: RefFamily, i: usize| -> &u32 {
            match family {
                RefFamily::Object => &codes.o[i],
                RefFamily::Predicate => &codes.p[i],
            }
        };
        Self::build(
            |family| {
                let mut perm: Vec<u32> = (0..codes.o.len() as u32).collect();
                perm.sort_unstable_by_key(|&i| (*term(family, i as usize), i));
                perm
            },
            term,
        )
    }

    /// Both families' `[val, rid]` columns: `perm_by` orders the dataset for
    /// a family, `term_of` reads row `i`'s value; each value column is stamped
    /// sorted.
    fn build<'a, V: TermColumn + 'a>(
        perm_by: impl Fn(RefFamily) -> Vec<u32>,
        term_of: impl Fn(RefFamily, usize) -> &'a V,
    ) -> Self {
        let build = |family: RefFamily| {
            let perm = perm_by(family);
            let val = V::column(perm.iter().map(|&i| term_of(family, i as usize)));
            stamp_is_sorted(&val);
            [val, PrimitiveArray::from_iter(perm).into_array()]
        };
        Self {
            object: build(RefFamily::Object),
            predicate: build(RefFamily::Predicate),
        }
    }

    /// This index's two children, a `{val, rid}` table per family.
    pub(crate) fn into_components(self) -> Result<Vec<IndexComponent>> {
        let Self { object, predicate } = self;
        components_from(
            &CHILD_COLUMNS,
            [
                (RefFamily::Object.identity(), object.to_vec()),
                (RefFamily::Predicate.identity(), predicate.to_vec()),
            ],
        )
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
        let layout = ResolvedLayout::Default;

        // A bound subject declines.
        assert!(
            choose(
                QuadPattern::new(Some(&s), Some(&p), Some(&o), None),
                &layout
            )
            .is_none()
        );

        // Object preferred over predicate when both are bound.
        let probe = choose(QuadPattern::new(None, Some(&p), Some(&o), None), &layout).unwrap();
        assert_eq!(probe.resolves, ResolvedRoles::Object);
        assert_eq!(probe.identity.name, "index:ref-o");
        assert_eq!(probe.keys.len(), 1);
        assert_eq!(probe.keys[0].0, COL_VAL);
        assert_eq!(probe.keys[0].1.to_string(), o.to_string());
        assert!(probe.serve.is_none());

        // Predicate only: the predicate side.
        let probe = choose(QuadPattern::new(None, Some(&p), None, None), &layout).unwrap();
        assert_eq!(probe.resolves, ResolvedRoles::Predicate);
        assert_eq!(probe.identity.name, "index:ref-p");
        assert_eq!(probe.keys[0].1.to_string(), p.to_string());

        // Nothing this index covers is bound.
        assert!(choose(QuadPattern::new(None, None, None, None), &layout).is_none());
    }
}
