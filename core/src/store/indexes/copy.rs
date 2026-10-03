//! The `SecondaryByCopy` index: two sorted copies of the quad columns,
//! `index:posg` by (p, o, s, g) and `index:ospg` by (o, s, p, g), each row
//! paired with its primary row id. Holds the family vocabulary, the probe a
//! pattern chooses, and the builders' column emission.

use std::cmp::Ordering;

use vortex_array::arrays::PrimitiveArray;
use vortex_array::{ArrayRef, IntoArray};

use super::components::{ComponentIdentity, IndexComponent, TermColumn, components_from};
use super::resolve::IndexProbe;
use super::serve::ServeDecode;
use super::{COL_RID, ResolvedRoles};
use crate::error::Result;
use crate::store::RawQuad;
use crate::store::array::stamp_is_sorted;
use crate::store::layouts::dictionary::QuadCodes;
use crate::store::layouts::{QuadPattern, ResolvedLayout, TermRef};
use crate::store::read::metadata::SortOrder;
use crate::store::schema::{COL_G, COL_O, COL_P, COL_S};

/// One sorted copy family, named after its sort order.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum CopyFamily {
    /// Quads sorted by (p, o, s, g).
    Posg,
    /// Quads sorted by (o, s, p, g).
    Ospg,
}

/// A copy child's columns: the primaries under the schema's names, then the
/// primary row id. Both families share them; the child's identity says which
/// sort order the rows are in.
const CHILD_COLUMNS: [&str; 5] = [COL_S, COL_P, COL_O, COL_G, COL_RID];
/// The child columns sourcing the primary `(s, p, o, g)` components.
const CHILD_PRIMARY: [&str; 4] = [COL_S, COL_P, COL_O, COL_G];

/// This index's child identities, in [`CopyFamily`] order.
pub(crate) static IDENTITIES: [ComponentIdentity; 2] = [
    ComponentIdentity {
        name: "index:posg",
        slug: "secondary-by-copy/posg",
    },
    ComponentIdentity {
        name: "index:ospg",
        slug: "secondary-by-copy/ospg",
    },
];

impl CopyFamily {
    /// The persisted child's identity.
    pub(crate) const fn identity(self) -> &'static ComponentIdentity {
        match self {
            CopyFamily::Posg => &IDENTITIES[0],
            CopyFamily::Ospg => &IDENTITIES[1],
        }
    }

    /// The family whose child is named `name`.
    pub(crate) fn of_component(name: &str) -> Option<CopyFamily> {
        [CopyFamily::Posg, CopyFamily::Ospg]
            .into_iter()
            .find(|family| family.identity().name == name)
    }

    /// The child's row order.
    pub(crate) fn sort_order(self) -> SortOrder {
        match self {
            CopyFamily::Posg => SortOrder::Posg,
            CopyFamily::Ospg => SortOrder::Ospg,
        }
    }

    /// The leading sort-key column of the child.
    fn child_lead_col(self) -> &'static str {
        match self {
            CopyFamily::Posg => COL_P,
            CopyFamily::Ospg => COL_O,
        }
    }

    /// The second sort-key column of the child.
    fn child_second_col(self) -> &'static str {
        match self {
            CopyFamily::Posg => COL_O,
            CopyFamily::Ospg => COL_S,
        }
    }

    /// The lead column's position in [`CHILD_COLUMNS`].
    fn lead_ix(self) -> usize {
        CHILD_COLUMNS
            .iter()
            .position(|c| *c == self.child_lead_col())
            .expect("the lead column is a child column")
    }

    /// Where each quad component (s, p, o, g) sits in this family's
    /// `CopyKey` tuple.
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    fn key_positions(self) -> [usize; 4] {
        match self {
            CopyFamily::Posg => [2, 0, 1, 3],
            CopyFamily::Ospg => [1, 2, 0, 3],
        }
    }

    /// This family's quad comparator over term strings.
    fn cmp_quads(self, a: &RawQuad, b: &RawQuad) -> Ordering {
        match self {
            CopyFamily::Posg => {
                a.p.cmp(&b.p)
                    .then_with(|| a.o.cmp(&b.o))
                    .then_with(|| a.s.cmp(&b.s))
                    .then_with(|| a.g.cmp(&b.g))
            }
            CopyFamily::Ospg => {
                a.o.cmp(&b.o)
                    .then_with(|| a.s.cmp(&b.s))
                    .then_with(|| a.p.cmp(&b.p))
                    .then_with(|| a.g.cmp(&b.g))
            }
        }
    }

    /// Row `i`'s sort key as a code tuple; sorted-dictionary codes are
    /// lexicographic ranks, so this orders like [`Self::cmp_quads`].
    fn code_key(self, codes: &QuadCodes, i: usize) -> [u32; 4] {
        match self {
            CopyFamily::Posg => [codes.p[i], codes.o[i], codes.s[i], codes.g[i]],
            CopyFamily::Ospg => [codes.o[i], codes.s[i], codes.p[i], codes.g[i]],
        }
    }
}

/// The probe this index runs for `pattern`: a bound predicate and object take
/// the POSG family's (p, o) prefix, a predicate alone POSG's lead, an object
/// alone OSPG's lead; a bound graph rides as a residual filter term. `None`
/// when a subject is bound or neither predicate nor object is.
pub(crate) fn choose<'a>(
    pattern: QuadPattern<'a>,
    layout: &ResolvedLayout,
) -> Option<IndexProbe<'a>> {
    if pattern.subject.is_some() {
        return None;
    }
    let (family, lead, second, resolves) = match (pattern.predicate, pattern.object) {
        (Some(predicate), Some(object)) => (
            CopyFamily::Posg,
            TermRef::Predicate(predicate),
            Some(TermRef::Object(object)),
            ResolvedRoles::PredicateObject,
        ),
        (Some(predicate), None) => (
            CopyFamily::Posg,
            TermRef::Predicate(predicate),
            None,
            ResolvedRoles::Predicate,
        ),
        (None, Some(object)) => (
            CopyFamily::Ospg,
            TermRef::Object(object),
            None,
            ResolvedRoles::Object,
        ),
        (None, None) => return None,
    };
    let mut keys = vec![(family.child_lead_col(), lead)];
    keys.extend(second.map(|term| (family.child_second_col(), term)));
    Some(IndexProbe {
        identity: family.identity(),
        keys,
        residual: pattern
            .graph
            .map(|graph| (COL_G, TermRef::Graph(graph)))
            .into_iter()
            .collect(),
        resolves,
        serve: Some(ServeDecode::new(
            CHILD_PRIMARY,
            COL_RID,
            copy_decode_layout(layout),
        )),
    })
}

/// The layout a copy child decodes through: the copies hold each component as
/// one full term, so only the Dictionary layout carries over.
fn copy_decode_layout(layout: &ResolvedLayout) -> ResolvedLayout {
    match layout {
        ResolvedLayout::Dictionary(dict) => ResolvedLayout::Dictionary(dict.clone()),
        _ => ResolvedLayout::Default,
    }
}

/// The out-of-core builder's emission surface: the child dtype, child chunks
/// from windows of merged `(sort key, row id)` entries, and the sort key.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) mod out_of_core {
    use vortex_array::arrays::PrimitiveArray;
    use vortex_array::dtype::DType;
    use vortex_array::{ArrayRef, IntoArray};

    use super::super::components::{TermColumn, child_struct, child_struct_dtype};
    use super::{CHILD_COLUMNS, CopyFamily};
    use crate::error::Result;
    use crate::store::array::stamp_is_sorted;

    /// The child's struct dtype: term strings, or u32 codes when `encoded`,
    /// plus the u32 primary row id.
    pub(crate) fn copy_child_dtype(encoded: bool) -> DType {
        child_struct_dtype(&CHILD_COLUMNS, encoded)
    }

    /// One child chunk from a window of a family's merged `(sort key, row
    /// id)` entries, lead column stamped sorted.
    pub(crate) fn copy_child_chunk<V: TermColumn>(
        family: CopyFamily,
        keys: &[(CopyKey<V>, u32)],
    ) -> Result<ArrayRef> {
        let [s_ix, p_ix, o_ix, g_ix] = family.key_positions();
        let col = |ix: usize| V::column(keys.iter().map(|(key, _)| &key.0[ix]));
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

    /// A quad's terms in one family's sort-key order, so the derived `Ord`
    /// is that family's comparator. Built by [`Self::posg`] / [`Self::ospg`]
    /// from an `[s, p, o, g]` tuple; `CopyFamily::key_positions` maps the
    /// components back out.
    #[derive(
        Clone,
        Debug,
        PartialEq,
        Eq,
        PartialOrd,
        Ord,
        rkyv::Archive,
        rkyv::Serialize,
        rkyv::Deserialize,
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

        /// The OSPG key of a quad given as `[s, p, o, g]`, consuming the
        /// tuple.
        pub(crate) fn ospg(spog: [V; 4]) -> Self {
            let [s, p, o, g] = spog;
            Self([o, s, p, g])
        }
    }
}

/// The permutation putting `quads` in `family` order.
fn string_perm(quads: &[RawQuad], family: CopyFamily) -> Vec<u32> {
    let mut perm: Vec<u32> = (0..quads.len() as u32).collect();
    perm.sort_unstable_by(|&a, &b| family.cmp_quads(&quads[a as usize], &quads[b as usize]));
    perm
}

/// The permutation putting the encoded dataset in `family` order.
fn code_perm(codes: &QuadCodes, family: CopyFamily) -> Vec<u32> {
    let mut perm: Vec<u32> = (0..codes.s.len() as u32).collect();
    perm.sort_unstable_by_key(|&i| family.code_key(codes, i as usize));
    perm
}

/// One family's five columns (s, p, o, g, rid) in `perm` order; `term_of`
/// reads row `i`'s term for one component. The row ids are the quads' own
/// positions.
fn family_columns<'a, V: TermColumn + 'a>(
    perm: &[u32],
    term_of: [&dyn Fn(usize) -> &'a V; 4],
) -> [ArrayRef; 5] {
    let col =
        |term_of: &dyn Fn(usize) -> &'a V| V::column(perm.iter().map(|&i| term_of(i as usize)));
    let [s, p, o, g] = term_of;
    [
        col(s),
        col(p),
        col(o),
        col(g),
        PrimitiveArray::from_iter(perm.iter().copied()).into_array(),
    ]
}

/// The complete dataset's copy columns in global family order, the in-memory
/// builders' emission.
pub(crate) struct GlobalCopyArrays {
    posg: [ArrayRef; 5],
    ospg: [ArrayRef; 5],
}

impl GlobalCopyArrays {
    /// Sorted by term strings. Row ids are the quads' positions in `quads`,
    /// which must be the dataset in final row order.
    pub(crate) fn from_quads(quads: &[RawQuad]) -> Self {
        Self::build(
            |family| string_perm(quads, family),
            [&|i| &quads[i].s, &|i| &quads[i].p, &|i| &quads[i].o, &|i| {
                &quads[i].g
            }],
        )
    }

    /// Sorted by u32 codes; same row-order precondition as
    /// [`Self::from_quads`].
    pub(crate) fn from_codes(codes: &QuadCodes) -> Self {
        Self::build(
            |family| code_perm(codes, family),
            [&|i| &codes.s[i], &|i| &codes.p[i], &|i| &codes.o[i], &|i| {
                &codes.g[i]
            }],
        )
    }

    /// Both families' columns: `perm_by` orders the dataset for a family,
    /// `term_of` reads row `i`'s `(s, p, o, g)` terms; each lead column is
    /// stamped sorted.
    fn build<'a, V: TermColumn + 'a>(
        perm_by: impl Fn(CopyFamily) -> Vec<u32>,
        term_of: [&dyn Fn(usize) -> &'a V; 4],
    ) -> Self {
        let build = |family: CopyFamily| {
            let perm = perm_by(family);
            let columns = family_columns(&perm, term_of);
            stamp_is_sorted(&columns[family.lead_ix()]);
            columns
        };
        Self {
            posg: build(CopyFamily::Posg),
            ospg: build(CopyFamily::Ospg),
        }
    }

    /// This index's two children, one sorted quad copy per family.
    pub(crate) fn into_components(self) -> Result<Vec<IndexComponent>> {
        let Self { posg, ospg } = self;
        components_from(
            &CHILD_COLUMNS,
            [
                (CopyFamily::Posg.identity(), posg.to_vec()),
                (CopyFamily::Ospg.identity(), ospg.to_vec()),
            ],
        )
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

    fn key_terms(probe: &IndexProbe<'_>) -> Vec<(&'static str, String)> {
        probe
            .keys
            .iter()
            .map(|(column, term)| (*column, term.to_string()))
            .collect()
    }

    #[test]
    fn choose_family_and_component() {
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

        // Predicate and object: the (p, o) prefix on POSG, resolving both.
        let probe = choose(QuadPattern::new(None, Some(&p), Some(&o), None), &layout).unwrap();
        assert_eq!(probe.identity.name, "index:posg");
        assert_eq!(probe.resolves, ResolvedRoles::PredicateObject);
        assert_eq!(
            key_terms(&probe),
            vec![(COL_P, p.to_string()), (COL_O, o.to_string())]
        );
        assert!(probe.residual.is_empty());
        assert!(probe.serve.is_some());

        // Predicate only: POSG's lead.
        let probe = choose(QuadPattern::new(None, Some(&p), None, None), &layout).unwrap();
        assert_eq!(probe.identity.name, "index:posg");
        assert_eq!(probe.resolves, ResolvedRoles::Predicate);
        assert_eq!(key_terms(&probe), vec![(COL_P, p.to_string())]);

        // Object only: OSPG's lead.
        let probe = choose(QuadPattern::new(None, None, Some(&o), None), &layout).unwrap();
        assert_eq!(probe.identity.name, "index:ospg");
        assert_eq!(probe.resolves, ResolvedRoles::Object);
        assert_eq!(key_terms(&probe), vec![(COL_O, o.to_string())]);

        // A bound graph rides as a residual term, never as a key.
        let g = oxrdf::GraphName::NamedNode(NamedNode::new("http://example.org/g").unwrap());
        let probe = choose(QuadPattern::new(None, Some(&p), None, Some(&g)), &layout).unwrap();
        assert_eq!(key_terms(&probe), vec![(COL_P, p.to_string())]);
        assert_eq!(probe.residual.len(), 1);
        assert_eq!(probe.residual[0].0, COL_G);

        // Nothing this index covers is bound.
        assert!(choose(QuadPattern::new(None, None, None, None), &layout).is_none());
    }

    #[test]
    fn family_permutations_follow_comparators() {
        let quads = vec![
            raw("s2", "p1", "o2", ""), // 0
            raw("s0", "p2", "o0", ""), // 1
            raw("s1", "p1", "o0", ""), // 2
        ];
        // (p, o, s, g): (p1,o0) < (p1,o2) < (p2,o0) → rows 2, 0, 1.
        assert_eq!(string_perm(&quads, CopyFamily::Posg), vec![2, 0, 1]);
        // (o, s, p, g): (o0,s0) < (o0,s1) < (o2,s2) → rows 1, 2, 0.
        assert_eq!(string_perm(&quads, CopyFamily::Ospg), vec![1, 2, 0]);

        // Codes that are lexicographic ranks of the terms order the same way.
        let codes = QuadCodes {
            s: vec![2, 0, 1],
            p: vec![0, 1, 0],
            o: vec![1, 0, 0],
            g: vec![0, 0, 0],
        };
        assert_eq!(code_perm(&codes, CopyFamily::Posg), vec![2, 0, 1]);
        assert_eq!(code_perm(&codes, CopyFamily::Ospg), vec![1, 2, 0]);
    }

    #[test]
    fn lead_ix_names_the_lead_column() {
        assert_eq!(CHILD_COLUMNS[CopyFamily::Posg.lead_ix()], COL_P);
        assert_eq!(CHILD_COLUMNS[CopyFamily::Ospg.lead_ix()], COL_O);
    }

    #[test]
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    fn copy_key_positions_roundtrip() {
        use super::out_of_core::CopyKey;

        let spog = [
            "s".to_string(),
            "p".to_string(),
            "o".to_string(),
            "g".to_string(),
        ];

        let posg = CopyKey::posg(&spog);
        let [s_ix, p_ix, o_ix, g_ix] = CopyFamily::Posg.key_positions();
        assert_eq!(
            [&posg.0[s_ix], &posg.0[p_ix], &posg.0[o_ix], &posg.0[g_ix]],
            [&spog[0], &spog[1], &spog[2], &spog[3]]
        );

        let ospg = CopyKey::ospg(spog.clone());
        let [s_ix, p_ix, o_ix, g_ix] = CopyFamily::Ospg.key_positions();
        assert_eq!(
            [&ospg.0[s_ix], &ospg.0[p_ix], &ospg.0[o_ix], &ospg.0[g_ix]],
            [&spog[0], &spog[1], &spog[2], &spog[3]]
        );

        // The derived Ord compares by the family's comparator: POSG keys
        // order by predicate first.
        let key = |s: &str, p: &str, o: &str| {
            CopyKey::posg(&[s.to_string(), p.to_string(), o.to_string(), String::new()])
        };
        assert!(key("s9", "p1", "o9") < key("s0", "p2", "o0"));
    }
}
