//! The [`IndexType::SecondaryByCopy`] index: two complete extra copies of the
//! quad columns, one per sort order, each paired with the primary row ids it
//! permutes (the classic triple-store POS/OSP permutation indexes, for
//! quads). This module builds the copies and chooses the probe a pattern runs
//! against them (`choose`), which one shared resolver per backend executes.
//!
//! - **`index:posg`**, quads sorted by (p, o, s, g): a bound predicate is a
//!   binary search of the child's `p` column, and a bound predicate and
//!   object a two-key *prefix* search, as the object column is itself sorted
//!   within a predicate's run (`ResolvedRoles::PredicateObject`).
//! - **`index:ospg`**, quads sorted by (o, s, p, g): a bound object is a
//!   binary search of the child's `o` column.
//!
//! Like [`reference`], resolutions answer in *base row ids* (each child's
//! `rid` column), so they compose with row selections, tombstones and
//! chained matches unchanged. The full copies add locality: the rows matching
//! a bound predicate or object are a *contiguous* run of the copy columns,
//! which the serve plan this index hands back (`InMemoryServePlan`,
//! `FileServePlan`) reads `quads()` from, instead of scattering row-id reads
//! across the primary columns.
//!
//! The copies hold term strings (Default and TypedObject layouts, the object
//! as its full N-Triples term) or dictionary codes ([`TermCode`]) under the
//! Dictionary layout, and, like the reference index, are always sorted over
//! the whole dataset: [`components`] for the in-memory builders, merged
//! `(sort key, row id)` spill runs for the out-of-core one, lead column
//! stamped either way. The in-memory resolver requires that global
//! provenance. The file resolver uses it to locate the matched run by binary
//! search over the child's cached chunk probes (lead key, then the windowed
//! second key), and falls back to a pushed-down equality scan when the
//! probes decline.
//!
//! [`IndexType::SecondaryByCopy`]: super::IndexType::SecondaryByCopy
//! [`reference`]: super::reference
//! [`TermCode`]: crate::store::TermCode

use std::cmp::Ordering;

use vortex_array::arrays::PrimitiveArray;
use vortex_array::{ArrayRef, IntoArray};

use super::components::{ComponentIdentity, IndexComponent, TermColumn, components_from};
use super::resolve::IndexProbe;
use super::serve::ServeDecode;
use super::{COL_RID, IndexInput, IndexType, ResolvedRoles};
use crate::error::Result;
use crate::store::RawQuad;
use crate::store::array::stamp_is_sorted;
use crate::store::layouts::dictionary::QuadCodes;
use crate::store::layouts::{QuadPattern, ResolvedLayout, TermRef};
use crate::store::schema::{COL_G, COL_O, COL_P, COL_S};

/// One of the two sorted copy families this index maintains, named after its
/// sort order. Each family owns five columns: the four quad components plus
/// the primary row id each copy row came from.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub(crate) enum CopyFamily {
    /// Quads sorted by (p, o, s, g).
    Posg,
    /// Quads sorted by (o, s, p, g).
    Ospg,
}

/// The columns of a copy family's persisted child: the primaries under the
/// schema's names plus the primary row id. Both families use the same names
/// (the child's identity says which sort order the rows are in), and a file
/// serve plan projects exactly these.
pub(super) const CHILD_COLUMNS: [&str; 5] = [COL_S, COL_P, COL_O, COL_G, COL_RID];

/// This index's persisted-child identity table, one differently sorted quad
/// table per family in [`CopyFamily`] order, for the hub's generic loops
/// (see [`IndexType::component_identities`](super::IndexType::component_identities)).
/// Each family reaches its row through [`CopyFamily::identity`], so each name
/// keeps exactly one spelling.
pub(crate) static IDENTITIES: [ComponentIdentity; 2] = [
    ComponentIdentity {
        name: "index:posg",
        slug: "secondary-by-copy/posg",
        index: IndexType::SecondaryByCopy,
    },
    ComponentIdentity {
        name: "index:ospg",
        slug: "secondary-by-copy/ospg",
        index: IndexType::SecondaryByCopy,
    },
];

impl CopyFamily {
    /// The persisted child's identity: its component name, implementation
    /// slug and owning index.
    pub(crate) const fn identity(self) -> &'static ComponentIdentity {
        match self {
            CopyFamily::Posg => &IDENTITIES[0],
            CopyFamily::Ospg => &IDENTITIES[1],
        }
    }

    /// This family's sort order: the `(s, p, o, g)` position of each of its
    /// sort keys, in key order (`(p, o, s, g)` for POSG, `(o, s, p, g)` for
    /// OSPG). The one spelling of the permutation: the child's sort-key
    /// columns ([`key_column`](Self::key_column)), the code sort key and the
    /// out-of-core [`CopyKey`] all derive from it. A position is also the
    /// component's column in [`CHILD_COLUMNS`].
    ///
    /// [`CopyKey`]: out_of_core::CopyKey
    const fn key_order(self) -> [usize; 4] {
        match self {
            CopyFamily::Posg => [1, 2, 0, 3],
            CopyFamily::Ospg => [2, 0, 1, 3],
        }
    }

    /// Where each quad component (s, p, o, g) sits inside this family's
    /// [`CopyKey`] tuple: the inverse of [`key_order`](Self::key_order).
    /// Compiled where the out-of-core builder is.
    ///
    /// [`CopyKey`]: out_of_core::CopyKey
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    const fn key_positions(self) -> [usize; 4] {
        let order = self.key_order();
        let mut positions = [0; 4];
        let mut key = 0;
        while key < order.len() {
            positions[order[key]] = key;
            key += 1;
        }
        positions
    }

    /// The child column holding sort key `key` (0 the lead, 1 the second).
    fn key_column(self, key: usize) -> &'static str {
        CHILD_COLUMNS[self.key_order()[key]]
    }

    /// This family's quad comparator over term strings, in
    /// [`key_order`](Self::key_order), written out by hand as the hot
    /// comparator of the in-memory string sort.
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
}

/// The probe this index runs for a pattern shape (its family's child, the
/// sort keys and terms to search, and the components a hit resolves),
/// independent of any backend: the shared front half of both resolvers.
///
/// A bound subject declines the index: the primary `s` column (binary-searched
/// or zone-pruned) is the better access path there. A bound predicate *and*
/// object take the POSG family's (p, o) prefix, resolving both components in
/// one probe. A bound graph is never one of the keys a copy is searched by
/// (it is the last key of both orders), so it rides as a residual term the
/// file serve plan filters on. The copies hold whole quads, so the probe
/// serves reads from its family's columns; a run's rids follow the family's
/// sort order, not row order. `None` when nothing this index covers is bound.
pub(super) fn choose<'a>(
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
    // The lead key, then, for a prefix probe, the second sort key, which the
    // family's comparator sorts within each lead run.
    let mut keys = Vec::with_capacity(2);
    keys.push((family.key_column(0), lead));
    keys.extend(second.map(|term| (family.key_column(1), term)));
    Some(IndexProbe {
        identity: family.identity(),
        keys,
        residual: pattern
            .graph
            .map(|graph| (COL_G, TermRef::Graph(graph)))
            .into_iter()
            .collect(),
        resolves,
        serve: Some(ServeDecode {
            decode_layout: copy_decode_layout(layout),
        }),
        rids_ascend: false,
    })
}

/// The layout a copy family's columns decode through: the copies always store
/// each component as one full term (dictionary codes under the Dictionary
/// layout, N-Triples strings otherwise), so even a TypedObject store's copies
/// decode as Default.
fn copy_decode_layout(layout: &ResolvedLayout) -> ResolvedLayout {
    match layout {
        ResolvedLayout::Dictionary(dict) => ResolvedLayout::Dictionary(dict.clone()),
        _ => ResolvedLayout::Default,
    }
}

/// The emission surface of the out-of-core builder (compiled out on
/// wasm32-unknown-unknown with it): the child chunks built from windows of
/// merged `(sort key, row id)` entries, and the sort key those entries carry.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) mod out_of_core {
    use vortex_array::arrays::PrimitiveArray;
    use vortex_array::{ArrayRef, IntoArray};

    use super::super::components::TermColumn;
    use super::{CHILD_COLUMNS, CopyFamily};
    use crate::error::Result;
    use crate::store::array::named_struct;
    use crate::store::array::stamp_is_sorted;
    use crate::store::schema::RowId;

    /// One chunk of a copy family's persisted child from a window of its merged
    /// `(sort key, row id)` entries, under plain child column names with the
    /// lead stamped. `V` is the term encoding: `String`, or [`TermCode`]s
    /// under the Dictionary layout.
    ///
    /// [`TermCode`]: crate::store::TermCode
    pub(crate) fn copy_child_chunk<V: TermColumn>(
        family: CopyFamily,
        keys: &[(CopyKey<V>, RowId)],
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
        stamp_is_sorted(&columns[family.key_order()[0]]);
        named_struct(&CHILD_COLUMNS, columns, keys.len()).map(|a| a.into_array())
    }

    /// A quad's terms rearranged into one family's sort-key order, so deriving
    /// `Ord` compares by exactly that family's comparator. `V` is the term
    /// encoding: `String`, [`TermCode`](crate::store::TermCode)s under the
    /// Dictionary layout, or the out-of-core build's `u32` term ranks, whose
    /// order is the terms' (its spilled copy entries carry their ranks in
    /// this order).
    ///
    /// Built via [`Self::new`] from an `[s, p, o, g]` tuple;
    /// [`CopyFamily::key_positions`](super::CopyFamily::key_positions) maps the
    /// components back out when the sorted keys are turned into columns.
    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
    pub(crate) struct CopyKey<V>(pub(crate) [V; 4]);

    impl<V: Default> CopyKey<V> {
        /// The `family` key of a quad given as `[s, p, o, g]`, consuming the
        /// tuple: each term moves into its place in the family's
        /// [`key_order`](CopyFamily::key_order), so the rearrangement clones
        /// nothing (a clone is a String allocation on the non-Dictionary
        /// layouts).
        pub(crate) fn new(family: CopyFamily, mut spog: [V; 4]) -> Self {
            let [a, b, c, d] = family.key_order();
            let mut take = |component: usize| std::mem::take(&mut spog[component]);
            Self([take(a), take(b), take(c), take(d)])
        }
    }
}

// ── build side ───────────────────────────────────────────────────────────────

/// The permutation of positions putting `quads` in `family` order.
fn string_perm(quads: &[RawQuad], family: CopyFamily) -> Vec<usize> {
    let mut perm: Vec<usize> = (0..quads.len()).collect();
    perm.sort_unstable_by(|&a, &b| family.cmp_quads(&quads[a], &quads[b]));
    perm
}

/// The permutation of positions putting the encoded dataset in `family`
/// order: each row's codes in [`key_order`](CopyFamily::key_order) are its
/// sort key, order-equivalent to [`CopyFamily::cmp_quads`] because
/// sorted-dictionary codes are lexicographic ranks.
fn code_perm(codes: &QuadCodes, family: CopyFamily) -> Vec<usize> {
    let spog = [&codes.s, &codes.p, &codes.o, &codes.g];
    let [a, b, c, d] = family.key_order().map(|component| spog[component]);
    let mut perm: Vec<usize> = (0..codes.s.len()).collect();
    perm.sort_unstable_by_key(|&i| [a[i], b[i], c[i], d[i]]);
    perm
}

/// One family's five columns (s, p, o, g, rid) over `perm` order; `term_of`
/// reads row `i`'s term for one component. The row ids number the quads'
/// own positions: the emission covers the whole dataset, so `perm` already
/// addresses the assembled array.
fn family_columns<'a, V: TermColumn + 'a>(
    perm: &[usize],
    term_of: [&dyn Fn(usize) -> &'a V; 4],
) -> [ArrayRef; 5] {
    let col = |term_of: &dyn Fn(usize) -> &'a V| V::column(perm.iter().map(|&i| term_of(i)));
    let [s, p, o, g] = term_of;
    [
        col(s),
        col(p),
        col(o),
        col(g),
        PrimitiveArray::from_iter(perm.iter().map(|&i| super::row_id(i))).into_array(),
    ]
}

/// This index's two persisted children over `input`, one sorted quad copy
/// per family in [`IDENTITIES`] order, each in its global family order with
/// its lead column stamped sorted: the in-memory builders' emission. Row ids
/// number the input's positions (the builder passes the dataset in final row
/// order), so each family is a permutation of positions, by term strings
/// ([`CopyFamily::cmp_quads`]) or by codes under the Dictionary layout, with
/// no per-term string copies beyond the columns.
pub(super) fn components(input: IndexInput<'_>) -> Result<Vec<IndexComponent>> {
    match input {
        IndexInput::Quads(quads) => family_components(
            |family| string_perm(quads, family),
            [&|i| &quads[i].s, &|i| &quads[i].p, &|i| &quads[i].o, &|i| {
                &quads[i].g
            }],
        ),
        IndexInput::Codes(codes) => family_components(
            |family| code_perm(codes, family),
            [&|i| &codes.s[i], &|i| &codes.p[i], &|i| &codes.o[i], &|i| {
                &codes.g[i]
            }],
        ),
    }
}

/// Both families' children: `perm_by` orders the dataset for a family,
/// `term_of` reads row `i`'s `(s, p, o, g)` terms; each family's lead column
/// is stamped sorted.
fn family_components<'a, V: TermColumn + 'a>(
    perm_by: impl Fn(CopyFamily) -> Vec<usize>,
    term_of: [&dyn Fn(usize) -> &'a V; 4],
) -> Result<Vec<IndexComponent>> {
    let build = |family: CopyFamily| {
        let perm = perm_by(family);
        let columns = family_columns(&perm, term_of);
        stamp_is_sorted(&columns[family.key_order()[0]]);
        (family.identity(), Vec::from(columns))
    };
    components_from(
        &CHILD_COLUMNS,
        [build(CopyFamily::Posg), build(CopyFamily::Ospg)],
    )
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
        let g = oxrdf::GraphName::NamedNode(NamedNode::new("http://example.org/g").unwrap());
        let layout = ResolvedLayout::Default;
        let terms = |pairs: &[(&'static str, TermRef<'_>)]| -> Vec<(&'static str, String)> {
            pairs.iter().map(|(c, t)| (*c, t.to_string())).collect()
        };

        // A bound subject declines: the primary sorted `s` column is the
        // better access path than this index.
        let spo = QuadPattern::new(Some(&s), Some(&p), Some(&o), None);
        assert!(choose(spo, &layout).is_none());

        // Predicate and object bound: (p, o) prefix probe on the POSG family,
        // resolving both components. Every probe serves reads from its
        // family's copy, whose rids follow the family's order.
        let probe = choose(QuadPattern::new(None, Some(&p), Some(&o), None), &layout).unwrap();
        assert_eq!(probe.identity.name, CopyFamily::Posg.identity().name);
        assert_eq!(probe.resolves, ResolvedRoles::PredicateObject);
        assert_eq!(
            terms(&probe.keys),
            [(COL_P, p.to_string()), (COL_O, o.to_string())]
        );
        assert!(probe.residual.is_empty());
        assert!(probe.serve.is_some() && !probe.rids_ascend);

        // Predicate-only patterns probe the POSG lead alone.
        let probe = choose(QuadPattern::new(None, Some(&p), None, None), &layout).unwrap();
        assert_eq!(probe.identity.name, CopyFamily::Posg.identity().name);
        assert_eq!(probe.resolves, ResolvedRoles::Predicate);
        assert_eq!(terms(&probe.keys), [(COL_P, p.to_string())]);

        // Object-only patterns probe the OSPG lead.
        let probe = choose(QuadPattern::new(None, None, Some(&o), None), &layout).unwrap();
        assert_eq!(probe.identity.name, CopyFamily::Ospg.identity().name);
        assert_eq!(probe.resolves, ResolvedRoles::Object);
        assert_eq!(terms(&probe.keys), [(COL_O, o.to_string())]);

        // A bound graph is never a searched key: it rides as a residual term.
        let probe = choose(QuadPattern::new(None, Some(&p), None, Some(&g)), &layout).unwrap();
        assert_eq!(terms(&probe.keys), [(COL_P, p.to_string())]);
        assert_eq!(terms(&probe.residual), [(COL_G, g.to_string())]);

        // Nothing this index covers is bound: declines.
        assert!(choose(QuadPattern::new(None, None, None, Some(&g)), &layout).is_none());
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
        assert_eq!(string_perm(&quads, CopyFamily::Posg), vec![2, 0, 1]);
        // (o, s, p, g): (o0,s0) < (o0,s1) < (o2,s2) → rows 1, 2, 0.
        assert_eq!(string_perm(&quads, CopyFamily::Ospg), vec![1, 2, 0]);

        // The hand-written comparator is the table's order: every pair
        // compares as its terms do in `key_order`.
        for family in [CopyFamily::Posg, CopyFamily::Ospg] {
            let key = |q: &RawQuad| {
                family
                    .key_order()
                    .map(|c| [&q.s, &q.p, &q.o, &q.g][c].clone())
            };
            for a in &quads {
                for b in &quads {
                    assert_eq!(family.cmp_quads(a, b), key(a).cmp(&key(b)), "{family:?}");
                }
            }
        }

        // The code comparator agrees with the string one when codes are
        // lexicographic ranks of the terms.
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
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    fn copy_key_positions_roundtrip() {
        use super::out_of_core::CopyKey;

        // Rearranging [s, p, o, g] into a key and reading it back through
        // key_positions must return the original components.
        let spog = [
            "s".to_string(),
            "p".to_string(),
            "o".to_string(),
            "g".to_string(),
        ];

        let posg = CopyKey::new(CopyFamily::Posg, spog.clone());
        let [s_ix, p_ix, o_ix, g_ix] = CopyFamily::Posg.key_positions();
        assert_eq!(
            [&posg.0[s_ix], &posg.0[p_ix], &posg.0[o_ix], &posg.0[g_ix]],
            [&spog[0], &spog[1], &spog[2], &spog[3]]
        );

        let ospg = CopyKey::new(CopyFamily::Ospg, spog.clone());
        let [s_ix, p_ix, o_ix, g_ix] = CopyFamily::Ospg.key_positions();
        assert_eq!(
            [&ospg.0[s_ix], &ospg.0[p_ix], &ospg.0[o_ix], &ospg.0[g_ix]],
            [&spog[0], &spog[1], &spog[2], &spog[3]]
        );

        // Derived Ord on the key compares by the family's comparator: POSG
        // keys order by predicate first.
        let key = |s: &str, p: &str, o: &str| {
            CopyKey::new(
                CopyFamily::Posg,
                [s.to_string(), p.to_string(), o.to_string(), String::new()],
            )
        };
        assert!(key("s9", "p1", "o9") < key("s0", "p2", "o0"));
    }
}
