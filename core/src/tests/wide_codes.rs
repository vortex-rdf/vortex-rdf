//! Term codes past `u32::MAX`. Every dictionary here is built or opened under
//! the [`CodeBase`] test hook, which numbers its terms from a base above
//! `u32::MAX`: a handful of terms then carry codes a 32-bit width cannot
//! hold, and every path that takes or returns a code — the build's code map,
//! the written code columns, matches through each index, keeps, batched
//! probes, mutation and compaction rebuilds, the dictionary handles under
//! both residencies, and the column kernels — has to carry them unchanged.
//! A code narrowed anywhere on the way lands below the base, where no term
//! is, so a truncation fails loudly instead of answering with another term.

use std::collections::BTreeSet;

use super::*;
use crate::store::test_hooks::CodeBase;
use crate::store::{Keep, Probe, QuadColumn, TermCode, TermPredicate};

/// The first code: past `u32::MAX`, and congruent to 5 modulo 2^32, so a
/// code narrowed to 32 bits becomes a small number — a valid code in an
/// unhooked dictionary of this size, but below every code here.
const BASE: TermCode = (1 << 32) + 5;

fn iri(s: &str) -> NamedNode {
    NamedNode::new(format!("http://example.org/{s}")).unwrap()
}

fn subject(s: &str) -> NamedOrBlankNode {
    NamedOrBlankNode::NamedNode(iri(s))
}

/// Quads covering every term kind: IRIs, plain, language-tagged and typed
/// literals, a blank node, the default graph and a named graph.
fn dataset() -> Vec<Quad> {
    let xsd_int = NamedNode::new("http://www.w3.org/2001/XMLSchema#integer").unwrap();
    let g1 = GraphName::NamedNode(iri("g1"));
    let blank = oxrdf::BlankNode::new("b1").unwrap();
    vec![
        Quad::new(
            subject("alice"),
            iri("name"),
            Term::Literal(Literal::new_simple_literal("Alice")),
            GraphName::DefaultGraph,
        ),
        Quad::new(
            subject("alice"),
            iri("age"),
            Term::Literal(Literal::new_typed_literal("30", xsd_int.clone())),
            GraphName::DefaultGraph,
        ),
        Quad::new(
            subject("alice"),
            iri("knows"),
            Term::NamedNode(iri("bob")),
            g1.clone(),
        ),
        Quad::new(
            subject("bob"),
            iri("name"),
            Term::Literal(Literal::new_language_tagged_literal("Bob", "en").unwrap()),
            GraphName::DefaultGraph,
        ),
        Quad::new(
            subject("bob"),
            iri("knows"),
            Term::BlankNode(blank.clone()),
            GraphName::DefaultGraph,
        ),
        Quad::new(
            NamedOrBlankNode::BlankNode(blank),
            iri("name"),
            Term::Literal(Literal::new_simple_literal("Carol")),
            g1,
        ),
        Quad::new(
            subject("bob"),
            iri("age"),
            Term::Literal(Literal::new_typed_literal("25", xsd_int)),
            GraphName::DefaultGraph,
        ),
    ]
}

/// The dataset's distinct terms in N-Triples spelling, sorted — so term `i`
/// must carry code `BASE + i`.
fn sorted_terms(quads: &[Quad]) -> Vec<String> {
    let mut terms = BTreeSet::new();
    for q in quads {
        let raw = crate::store::RawQuad::from_quad(q);
        terms.extend([raw.s, raw.p, raw.o, raw.g]);
    }
    terms.into_iter().collect()
}

fn as_set(quads: Vec<Quad>) -> BTreeSet<String> {
    quads.into_iter().map(|q| q.to_string()).collect()
}

/// The code of `term`, asserted to be its rank past the base.
async fn code(store: &VortexRdfStore, terms: &[String], term: &str) -> TermCode {
    let rank = terms.iter().position(|t| t == term).unwrap() as TermCode;
    let code = store
        .dict_reader()
        .unwrap()
        .encode(term)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(code, BASE + rank, "{term}");
    code
}

/// The rows of `view` as quads decoded from its gathered code columns,
/// asserting every code is past `u32::MAX`.
async fn decoded_rows(view: &VortexRdfStore) -> BTreeSet<String> {
    let reader = view.dict_reader().unwrap();
    let [s, p, o, g] = view.code_columns_gathered().await.unwrap().unwrap();
    let mut rows = BTreeSet::new();
    for i in 0..s.len() {
        let mut terms = Vec::new();
        for column in [&s, &p, &o, &g] {
            let code = column.as_slice()[i];
            assert!(code > TermCode::from(u32::MAX), "code {code} fits 32 bits");
            terms.push(reader.decode(code).await.unwrap().unwrap());
        }
        let graph = if terms[3].is_empty() {
            String::new()
        } else {
            format!(" {}", terms[3])
        };
        rows.insert(format!("{} {} {}{graph}", terms[0], terms[1], terms[2]));
    }
    rows
}

/// What `as_set` spells for the quads of `quads` passing `keep`.
fn expected(quads: &[Quad], keep: impl Fn(&Quad) -> bool) -> BTreeSet<String> {
    quads
        .iter()
        .filter(|q| keep(q))
        .map(|q| q.to_string())
        .collect()
}

/// Every code-taking and code-returning surface of `store`, which holds
/// `quads` and was built or opened under the [`BASE`] guard.
async fn check_store(store: &VortexRdfStore, quads: &[Quad], label: &str) {
    let terms = sorted_terms(quads);
    let n = terms.len() as TermCode;
    let reader = store.dict_reader().unwrap();
    assert_eq!(reader.len(), terms.len(), "{label}");

    // ── the dictionary handle ───────────────────────────────────────────
    for (rank, term) in terms.iter().enumerate() {
        let code = BASE + rank as TermCode;
        assert_eq!(reader.encode(term).await.unwrap(), Some(code), "{label}");
        assert_eq!(
            reader.decode(code).await.unwrap().as_deref(),
            Some(term.as_str()),
            "{label}"
        );
        // Narrowed to 32 bits, the code is below the base: no term.
        assert_eq!(
            reader.decode(TermCode::from(code as u32)).await.unwrap(),
            None,
            "{label}"
        );
    }
    let codes: Vec<TermCode> = (BASE..BASE + n).collect();
    // Every term, then one the dictionary lacks.
    let mut refs: Vec<&str> = terms.iter().map(String::as_str).collect();
    refs.push("<http://example.org/nobody>");
    let mut encoded: Vec<Option<TermCode>> = codes.iter().copied().map(Some).collect();
    encoded.push(None);
    assert_eq!(reader.encode_many(&refs).await.unwrap(), encoded, "{label}");
    let mut probe = codes.clone();
    probe.extend([BASE - 1, BASE + n, 5, TermCode::MAX]);
    let mut decoded: Vec<Option<String>> = terms.iter().cloned().map(Some).collect();
    decoded.extend([None, None, None, None]);
    assert_eq!(
        reader.decode_many(&probe).await.unwrap(),
        decoded,
        "{label}"
    );
    let iri_lo = BASE + terms.iter().position(|t| t.starts_with('<')).unwrap() as TermCode;
    let iri_hi = BASE + terms.iter().rposition(|t| t.starts_with('<')).unwrap() as TermCode + 1;
    assert_eq!(
        reader.prefix_range("<").await.unwrap(),
        (iri_lo, iri_hi),
        "{label}"
    );
    assert_eq!(reader.lower_bound("<").await.unwrap(), iri_lo, "{label}");
    assert_eq!(reader.lower_bound("~").await.unwrap(), BASE + n, "{label}");
    let kinds = reader.kind_ranges().await.unwrap();
    assert_eq!(kinds.start, BASE, "{label}");
    assert_eq!(kinds.default_graph, Some(BASE), "{label}: \"\" sorts first");
    assert_eq!(kinds.iris, iri_lo..iri_hi, "{label}");
    assert_eq!(kinds.len, BASE + n, "{label}");
    assert!(kinds.literals.start > BASE && kinds.blanks.end <= BASE + n);
    // The gaps start at the first code, not at 0: only `""` is in them
    // (taking two, so gaps from 0 fail fast instead of counting to BASE).
    assert_eq!(
        kinds.gaps().take(2).collect::<Vec<_>>(),
        vec![BASE],
        "{label}"
    );

    let is_iri = TermPredicate::parse("is_iri", "").unwrap();
    let (passed, undecided) = reader.filter_codes(&is_iri, &codes).await.unwrap();
    assert_eq!(
        passed.as_slice(),
        (iri_lo..iri_hi).collect::<Vec<_>>().as_slice(),
        "{label}"
    );
    // The default graph's `""` falls in the kind gaps: undecided.
    assert_eq!(undecided.as_slice(), &[BASE], "{label}");
    let lang = TermPredicate::parse("lang", "en").unwrap();
    let bob = code(store, &terms, "\"Bob\"@en").await;
    let (passed, _) = reader.filter_codes(&lang, &codes).await.unwrap();
    assert_eq!(passed.as_slice(), &[bob], "{label}");
    // A candidate outside the dictionary is refused, below it or past it.
    for outside in [BASE - 1, TermCode::from(bob as u32), BASE + n] {
        assert!(
            reader.filter_codes(&is_iri, &[outside]).await.is_err(),
            "{label}: {outside}"
        );
    }

    // A resident dictionary also answers through its snapshot.
    if let Some(snapshot) = reader.snapshot() {
        assert_eq!(snapshot.encode(&terms[1]), Some(BASE + 1), "{label}");
        assert_eq!(snapshot.encode_many(&refs).unwrap(), encoded, "{label}");
        assert_eq!(
            snapshot.decode(BASE + 1).as_deref(),
            Some(terms[1].as_str())
        );
        assert_eq!(snapshot.decode(TermCode::from((BASE + 1) as u32)), None);
        assert_eq!(
            snapshot.encode_tolerant("http://example.org/bob").unwrap(),
            Some(code(store, &terms, "<http://example.org/bob>").await)
        );
        assert_eq!(snapshot.decode_many(&probe), decoded, "{label}");
        assert_eq!(snapshot.lower_bound("<"), iri_lo);
        assert_eq!(snapshot.prefix_range("<"), (iri_lo, iri_hi));
        assert_eq!(snapshot.kind_ranges(), kinds);
        let (passed, _) = snapshot.filter_codes(&lang, &codes).unwrap();
        assert_eq!(passed.as_slice(), &[bob]);
    }

    // ── a keep before anything gathers codes ────────────────────────────
    // A fresh built base holds its code columns compressed behind an unfilled
    // payload cache, so this keep reads them through the encoded probe
    // rather than a canonical slice.
    let alice = subject("alice");
    let alice_code = code(store, &terms, "<http://example.org/alice>").await;
    let alice_rows = expected(quads, |q| q.subject == alice);
    let early = store
        .keep(QuadColumn::S, &Keep::set([alice_code]))
        .await
        .unwrap();
    assert_eq!(early.size().await.unwrap(), alice_rows.len(), "{label}");

    // ── matches, through every access path ──────────────────────────────
    assert_eq!(decoded_rows(store).await, as_set(quads.to_vec()), "{label}");
    assert_eq!(
        as_set(store.quads_vec().await.unwrap()),
        as_set(quads.to_vec())
    );
    let name = iri("name");
    let by_predicate = store
        .match_pattern(None, Some(&name), None, None)
        .await
        .unwrap();
    assert_eq!(
        decoded_rows(&by_predicate).await,
        expected(quads, |q| q.predicate == name),
        "{label}: predicate (index) match"
    );
    let bob_iri = Term::NamedNode(iri("bob"));
    let by_object = store
        .match_pattern(None, None, Some(&bob_iri), None)
        .await
        .unwrap();
    assert_eq!(
        decoded_rows(&by_object).await,
        expected(quads, |q| q.object == bob_iri),
        "{label}: object (index) match"
    );
    let age = iri("age");
    let residual = store
        .match_pattern(Some(&alice), Some(&age), None, None)
        .await
        .unwrap();
    assert_eq!(
        decoded_rows(&residual).await,
        expected(quads, |q| q.subject == alice && q.predicate == age),
        "{label}: subject + residual match"
    );
    let g1 = GraphName::NamedNode(iri("g1"));
    let by_graph = store
        .match_pattern(None, None, None, Some(&g1))
        .await
        .unwrap();
    assert_eq!(
        decoded_rows(&by_graph).await,
        expected(quads, |q| q.graph_name == g1),
        "{label}: graph match"
    );

    // ── keeps: sets, ranges, and the narrowed code admitting nothing ────
    let kept = store
        .keep(QuadColumn::S, &Keep::set([alice_code]))
        .await
        .unwrap();
    assert_eq!(decoded_rows(&kept).await, alice_rows, "{label}: keep set");
    // A set too sparse for a bitmap is searched instead.
    let sparse = Keep::set([alice_code, alice_code + (1 << 33)]);
    let kept = store.keep(QuadColumn::S, &sparse).await.unwrap();
    assert_eq!(
        decoded_rows(&kept).await,
        alice_rows,
        "{label}: sparse keep set"
    );
    let kept = store.keep(QuadColumn::O, &sparse).await.unwrap();
    assert_eq!(kept.size().await.unwrap(), 0, "{label}: sparse keep on o");
    let kept = store
        .keep(QuadColumn::S, &Keep::range(alice_code..alice_code + 1))
        .await
        .unwrap();
    assert_eq!(decoded_rows(&kept).await, alice_rows, "{label}: keep range");
    let narrowed = TermCode::from(alice_code as u32);
    for keep in [Keep::set([narrowed]), Keep::range(narrowed..narrowed + 1)] {
        let kept = store.keep(QuadColumn::S, &keep).await.unwrap();
        assert_eq!(kept.size().await.unwrap(), 0, "{label}: {keep:?}");
    }
    let literal_keep = Keep::range(kinds.literals.clone());
    let kept = by_predicate
        .keep(QuadColumn::O, &literal_keep)
        .await
        .unwrap();
    assert_eq!(
        decoded_rows(&kept).await,
        expected(quads, |q| q.predicate == name
            && matches!(q.object, Term::Literal(_))),
        "{label}: keep a kind range after an index match"
    );

    // ── batched probes ───────────────────────────────────────────────────
    let probes = [
        Probe::new(None, Some(name.clone()), None, None).keep(QuadColumn::O, Keep::set([bob])),
        Probe::new(None, None, None, None).keep(QuadColumn::S, Keep::set([alice_code])),
        Probe::new(None, None, None, None).keep(QuadColumn::S, Keep::set([narrowed])),
        Probe::new(None, None, None, None)
            .keep(QuadColumn::G, Keep::range(BASE..BASE + 1))
            .window(1, Some(2)),
    ];
    assert_eq!(
        store.count_many(&probes).await.unwrap(),
        vec![1, alice_rows.len(), 0, 2],
        "{label}"
    );
    let matched = store.run_probe(&probes[0]).await.unwrap();
    let [_, _, o, _] = matched.code_columns_gathered().await.unwrap().unwrap();
    assert_eq!(o.as_slice(), &[bob], "{label}");
    let views = store.match_many(&probes).await.unwrap();
    let bob_name = Term::Literal(Literal::new_language_tagged_literal("Bob", "en").unwrap());
    assert_eq!(
        decoded_rows(&views[0]).await,
        expected(quads, |q| q.predicate == name && q.object == bob_name),
        "{label}: match_many"
    );
    assert_eq!(
        decoded_rows(&views[1]).await,
        alice_rows,
        "{label}: match_many"
    );
    assert_eq!(views[2].size().await.unwrap(), 0, "{label}: match_many");
    assert_eq!(views[3].size().await.unwrap(), 2, "{label}: match_many");

    // ── the column kernels over wide codes ──────────────────────────────
    let [s, p, _, _] = store.code_columns_gathered().await.unwrap().unwrap();
    let distinct = crate::columns::distinct_first_seen(s.as_slice());
    assert!(distinct.as_slice().iter().all(|&c| c >= BASE));
    let (values, counts) = crate::columns::value_counts(p.as_slice());
    assert_eq!(values.len(), 3, "{label}: name, age, knows");
    assert_eq!(counts.as_slice().iter().sum::<u64>(), p.len() as u64);
    let (left, right) = crate::columns::equi_join_indices(s.as_slice(), distinct.as_slice());
    assert_eq!(
        left.len(),
        s.len(),
        "{label}: every row joins its own subject"
    );
    let joined = crate::columns::take(distinct.as_slice(), right.as_slice()).unwrap();
    let rows = crate::columns::take(s.as_slice(), left.as_slice()).unwrap();
    assert_eq!(joined.as_slice(), rows.as_slice(), "{label}");
}

fn indexes() -> Indexes {
    vec![IndexType::SecondaryByReference, IndexType::SecondaryByCopy]
}

/// An in-memory build and its serialized bytes reopened: every surface
/// carries codes past `u32::MAX`.
#[tokio::test]
async fn codes_past_u32_round_trip_in_memory() {
    let _base = CodeBase::set(BASE);
    let quads = dataset();
    let store = VortexRdfStore::from_quads(
        quad_stream(quads.clone()),
        LayoutStrategy::Dictionary,
        indexes(),
    )
    .await
    .unwrap();
    // Compressed and uncached, so the first keep reads through the probe.
    #[cfg(feature = "file-io")]
    assert_eq!(store.debug_base_child_int_canonical("s"), Some(false));
    check_store(&store, &quads, "built in memory").await;

    // Handed across as parts and adopted.
    let parts = store.to_serializable_parts().await.unwrap();
    let adopted = VortexRdfStore::from_parts(parts).unwrap();
    check_store(&adopted, &quads, "from_parts").await;

    // Serialization needs a writer: compiled in under `file-io` (and on wasm).
    #[cfg(feature = "file-io")]
    {
        let bytes = store.to_bytes().await.unwrap();
        let reopened = VortexRdfStore::from_bytes(&bytes).await.unwrap();
        check_store(&reopened, &quads, "from_bytes").await;
    }
}

/// A file written and reopened under the guard: its code columns are u64
/// and hold the wide codes, and the mapped (file-backed dictionary) and
/// in-memory opens both carry them.
#[cfg(feature = "file-io")]
#[tokio::test]
async fn codes_past_u32_round_trip_through_a_file() {
    use vortex_array::dtype::{DType, Nullability, PType};

    let _base = CodeBase::set(BASE);
    let quads = dataset();
    let (_dir, path) = write_store_file(quads.clone(), LayoutStrategy::Dictionary, indexes()).await;

    // The wire: every code column of the quad table and of the index
    // children is a non-nullable u64; row ids stay u32.
    let bytes = std::fs::read(&path).unwrap();
    let (_, components) = crate::io::container::store_metadata_of_bytes(&bytes);
    let u64_dtype = DType::Primitive(PType::U64, Nullability::NonNullable);
    let u32_dtype = DType::Primitive(PType::U32, Nullability::NonNullable);
    let indexes_seen = components
        .iter()
        .filter(|c| c.name.starts_with("index:"))
        .inspect(|component| {
            let DType::Struct(fields, _) = &component.dtype else {
                panic!("{} is a struct", component.name);
            };
            for (name, dtype) in fields.names().iter().zip(fields.fields()) {
                let want = if name.as_ref() == "rid" {
                    &u32_dtype
                } else {
                    &u64_dtype
                };
                assert_eq!(&dtype, want, "{}.{name}", component.name);
            }
        })
        .count();
    assert_eq!(indexes_seen, 4, "two reference and two copy children");
    {
        use vortex_file::OpenOptionsSessionExt as _;
        let file = crate::session::VORTEX_SESSION
            .open_options()
            .open_buffer(vortex_buffer::ByteBuffer::from(bytes))
            .unwrap();
        let DType::Struct(fields, _) = file.dtype() else {
            panic!("the quad table is a struct");
        };
        for column in ["s", "p", "o", "g"] {
            assert_eq!(fields.field(column).as_ref(), Some(&u64_dtype), "{column}");
        }
    }

    let mapped = VortexRdfStore::from_file(&path).await.unwrap();
    assert!(mapped.dict_reader().unwrap().is_file_backed());
    check_store(&mapped, &quads, "mapped").await;

    let resident = VortexRdfStore::from_file_in_memory(&path).await.unwrap();
    assert!(!resident.dict_reader().unwrap().is_file_backed());
    check_store(&resident, &quads, "from_file_in_memory").await;
}

/// The rebuilds — an append flattened by compaction, a deletion — mint a
/// fresh dictionary under the guard, and the rebuilt store's codes are wide
/// again.
#[tokio::test]
async fn codes_past_u32_survive_mutation_rebuilds() {
    let _base = CodeBase::set(BASE);
    let mut quads = dataset();
    let extra = quads.pop().unwrap();
    let store = VortexRdfStore::from_quads(
        quad_stream(quads.clone()),
        LayoutStrategy::Dictionary,
        indexes(),
    )
    .await
    .unwrap();
    let grown = store
        .add_quad(extra.clone())
        .await
        .unwrap()
        .compact()
        .await
        .unwrap();
    quads.push(extra.clone());
    check_store(&grown, &quads, "added and compacted").await;

    let shrunk = grown
        .delete_quad(&extra)
        .await
        .unwrap()
        .compact()
        .await
        .unwrap();
    quads.pop();
    check_store(&shrunk, &quads, "deleted and compacted").await;
}
