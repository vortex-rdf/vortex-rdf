//! Literal escaping: the store's columns hold N-Triples *escaped* lexical
//! forms, so every decode path has to unescape. The pure parser mechanism
//! tests (structure scan, escape decoding, backslash-run parity) live inline
//! beside the scanner in `common::terms`; this file keeps the store-level
//! round-trips.

use super::*;
use std::collections::HashMap;

/// Literals whose lexical value either needs escaping when serialized or
/// contains a sequence that looks like literal structure (`^^`, `"@`).
/// Shared with `common::terms`' inline parser tests, so the parse family and
/// the store round-trips are exercised over one case list.
pub(crate) fn escaped_literal_cases() -> Vec<Term> {
    let dt = NamedNode::new("http://example.org/dt").unwrap();
    vec![
        Term::Literal(Literal::new_simple_literal("with \"quotes\" inside")),
        Term::Literal(Literal::new_simple_literal("back\\slash")),
        Term::Literal(Literal::new_simple_literal("trailing backslash \\")),
        Term::Literal(Literal::new_simple_literal("line\nbreak")),
        Term::Literal(Literal::new_simple_literal("carriage\rreturn")),
        Term::Literal(Literal::new_simple_literal("tab\there")),
        Term::Literal(Literal::new_simple_literal(
            "caret^^<http://example.org/nope> inside",
        )),
        Term::Literal(Literal::new_simple_literal("say \"hi\"@home")),
        Term::Literal(Literal::new_simple_literal("emoji \u{1F600} tail")),
        Term::Literal(Literal::new_language_tagged_literal("q\"x", "en").unwrap()),
        Term::Literal(Literal::new_language_tagged_literal("say \"hi\"@home", "en").unwrap()),
        Term::Literal(Literal::new_language_tagged_literal("back\\slash", "en-GB").unwrap()),
        Term::Literal(Literal::new_typed_literal("a \"b\" ^^ c", dt.clone())),
        Term::Literal(Literal::new_typed_literal("back\\slash\nand a newline", dt)),
    ]
}

fn escaped_literal_quads() -> Vec<Quad> {
    escaped_literal_cases()
        .into_iter()
        .enumerate()
        .map(|(i, o)| {
            Quad::new(
                subject_node(i, 2),
                NamedNode::new("http://example.org/p").unwrap(),
                o,
                GraphName::DefaultGraph,
            )
        })
        .collect()
}

async fn run_escaped_literal_roundtrip(layout: LayoutStrategy, indexes: Vec<IndexType>) {
    let quads = escaped_literal_quads();

    let store = built_store(quads.clone(), layout, indexes).await;

    let decoded = store.quads_vec().await.unwrap();
    assert_eq!(decoded.len(), quads.len());
    let mut by_subject: HashMap<String, Term> = decoded
        .into_iter()
        .map(|q| (q.subject.to_string(), q.object))
        .collect();

    for q in &quads {
        let object = by_subject
            .remove(&q.subject.to_string())
            .unwrap_or_else(|| panic!("{:?}: missing row for {}", layout, q.subject));
        assert_eq!(object, q.object, "{:?}: object not round-tripped", layout);

        let matched = matched_view(&store, None, None, Some(&q.object), None).await;
        assert_eq!(
            matched.size().await.unwrap(),
            1,
            "{:?}: match by {} did not select exactly its row",
            layout,
            q.object
        );
        let rows = matched.quads_vec().await.unwrap();
        assert_eq!(rows[0].subject, q.subject);
        assert_eq!(rows[0].object, q.object);
    }
}

#[tokio::test]
async fn escaped_literals_roundtrip_default_layout() {
    run_escaped_literal_roundtrip(LayoutStrategy::Default, vec![]).await;
}

#[tokio::test]
async fn escaped_literals_roundtrip_dictionary_layout() {
    run_escaped_literal_roundtrip(LayoutStrategy::Dictionary, vec![]).await;
}

#[tokio::test]
async fn escaped_literals_roundtrip_typed_object_layout() {
    run_escaped_literal_roundtrip(LayoutStrategy::TypedObject, vec![]).await;
}

/// An index-routed O-bound match must agree with the scan on the stored
/// object spelling (escaped string): both secondary index kinds hold that
/// spelling — the by-reference object family and the by-copy `ospg` family —
/// under the string and code layouts alike.
#[tokio::test]
async fn escaped_literals_roundtrip_with_secondary_indexes() {
    for layout in [LayoutStrategy::Default, LayoutStrategy::Dictionary] {
        for indexes in [
            vec![IndexType::SecondaryByReference],
            vec![IndexType::SecondaryByCopy],
            vec![IndexType::SecondaryByCopy, IndexType::SecondaryByReference],
        ] {
            run_escaped_literal_roundtrip(layout, indexes).await;
        }
    }
}
