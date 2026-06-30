use futures::stream;
use oxrdf::{GraphName, Literal, NamedNode, Quad, Term};
use vortex_rdf_core::VortexRdfError;
use vortex_rdf_core::common::utils::parse_term;
use vortex_rdf_core::io::{
    CottasNativeStringConfig, match_cottas_native_string_file_as_triples,
    serialize_cottas_native_string_file,
};

#[tokio::test]
async fn native_string_object_literal_match_returns_expected_rows() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data_path = dir.path().join("data.vortex");

    let s1 = NamedNode::new("http://example.org/s1").unwrap();
    let s2 = NamedNode::new("http://example.org/s2").unwrap();
    let p = NamedNode::new("http://example.org/p").unwrap();

    let o1 = Term::Literal(Literal::new_language_tagged_literal("Athens", "en").unwrap());
    let o2 = Term::Literal(Literal::new_language_tagged_literal("Paris", "en").unwrap());

    let q1 = Quad::new(s1.clone(), p.clone(), o1, GraphName::DefaultGraph);

    let q2 = Quad::new(s2, p, o2, GraphName::DefaultGraph);

    serialize_cottas_native_string_file(
        stream::iter(vec![
            Ok::<_, VortexRdfError>(q1),
            Ok::<_, VortexRdfError>(q2),
        ]),
        &data_path,
        CottasNativeStringConfig {
            row_group_size: 1,
            ..Default::default()
        },
    )
    .await
    .expect("serialize native string cottas");

    let object = parse_term("\"Athens\"@en").expect("parse object");
    assert_eq!(object.to_string(), "\"Athens\"@en");

    let all_rows = match_cottas_native_string_file_as_triples(&data_path, None, None, None, None)
        .await
        .expect("native string full scan");

    eprintln!("ALL ROWS = {all_rows:#?}");
    eprintln!("FILTER OBJECT = {:?}", object.to_string());

    assert_eq!(all_rows.len(), 2);

    let rows =
        match_cottas_native_string_file_as_triples(&data_path, None, None, Some(&object), None)
            .await
            .expect("native string object match");

    eprintln!("FILTERED ROWS = {rows:#?}");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].2, "\"Athens\"@en");
}

#[tokio::test]
async fn native_string_full_scan_contains_literal_string() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data_path = dir.path().join("data.vortex");

    let s = NamedNode::new("http://example.org/s").unwrap();
    let p = NamedNode::new("http://example.org/p").unwrap();
    let o = Term::Literal(Literal::new_language_tagged_literal("Athens", "en").unwrap());

    let q = Quad::new(s, p, o.clone(), GraphName::DefaultGraph);

    serialize_cottas_native_string_file(
        stream::iter(vec![Ok(q)]),
        &data_path,
        CottasNativeStringConfig {
            row_group_size: 1,
            ..Default::default()
        },
    )
    .await
    .expect("serialize");

    let rows = match_cottas_native_string_file_as_triples(&data_path, None, None, None, None)
        .await
        .expect("full scan");

    eprintln!("object.to_string() = {:?}", o.to_string());
    eprintln!("rows = {:?}", rows);

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].2, o.to_string());
}

#[test]
fn parse_term_preserves_language_tagged_literal() {
    let term = parse_term("\"Athens\"@en").expect("parse language-tagged literal");
    assert_eq!(term.to_string(), "\"Athens\"@en");
}

#[test]
fn parse_term_preserves_typed_literal() {
    let term = parse_term("\"42\"^^<http://www.w3.org/2001/XMLSchema#integer>")
        .expect("parse typed literal");

    assert_eq!(
        term.to_string(),
        "\"42\"^^<http://www.w3.org/2001/XMLSchema#integer>"
    );
}
