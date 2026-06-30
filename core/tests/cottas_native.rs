use futures::stream;
use oxrdf::{GraphName, NamedNode, Quad, Term};
use vortex_rdf_core::common::utils::parse_subject;
use vortex_rdf_core::index::SimpleDictionary;
use vortex_rdf_core::io::{
    CottasNativeConfig, match_cottas_native_file, serialize_cottas_native_file,
};

#[tokio::test]
async fn native_cottas_subject_match_returns_expected_rows() {
    let dir = tempfile::tempdir().expect("tempdir");
    let data_path = dir.path().join("data.vortex");
    let output_path = dir.path().join("out.nq");

    let s1 = NamedNode::new("http://example.org/s1").unwrap();
    let s2 = NamedNode::new("http://example.org/s2").unwrap();
    let p = NamedNode::new("http://example.org/p").unwrap();
    let o1 = NamedNode::new("http://example.org/o1").unwrap();
    let o2 = NamedNode::new("http://example.org/o2").unwrap();

    let q1 = Quad::new(
        s1.clone(),
        p.clone(),
        Term::NamedNode(o1),
        GraphName::DefaultGraph,
    );

    let q2 = Quad::new(s2, p, Term::NamedNode(o2), GraphName::DefaultGraph);

    let quads = vec![Ok(q1), Ok(q2)];

    serialize_cottas_native_file::<SimpleDictionary, _>(
        stream::iter(quads),
        &data_path,
        CottasNativeConfig {
            row_group_size: 1,
            ..Default::default()
        },
    )
    .await
    .expect("serialize native cottas");

    let writer = std::fs::File::create(&output_path).expect("create output");

    let subject = parse_subject("<http://example.org/s1>").expect("parse subject");

    match_cottas_native_file(
        &data_path,
        Some(&subject),
        None,
        None,
        None,
        writer,
        oxrdfio::RdfFormat::NQuads,
    )
    .await
    .expect("native match");

    let output = std::fs::read_to_string(output_path).expect("read output");

    assert!(output.contains("http://example.org/s1"));
    assert!(!output.contains("http://example.org/s2"));
}
