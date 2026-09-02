//! The crate's behavioral test suite, split by area. Shared fixtures live
//! here; each submodule covers one section of behavior.

use super::*;
#[cfg(feature = "file-io")]
use crate::io::quads_stream_to_vortex_writer;
use crate::store::VortexArrayBuilder;
use futures::{StreamExt, TryStreamExt, stream};
use oxrdf::{GraphName, Literal, NamedNode, NamedOrBlankNode, Quad, Term};
#[cfg(feature = "file-io")]
use std::sync::OnceLock;

mod adoption;
mod arrow;
mod builders;
mod dictionary;
#[cfg(feature = "file-io")]
mod dictionary_file_backed;
// `pub(crate)`: `common::terms`' inline parser tests borrow this module's
// shared escaped-literal case list.
pub(crate) mod escaping;
#[cfg(feature = "file-io")]
mod file_backed;
mod indexes;
#[cfg(feature = "file-io")]
mod indexes_file;
mod matching;
mod mutation;
mod names;
mod roundtrip;
#[cfg(feature = "file-io")]
mod serialization;
mod streaming;

fn make_quad(s: &str, p: &str, o_lit: &str, g: GraphName) -> Quad {
    Quad::new(
        NamedOrBlankNode::NamedNode(NamedNode::new(s).unwrap()),
        NamedNode::new(p).unwrap(),
        Term::Literal(Literal::new_simple_literal(o_lit)),
        g,
    )
}

/// `n` quads `s{i:02} p{i % p_mod} "object {i % o_mod}"` in the default
/// graph — the suite's stock modular dataset. Subjects are unique and
/// zero-padded (lexicographic order is numeric order); predicates and
/// objects recur on their moduli, so match selectivities are arithmetic.
fn modular_quads(n: usize, p_mod: usize, o_mod: usize) -> Vec<Quad> {
    (0..n)
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{:02}", i),
                &format!("http://example.org/p{}", i % p_mod),
                &format!("object {}", i % o_mod),
                GraphName::DefaultGraph,
            )
        })
        .collect()
}

/// Serialize `quads` with `quads_stream_to_vortex_file` into `store.vortex`
/// inside a fresh temp dir, handing back the dir guard beside the path. Keep
/// the `TempDir` alive for the store's lifetime.
#[cfg(feature = "file-io")]
async fn write_store_file(
    quads: Vec<Quad>,
    layout: LayoutStrategy,
    indexes: Indexes,
) -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.vortex");
    crate::io::quads_stream_to_vortex_file(quad_stream(quads), &path, layout, indexes)
        .await
        .unwrap();
    (dir, path)
}

/// The serialized bytes in `cell`, built by `build` on first use, so a
/// heavyweight fixture is serialized once per process. Concurrent first
/// callers may both build; `get_or_init` keeps one.
#[cfg(feature = "file-io")]
async fn cached_store_bytes<F, Fut>(cell: &'static OnceLock<Vec<u8>>, build: F) -> &'static [u8]
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Vec<u8>>,
{
    if let Some(bytes) = cell.get() {
        return bytes;
    }
    let bytes = build().await;
    cell.get_or_init(|| bytes)
}

fn quad_stream(
    quads: Vec<Quad>,
) -> impl futures::Stream<Item = crate::error::Result<crate::store::RawQuad>> + Unpin + Send + 'static
{
    stream::iter(
        quads
            .into_iter()
            .map(|q| Ok::<_, VortexRdfError>(crate::store::RawQuad::from_quad(&q))),
    )
}

/// [`VortexArrayBuilder::build_vortex_array`] through builder `B` — the
/// trait entrypoint takes its quad stream boxed, and this shim owns the
/// boxing so the suite's many call sites don't each repeat it.
async fn build_array<B: VortexArrayBuilder>(
    quads: impl futures::Stream<Item = crate::error::Result<crate::store::RawQuad>>
    + Unpin
    + Send
    + 'static,
    layout: LayoutStrategy,
    indexes: Indexes,
) -> crate::error::Result<BuiltArray> {
    B::build_vortex_array(Box::new(quads), layout, indexes).await
}

/// The quad-row column names `layout` builds — what a schema assertion
/// expects the primary struct to carry and nothing else.
fn primary_columns(layout: LayoutStrategy) -> &'static [&'static str] {
    match layout {
        LayoutStrategy::Default | LayoutStrategy::Dictionary => &["s", "p", "o", "g"],
        LayoutStrategy::TypedObject => {
            &["s", "p", "o_kind", "o_value", "o_datatype", "o_lang", "g"]
        }
    }
}

/// The names of a build's index children, in emission order — what a schema
/// assertion checks, since index data never rides in the quad rows.
fn component_names(built: &BuiltArray) -> Vec<&'static str> {
    built.components.iter().map(|c| c.name).collect()
}

/// Sorted subject strings of every quad a store exposes.
async fn subjects_of(store: &VortexRdfStore) -> Vec<String> {
    let mut got: Vec<String> = store
        .quads()
        .unwrap()
        .map(|q| q.unwrap().subject.to_string())
        .collect()
        .await;
    got.sort();
    got
}

/// Sorted string forms of every quad a store view exposes.
async fn view_strings(view: &VortexRdfStore) -> Vec<String> {
    quad_strings(&view.quads_vec().await.unwrap())
}

/// Quads with shared terms across positions, a named graph, and the
/// default graph — exercises the single shared dictionary.
fn dictionary_test_quads() -> Vec<Quad> {
    (0..10)
        .map(|i| {
            let g = if i % 2 == 0 {
                GraphName::DefaultGraph
            } else {
                GraphName::NamedNode(NamedNode::new("http://example.org/g").unwrap())
            };
            make_quad(
                &format!("http://example.org/s{:02}", i),
                &format!("http://example.org/p{}", i % 3),
                &format!("object {}", i % 4),
                g,
            )
        })
        .collect()
}

fn quad_strings(quads: &[Quad]) -> Vec<String> {
    let mut v: Vec<String> = quads.iter().map(|q| q.to_string()).collect();
    v.sort();
    v
}

/// An in-memory Default-layout store holding `quads` in exactly the order
/// given, built without sorted provenance — the shape a foreign writer's
/// file arrives in.
fn unstamped_store(quads: &[Quad]) -> VortexRdfStore {
    let raws: Vec<crate::store::RawQuad> =
        quads.iter().map(crate::store::RawQuad::from_quad).collect();
    let array =
        crate::store::builders::build_struct_array(&raws, LayoutStrategy::Default, false).unwrap();
    VortexRdfStore::from_built(BuiltArray {
        array,
        components: vec![],
        dict: None,
    })
    .unwrap()
}

/// Native store bytes over `chunks` (all of one dtype) written without the
/// sorted stamp, carrying `components` beside the rows.
#[cfg(feature = "file-io")]
async fn unstamped_store_bytes(
    chunks: Vec<vortex_array::ArrayRef>,
    components: Vec<crate::io::container::NativeComponentWrite>,
) -> Vec<u8> {
    let dtype = chunks[0].dtype().clone();
    let mut bytes: Vec<u8> = Vec::new();
    crate::io::container::write_store(
        &crate::session::VORTEX_SESSION,
        &mut bytes,
        vortex_array::stream::ArrayStreamAdapter::new(
            dtype,
            Box::pin(stream::iter(chunks.into_iter().map(Ok))),
        ),
        crate::io::container::default_child_strategy(),
        false,
        components,
    )
    .await
    .unwrap();
    bytes
}

/// A minimal native Dictionary-layout file: a one-row zero-code quad child
/// beside `dict` written as the dictionary component.
#[cfg(feature = "file-io")]
pub(crate) async fn write_dict_only_store(
    dict: &crate::store::layouts::dictionary::TermDictionary,
) -> Vec<u8> {
    unstamped_store_bytes(
        vec![bare_code_quad_array(&[0])],
        vec![dict.to_write().unwrap()],
    )
    .await
}

/// A `{s, p, o, g}` struct of four identical non-nullable u32 columns
/// holding `codes` — the Dictionary layout's row shape without a
/// dictionary to give the codes meaning.
fn bare_code_quad_array(codes: &[u32]) -> vortex_array::ArrayRef {
    use vortex_array::IntoArray as _;
    let col = || vortex_buffer::Buffer::from_iter(codes.iter().copied()).into_array();
    vortex_array::arrays::StructArray::try_new(
        ["s", "p", "o", "g"].into(),
        vec![col(), col(), col(), col()],
        codes.len(),
        vortex_array::validity::Validity::NonNullable,
    )
    .unwrap()
    .into_array()
}

/// `<http://example.org/s{i}>` zero-padded to `width` digits.
fn subject_node(i: usize, width: usize) -> NamedOrBlankNode {
    NamedOrBlankNode::NamedNode(NamedNode::new(format!("http://example.org/s{i:0width$}")).unwrap())
}

/// Sorted string forms of the quads at the indexes `keep` accepts.
fn expected_strings(quads: &[Quad], keep: impl Fn(usize) -> bool) -> Vec<String> {
    let mut strings: Vec<String> = quads
        .iter()
        .enumerate()
        .filter(|(i, _)| keep(*i))
        .map(|(_, q)| q.to_string())
        .collect();
    strings.sort();
    strings
}

/// `n` quads `s{i:0width} p{i % p_mod} "o{i % o_mod}"` cycling through
/// `graphs` — the modular dataset with short objects and a graph axis.
fn graph_modular_quads(
    n: usize,
    subject_width: usize,
    p_mod: usize,
    o_mod: usize,
    graphs: &[GraphName],
) -> Vec<Quad> {
    (0..n)
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{i:0subject_width$}"),
                &format!("http://example.org/p{}", i % p_mod),
                &format!("o{}", i % o_mod),
                graphs[i % graphs.len()].clone(),
            )
        })
        .collect()
}

/// The two-quad dataset the Default and TypedObject probes match over.
fn two_quads() -> Vec<Quad> {
    vec![
        make_quad(
            "http://example.org/s1",
            "http://example.org/p1",
            "o1",
            GraphName::DefaultGraph,
        ),
        make_quad(
            "http://example.org/s2",
            "http://example.org/p2",
            "o2",
            GraphName::DefaultGraph,
        ),
    ]
}

/// Default layout over [`two_quads`]: a bound predicate over the string
/// columns, hit and miss.
async fn probe_default(store: VortexRdfStore) {
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let filtered = store
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    assert_eq!(filtered.size().await.unwrap(), 1);
    let results: Vec<Quad> = filtered.quads().unwrap().try_collect().await.unwrap();
    assert_eq!(results[0].subject.to_string(), "<http://example.org/s1>");

    let p3 = NamedNode::new("http://example.org/p3").unwrap();
    let empty = store
        .match_pattern(None, Some(&p3), None, None)
        .await
        .unwrap();
    assert_eq!(empty.size().await.unwrap(), 0);
}

/// TypedObject layout over [`two_quads`]: a bound object literal (over the
/// typed o_kind/o_value columns), hit and miss, and a bound predicate.
async fn probe_typed_object(store: VortexRdfStore) {
    let o1 = Term::Literal(Literal::new_simple_literal("o1"));
    let filtered = store
        .match_pattern(None, None, Some(&o1), None)
        .await
        .unwrap();
    assert_eq!(filtered.size().await.unwrap(), 1);
    let results: Vec<Quad> = filtered.quads().unwrap().try_collect().await.unwrap();
    assert_eq!(results[0].subject.to_string(), "<http://example.org/s1>");
    assert_eq!(results[0].object.to_string(), "\"o1\"");

    let p2 = NamedNode::new("http://example.org/p2").unwrap();
    let matched_p = store
        .match_pattern(None, Some(&p2), None, None)
        .await
        .unwrap();
    assert_eq!(matched_p.size().await.unwrap(), 1);

    let o3 = Term::Literal(Literal::new_simple_literal("o3"));
    let empty = store
        .match_pattern(None, None, Some(&o3), None)
        .await
        .unwrap();
    assert_eq!(empty.size().await.unwrap(), 0);
}

/// One row per term kind in every role, each object distinct: an IRI, a
/// blank node, a plain, a language-tagged and a typed literal as objects;
/// a blank-node subject; a blank-node graph beside a named one and the
/// default graph.
fn term_kind_quads() -> Vec<Quad> {
    use oxrdf::BlankNode;
    let s = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s").unwrap());
    let blank_s = NamedOrBlankNode::BlankNode(BlankNode::new("bs").unwrap());
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let p2 = NamedNode::new("http://example.org/p2").unwrap();
    let g = GraphName::NamedNode(NamedNode::new("http://example.org/g").unwrap());
    let blank_g = GraphName::BlankNode(BlankNode::new("bg").unwrap());
    let plain = |text: &str| Term::Literal(Literal::new_simple_literal(text));
    vec![
        Quad::new(
            s.clone(),
            p1.clone(),
            Term::NamedNode(NamedNode::new("http://example.org/o-iri").unwrap()),
            GraphName::DefaultGraph,
        ),
        Quad::new(
            s.clone(),
            p1.clone(),
            Term::BlankNode(BlankNode::new("bo").unwrap()),
            GraphName::DefaultGraph,
        ),
        Quad::new(s.clone(), p2.clone(), plain("plain"), g.clone()),
        Quad::new(
            s.clone(),
            p2.clone(),
            Term::Literal(Literal::new_language_tagged_literal("hello", "en").unwrap()),
            g,
        ),
        Quad::new(
            s,
            p2.clone(),
            Term::Literal(Literal::new_typed_literal(
                "42",
                NamedNode::new("http://www.w3.org/2001/XMLSchema#integer").unwrap(),
            )),
            GraphName::DefaultGraph,
        ),
        Quad::new(
            blank_s.clone(),
            p1,
            plain("blank subject, blank graph"),
            blank_g.clone(),
        ),
        Quad::new(blank_s, p2, plain("blank subject, second row"), blank_g),
    ]
}

/// 2,000 quads with ~4,000 distinct terms: enough for the dictionary to be
/// FSST-compressed as built.
fn fsst_dictionary_quads() -> Vec<Quad> {
    (0..2_000)
        .map(|i| {
            make_quad(
                &format!("http://example.org/subject/{i:06}"),
                &format!("http://example.org/predicate/{}", i % 16),
                &format!("object value {:06}", i / 2),
                GraphName::DefaultGraph,
            )
        })
        .collect()
}

/// Every chunk of `store`'s held term column is FSST-encoded.
fn assert_dictionary_terms_fsst(store: &VortexRdfStore, when: &str) {
    let dict = store.dictionary_snapshot().unwrap().0;
    for chunk in dict.term_chunks() {
        assert_eq!(
            chunk.encoding_id().to_string(),
            "vortex.fsst",
            "{when}: dictionary chunk not FSST"
        );
    }
}

/// Quads as `(s, p, o, g)` N-Triples tuples, the default graph as `""` —
/// the spelling [`SharedQuad`](crate::store::SharedQuad) rows carry, so the
/// two decodes compare directly.
fn tuple_rows(quads: &[Quad]) -> Vec<(String, String, String, String)> {
    quads
        .iter()
        .map(|q| {
            let r = crate::store::RawQuad::from_quad(q);
            (r.s, r.p, r.o, r.g)
        })
        .collect()
}

fn shared_tuple_rows(rows: &[crate::store::SharedQuad]) -> Vec<(String, String, String, String)> {
    rows.iter()
        .map(|r| {
            (
                r.s.to_string(),
                r.p.to_string(),
                r.o.to_string(),
                r.g.to_string(),
            )
        })
        .collect()
}

/// A view's shared rows must be `quads_vec`'s rows — same content, same
/// order — whether materialized or streamed by chunk, and each must parse
/// back to the quad it came from. Hands the rows back for further checks.
async fn assert_shared_matches_quads(
    view: &VortexRdfStore,
    tag: &str,
) -> Vec<crate::store::SharedQuad> {
    let quads = view.quads_vec().await.unwrap();
    let shared = view.shared_quads_vec().await.unwrap();
    assert_eq!(shared_tuple_rows(&shared), tuple_rows(&quads), "{tag}");
    let chunked: Vec<crate::store::SharedQuad> = view
        .shared_quad_chunks()
        .unwrap()
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .flatten()
        .map(|row| row.unwrap())
        .collect();
    assert_eq!(chunked, shared, "{tag}: chunk stream");
    let parsed: Vec<Quad> = shared.iter().map(|r| r.to_quad().unwrap()).collect();
    assert_eq!(parsed, quads, "{tag}: to_quad");
    shared
}
