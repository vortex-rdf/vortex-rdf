//! The crate's behavioral test suite, split by area. Shared fixtures live
//! here; each submodule covers one section of behavior.

use super::*;
#[cfg(feature = "file-io")]
use crate::io::quads_stream_to_vortex_writer;
use crate::store::VortexArrayBuilder;
use futures::{StreamExt, TryStreamExt, stream};
use oxrdf::{GraphName, Literal, NamedNode, NamedOrBlankNode, Quad, Term};

mod dictionary;
#[cfg(feature = "file-io")]
mod dictionary_file_backed;
// `pub(crate)`: `common::terms`' inline parser tests borrow this module's
// shared escaped-literal case list.
pub(crate) mod escaping;
#[cfg(feature = "file-io")]
mod file_backed;
mod indexes;
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

/// Serialize `quads` through builder `B` into `store.vortex` inside a fresh
/// temp dir, handing back the dir guard beside the path. Keep the `TempDir`
/// alive for the store's lifetime; dropping it cleans up even when an
/// assertion panics, which the old hand-rolled `temp_dir() + uuid` pattern
/// never did.
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

/// The serialized bytes in `cell`, built by `build` on first use — the
/// heavyweight fixtures (24k-row zone-map files, the 20k-term dictionary)
/// are serialized once per process and shared by every test over that
/// shape. Concurrent first callers may both build; `get_or_init` keeps one.
#[cfg(feature = "file-io")]
async fn cached_store_bytes<F, Fut>(
    cell: &'static std::sync::OnceLock<Vec<u8>>,
    build: F,
) -> &'static [u8]
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

/// A quad stream serialized to native store bytes with the suite's default
/// configuration (Default layout, no indexes).
#[cfg(feature = "file-io")]
async fn quads_stream_to_vortex<S>(quads: S) -> crate::error::Result<Vec<u8>>
where
    S: futures::Stream<Item = crate::error::Result<crate::store::RawQuad>> + Unpin + Send + 'static,
{
    let mut buffer = Vec::new();
    quads_stream_to_vortex_writer(quads, &mut buffer, LayoutStrategy::Default, Vec::new()).await?;
    Ok(buffer)
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
