//! The crate's behavioral test suite, split by area. Shared fixtures live
//! here; each submodule covers one section of behavior.

use super::*;
use crate::io::quads_stream_to_vortex_writer;
use crate::observe::{Event, Recorder};
use crate::store::VortexArrayBuilder;
use crate::store::indexes::ALL_INDEX_TYPES;
use futures::{StreamExt, TryStreamExt, stream};
use oxrdf::{GraphName, Literal, NamedNode, NamedOrBlankNode, Quad, Term};
use std::sync::OnceLock;

mod adoption;
mod atomic_write;
mod builders;
mod chained;
mod chunks;
mod dictionary;
mod dictionary_file_backed;
mod dictionary_handle;
// `pub(crate)`: `common::terms`' inline parser tests borrow this module's
// shared escaped-literal case list.
pub(crate) mod escaping;
mod file_backed;
mod indexes;
mod indexes_file;
mod leaf_geometry;
mod mapping;
mod masks_first;
mod matching;
mod metadata;
mod mutation;
mod names;
mod narrowing;
mod open_errors;
mod pending_residual;
mod predicates;
mod rebuild;
mod roundtrip;
mod row_limit;
mod serialization;
mod streaming;
mod tail_keys;
mod unique_quads;
mod wide_codes;
mod wide_row_ids;
mod zone_bounds;

const LAYOUTS: [LayoutStrategy; 3] = [
    LayoutStrategy::Default,
    LayoutStrategy::TypedObject,
    LayoutStrategy::Dictionary,
];

/// A store's index configuration in the suites' fixture matrix: no index,
/// or one family alone.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum IndexConfig {
    NoIndex,
    Copy,
    Reference,
}

impl IndexConfig {
    /// Every configuration, in the order the suites check them.
    const ALL: [IndexConfig; 3] = [
        IndexConfig::NoIndex,
        IndexConfig::Copy,
        IndexConfig::Reference,
    ];

    /// The index set a store of this configuration is built with.
    fn types(self) -> Indexes {
        match self {
            IndexConfig::NoIndex => vec![],
            IndexConfig::Copy => vec![IndexType::SecondaryByCopy],
            IndexConfig::Reference => vec![IndexType::SecondaryByReference],
        }
    }

    /// The configuration as a fixture's tag names it.
    fn name(self) -> &'static str {
        match self {
            IndexConfig::NoIndex => "no-index",
            IndexConfig::Copy => "copy",
            IndexConfig::Reference => "reference",
        }
    }
}

/// Every index set worth building: none, each family alone, both.
fn index_sets() -> Vec<Indexes> {
    IndexConfig::ALL
        .into_iter()
        .map(IndexConfig::types)
        .chain([ALL_INDEX_TYPES.to_vec()])
        .collect()
}

/// The IRI `<http://example.org/{name}>`: the namespace of the suites'
/// terms.
fn iri(name: &str) -> NamedNode {
    NamedNode::new(format!("http://example.org/{name}")).unwrap()
}

/// [`iri`] as a subject.
fn iri_subject(name: &str) -> NamedOrBlankNode {
    NamedOrBlankNode::NamedNode(iri(name))
}

/// [`iri`] as a named graph.
fn iri_graph(name: &str) -> GraphName {
    GraphName::NamedNode(iri(name))
}

/// Graph `k` of a dataset that numbers its graphs: the default graph for 0,
/// `g{k}` for any other `k`.
fn numbered_graph(k: usize) -> GraphName {
    match k {
        0 => GraphName::DefaultGraph,
        k => iri_graph(&format!("g{k}")),
    }
}

/// The plain literal `"{value}"`, as an object.
fn plain_literal(value: &str) -> Term {
    Term::Literal(Literal::new_simple_literal(value))
}

fn make_quad(s: &str, p: &str, o_lit: &str, g: GraphName) -> Quad {
    Quad::new(
        NamedOrBlankNode::NamedNode(NamedNode::new(s).unwrap()),
        NamedNode::new(p).unwrap(),
        plain_literal(o_lit),
        g,
    )
}

/// `n` quads `s{i:02} p{i % p_mod} "object {i % o_mod}"` in the default
/// graph: the suite's stock modular dataset. Subjects are unique and
/// zero-padded to two digits (lexicographic order is numeric order below
/// 100); predicates and objects recur on their moduli, so match
/// selectivities are arithmetic.
pub(crate) fn modular_quads(n: usize, p_mod: usize, o_mod: usize) -> Vec<Quad> {
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
pub(crate) async fn write_store_file(
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

/// A Dictionary-layout store file holding `quads` out of subject order —
/// rotated left by `rotate` rows — and written without the sorted stamp: the
/// shape a foreign writer's file arrives in. Keep the `TempDir` alive for the
/// store's lifetime.
async fn write_unsorted_store_file(
    quads: &[Quad],
    rotate: usize,
) -> (tempfile::TempDir, std::path::PathBuf) {
    use crate::store::layouts::dictionary::{self, TermDictionary};

    let mut raws: Vec<crate::store::RawQuad> =
        quads.iter().map(crate::store::RawQuad::from_quad).collect();
    raws.rotate_left(rotate);
    let (dict, code_map) = TermDictionary::from_quads_with_map(&raws).unwrap();
    let codes = dictionary::encode_quads(&raws, &code_map).unwrap();
    let primary = dictionary::build_code_chunk(&codes, 0..raws.len(), false).unwrap();
    let parts = crate::store::BuiltArray {
        array: primary,
        components: Vec::new(),
        dict: Some(std::sync::Arc::new(dict)),
    };
    let mut bytes: Vec<u8> = Vec::new();
    crate::io::write::built_stream_to_vortex_writer(parts.into_stream().unwrap(), &mut bytes)
        .await
        .unwrap();
    temp_store_file(&bytes)
}

/// A Dictionary-layout store file holding `quads` in global `s` order and
/// stamped sorted, with every column written as `chunk_rows`-row flat leaves:
/// the multi-leaf columns a large file written by the default strategy has,
/// at a scale a test can afford (the default strategy writes a 160,000-row
/// column as one leaf). Keep the `TempDir` alive for the store's lifetime.
async fn write_chunked_store_file(
    quads: &[Quad],
    chunk_rows: usize,
) -> (tempfile::TempDir, std::path::PathBuf) {
    write_chunked_store(quads, chunk_rows, false, false, None).await
}

/// [`write_chunked_store_file`] with the reference index built as well and
/// its two children written in `chunk_rows`-row flat leaves too — the
/// multi-leaf `val` and `rid` columns of a large file, so that a located run
/// can cross a leaf boundary at a scale a test can afford.
async fn write_chunked_reference_store_file(
    quads: &[Quad],
    chunk_rows: usize,
) -> (tempfile::TempDir, std::path::PathBuf) {
    write_chunked_store(quads, chunk_rows, true, false, None).await
}

/// The file behind [`write_chunked_store_file`] and
/// [`write_chunked_reference_store_file`]; with `flat_dictionary` its
/// dictionary child is one flat struct leaf, a shape the file-backed
/// dictionary declines, so a mapped open holds the dictionary resident. With
/// `unprobed`, that primary column is written as a dictionary whose codes are
/// a dictionary again, a shape the chunk probes decline, so the column has
/// none.
async fn write_chunked_store(
    quads: &[Quad],
    chunk_rows: usize,
    reference_index: bool,
    flat_dictionary: bool,
    unprobed: Option<&str>,
) -> (tempfile::TempDir, std::path::PathBuf) {
    use crate::io::container::BufferedComponentSource;
    use crate::store::indexes::IndexInput;
    use crate::store::layouts::dictionary::{self, TermDictionary};
    use vortex_array::IntoArray as _;
    use vortex_layout::layouts::chunked::writer::ChunkedLayoutStrategy;
    use vortex_layout::layouts::flat::writer::FlatLayoutStrategy;
    use vortex_layout::layouts::struct_::StructStrategy;

    let mut raws: Vec<crate::store::RawQuad> =
        quads.iter().map(crate::store::RawQuad::from_quad).collect();
    raws.sort_unstable();
    let (dict, code_map) = TermDictionary::from_quads_with_map(&raws).unwrap();
    let codes = dictionary::encode_quads(&raws, &code_map).unwrap();
    let chunks: Vec<vortex_array::ArrayRef> = (0..raws.len())
        .step_by(chunk_rows)
        .map(|start| {
            let end = (start + chunk_rows).min(raws.len());
            dictionary::build_code_chunk(&codes, start..end, true).unwrap()
        })
        .collect();
    let dtype = chunks[0].dtype().clone();
    let leaves = ChunkedLayoutStrategy::new(FlatLayoutStrategy::default());
    let mut dictionary = dict.to_write().unwrap();
    if flat_dictionary {
        dictionary.strategy = std::sync::Arc::new(FlatLayoutStrategy::default());
    }
    let mut components = vec![dictionary];
    if reference_index {
        // The children's rows are the global `(value, row id)` sort of the
        // codes, cut into `chunk_rows`-row chunks; each chunk is one flat leaf
        // of each of its columns.
        for component in IndexType::SecondaryByReference
            .components(IndexInput::Codes(&codes))
            .unwrap()
        {
            let rows = component.rows().unwrap().clone().into_array();
            let cut: Vec<vortex_array::ArrayRef> = (0..rows.len())
                .step_by(chunk_rows)
                .map(|start| {
                    rows.slice(start..(start + chunk_rows).min(rows.len()))
                        .unwrap()
                })
                .collect();
            let mut write = component.to_write().unwrap();
            write.source = std::sync::Arc::new(BufferedComponentSource::try_new(cut).unwrap());
            write.strategy = std::sync::Arc::new(StructStrategy::new(
                std::sync::Arc::new(FlatLayoutStrategy::default()),
                std::sync::Arc::new(leaves.clone()),
            ));
            components.push(write);
        }
    }
    let primary: std::sync::Arc<dyn vortex_layout::LayoutStrategy> = match unprobed {
        None => std::sync::Arc::new(StructStrategy::new(
            std::sync::Arc::new(FlatLayoutStrategy::default()),
            std::sync::Arc::new(leaves),
        )),
        Some(column) => {
            use vortex_layout::layouts::dict::writer::DictStrategy;
            use vortex_layout::layouts::table::TableStrategy;
            // Every chunk dictionary-encodes, so each level is a dictionary.
            let always = |chunk: &vortex_array::ArrayRef, ctx: &mut vortex_array::ExecutionCtx| {
                vortex_array::builders::dict::dict_encode(chunk, ctx).map(|dict| dict.into_array())
            };
            let inner = DictStrategy::new(
                leaves.clone(),
                FlatLayoutStrategy::default(),
                leaves.clone(),
                Default::default(),
                std::sync::Arc::new(always),
            );
            let dict = DictStrategy::new(
                inner,
                FlatLayoutStrategy::default(),
                leaves.clone(),
                Default::default(),
                std::sync::Arc::new(always),
            );
            std::sync::Arc::new(
                TableStrategy::new(
                    std::sync::Arc::new(FlatLayoutStrategy::default()),
                    std::sync::Arc::new(leaves),
                )
                .with_field_writer(
                    vortex_array::dtype::FieldPath::from_name(column),
                    std::sync::Arc::new(dict),
                ),
            )
        }
    };
    let mut bytes: Vec<u8> = Vec::new();
    crate::io::container::write_store(
        &crate::session::VORTEX_SESSION,
        &mut bytes,
        vortex_array::stream::ArrayStreamAdapter::new(
            dtype,
            Box::pin(stream::iter(chunks.into_iter().map(Ok))),
        ),
        primary,
        true,
        components,
    )
    .await
    .unwrap();
    temp_store_file(&bytes)
}

/// `bytes` written to `store.vortex` in a fresh temp dir, handing back the dir
/// guard beside the path: the file of a test that holds a store's serialized
/// bytes rather than its quads (see [`write_store_file`]). Keep the `TempDir`
/// alive for the file's lifetime.
pub(crate) fn temp_store_file(bytes: &[u8]) -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.vortex");
    std::fs::write(&path, bytes).unwrap();
    (dir, path)
}

/// The serialized bytes in `cell`, built by `build` on first use, so a
/// heavyweight fixture is serialized once per process. Concurrent first
/// callers may both build; `get_or_init` keeps one.
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

/// A store file of `quads()` written with `layout` and `indexes`, in a fresh
/// temp dir. The bytes are serialized once per process into `cell` (see
/// [`cached_store_bytes`]) and `quads` runs only for that write, so a fixture
/// too heavy to write per test is written once. Keep the `TempDir` alive for
/// the file's lifetime.
async fn cached_store_file(
    cell: &'static OnceLock<Vec<u8>>,
    quads: impl FnOnce() -> Vec<Quad>,
    layout: LayoutStrategy,
    indexes: Indexes,
) -> (tempfile::TempDir, std::path::PathBuf) {
    let bytes = cached_store_bytes(cell, || async move {
        let mut bytes: Vec<u8> = Vec::new();
        quads_stream_to_vortex_writer(quad_stream(quads()), &mut bytes, layout, indexes)
            .await
            .unwrap();
        bytes
    })
    .await;
    temp_store_file(bytes)
}

/// A Dictionary-layout store file of `modular_quads(160_000, 3, 8)`, without
/// indexes, in a fresh temp dir: about 160,012 terms, so a dictionary of
/// three 65,536-term windows, and rows past one scan split. Serialized once
/// per process (see [`cached_store_file`]). Keep the `TempDir` alive for the
/// file's lifetime.
async fn multi_window_dictionary_file() -> (tempfile::TempDir, std::path::PathBuf) {
    static BYTES: OnceLock<Vec<u8>> = OnceLock::new();
    cached_store_file(
        &BYTES,
        || modular_quads(160_000, 3, 8),
        LayoutStrategy::Dictionary,
        vec![],
    )
    .await
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

/// The store [`SortedInMemoryBuilder`] builds from `quads` — [`build_array`]
/// and [`VortexRdfStore::from_built`] in one call, for the tests that never
/// look at the built array itself.
pub(crate) async fn built_store(
    quads: Vec<Quad>,
    layout: LayoutStrategy,
    indexes: Indexes,
) -> VortexRdfStore {
    let built = build_array::<SortedInMemoryBuilder>(quad_stream(quads), layout, indexes)
        .await
        .unwrap();
    VortexRdfStore::from_built(built).unwrap()
}

/// The store [`VortexRdfStore::from_quads`] builds from `quads`, unwrapped:
/// [`built_store`]'s counterpart through the crate's own entry point, which
/// sorts through [`SortedStreamBuilder`] wherever a filesystem exists.
async fn streamed_store(
    quads: Vec<Quad>,
    layout: LayoutStrategy,
    indexes: Indexes,
) -> VortexRdfStore {
    VortexRdfStore::from_quads(quad_stream(quads), layout, indexes)
        .await
        .unwrap()
}

/// The view of `store` the pattern `(s, p, o, g)` matches: a
/// [`match_pattern`](VortexRdfStore::match_pattern), unwrapped.
async fn matched_view(
    store: &VortexRdfStore,
    s: Option<&NamedOrBlankNode>,
    p: Option<&NamedNode>,
    o: Option<&Term>,
    g: Option<&GraphName>,
) -> VortexRdfStore {
    store.match_pattern(s, p, o, g).await.unwrap()
}

/// The number of quads `store` holds for the pattern `(s, p, o, g)`: the
/// [`matched_view`]'s [`size`](VortexRdfStore::size), unwrapped.
async fn matched_size(
    store: &VortexRdfStore,
    s: Option<&NamedOrBlankNode>,
    p: Option<&NamedNode>,
    o: Option<&Term>,
    g: Option<&GraphName>,
) -> usize {
    matched_view(store, s, p, o, g).await.size().await.unwrap()
}

/// `store` with each quad of `doomed` deleted, in turn: a
/// [`delete_quad`](VortexRdfStore::delete_quad) each, unwrapped.
async fn delete_all<'a>(
    store: &VortexRdfStore,
    doomed: impl IntoIterator<Item = &'a Quad>,
) -> VortexRdfStore {
    let mut store = store.clone();
    for quad in doomed {
        store = store.delete_quad(quad).await.unwrap();
    }
    store
}

/// The index child that answers `view`'s reads from its own columns, if any:
/// the [`served_component`](ViewStatistics::served_component) of its
/// statistics. Taking the statistics of an in-memory view that carries
/// tombstones decodes a pending run's row ids, so a test of laziness asks a
/// view without any.
fn served_component(view: &VortexRdfStore) -> Option<&'static str> {
    view.view_statistics().unwrap().served_component
}

/// Whether `view`'s base selection is an index run whose row ids no consumer
/// has needed yet: its statistics' [`selection`](ViewStatistics::selection)
/// is [`SelectionKind::PendingIndexRun`]. The note on [`served_component`]
/// about tombstones applies.
fn selection_pending(view: &VortexRdfStore) -> bool {
    view.view_statistics().unwrap().selection == SelectionKind::PendingIndexRun
}

/// The quad-row column names `layout` builds — what a schema assertion
/// expects the primary struct to carry and nothing else.
/// The probes the matches `rec` saw bind a column: `(the store's own,
/// resolved for the call)`.
fn probe_binds(rec: &Recorder) -> (usize, usize) {
    (
        rec.count(&Event::ProbeBound { cached: true }),
        rec.count(&Event::ProbeBound { cached: false }),
    )
}

/// The `rid` rows of in-memory index components `rec` saw decoded.
fn rids_decoded(rec: &Recorder) -> usize {
    rec.picked(|e| match e {
        Event::RidsDecoded(rows) => Some(*rows),
        _ => None,
    })
    .iter()
    .sum()
}

/// The directory the last out-of-core build `rec` saw spill its runs under.
fn last_spill_dir(rec: &Recorder) -> std::path::PathBuf {
    rec.picked(|e| match e {
        Event::Spilled(dir) => Some(dir.clone()),
        _ => None,
    })
    .pop()
    .expect("the build made a run directory")
}

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
    built.components.iter().map(|c| c.identity.name).collect()
}

/// One `val` of a reference child: a term code under the Dictionary layout,
/// the term's N-Triples string under the others.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Val {
    Code(TermCode),
    Term(String),
}

/// A reference child's rows as `(val, rid)` pairs, in the child's row order:
/// what its order contract, strictly ascending pairs, is checked on.
fn reference_pairs(
    component: &crate::store::indexes::IndexComponent,
) -> Vec<(Val, crate::store::RowId)> {
    use crate::store::array::{StrColReader, field_as};
    use vortex_array::VortexSessionExecute as _;
    use vortex_array::arrays::struct_::StructArrayExt as _;
    use vortex_array::arrays::{PrimitiveArray, VarBinViewArray};

    let mut ctx = crate::session::VORTEX_SESSION.create_execution_ctx();
    let rows = component.rows().unwrap();
    let rid: PrimitiveArray = field_as(rows, "rid", &mut ctx).unwrap();
    let val = rows.unmasked_field_by_name("val").unwrap();
    let vals: Vec<Val> = if matches!(val.dtype(), vortex_array::dtype::DType::Utf8(_)) {
        let col: VarBinViewArray = field_as(rows, "val", &mut ctx).unwrap();
        let reader = StrColReader::new(&col);
        (0..col.len())
            .map(|i| Val::Term(reader.str_at(i).to_owned()))
            .collect()
    } else {
        let col: PrimitiveArray = field_as(rows, "val", &mut ctx).unwrap();
        col.as_slice::<TermCode>()
            .iter()
            .map(|&c| Val::Code(c))
            .collect()
    };
    vals.into_iter()
        .zip(rid.as_slice::<crate::store::RowId>().iter().copied())
        .collect()
}

/// Sorted subject strings of every quad a store exposes.
async fn subjects_of(store: &VortexRdfStore) -> Vec<String> {
    let mut got: Vec<String> = store
        .quads()
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
                iri_graph("g")
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

/// The write of a component holding `chunks` under `descriptor`, laid out
/// by `strategy`.
fn component_write(
    descriptor: crate::io::container::StoreComponentDescriptor,
    chunks: Vec<vortex_array::ArrayRef>,
    strategy: std::sync::Arc<dyn vortex_layout::LayoutStrategy>,
) -> crate::io::container::NativeComponentWrite {
    use crate::io::container::{BufferedComponentSource, NativeComponentWrite};
    NativeComponentWrite::new(
        descriptor,
        std::sync::Arc::new(BufferedComponentSource::try_new(chunks).unwrap()),
        strategy,
    )
    .unwrap()
}

/// The [`component_write`] of an index child named `name` under the
/// implementation slug `slug`, holding `chunks`: at version 1, optional
/// unless `required`, its sort claimed global when `sorted`, of the first
/// chunk's dtype and laid out by the default child strategy.
fn index_child_write(
    name: &str,
    slug: &str,
    required: bool,
    sorted: bool,
    chunks: Vec<vortex_array::ArrayRef>,
) -> crate::io::container::NativeComponentWrite {
    use crate::io::container::{self, StoreComponentDescriptor, StoreComponentRole};
    let dtype = chunks[0].dtype().clone();
    component_write(
        StoreComponentDescriptor {
            name: name.into(),
            role: StoreComponentRole::Index,
            implementation: slug.into(),
            version: 1,
            required,
            sorted,
            bracketed_iris: false,
            dtype,
        },
        chunks,
        container::default_child_strategy(),
    )
}

/// The [`component_write`] of `dict`'s child chunks as the dictionary
/// component, its descriptor at `version`, laid out by `strategy`.
fn dict_child_write(
    dict: &crate::store::layouts::dictionary::TermDictionary,
    version: u32,
    strategy: std::sync::Arc<dyn vortex_layout::LayoutStrategy>,
) -> crate::io::container::NativeComponentWrite {
    use crate::io::container::{self, StoreComponentDescriptor, StoreComponentRole};
    let chunks = dict.child_chunks().unwrap();
    let dtype = chunks[0].dtype().clone();
    component_write(
        StoreComponentDescriptor {
            name: container::DICT_COMPONENT_NAME.into(),
            role: StoreComponentRole::Dictionary,
            implementation: container::DICT_IMPLEMENTATION.into(),
            version,
            required: true,
            sorted: true,
            bracketed_iris: dict.bracketed_iris(),
            dtype,
        },
        chunks,
        strategy,
    )
}

/// A component of the shape a future change set could write: an optional
/// (unless `required`) index child named `name` under the implementation slug
/// `slug`, holding three bare code rows.
fn unknown_component(
    name: &str,
    slug: &str,
    required: bool,
) -> crate::io::container::NativeComponentWrite {
    index_child_write(
        name,
        slug,
        required,
        false,
        vec![bare_code_quad_array(&[1, 2, 3])],
    )
}

/// A minimal native Dictionary-layout file: a one-row zero-code quad child
/// beside `dict` written as the dictionary component.
pub(crate) async fn write_dict_only_store(
    dict: &crate::store::layouts::dictionary::TermDictionary,
) -> Vec<u8> {
    unstamped_store_bytes(
        vec![bare_code_quad_array(&[0])],
        vec![dict.to_write().unwrap()],
    )
    .await
}

/// [`write_dict_only_store`] with the dictionary component's descriptor at
/// `version`, and — when `zoned` is false — no zone maps on the term column,
/// which is what `TermDictionary::to_write` produces for chunks of uneven
/// length: a reader finds no window bounds and reads them from each leaf.
pub(crate) async fn write_dict_only_store_as(
    dict: &crate::store::layouts::dictionary::TermDictionary,
    version: u32,
    zoned: bool,
) -> Vec<u8> {
    let strategy =
        crate::io::container::dict_child_strategy(zoned.then(|| dict.uniform_window()).flatten());
    let component = dict_child_write(dict, version, strategy);
    unstamped_store_bytes(vec![bare_code_quad_array(&[0])], vec![component]).await
}

/// Sorted, unique terms whose lengths swing from a few bytes to tens of
/// thousands, so a chunk holds terms far longer than any scratch buffer sized
/// for the ones before them.
pub(crate) fn widely_varying_terms() -> Vec<String> {
    const LENGTHS: [usize; 16] = [
        10, 40, 3_000, 15, 200, 12_000, 8, 90, 600, 40_000, 12, 5, 700, 20, 9_000, 30,
    ];
    let mut state = 0x2545_f491_4f6c_dd1du64;
    LENGTHS
        .iter()
        .enumerate()
        .map(|(i, &len)| {
            let filler: String = (0..len)
                .map(|_| {
                    state = state
                        .wrapping_mul(6364136223846793005)
                        .wrapping_add(1442695040888963407);
                    char::from(b'a' + ((state >> 33) % 26) as u8)
                })
                .collect();
            format!("<http://e.org/{i:02}/{filler}>")
        })
        .collect()
}

/// A native store file opened over `bytes` held in memory (no file behind it).
pub(crate) fn open_native_bytes(
    bytes: Vec<u8>,
) -> crate::store::persist::native_file::NativeStoreFile {
    use vortex_file::OpenOptionsSessionExt as _;
    let file = crate::session::VORTEX_SESSION
        .open_options()
        .open_buffer(vortex_buffer::ByteBuffer::from(bytes))
        .unwrap();
    crate::store::persist::native_file::NativeStoreFile::try_new(file).unwrap()
}

/// `path` opened mapped, as a file-backed store opens it: the
/// [`NativeStoreFile`](crate::store::persist::native_file::NativeStoreFile) around the
/// opened file.
pub(crate) async fn open_mapped(
    path: &std::path::Path,
) -> crate::store::persist::native_file::NativeStoreFile {
    use crate::io::file::{FileAccess, open_vortex_file};
    crate::store::persist::native_file::NativeStoreFile::try_new(
        open_vortex_file(path, FileAccess::Mapped).await.unwrap(),
    )
    .unwrap()
}

/// A mapped Dictionary-layout store file of `n` quads (subject-sorted,
/// subjects unique) with `indexes`. Keep the `TempDir` alive for the file's
/// lifetime.
pub(crate) async fn mapped_file(
    n: usize,
    indexes: Indexes,
) -> (
    tempfile::TempDir,
    crate::store::persist::native_file::NativeStoreFile,
) {
    let (dir, path) =
        write_store_file(modular_quads(n, 3, 4), LayoutStrategy::Dictionary, indexes).await;
    (dir, open_mapped(&path).await)
}

/// The layout node of a native store file's dictionary-child term column —
/// the child struct's `_dict_term` field exactly as written: a `Zoned` node
/// when the column carries zone maps, its chunked (or flat) data otherwise.
pub(crate) fn dict_term_column(
    native: &crate::store::persist::native_file::NativeStoreFile,
) -> vortex_layout::LayoutRef {
    use crate::io::container::DICT_COMPONENT_NAME;
    use crate::store::layouts::dictionary::storage::COL_DICT_TERM;
    use vortex_layout::LayoutChildType;
    let child = native
        .component_layout(DICT_COMPONENT_NAME)
        .unwrap()
        .expect("the file carries a dictionary component");
    (0..child.nslots())
        .find_map(|i| {
            matches!(child.slot_type(i), Some(LayoutChildType::Field(ref n)) if n.as_ref() == COL_DICT_TERM)
                .then(|| child.slot(i).ok().flatten())
                .flatten()
        })
        .expect("the dictionary child has a term column")
}

/// A `{s, p, o, g}` struct of four identical non-nullable u64 columns
/// holding `codes` — the Dictionary layout's row shape without a
/// dictionary to give the codes meaning.
fn bare_code_quad_array(codes: &[TermCode]) -> vortex_array::ArrayRef {
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
    iri_subject(&format!("s{i:0width$}"))
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

/// Sorted [`tuple_rows`] of the quads at the indexes `keep` accepts: what the
/// sorted [`decoded_rows`] of the codes standing for those quads must equal.
fn expected_rows(quads: &[Quad], keep: impl Fn(usize) -> bool) -> Vec<Row> {
    let kept: Vec<Quad> = quads
        .iter()
        .enumerate()
        .filter(|(i, _)| keep(*i))
        .map(|(_, q)| q.clone())
        .collect();
    let mut rows = tuple_rows(&kept);
    rows.sort();
    rows
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

/// The quad `s99 p0 "object 9"` in the default graph: one to add to a store
/// of a few [`modular_quads`], whose subjects stop short of it.
fn extra_quad() -> Quad {
    make_quad(
        "http://example.org/s99",
        "http://example.org/p0",
        "object 9",
        GraphName::DefaultGraph,
    )
}

/// 3,000 quads `s{i:04} p{i % 5} "o{i % 7}"` cycling through the default
/// graph, `g1` and `g2`: the dataset of the suites that check narrowed views
/// and their exports against the gathered codes. Subjects are unique, so a
/// bound subject matches one row.
fn fixture_quads() -> Vec<Quad> {
    graph_modular_quads(
        3_000,
        4,
        5,
        7,
        &[GraphName::DefaultGraph, iri_graph("g1"), iri_graph("g2")],
    )
}

/// `n` quads `tail{i} p9 "late"` in the default graph, terms
/// [`fixture_quads`] lacks: rows to append into a tail.
fn appended_quads(n: usize) -> Vec<Quad> {
    (0..n)
        .map(|i| {
            make_quad(
                &format!("http://example.org/tail{i}"),
                "http://example.org/p9",
                "late",
                GraphName::DefaultGraph,
            )
        })
        .collect()
}

/// One way to open a store's serialized form: from its bytes, borrowed or
/// adopted, or from a file holding them, mapped or loaded whole.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Open {
    /// [`from_bytes`](VortexRdfStore::from_bytes), over the buffer.
    Bytes,
    /// [`from_bytes_owned`](VortexRdfStore::from_bytes_owned), adopting it.
    BytesOwned,
    /// [`from_file`](VortexRdfStore::from_file), the file mapped.
    Mapped,
    /// [`from_file_in_memory`](VortexRdfStore::from_file_in_memory), the file
    /// loaded whole.
    Loaded,
}

impl Open {
    /// The opens the suites check a store file under, in their order: mapped
    /// (a Dictionary layout's dictionary left in the file), loaded whole, and
    /// adopted from its bytes.
    const FILE: [Open; 3] = [Open::Mapped, Open::Loaded, Open::BytesOwned];

    /// The store file at `path`, opened this way; the byte opens read its
    /// bytes first.
    async fn open(self, path: &std::path::Path) -> crate::error::Result<VortexRdfStore> {
        match self {
            Open::Bytes => VortexRdfStore::from_bytes(&std::fs::read(path).unwrap()).await,
            Open::BytesOwned => {
                VortexRdfStore::from_bytes_owned(std::fs::read(path).unwrap()).await
            }
            Open::Mapped => VortexRdfStore::from_file(path).await,
            Open::Loaded => VortexRdfStore::from_file_in_memory(path).await,
        }
    }

    /// The open as a fixture's tag names it.
    fn tag(self) -> &'static str {
        match self {
            Open::Bytes | Open::BytesOwned => "bytes",
            Open::Mapped => "mapped",
            Open::Loaded => "loaded",
        }
    }
}

/// An open as failure messages name it: the constructor's name.
impl std::fmt::Display for Open {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Open::Bytes => "from_bytes",
            Open::BytesOwned => "from_bytes_owned",
            Open::Mapped => "from_file",
            Open::Loaded => "from_file_in_memory",
        })
    }
}

/// The store file at `path` opened each way [`Open::FILE`] lists, each
/// beside its open.
async fn reopened_stores(path: &std::path::Path) -> Vec<(Open, VortexRdfStore)> {
    let mut stores = Vec::new();
    for open in Open::FILE {
        stores.push((open, open.open(path).await.unwrap()));
    }
    stores
}

/// A store of the fixture matrix, beside how it was made: its index
/// configuration, and the open of its file, or none for a store built in
/// memory. The dir guard keeps a file alive for the store's lifetime.
struct FixtureStore {
    index: IndexConfig,
    open: Option<Open>,
    store: VortexRdfStore,
    _dir: Option<std::sync::Arc<tempfile::TempDir>>,
}

impl FixtureStore {
    /// The store's label in failure messages: `file/<index>/<open>`, or
    /// `memory/<index>` for a store built in memory.
    fn tag(&self) -> String {
        match self.open {
            Some(open) => format!("file/{}/{}", self.index.name(), open.tag()),
            None => format!("memory/{}", self.index.name()),
        }
    }

    /// Whether the store is a file opened mapped.
    fn mapped(&self) -> bool {
        self.open == Some(Open::Mapped)
    }
}

/// [`fixture_quads`] written as a Dictionary-layout file under each index
/// configuration, each file opened every way [`Open::FILE`] lists. Each file
/// is written by [`write_store_file`] once per process (see
/// [`cached_store_bytes`]), and its bytes into a fresh temp dir per call.
async fn file_stores() -> Vec<FixtureStore> {
    static BYTES: [OnceLock<Vec<u8>>; 3] = [const { OnceLock::new() }; 3];
    let mut stores = Vec::new();
    for (index, cell) in IndexConfig::ALL.into_iter().zip(&BYTES) {
        let bytes = cached_store_bytes(cell, || async move {
            let (_dir, path) =
                write_store_file(fixture_quads(), LayoutStrategy::Dictionary, index.types()).await;
            std::fs::read(&path).unwrap()
        })
        .await;
        let (dir, path) = temp_store_file(bytes);
        let dir = std::sync::Arc::new(dir);
        for (open, store) in reopened_stores(&path).await {
            let fixture = FixtureStore {
                index,
                open: Some(open),
                store,
                _dir: Some(std::sync::Arc::clone(&dir)),
            };
            assert_eq!(
                fixture.store.dict_reader().unwrap().is_file_backed(),
                fixture.mapped(),
                "{}: only the mapped open keeps its dictionary in the file",
                fixture.tag()
            );
            stores.push(fixture);
        }
    }
    stores
}

/// The views of a [`fixture_quads`] Dictionary-layout store the planner
/// suites check: the whole store, a bound subject, an index-eligible
/// predicate and object, a subject keep, a window, two chained patterns and
/// an empty window.
async fn views_of(store: &VortexRdfStore) -> Vec<(&'static str, VortexRdfStore)> {
    let s = subject_node(17, 4);
    let p1 = iri("p1");
    let o3 = plain_literal("o3");
    let dict = store.dict_reader().unwrap();
    let (lo, hi) = dict.prefix_range("<http://example.org/s1").await.unwrap();
    let predicate = matched_view(store, None, Some(&p1), None, None).await;
    let chained = matched_view(&predicate, None, None, Some(&o3), None).await;
    vec![
        ("whole", store.clone()),
        (
            "subject",
            matched_view(store, Some(&s), None, None, None).await,
        ),
        ("predicate", predicate),
        (
            "object",
            matched_view(store, None, None, Some(&o3), None).await,
        ),
        (
            "keep",
            store
                .keep(QuadColumn::S, &Keep::range(lo..hi))
                .await
                .unwrap(),
        ),
        ("window", store.window(100, 250).await.unwrap()),
        ("chained", chained),
        ("empty", store.window(10_000, 5).await.unwrap()),
    ]
}

/// The multi-chunk fixture: 20,000 [`modular_quads`] in a Dictionary-layout
/// store with no index, built in memory. Its rows are more than two 8,192-row
/// zones, so its file ([`multi_chunk_file`]) holds them in several chunks and
/// a scan of it crosses chunk boundaries.
async fn multi_chunk_store() -> VortexRdfStore {
    built_store(
        modular_quads(20_000, 3, 8),
        LayoutStrategy::Dictionary,
        vec![],
    )
    .await
}

/// [`multi_chunk_store`]'s file, serialized once per process (see
/// [`cached_store_bytes`]), written into a fresh temp dir and opened mapped.
/// Keep the `TempDir` alive for the store's lifetime.
async fn multi_chunk_file() -> (tempfile::TempDir, VortexRdfStore) {
    static BYTES: OnceLock<Vec<u8>> = OnceLock::new();
    let bytes = cached_store_bytes(&BYTES, || async {
        multi_chunk_store().await.to_bytes().await.unwrap()
    })
    .await;
    let (dir, path) = temp_store_file(bytes);
    (dir, VortexRdfStore::from_file(&path).await.unwrap())
}

/// [`fixture_quads`] built under the Dictionary layout, with the base's
/// `column` pcodec-compressed (`vortex.pco`), its sorted stamp kept: an
/// encoding no probe reads. `from_built` leaves such a column as it is, so it
/// has neither a canonical primitive nor a probe; `from_parts` decodes it.
async fn pco_coded_build(column: &str) -> BuiltArray {
    use crate::store::array::{column_is_sorted, stamp_is_sorted};
    use vortex::encodings::pco::Pco;
    use vortex_array::arrays::struct_::StructArrayExt as _;
    use vortex_array::arrays::{PrimitiveArray, StructArray};
    use vortex_array::{IntoArray as _, VortexSessionExecute as _};

    let mut built = build_array::<SortedInMemoryBuilder>(
        quad_stream(fixture_quads()),
        LayoutStrategy::Dictionary,
        vec![],
    )
    .await
    .unwrap();
    let mut ctx = crate::session::VORTEX_SESSION.create_execution_ctx();
    let base = built
        .array
        .clone()
        .execute::<StructArray>(&mut ctx)
        .unwrap();
    let names = base.names().clone();
    let children: Vec<vortex_array::ArrayRef> = names
        .iter()
        .map(|name| {
            let child = base.unmasked_field_by_name(name.as_ref()).unwrap().clone();
            if name.as_ref() != column {
                return child;
            }
            let sorted = column_is_sorted(&child);
            let codes = child.execute::<PrimitiveArray>(&mut ctx).unwrap();
            let encoded = Pco::from_primitive(codes.as_view(), 3, 0, &mut ctx)
                .unwrap()
                .into_array();
            assert!(
                vortex_rdf_encoded_search::SortedProbe::resolve(&encoded).is_none(),
                "no probe reads pcodec"
            );
            if sorted {
                stamp_is_sorted(&encoded);
            }
            encoded
        })
        .collect();
    built.array = StructArray::try_new(
        names,
        children,
        base.len(),
        vortex_array::validity::Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    built
}

/// [`pco_coded_build`] of the `o` column adopted by `from_built`: a
/// store whose `o` has neither a canonical primitive nor a probe.
async fn unprobed_object_store() -> VortexRdfStore {
    VortexRdfStore::from_built(pco_coded_build("o").await).unwrap()
}

/// The rows of `N` code columns, in order: row `i` holds code `i` of each
/// column, in the columns' order.
fn code_rows<const N: usize>(columns: &[vortex_buffer::Buffer<TermCode>]) -> Vec<[TermCode; N]> {
    (0..columns[0].len())
        .map(|i| std::array::from_fn(|c| columns[c][i]))
        .collect()
}

/// The row tuples of a view's gathered codes, in the order gathered.
async fn row_list(view: &VortexRdfStore) -> Vec<[TermCode; 4]> {
    let cols = view
        .code_columns_gathered()
        .await
        .unwrap()
        .expect("a Dictionary-layout view without a tail gathers codes");
    code_rows(&cols)
}

/// The row tuples of a view's gathered codes, sorted — order-free, since a
/// served view gathers in its index's order and a narrowed one in base
/// order.
async fn sorted_rows(view: &VortexRdfStore) -> Vec<[TermCode; 4]> {
    let mut rows = row_list(view).await;
    rows.sort_unstable();
    rows
}

/// The view's rows in base row order: a keep that admits every code drops
/// any serve plan without dropping a row.
async fn base_ordered_codes(view: &VortexRdfStore) -> Vec<[TermCode; 4]> {
    let all = Keep::range(0..TermCode::MAX);
    row_list(&view.keep(QuadColumn::S, &all).await.unwrap()).await
}

/// The rows of one [`code_chunks`](VortexRdfStore::code_chunks) chunk over
/// `N` columns, in the chunk's column order ([`code_rows`]). Asserts one
/// buffer per column, all of one length.
fn code_chunk_rows<const N: usize>(
    chunk: &[vortex_buffer::Buffer<TermCode>],
) -> Vec<[TermCode; N]> {
    assert_eq!(chunk.len(), N, "one buffer per requested column");
    let n = chunk[0].len();
    assert!(
        chunk.iter().all(|b| b.len() == n),
        "column buffers agree in length"
    );
    code_rows(chunk)
}

/// The rows of every `code_chunks` chunk a drained stream held, in order:
/// each chunk read by [`code_chunk_rows`], with its checks.
fn drained_code_rows<const N: usize>(
    chunks: &[Vec<vortex_buffer::Buffer<TermCode>>],
) -> Vec<[TermCode; N]> {
    chunks
        .iter()
        .flat_map(|chunk| code_chunk_rows::<N>(chunk))
        .collect()
}

/// Every chunk [`code_chunks`](VortexRdfStore::code_chunks) yields for
/// `view` over `columns` at `batch` rows, as rows in the requested column
/// order ([`code_chunk_rows`]), beside each chunk's row count.
async fn collect_code_chunks<const N: usize>(
    view: &VortexRdfStore,
    columns: &[QuadColumn; N],
    batch: usize,
) -> (Vec<[TermCode; N]>, Vec<usize>) {
    let mut stream = view.code_chunks(columns, batch).unwrap();
    let mut rows = Vec::new();
    let mut sizes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = code_chunk_rows::<N>(&chunk.unwrap());
        sizes.push(chunk.len());
        rows.extend(chunk);
    }
    (rows, sizes)
}

/// Every chunk [`row_chunks`](VortexRdfStore::row_chunks) yields for `view`
/// at `batch` rows. Asserts each holds at least one row and at most `batch`.
async fn collect_row_chunks(view: &VortexRdfStore, batch: usize) -> Vec<vortex_array::ArrayRef> {
    let mut stream = view.row_chunks(batch).unwrap();
    let mut chunks = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.unwrap();
        assert!(
            !chunk.is_empty() && chunk.len() <= batch,
            "chunk of {} rows under batch {batch}",
            chunk.len()
        );
        chunks.push(chunk);
    }
    chunks
}

/// The `names` columns of struct chunks — `u64` codes or row ids —
/// concatenated: one row per chunk row, its values in the order of `names`.
fn chunk_columns(chunks: &[vortex_array::ArrayRef], names: &[&str]) -> Vec<Vec<u64>> {
    use crate::store::array::{field_as, into_struct_array};
    use vortex_array::VortexSessionExecute as _;
    use vortex_array::arrays::PrimitiveArray;

    let mut rows = Vec::new();
    for chunk in chunks {
        let s = into_struct_array(chunk.clone()).unwrap();
        let mut ctx = crate::session::VORTEX_SESSION.create_execution_ctx();
        let columns: Vec<vortex_buffer::Buffer<u64>> = names
            .iter()
            .map(|name| {
                field_as::<PrimitiveArray>(&s, name, &mut ctx)
                    .unwrap()
                    .into_buffer::<u64>()
            })
            .collect();
        for i in 0..chunk.len() {
            rows.push(columns.iter().map(|column| column[i]).collect());
        }
    }
    rows
}

/// The `(s, p, o, g)` codes of Dictionary-layout struct chunks — what
/// `row_chunks` yields — concatenated: one `[s, p, o, g]` row per chunk row.
fn struct_chunk_codes(chunks: &[vortex_array::ArrayRef]) -> Vec<[TermCode; 4]> {
    chunk_columns(chunks, &crate::store::schema::PRIMARY_COLUMNS)
        .into_iter()
        .map(|r| [r[0], r[1], r[2], r[3]])
        .collect()
}

/// The strings of the `name` column of struct chunks, concatenated.
fn chunk_strings(chunks: &[vortex_array::ArrayRef], name: &str) -> Vec<String> {
    use crate::store::array::{StrColReader, field_as, into_struct_array};
    use vortex_array::VortexSessionExecute as _;
    use vortex_array::arrays::VarBinViewArray;

    let mut out = Vec::new();
    for chunk in chunks {
        let s = into_struct_array(chunk.clone()).unwrap();
        let mut ctx = crate::session::VORTEX_SESSION.create_execution_ctx();
        let col = field_as::<VarBinViewArray>(&s, name, &mut ctx).unwrap();
        let reader = StrColReader::new(&col);
        for i in 0..col.len() {
            out.push(reader.str_at(i).to_string());
        }
    }
    out
}

/// The `s` column strings of string-layout struct chunks, concatenated.
fn chunk_subjects(chunks: &[vortex_array::ArrayRef]) -> Vec<String> {
    chunk_strings(chunks, "s")
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

/// One row per term kind in every role, each object distinct: an IRI, a
/// blank node, a plain, a language-tagged and a typed literal as objects;
/// a blank-node subject; a blank-node graph beside a named one and the
/// default graph.
fn term_kind_quads() -> Vec<Quad> {
    use oxrdf::BlankNode;
    let s = iri_subject("s");
    let blank_s = NamedOrBlankNode::BlankNode(BlankNode::new("bs").unwrap());
    let p1 = iri("p1");
    let p2 = iri("p2");
    let g = iri_graph("g");
    let blank_g = GraphName::BlankNode(BlankNode::new("bg").unwrap());
    vec![
        Quad::new(
            s.clone(),
            p1.clone(),
            Term::NamedNode(iri("o-iri")),
            GraphName::DefaultGraph,
        ),
        Quad::new(
            s.clone(),
            p1.clone(),
            Term::BlankNode(BlankNode::new("bo").unwrap()),
            GraphName::DefaultGraph,
        ),
        Quad::new(s.clone(), p2.clone(), plain_literal("plain"), g.clone()),
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
            plain_literal("blank subject, blank graph"),
            blank_g.clone(),
        ),
        Quad::new(
            blank_s,
            p2,
            plain_literal("blank subject, second row"),
            blank_g,
        ),
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
    let dict = store.dict_reader().unwrap().snapshot().unwrap().0;
    for chunk in dict.term_chunks() {
        assert_eq!(
            chunk.encoding_id().to_string(),
            "vortex.fsst",
            "{when}: dictionary chunk not FSST"
        );
    }
}

/// One quad as `(s, p, o, g)` N-Triples spellings, the default graph as `""`:
/// the spelling [`SharedQuad`](crate::store::SharedQuad) rows carry.
type Row = (String, String, String, String);

/// Quads as [`Row`]s, so the two decodes compare directly.
fn tuple_rows(quads: &[Quad]) -> Vec<Row> {
    quads
        .iter()
        .map(|q| {
            let r = crate::store::RawQuad::from_quad(q);
            (r.s, r.p, r.o, r.g)
        })
        .collect()
}

fn shared_tuple_rows(rows: &[crate::store::SharedQuad]) -> Vec<Row> {
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

/// The `(s, p, o, g)` rows of the code columns `cols`, in the order of the
/// code rows: each code decoded through `dict` and spelled as [`tuple_rows`]
/// spells quads. Fails on a code the dictionary does not hold.
async fn decoded_rows(cols: &[vortex_buffer::Buffer<TermCode>; 4], dict: &DictReader) -> Vec<Row> {
    let mut terms = Vec::with_capacity(4);
    for col in cols {
        terms.push(dict.decode_many(col.as_slice()).await.unwrap());
    }
    (0..cols[0].len())
        .map(|i| {
            let term = |c: usize| {
                terms[c][i]
                    .clone()
                    .expect("every code addresses the dictionary")
            };
            (term(0), term(1), term(2), term(3))
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

/// A pattern for [`assert_patterns_agree`]: its name in failure messages, then
/// the subject, predicate, object and graph it binds.
type Pattern<'a> = (
    &'a str,
    Option<&'a NamedOrBlankNode>,
    Option<&'a NamedNode>,
    Option<&'a Term>,
    Option<&'a GraphName>,
);

/// Every store in `got`, beside its name, answers each of `patterns` as `want`
/// does: the same size and the same quads.
async fn assert_patterns_agree(
    want: &VortexRdfStore,
    got: &[(&str, &VortexRdfStore)],
    patterns: &[Pattern<'_>],
) {
    for &(name, s, p, o, g) in patterns {
        let expected = matched_view(want, s, p, o, g).await;
        let size = expected.size().await.unwrap();
        let strings = view_strings(&expected).await;
        for (kind, store) in got {
            let view = matched_view(store, s, p, o, g).await;
            assert_eq!(
                view.size().await.unwrap(),
                size,
                "size diverged on [{name}] ({kind})"
            );
            assert_eq!(
                view_strings(&view).await,
                strings,
                "results diverged on [{name}] ({kind})"
            );
        }
    }
}
