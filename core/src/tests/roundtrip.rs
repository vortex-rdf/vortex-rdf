//! Adoption refusals, decode errors, base rebuilds, and the textual export
//! paths (raw N-Triples/N-Quads fast path, structured serializer) checked
//! against oxrdfio.

use super::*;

// ─── Adoption, decode errors and rebuilds ──────────────────────────────

/// A bare Dictionary-layout array cannot self-describe, and `from_parts`
/// says so.
#[test]
fn test_from_parts_rejects_bare_dictionary_array() {
    let err = VortexRdfStore::from_parts(crate::store::BuiltArray {
        array: bare_code_quad_array(&[1, 2, 3]),
        components: Vec::new(),
        dict: None,
    })
    .err()
    .expect("from_parts should fail");
    assert!(
        err.to_string().contains("from_built"),
        "unexpected error: {err}"
    );
}

/// Rows for the decode-error cases: `modular_quads(3, 3, 4)` with row 1's
/// object replaced by a string in none of the N-Triples term forms.
fn raws_with_bad_object() -> Vec<crate::store::RawQuad> {
    let mut raws: Vec<crate::store::RawQuad> = modular_quads(3, 3, 4)
        .iter()
        .map(crate::store::RawQuad::from_quad)
        .collect();
    raws[1].o = "bogus".to_string();
    raws
}

/// `chunk` with one column replaced by `column`, everything else as it was.
fn with_column(
    chunk: &vortex_array::ArrayRef,
    name: &str,
    column: vortex_array::ArrayRef,
) -> vortex_array::ArrayRef {
    use vortex_array::arrays::struct_::{StructArray, StructArrayExt};
    use vortex_array::{IntoArray as _, VortexSessionExecute as _};

    let mut ctx = crate::session::VORTEX_SESSION.create_execution_ctx();
    let struct_arr = chunk.clone().execute::<StructArray>(&mut ctx).unwrap();
    let fields: Vec<vortex_array::ArrayRef> = struct_arr
        .names()
        .iter()
        .map(|field| {
            if field.as_ref() == name {
                column.clone()
            } else {
                struct_arr
                    .unmasked_field_by_name(field.as_ref())
                    .unwrap()
                    .clone()
            }
        })
        .collect();
    StructArray::try_new(
        struct_arr.names().clone(),
        fields,
        struct_arr.len(),
        vortex_array::validity::Validity::NonNullable,
    )
    .unwrap()
    .into_array()
}

/// `layout`'s chunk over `raws`, paired with the resolved layout that
/// decodes it.
fn chunk_for(
    layout: LayoutStrategy,
    raws: &[crate::store::RawQuad],
) -> (
    vortex_array::ArrayRef,
    crate::store::layouts::ResolvedLayout,
) {
    use crate::store::builders::build_struct_array;
    use crate::store::layouts::dictionary::{self, TermDictionary};
    use crate::store::layouts::{DictAccess, ResolvedLayout};

    match layout {
        LayoutStrategy::Default => (
            build_struct_array(raws, layout, false).unwrap(),
            ResolvedLayout::Default,
        ),
        LayoutStrategy::TypedObject => (
            build_struct_array(raws, layout, false).unwrap(),
            ResolvedLayout::TypedObject,
        ),
        LayoutStrategy::Dictionary => {
            let (dict, code_map) = TermDictionary::from_quads_with_map(raws).unwrap();
            let codes = dictionary::encode_quads(raws, &code_map).unwrap();
            (
                dictionary::build_code_chunk(&codes, 0..raws.len(), false).unwrap(),
                ResolvedLayout::Dictionary(DictAccess::Resident(std::sync::Arc::new(dict))),
            )
        }
    }
}

/// `chunk` without its `g` column (the last column of every layout).
fn drop_graph_column(chunk: &vortex_array::ArrayRef) -> vortex_array::ArrayRef {
    use vortex_array::arrays::struct_::{StructArray, StructArrayExt};
    use vortex_array::{IntoArray as _, VortexSessionExecute as _};

    let mut ctx = crate::session::VORTEX_SESSION.create_execution_ctx();
    let struct_arr = chunk.clone().execute::<StructArray>(&mut ctx).unwrap();
    let names: Vec<_> = struct_arr
        .names()
        .iter()
        .take(struct_arr.names().len() - 1)
        .cloned()
        .collect();
    let fields: Vec<vortex_array::ArrayRef> = names
        .iter()
        .map(|name| {
            struct_arr
                .unmasked_field_by_name(name.as_ref())
                .unwrap()
                .clone()
        })
        .collect();
    StructArray::try_new(
        names.into(),
        fields,
        struct_arr.len(),
        vortex_array::validity::Validity::NonNullable,
    )
    .unwrap()
    .into_array()
}

/// `bytes`, a store file, with the first leaf of `column` damaged in `table`:
/// the quad table (`None`), or the component of that name. The trailing
/// flatbuffer length of the leaf's segment is set past the segment, so the
/// file still opens and a read of that leaf fails to decode it.
fn with_damaged_leaf(bytes: &[u8], table: Option<&str>, column: &str) -> Vec<u8> {
    use crate::io::container;
    use vortex_file::OpenOptionsSessionExt as _;
    use vortex_layout::LayoutChildType;

    let file = crate::session::VORTEX_SESSION
        .open_options()
        .open_buffer(vortex_buffer::ByteBuffer::from(bytes.to_vec()))
        .unwrap();
    let root = file
        .footer()
        .layout()
        .as_::<container::RdfStoreLayoutVTable>();
    let table = match table {
        None => container::quad_source(root).unwrap(),
        Some(name) => {
            let slot = root
                .data()
                .components
                .iter()
                .position(|component| component.name == name)
                .unwrap_or_else(|| panic!("no component {name}"));
            container::component_child(root, slot).unwrap()
        }
    };
    let field = (0..table.nslots())
        .find(|&i| {
            matches!(table.slot_type(i), Some(LayoutChildType::Field(ref name)) if name.as_ref() == column)
        })
        .unwrap();
    let leaves =
        vortex_rdf_encoded_search::flat_leaves(&table.slot(field).unwrap().unwrap()).unwrap();
    let segment = &file.footer().segment_map()[*leaves[0].layout.segment_id() as usize];
    let end = usize::try_from(segment.offset).unwrap() + segment.length as usize;
    let mut damaged = bytes.to_vec();
    damaged[end - 4..end].copy_from_slice(&u32::MAX.to_le_bytes());
    damaged
}

/// A chunk that cannot be read as the layout's columns at all decodes to a
/// single `Err`, whatever its row count.
#[test]
fn test_decode_chunk_reports_chunk_failure_as_single_err() {
    let raws: Vec<crate::store::RawQuad> = modular_quads(3, 3, 4)
        .iter()
        .map(crate::store::RawQuad::from_quad)
        .collect();
    for layout in LAYOUTS {
        let (chunk, resolved) = chunk_for(layout, &raws);
        let decoded = resolved.decode_chunk(&drop_graph_column(&chunk));
        assert_eq!(decoded.len(), 1, "{layout:?}: one error for the chunk");
        assert!(decoded[0].is_err(), "{layout:?}");
    }
}

/// A row whose term fails to parse is reported at its own position, with the
/// rows around it decoded: an object string in no term form under the
/// layouts that store the object spelling, an unknown `o_kind` under the
/// typed layout. The typed layout stores no object spelling, so it meets
/// that object string at build instead: `from_quads` refuses it with the
/// parser's error. A store holding the typed layout's bad row fails its
/// shared read and its N-Quads export with that row's error, and a store
/// file with a damaged leaf fails every read of that chunk, under each
/// layout, as a damaged dictionary window or copy-index child does the reads
/// that decode through it.
#[tokio::test]
async fn test_decode_chunk_reports_bad_row_at_its_position() {
    use vortex_array::IntoArray as _;
    use vortex_array::arrays::PrimitiveArray;

    let good: Vec<crate::store::RawQuad> = modular_quads(3, 3, 4)
        .iter()
        .map(crate::store::RawQuad::from_quad)
        .collect();
    let bad = raws_with_bad_object();
    let built = VortexRdfStore::from_quads(
        futures::stream::iter(bad.clone().into_iter().map(Ok)),
        LayoutStrategy::TypedObject,
        vec![],
    )
    .await;
    match built.err() {
        Some(VortexRdfError::Deserialization(m))
            if m == "invalid term \"bogus\": not an IRI, blank node or literal" => {}
        other => panic!("{other:?}"),
    }
    let typed = {
        let (chunk, resolved) = chunk_for(LayoutStrategy::TypedObject, &good);
        let kinds = PrimitiveArray::from_iter([2u8, 9, 2]).into_array();
        (with_column(&chunk, "o_kind", kinds), resolved)
    };
    let typed_store = VortexRdfStore::from_built(crate::store::BuiltArray {
        array: typed.0.clone(),
        components: Vec::new(),
        dict: None,
    })
    .unwrap();
    let cases = [
        (
            LayoutStrategy::Default,
            chunk_for(LayoutStrategy::Default, &bad),
        ),
        (LayoutStrategy::TypedObject, typed),
        (
            LayoutStrategy::Dictionary,
            chunk_for(LayoutStrategy::Dictionary, &bad),
        ),
    ];

    for (layout, (chunk, resolved)) in cases {
        let decoded = resolved.decode_chunk(&chunk);
        assert_eq!(decoded.len(), 3, "{layout:?}: one result per row");
        assert!(decoded[0].is_ok(), "{layout:?}: row 0");
        assert!(
            decoded[1].is_err(),
            "{layout:?}: row 1 carries the bad object"
        );
        assert!(decoded[2].is_ok(), "{layout:?}: row 2");
        assert_eq!(
            decoded[2].as_ref().unwrap().subject.to_string(),
            "<http://example.org/s02>",
            "{layout:?}"
        );
    }

    // The typed layout's bad row in a store: its shared read and its N-Quads
    // export recompose the object from the typed columns, and fail with the
    // row's error rather than panic.
    let unknown_kind = |error: &VortexRdfError| match error {
        VortexRdfError::Deserialization(message) => message == "Unknown object kind: 9",
        _ => false,
    };
    let error = typed_store
        .shared_quads_vec()
        .await
        .expect_err("the shared read fails");
    assert!(unknown_kind(&error), "shared read: {error}");
    let error = crate::export_rdf(typed_store, Vec::new(), oxrdfio::RdfFormat::NQuads)
        .await
        .expect_err("the N-Quads export fails");
    assert!(unknown_kind(&error), "N-Quads export: {error}");

    // A store file whose quad table has a damaged leaf opens, and a read of
    // that chunk reports the failed decode as an error, never a panic: the
    // row chunks' scan and the quads at the chunk's position, the shared
    // quads and the N-Quads export as the call's error, under each layout.
    for layout in LAYOUTS {
        let (_dir, path) = write_store_file(modular_quads(12, 3, 4), layout, vec![]).await;
        std::fs::write(
            &path,
            with_damaged_leaf(&std::fs::read(&path).unwrap(), None, "p"),
        )
        .unwrap();
        let store = VortexRdfStore::from_file(&path).await.unwrap();
        let chunks: Vec<_> = store.row_chunks(64).unwrap().collect().await;
        assert!(
            matches!(chunks.first(), Some(Err(_))),
            "{layout:?}: row chunks"
        );
        let quads: Vec<_> = store.quads().collect().await;
        assert!(matches!(quads.first(), Some(Err(_))), "{layout:?}: quads");
        assert!(
            store.shared_quads_vec().await.is_err(),
            "{layout:?}: shared quads"
        );
        assert!(
            crate::export_rdf(store, Vec::new(), oxrdfio::RdfFormat::NQuads)
                .await
                .is_err(),
            "{layout:?}: N-Quads export"
        );
    }

    // A damaged leaf of the file's other tables. A dictionary window, which
    // a mapped store reads only to decode a chunk's codes, fails the reads
    // of the rows it decodes. A copy-index child's `o` leaf fails the served
    // read of a point-sized run, which the match located through the child's
    // `p` and `rid` columns.
    {
        use crate::io::container::DICT_COMPONENT_NAME;
        use crate::store::layouts::dictionary::storage::COL_DICT_TERM;

        let damaged = |path: &std::path::Path, table: &str, column: &str| {
            let bytes = with_damaged_leaf(&std::fs::read(path).unwrap(), Some(table), column);
            std::fs::write(path, bytes).unwrap();
        };
        let (_dir, path) =
            write_store_file(modular_quads(12, 3, 4), LayoutStrategy::Dictionary, vec![]).await;
        damaged(&path, DICT_COMPONENT_NAME, COL_DICT_TERM);
        let store = VortexRdfStore::from_file(&path).await.unwrap();
        assert!(store.dict_reader().unwrap().is_file_backed());
        let quads: Vec<_> = store.quads().collect().await;
        assert!(matches!(quads.first(), Some(Err(_))), "dictionary: quads");
        assert!(
            store.shared_quads_vec().await.is_err(),
            "dictionary: shared quads"
        );

        let (_dir, path) = write_store_file(
            modular_quads(12, 3, 4),
            LayoutStrategy::Dictionary,
            vec![IndexType::SecondaryByCopy],
        )
        .await;
        damaged(&path, "index:posg", "o");
        let store = VortexRdfStore::from_file(&path).await.unwrap();
        let p1 = iri("p1");
        let served = matched_view(&store, None, Some(&p1), None, None).await;
        assert_eq!(served_component(&served), Some("index:posg"));
        assert_eq!(served.debug_serve_row_range(), Some(4..8));
        let quads: Vec<_> = served.quads().collect().await;
        assert!(matches!(quads.first(), Some(Err(_))), "copy child: quads");
        assert!(
            served.shared_quads_vec().await.is_err(),
            "copy child: shared quads"
        );
    }
}

/// A rebuild over a base without the sorted stamp sorts every row: the
/// re-emitted rows are in `(s, p, o, g)` order, the base and tail rows
/// interleaved, and the adopted store carries the stamp.
#[tokio::test]
async fn test_unstamped_base_rebuild_sorts_fully() {
    let quads = modular_quads(12, 3, 4);
    let reversed: Vec<Quad> = quads.iter().rev().cloned().collect();
    let base = unstamped_store(&reversed);
    assert_eq!(base.view_statistics().unwrap().sort_order, None);

    let appended = vec![
        make_quad(
            "http://example.org/s05a",
            "http://example.org/p9",
            "object 9",
            GraphName::DefaultGraph,
        ),
        make_quad(
            "http://example.org/s00a",
            "http://example.org/p9",
            "object 9",
            GraphName::DefaultGraph,
        ),
    ];
    let tailed = base.add_quads(appended.clone()).await.unwrap();
    let rebuilt =
        VortexRdfStore::from_parts(tailed.to_serializable_parts().await.unwrap()).unwrap();
    assert_eq!(
        rebuilt.view_statistics().unwrap().sort_order,
        Some(SortOrder::Spog),
        "a rebuild over an unstamped base must sort and stamp its rows"
    );

    let got = rebuilt.quads_vec().await.unwrap();
    let mut union: Vec<crate::store::RawQuad> = quads
        .iter()
        .chain(appended.iter())
        .map(crate::store::RawQuad::from_quad)
        .collect();
    union.sort();
    let union: Vec<Row> = union.into_iter().map(|r| (r.s, r.p, r.o, r.g)).collect();
    assert_eq!(
        tuple_rows(&got),
        union,
        "rows must come back in (s, p, o, g) order"
    );
}

// ─── Textual export ────────────────────────────────────────────────────

/// Quads whose terms exercise everything the N-Triples escape and suffix
/// grammar has: quotes, backslashes, newlines, a language tag, a datatype,
/// a blank node, and a named graph.
fn export_test_quads() -> Vec<Quad> {
    vec![
        Quad::new(
            NamedOrBlankNode::BlankNode(oxrdf::BlankNode::new("b0").unwrap()),
            iri("p"),
            Term::Literal(
                Literal::new_language_tagged_literal("héllo \"quoted\"\nline\\slash", "en")
                    .unwrap(),
            ),
            GraphName::DefaultGraph,
        ),
        Quad::new(
            iri_subject("s"),
            iri("p"),
            Term::Literal(Literal::new_typed_literal(
                "42",
                NamedNode::new("http://www.w3.org/2001/XMLSchema#integer").unwrap(),
            )),
            iri_graph("g"),
        ),
    ]
}

/// What oxrdfio's own serializer produces for `quads` — the reference the
/// raw fast path must match byte for byte.
fn oxrdf_reference(quads: &[Quad], format: oxrdfio::RdfFormat) -> String {
    let mut reference = Vec::new();
    let mut ser = oxrdfio::RdfSerializer::from_format(format).for_writer(&mut reference);
    for quad in quads {
        ser.serialize_quad(quad).unwrap();
    }
    ser.finish().unwrap();
    String::from_utf8(reference).unwrap()
}

/// Pins the N-Quads export fast path on `store`, which holds
/// [`export_test_quads`]: as it stands, and again with its second quad
/// tombstoned and a quad appended to the tail, where the export must skip the
/// deleted row and include the tail row. Each view must read the same rows
/// through `quads()` and `quads_vec()` and export the bytes oxrdfio's
/// serializer gives for them. The reference follows the store's own row order,
/// so what this pins is the byte-level escaping rather than the build's
/// ordering. A failure names the store's layout and the view.
async fn assert_export_matches_oxrdf(store: VortexRdfStore) {
    let layout = store.layout();
    let quads = export_test_quads();
    let appended = make_quad(
        "http://example.org/s9",
        "http://example.org/p",
        "appended",
        GraphName::DefaultGraph,
    );
    let edited = store
        .delete_quad(&quads[1])
        .await
        .unwrap()
        .add_quad(appended)
        .await
        .unwrap();
    for (cell, store) in [("as written", store), ("tombstoned and tailed", edited)] {
        // Both hold as many quads as were written: the edit drops one, adds one.
        let stored = store.quads_vec().await.unwrap();
        assert_eq!(stored.len(), quads.len(), "{layout:?} {cell}");
        let streamed: Vec<Quad> = store.quads().try_collect().await.unwrap();
        assert_eq!(streamed, stored, "{layout:?} {cell}");
        let mut exported = Vec::new();
        crate::export_rdf(store, &mut exported, oxrdfio::RdfFormat::NQuads)
            .await
            .unwrap();
        assert_eq!(
            String::from_utf8(exported).unwrap(),
            oxrdf_reference(&stored, oxrdfio::RdfFormat::NQuads),
            "{layout:?} {cell}"
        );
    }
}

/// The N-Quads export fast path writes the store's raw term strings
/// verbatim; this pins it byte-for-byte against oxrdfio's serializer over
/// terms that exercise escaping, tags, datatypes, blank nodes, and a named
/// graph, under every layout's raw chunk decoding — and, with a tombstoned
/// base row and an append tail, skipping the deleted row and including the
/// tail row.
#[tokio::test]
async fn test_export_nquads_fast_path_matches_oxrdf() {
    for layout in LAYOUTS {
        let store = built_store(export_test_quads(), layout, vec![]).await;
        assert_export_matches_oxrdf(store).await;
    }
}

/// The N-Triples fast path: byte-for-byte on a default-graph store, and the
/// same named-graph refusal oxrdfio's serializer gives.
#[tokio::test]
async fn test_export_ntriples_fast_path_and_named_graph_refusal() {
    let quads = export_test_quads();
    let default_graph_only: Vec<Quad> = quads
        .iter()
        .filter(|q| q.graph_name == GraphName::DefaultGraph)
        .cloned()
        .collect();
    let store = built_store(default_graph_only.clone(), LayoutStrategy::Default, vec![]).await;
    let stored: Vec<Quad> = store.quads().try_collect().await.unwrap();
    let mut exported = Vec::new();
    crate::export_rdf(store, &mut exported, oxrdfio::RdfFormat::NTriples)
        .await
        .unwrap();
    assert_eq!(
        String::from_utf8(exported).unwrap(),
        oxrdf_reference(&stored, oxrdfio::RdfFormat::NTriples)
    );

    // A named graph cannot be represented in a graph format: the fast path
    // (N-Triples) and the structured serializer (Turtle) both refuse it as a
    // Serialization error, not a writer failure.
    let store = built_store(quads, LayoutStrategy::Default, vec![]).await;
    for format in [oxrdfio::RdfFormat::NTriples, oxrdfio::RdfFormat::Turtle] {
        let result = crate::export_rdf(store.clone(), Vec::new(), format).await;
        assert!(
            matches!(result, Err(VortexRdfError::Serialization(_))),
            "{format:?}: {result:?}"
        );
    }
}

/// The fast path's file arm: the raw chunk stream over a scan must produce
/// the same bytes as the in-memory arm under every layout — the file-backed
/// dictionary's codes resolved through the async read path included — with
/// the tombstone and the tail held in memory beside the file.
#[tokio::test]
async fn test_export_nquads_fast_path_file_backed() {
    for layout in LAYOUTS {
        let (_dir, path) = write_store_file(export_test_quads(), layout, vec![]).await;
        let store = VortexRdfStore::from_file(&path).await.unwrap();
        assert_export_matches_oxrdf(store).await;
    }
}

/// The full textual round trip through both export paths: N-Quads (the raw
/// fast path) and TriG (the structured serializer, which owns the format's
/// syntax state) must re-parse to exactly the quad set the store was built
/// from — escaped literals, language tag, datatype, and the named graph
/// included. Blank nodes are deliberately absent: parsers may relabel them,
/// which would turn a set comparison into a graph-isomorphism check.
#[tokio::test]
async fn test_export_reparses_to_the_same_quads() {
    let g = iri_graph("g");
    let quads = vec![
        make_quad(
            "http://example.org/s1",
            "http://example.org/p",
            "with \"quotes\"\nand back\\slash",
            GraphName::DefaultGraph,
        ),
        Quad::new(
            iri_subject("s2"),
            iri("p"),
            Term::Literal(Literal::new_language_tagged_literal("say \"hi\"@home", "en").unwrap()),
            g.clone(),
        ),
        Quad::new(
            iri_subject("s3"),
            iri("p"),
            Term::Literal(Literal::new_typed_literal("tab\there", iri("dt"))),
            g,
        ),
    ];

    for format in [oxrdfio::RdfFormat::NQuads, oxrdfio::RdfFormat::TriG] {
        let store = built_store(quads.clone(), LayoutStrategy::Default, vec![]).await;
        let mut exported = Vec::new();
        crate::export_rdf(store, &mut exported, format)
            .await
            .unwrap();

        let reparsed: Vec<Quad> = oxrdfio::RdfParser::from_format(format)
            .for_reader(exported.as_slice())
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap_or_else(|e| panic!("{format:?}: exported document failed to parse: {e}"));
        assert_eq!(
            quad_strings(&reparsed),
            quad_strings(&quads),
            "{format:?}: export did not round-trip"
        );
    }
}

/// A writer that takes `budget` bytes and fails every write after them, as
/// a full volume does, with the error [`FailingWriter::MESSAGE`].
struct FailingWriter {
    budget: usize,
}

impl FailingWriter {
    const MESSAGE: &'static str = "no storage space";
}

impl std::io::Write for FailingWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if self.budget == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::StorageFull,
                Self::MESSAGE,
            ));
        }
        let taken = buf.len().min(self.budget);
        self.budget -= taken;
        Ok(taken)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Both export paths, the structured serializer (TriG) and the N-Quads fast
/// path, report a writer that fails as the writer's own `Io` error, with its
/// kind: wherever the failure falls, from the first byte to the closing of the
/// last graph, with no panic and no partial success.
#[tokio::test]
async fn test_export_reports_a_failing_writer_as_its_io_error() {
    let g = iri_graph("g");
    let quads = graph_modular_quads(4, 2, 2, 3, &[GraphName::DefaultGraph, g]);
    let store = built_store(quads, LayoutStrategy::Default, vec![]).await;
    for format in [oxrdfio::RdfFormat::TriG, oxrdfio::RdfFormat::NQuads] {
        let mut whole = Vec::new();
        crate::export_rdf(store.clone(), &mut whole, format)
            .await
            .unwrap();
        if format == oxrdfio::RdfFormat::TriG {
            assert!(
                whole.ends_with(b"}\n"),
                "the last graph is closed at the end"
            );
        }

        for budget in 0..whole.len() {
            let result = crate::export_rdf(store.clone(), FailingWriter { budget }, format).await;
            assert!(
                matches!(&result, Err(VortexRdfError::Io(e))
                    if e.kind() == std::io::ErrorKind::StorageFull
                        && e.to_string().contains(FailingWriter::MESSAGE)),
                "{format:?}: a writer failing after {budget} of {} bytes: {result:?}",
                whole.len()
            );
        }
        crate::export_rdf(
            store.clone(),
            FailingWriter {
                budget: whole.len(),
            },
            format,
        )
        .await
        .unwrap();
    }
}
