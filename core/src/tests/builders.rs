//! The builders: chunk-boundary behaviour of the streaming builder, the
//! spill-and-merge path of the streaming one, the file writer's ordering,
//! and the sorted stamp a build leaves on its subject column.

use super::*;
use crate::store::builders::{BuildSizes, sorted_stream};

/// The chunk-boundary harness: chunk_size 3 over the 10 given quads must
/// split as [3, 3, 3, 1], whatever the `layout`. Hands back the collected
/// chunks — plus the dictionary the stream carried beside them, for the
/// Dictionary layout — for the callers' layout-specific follow-up assertions.
async fn run_chunk_boundary_builders(
    layout: LayoutStrategy,
    quads: &[Quad],
) -> Vec<(
    &'static str,
    Vec<vortex_array::ArrayRef>,
    Option<std::sync::Arc<crate::store::layouts::dictionary::TermDictionary>>,
)> {
    let mut out = Vec::new();
    for (name, result) in [(
        "sorted_stream",
        sorted_stream::build_chunk_stream(
            Box::new(quad_stream(quads.to_vec())),
            layout,
            vec![],
            BuildSizes::uniform(3),
            None,
        )
        .await,
    )] {
        let built = result.unwrap_or_else(|e| panic!("{name}: {e}"));
        let dict = built.dict.clone();
        let chunks: Vec<_> = built
            .chunks
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|c| c.unwrap_or_else(|e| panic!("{name}: {e}")))
            .collect();
        let lens: Vec<usize> = chunks.iter().map(|c| c.len()).collect();
        assert_eq!(lens, [3, 3, 3, 1], "{name}: unexpected chunk sizes");
        out.push((name, chunks, dict));
    }
    out
}

#[tokio::test]
async fn test_default_streaming_chunk_boundaries() {
    // Quads fed in REVERSE subject order; the builder must emit
    // globally sorted output across chunk boundaries.
    let quads: Vec<Quad> = (0..10)
        .rev()
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{:02}", i),
                "http://example.org/p",
                "o",
                GraphName::DefaultGraph,
            )
        })
        .collect();

    for (name, chunks, _) in run_chunk_boundary_builders(LayoutStrategy::Default, &quads).await {
        // Decode all chunks in order and verify global subject sort.
        let subjects: Vec<String> = chunks
            .iter()
            .flat_map(|c| store::layouts::ResolvedLayout::Default.decode_chunk(c))
            .map(|q| q.unwrap().subject.to_string())
            .collect();
        let mut sorted = subjects.clone();
        sorted.sort();
        assert_eq!(subjects, sorted, "{name}: output not globally sorted");
        assert_eq!(subjects.len(), 10, "{name}: wrong quad count");
        if let vortex_array::dtype::DType::Struct(fields, _) = chunks[0].dtype() {
            let names: Vec<&str> = fields.names().iter().map(|n| n.as_ref()).collect();
            assert_eq!(names, ["s", "p", "o", "g"], "{name}");
        } else {
            panic!("{name}: expected struct dtype");
        }
    }
}

#[tokio::test]
async fn test_dictionary_streaming_chunk_boundaries() {
    let quads = dictionary_test_quads();

    for (name, chunks, dict) in
        run_chunk_boundary_builders(LayoutStrategy::Dictionary, &quads).await
    {
        // Reassemble and decode through a store: the chunks hold bare
        // codes, and the dictionary the stream carried beside them is
        // handed back with the reassembled array — all chunks' codes must
        // reference that same global dictionary.
        let dtype = chunks[0].dtype().clone();
        let arr = crate::store::array::chunked_or_single(chunks, dtype).unwrap();
        let store = VortexRdfStore::from_built(crate::store::builders::BuiltArray {
            array: arr,
            components: Vec::new(),
            dict,
        })
        .unwrap();
        assert_eq!(store.layout(), LayoutStrategy::Dictionary, "{name}");
        let decoded: Vec<Quad> = store.quads().try_collect().await.unwrap();
        assert_eq!(
            quad_strings(&decoded),
            quad_strings(&quads),
            "{name}: bad roundtrip"
        );
    }
}

/// With a chunk size far below the dataset the streaming ingest spills
/// several runs, so the quad merge and every index family's `(value, row id)`
/// sort take the spill-and-K-way-merge branch: the chunks come out `[3; 8]`,
/// every quad survives in global subject order, and the merged index columns
/// still route P, O and PO matches to their exact selectivities — under the
/// string layout and the code layout (whose index children carry codes
/// against the merged dictionary) alike.
#[tokio::test]
async fn test_sorted_streaming_spilled_indexes_match_in_memory() {
    let indexes = vec![IndexType::SecondaryByCopy, IndexType::SecondaryByReference];
    // Fed in reverse so nothing is accidentally in order, and wide enough
    // (24 quads over a chunk size of 3) to force 8 runs.
    let mut quads = modular_quads(24, 3, 4);
    quads.reverse();

    for layout in [LayoutStrategy::Default, LayoutStrategy::Dictionary] {
        // The quad chunk stream is primary-only (index families stream as
        // native components beside it); chunk sizes still follow the merge
        // windows.
        let built = sorted_stream::build_chunk_stream(
            Box::new(quad_stream(quads.clone())),
            layout,
            indexes.clone(),
            BuildSizes::uniform(3),
            None,
        )
        .await
        .expect("spilled build");
        assert_eq!(
            built.components.len(),
            4,
            "{layout:?}: posg/ospg/ref-o/ref-p components"
        );
        let chunks: Vec<_> = built
            .chunks
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .map(|c| c.expect("chunk"))
            .collect();
        assert_eq!(
            chunks.iter().map(|c| c.len()).collect::<Vec<_>>(),
            [3, 3, 3, 3, 3, 3, 3, 3],
            "{layout:?}: unexpected chunk sizes"
        );

        // The materializing path re-glues the streamed components into the
        // in-memory build, index routing included.
        let built = sorted_stream::build_array(
            Box::new(quad_stream(quads.clone())),
            layout,
            indexes.clone(),
            BuildSizes::uniform(3),
        )
        .await
        .expect("spilled array build");
        let store = VortexRdfStore::from_built(built).unwrap();
        assert_eq!(store.layout(), layout);
        assert_eq!(store.indexes(), &indexes[..]);

        // Every quad survived the spill round-trip, in global subject order.
        let decoded: Vec<Quad> = store.quads().try_collect().await.unwrap();
        let mut expected = quad_strings(&quads);
        expected.sort();
        assert_eq!(quad_strings(&decoded), expected, "{layout:?}");

        // And the index columns the spill merge emitted actually route: the
        // same selectivities the single-run copy-index test asserts.
        let p1 = NamedNode::new("http://example.org/p1").unwrap();
        let o1 = Term::Literal(Literal::new_simple_literal("object 1"));
        for (tag, pp, po, want) in [
            ("P", Some(&p1), None, 8),
            ("O", None, Some(&o1), 6),
            ("PO", Some(&p1), Some(&o1), 2),
        ] {
            assert_eq!(
                matched_size(&store, None, pp, po, None).await,
                want,
                "{layout:?}: {tag}"
            );
        }
    }
}

/// An empty input through the streaming builder yields an empty store that
/// still declares the requested index set, under every layout.
#[tokio::test]
async fn test_sorted_stream_empty_dataset() {
    for layout in LAYOUTS {
        for indexes in [
            vec![],
            vec![IndexType::SecondaryByCopy, IndexType::SecondaryByReference],
        ] {
            let built = sorted_stream::build_array(
                Box::new(quad_stream(vec![])),
                layout,
                indexes.clone(),
                BuildSizes::uniform(3),
            )
            .await
            .unwrap_or_else(|e| panic!("{layout:?}/{indexes:?}: {e}"));
            assert_eq!(built.array.len(), 0, "{layout:?}/{indexes:?}");
            assert_eq!(
                built.components.len(),
                2 * indexes.len(),
                "{layout:?}/{indexes:?}: two components per index kind"
            );
            let store = VortexRdfStore::from_built(built).unwrap();
            assert_eq!(store.layout(), layout);
            assert_eq!(store.indexes(), indexes.as_slice());
            assert_eq!(store.size().await.unwrap(), 0, "{layout:?}/{indexes:?}");
            let p = NamedNode::new("http://example.org/p0").unwrap();
            assert_eq!(
                matched_size(&store, None, Some(&p), None, None).await,
                0,
                "{layout:?}/{indexes:?}"
            );
        }
    }
}

/// The writer's output read back through `from_bytes`: quads fed in reverse
/// subject order come out sorted, every row and per-row object intact.
#[tokio::test]
async fn test_writer_sorts_reversed_input() {
    let quads: Vec<Quad> = (0..25)
        .rev()
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{:02}", i),
                "http://example.org/p",
                &format!("object value {}", i),
                GraphName::DefaultGraph,
            )
        })
        .collect();

    let mut buffer = Vec::new();
    quads_stream_to_vortex_writer(
        quad_stream(quads),
        &mut buffer,
        LayoutStrategy::Default,
        vec![],
    )
    .await
    .unwrap();

    let store = VortexRdfStore::from_bytes(&buffer).await.unwrap();
    assert_eq!(store.size().await.unwrap(), 25);

    let decoded: Vec<Quad> = store.quads().try_collect().await.unwrap();
    assert_eq!(decoded[0].subject.to_string(), "<http://example.org/s00>");
    assert_eq!(decoded[0].object.to_string(), "\"object value 0\"");
    assert_eq!(decoded[24].subject.to_string(), "<http://example.org/s24>");
    assert_eq!(decoded[24].object.to_string(), "\"object value 24\"");
}

/// Reverse-subject-order quads, so only a genuinely sorting builder may end
/// up with a sorted `s` column.
fn reverse_order_quads() -> Vec<Quad> {
    let mut quads = modular_quads(10, 1, 1);
    quads.reverse();
    quads
}

/// Both sorted builders stamp the `s` column sorted, and the stamp is the
/// binary-search fast path's sole license. The streaming builder stamps it
/// after materializing several chunks into one array.
#[tokio::test]
async fn test_sorted_builders_stamp_is_sorted() {
    let quads = reverse_order_quads();

    let sorted = build_array::<SortedInMemoryBuilder>(
        quad_stream(quads.clone()),
        LayoutStrategy::Default,
        vec![],
    )
    .await
    .unwrap();
    assert!(
        crate::store::array::subject_sorted(&sorted.array),
        "sorted_in_memory: no IsSorted stamp on s"
    );

    let streamed = sorted_stream::build_array(
        Box::new(quad_stream(quads)),
        LayoutStrategy::Default,
        vec![],
        BuildSizes::uniform(3),
    )
    .await
    .unwrap();
    assert!(
        crate::store::array::subject_sorted(&streamed.array),
        "sorted_stream: no IsSorted stamp on s"
    );
}

/// Every field of an index child as strings, by field name: terms as their
/// N-Triples spelling, codes and row ids as numbers.
fn child_table(component: &crate::store::indexes::IndexComponent) -> Vec<(String, Vec<String>)> {
    use crate::store::array::{StrColReader, field_as};
    use vortex_array::VortexSessionExecute as _;
    use vortex_array::arrays::struct_::StructArrayExt as _;
    use vortex_array::arrays::{PrimitiveArray, VarBinViewArray};

    let mut ctx = crate::session::VORTEX_SESSION.create_execution_ctx();
    let rows = component.rows().unwrap();
    rows.names()
        .iter()
        .map(|name| {
            let field = rows.unmasked_field_by_name(name.as_ref()).unwrap();
            let values: Vec<String> =
                if matches!(field.dtype(), vortex_array::dtype::DType::Utf8(_)) {
                    let col: VarBinViewArray = field_as(rows, name.as_ref(), &mut ctx).unwrap();
                    let reader = StrColReader::new(&col);
                    (0..col.len())
                        .map(|i| reader.str_at(i).unwrap().to_owned())
                        .collect()
                } else {
                    let col: PrimitiveArray = field_as(rows, name.as_ref(), &mut ctx).unwrap();
                    col.as_slice::<u64>().iter().map(u64::to_string).collect()
                };
            (name.to_string(), values)
        })
        .collect()
}

/// The out-of-core build, its runs spilled, emits the very children and rows
/// the in-memory build does, under every layout and index set: the same
/// children in the same order, every field equal row by row, and the same
/// quads in the same order. The quads cycle through the default graph and two
/// named ones, so every key position of a copy child is exercised.
#[tokio::test]
async fn test_spilled_children_equal_in_memory_children() {
    let graph = |name: &str| {
        GraphName::NamedNode(NamedNode::new(format!("http://example.org/{name}")).unwrap())
    };
    let graphs = [GraphName::DefaultGraph, graph("g1"), graph("g2")];
    let mut quads = graph_modular_quads(24, 2, 3, 4, &graphs);
    quads.reverse();
    quads.extend(quads[..6].to_vec());
    for layout in LAYOUTS {
        for indexes in index_sets() {
            let who = format!("{layout:?} / {indexes:?}");
            let spilled = sorted_stream::build_array(
                Box::new(quad_stream(quads.clone())),
                layout,
                indexes.clone(),
                BuildSizes::uniform(3),
            )
            .await
            .unwrap();
            let in_memory = build_array::<SortedInMemoryBuilder>(
                quad_stream(quads.clone()),
                layout,
                indexes.clone(),
            )
            .await
            .unwrap();
            assert_eq!(
                component_names(&spilled),
                component_names(&in_memory),
                "{who}"
            );
            for (a, b) in spilled.components.iter().zip(&in_memory.components) {
                assert_eq!(child_table(a), child_table(b), "{who}: {}", a.identity.name);
            }
            let rows = |built: BuiltArray| async move {
                let store = VortexRdfStore::from_built(built).unwrap();
                let quads = store.quads_vec().await.unwrap();
                quads.iter().map(ToString::to_string).collect::<Vec<_>>()
            };
            let (spilled_rows, in_memory_rows) = (rows(spilled).await, rows(in_memory).await);
            assert_eq!(spilled_rows.len(), 24, "{who}: the repeats are gone");
            assert_eq!(spilled_rows, in_memory_rows, "{who}");
        }
    }
}

/// A string layout's spilled build stores every spelling it was given, byte
/// for byte: escapes, a language tag, a datatype, blank nodes, a named graph
/// and the default graph. The rows are read as the stored strings, with no
/// parse that could hide another spelling of the same term, and in order.
#[tokio::test]
async fn test_spilled_string_layouts_decode_terms_verbatim() {
    use crate::common::terms::parse_quads_from_reader;
    use crate::store::layouts::ResolvedLayout;
    use oxrdfio::RdfFormat;

    let text = concat!(
        "<http://e.org/s> <http://e.org/p> \"a\\\"b\\\\c\" .\n",
        "<http://e.org/s> <http://e.org/p> \"hello\"@en <http://e.org/g> .\n",
        "_:b1 <http://e.org/p> \"5\"^^<http://www.w3.org/2001/XMLSchema#integer> .\n",
        "<http://e.org/s> <http://e.org/q> _:b2 <http://e.org/g> .\n",
        "_:b1 <http://e.org/q> <http://e.org/o> .\n",
        "<http://e.org/t> <http://e.org/p> \"x\\u00e9\\n\" .\n",
    );
    let raws: Vec<crate::store::RawQuad> =
        parse_quads_from_reader(std::io::Cursor::new(text.as_bytes()), RdfFormat::NQuads)
            .try_collect()
            .await
            .unwrap();
    assert_eq!(raws.len(), 6);
    let row = |q: &crate::store::RawQuad| (q.s.clone(), q.p.clone(), q.o.clone(), q.g.clone());
    let mut sorted = raws.clone();
    sorted.sort();
    let expected: Vec<_> = sorted.iter().map(row).collect();
    for (layout, resolved) in [
        (LayoutStrategy::Default, ResolvedLayout::Default),
        (LayoutStrategy::TypedObject, ResolvedLayout::TypedObject),
    ] {
        let built = sorted_stream::build_array(
            Box::new(stream::iter(raws.clone().into_iter().map(Ok))),
            layout,
            vec![IndexType::SecondaryByCopy, IndexType::SecondaryByReference],
            BuildSizes::uniform(2),
        )
        .await
        .unwrap();
        let got: Vec<_> = resolved
            .raw_quads(&built.array)
            .unwrap()
            .iter()
            .map(row)
            .collect();
        assert_eq!(got, expected, "{layout:?}");
    }
}

/// A run spills only past its capacity: exactly that many quads stay one
/// in-memory run and write nothing, one more writes two quad runs; both
/// builds emit every quad.
#[tokio::test]
async fn test_run_capacity_boundary_spills_only_past_it() {
    use crate::store::builders::spill::spill_hooks;

    for layout in LAYOUTS {
        for (n, files) in [(4usize, 0usize), (5, 2)] {
            let quads = modular_quads(n, 3, 4);
            let built = sorted_stream::build_chunk_stream(
                Box::new(quad_stream(quads)),
                layout,
                vec![],
                BuildSizes::uniform(4),
                None,
            )
            .await
            .unwrap();
            let dir = spill_hooks::last_dir().expect("the build made a run directory");
            let mut names: Vec<String> = std::fs::read_dir(&dir)
                .unwrap()
                .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
                .collect();
            names.sort();
            assert_eq!(names.len(), files, "{layout:?}, {n} quads: {names:?}");
            assert!(
                names.iter().all(|name| name.starts_with("quads_run_")),
                "{layout:?}, {n} quads: {names:?}"
            );
            let chunks: Vec<_> = built.chunks.try_collect().await.unwrap();
            assert_eq!(
                chunks.iter().map(|c| c.len()).sum::<usize>(),
                n,
                "{layout:?}"
            );
        }
    }
}

/// An input that fails after runs were spilled fails the build with its
/// error, and the run directory is gone.
#[tokio::test]
async fn test_input_error_after_spilling_removes_the_runs() {
    use crate::store::builders::spill::spill_hooks;

    for layout in LAYOUTS {
        let raws: Vec<_> = modular_quads(10, 3, 4)
            .iter()
            .map(crate::store::RawQuad::from_quad)
            .map(Ok)
            .chain(std::iter::once(Err(VortexRdfError::InvalidInput(
                "boom".to_owned(),
            ))))
            .collect();
        let result = sorted_stream::build_chunk_stream(
            Box::new(stream::iter(raws)),
            layout,
            vec![IndexType::SecondaryByCopy, IndexType::SecondaryByReference],
            BuildSizes::uniform(3),
            None,
        )
        .await;
        assert!(
            matches!(&result, Err(VortexRdfError::InvalidInput(m)) if m == "boom"),
            "{layout:?}"
        );
        let dir = spill_hooks::last_dir().expect("the build made a run directory");
        assert!(!dir.exists(), "{layout:?}: {} is left", dir.display());
    }
}

/// The peak spill of an out-of-core build — its run directory once the merge
/// that numbers the rows has run, before any chunk is pulled — is the quads as
/// term ids, 16 bytes each, and the requested families' compact entries (an
/// 8-byte header per run, 8 bytes per reference entry, 20 per copy entry),
/// with no merged copy of the quads, under every layout.
#[tokio::test]
async fn test_peak_spill_is_term_ids_and_compact_entries() {
    use crate::store::builders::spill::spill_hooks;

    for layout in LAYOUTS {
        for indexes in index_sets() {
            let who = format!("{layout:?} / {indexes:?}");
            // 10 distinct quads in runs of 3: 4 runs of each kind (3, 3, 3, 1).
            let built = sorted_stream::build_chunk_stream(
                Box::new(quad_stream(modular_quads(10, 3, 4))),
                layout,
                indexes.clone(),
                BuildSizes::uniform(3),
                None,
            )
            .await
            .unwrap();
            let dir = spill_hooks::last_dir().expect("the build made a run directory");
            let files: Vec<(String, u64)> = std::fs::read_dir(&dir)
                .unwrap()
                .map(|e| {
                    let e = e.unwrap();
                    let name = e.file_name().to_string_lossy().into_owned();
                    (name, e.metadata().unwrap().len())
                })
                .collect();
            assert!(
                !files.iter().any(|(name, _)| name == "merged.bin"),
                "{who}: {files:?}"
            );
            let families = |index| if indexes.contains(&index) { 2 } else { 0 };
            let want = 16 * 10
                + families(IndexType::SecondaryByReference) * (4 * 8 + 10 * 8)
                + families(IndexType::SecondaryByCopy) * (4 * 8 + 10 * 20);
            let total: u64 = files.iter().map(|(_, len)| len).sum();
            assert_eq!(total, want, "{who}: {files:?}");
            let chunks: Vec<_> = built.chunks.try_collect().await.unwrap();
            assert_eq!(chunks.iter().map(|c| c.len()).sum::<usize>(), 10, "{who}");
        }
    }
}
