//! Chunked exports — `row_chunks` and `code_chunks` — against the whole-view
//! reads, on every backend, over narrowed, served, tombstoned and tailed
//! views.

use super::*;
use crate::store::array::{StrColReader, field_as, into_struct_array};
use crate::store::{Keep, QuadColumn};
use vortex_array::VortexSessionExecute as _;
use vortex_array::arrays::{PrimitiveArray, VarBinViewArray};

fn fixture_quads() -> Vec<Quad> {
    let g = |name: &str| {
        GraphName::NamedNode(NamedNode::new(format!("http://example.org/{name}")).unwrap())
    };
    graph_modular_quads(3_000, 4, 5, 7, &[GraphName::DefaultGraph, g("g1"), g("g2")])
}

/// The view's codes in base row order: a keep admitting every code drops
/// any serve plan without dropping a row.
pub(super) async fn base_ordered_codes(view: &VortexRdfStore) -> Vec<[u32; 4]> {
    let all = Keep::range(0..u32::MAX);
    let cols = view
        .keep(QuadColumn::S, &all)
        .await
        .unwrap()
        .code_columns_gathered()
        .await
        .unwrap()
        .expect("a Dictionary-layout view without a tail gathers codes");
    (0..cols[0].len())
        .map(|i| [cols[0][i], cols[1][i], cols[2][i], cols[3][i]])
        .collect()
}

/// The `(s, p, o, g)` codes of struct chunks, concatenated.
pub(super) fn chunk_codes(chunks: &[vortex_array::ArrayRef]) -> Vec<[u32; 4]> {
    let mut rows = Vec::new();
    for chunk in chunks {
        let s = into_struct_array(chunk.clone()).unwrap();
        let mut ctx = crate::session::VORTEX_SESSION.create_execution_ctx();
        let col = |name: &str, ctx: &mut vortex_array::ExecutionCtx| {
            field_as::<PrimitiveArray>(&s, name, ctx)
                .unwrap()
                .into_buffer::<u32>()
        };
        let (cs, cp, co, cg) = (
            col("s", &mut ctx),
            col("p", &mut ctx),
            col("o", &mut ctx),
            col("g", &mut ctx),
        );
        for i in 0..cs.len() {
            rows.push([cs[i], cp[i], co[i], cg[i]]);
        }
    }
    rows
}

/// The `s` column strings of struct chunks, concatenated.
pub(super) fn chunk_subjects(chunks: &[vortex_array::ArrayRef]) -> Vec<String> {
    let mut out = Vec::new();
    for chunk in chunks {
        let s = into_struct_array(chunk.clone()).unwrap();
        let mut ctx = crate::session::VORTEX_SESSION.create_execution_ctx();
        let col = field_as::<VarBinViewArray>(&s, "s", &mut ctx).unwrap();
        let reader = StrColReader::new(&col);
        for i in 0..col.len() {
            out.push(reader.str_at(i).unwrap().to_string());
        }
    }
    out
}

async fn collect_code_chunks(
    view: &VortexRdfStore,
    columns: &[QuadColumn],
    batch: usize,
) -> (Vec<Vec<u32>>, Vec<usize>) {
    let mut stream = view.code_chunks(columns, batch).unwrap();
    let mut rows = Vec::new();
    let mut sizes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.unwrap();
        assert_eq!(
            chunk.len(),
            columns.len(),
            "one buffer per requested column"
        );
        let n = chunk[0].len();
        assert!(
            chunk.iter().all(|b| b.len() == n),
            "column buffers agree in length"
        );
        sizes.push(n);
        for i in 0..n {
            rows.push(chunk.iter().map(|b| b[i]).collect());
        }
    }
    (rows, sizes)
}

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

/// Both exports of `view` agree with its base-ordered codes at several
/// batch sizes, and a column subset comes in the requested order.
async fn assert_chunks_match(view: &VortexRdfStore, tag: &str) {
    let expected = base_ordered_codes(view).await;
    for batch in [1, 7, 256, 100_000] {
        let (rows, sizes) = collect_code_chunks(view, &QuadColumn::ALL, batch).await;
        let rows: Vec<[u32; 4]> = rows.iter().map(|r| [r[0], r[1], r[2], r[3]]).collect();
        assert_eq!(rows, expected, "{tag}: code_chunks at batch {batch}");
        assert!(
            sizes.iter().all(|&n| n > 0 && n <= batch),
            "{tag}: chunk sizes {sizes:?} under batch {batch}"
        );
        let chunks = collect_row_chunks(view, batch).await;
        assert_eq!(
            chunk_codes(&chunks),
            expected,
            "{tag}: row_chunks at batch {batch}"
        );
    }
    let (rows, _) = collect_code_chunks(view, &[QuadColumn::O, QuadColumn::S], 1_000).await;
    let want: Vec<Vec<u32>> = expected.iter().map(|r| vec![r[2], r[0]]).collect();
    assert_eq!(rows, want, "{tag}: a column subset in the requested order");
}

/// The views every backend is exercised over: the whole store, a subject
/// prefix search, an index-eligible predicate match, a keep, a window and
/// a chain of two patterns.
async fn views_of(store: &VortexRdfStore) -> Vec<(&'static str, VortexRdfStore)> {
    let s = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s0017").unwrap());
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let o3 = Term::Literal(Literal::new_simple_literal("o3"));
    let dict = store.dict_reader().unwrap();
    let (lo, hi) = dict.prefix_range("<http://example.org/s1").await.unwrap();
    vec![
        ("whole", store.clone()),
        (
            "subject",
            store
                .match_pattern(Some(&s), None, None, None)
                .await
                .unwrap(),
        ),
        (
            "predicate",
            store
                .match_pattern(None, Some(&p1), None, None)
                .await
                .unwrap(),
        ),
        (
            "object",
            store
                .match_pattern(None, None, Some(&o3), None)
                .await
                .unwrap(),
        ),
        (
            "keep",
            store
                .keep(QuadColumn::S, &Keep::range(lo..hi))
                .await
                .unwrap(),
        ),
        ("window", store.window(100, 250).await.unwrap()),
        (
            "chained",
            store
                .match_pattern(None, Some(&p1), None, None)
                .await
                .unwrap()
                .match_pattern(None, None, Some(&o3), None)
                .await
                .unwrap(),
        ),
        ("empty", store.window(10_000, 5).await.unwrap()),
    ]
}

#[tokio::test]
async fn test_chunks_in_memory_match_gathered() {
    for indexes in [
        vec![],
        vec![IndexType::SecondaryByCopy],
        vec![IndexType::SecondaryByReference],
    ] {
        let store = VortexRdfStore::from_quads(
            quad_stream(fixture_quads()),
            LayoutStrategy::Dictionary,
            indexes.clone(),
        )
        .await
        .unwrap();
        for (name, view) in views_of(&store).await {
            assert_chunks_match(&view, &format!("memory {indexes:?} {name}")).await;
        }
    }
}

#[tokio::test]
async fn test_chunks_skip_tombstones() {
    let quads = fixture_quads();
    let store = VortexRdfStore::from_quads(
        quad_stream(quads.clone()),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await
    .unwrap();
    let mut deleted = store.clone();
    for quad in quads.iter().step_by(7).take(50) {
        deleted = deleted.delete_quad(quad).await.unwrap();
    }
    assert_eq!(deleted.size().await.unwrap(), quads.len() - 50);
    for (name, view) in views_of(&deleted).await {
        assert_chunks_match(&view, &format!("tombstoned {name}")).await;
    }
}

#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_chunks_file_backed_match_gathered() {
    for indexes in [vec![], vec![IndexType::SecondaryByCopy]] {
        let (_dir, path) =
            write_store_file(fixture_quads(), LayoutStrategy::Dictionary, indexes.clone()).await;
        let resident = VortexRdfStore::from_file_with_dict_residency(&path, u64::MAX)
            .await
            .unwrap();
        let file_backed = VortexRdfStore::from_file_with_dict_residency(&path, 0)
            .await
            .unwrap();
        let adopted = VortexRdfStore::from_bytes_owned(std::fs::read(&path).unwrap())
            .await
            .unwrap();
        for (backend, store) in [
            ("file", resident),
            ("file/dict-in-file", file_backed),
            ("bytes", adopted),
        ] {
            for (name, view) in views_of(&store).await {
                assert_chunks_match(&view, &format!("{backend} {indexes:?} {name}")).await;
            }
        }
    }
}

/// The string layouts stream their native struct rows, the tail last, in the
/// order `quads()` decodes them.
#[tokio::test]
async fn test_chunks_string_layouts_include_tail() {
    let quads = fixture_quads();
    let appended: Vec<Quad> = (0..40)
        .map(|i| {
            make_quad(
                &format!("http://example.org/tail{i}"),
                "http://example.org/p9",
                "late",
                GraphName::DefaultGraph,
            )
        })
        .collect();
    for layout in [LayoutStrategy::Default, LayoutStrategy::TypedObject] {
        let store = VortexRdfStore::from_quads(quad_stream(quads.clone()), layout, vec![])
            .await
            .unwrap();
        let tailed = store.add_quads(appended.clone()).await.unwrap();
        assert_eq!(tailed.tail_len(), 40);
        let expected: Vec<String> = tailed
            .quads_vec()
            .await
            .unwrap()
            .iter()
            .map(|q| q.subject.to_string())
            .collect();
        for batch in [64, 100_000] {
            let chunks = collect_row_chunks(&tailed, batch).await;
            assert_eq!(
                chunk_subjects(&chunks),
                expected,
                "{layout:?} at batch {batch}"
            );
        }
        assert!(
            tailed.code_chunks(&QuadColumn::ALL, 10).is_err(),
            "no codes under {layout:?}"
        );
    }
    #[cfg(feature = "file-io")]
    {
        let (_dir, path) = write_store_file(quads.clone(), LayoutStrategy::Default, vec![]).await;
        let tailed = VortexRdfStore::from_file(&path)
            .await
            .unwrap()
            .add_quads(appended.clone())
            .await
            .unwrap();
        let expected: Vec<String> = tailed
            .quads_vec()
            .await
            .unwrap()
            .iter()
            .map(|q| q.subject.to_string())
            .collect();
        let chunks = collect_row_chunks(&tailed, 500).await;
        assert_eq!(
            chunk_subjects(&chunks),
            expected,
            "file-backed Default with a tail"
        );
    }
}

/// The gates: a zero batch, no columns, and a Dictionary view whose tail
/// holds terms without codes.
#[tokio::test]
async fn test_chunks_gates() {
    let store = VortexRdfStore::from_quads(
        quad_stream(fixture_quads()),
        LayoutStrategy::Dictionary,
        vec![],
    )
    .await
    .unwrap();
    assert!(store.row_chunks(0).is_err());
    assert!(store.code_chunks(&QuadColumn::ALL, 0).is_err());
    assert!(store.code_chunks(&[], 10).is_err());
    let appended: Vec<Quad> = (0..3)
        .map(|i| {
            make_quad(
                &format!("http://example.org/tail{i}"),
                "http://example.org/p9",
                "late",
                GraphName::DefaultGraph,
            )
        })
        .collect();
    let tailed = store.add_quads(appended).await.unwrap();
    assert_eq!(tailed.tail_len(), 3);
    assert!(
        tailed.row_chunks(10).is_err(),
        "a tailed Dictionary view has no single dtype"
    );
    assert!(tailed.code_chunks(&QuadColumn::ALL, 10).is_err());
}
