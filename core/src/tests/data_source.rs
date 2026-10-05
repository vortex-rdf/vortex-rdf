//! The vortex `DataSource` over store views and components: scans agree
//! with the gathered codes on every backend, and a scan request's
//! projection, filter, row window, selection and limit apply exactly.

use super::*;
use crate::store::{Keep, QuadColumn};
use crate::tests::chunks::{base_ordered_codes, chunk_codes, chunk_subjects};
use vortex_array::ArrayRef;
use vortex_array::dtype::{DType, Nullability};
use vortex_array::expr::stats::Precision;
use vortex_array::expr::{eq, get_item, lit, pack, root, select};
use vortex_buffer::Buffer;
use vortex_scan::selection::Selection;
use vortex_scan::strict_sorted_buffer::StrictSortedBuffer;
use vortex_scan::{DataSourceRef, ScanRequest};

fn fixture_quads() -> Vec<Quad> {
    let g = |name: &str| {
        GraphName::NamedNode(NamedNode::new(format!("http://example.org/{name}")).unwrap())
    };
    graph_modular_quads(3_000, 4, 5, 7, &[GraphName::DefaultGraph, g("g1"), g("g2")])
}

/// Every chunk of every partition of one scan.
async fn scan_chunks(source: &DataSourceRef, request: ScanRequest) -> Vec<ArrayRef> {
    let scan = source.scan(request).await.unwrap();
    let mut partitions = scan.partitions();
    let mut chunks = Vec::new();
    while let Some(partition) = partitions.next().await {
        let mut stream = partition.unwrap().execute().unwrap();
        while let Some(chunk) = stream.next().await {
            chunks.push(chunk.unwrap());
        }
    }
    chunks
}

async fn scan_codes(source: &DataSourceRef, request: ScanRequest) -> Vec<[u32; 4]> {
    chunk_codes(&scan_chunks(source, request).await)
}

fn exact(p: Precision<u64>) -> Option<u64> {
    match p {
        Precision::Exact(v) => Some(v),
        _ => None,
    }
}

/// The source of `view` scans to its base-ordered codes and advertises
/// their count exactly.
async fn assert_source_matches(view: &VortexRdfStore, tag: &str) {
    let expected = base_ordered_codes(view).await;
    let source = view.data_source().await.unwrap();
    assert!(
        matches!(source.dtype(), DType::Struct(_, Nullability::NonNullable)),
        "{tag}: a non-nullable struct dtype"
    );
    assert_eq!(
        exact(source.row_count()),
        Some(expected.len() as u64),
        "{tag}: exact row count"
    );
    assert_eq!(
        scan_codes(&source, ScanRequest::default()).await,
        expected,
        "{tag}: default scan"
    );
}

async fn views_of(store: &VortexRdfStore) -> Vec<(&'static str, VortexRdfStore)> {
    let s = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s0017").unwrap());
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
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
            "keep",
            store
                .keep(QuadColumn::S, &Keep::range(lo..hi))
                .await
                .unwrap(),
        ),
        ("window", store.window(100, 250).await.unwrap()),
        ("empty", store.window(10_000, 5).await.unwrap()),
    ]
}

#[tokio::test]
async fn test_data_source_in_memory_matches_gathered() {
    for indexes in [vec![], vec![IndexType::SecondaryByCopy]] {
        let store = VortexRdfStore::from_quads(
            quad_stream(fixture_quads()),
            LayoutStrategy::Dictionary,
            indexes.clone(),
        )
        .await
        .unwrap();
        for (name, view) in views_of(&store).await {
            assert_source_matches(&view, &format!("memory {indexes:?} {name}")).await;
        }
        let quads = fixture_quads();
        let mut deleted = store.clone();
        for quad in quads.iter().step_by(11).take(30) {
            deleted = deleted.delete_quad(quad).await.unwrap();
        }
        assert_source_matches(&deleted, &format!("memory {indexes:?} tombstoned")).await;
    }
}

/// Projection, filter, row window, selection and limit — each applied
/// exactly, in the order filter, limit, projection.
#[tokio::test]
async fn test_data_source_request_semantics() {
    let store = VortexRdfStore::from_quads(
        quad_stream(fixture_quads()),
        LayoutStrategy::Dictionary,
        vec![],
    )
    .await
    .unwrap();
    let all = base_ordered_codes(&store).await;
    let source = store.data_source().await.unwrap();
    let p1 = store
        .dict_reader()
        .unwrap()
        .encode("<http://example.org/p1>")
        .await
        .unwrap()
        .unwrap();

    // A column subset, and a renaming pack over computed items.
    let chunks = scan_chunks(
        &source,
        ScanRequest {
            projection: select(["o", "s"], root()),
            ..Default::default()
        },
    )
    .await;
    let DType::Struct(fields, _) = chunks[0].dtype() else {
        panic!("struct chunks")
    };
    assert_eq!(
        fields
            .names()
            .iter()
            .map(|n| n.to_string())
            .collect::<Vec<_>>(),
        ["o", "s"]
    );
    let chunks = scan_chunks(
        &source,
        ScanRequest {
            projection: pack(
                [
                    ("s", get_item("s", root())),
                    ("p", get_item("g", root())),
                    ("o", get_item("o", root())),
                    ("g", get_item("p", root())),
                ],
                Nullability::NonNullable,
            ),
            ..Default::default()
        },
    )
    .await;
    let swapped: Vec<[u32; 4]> = all.iter().map(|r| [r[0], r[3], r[2], r[1]]).collect();
    assert_eq!(chunk_codes(&chunks), swapped, "a pack renames and reorders");

    // A filter on a code column.
    let filtered = scan_codes(
        &source,
        ScanRequest {
            filter: Some(eq(get_item("p", root()), lit(p1))),
            ..Default::default()
        },
    )
    .await;
    let want: Vec<[u32; 4]> = all.iter().copied().filter(|r| r[1] == p1).collect();
    assert!(!want.is_empty());
    assert_eq!(filtered, want, "an equality filter");

    // A limit, alone and after a filter.
    let limited = scan_codes(
        &source,
        ScanRequest {
            limit: Some(17),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(limited, all[..17].to_vec());
    let limited = scan_codes(
        &source,
        ScanRequest {
            filter: Some(eq(get_item("p", root()), lit(p1))),
            limit: Some(5),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(limited, want[..5].to_vec(), "a limit counts filtered rows");
    assert!(
        scan_codes(
            &source,
            ScanRequest {
                limit: Some(0),
                ..Default::default()
            }
        )
        .await
        .is_empty()
    );

    // A row window and an id selection over the output rows.
    let windowed = scan_codes(
        &source,
        ScanRequest {
            row_range: Some(100..2_950),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(windowed, all[100..2_950].to_vec(), "a row range");
    let ids: Vec<u64> = (0..all.len() as u64).step_by(13).collect();
    let selected = scan_codes(
        &source,
        ScanRequest {
            selection: Selection::IncludeByIndex(
                StrictSortedBuffer::try_new(Buffer::from_iter(ids.iter().copied())).unwrap(),
            ),
            ..Default::default()
        },
    )
    .await;
    let want: Vec<[u32; 4]> = ids.iter().map(|&i| all[i as usize]).collect();
    assert_eq!(selected, want, "an id selection");
    let both = scan_codes(
        &source,
        ScanRequest {
            row_range: Some(50..1_000),
            selection: Selection::IncludeByIndex(
                StrictSortedBuffer::try_new(Buffer::from_iter(ids.iter().copied())).unwrap(),
            ),
            limit: Some(10),
            ..Default::default()
        },
    )
    .await;
    let want: Vec<[u32; 4]> = ids
        .iter()
        .filter(|&&i| (50..1_000).contains(&i))
        .take(10)
        .map(|&i| all[i as usize])
        .collect();
    assert_eq!(both, want, "a range, a selection and a limit together");

    // The one partition can be left out by the partition window.
    let none = scan_chunks(
        &source,
        ScanRequest {
            partition_range: Some(1..2),
            ..Default::default()
        },
    )
    .await;
    assert!(none.is_empty());

    // Field statistics: no nulls, and the code columns' uncompressed size.
    let stats = source
        .field_statistics(&vortex_array::dtype::FieldPath::from_name("s"))
        .await
        .unwrap();
    assert_eq!(
        stats.get_as::<u64>(
            vortex_array::expr::stats::Stat::NullCount,
            &DType::Primitive(vortex_array::dtype::PType::U64, Nullability::NonNullable)
        ),
        Precision::exact(0u64)
    );
    assert_eq!(
        stats.get_as::<u64>(
            vortex_array::expr::stats::Stat::UncompressedSizeInBytes,
            &DType::Primitive(vortex_array::dtype::PType::U64, Nullability::NonNullable)
        ),
        Precision::exact(all.len() as u64 * 4)
    );
}

#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_data_source_file_backed() {
    for indexes in [vec![], vec![IndexType::SecondaryByCopy]] {
        let (_dir, path) =
            write_store_file(fixture_quads(), LayoutStrategy::Dictionary, indexes.clone()).await;
        let resident = VortexRdfStore::from_file_with_dict_residency(&path, u64::MAX)
            .await
            .unwrap();
        let in_file = VortexRdfStore::from_file_with_dict_residency(&path, 0)
            .await
            .unwrap();
        let adopted = VortexRdfStore::from_bytes_owned(std::fs::read(&path).unwrap())
            .await
            .unwrap();
        for (backend, store) in [
            ("file", resident),
            ("file/dict-in-file", in_file),
            ("bytes", adopted),
        ] {
            for (name, view) in views_of(&store).await {
                let tag = format!("{backend} {indexes:?} {name}");
                let expected = base_ordered_codes(&view).await;
                let source = view.data_source().await.unwrap();
                assert_eq!(
                    scan_codes(&source, ScanRequest::default()).await,
                    expected,
                    "{tag}"
                );
                match source.row_count() {
                    Precision::Exact(n) => assert_eq!(n, expected.len() as u64, "{tag}"),
                    // A pushed-down filter still to run: an upper bound.
                    Precision::Inexact(n) => assert!(n >= expected.len() as u64, "{tag}"),
                    Precision::Absent => panic!("{tag}: a row count hint"),
                }
            }
        }
    }
    // An unindexed file store's predicate match keeps its filter pending,
    // so the count is a bound and the scan applies the filter.
    let (_dir, path) = write_store_file(fixture_quads(), LayoutStrategy::Dictionary, vec![]).await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let view = store
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    let source = view.data_source().await.unwrap();
    assert!(
        matches!(source.row_count(), Precision::Inexact(_)),
        "pending filter: inexact"
    );
    assert_eq!(
        scan_codes(&source, ScanRequest::default()).await,
        base_ordered_codes(&view).await
    );
}

/// Index children and the dictionary as plain tables, in memory and on file.
#[tokio::test]
async fn test_component_data_sources() {
    let store = VortexRdfStore::from_quads(
        quad_stream(fixture_quads()),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy, IndexType::SecondaryByReference],
    )
    .await
    .unwrap();
    assert!(store.component_data_source("nope").unwrap().is_none());
    assert_posg_component(&store, "memory").await;
    #[cfg(feature = "file-io")]
    {
        assert_dictionary_component(&store, "memory").await;
        let (_dir, path) = write_store_file(
            fixture_quads(),
            LayoutStrategy::Dictionary,
            vec![IndexType::SecondaryByCopy, IndexType::SecondaryByReference],
        )
        .await;
        let file = VortexRdfStore::from_file(&path).await.unwrap();
        assert!(file.component_data_source("nope").unwrap().is_none());
        assert_posg_component(&file, "file").await;
        assert_dictionary_component(&file, "file").await;
        let ref_p = file.component_data_source("index:ref-p").unwrap().unwrap();
        assert_eq!(exact(ref_p.row_count()), Some(3_000));
    }
}

/// `index:posg` scans to `{s, p, o, g, rid}` rows sorted by `(p, o, s, g)`,
/// whose `rid`s address the base rows.
async fn assert_posg_component(store: &VortexRdfStore, tag: &str) {
    let base = base_ordered_codes(store).await;
    let posg = store.component_data_source("index:posg").unwrap().unwrap();
    assert_eq!(
        exact(posg.row_count()),
        Some(base.len() as u64),
        "{tag}: posg rows"
    );
    let chunks = scan_chunks(&posg, ScanRequest::default()).await;
    let DType::Struct(fields, _) = chunks[0].dtype() else {
        panic!("struct chunks")
    };
    assert_eq!(
        fields
            .names()
            .iter()
            .map(|n| n.to_string())
            .collect::<Vec<_>>(),
        ["s", "p", "o", "g", "rid"],
        "{tag}"
    );
    let rows = chunk_codes(&chunks);
    let keys: Vec<[u32; 4]> = rows.iter().map(|r| [r[1], r[2], r[0], r[3]]).collect();
    assert!(
        keys.windows(2).all(|w| w[0] <= w[1]),
        "{tag}: sorted by (p, o, s, g)"
    );
    let mut sorted = base.clone();
    sorted.sort_unstable();
    let mut got = rows.clone();
    got.sort_unstable();
    assert_eq!(got, sorted, "{tag}: the same rows as the base");
}

/// `dictionary` scans to `{_dict_term}` with one sorted row per term.
#[cfg(feature = "file-io")]
async fn assert_dictionary_component(store: &VortexRdfStore, tag: &str) {
    use vortex_array::VortexSessionExecute as _;
    let dict = store.component_data_source("dictionary").unwrap().unwrap();
    let len = store.dict_reader().unwrap().len() as u64;
    assert_eq!(
        exact(dict.row_count()),
        Some(len),
        "{tag}: one row per term"
    );
    let chunks = scan_chunks(&dict, ScanRequest::default()).await;
    let DType::Struct(fields, _) = chunks[0].dtype() else {
        panic!("struct chunks")
    };
    assert_eq!(
        fields
            .names()
            .iter()
            .map(|n| n.to_string())
            .collect::<Vec<_>>(),
        ["_dict_term"],
        "{tag}"
    );
    let terms: Vec<String> = {
        let mut out = Vec::new();
        for chunk in &chunks {
            let s = crate::store::array::into_struct_array(chunk.clone()).unwrap();
            let mut ctx = crate::session::VORTEX_SESSION.create_execution_ctx();
            let col = crate::store::array::field_as::<vortex_array::arrays::VarBinViewArray>(
                &s,
                "_dict_term",
                &mut ctx,
            )
            .unwrap();
            let reader = crate::store::array::StrColReader::new(&col);
            for i in 0..col.len() {
                out.push(reader.str_at(i).unwrap().to_string());
            }
        }
        out
    };
    assert_eq!(terms.len() as u64, len, "{tag}");
    assert!(
        terms.windows(2).all(|w| w[0].as_bytes() < w[1].as_bytes()),
        "{tag}: strictly sorted terms"
    );
    assert_eq!(
        store
            .dict_reader()
            .unwrap()
            .decode(7)
            .await
            .unwrap()
            .as_deref(),
        Some(terms[7].as_str()),
        "{tag}: row i = code i"
    );
}

/// A string layout's source carries utf8 columns, the tail included.
#[tokio::test]
async fn test_data_source_string_layouts() {
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
            .unwrap()
            .add_quads(appended.clone())
            .await
            .unwrap();
        let expected: Vec<String> = store
            .quads_vec()
            .await
            .unwrap()
            .iter()
            .map(|q| q.subject.to_string())
            .collect();
        let source = store.data_source().await.unwrap();
        assert_eq!(
            exact(source.row_count()),
            Some(expected.len() as u64),
            "{layout:?}"
        );
        let chunks = scan_chunks(&source, ScanRequest::default()).await;
        assert_eq!(chunk_subjects(&chunks), expected, "{layout:?}");
    }
    let dictionary = VortexRdfStore::from_quads(
        quad_stream(quads.clone()),
        LayoutStrategy::Dictionary,
        vec![],
    )
    .await
    .unwrap()
    .add_quads(appended)
    .await
    .unwrap();
    assert!(
        dictionary.data_source().await.is_err(),
        "a tailed Dictionary view has no source"
    );
}
