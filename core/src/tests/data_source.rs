//! The vortex `DataSource` over store views and component tables: a view's
//! source scans to its base-ordered codes on every backend and advertises its
//! statistics' count, a scan request's projection, filter, row window,
//! selection, limit and partition window apply exactly, and a store's
//! children read as plain tables.

use super::*;
use crate::store::{SortOrder, TermCode};
use crate::vortex_scan::selection::Selection;
use crate::vortex_scan::strict_sorted_buffer::StrictSortedBuffer;
use crate::vortex_scan::{DataSourceRef, ScanRequest};
use vortex_array::dtype::{DType, FieldPath, Nullability, PType};
use vortex_array::expr::stats::{Precision, Stat};
use vortex_array::expr::{Expression, eq, get_item, in_list, lit, pack, root, select};
use vortex_array::scalar::Scalar;
use vortex_buffer::Buffer;

/// The field names of a struct chunk.
fn field_names(chunk: &vortex_array::ArrayRef) -> Vec<String> {
    let DType::Struct(fields, _) = chunk.dtype() else {
        panic!("struct chunks")
    };
    fields.names().iter().map(|n| n.to_string()).collect()
}

/// A scan request with only `filter` set.
fn filtered(filter: Expression) -> ScanRequest {
    ScanRequest {
        filter: Some(filter),
        ..Default::default()
    }
}

/// What one scan of `source` advertises before it reads: its partition
/// count, and each partition's row count.
async fn partition_counts(
    source: &DataSourceRef,
    request: ScanRequest,
) -> (Precision<usize>, Vec<Precision<u64>>) {
    let scan = source.scan(request).await.unwrap();
    let count = scan.partition_count();
    let rows = scan
        .partitions()
        .map(|partition| partition.unwrap().row_count())
        .collect()
        .await;
    (count, rows)
}

/// `view`'s source scans to its base-ordered codes under a non-nullable
/// struct dtype, and advertises its statistics' count (exact, or as the
/// upper bound) and four `u64` codes a row as its byte size. The source and
/// the statistics are taken before anything reads the view. Returns the
/// count the source advertises.
async fn assert_source_matches(view: &VortexRdfStore, tag: &str) -> Precision<u64> {
    let source = view.data_source().unwrap();
    let hint = view.view_statistics().unwrap().rows;
    let expected = base_ordered_codes(view).await;
    assert!(
        matches!(source.dtype(), DType::Struct(_, Nullability::NonNullable)),
        "{tag}: a non-nullable struct dtype"
    );
    let rows = match hint.exact {
        Some(n) => {
            assert_eq!(n, expected.len(), "{tag}: an exact count");
            Precision::exact(n as u64)
        }
        None => {
            assert!(hint.upper_bound >= expected.len(), "{tag}: a bound");
            Precision::inexact(hint.upper_bound as u64)
        }
    };
    assert_eq!(source.row_count(), rows, "{tag}: the statistics' count");
    assert_eq!(
        source.byte_size(),
        rows.map(|n| n * 32),
        "{tag}: four u64 codes a row"
    );
    assert_eq!(
        scan_codes(&source, ScanRequest::default()).await,
        expected,
        "{tag}: default scan"
    );
    rows
}

/// In memory, over every index set: each view's source, tombstoned views'
/// included, scans to its base-ordered codes and counts them exactly.
#[tokio::test]
async fn test_data_source_in_memory_matches_gathered() {
    let quads = fixture_quads();
    for indexes in [
        vec![],
        vec![IndexType::SecondaryByCopy],
        vec![IndexType::SecondaryByReference],
    ] {
        let store = built_store(quads.clone(), LayoutStrategy::Dictionary, indexes.clone()).await;
        let mut deleted = store.clone();
        for quad in quads.iter().step_by(11).take(30) {
            deleted = deleted.delete_quad(quad).await.unwrap();
        }
        for (state, base) in [("live", &store), ("tombstoned", &deleted)] {
            for (name, view) in views_of(base).await {
                let tag = format!("memory {indexes:?} {state} {name}");
                let rows = assert_source_matches(&view, &tag).await;
                assert!(
                    matches!(rows, Precision::Exact(_)),
                    "{tag}: exact in memory"
                );
            }
        }
    }
}

/// Projection, filter, row window, selection, limit and partition window,
/// each applied exactly, in the order filter, limit, projection; the
/// partition's advertised count; a request that does not bind; and the
/// field statistics.
#[tokio::test]
async fn test_data_source_request_semantics() {
    let store = built_store(fixture_quads(), LayoutStrategy::Dictionary, vec![]).await;
    let all = base_ordered_codes(&store).await;
    let len = all.len() as u64;
    let source = store.data_source().unwrap();
    let dict = store.dict_reader().unwrap();
    let p1 = dict
        .encode("<http://example.org/p1>")
        .await
        .unwrap()
        .unwrap();
    let p3 = dict
        .encode("<http://example.org/p3>")
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
    assert_eq!(field_names(&chunks[0]), ["o", "s"]);
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
    let swapped: Vec<[TermCode; 4]> = all.iter().map(|r| [r[0], r[3], r[2], r[1]]).collect();
    assert_eq!(chunk_codes(&chunks), swapped, "a pack renames and reorders");

    // A filter on a code column.
    let p1_eq = || eq(get_item("p", root()), lit(p1));
    let want: Vec<[TermCode; 4]> = all.iter().copied().filter(|r| r[1] == p1).collect();
    assert!(!want.is_empty());
    assert_eq!(
        scan_codes(&source, filtered(p1_eq())).await,
        want,
        "an equality filter"
    );

    // `p IN (p1, p3)` as vortex-datafusion emits it, over a non-nullable list
    // literal; and `p IN (p1, NULL)`, whose null element makes every row that
    // matches no element null, which the filter drops.
    let in_list_of = |elements: Vec<Scalar>| {
        let element_dtype = elements[0].dtype().clone();
        in_list(
            get_item("p", root()),
            lit(Scalar::list(
                element_dtype,
                elements,
                Nullability::NonNullable,
            )),
        )
    };
    let listed = in_list_of(vec![Scalar::from(p1), Scalar::from(p3)]);
    assert_eq!(
        scan_codes(&source, filtered(listed)).await,
        all.iter()
            .copied()
            .filter(|r| r[1] == p1 || r[1] == p3)
            .collect::<Vec<_>>(),
        "an IN list"
    );
    let null = Scalar::null(DType::Primitive(PType::U64, Nullability::Nullable));
    let with_null = in_list_of(vec![Scalar::from(p1).into_nullable(), null]);
    assert_eq!(
        scan_codes(&source, filtered(with_null)).await,
        want,
        "an IN list holding a null"
    );

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
            filter: Some(p1_eq()),
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
    let ids: Vec<u64> = (0..len).step_by(13).collect();
    let by_ids = || {
        Selection::IncludeByIndex(
            StrictSortedBuffer::try_new(Buffer::from_iter(ids.iter().copied())).unwrap(),
        )
    };
    let selected = scan_codes(
        &source,
        ScanRequest {
            selection: by_ids(),
            ..Default::default()
        },
    )
    .await;
    let want_ids: Vec<[TermCode; 4]> = ids.iter().map(|&i| all[i as usize]).collect();
    assert_eq!(selected, want_ids, "an id selection");
    let both = scan_codes(
        &source,
        ScanRequest {
            row_range: Some(50..1_000),
            selection: by_ids(),
            limit: Some(10),
            ..Default::default()
        },
    )
    .await;
    let want_both: Vec<[TermCode; 4]> = ids
        .iter()
        .filter(|&&i| (50..1_000).contains(&i))
        .take(10)
        .map(|&i| all[i as usize])
        .collect();
    assert_eq!(both, want_both, "a range, a selection and a limit together");

    // The one partition's count: exact under a row window or a limit, a
    // bound while a filter or a selection has rows to test.
    for (tag, request, rows) in [
        ("whole", ScanRequest::default(), Precision::exact(len)),
        (
            "row range",
            ScanRequest {
                row_range: Some(100..2_950),
                ..Default::default()
            },
            Precision::exact(2_850u64),
        ),
        (
            "limit",
            ScanRequest {
                limit: Some(17),
                ..Default::default()
            },
            Precision::exact(17u64),
        ),
        ("filter", filtered(p1_eq()), Precision::inexact(len)),
        (
            "selection",
            ScanRequest {
                selection: by_ids(),
                ..Default::default()
            },
            Precision::inexact(len),
        ),
    ] {
        assert_eq!(
            partition_counts(&source, request).await,
            (Precision::exact(1usize), vec![rows]),
            "{tag}"
        );
    }
    // The partition window and the partition selection can leave it out.
    let first = || StrictSortedBuffer::try_new(Buffer::from_iter([0u64])).unwrap();
    for (tag, request) in [
        (
            "partition range",
            ScanRequest {
                partition_range: Some(1..2),
                ..Default::default()
            },
        ),
        (
            "partition selection",
            ScanRequest {
                partition_selection: Selection::ExcludeByIndex(first()),
                ..Default::default()
            },
        ),
    ] {
        assert_eq!(
            partition_counts(&source, request.clone()).await,
            (Precision::exact(0usize), vec![]),
            "{tag}"
        );
        assert!(scan_chunks(&source, request).await.is_empty(), "{tag}");
    }

    // A projection or a filter over a field the dtype lacks fails the scan.
    for (tag, request) in [
        (
            "projection",
            ScanRequest {
                projection: get_item("nope", root()),
                ..Default::default()
            },
        ),
        ("filter", filtered(eq(get_item("nope", root()), lit(p1)))),
    ] {
        assert!(source.scan(request).await.is_err(), "{tag}");
    }

    // Field statistics: no nulls, and a code column's uncompressed size; none
    // for a name the dtype lacks or a path into a code column.
    let u64_dtype = DType::Primitive(PType::U64, Nullability::NonNullable);
    let stats = source
        .field_statistics(&FieldPath::from_name("s"))
        .await
        .unwrap();
    assert_eq!(
        stats.get_as::<u64>(Stat::NullCount, &u64_dtype),
        Precision::exact(0u64)
    );
    assert_eq!(
        stats.get_as::<u64>(Stat::UncompressedSizeInBytes, &u64_dtype),
        Precision::exact(len * 8)
    );
    for path in [
        FieldPath::from_name("nope"),
        FieldPath::from_name("s").push("x"),
    ] {
        assert!(
            source.field_statistics(&path).await.unwrap().is_empty(),
            "{path}"
        );
    }
}

/// On every file open and index set, each view's source scans to its
/// base-ordered codes and counts them exactly where the statistics know the
/// count, as a bound while a pushed-down filter is pending; the matrix
/// reaches both.
#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_data_source_file_backed() {
    let (mut exact_counts, mut bounds) = (0, 0);
    for (tag, _dir, store) in file_stores().await {
        for (name, view) in views_of(&store).await {
            match assert_source_matches(&view, &format!("{tag} {name}")).await {
                Precision::Exact(_) => exact_counts += 1,
                _ => bounds += 1,
            }
        }
    }
    assert!(
        exact_counts > 0 && bounds > 0,
        "{exact_counts} exact counts, {bounds} bounds"
    );
}

/// The store's children as plain tables, built in memory and on every file
/// open: a name the store has no child of has none; every child
/// [`index_components`](VortexRdfStore::index_components) lists is a table of
/// one row per quad, each row addressing its base row by `rid` (a copy
/// child's quads in its sort order, a reference child's sorted `(val, rid)`
/// pairs); and the dictionary's table holds its terms, row i the term of
/// code i.
#[tokio::test]
async fn test_component_data_sources() {
    let store = built_store(
        fixture_quads(),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy, IndexType::SecondaryByReference],
    )
    .await;
    assert_eq!(assert_component_tables(&store, "memory").await, 4);
    #[cfg(feature = "file-io")]
    {
        for (tag, _dir, store) in file_stores().await {
            let children = if tag.starts_with("file/no-index") {
                0
            } else {
                2
            };
            assert_eq!(
                assert_component_tables(&store, &tag).await,
                children,
                "{tag}"
            );
        }
    }
}

/// The checks of [`test_component_data_sources`] on `store`. Returns how
/// many index children it checked.
async fn assert_component_tables(store: &VortexRdfStore, tag: &str) -> usize {
    assert!(
        store.component_data_source("nope").unwrap().is_none(),
        "{tag}: no child of that name"
    );
    let base = base_ordered_codes(store).await;
    let children = store.index_components().unwrap();
    for child in &children {
        let name = child.name;
        let tag = format!("{tag} {name}");
        let table = store
            .component_data_source(name)
            .unwrap()
            .unwrap_or_else(|| panic!("{tag}: a listed child is a table"));
        assert_eq!(
            table.row_count().as_exact(),
            Some(base.len() as u64),
            "{tag}: one row per quad"
        );
        let chunks = scan_chunks(&table, ScanRequest::default()).await;
        let rids: Vec<u64> = match child.sort_order {
            // A copy child: the base's quads in the child's order.
            Some(order) => {
                let columns = ["s", "p", "o", "g", "rid"];
                assert_eq!(field_names(&chunks[0]), columns, "{tag}");
                let rows = chunk_columns(&chunks, &columns);
                let key = |r: &[u64]| match order {
                    SortOrder::Spog => [r[0], r[1], r[2], r[3]],
                    SortOrder::Posg => [r[1], r[2], r[0], r[3]],
                    SortOrder::Ospg => [r[2], r[0], r[1], r[3]],
                };
                assert!(
                    rows.windows(2).all(|w| key(&w[0]) <= key(&w[1])),
                    "{tag}: sorted {order:?}"
                );
                assert!(
                    rows.iter()
                        .all(|r| base[r[4] as usize] == [r[0], r[1], r[2], r[3]]),
                    "{tag}: each row is its base row"
                );
                rows.iter().map(|r| r[4]).collect()
            }
            // A reference child: the code each base row holds in the child's
            // role, beside the row's id, sorted.
            None => {
                let columns = ["val", "rid"];
                assert_eq!(field_names(&chunks[0]), columns, "{tag}");
                let role = match name {
                    "index:ref-p" => QuadColumn::P,
                    "index:ref-o" => QuadColumn::O,
                    other => panic!("{other}: not a reference child"),
                };
                let rows = chunk_columns(&chunks, &columns);
                assert!(
                    rows.windows(2).all(|w| w[0] <= w[1]),
                    "{tag}: sorted (val, rid) pairs"
                );
                assert!(
                    rows.iter()
                        .all(|r| base[r[1] as usize][role.index()] == r[0]),
                    "{tag}: each val is its base row's code"
                );
                rows.iter().map(|r| r[1]).collect()
            }
        };
        let mut distinct = rids;
        distinct.sort_unstable();
        distinct.dedup();
        assert_eq!(distinct.len(), base.len(), "{tag}: a row per base row");
    }
    #[cfg(feature = "file-io")]
    assert_dictionary_component(store, tag).await;
    children.len()
}

/// The `dictionary` child's table: `{_dict_term}`, one strictly sorted row
/// per term, row i the term of code i.
#[cfg(feature = "file-io")]
async fn assert_dictionary_component(store: &VortexRdfStore, tag: &str) {
    let table = store
        .component_data_source("dictionary")
        .unwrap()
        .unwrap_or_else(|| panic!("{tag}: the dictionary is a table"));
    let dict = store.dict_reader().unwrap();
    assert_eq!(
        table.row_count().as_exact(),
        Some(dict.len() as u64),
        "{tag}: one row per term"
    );
    let chunks = scan_chunks(&table, ScanRequest::default()).await;
    assert_eq!(field_names(&chunks[0]), ["_dict_term"], "{tag}");
    let terms = chunk_strings(&chunks, "_dict_term");
    assert!(
        terms.windows(2).all(|w| w[0].as_bytes() < w[1].as_bytes()),
        "{tag}: strictly sorted terms"
    );
    let codes: Vec<TermCode> = (0..terms.len() as TermCode).collect();
    let decoded: Vec<String> = dict
        .decode_many(&codes)
        .await
        .unwrap()
        .into_iter()
        .map(|term| term.expect("every code below the length has a term"))
        .collect();
    assert_eq!(decoded, terms, "{tag}: row i is the term of code i");
}

/// A string layout's source carries its utf8 columns, the tail's rows after
/// the base's, and advertises no byte size; a Dictionary view with a tail
/// has no source.
#[tokio::test]
async fn test_data_source_string_layouts() {
    let appended = appended_quads(40);
    for layout in [LayoutStrategy::Default, LayoutStrategy::TypedObject] {
        let store = built_store(fixture_quads(), layout, vec![])
            .await
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
        let source = store.data_source().unwrap();
        assert_eq!(
            source.row_count().as_exact(),
            Some(expected.len() as u64),
            "{layout:?}"
        );
        assert_eq!(
            source.byte_size(),
            Precision::Absent,
            "{layout:?}: string columns have no fixed width"
        );
        let chunks = scan_chunks(&source, ScanRequest::default()).await;
        assert_eq!(chunk_subjects(&chunks), expected, "{layout:?}");
    }
    let tailed = built_store(fixture_quads(), LayoutStrategy::Dictionary, vec![])
        .await
        .add_quads(appended)
        .await
        .unwrap();
    assert!(
        matches!(
            tailed.data_source(),
            Err(VortexRdfError::InvalidOperation(_))
        ),
        "a tailed Dictionary view has no source"
    );
}

/// The session [`vortex_session`](crate::vortex_session) hands out opens a
/// store file through vortex's own reader: it holds the store's container
/// layout, whose root is the quad table.
#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_vortex_session_opens_store_files() {
    use vortex_file::OpenOptionsSessionExt as _;

    let (_dir, path) = write_store_file(fixture_quads(), LayoutStrategy::Dictionary, vec![]).await;
    let bytes = vortex_buffer::ByteBuffer::from(std::fs::read(&path).unwrap());
    let file = crate::vortex_session()
        .open_options()
        .open_buffer(bytes)
        .unwrap();
    assert_eq!(file.row_count(), 3_000);
    let DType::Struct(fields, _) = file.dtype() else {
        panic!("the quad table is a struct")
    };
    let names: Vec<String> = fields.names().iter().map(|n| n.to_string()).collect();
    assert_eq!(names, ["s", "p", "o", "g"]);
}
