//! Chunked exports — `row_chunks`, `code_chunks` and the data source's scan
//! over them — against the whole-view reads, on every backend, over
//! narrowed, served, tombstoned and tailed views.

use super::*;
use crate::store::read::chunks::{gather_hooks, run_hooks};
use crate::store::{QuadColumn, TermCode};
use crate::vortex_scan::ScanRequest;
use vortex_array::dtype::{DType, Nullability};
use vortex_array::expr::stats::Precision;

/// Both exports of `view` agree with its base-ordered codes at several
/// batch sizes, and a column subset comes in the requested order. Its data
/// source scans to the same codes under a non-nullable struct dtype, and
/// advertises its statistics' count (exact, or as the upper bound) and four
/// `u64` codes a row as its byte size; the source and the statistics are
/// taken before anything reads the view. Returns the count the source
/// advertises.
async fn assert_chunks_match(view: &VortexRdfStore, tag: &str) -> Precision<u64> {
    let source = view.data_source().unwrap();
    let hint = view.view_statistics().unwrap().rows;
    let expected = base_ordered_codes(view).await;
    for batch in [1, 7, 256, 100_000] {
        let (rows, sizes) = collect_code_chunks(view, &QuadColumn::ALL, batch).await;
        assert_eq!(rows, expected, "{tag}: code_chunks at batch {batch}");
        assert!(
            sizes.iter().all(|&n| n > 0 && n <= batch),
            "{tag}: chunk sizes {sizes:?} under batch {batch}"
        );
        let chunks = collect_row_chunks(view, batch).await;
        assert_eq!(
            chunk_code_rows(&chunks),
            expected,
            "{tag}: row_chunks at batch {batch}"
        );
    }
    let (rows, _) = collect_code_chunks(view, &[QuadColumn::O, QuadColumn::S], 1_000).await;
    let want: Vec<[TermCode; 2]> = expected.iter().map(|r| [r[2], r[0]]).collect();
    assert_eq!(rows, want, "{tag}: a column subset in the requested order");

    assert!(
        matches!(source.dtype(), DType::Struct(_, Nullability::NonNullable)),
        "{tag}: a non-nullable struct dtype"
    );
    let count = match hint.exact {
        Some(n) => {
            assert_eq!(n, expected.len(), "{tag}: an exact count");
            Precision::exact(n as u64)
        }
        None => {
            assert!(hint.upper_bound >= expected.len(), "{tag}: a bound");
            Precision::inexact(hint.upper_bound as u64)
        }
    };
    assert_eq!(source.row_count(), count, "{tag}: the statistics' count");
    assert_eq!(
        source.byte_size(),
        count.map(|n| n * 32),
        "{tag}: four u64 codes a row"
    );
    assert_eq!(
        scan_codes(&source, ScanRequest::default()).await,
        expected,
        "{tag}: the data source's scan"
    );
    count
}

/// Every item of `stream`, unwrapped, beside how far `counter` rose over the
/// stream's first pull and over the whole stream.
async fn drain_counted<T>(
    mut stream: futures::stream::BoxStream<'static, crate::error::Result<T>>,
    counter: impl Fn() -> usize,
) -> (Vec<T>, usize, usize) {
    let start = counter();
    let mut items = Vec::new();
    if let Some(item) = stream.next().await {
        items.push(item.unwrap());
    }
    let first = counter() - start;
    while let Some(item) = stream.next().await {
        items.push(item.unwrap());
    }
    (items, first, counter() - start)
}

/// The `(s, p, o, g)` rows of code chunks over every column, concatenated.
fn code_rows(chunks: &[Vec<vortex_buffer::Buffer<TermCode>>]) -> Vec<[TermCode; 4]> {
    chunks
        .iter()
        .flat_map(|c| (0..c[0].len()).map(move |i| [c[0][i], c[1][i], c[2][i], c[3][i]]))
        .collect()
}

/// Both exports of the in-memory `view` at `batch` rows a chunk answer its
/// base-ordered codes through `gathers` window gathers, one per pull: the
/// first pull gathers one window, or none when the view gathers none.
async fn assert_gathers(view: &VortexRdfStore, batch: usize, gathers: usize, tag: &str) {
    let expected = base_ordered_codes(view).await;
    let first_pull = gathers.min(1);
    let codes = view.code_chunks(&QuadColumn::ALL, batch).unwrap();
    let (chunks, first, total) = drain_counted(codes, gather_hooks::gathers).await;
    assert_eq!(
        first, first_pull,
        "{tag}: code_chunks' first pull at batch {batch}"
    );
    assert_eq!(
        total, gathers,
        "{tag}: code_chunks' gathers at batch {batch}"
    );
    assert_eq!(
        code_rows(&chunks),
        expected,
        "{tag}: code_chunks at batch {batch}"
    );
    let rows = view.row_chunks(batch).unwrap();
    let (chunks, first, total) = drain_counted(rows, gather_hooks::gathers).await;
    assert_eq!(
        first, first_pull,
        "{tag}: row_chunks' first pull at batch {batch}"
    );
    assert_eq!(
        total, gathers,
        "{tag}: row_chunks' gathers at batch {batch}"
    );
    assert_eq!(
        chunk_code_rows(&chunks),
        expected,
        "{tag}: row_chunks at batch {batch}"
    );
}

/// In memory, over every index set, and the multi-chunk store whole: each
/// view's chunks and data source scan are its base-ordered codes, which the
/// source counts exactly, and the whole store's code chunks are its code
/// columns, not copies of them. An index-served match gathers one window of
/// its ids per chunk, as the chunk is pulled; the whole store, a contiguous
/// run, gathers nothing.
#[tokio::test]
async fn test_chunks_in_memory_match_gathered() {
    let count = assert_chunks_match(&multi_chunk_store().await, "memory multi-chunk").await;
    assert!(
        matches!(count, Precision::Exact(_)),
        "multi-chunk: exact in memory"
    );
    for indexes in [
        vec![],
        vec![IndexType::SecondaryByCopy],
        vec![IndexType::SecondaryByReference],
    ] {
        let store = built_store(fixture_quads(), LayoutStrategy::Dictionary, indexes.clone()).await;
        for (name, view) in views_of(&store).await {
            let tag = format!("memory {indexes:?} {name}");
            let count = assert_chunks_match(&view, &tag).await;
            assert!(
                matches!(count, Precision::Exact(_)),
                "{tag}: exact in memory"
            );
        }
        let [s, ..] = store.code_columns_gathered().await.unwrap().unwrap();
        let mut chunks = store.code_chunks(&[QuadColumn::S], usize::MAX).unwrap();
        let chunk = chunks.next().await.unwrap().unwrap();
        assert!(chunks.next().await.is_none(), "one chunk under the cap");
        assert_eq!(
            chunk[0].as_slice().as_ptr(),
            s.as_slice().as_ptr(),
            "{indexes:?}: the chunk is the base's column buffer"
        );
    }

    let store = built_store(
        fixture_quads(),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let served = store
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    assert_eq!(
        served_component(&served),
        Some("index:posg"),
        "the P match is served by the copy index"
    );
    let rows = served.size().await.unwrap();
    for batch in [7, 256] {
        assert_gathers(&served, batch, rows.div_ceil(batch), "served P match").await;
    }
    assert_gathers(&store, 7, 0, "the whole store").await;
}

/// In memory, the whole store is one contiguous run, and its code chunks are
/// cut as they are pulled: at one row a chunk the first pull cuts one chunk,
/// and the stream one per row.
#[tokio::test]
async fn test_chunks_cut_a_run_per_pull() {
    let store = built_store(fixture_quads(), LayoutStrategy::Dictionary, vec![]).await;
    let expected = base_ordered_codes(&store).await;
    let codes = store.code_chunks(&QuadColumn::ALL, 1).unwrap();
    let (chunks, first, total) = drain_counted(codes, run_hooks::cuts).await;
    assert_eq!(first, 1, "the first pull cuts one chunk");
    assert_eq!(total, expected.len(), "one cut per row at one row a chunk");
    assert_eq!(code_rows(&chunks), expected, "the cut chunks are the rows");
}

/// In memory, over every index set, the views of a store with tombstones:
/// the exports and the data source's scan skip the deleted rows, and the
/// source counts the live ones exactly. Every view, the whole store's
/// included, gathers one window of its live ids per chunk.
#[tokio::test]
async fn test_chunks_skip_tombstones() {
    let quads = fixture_quads();
    for indexes in [
        vec![],
        vec![IndexType::SecondaryByCopy],
        vec![IndexType::SecondaryByReference],
    ] {
        let store = built_store(quads.clone(), LayoutStrategy::Dictionary, indexes.clone()).await;
        let mut deleted = store.clone();
        for quad in quads.iter().step_by(7).take(50) {
            deleted = deleted.delete_quad(quad).await.unwrap();
        }
        assert_eq!(deleted.size().await.unwrap(), quads.len() - 50);
        for (name, view) in views_of(&deleted).await {
            let tag = format!("tombstoned {indexes:?} {name}");
            let count = assert_chunks_match(&view, &tag).await;
            assert!(
                matches!(count, Precision::Exact(_)),
                "{tag}: exact in memory"
            );
            let rows = view.size().await.unwrap();
            assert_gathers(&view, 7, rows.div_ceil(7), &tag).await;
        }
    }
}

/// On every file open and index set, and on the multi-chunk store's mapped
/// file whole, each view's chunks and data source scan are its base-ordered
/// codes; the source counts them exactly where the statistics know the count
/// and as a bound while a pushed-down filter is pending, and the matrix
/// reaches both.
#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_chunks_file_backed_match_gathered() {
    let (_multi_dir, multi_chunk) = multi_chunk_file().await;
    let mut counts = vec![assert_chunks_match(&multi_chunk, "mapped multi-chunk").await];
    for (tag, _dir, store) in file_stores().await {
        for (name, view) in views_of(&store).await {
            counts.push(assert_chunks_match(&view, &format!("{tag} {name}")).await);
        }
    }
    let exact_counts = counts
        .iter()
        .filter(|count| matches!(count, Precision::Exact(_)))
        .count();
    let bounds = counts.len() - exact_counts;
    assert!(
        exact_counts > 0 && bounds > 0,
        "{exact_counts} exact counts, {bounds} bounds"
    );
}

/// A file view of exact ids past the point-read cap and sparse in their
/// split — every 20th subject of 20,000 — is read by taking its ids, as the
/// whole-view gather is, one chunk's ids per take: at 256 rows a chunk each
/// export's first pull takes one window of its 1,000 ids through the chunk
/// probes and the whole stream four, and the exports answer the codes of
/// the same keep on the store loaded whole.
#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_chunks_take_sparse_file_ids() {
    use crate::store::Keep;
    use crate::store::scan::file_reads::take_hooks;

    let quads = modular_quads(20_000, 3, 4);
    let (_dir, path) = write_store_file(quads, LayoutStrategy::Dictionary, vec![]).await;
    let file = VortexRdfStore::from_file(&path).await.unwrap();
    let loaded = VortexRdfStore::from_file_in_memory(&path).await.unwrap();
    let dict = file.dict_reader().unwrap();
    let (lo, hi) = dict.prefix_range("<http://example.org/s").await.unwrap();
    let keep = Keep::set((lo..hi).step_by(20));
    let view = file.keep(QuadColumn::S, &keep).await.unwrap();
    let expected = base_ordered_codes(&loaded.keep(QuadColumn::S, &keep).await.unwrap()).await;
    assert_eq!(expected.len(), 1_000);

    let takes = || take_hooks::takes().0;
    let codes = view.code_chunks(&QuadColumn::ALL, 256).unwrap();
    let (code_chunks, first, total) = drain_counted(codes, takes).await;
    assert_eq!(first, 1, "code_chunks' first pull takes one chunk's ids");
    assert_eq!(total, 4, "code_chunks takes one chunk's ids at a time");
    let rows = view.row_chunks(256).unwrap();
    let (row_chunks, first, total) = drain_counted(rows, takes).await;
    assert_eq!(first, 1, "row_chunks' first pull takes one chunk's ids");
    assert_eq!(total, 4, "row_chunks takes one chunk's ids at a time");
    assert_eq!(code_rows(&code_chunks), expected, "code_chunks");
    assert_eq!(chunk_code_rows(&row_chunks), expected, "row_chunks");
}

/// A subject run on a sorted file whose bound role past a gap in the
/// `(s, p, o, g)` order stays a pending filter — `<s> ?p "plain"`, and
/// `GRAPH <g> { <s> <p2> ?o }` — is point-sized, so its code chunks read it
/// by id even when they project only an unbound role. With the split limit
/// forced to zero a scan would spawn its splits; nothing spawns, and the
/// chunks are the view's base-ordered codes.
#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_chunks_read_filtered_file_runs_by_id() {
    use crate::store::scan::file_reads::driver_hooks;

    let (_dir, path) =
        write_store_file(term_kind_quads(), LayoutStrategy::Dictionary, vec![]).await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let s = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s").unwrap());
    let p2 = NamedNode::new("http://example.org/p2").unwrap();
    let plain = Term::Literal(Literal::new_simple_literal("plain"));
    let g = GraphName::NamedNode(NamedNode::new("http://example.org/g").unwrap());
    let views = [
        (
            "<s> ?p \"plain\"",
            store
                .match_pattern(Some(&s), None, Some(&plain), None)
                .await
                .unwrap(),
            QuadColumn::P,
            1,
        ),
        (
            "GRAPH <g> { <s> <p2> ?o }",
            store
                .match_pattern(Some(&s), Some(&p2), None, Some(&g))
                .await
                .unwrap(),
            QuadColumn::O,
            2,
        ),
    ];
    for (tag, view, column, rows) in views {
        let expected: Vec<[TermCode; 1]> = base_ordered_codes(&view)
            .await
            .iter()
            .map(|row| [row[column.index()]])
            .collect();
        assert_eq!(expected.len(), rows, "{tag}");
        let _limit = driver_hooks::ForcedLimit::set(0);
        driver_hooks::take_spawned();
        let (codes, _) = collect_code_chunks(&view, &[column], 256).await;
        assert_eq!(
            driver_hooks::take_spawned(),
            0,
            "{tag}: read by id, not scanned"
        );
        assert_eq!(codes, expected, "{tag}");
    }
}

/// A file view's chunks, and its data source's scan, read with no tokio
/// runtime: a bare executor on a plain thread polls them, with the split
/// limit forced to zero so the scan would spawn wherever a runtime were there
/// to spawn onto. Nothing is spawned, and both exports and the scan answer
/// the view's base-ordered codes.
#[cfg(feature = "file-io")]
#[tokio::test]
async fn test_chunks_read_a_file_outside_a_tokio_runtime() {
    use crate::store::scan::file_reads::driver_hooks;

    let (_dir, path) = write_store_file(fixture_quads(), LayoutStrategy::Dictionary, vec![]).await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    // A pending filter: both exports scan the file.
    let view = store
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    let expected = base_ordered_codes(&view).await;
    assert_eq!(expected.len(), 600);

    let (codes, rows, scanned, spawned) = std::thread::spawn(move || {
        let _limit = driver_hooks::ForcedLimit::set(0);
        futures::executor::block_on(async {
            driver_hooks::take_spawned();
            let (codes, _) = collect_code_chunks(&view, &QuadColumn::ALL, 256).await;
            let rows = chunk_code_rows(&collect_row_chunks(&view, 256).await);
            let source = view.data_source().unwrap();
            let scanned = scan_codes(&source, ScanRequest::default()).await;
            (codes, rows, scanned, driver_hooks::take_spawned())
        })
    })
    .join()
    .expect("a chunk stream must not panic without a tokio runtime");
    assert_eq!(spawned, 0, "no runtime to spawn onto, so nothing spawned");
    assert_eq!(codes, expected, "code_chunks");
    assert_eq!(rows, expected, "row_chunks");
    assert_eq!(scanned, expected, "the data source's scan");
}

/// The data source of a string-layout `view` whose subjects, in the order
/// `quads()` decodes them, are `expected`: it counts them exactly, reports no
/// byte size, and its scan carries the same utf8 rows.
async fn assert_string_source(view: &VortexRdfStore, expected: &[String], tag: &str) {
    let source = view.data_source().unwrap();
    assert_eq!(
        source.row_count(),
        Precision::exact(expected.len() as u64),
        "{tag}"
    );
    assert_eq!(
        source.byte_size(),
        Precision::Absent,
        "{tag}: string columns have no fixed width"
    );
    let chunks = scan_chunks(&source, ScanRequest::default()).await;
    assert_eq!(
        chunk_subjects(&chunks),
        expected,
        "{tag}: the data source's scan"
    );
}

/// The string layouts stream their native struct rows, the tail last, in the
/// order `quads()` decodes them, through the chunks and the data source.
#[tokio::test]
async fn test_chunks_string_layouts_include_tail() {
    let quads = fixture_quads();
    let appended = appended_quads(40);
    for layout in [LayoutStrategy::Default, LayoutStrategy::TypedObject] {
        let store = built_store(quads.clone(), layout, vec![]).await;
        let base_rows = store.size().await.unwrap();
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
            // The base's rows and the tail's are cut apart, each to the cap.
            let cut = |rows: usize| {
                (0..rows)
                    .step_by(batch)
                    .map(move |start| (rows - start).min(batch))
            };
            let lengths: Vec<usize> = cut(base_rows).chain(cut(tailed.tail_len())).collect();
            assert_eq!(
                chunks.iter().map(|c| c.len()).collect::<Vec<_>>(),
                lengths,
                "{layout:?} at batch {batch}: the base's chunks, then the tail's"
            );
        }
        assert!(
            matches!(
                tailed.code_chunks(&QuadColumn::ALL, 10),
                Err(VortexRdfError::InvalidOperation(_))
            ),
            "no codes under {layout:?}"
        );
        assert_string_source(&tailed, &expected, &format!("{layout:?}")).await;
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
        assert_string_source(&tailed, &expected, "file-backed Default with a tail").await;
    }
}

/// The gates: a zero batch and no columns are invalid input; a Dictionary
/// view whose tail holds terms without codes has no chunks and no data
/// source, an invalid operation.
#[tokio::test]
async fn test_chunks_gates() {
    let store = built_store(fixture_quads(), LayoutStrategy::Dictionary, vec![]).await;
    let invalid =
        |result: crate::error::Result<_>| matches!(result, Err(VortexRdfError::InvalidInput(_)));
    assert!(invalid(store.row_chunks(0).map(drop)));
    assert!(invalid(store.code_chunks(&QuadColumn::ALL, 0).map(drop)));
    assert!(invalid(store.code_chunks(&[], 10).map(drop)));
    let refused = |result: crate::error::Result<_>| {
        matches!(result, Err(VortexRdfError::InvalidOperation(_)))
    };
    let tailed = store.add_quads(appended_quads(3)).await.unwrap();
    assert_eq!(tailed.tail_len(), 3);
    assert!(
        refused(tailed.row_chunks(10).map(drop)),
        "a tailed Dictionary view has no single dtype"
    );
    assert!(refused(tailed.code_chunks(&QuadColumn::ALL, 10).map(drop)));
    assert!(
        refused(tailed.data_source().map(drop)),
        "a tailed Dictionary view has no source"
    );
}
