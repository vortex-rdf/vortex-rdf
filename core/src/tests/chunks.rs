//! Chunked exports — `row_chunks`, `code_chunks` and the data source's scan
//! over them — against the whole-view reads, on every backend, over
//! narrowed, served, tombstoned and tailed views.

use super::masks_first::{MANY_SPLITS, many_splits_file};
use super::*;
use crate::store::array::rechunk_hooks;
use crate::store::read::chunks::{RowChunkStream, gather_hooks, run_hooks};
use crate::store::read::rows::column_hooks;
use crate::store::{QuadColumn, TermCode};
use crate::vortex_scan::ScanRequest;
use vortex_array::dtype::{DType, Nullability};
use vortex_array::expr::stats::Precision;

/// Both exports of `view` agree with its base-ordered codes at several batch
/// sizes — one row (on a view of at most 256 rows), a size the rows do not
/// divide, the view's length — in the chunks the view reads at its length
/// (one, or a file scan's split each), each cut to the batch; every column
/// subset reads the subset's columns of the four-column read, and a subset
/// comes in the requested order. Its data source scans to the same codes
/// under a non-nullable struct dtype, and advertises its statistics' count
/// (exact, or as the upper bound) and four `u64` codes a row as its byte
/// size; the source and the statistics are taken before anything reads the
/// view. Returns the count the source advertises.
async fn assert_chunks_match(view: &VortexRdfStore, tag: &str) -> Precision<u64> {
    let source = view.data_source().unwrap();
    let hint = view.view_statistics().unwrap().rows;
    let expected = base_ordered_codes(view).await;
    let n = expected.len();
    let uncut: Vec<usize> = collect_row_chunks(view, n.max(1))
        .await
        .iter()
        .map(|c| c.len())
        .collect();
    let mut batches = vec![short_last(n), 256, n.max(1), 100_000];
    // One row a chunk is a pull per row: a small view holds the case, and
    // the other batches cut every larger view's chunks and splits.
    if n <= 256 {
        batches.push(1);
    }
    batches.sort_unstable();
    batches.dedup();
    for batch in batches {
        let cut: Vec<usize> = uncut.iter().flat_map(|&m| cut_lengths(m, batch)).collect();
        let (rows, sizes) = collect_code_chunks(view, &QuadColumn::ALL, batch).await;
        assert_eq!(rows, expected, "{tag}: code_chunks at batch {batch}");
        assert_eq!(sizes, cut, "{tag}: code_chunks' chunks at batch {batch}");
        let chunks = collect_row_chunks(view, batch).await;
        assert_eq!(
            struct_chunk_codes(&chunks),
            expected,
            "{tag}: row_chunks at batch {batch}"
        );
        assert_eq!(
            chunks.iter().map(|c| c.len()).collect::<Vec<_>>(),
            cut,
            "{tag}: row_chunks' chunks at batch {batch}"
        );
    }
    let (all, _) = collect_code_chunks(view, &QuadColumn::ALL, 1_000).await;
    assert_eq!(all, expected, "{tag}: code_chunks at 1,000 rows");
    assert_subsets::<1>(view, &all, tag).await;
    assert_subsets::<2>(view, &all, tag).await;
    assert_subsets::<3>(view, &all, tag).await;
    let (rows, _) = collect_code_chunks(view, &[QuadColumn::O, QuadColumn::S], 1_000).await;
    let want: Vec<[TermCode; 2]> = all.iter().map(|r| [r[2], r[0]]).collect();
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

/// Every subset of `N` code columns, in `(s, p, o, g)` order, read from
/// `view` at 1,000 rows a chunk: the subset's columns of `all`, the view's
/// four-column read at that size.
async fn assert_subsets<const N: usize>(view: &VortexRdfStore, all: &[[TermCode; 4]], tag: &str) {
    for mask in (1u8..16).filter(|mask| mask.count_ones() as usize == N) {
        let columns: [QuadColumn; N] = QuadColumn::ALL
            .into_iter()
            .filter(|c| mask & (1 << c.index()) != 0)
            .collect::<Vec<_>>()
            .try_into()
            .unwrap();
        let (rows, _) = collect_code_chunks(view, &columns, 1_000).await;
        let want: Vec<[TermCode; N]> = all.iter().map(|r| columns.map(|c| r[c.index()])).collect();
        assert_eq!(rows, want, "{tag}: the columns {columns:?}");
    }
}

/// The lengths of the chunks `rows` rows cut into at `batch` rows a chunk
/// make, in order: `batch` each, but the last.
fn cut_lengths(rows: usize, batch: usize) -> impl Iterator<Item = usize> {
    (0..rows)
        .step_by(batch)
        .map(move |start| (rows - start).min(batch))
}

/// The smallest batch of 7 rows or more that does not divide `rows`, so the
/// last chunk is short; 7 for no rows.
fn short_last(rows: usize) -> usize {
    (7..=rows + 7)
        .find(|&b| !rows.is_multiple_of(b))
        .unwrap_or(7)
}

/// How many slices `rechunk` cut while `stream` was pulled `pulls` times,
/// each pull a chunk, before it was dropped.
async fn cuts_pulling(mut stream: RowChunkStream, pulls: usize) -> usize {
    let start = rechunk_hooks::cuts();
    for _ in 0..pulls {
        stream.next().await.unwrap().unwrap();
    }
    drop(stream);
    rechunk_hooks::cuts() - start
}

/// How many chunks `stream` yields before its first error item, and the
/// error's message.
async fn chunks_before_error<T, E: std::fmt::Display>(
    mut stream: impl futures::Stream<Item = std::result::Result<T, E>> + Unpin,
) -> (usize, String) {
    let mut chunks = 0;
    while let Some(item) = stream.next().await {
        match item {
            Ok(_) => chunks += 1,
            Err(e) => return (chunks, e.to_string()),
        }
    }
    panic!("no error item after {chunks} chunks")
}

/// [`multi_chunk_store`]'s quads with the copy index: an index child of
/// 20,000 rows, which a data source cuts into three chunks.
async fn indexed_multi_chunk_store() -> VortexRdfStore {
    built_store(
        modular_quads(20_000, 3, 8),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await
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

/// Both exports of the in-memory `view` at `batch` rows a chunk answer its
/// base-ordered codes through `gathers` window gathers, one per pull: the
/// first pull gathers one window, or none when the view gathers none. A read
/// of all four code columns, in any order, gathers each window's four in one
/// pass (`gather_codes`), and a read of `o` alone only that column.
async fn assert_gathers(view: &VortexRdfStore, batch: usize, gathers: usize, tag: &str) {
    let expected = base_ordered_codes(view).await;
    let first_pull = gathers.min(1);
    // The gathers by path since `start`: `(of all four columns, of one)`.
    let since = |start: (usize, usize)| {
        let (four, one) = column_hooks::gathers();
        (four - start.0, one - start.1)
    };
    let start = column_hooks::gathers();
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
        since(start),
        (gathers, 0),
        "{tag}: one four-column gather a window at batch {batch}"
    );
    assert_eq!(
        drained_code_rows::<4>(&chunks),
        expected,
        "{tag}: code_chunks at batch {batch}"
    );
    let start = column_hooks::gathers();
    let reordered = [QuadColumn::G, QuadColumn::O, QuadColumn::S, QuadColumn::P];
    let codes = view.code_chunks(&reordered, batch).unwrap();
    let (chunks, ..) = drain_counted(codes, gather_hooks::gathers).await;
    assert_eq!(
        since(start),
        (gathers, 0),
        "{tag}: all four reordered, one four-column gather a window at batch {batch}"
    );
    assert_eq!(
        drained_code_rows::<4>(&chunks),
        expected
            .iter()
            .map(|r| [r[3], r[2], r[0], r[1]])
            .collect::<Vec<_>>(),
        "{tag}: all four reordered at batch {batch}"
    );
    let start = column_hooks::gathers();
    let codes = view.code_chunks(&[QuadColumn::O], batch).unwrap();
    let (chunks, first, total) = drain_counted(codes, || column_hooks::gathers().1).await;
    assert_eq!(
        (first, total),
        (first_pull, gathers),
        "{tag}: one column a window at batch {batch}"
    );
    assert_eq!(
        since(start),
        (0, gathers),
        "{tag}: `o` alone, one one-column gather a window at batch {batch}"
    );
    assert_eq!(
        drained_code_rows::<1>(&chunks),
        expected.iter().map(|r| [r[2]]).collect::<Vec<_>>(),
        "{tag}: the `o` chunks at batch {batch}"
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
        struct_chunk_codes(&chunks),
        expected,
        "{tag}: row_chunks at batch {batch}"
    );
}

/// In memory, over every index set, and the multi-chunk store whole: each
/// view's chunks and data source scan are its base-ordered codes, which the
/// source counts exactly, and the whole store's code chunks are its code
/// columns, not copies of them. An index-served match gathers one window of
/// its ids per chunk, as the chunk is pulled, and of only the columns read;
/// the whole store, a contiguous run, gathers nothing.
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
    let p1 = iri("p1");
    let served = matched_view(&store, None, Some(&p1), None, None).await;
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

/// Chunks are cut as they are pulled. In memory the whole store is one
/// contiguous run: at one row a chunk the first pull of its code chunks, and
/// of its row chunks, cuts one chunk, and each stream one per row; at the
/// run's length its one row chunk is the run itself, cut nowhere. A stream
/// dropped early cuts only the chunks it pulled — the store's, a file scan's
/// split, a tailed store's before its tail — and a `LIMIT` scan only its
/// first chunk, of the multi-chunk store and of an index child's table,
/// whose drained chunks are the child's rows cut to the data source's batch;
/// a child the batch holds whole is one chunk, cut nowhere.
#[tokio::test]
async fn test_chunks_cut_a_run_per_pull() {
    use crate::store::read::data_source::DATA_SOURCE_BATCH_ROWS;

    let store = built_store(fixture_quads(), LayoutStrategy::Dictionary, vec![]).await;
    let expected = base_ordered_codes(&store).await;
    let codes = store.code_chunks(&QuadColumn::ALL, 1).unwrap();
    let (chunks, first, total) = drain_counted(codes, run_hooks::cuts).await;
    assert_eq!(first, 1, "the first pull cuts one chunk");
    assert_eq!(total, expected.len(), "one cut per row at one row a chunk");
    assert_eq!(
        drained_code_rows::<4>(&chunks),
        expected,
        "the cut chunks are the rows"
    );
    let rows = store.row_chunks(1).unwrap();
    let (chunks, first, total) = drain_counted(rows, rechunk_hooks::cuts).await;
    assert_eq!(
        (first, total),
        (1, expected.len()),
        "row chunks: one cut a pull"
    );
    assert_eq!(struct_chunk_codes(&chunks), expected, "the cut row chunks");
    assert_eq!(
        cuts_pulling(store.row_chunks(expected.len()).unwrap(), 1).await,
        0,
        "at the run's length: the run itself, cut nowhere"
    );

    assert_eq!(
        cuts_pulling(store.row_chunks(7).unwrap(), 2).await,
        2,
        "the store's row chunks, two pulled"
    );
    {
        let (_dir, mapped) = multi_chunk_file().await;
        assert_eq!(
            cuts_pulling(mapped.row_chunks(7).unwrap(), 2).await,
            2,
            "a file scan's split, two pulled"
        );
    }
    let tailed = built_store(fixture_quads(), LayoutStrategy::Default, vec![])
        .await
        .add_quads(appended_quads(40))
        .await
        .unwrap();
    assert_eq!(
        cuts_pulling(tailed.row_chunks(7).unwrap(), 1).await,
        1,
        "a tailed store's first chunk, the tail uncut"
    );

    let limited = ScanRequest {
        limit: Some(100),
        ..Default::default()
    };
    let multi = multi_chunk_store().await;
    let start = rechunk_hooks::cuts();
    let rows = scan_codes(&multi.data_source().unwrap(), limited.clone()).await;
    assert_eq!(rows.len(), 100);
    assert_eq!(rechunk_hooks::cuts() - start, 1, "the store's LIMIT scan");
    let indexed = indexed_multi_chunk_store().await;
    let table = indexed
        .component_data_source("index:posg")
        .unwrap()
        .unwrap();
    let start = rechunk_hooks::cuts();
    assert_eq!(scan_chunks(&table, limited).await.len(), 1);
    assert_eq!(
        rechunk_hooks::cuts() - start,
        1,
        "an index child's LIMIT scan"
    );
    let start = rechunk_hooks::cuts();
    let lengths: Vec<usize> = scan_chunks(&table, ScanRequest::default())
        .await
        .iter()
        .map(|c| c.len())
        .collect();
    let want: Vec<usize> = cut_lengths(20_000, DATA_SOURCE_BATCH_ROWS).collect();
    assert_eq!(lengths, want, "an index child's rows cut to the batch");
    assert_eq!(rechunk_hooks::cuts() - start, want.len(), "a cut a chunk");
    let small = built_store(
        fixture_quads(),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    let table = small.component_data_source("index:posg").unwrap().unwrap();
    let start = rechunk_hooks::cuts();
    let lengths: Vec<usize> = scan_chunks(&table, ScanRequest::default())
        .await
        .iter()
        .map(|c| c.len())
        .collect();
    assert_eq!(lengths, [3_000], "a child the batch holds is one chunk");
    assert_eq!(rechunk_hooks::cuts() - start, 0, "and is cut nowhere");
}

/// A slice that fails to cut is its chunk's error item, after the chunks
/// before it, wherever rows are cut into chunks: an in-memory store's row
/// chunks, a file scan's split through both exports, a tailed store's tail,
/// and an index child's table, whose partition still executes and whose
/// stream carries the error.
#[tokio::test]
async fn test_chunks_surface_a_failed_cut() {
    use crate::store::array::rechunk_hooks::FailingCut;

    let hooked = |(chunks, message): (usize, String)| {
        assert!(message.contains("test hook"), "the hook's error: {message}");
        chunks
    };
    let store = built_store(fixture_quads(), LayoutStrategy::Dictionary, vec![]).await;
    {
        let _fail = FailingCut::after(2);
        let before = hooked(chunks_before_error(store.row_chunks(7).unwrap()).await);
        assert_eq!(before, 2, "in memory: the third chunk fails");
    }
    {
        let (_dir, mapped) = multi_chunk_file().await;
        let fail = FailingCut::after(2);
        let before = hooked(chunks_before_error(mapped.row_chunks(7).unwrap()).await);
        assert_eq!(before, 2, "a file scan's row chunks");
        drop(fail);
        let _fail = FailingCut::after(2);
        let codes = mapped.code_chunks(&QuadColumn::ALL, 7).unwrap();
        assert_eq!(
            hooked(chunks_before_error(codes).await),
            2,
            "a file scan's code chunks"
        );
    }
    let tailed = built_store(fixture_quads(), LayoutStrategy::Default, vec![])
        .await
        .add_quads(appended_quads(40))
        .await
        .unwrap();
    {
        // 3,000 base rows make 150 chunks of 20, the tail's 40 two more.
        let _fail = FailingCut::after(151);
        let before = hooked(chunks_before_error(tailed.row_chunks(20).unwrap()).await);
        assert_eq!(before, 151, "the tail's second chunk fails");
    }
    let indexed = indexed_multi_chunk_store().await;
    let table = indexed
        .component_data_source("index:posg")
        .unwrap()
        .unwrap();
    let _fail = FailingCut::after(1);
    let mut partitions = table
        .scan(ScanRequest::default())
        .await
        .unwrap()
        .partitions();
    let partition = partitions.next().await.unwrap().unwrap();
    let stream = partition.execute().expect("the partition executes");
    assert_eq!(
        hooked(chunks_before_error(stream).await),
        1,
        "an index child's table: its second chunk fails"
    );
}

/// In memory, over every index set, the views of a store with tombstones:
/// the exports and the data source's scan skip the deleted rows, and the
/// source counts the live ones exactly. Every view, the whole store's
/// included, gathers one window of its live ids per chunk, of only the
/// columns read.
#[tokio::test]
async fn test_chunks_skip_tombstones() {
    let quads = fixture_quads();
    for indexes in [
        vec![],
        vec![IndexType::SecondaryByCopy],
        vec![IndexType::SecondaryByReference],
    ] {
        let store = built_store(quads.clone(), LayoutStrategy::Dictionary, indexes.clone()).await;
        let deleted = delete_all(&store, quads.iter().step_by(7).take(50)).await;
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
    assert_eq!(
        drained_code_rows::<4>(&code_chunks),
        expected,
        "code_chunks"
    );
    assert_eq!(struct_chunk_codes(&row_chunks), expected, "row_chunks");
}

/// A file view's chunk stream chooses its read ([`ExactRead::of`]) once, on
/// its first pull, whichever read it is: the take of sparse exact ids, the
/// point read of a point-sized filtered run, and the scan of an unrestricted
/// view, through both exports. With the split limit forced to zero a scan
/// spawns its splits, which tells the scan apart from the reads by id.
///
/// [`ExactRead::of`]: crate::store::scan::read_plan::ExactRead::of
#[tokio::test]
async fn test_chunks_choose_a_file_read_once() {
    use crate::store::Keep;
    use crate::store::scan::{
        file_reads::take_hooks, file_scans::driver_hooks, read_plan::exact_read_hooks,
    };

    let (_dir, path) = write_store_file(
        modular_quads(20_000, 3, 4),
        LayoutStrategy::Dictionary,
        vec![],
    )
    .await;
    let file = VortexRdfStore::from_file(&path).await.unwrap();
    let dict = file.dict_reader().unwrap();
    let (lo, hi) = dict.prefix_range("<http://example.org/s").await.unwrap();
    let sparse = file
        .keep(QuadColumn::S, &Keep::set((lo..hi).step_by(20)))
        .await
        .unwrap();
    let (_runs_dir, runs_path) =
        write_store_file(term_kind_quads(), LayoutStrategy::Dictionary, vec![]).await;
    let runs = VortexRdfStore::from_file(&runs_path).await.unwrap();
    let s = iri_subject("s");
    let plain = plain_literal("plain");
    let point_run = matched_view(&runs, Some(&s), None, Some(&plain), None).await;

    // (view, taken by id, scanned)
    let reads = [
        ("sparse ids, taken", &sparse, true, false),
        (
            "point-sized run, read point by point",
            &point_run,
            false,
            false,
        ),
        ("unrestricted view, scanned", &file, false, true),
    ];
    let _limit = driver_hooks::ForcedLimit::set(0);
    for (tag, view, taken, scanned) in reads {
        let takes = take_hooks::takes().0;
        driver_hooks::take_spawned();
        let codes = view.code_chunks(&QuadColumn::ALL, 256).unwrap();
        let (chunks, first, total) = drain_counted(codes, exact_read_hooks::chosen).await;
        assert!(!chunks.is_empty(), "{tag}: code_chunks read rows");
        assert_eq!(
            (first, total),
            (1, 1),
            "{tag}: code_chunks chooses its read once"
        );
        let rows = view.row_chunks(256).unwrap();
        let (_, first, total) = drain_counted(rows, exact_read_hooks::chosen).await;
        assert_eq!(
            (first, total),
            (1, 1),
            "{tag}: row_chunks chooses its read once"
        );
        assert_eq!(take_hooks::takes().0 > takes, taken, "{tag}: taken by id");
        assert_eq!(driver_hooks::take_spawned() > 0, scanned, "{tag}: scanned");
    }
}

/// A subject run on a sorted file whose bound role past a gap in the
/// `(s, p, o, g)` order stays a pending filter — `<s> ?p "plain"`, and
/// `GRAPH <g> { <s> <p2> ?o }` — is point-sized, so its code chunks read it
/// by id even when they project only an unbound role. With the split limit
/// forced to zero a scan would spawn its splits; nothing spawns, and the
/// chunks are the view's base-ordered codes.
#[tokio::test]
async fn test_chunks_read_filtered_file_runs_by_id() {
    use crate::store::scan::file_scans::driver_hooks;

    let (_dir, path) =
        write_store_file(term_kind_quads(), LayoutStrategy::Dictionary, vec![]).await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let s = iri_subject("s");
    let p2 = iri("p2");
    let plain = plain_literal("plain");
    let g = iri_graph("g");
    let views = [
        (
            "<s> ?p \"plain\"",
            matched_view(&store, Some(&s), None, Some(&plain), None).await,
            QuadColumn::P,
            1,
        ),
        (
            "GRAPH <g> { <s> <p2> ?o }",
            matched_view(&store, Some(&s), Some(&p2), None, Some(&g)).await,
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
#[tokio::test]
async fn test_chunks_read_a_file_outside_a_tokio_runtime() {
    use crate::store::scan::file_scans::driver_hooks;

    let (_dir, path) = write_store_file(fixture_quads(), LayoutStrategy::Dictionary, vec![]).await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let p1 = iri("p1");
    // A pending filter: both exports scan the file.
    let view = matched_view(&store, None, Some(&p1), None, None).await;
    let expected = base_ordered_codes(&view).await;
    assert_eq!(expected.len(), 600);

    let (codes, rows, scanned, spawned) = std::thread::spawn(move || {
        let _limit = driver_hooks::ForcedLimit::set(0);
        futures::executor::block_on(async {
            driver_hooks::take_spawned();
            let (codes, _) = collect_code_chunks(&view, &QuadColumn::ALL, 256).await;
            let rows = struct_chunk_codes(&collect_row_chunks(&view, 256).await);
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
/// order `quads()` decodes them, through the chunks — the base's rows and the
/// tail's cut apart, each to the batch, from one row a chunk to the view's
/// length — and the data source.
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
        for batch in [1, 7, 64, expected.len(), 100_000] {
            let chunks = collect_row_chunks(&tailed, batch).await;
            assert_eq!(
                chunk_subjects(&chunks),
                expected,
                "{layout:?} at batch {batch}"
            );
            // The base's rows and the tail's are cut apart, each to the cap.
            let lengths: Vec<usize> = cut_lengths(base_rows, batch)
                .chain(cut_lengths(tailed.tail_len(), batch))
                .collect();
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

/// `stream` pulled once and then drained: the splits its first pull spawned,
/// and the rows its items hold (`rows_of`). `before_first_poll` runs once the
/// stream is built and before it is first polled.
async fn pull_and_drain<S: futures::Stream + Unpin>(
    mut stream: S,
    before_first_poll: impl FnOnce(),
    rows_of: impl Fn(S::Item) -> usize,
) -> (usize, usize) {
    use crate::store::scan::file_scans::driver_hooks;
    before_first_poll();
    driver_hooks::take_spawned();
    let first = stream.next().await.expect("a first item");
    let spawned = driver_hooks::take_spawned();
    let mut rows = rows_of(first);
    while let Some(item) = stream.next().await {
        rows += rows_of(item);
    }
    (spawned, rows)
}

/// The chunk streams over a filtered no-index file view (`code_chunks`,
/// `row_chunks`, `quads()` and the data source's scan) stay lazy and scan:
/// nothing is evaluated or scanned before the first poll, each drained stream
/// runs one row scan and never reads the filter's masks first, and with the
/// split limit forced to zero a first pull has at most a scan's window of
/// splits spawned, four per unit of available parallelism, on a file of more
/// splits than that. The row-chunk streams spawn through the crate's driver,
/// which counts each spawn; `quads()` reads through vortex's own scan stream,
/// whose window the count does not see.
#[tokio::test]
async fn test_chunk_stream_over_filtered_file_stays_lazy() {
    use crate::io::read::available_parallelism;
    use crate::store::scan::file_filter::split_hooks;
    use crate::store::scan::{
        file_scans::{driver_hooks, scan_hooks},
        read_plan::masked_read_hooks,
    };

    let (_dir, path) = many_splits_file().await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let p0 = iri("p0");
    let view = matched_view(&store, None, Some(&p0), None, None).await;
    assert!(view.view_statistics().unwrap().pending_filter);
    let n = view.size().await.unwrap();
    let window = 4 * available_parallelism();
    let _limit = driver_hooks::ForcedLimit::set(0);
    for read in ["code_chunks", "row_chunks", "quads", "data_source"] {
        let (scans, evaluated) = (scan_hooks::scans(), split_hooks::evaluated());
        let masked = masked_read_hooks::reads();
        let lazy = || {
            assert_eq!(
                scan_hooks::scans(),
                scans,
                "{read}: no scan before the first poll"
            );
            assert_eq!(
                split_hooks::evaluated(),
                evaluated,
                "{read}: no split evaluated before the first poll"
            );
        };
        let (spawned, rows) = match read {
            "code_chunks" => {
                let chunks = view.code_chunks(&QuadColumn::ALL, 256).unwrap();
                pull_and_drain(chunks, lazy, |chunk| chunk.unwrap()[0].len()).await
            }
            "row_chunks" => {
                let chunks = view.row_chunks(256).unwrap();
                pull_and_drain(chunks, lazy, |chunk| chunk.unwrap().len()).await
            }
            "quads" => {
                pull_and_drain(view.quads(), lazy, |quad| {
                    quad.unwrap();
                    1
                })
                .await
            }
            _ => {
                let source = view.data_source().unwrap();
                let scan = source.scan(ScanRequest::default()).await.unwrap();
                let mut partitions = scan.partitions();
                let partition = partitions.next().await.unwrap().unwrap();
                assert!(partitions.next().await.is_none(), "one partition");
                let chunks = partition.execute().unwrap();
                pull_and_drain(chunks, lazy, |chunk| chunk.unwrap().len()).await
            }
        };
        assert_eq!(rows, n, "{read}: every row");
        assert!(
            spawned <= window,
            "{read}: {spawned} splits spawned by a first pull"
        );
        if read != "quads" {
            assert_eq!(spawned, window.min(MANY_SPLITS), "{read}: a scan's window");
        }
        assert_eq!(scan_hooks::scans() - scans, 1, "{read}: one row scan");
        assert_eq!(
            split_hooks::evaluated(),
            evaluated,
            "{read}: no masks evaluated"
        );
        assert_eq!(masked_read_hooks::reads(), masked, "{read}: no masked read");
    }
}
