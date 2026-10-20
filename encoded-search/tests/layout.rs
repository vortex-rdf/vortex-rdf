//! Layout-feature test: chunk-probe bounds over a written vortex file must
//! agree with the canonical `partition_point` floor across chunk boundaries.

mod common;

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use common::{
    canonical_bounds, data_node, struct_chunk, struct_of, taken_u32, write_file, writer_session,
};
use vortex_array::arrays::PrimitiveArray;
use vortex_array::{ArrayRef, IntoArray};
use vortex_error::VortexResult;
use vortex_layout::layouts::chunked::Chunked as ChunkedLayout;
use vortex_layout::layouts::chunked::writer::ChunkedLayoutStrategy;
use vortex_layout::layouts::dict::Dict as DictLayout;
use vortex_layout::layouts::dict::writer::{
    DictLayoutConstraints, DictLayoutOptions, DictStrategy,
};
use vortex_layout::layouts::flat::writer::FlatLayoutStrategy;
use vortex_layout::layouts::repartition::{RepartitionStrategy, RepartitionWriterOptions};
use vortex_layout::layouts::struct_::StructStrategy;
use vortex_layout::segments::{SegmentFuture, SegmentId, SegmentSinkRef, SegmentSource};
use vortex_layout::sequence::{SendableSequentialStream, SequencePointer};
use vortex_layout::{LayoutRef, LayoutStrategy, LayoutWriterContext};
use vortex_rdf_encoded_search::{ColumnChunks, fetch_flat, flat_leaves};
use vortex_session::VortexSession;

/// A dictionary strategy with the given codes and values strategies and a
/// `max_len`-bounded dictionary.
fn dict_strategy<C: LayoutStrategy, V: LayoutStrategy>(
    codes: C,
    values: V,
    max_len: u16,
) -> DictStrategy {
    use vortex_btrblocks::BtrBlocksCompressorBuilder;
    use vortex_layout::layouts::compressed::CompressorPlugin;
    let compressor: Arc<dyn CompressorPlugin> =
        Arc::new(BtrBlocksCompressorBuilder::from_session(&common::session()).build());
    DictStrategy::new(
        codes,
        values,
        FlatLayoutStrategy::default(),
        DictLayoutOptions {
            constraints: DictLayoutConstraints {
                max_len,
                ..Default::default()
            },
        },
        compressor,
    )
}

fn struct_strategy<S: LayoutStrategy>(field: S) -> Arc<dyn LayoutStrategy> {
    Arc::new(StructStrategy::new(
        Arc::new(FlatLayoutStrategy::default()),
        Arc::new(field),
    ))
}

/// A segment source that forwards each request to `inner`, counting the
/// requests made for every segment.
struct CountingSource {
    inner: Arc<dyn SegmentSource>,
    requests: Mutex<HashMap<SegmentId, usize>>,
}

impl CountingSource {
    fn new(inner: Arc<dyn SegmentSource>) -> Self {
        Self {
            inner,
            requests: Mutex::default(),
        }
    }

    /// The requests made for `id` so far.
    fn requests(&self, id: SegmentId) -> usize {
        self.requests.lock().unwrap().get(&id).copied().unwrap_or(0)
    }
}

impl SegmentSource for CountingSource {
    fn request(&self, id: SegmentId) -> SegmentFuture {
        *self.requests.lock().unwrap().entry(id).or_default() += 1;
        self.inner.request(id)
    }
}

/// A strategy handing its inner one the column's chunks with a zero-row
/// chunk after the first. The file writer drops empty input chunks before
/// any strategy sees them, so only a strategy beneath it can pass one on;
/// vortex's chunked writer then keeps it as a zero-row child.
struct EmptyChunkAfterFirst<S>(S);

impl<S: LayoutStrategy> LayoutStrategy for EmptyChunkAfterFirst<S> {
    fn write_stream<'life0, 'life1, 'async_trait>(
        &'life0 self,
        ctx: LayoutWriterContext,
        segment_sink: SegmentSinkRef,
        stream: SendableSequentialStream,
        eof: SequencePointer,
        session: &'life1 VortexSession,
    ) -> Pin<Box<dyn Future<Output = VortexResult<LayoutRef>> + Send + 'async_trait>>
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        Self: 'async_trait,
    {
        use futures::StreamExt as _;
        use vortex_layout::sequence::{
            SequentialStream as _, SequentialStreamAdapter, SequentialStreamExt as _,
        };

        let dtype = stream.dtype().clone();
        let mut first = true;
        let stream = stream.flat_map(move |item| {
            let items = match item {
                Ok((id, chunk)) if std::mem::take(&mut first) => {
                    let mut ids = id.descend();
                    let (chunk_id, empty_id) = (ids.advance(), ids.advance());
                    let empty = chunk.slice(0..0).map(|empty| (empty_id, empty));
                    vec![Ok((chunk_id, chunk)), empty]
                }
                other => vec![other],
            };
            futures::stream::iter(items)
        });
        let stream = SequentialStreamAdapter::new(dtype, stream).sendable();
        Box::pin(async move {
            self.0
                .write_stream(ctx, segment_sink, stream, eof, session)
                .await
        })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn column_chunks_match_canonical() {
    let session = writer_session();

    // Sorted with 11-row runs, fed as three input chunks so the column spans
    // several flat leaves; an equal run crosses every input boundary.
    let data: Vec<u32> = (0..600_000).map(|i| (i / 11) as u32).collect();
    let chunks: Vec<ArrayRef> = data
        .chunks(200_000)
        .map(|c| struct_chunk(&[("s", c)]))
        .collect();
    let file = write_file(&session, chunks, None).await;
    let root = file.footer().layout().clone();
    let node = data_node(&root, "s");
    assert!(
        node.is::<ChunkedLayout>() && node.nslots() >= 2,
        "fixture must span several leaves: {}",
        node.display_tree()
    );

    let column =
        ColumnChunks::from_struct_layout(&root, "s").expect("the written shape must resolve");
    assert_eq!(column.row_count(), data.len() as u64);

    let source = file.segment_source();
    let max = u64::from(*data.last().unwrap());
    let mut needles: Vec<u64> = vec![0, 1, max, max + 1, u64::MAX];
    // Every ~997th distinct value and its neighbors, plus the values at the
    // input-chunk boundaries (where equal runs span chunks).
    needles.extend(
        (0..max)
            .step_by(997)
            .flat_map(|v| [v.saturating_sub(1), v, v + 1]),
    );
    for boundary in [200_000u64, 400_000] {
        let v = u64::from(data[boundary as usize]);
        needles.extend([v - 1, v, v + 1]);
    }
    for needle in needles {
        let got = column
            .bounds(needle, &source, &session)
            .await
            .unwrap()
            .expect("all chunks are probeable");
        assert_eq!(got, canonical_bounds(&data, needle), "needle {needle}");
        assert_eq!(
            column.lower_bound(needle, &source, &session).await.unwrap(),
            Some(got.start),
            "lower bound of {needle}"
        );
    }

    // Point reads across chunk interiors and boundaries need no sort order
    // and must agree with the source data exactly.
    let mut rows: Vec<u64> = (0..data.len() as u64).step_by(9973).collect();
    rows.extend([0, 199_999, 200_000, 399_999, 400_000, data.len() as u64 - 1]);
    for row in rows {
        let got = column
            .value_at(row, &source, &session)
            .await
            .unwrap()
            .expect("all chunks are probeable");
        assert_eq!(got, u64::from(data[row as usize]), "row {row}");
    }

    // Windowed bounds: search inside sub-ranges (including ones spanning a
    // chunk boundary) and compare against the canonical floor of the window.
    for window in [0u64..50_000, 150_000..250_000, 380_000..420_000, 5..6, 9..9] {
        let wdata = &data[window.start as usize..window.end as usize];
        for needle in [0u64, 3000, 13_600, 18_181, 36_363, u64::MAX] {
            let got = column
                .bounds_in(window.clone(), needle, &source, &session)
                .await
                .unwrap()
                .expect("all chunks are probeable");
            let want = canonical_bounds(wdata, needle);
            assert_eq!(
                got,
                window.start + want.start..window.start + want.end,
                "window {window:?} needle {needle}"
            );
        }
    }
    // A reversed window holds no row: at `u64::MAX` too, its bounds are
    // empty at its start.
    #[allow(clippy::reversed_empty_ranges)]
    let reversed = 9..5;
    assert_eq!(
        column
            .bounds_in(reversed, u64::MAX, &source, &session)
            .await
            .unwrap(),
        Some(9..9),
        "reversed window at u64::MAX"
    );
}

/// Only non-nullable unsigned-integer fields of the struct resolve.
#[tokio::test(flavor = "multi_thread")]
async fn column_chunks_decline_unsupported_fields() {
    let session = writer_session();
    let n = 4_000usize;
    let chunk = struct_of(vec![
        (
            "s",
            PrimitiveArray::from_iter((0..n).map(|i| i as u32)).into_array(),
        ),
        (
            "n",
            PrimitiveArray::from_option_iter((0..n).map(|i| (i % 3 != 0).then_some(i as u32)))
                .into_array(),
        ),
        (
            "i",
            PrimitiveArray::from_iter((0..n).map(|i| i as i32)).into_array(),
        ),
    ]);
    let file = write_file(&session, vec![chunk], None).await;
    let root = file.footer().layout().clone();

    assert!(ColumnChunks::from_struct_layout(&root, "s").is_some());
    assert!(ColumnChunks::from_struct_layout(&root, "n").is_none());
    assert!(ColumnChunks::from_struct_layout(&root, "i").is_none());
    assert!(ColumnChunks::from_struct_layout(&root, "absent").is_none());
}

/// A column reports the flat leaves it is cut into and how many a probe has
/// fetched, and its `Debug` prints the same two numbers. A zero-row chunk is
/// no leaf.
#[tokio::test(flavor = "multi_thread")]
async fn column_chunks_count_their_leaves() {
    let session = writer_session();
    let data: Vec<u32> = (0..600_000).map(|i| (i / 11) as u32).collect();
    let chunks: Vec<ArrayRef> = data
        .chunks(200_000)
        .map(|c| struct_chunk(&[("s", c)]))
        .collect();
    let file = write_file(&session, chunks.clone(), None).await;
    let root = file.footer().layout().clone();
    let column = ColumnChunks::from_struct_layout(&root, "s").expect("the shape must resolve");
    let leaves = column.chunk_count();
    assert!(
        leaves >= 3,
        "the fixture must span several leaves: {leaves}"
    );
    assert_eq!(
        column.fetched_chunks(),
        0,
        "nothing is fetched at construction"
    );

    let source = file.segment_source();
    column.value_at(0, &source, &session).await.unwrap();
    assert_eq!(column.fetched_chunks(), 1);
    column.value_at(1, &source, &session).await.unwrap();
    assert_eq!(column.fetched_chunks(), 1, "a leaf is fetched once");
    column
        .value_at(data.len() as u64 - 1, &source, &session)
        .await
        .unwrap();
    assert_eq!(column.fetched_chunks(), 2);
    assert_eq!(column.chunk_count(), leaves);

    let debug = format!("{column:?}");
    assert!(
        debug.contains(&format!("chunks: {leaves}")) && debug.contains("fetched: 2"),
        "{debug}"
    );

    // The same leaves tile the column in row order, and a fetched leaf holds
    // exactly its own rows.
    let flat = flat_leaves(&data_node(&root, "s")).expect("the shape resolves");
    assert_eq!(flat.len(), leaves);
    let mut next = 0;
    for leaf in &flat {
        assert_eq!((leaf.row_offset, leaf.dict_values.is_none()), (next, true));
        next += leaf.layout.row_count();
    }
    assert_eq!(next, data.len() as u64);
    let last = flat.last().unwrap();
    let rows = fetch_flat(&last.layout, &source, &session).await.unwrap();
    assert_eq!(taken_u32(rows, &session), &data[last.row_offset as usize..]);

    // A chunked layout holding a zero-row chunk, which vortex's chunked
    // writer keeps when a strategy hands it one: the empty chunk has no leaf,
    // the leaves still tile the column, and its bounds and point reads are
    // exact.
    let empty_after_first =
        EmptyChunkAfterFirst(ChunkedLayoutStrategy::new(FlatLayoutStrategy::default()));
    let file = write_file(&session, chunks, Some(struct_strategy(empty_after_first))).await;
    let root = file.footer().layout().clone();
    let node = data_node(&root, "s");
    let chunk_rows: Vec<u64> = (0..node.nslots())
        .map(|i| node.slot(i).unwrap().unwrap().row_count())
        .collect();
    assert!(
        node.is::<ChunkedLayout>() && chunk_rows.contains(&0),
        "the fixture must keep a zero-row chunk: {}",
        node.display_tree()
    );
    let flat = flat_leaves(&node).expect("the shape resolves");
    assert_eq!(flat.len(), node.nslots() - 1, "the empty chunk has no leaf");
    let mut next = 0;
    for leaf in &flat {
        assert_eq!(leaf.row_offset, next);
        next += leaf.layout.row_count();
    }
    assert_eq!(next, data.len() as u64);
    let column = ColumnChunks::from_struct_layout(&root, "s").expect("the shape resolves");
    assert_eq!(column.chunk_count(), flat.len());
    let source = file.segment_source();
    for needle in [0u64, 1, 18_181, 36_363, u64::MAX] {
        assert_eq!(
            column.bounds(needle, &source, &session).await.unwrap(),
            Some(canonical_bounds(&data, needle)),
            "needle {needle}"
        );
    }
    for row in [0, 199_999, 200_000, data.len() as u64 - 1] {
        assert_eq!(
            column.value_at(row, &source, &session).await.unwrap(),
            Some(u64::from(data[row as usize])),
            "row {row}"
        );
    }
}

/// A zero-row column resolves to a column with no leaves and answers empty
/// bounds without fetching.
#[tokio::test(flavor = "multi_thread")]
async fn column_chunks_empty_column() {
    let session = writer_session();
    let file = write_file(&session, vec![struct_chunk(&[("s", &[])])], None).await;
    let root = file.footer().layout().clone();
    let column = ColumnChunks::from_struct_layout(&root, "s").expect("an empty column resolves");
    assert_eq!(column.row_count(), 0);
    let source = file.segment_source();
    assert_eq!(
        column.bounds(5, &source, &session).await.unwrap(),
        Some(0..0)
    );
    assert_eq!(
        column.bounds_in(0..0, 5, &source, &session).await.unwrap(),
        Some(0..0)
    );
}

/// A leaf whose wire encoding the probe does not support (sparse) declines
/// with `Ok(None)` at query time, while the column itself still resolves.
#[tokio::test(flavor = "multi_thread")]
async fn column_chunks_decline_unsupported_leaf() {
    use vortex_array::scalar::Scalar;
    use vortex_sparse::Sparse;

    let session = writer_session();
    let data: Vec<u32> = (0..4_096).map(|i| i / 5).collect();
    // A few explicit values over a constant fill: still non-decreasing, so
    // only the encoding — not the data — is what the probe declines.
    let indices: Vec<u64> = vec![4_093, 4_094, 4_095];
    let values: Vec<u32> = indices.iter().map(|&i| data[i as usize]).collect();
    let sparse = Sparse::try_new(
        PrimitiveArray::from_iter(indices).into_array(),
        PrimitiveArray::from_iter(values).into_array(),
        data.len(),
        Scalar::from(0u32),
    )
    .unwrap()
    .into_array();
    assert_eq!(sparse.encoding_id().as_str(), "vortex.sparse");
    // The flat strategy writes the chunk in the encoding it arrives in.
    let strategy = struct_strategy(FlatLayoutStrategy::default());
    let file = write_file(
        &session,
        vec![struct_of(vec![("s", sparse)])],
        Some(strategy),
    )
    .await;
    let root = file.footer().layout().clone();

    let column = ColumnChunks::from_struct_layout(&root, "s").expect("a flat leaf resolves");
    assert_eq!(column.row_count(), data.len() as u64);
    let source = file.segment_source();
    assert_eq!(column.bounds(7, &source, &session).await.unwrap(), None);
    assert_eq!(column.value_at(7, &source, &session).await.unwrap(), None);
    assert_eq!(
        column
            .bounds_in(0..100, 7, &source, &session)
            .await
            .unwrap(),
        None
    );
    // A leaf whose encoding declined was fetched all the same.
    assert_eq!((column.chunk_count(), column.fetched_chunks()), (1, 1));
}

/// Dictionary layouts whose shape the probe does not support decline at
/// construction: a dictionary nested inside another's codes, and a values
/// child that is not a single flat leaf.
#[tokio::test(flavor = "multi_thread")]
async fn column_chunks_reject_unsupported_dict_shapes() {
    let session = writer_session();
    let data: Vec<u32> = (0..20_000).map(|i| (i / 500) as u32).collect();
    let chunk = struct_chunk(&[("s", &data)]);

    // Codes strategy that is itself a dictionary strategy.
    let nested = dict_strategy(
        dict_strategy(
            FlatLayoutStrategy::default(),
            FlatLayoutStrategy::default(),
            u16::MAX,
        ),
        FlatLayoutStrategy::default(),
        u16::MAX,
    );
    let file = write_file(&session, vec![chunk.clone()], Some(struct_strategy(nested))).await;
    let root = file.footer().layout().clone();
    let node = data_node(&root, "s");
    assert!(
        node.is::<DictLayout>() && node.slot(1).unwrap().unwrap().is::<DictLayout>(),
        "fixture must nest a dictionary inside the codes: {}",
        node.display_tree()
    );
    assert!(ColumnChunks::from_struct_layout(&root, "s").is_none());

    // Values strategy that yields a chunked values child: the values are
    // repartitioned into 8-row blocks under a chunked layout.
    let chunked_values = dict_strategy(
        FlatLayoutStrategy::default(),
        RepartitionStrategy::new(
            ChunkedLayoutStrategy::new(FlatLayoutStrategy::default()),
            RepartitionWriterOptions {
                block_size_minimum: 1,
                block_len_multiple: 8,
                block_size_target: None,
                canonicalize: false,
            },
        ),
        u16::MAX,
    );
    let file = write_file(&session, vec![chunk], Some(struct_strategy(chunked_values))).await;
    let root = file.footer().layout().clone();
    let node = data_node(&root, "s");
    assert!(
        node.is::<DictLayout>() && node.slot(0).unwrap().unwrap().is::<ChunkedLayout>(),
        "fixture must chunk the values leaf: {}",
        node.display_tree()
    );
    assert!(ColumnChunks::from_struct_layout(&root, "s").is_none());
}

/// A column the default write strategy dictionary-encodes at the layout
/// level — its first block holds only a few distinct values — probes through
/// the codes leaves and the shared values leaf: global bounds over the sorted
/// lead column, point reads anywhere, and windowed bounds over a second
/// column sorted only within each lead run.
#[tokio::test(flavor = "multi_thread")]
async fn column_chunks_probe_dictionary_coded_columns() {
    let session = writer_session();
    // Three lead runs of 3,000 rows; the second column restarts inside each.
    let p: Vec<u32> = (0..9_000).map(|i| (i / 3_000) as u32).collect();
    let o: Vec<u32> = (0..9_000).map(|i| ((i % 3_000) / 300) as u32).collect();
    let file = write_file(&session, vec![struct_chunk(&[("p", &p), ("o", &o)])], None).await;
    let root = file.footer().layout().clone();
    for field in ["p", "o"] {
        assert!(
            data_node(&root, field).is::<DictLayout>(),
            "the writer no longer dictionary-encodes `{field}`; the fixture must change to keep covering the dict layout"
        );
    }

    let source = file.segment_source();
    let pc = ColumnChunks::from_struct_layout(&root, "p").expect("a dict layout resolves");
    let oc = ColumnChunks::from_struct_layout(&root, "o").expect("a dict layout resolves");
    assert_eq!(pc.row_count(), 9_000);
    assert_eq!(oc.row_count(), 9_000);

    for needle in [0u64, 1, 2, 3, 17, u64::MAX] {
        let got = pc
            .bounds(needle, &source, &session)
            .await
            .unwrap()
            .expect("dictionary-coded leaves are probeable");
        assert_eq!(got, canonical_bounds(&p, needle), "needle {needle}");
    }

    let mut rows: Vec<u64> = (0..9_000u64).step_by(97).collect();
    rows.extend([0, 2_999, 3_000, 5_999, 6_000, 8_999]);
    for row in rows {
        for (column, data) in [(&pc, &p), (&oc, &o)] {
            let got = column
                .value_at(row, &source, &session)
                .await
                .unwrap()
                .expect("dictionary-coded leaves are probeable");
            assert_eq!(got, u64::from(data[row as usize]), "row {row}");
        }
    }

    for run in 0..3u64 {
        let window = run * 3_000..(run + 1) * 3_000;
        let wdata = &o[window.start as usize..window.end as usize];
        for needle in (0..=10u64).chain([u64::MAX]) {
            let got = oc
                .bounds_in(window.clone(), needle, &source, &session)
                .await
                .unwrap()
                .expect("dictionary-coded leaves are probeable");
            let want = canonical_bounds(wdata, needle);
            assert_eq!(
                got,
                window.start + want.start..window.start + want.end,
                "window {window:?} needle {needle}"
            );
        }
    }
}

/// A column whose dictionary outgrows its constraints becomes a chunked run
/// of dictionary layouts, each with its own values leaf and chunked codes;
/// probes address rows across dictionary boundaries exactly, fetching each
/// dictionary's values once for all its codes leaves.
#[tokio::test(flavor = "multi_thread")]
async fn column_chunks_probe_run_of_dictionaries() {
    let session = writer_session();
    // Forty distinct values of 500 rows each, fed as four input chunks; a
    // dictionary holds at most sixteen values, so the column becomes three
    // dictionaries whose codes leaves follow the input chunking.
    let data: Vec<u32> = (0..20_000).map(|i| (i / 500) as u32).collect();
    let chunks: Vec<ArrayRef> = data
        .chunks(5_000)
        .map(|c| struct_chunk(&[("s", c)]))
        .collect();
    let dict = dict_strategy(
        ChunkedLayoutStrategy::new(FlatLayoutStrategy::default()),
        FlatLayoutStrategy::default(),
        16,
    );
    let file = write_file(&session, chunks, Some(struct_strategy(dict))).await;
    let root = file.footer().layout().clone();
    let node = data_node(&root, "s");
    assert!(
        node.is::<ChunkedLayout>() && node.nslots() == 3,
        "expected a chunked run of three dictionaries, got {}",
        node.display_tree()
    );
    for i in 0..node.nslots() {
        assert!(node.slot(i).unwrap().unwrap().is::<DictLayout>());
    }
    // Every leaf holds codes, and each dictionary's leaves are adjacent and
    // name its one values leaf.
    let leaves = flat_leaves(&node).expect("a run of dictionaries resolves");
    let mut values = Vec::new();
    for leaf in &leaves {
        values.push(
            leaf.dict_values
                .as_ref()
                .expect("a codes leaf")
                .segment_id(),
        );
    }
    values.dedup();
    assert_eq!(values.len(), 3);
    assert!(
        leaves.len() > values.len(),
        "a dictionary spans several codes leaves"
    );

    // Probing every codes leaf requests each one's segment once, and each
    // dictionary's values segment once, shared by its codes leaves.
    let counting = Arc::new(CountingSource::new(file.segment_source()));
    let counted: Arc<dyn SegmentSource> = counting.clone();
    let probed =
        ColumnChunks::from_struct_layout(&root, "s").expect("a run of dictionaries resolves");
    for leaf in &leaves {
        assert_eq!(
            probed
                .value_at(leaf.row_offset, &counted, &session)
                .await
                .unwrap(),
            Some(u64::from(data[leaf.row_offset as usize]))
        );
    }
    for leaf in &leaves {
        assert_eq!(counting.requests(leaf.layout.segment_id()), 1);
    }
    for id in values {
        assert_eq!(counting.requests(id), 1, "values segment {id:?}");
    }

    let source = file.segment_source();
    let column =
        ColumnChunks::from_struct_layout(&root, "s").expect("a run of dictionaries resolves");
    assert_eq!(column.row_count(), 20_000);

    for needle in (0..=40u64).chain([100, u64::MAX]) {
        let got = column
            .bounds(needle, &source, &session)
            .await
            .unwrap()
            .expect("dictionary-coded leaves are probeable");
        assert_eq!(got, canonical_bounds(&data, needle), "needle {needle}");
    }

    // Rows around every dictionary boundary (values 15→16 and 31→32) and the
    // input chunk boundaries, plus a stride over the interior.
    let mut rows: Vec<u64> = (0..20_000u64).step_by(251).collect();
    rows.extend([
        0, 4_999, 5_000, 7_999, 8_000, 9_999, 10_000, 15_999, 16_000, 19_999,
    ]);
    for row in rows {
        let got = column
            .value_at(row, &source, &session)
            .await
            .unwrap()
            .expect("dictionary-coded leaves are probeable");
        assert_eq!(got, u64::from(data[row as usize]), "row {row}");
    }

    for window in [7_000u64..9_000, 15_500..16_500, 0..20_000, 4_900..5_100] {
        let wdata = &data[window.start as usize..window.end as usize];
        for needle in [0u64, 14, 15, 16, 17, 31, 32, 39, 40, u64::MAX] {
            let got = column
                .bounds_in(window.clone(), needle, &source, &session)
                .await
                .unwrap()
                .expect("dictionary-coded leaves are probeable");
            let want = canonical_bounds(wdata, needle);
            assert_eq!(
                got,
                window.start + want.start..window.start + want.end,
                "window {window:?} needle {needle}"
            );
        }
    }
}

/// A take at ascending rows answers exactly the values at those rows, each
/// leaf taking its own — across leaf boundaries, from run-end and
/// dictionary-coded columns alike; no row, an empty array.
#[tokio::test(flavor = "multi_thread")]
async fn column_chunks_take_rows_across_leaves() {
    let session = writer_session();
    let data: Vec<u32> = (0..600_000).map(|i| (i / 11) as u32).collect();
    let chunks: Vec<ArrayRef> = data
        .chunks(200_000)
        .map(|c| struct_chunk(&[("s", c)]))
        .collect();
    let file = write_file(&session, chunks, None).await;
    let root = file.footer().layout().clone();
    let column = ColumnChunks::from_struct_layout(&root, "s").expect("the shape resolves");
    let source = file.segment_source();
    let mut rows: Vec<u64> = (0..data.len() as u64).step_by(7919).collect();
    rows.extend([199_999, 200_000, 399_999, 400_000, data.len() as u64 - 1]);
    rows.sort_unstable();
    rows.dedup();
    let taken = column
        .take(&rows, &source, &session)
        .await
        .unwrap()
        .expect("all chunks are probeable");
    let want: Vec<u32> = rows.iter().map(|&r| data[r as usize]).collect();
    assert_eq!(taken_u32(taken, &session), want);
    let none = column
        .take(&[], &source, &session)
        .await
        .unwrap()
        .expect("no row needs no chunk");
    assert_eq!(none.len(), 0);

    let p: Vec<u32> = (0..9_000).map(|i| (i / 3_000) as u32).collect();
    let o: Vec<u32> = (0..9_000).map(|i| ((i % 3_000) / 300) as u32).collect();
    let file = write_file(&session, vec![struct_chunk(&[("p", &p), ("o", &o)])], None).await;
    let root = file.footer().layout().clone();
    let source = file.segment_source();
    let rows: Vec<u64> = (0..9_000u64).step_by(37).chain([8_999]).collect();
    for (field, data) in [("p", &p), ("o", &o)] {
        let column =
            ColumnChunks::from_struct_layout(&root, field).expect("a dict layout resolves");
        let taken = column
            .take(&rows, &source, &session)
            .await
            .unwrap()
            .expect("dictionary-coded leaves are probeable");
        let want: Vec<u32> = rows.iter().map(|&r| data[r as usize]).collect();
        assert_eq!(taken_u32(taken, &session), want, "{field}");
    }
}
