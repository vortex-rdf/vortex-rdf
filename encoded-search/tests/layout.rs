//! Layout-feature test: chunk-probe bounds over a written vortex file must
//! agree with the canonical `partition_point` floor across chunk boundaries.

mod common;

use std::sync::Arc;

use common::{canonical_bounds, writer_session};
use vortex_array::arrays::{PrimitiveArray, StructArray};
use vortex_array::stream::ArrayStreamAdapter;
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray};
use vortex_file::{OpenOptionsSessionExt as _, WriteOptionsSessionExt as _};
use vortex_layout::layouts::chunked::Chunked as ChunkedLayout;
use vortex_layout::layouts::chunked::writer::ChunkedLayoutStrategy;
use vortex_layout::layouts::dict::Dict as DictLayout;
use vortex_layout::layouts::dict::writer::{
    DictLayoutConstraints, DictLayoutOptions, DictStrategy,
};
use vortex_layout::layouts::flat::writer::FlatLayoutStrategy;
use vortex_layout::layouts::repartition::{RepartitionStrategy, RepartitionWriterOptions};
use vortex_layout::layouts::struct_::StructStrategy;
use vortex_layout::{LayoutChildType, LayoutRef, LayoutStrategy};
use vortex_rdf_encoded_search::ColumnChunks;
use vortex_session::VortexSession;

/// A struct chunk over the given arrays, all of one length.
fn struct_of(columns: Vec<(&str, ArrayRef)>) -> ArrayRef {
    let len = columns[0].1.len();
    let (names, arrays): (Vec<&str>, Vec<ArrayRef>) = columns.into_iter().unzip();
    StructArray::try_new(names.into(), arrays, len, Validity::NonNullable)
        .unwrap()
        .into_array()
}

/// A struct chunk over non-nullable `u32` columns.
fn struct_chunk(columns: &[(&str, &[u32])]) -> ArrayRef {
    struct_of(
        columns
            .iter()
            .map(|(name, data)| {
                (
                    *name,
                    PrimitiveArray::from_iter(data.iter().copied()).into_array(),
                )
            })
            .collect(),
    )
}

/// Writes `chunks` as one file (through `strategy` when given) and returns
/// the opened file.
async fn write_file(
    session: &VortexSession,
    chunks: Vec<ArrayRef>,
    strategy: Option<Arc<dyn LayoutStrategy>>,
) -> vortex_file::VortexFile {
    let dtype = chunks[0].dtype().clone();
    let mut options = session.write_options();
    if let Some(strategy) = strategy {
        options = options.with_strategy(strategy);
    }
    let mut bytes = Vec::new();
    options
        .write(
            &mut bytes,
            ArrayStreamAdapter::new(dtype, futures::stream::iter(chunks.into_iter().map(Ok))),
        )
        .await
        .unwrap();
    session.open_options().open_buffer(bytes).unwrap()
}

/// A field's data node beneath its zoned wrappers, for shape assertions.
fn data_node(root: &LayoutRef, field: &str) -> LayoutRef {
    use vortex_layout::layouts::zoned::Zoned;
    let mut node = (0..root.nslots())
        .find_map(|i| {
            matches!(root.slot_type(i), Some(LayoutChildType::Field(ref name)) if name.as_ref() == field)
                .then(|| root.slot(i).ok().flatten())
                .flatten()
        })
        .expect("field present");
    while node.is::<Zoned>() {
        node = node.slot(0).unwrap().unwrap();
    }
    node
}

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
        Arc::new(BtrBlocksCompressorBuilder::default().build());
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
/// probes address rows across dictionary boundaries exactly.
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
