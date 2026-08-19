//! Layout-feature test: chunk-probe bounds over a written vortex file must
//! agree with the canonical `partition_point` floor across chunk boundaries.
#![cfg(feature = "layout")]

use vortex_array::arrays::{PrimitiveArray, StructArray};
use vortex_array::scalar_fn::session::ScalarFnSession;
use vortex_array::session::ArraySession;
use vortex_array::stream::ArrayStreamAdapter;
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray};
use vortex_encoded_search::ColumnChunks;
use vortex_file::{OpenOptionsSessionExt as _, WriteOptionsSessionExt as _};
use vortex_io::session::RuntimeSession;
use vortex_layout::session::LayoutSession;
use vortex_session::VortexSession;

fn session() -> VortexSession {
    use vortex_io::session::RuntimeSessionExt as _;
    let session = VortexSession::empty()
        .with::<ArraySession>()
        .with::<LayoutSession>()
        .with::<ScalarFnSession>()
        .with::<RuntimeSession>()
        .with_tokio();
    vortex_file::register_default_encodings(&session);
    session
}

fn chunk(data: &[u32]) -> ArrayRef {
    let s = PrimitiveArray::from_iter(data.iter().copied()).into_array();
    StructArray::try_new(["s"].into(), vec![s], data.len(), Validity::NonNullable)
        .unwrap()
        .into_array()
}

#[tokio::test(flavor = "multi_thread")]
async fn layout_chunks_probe_matches_canonical() {
    let session = session();

    // Sorted with 11-row runs, sized so the write strategy's coalescing still
    // yields multiple flat chunks; an equal run crosses every input boundary.
    let data: Vec<u32> = (0..600_000).map(|i| (i / 11) as u32).collect();
    let chunks: Vec<ArrayRef> = data.chunks(200_000).map(chunk).collect();
    let dtype = chunks[0].dtype().clone();

    let mut bytes = Vec::new();
    session
        .write_options()
        .write(
            &mut bytes,
            ArrayStreamAdapter::new(dtype, futures::stream::iter(chunks.into_iter().map(Ok))),
        )
        .await
        .unwrap();

    let file = session.open_options().open_buffer(bytes).unwrap();
    let root = file.footer().layout().clone();

    let column =
        ColumnChunks::from_struct_layout(&root, "s").expect("the written shape must resolve");
    assert!(ColumnChunks::from_struct_layout(&root, "absent").is_none());
    assert_eq!(column.row_count(), data.len() as u64);

    let source = file.segment_source();
    let canonical = |needle: u64| {
        let lo = data.partition_point(|&v| u64::from(v) < needle) as u64;
        let hi = data.partition_point(|&v| u64::from(v) <= needle) as u64;
        lo..hi.max(lo)
    };

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
        assert_eq!(got, canonical(needle), "needle {needle}");
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
            let lo = wdata.partition_point(|&v| u64::from(v) < needle) as u64;
            let hi = wdata.partition_point(|&v| u64::from(v) <= needle) as u64;
            assert_eq!(
                got,
                window.start + lo..window.start + hi,
                "window {window:?} needle {needle}"
            );
        }
    }
}
