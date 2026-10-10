//! Row ids never wrap. An index child records each row's id as a u64, so no
//! store reaches the top of the id space — but every build path that numbers
//! rows (the out-of-core merge, the in-memory index builds, the mutation and
//! compaction rebuilds) still numbers them with checked arithmetic and
//! refuses a store whose ids would run past the largest one, rather than let
//! an id wrap onto row 0. The [`RowIdBase`] hook starts the ids [`LIMIT`]
//! short of the top, so each refusal is reached with a handful of quads, and
//! a store numbered right up to the top still reads back.

use super::*;
use crate::store::RowId;
use crate::store::indexes::{check_indexed_rows, index_of, next_row_id, row_id};
use crate::store::test_hooks::RowIdBase;

/// How many row ids the build-path tests leave below the top.
const LIMIT: usize = 5;

/// The base that leaves exactly [`LIMIT`] ids: the last one assigned is
/// `RowId::MAX - 1`.
const TOP: RowId = RowId::MAX - LIMIT as RowId;

/// `n` distinct quads.
fn quads(n: usize) -> Vec<Quad> {
    modular_quads(n, 2, 3)
}

fn indexes() -> Indexes {
    vec![IndexType::SecondaryByReference, IndexType::SecondaryByCopy]
}

/// `result` is a build's refusal of a store whose row ids would run past the
/// top, numbered from [`TOP`].
fn assert_refused<T>(result: crate::error::Result<T>, label: &str) {
    let Err(err) = result else {
        panic!("{label}: a store whose row ids would wrap must be refused");
    };
    let message = err.to_string();
    assert!(
        matches!(err, VortexRdfError::Serialization(_))
            && message.contains("would exceed 5 quads")
            && message.contains("u64"),
        "{label}: {message}"
    );
}

/// A store numbered up to the top answers its index matches: the predicate
/// `p1` covers the odd rows.
async fn assert_reads_back(store: &VortexRdfStore, rows: usize, label: &str) {
    let quads = quads(rows);
    assert_eq!(store.size().await.unwrap(), rows, "{label}");
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let matched = store
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    assert_eq!(
        view_strings(&matched).await,
        expected_strings(&quads, |i| i % 2 == 1),
        "{label}"
    );
}

/// The checked increment counts up to the last row id and refuses the row
/// past it, leaving the count where it was; the up-front check admits as
/// many rows as there are ids and no more; and the in-memory numbering
/// offsets by the base.
#[test]
fn row_ids_count_to_the_top_and_refuse_the_row_past_it() {
    let mut rows = RowId::MAX - 2;
    assert_eq!(next_row_id(&mut rows).unwrap(), RowId::MAX - 2);
    assert_eq!(next_row_id(&mut rows).unwrap(), RowId::MAX - 1);
    assert_eq!(rows, RowId::MAX);
    let message = next_row_id(&mut rows).unwrap_err().to_string();
    assert_eq!(rows, RowId::MAX, "a refused row is not counted");
    assert!(
        message.contains("would exceed 18,446,744,073,709,551,615 quads"),
        "{message}"
    );
    assert!(check_indexed_rows(RowId::MAX).is_ok());
    assert_eq!(row_id(7), 7);

    let _base = RowIdBase::set(TOP);
    assert!(check_indexed_rows(LIMIT as u64).is_ok());
    let message = check_indexed_rows(LIMIT as u64 + 1)
        .unwrap_err()
        .to_string();
    assert!(
        message.contains("would exceed 5 quads (6 quads)"),
        "{message}"
    );
    assert_eq!(row_id(LIMIT - 1), RowId::MAX - 1);
    let mut rows = 0;
    for want in TOP..RowId::MAX {
        assert_eq!(next_row_id(&mut rows).unwrap(), want);
    }
    assert!(next_row_id(&mut rows).is_err());
    assert_eq!(rows, LIMIT as u64);
}

/// A row id becomes a position only where it fits the index type: on a
/// 32-bit target (wasm) an id past `u32::MAX` is refused, never narrowed
/// onto a row of the base.
#[test]
fn a_row_id_past_the_index_width_is_refused_not_narrowed() {
    assert_eq!(index_of::<u32>(RowId::from(u32::MAX)).unwrap(), u32::MAX);
    for past in [1 << 32, (1 << 32) + 5, RowId::MAX] {
        let message = index_of::<u32>(past).unwrap_err().to_string();
        assert!(message.contains(&past.to_string()), "{message}");
    }
    assert_eq!(
        index_of::<usize>(RowId::MAX).ok(),
        usize::try_from(RowId::MAX).ok()
    );
}

/// The in-memory index builds — the sorted in-memory builder under every
/// layout, the interning sink — refuse a store whose ids would run past the
/// top and build one that ends at it; a store without indexes numbers no
/// rows and is not limited.
#[tokio::test]
async fn in_memory_index_builds_refuse_row_ids_past_the_top() {
    let _base = RowIdBase::set(TOP);
    for layout in [
        LayoutStrategy::Default,
        LayoutStrategy::TypedObject,
        LayoutStrategy::Dictionary,
    ] {
        let label = format!("{layout:?}");
        let over =
            build_array::<SortedInMemoryBuilder>(quad_stream(quads(LIMIT + 1)), layout, indexes())
                .await;
        assert_refused(over, &label);
        let at = build_array::<SortedInMemoryBuilder>(quad_stream(quads(LIMIT)), layout, indexes())
            .await
            .unwrap();
        assert_reads_back(&VortexRdfStore::from_built(at).unwrap(), LIMIT, &label).await;
        let unindexed =
            build_array::<SortedInMemoryBuilder>(quad_stream(quads(LIMIT + 1)), layout, vec![])
                .await
                .unwrap();
        assert_eq!(
            VortexRdfStore::from_built(unindexed)
                .unwrap()
                .size()
                .await
                .unwrap(),
            LIMIT + 1
        );
    }

    let mut sink = DictionaryQuadSink::new(indexes());
    for quad in quads(LIMIT + 1) {
        sink.push(crate::store::RawQuad::from_quad(&quad));
    }
    assert_refused(sink.finish(), "interning sink");
}

/// The out-of-core builder numbers rows as its merge emits them: the
/// checked increment refuses the store at the first row past the top, under
/// the code and the string layouts, to a file (which is then left absent)
/// and to an in-memory array alike.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
#[tokio::test]
async fn the_out_of_core_merge_refuses_row_ids_past_the_top() {
    let _base = RowIdBase::set(TOP);
    for layout in [LayoutStrategy::Default, LayoutStrategy::Dictionary] {
        let label = format!("{layout:?}");
        let over =
            build_array::<SortedStreamBuilder>(quad_stream(quads(LIMIT + 1)), layout, indexes())
                .await;
        assert_refused(over, &label);
        let at = build_array::<SortedStreamBuilder>(quad_stream(quads(LIMIT)), layout, indexes())
            .await
            .unwrap();
        assert_reads_back(&VortexRdfStore::from_built(at).unwrap(), LIMIT, &label).await;
    }

    #[cfg(feature = "file-io")]
    {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("over.vortex");
        let written = crate::io::quads_stream_to_vortex_file(
            quad_stream(quads(LIMIT + 1)),
            &path,
            LayoutStrategy::Dictionary,
            indexes(),
        )
        .await;
        assert_refused(written, "file");
        assert!(!path.exists(), "a refused build leaves no file");
        let at = dir.path().join("at.vortex");
        crate::io::quads_stream_to_vortex_file(
            quad_stream(quads(LIMIT)),
            &at,
            LayoutStrategy::Dictionary,
            indexes(),
        )
        .await
        .unwrap();
        assert_reads_back(
            &VortexRdfStore::from_file(&at).await.unwrap(),
            LIMIT,
            "file",
        )
        .await;
        let unindexed = dir.path().join("unindexed.vortex");
        crate::io::quads_stream_to_vortex_file(
            quad_stream(quads(LIMIT + 1)),
            &unindexed,
            LayoutStrategy::Dictionary,
            vec![],
        )
        .await
        .unwrap();
    }
}

/// The rebuilds — compacting an append into an indexed store, serializing a
/// store with an append tail — refuse a store grown past the top; the store
/// they start from is untouched.
#[tokio::test]
async fn rebuilds_refuse_a_store_grown_past_the_top() {
    let _base = RowIdBase::set(TOP);
    let mut all = quads(LIMIT + 1);
    let extra = all.pop().unwrap();
    let store = VortexRdfStore::from_quads(quad_stream(all), LayoutStrategy::Dictionary, indexes())
        .await
        .unwrap();
    let grown = store.add_quad(extra).await.unwrap();
    assert_eq!(
        grown.size().await.unwrap(),
        LIMIT + 1,
        "the append itself is a tail"
    );
    assert_refused(grown.compact().await, "compaction");
    #[cfg(feature = "file-io")]
    assert_refused(grown.to_bytes().await, "serialization of the tail");
    assert_reads_back(&store, LIMIT, "the store grown from").await;
}
