//! The row-id limit of an indexed store. An index child records each row's
//! id as a u32, so a store with secondary indexes holds at most `u32::MAX`
//! quads; every build path that numbers rows — the out-of-core merge, the
//! in-memory index builds, the mutation and compaction rebuilds — and every
//! adoption of index children refuses a store past that, instead of letting
//! an id wrap. The [`RowLimit`] hook lowers the limit, so each refusal is
//! reached with a handful of quads.

use super::*;
use crate::store::indexes::{
    MAX_INDEXED_ROWS, check_adopted_rows, check_indexed_rows, next_row_id,
};
use crate::store::test_hooks::RowLimit;

/// The lowered limit the build-path tests run under.
const LIMIT: usize = 5;

/// `n` distinct quads.
fn quads(n: usize) -> Vec<Quad> {
    modular_quads(n, 2, 3)
}

fn indexes() -> Indexes {
    vec![IndexType::SecondaryByReference, IndexType::SecondaryByCopy]
}

/// `result` is a build's refusal of a store past [`LIMIT`] quads.
fn assert_refused<T>(result: crate::error::Result<T>, label: &str) {
    let Err(err) = result else {
        panic!("{label}: a store past the row limit must be refused");
    };
    let message = err.to_string();
    assert!(
        matches!(err, VortexRdfError::Serialization(_))
            && message.contains("would exceed 5 quads")
            && message.contains("u32"),
        "{label}: {message}"
    );
}

/// `result` is the refusal to read a store that already holds
/// `LIMIT + 1` quads: nothing is built, so the store is unreadable, not
/// unwritable.
fn assert_unreadable<T>(result: crate::error::Result<T>, label: &str) {
    let Err(err) = result else {
        panic!("{label}: a store past the row limit must be refused");
    };
    let message = err.to_string();
    assert!(
        matches!(err, VortexRdfError::Deserialization(_))
            && message.contains("the store holds 6 quads, more than this version")
            && message.contains("at most 5 quads")
            && message.contains("u32"),
        "{label}: {message}"
    );
}

/// The checked increment counts up to the limit and refuses the row past
/// it, leaving the count where it was; the up-front check admits the limit
/// itself and nothing more.
#[test]
fn row_ids_count_to_the_limit_and_refuse_the_row_past_it() {
    let _limit = RowLimit::set(3);
    let mut rows = 0;
    for want in 0..3 {
        assert_eq!(next_row_id(&mut rows).unwrap(), want);
    }
    assert_eq!(rows, 3);
    assert!(next_row_id(&mut rows).is_err());
    assert_eq!(rows, 3, "a refused row is not counted");
    assert!(check_indexed_rows(3).is_ok());
    assert!(check_indexed_rows(4).is_err());
    assert!(check_adopted_rows(3).is_ok());
    assert!(check_adopted_rows(4).is_err());
}

/// Without the hook the limit is `u32::MAX` quads: the last row id a store
/// assigns is `u32::MAX - 1`, and the build's and the reader's refusals
/// name the limit in full.
#[test]
fn the_format_limit_is_u32_max_quads() {
    assert_eq!(MAX_INDEXED_ROWS, u64::from(u32::MAX));
    assert!(check_indexed_rows(MAX_INDEXED_ROWS).is_ok());
    let message = check_indexed_rows(MAX_INDEXED_ROWS + 1)
        .unwrap_err()
        .to_string();
    assert!(
        message.contains("would exceed 4,294,967,295 quads (4,294,967,296 quads)"),
        "{message}"
    );
    assert!(check_adopted_rows(MAX_INDEXED_ROWS).is_ok());
    let message = check_adopted_rows(MAX_INDEXED_ROWS + 1)
        .unwrap_err()
        .to_string();
    assert!(
        message.contains("holds 4,294,967,296 quads, more than this version")
            && message.contains("at most 4,294,967,295 quads"),
        "{message}"
    );
    let mut rows = MAX_INDEXED_ROWS - 1;
    assert_eq!(next_row_id(&mut rows).unwrap(), u32::MAX - 1);
    let message = next_row_id(&mut rows).unwrap_err().to_string();
    assert!(
        message.contains("would exceed 4,294,967,295 quads"),
        "{message}"
    );
}

/// The in-memory index builds — the sorted in-memory builder under every
/// layout, the interning sink — refuse past the limit and build at it; a
/// store without indexes assigns no row ids and is not limited.
#[tokio::test]
async fn in_memory_index_builds_refuse_past_the_row_limit() {
    let _limit = RowLimit::set(LIMIT as u64);
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
        assert_eq!(
            VortexRdfStore::from_built(at)
                .unwrap()
                .size()
                .await
                .unwrap(),
            LIMIT
        );
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
/// checked increment refuses the store at the first row past the limit,
/// under the code and the string layouts, to a file (which is then left
/// absent) and to an in-memory array alike.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
#[tokio::test]
async fn the_out_of_core_merge_refuses_past_the_row_limit() {
    let _limit = RowLimit::set(LIMIT as u64);
    for layout in [LayoutStrategy::Default, LayoutStrategy::Dictionary] {
        let label = format!("{layout:?}");
        let over =
            build_array::<SortedStreamBuilder>(quad_stream(quads(LIMIT + 1)), layout, indexes())
                .await;
        assert_refused(over, &label);
        let at = build_array::<SortedStreamBuilder>(quad_stream(quads(LIMIT)), layout, indexes())
            .await
            .unwrap();
        assert_eq!(
            VortexRdfStore::from_built(at)
                .unwrap()
                .size()
                .await
                .unwrap(),
            LIMIT
        );
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

/// The rebuilds — compacting an append into an indexed store, serializing
/// a store with an append tail — refuse a store grown past the limit; the
/// store they start from is untouched.
#[tokio::test]
async fn rebuilds_refuse_a_store_grown_past_the_row_limit() {
    let _limit = RowLimit::set(LIMIT as u64);
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
    assert_eq!(store.size().await.unwrap(), LIMIT);
}

/// Adopting index children over a base past the limit is refused, whoever
/// built the parts — `from_parts`, `from_built` — and so is opening a file
/// whose index children would have to address it, mapped or loaded. The
/// store already holds the rows, so each is a read error.
#[tokio::test]
async fn adoption_refuses_index_children_past_the_row_limit() {
    // Built under the format's limit, adopted under the lowered one.
    let built = build_array::<SortedInMemoryBuilder>(
        quad_stream(quads(LIMIT + 1)),
        LayoutStrategy::Dictionary,
        indexes(),
    )
    .await
    .unwrap();
    let parts = VortexRdfStore::from_built(built.clone())
        .unwrap()
        .to_serializable_parts()
        .await
        .unwrap();
    #[cfg(feature = "file-io")]
    let bytes = VortexRdfStore::from_built(built.clone())
        .unwrap()
        .to_bytes()
        .await
        .unwrap();

    let _limit = RowLimit::set(LIMIT as u64);
    assert_unreadable(VortexRdfStore::from_parts(parts), "from_parts");
    assert_unreadable(VortexRdfStore::from_built(built), "from_built");
    #[cfg(feature = "file-io")]
    {
        assert_unreadable(VortexRdfStore::from_bytes(&bytes).await, "from_bytes");
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("store.vortex");
        std::fs::write(&path, &bytes).unwrap();
        assert_unreadable(VortexRdfStore::from_file(&path).await, "from_file");
        assert_unreadable(
            VortexRdfStore::from_file_in_memory(&path).await,
            "from_file_in_memory",
        );
    }
}
