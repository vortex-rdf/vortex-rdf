//! Chunked exports — `row_chunks` and `code_chunks` — against the whole-view
//! reads, on every backend, over narrowed, served, tombstoned and tailed
//! views.

use super::*;
use crate::store::{QuadColumn, TermCode};

/// Both exports of `view` agree with its base-ordered codes at several
/// batch sizes, and a column subset comes in the requested order.
async fn assert_chunks_match(view: &VortexRdfStore, tag: &str) {
    let expected = base_ordered_codes(view).await;
    for batch in [1, 7, 256, 100_000] {
        let (rows, sizes) = collect_code_chunks(view, &QuadColumn::ALL, batch).await;
        let rows: Vec<[TermCode; 4]> = rows.iter().map(|r| [r[0], r[1], r[2], r[3]]).collect();
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
    let want: Vec<Vec<TermCode>> = expected.iter().map(|r| vec![r[2], r[0]]).collect();
    assert_eq!(rows, want, "{tag}: a column subset in the requested order");
}

/// In memory, over every index set: each view's chunks are its base-ordered
/// codes, and the whole store's code chunks are its code columns, not
/// copies of them.
#[tokio::test]
async fn test_chunks_in_memory_match_gathered() {
    for indexes in [
        vec![],
        vec![IndexType::SecondaryByCopy],
        vec![IndexType::SecondaryByReference],
    ] {
        let store = built_store(fixture_quads(), LayoutStrategy::Dictionary, indexes.clone()).await;
        for (name, view) in views_of(&store).await {
            assert_chunks_match(&view, &format!("memory {indexes:?} {name}")).await;
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
}

#[tokio::test]
async fn test_chunks_skip_tombstones() {
    let quads = fixture_quads();
    let store = built_store(
        quads.clone(),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
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
    for (tag, _dir, store) in file_stores().await {
        for (name, view) in views_of(&store).await {
            assert_chunks_match(&view, &format!("{tag} {name}")).await;
        }
    }
}

/// A file view of exact ids past the point-read cap and sparse in their
/// split — every 20th subject of 20,000 — is read by taking its ids, as the
/// whole-view gather is: each export takes through the chunk probes and
/// answers the codes of the same keep on the store loaded whole.
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

    let (through_chunks, _) = take_hooks::takes();
    let (rows, _) = collect_code_chunks(&view, &QuadColumn::ALL, 256).await;
    let (after_codes, _) = take_hooks::takes();
    let row_rows = chunk_codes(&collect_row_chunks(&view, 256).await);
    let (after_rows, _) = take_hooks::takes();
    assert_eq!(after_codes, through_chunks + 1, "code_chunks takes its ids");
    assert_eq!(after_rows, after_codes + 1, "row_chunks takes its ids");
    let rows: Vec<[TermCode; 4]> = rows.iter().map(|r| [r[0], r[1], r[2], r[3]]).collect();
    assert_eq!(rows, expected, "code_chunks");
    assert_eq!(row_rows, expected, "row_chunks");
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
        let expected: Vec<Vec<TermCode>> = base_ordered_codes(&view)
            .await
            .iter()
            .map(|row| vec![row[column.index()]])
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

/// A file view's chunks read with no tokio runtime: a bare executor on a
/// plain thread polls them, with the split limit forced to zero so the scan
/// would spawn wherever a runtime were there to spawn onto. Nothing is
/// spawned, and both exports answer the view's base-ordered codes.
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

    let (codes, rows, spawned) = std::thread::spawn(move || {
        let _limit = driver_hooks::ForcedLimit::set(0);
        futures::executor::block_on(async {
            driver_hooks::take_spawned();
            let (codes, _) = collect_code_chunks(&view, &QuadColumn::ALL, 256).await;
            let rows = chunk_codes(&collect_row_chunks(&view, 256).await);
            (codes, rows, driver_hooks::take_spawned())
        })
    })
    .join()
    .expect("a chunk stream must not panic without a tokio runtime");
    assert_eq!(spawned, 0, "no runtime to spawn onto, so nothing spawned");
    let codes: Vec<[TermCode; 4]> = codes.iter().map(|r| [r[0], r[1], r[2], r[3]]).collect();
    assert_eq!(codes, expected, "code_chunks");
    assert_eq!(rows, expected, "row_chunks");
}

/// The string layouts stream their native struct rows, the tail last, in the
/// order `quads()` decodes them.
#[tokio::test]
async fn test_chunks_string_layouts_include_tail() {
    let quads = fixture_quads();
    let appended = appended_quads(40);
    for layout in [LayoutStrategy::Default, LayoutStrategy::TypedObject] {
        let store = built_store(quads.clone(), layout, vec![]).await;
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
            matches!(
                tailed.code_chunks(&QuadColumn::ALL, 10),
                Err(VortexRdfError::InvalidOperation(_))
            ),
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
    let store = built_store(fixture_quads(), LayoutStrategy::Dictionary, vec![]).await;
    let refused = |result: crate::error::Result<_>| {
        matches!(result, Err(VortexRdfError::InvalidOperation(_)))
    };
    assert!(refused(store.row_chunks(0).map(drop)));
    assert!(refused(store.code_chunks(&QuadColumn::ALL, 0).map(drop)));
    assert!(refused(store.code_chunks(&[], 10).map(drop)));
    let tailed = store.add_quads(appended_quads(3)).await.unwrap();
    assert_eq!(tailed.tail_len(), 3);
    assert!(
        refused(tailed.row_chunks(10).map(drop)),
        "a tailed Dictionary view has no single dtype"
    );
    assert!(refused(tailed.code_chunks(&QuadColumn::ALL, 10).map(drop)));
}
