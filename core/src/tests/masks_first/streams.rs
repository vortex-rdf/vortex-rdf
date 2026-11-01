//! The chunk streams over a filtered no-index file view take its pending filter by its masks first, a split at a time
//! as the stream reaches it: over a code held only by the last split's rows, a code no row holds, a filter whose rows
//! are all deleted, and filters that keep many rows.

use super::*;

/// `rows` quads `s{i:05} p{i % 3} "o{i % 4}"` in the default graph, except
/// the last three, whose object is `"rare"`, as a Dictionary file of 32-row
/// leaves with no index: the rare object's rows in the last split only.
async fn rare_tail_file(rows: usize) -> (tempfile::TempDir, std::path::PathBuf) {
    let quads: Vec<Quad> = (0..rows)
        .map(|i| {
            let o = if i + 3 >= rows {
                "rare".to_string()
            } else {
                format!("o{}", i % 4)
            };
            make_quad(
                &format!("http://example.org/s{i:05}"),
                &format!("http://example.org/p{}", i % 3),
                &o,
                GraphName::DefaultGraph,
            )
        })
        .collect();
    write_chunked_store_file(&quads, 32).await
}

#[tokio::test]
async fn test_masked_streams_of_rare_and_absent_codes() {
    let (_dir, path) = rare_tail_file(8_192).await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    // Held only by the last split's rows: the whole file is evaluated before
    // the first chunk, which holds them.
    let rare = matched_view(&store, None, None, Some(&plain_literal("rare")), None).await;
    let mut rec = Recorder::start();
    let mut chunks = rare.code_chunks(&QuadColumn::ALL, 256).unwrap();
    let first = chunks.next().await.unwrap().unwrap();
    assert_eq!(first[0].len(), 3, "the rare rows in the first chunk");
    assert!(chunks.next().await.is_none());
    assert_eq!(rec.take_count(&Event::MasksFirstStream), 1);
    // A dictionary code no quad holds as an object (an IRI only predicates
    // hold, which the object column's zones rule out): no chunk, no read.
    let p0 = Term::NamedNode(iri("p0"));
    let absent = matched_view(&store, None, None, Some(&p0), None).await;
    rec.take();
    let mut chunks = absent.code_chunks(&QuadColumn::ALL, 256).unwrap();
    assert!(chunks.next().await.is_none(), "no chunk");
    assert!(masked_reads(&mut rec).is_empty(), "no read of kept rows");
    // Every row the filter keeps deleted: the masked streams are taken and
    // their masks keep nothing, so no batch yields a chunk or reads a row.
    let tombstoned = store
        .delete_matching(None, None, Some(&plain_literal("rare")), None)
        .await
        .unwrap();
    assert_eq!(
        tombstoned.size().await.unwrap(),
        store.size().await.unwrap() - 3
    );
    let gone = matched_view(&tombstoned, None, None, Some(&plain_literal("rare")), None).await;
    for batch_rows in [1, 256, 65_536] {
        rec.take();
        let mut codes = gone.code_chunks(&QuadColumn::ALL, batch_rows).unwrap();
        assert!(
            codes.next().await.is_none(),
            "no code chunk, batch {batch_rows}"
        );
        let mut rows = gone.row_chunks(batch_rows).unwrap();
        assert!(
            rows.next().await.is_none(),
            "no row chunk, batch {batch_rows}"
        );
        assert_eq!(
            rec.count(&Event::MasksFirstStream),
            2,
            "both streams masked, batch {batch_rows}"
        );
        assert!(
            masked_reads(&mut rec).is_empty(),
            "no read of kept rows, batch {batch_rows}"
        );
    }
}

#[tokio::test]
async fn test_masked_streams_are_taken() {
    let (_dir, path) = stock_file(LayoutStrategy::Dictionary, IndexConfig::NoIndex).await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let (p2, o3) = (iri("p2"), plain_literal("object 3"));
    for (name, view) in [
        ("P", matched_view(&store, None, Some(&p2), None, None).await),
        ("O", matched_view(&store, None, None, Some(&o3), None).await),
    ] {
        // No index serves the view: both streams read in base order.
        let want = base_ordered_codes(&view).await;
        for batch_rows in [1, 256, 65_536] {
            let mut rec = Recorder::start();
            let (rows, _) = collect_code_chunks(&view, &QuadColumn::ALL, batch_rows).await;
            assert_eq!(
                rec.take_count(&Event::MasksFirstStream),
                1,
                "{name}: code_chunks at {batch_rows}"
            );
            assert_eq!(rows, want, "{name}: code_chunks rows at {batch_rows}");
            let chunks = collect_row_chunks(&view, batch_rows).await;
            assert_eq!(
                rec.take_count(&Event::MasksFirstStream),
                1,
                "{name}: row_chunks at {batch_rows}"
            );
            assert_eq!(
                struct_chunk_codes(&chunks),
                want,
                "{name}: row_chunks rows at {batch_rows}"
            );
        }
    }
}
