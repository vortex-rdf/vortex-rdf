//! The public pieces the bindings compose a chunked read from (a code pattern's probe against the store's own
//! dictionary, a string-layout row chunk decoded as shared quads), and the chunk streams themselves: every stream
//! yields its view's rows in the order the view's reads do, over every index set, residency, tombstone and window,
//! and a run a by-copy index serves streams off the index.

use std::collections::BTreeSet;

use super::*;
use crate::store::{CodePattern, QuadColumn, TermCode};
use oxrdf::BlankNode;
use vortex_buffer::Buffer;

/// The stock modular dataset plus one quad with a blank subject, so that every term kind has a code: 201
/// quads in a Dictionary store with the secondary-by-copy index, which locates the runs of a bound `p` or `o`.
async fn coded_store() -> (tempfile::TempDir, VortexRdfStore) {
    let mut quads = modular_quads(200, 3, 5);
    quads.push(Quad::new(
        NamedOrBlankNode::BlankNode(BlankNode::new("b0").unwrap()),
        iri("p0"),
        plain_literal("object 0"),
        GraphName::DefaultGraph,
    ));
    let (dir, path) = write_store_file(
        quads,
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    (dir, VortexRdfStore::from_file(&path).await.unwrap())
}

/// A match's rows as a set of code rows, and how many rows it has.
fn code_row_set(columns: &[Buffer<TermCode>; 4]) -> (BTreeSet<[TermCode; 4]>, usize) {
    let rows = (0..columns[0].len())
        .map(|row| std::array::from_fn(|c| columns[c][row]))
        .collect();
    (rows, columns[0].len())
}

#[tokio::test]
async fn code_probe_answers_what_match_code_pattern_answers() {
    let (_dir, store) = coded_store().await;
    let all = store
        .match_code_pattern(CodePattern::default(), 0, None)
        .await
        .unwrap();
    for row in (0..all[0].len()).step_by(37) {
        let quad: [TermCode; 4] = std::array::from_fn(|c| all[c][row]);
        for mask in 0..16u8 {
            let slot = |i: usize| (mask & (1 << i) != 0).then_some(quad[i]);
            let pattern = CodePattern::new(slot(0), slot(1), slot(2), slot(3));
            let probe = store
                .code_probe(pattern)
                .await
                .unwrap()
                .expect("a stored quad's codes can match");
            let got = store
                .run_probe(&probe)
                .await
                .unwrap()
                .code_columns_gathered()
                .await
                .unwrap()
                .expect("a Dictionary view's codes");
            let want = store.match_code_pattern(pattern, 0, None).await.unwrap();
            assert_eq!(code_row_set(&got), code_row_set(&want), "{pattern:?}");
        }
    }
}

#[tokio::test]
async fn code_probe_is_none_where_nothing_can_match() {
    let (_dir, store) = coded_store().await;
    let kinds = store.dict_reader().unwrap().kind_ranges().await.unwrap();
    assert!(!kinds.literals.is_empty() && !kinds.blanks.is_empty());
    let past = store.dict_reader().unwrap().len() as TermCode + 3;
    for pattern in [
        CodePattern::new(Some(kinds.literals.start), None, None, None),
        CodePattern::new(None, Some(kinds.blanks.start), None, None),
        CodePattern::new(None, None, Some(past), None),
    ] {
        assert!(
            store.code_probe(pattern).await.unwrap().is_none(),
            "{pattern:?}"
        );
        assert_eq!(
            store.count_code_pattern(pattern).await.unwrap(),
            0,
            "{pattern:?}"
        );
    }
}

#[tokio::test]
async fn code_probe_refuses_a_store_without_a_dictionary() {
    let (_dir, path) =
        write_store_file(modular_quads(20, 3, 5), LayoutStrategy::Default, vec![]).await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let error = store
        .code_probe(CodePattern::default())
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("no term dictionary"), "{error}");
}

/// Every row chunk of `view` at `batch_rows`, each decoded with `decode_row_chunk`, in order.
async fn decoded_chunks(view: &VortexRdfStore, batch_rows: usize) -> Vec<SharedQuad> {
    let mut chunks = view.row_chunks(batch_rows).unwrap();
    let mut rows = Vec::new();
    while let Some(chunk) = chunks.next().await {
        rows.extend(view.decode_row_chunk(&chunk.unwrap()).unwrap());
    }
    rows
}

#[tokio::test]
async fn decode_row_chunk_equals_shared_quads() {
    let quads = modular_quads(200, 3, 5);
    for layout in [LayoutStrategy::Default, LayoutStrategy::TypedObject] {
        let (_dir, path) = write_store_file(quads.clone(), layout, vec![]).await;
        for open in [Open::Mapped, Open::Loaded] {
            let store = open.open(&path).await.unwrap();
            let p1 = Probe::parse(None, Some("<http://example.org/p1>"), None, None).unwrap();
            let bound = store.run_probe(&p1).await.unwrap();
            for (view, what) in [(&store, "whole"), (&bound, "p-bound")] {
                let want = view.shared_quads_vec().await.unwrap();
                assert!(!want.is_empty());
                for batch_rows in [1, 7, 65_536] {
                    let tag = format!("{layout:?} {open} {what} at {batch_rows}");
                    assert_eq!(decoded_chunks(view, batch_rows).await, want, "{tag}");
                }
            }
        }
    }
}

#[tokio::test]
async fn decode_row_chunk_refuses_a_dictionary_store() {
    let (_dir, path) =
        write_store_file(modular_quads(20, 3, 5), LayoutStrategy::Dictionary, vec![]).await;
    let store = VortexRdfStore::from_file_in_memory(&path).await.unwrap();
    let chunk = store.row_chunks(8).unwrap().next().await.unwrap().unwrap();
    let error = store.decode_row_chunk(&chunk).unwrap_err().to_string();
    assert!(
        error.contains("decode_row_chunk decodes string layouts"),
        "{error}"
    );
}

/// Drains `view`'s `code_chunks` over `columns` at `batch_rows`: the view's rows, in the order its reads yield them
/// ([`read_order_codes`]: read with no serve plan, then put in its index's order).
async fn assert_code_chunks_in_read_order<const N: usize>(
    view: &VortexRdfStore,
    columns: &[QuadColumn; N],
    batch_rows: usize,
    tag: &str,
) {
    let (rows, _) = collect_code_chunks(view, columns, batch_rows).await;
    let order: Vec<[TermCode; N]> = read_order_codes(view)
        .await
        .iter()
        .map(|row| columns.map(|c| row[c.index()]))
        .collect();
    assert_eq!(
        rows, order,
        "{tag}: code_chunks {columns:?} at {batch_rows}"
    );
}

/// [`assert_code_chunks_in_read_order`] for `row_chunks`.
async fn assert_row_chunks_in_read_order(view: &VortexRdfStore, batch_rows: usize, tag: &str) {
    assert_eq!(
        struct_chunk_codes(&collect_row_chunks(view, batch_rows).await),
        read_order_codes(view).await,
        "{tag}: row_chunks at {batch_rows}"
    );
}

/// The Dictionary store file of `modular_quads(20_000, 7, 11)` with `config`'s indexes, written once per process per
/// configuration ([`cached_store_file`]). Keep the `TempDir` alive while the file is used.
async fn matrix_file(config: IndexConfig) -> (tempfile::TempDir, std::path::PathBuf) {
    static FILES: [OnceLock<Vec<u8>>; 3] = [const { OnceLock::new() }; 3];
    let cell = &FILES[IndexConfig::ALL.iter().position(|&c| c == config).unwrap()];
    let quads = || modular_quads(20_000, 7, 11);
    cached_store_file(cell, quads, LayoutStrategy::Dictionary, config.types()).await
}

/// The patterns the stream matrix reads on [`matrix_file`], each beside whether a by-copy index serves it: one
/// subject, a predicate and an object each bound alone, the default graph (every row), the predicate and object
/// together, the subject with its predicate, and nothing bound.
const MATRIX_PATTERNS: [(&str, [Option<&str>; 4], bool); 7] = [
    (
        "S",
        [Some("<http://example.org/s1234>"), None, None, None],
        false,
    ),
    (
        "P",
        [None, Some("<http://example.org/p2>"), None, None],
        true,
    ),
    ("O", [None, None, Some("\"object 3\""), None], true),
    ("G", [None, None, None, Some("")], false),
    (
        "PO",
        [
            None,
            Some("<http://example.org/p2>"),
            Some("\"object 3\""),
            None,
        ],
        true,
    ),
    (
        "SP",
        [
            Some("<http://example.org/s1234>"),
            Some("<http://example.org/p2>"),
            None,
            None,
        ],
        false,
    ),
    ("all", [None; 4], false),
];

/// Every chunk stream yields its view's rows in the order the view's reads do, row for row, across the shapes a view
/// takes: each index set, both residencies, bound positions served or filtered, tombstones, and a window.
#[tokio::test]
async fn chunk_streams_follow_read_order() {
    for config in IndexConfig::ALL {
        let (_dir, path) = matrix_file(config).await;
        for open in [Open::Mapped, Open::Loaded] {
            let store = open.open(&path).await.unwrap();
            // About a fifth of every view's rows tombstoned: p0's and "object 0"'s.
            let tombstoned = store
                .delete_matching(None, Some(&iri("p0")), None, None)
                .await
                .unwrap()
                .delete_matching(None, None, Some(&plain_literal("object 0")), None)
                .await
                .unwrap();
            for (deleted, owner) in [(false, &store), (true, &tombstoned)] {
                for (pattern, [s, p, o, g], copy_serves) in MATRIX_PATTERNS {
                    for window in [None, Some((3, Some(50)))] {
                        let mut probe = Probe::parse(s, p, o, g).unwrap();
                        if let Some((offset, limit)) = window {
                            probe = probe.window(offset, limit);
                        }
                        let view = owner.run_probe(&probe).await.unwrap();
                        let tag = format!(
                            "{} {open} deleted={deleted} {pattern} window={window:?}",
                            config.name()
                        );
                        assert_eq!(
                            served_component(&view).is_some(),
                            copy_serves && config == IndexConfig::Copy,
                            "{tag}: served"
                        );
                        let rows = collect_code_chunks(&view, &QuadColumn::ALL, 65_536)
                            .await
                            .0
                            .len();
                        // One row a chunk is a pull per row: views of at most 256 rows hold the case.
                        for batch_rows in [1, 256, 65_536] {
                            if batch_rows > 1 || rows <= 256 {
                                assert_code_chunks_in_read_order(
                                    &view,
                                    &QuadColumn::ALL,
                                    batch_rows,
                                    &tag,
                                )
                                .await;
                            }
                        }
                        assert_code_chunks_in_read_order(
                            &view,
                            &[QuadColumn::O, QuadColumn::S],
                            256,
                            &tag,
                        )
                        .await;
                        assert_row_chunks_in_read_order(&view, 256, &tag).await;
                    }
                }
            }
        }
    }
}

/// A view's code-chunk stream at `batch_rows`, drained, and whether it was read off its serving index's run.
async fn drain_served(view: &VortexRdfStore, batch_rows: usize) -> (Vec<[TermCode; 4]>, bool) {
    let rec = Recorder::start();
    let (rows, _) = collect_code_chunks(view, &QuadColumn::ALL, batch_rows).await;
    let served = rec.count(&Event::ServedRunChunks);
    assert!(served <= 1, "one stream, one run");
    (rows, served == 1)
}

/// A served file run of any width streams off the index, whatever the batch, and its tombstoned rows are dropped.
#[tokio::test]
async fn served_runs_of_any_width_and_with_tombstones() {
    // p0 holds every even row: a run of 4,096 rows, which chunks of 256 divide and chunks of 255 leave a short last
    // one of; its objects cycle 0, 2, 1, so "object 0" is every third of its rows.
    let (_dir, path) = write_store_file(
        modular_quads(8_192, 2, 3),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let p0 = iri("p0");
    let view = matched_view(&store, None, Some(&p0), None, None).await;
    for batch_rows in [256, 255] {
        let (rows, served) = drain_served(&view, batch_rows).await;
        assert_eq!(rows.len(), 4_096, "at {batch_rows}");
        assert!(served, "at {batch_rows}: read off the index");
        assert_code_chunks_in_read_order(&view, &QuadColumn::ALL, batch_rows, "the run").await;
    }

    let tombstoned = store
        .delete_matching(None, None, Some(&plain_literal("object 0")), None)
        .await
        .unwrap();
    let deleted = store
        .dict_reader()
        .unwrap()
        .encode("\"object 0\"")
        .await
        .unwrap()
        .unwrap();
    let view = matched_view(&tombstoned, None, Some(&p0), None, None).await;
    let (rows, served) = drain_served(&view, 256).await;
    assert!(served, "tombstones keep the index path");
    assert_eq!(
        rows.len(),
        4_096 - 1_366,
        "every third row of the run deleted"
    );
    assert!(rows.iter().all(|row| row[2] != deleted), "no deleted row");
    assert_code_chunks_in_read_order(&view, &QuadColumn::ALL, 256, "tombstoned").await;
}

/// Which runs stream off their index: an in-memory copy's, off its plan, and a point-sized file run, point by point
/// with no scan; a by-reference index's run is read off the base.
#[tokio::test]
async fn which_runs_stream_off_the_index() {
    let (_copy_dir, copy) = matrix_file(IndexConfig::Copy).await;
    let (_reference_dir, reference) = matrix_file(IndexConfig::Reference).await;
    let memory = VortexRdfStore::from_file_in_memory(&copy).await.unwrap();
    let reference = VortexRdfStore::from_file(&reference).await.unwrap();
    for (name, store, off_the_index) in [
        ("copy in memory", memory, true),
        ("reference on file", reference, false),
    ] {
        let view = matched_view(&store, None, Some(&iri("p2")), None, None).await;
        for batch_rows in [256, 65_536] {
            let (_, served) = drain_served(&view, batch_rows).await;
            assert_eq!(served, off_the_index, "{name} at {batch_rows}");
            assert_code_chunks_in_read_order(&view, &QuadColumn::ALL, batch_rows, name).await;
        }
    }
    let mut quads = modular_quads(2_000, 7, 11);
    for s in ["rare-a", "rare-b"] {
        quads.push(make_quad(
            &format!("http://example.org/{s}"),
            "http://example.org/p0",
            "rare",
            GraphName::DefaultGraph,
        ));
    }
    let (_rare_dir, rare) = write_store_file(
        quads,
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    let store = VortexRdfStore::from_file(&rare).await.unwrap();
    let view = matched_view(&store, None, None, Some(&plain_literal("rare")), None).await;
    let rec = Recorder::start();
    let (rows, served) = drain_served(&view, 256).await;
    assert_eq!(rows.len(), 2);
    assert!(served, "a point-sized run is read off the index");
    assert_eq!(
        rec.count(&Event::RowScan),
        0,
        "point by point, with no scan"
    );
    assert_code_chunks_in_read_order(&view, &QuadColumn::ALL, 256, "point-sized").await;
}
