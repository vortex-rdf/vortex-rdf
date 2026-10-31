//! The public pieces the bindings compose a chunked read from: a code pattern's probe against the store's own
//! dictionary, and a string-layout row chunk decoded as shared quads.

use std::collections::BTreeSet;

use super::*;
use crate::store::test_hooks::{LegacyChunkPaths, legacy_chunk_paths};
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
fn code_rows(columns: &[Buffer<TermCode>; 4]) -> (BTreeSet<[TermCode; 4]>, usize) {
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
            assert_eq!(code_rows(&got), code_rows(&want), "{pattern:?}");
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
        for in_memory in [false, true] {
            let store = if in_memory {
                VortexRdfStore::from_file_in_memory(&path).await
            } else {
                VortexRdfStore::from_file(&path).await
            }
            .unwrap();
            let p1 = Probe::parse(None, Some("<http://example.org/p1>"), None, None).unwrap();
            let bound = store.run_probe(&p1).await.unwrap();
            for (view, what) in [(&store, "whole"), (&bound, "p-bound")] {
                let want = view.shared_quads_vec().await.unwrap();
                assert!(!want.is_empty());
                for batch_rows in [1, 7, 65_536] {
                    let tag = format!("{layout:?} in_memory={in_memory} {what} at {batch_rows}");
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

/// Drains `view`'s `code_chunks` over `columns` at `batch_rows` as it reads
/// now and on its base-order path ([`LegacyChunkPaths`]): the same rows, the
/// stream's in the order the view's reads yield them ([`read_order_codes`]).
async fn assert_code_chunks_equal_legacy<const N: usize>(
    view: &VortexRdfStore,
    columns: &[QuadColumn; N],
    batch_rows: usize,
    tag: &str,
) {
    let (rows, _) = collect_code_chunks(view, columns, batch_rows).await;
    let legacy = {
        let _legacy = LegacyChunkPaths::set();
        assert!(legacy_chunk_paths());
        collect_code_chunks(view, columns, batch_rows).await.0
    };
    assert!(!legacy_chunk_paths());
    let tag = format!("{tag}: code_chunks {columns:?} at {batch_rows}");
    assert_eq!(sorted(rows.clone()), sorted(legacy), "{tag}");
    let order: Vec<[TermCode; N]> = read_order_codes(view)
        .await
        .iter()
        .map(|row| columns.map(|c| row[c.index()]))
        .collect();
    assert_eq!(rows, order, "{tag}: the view's order");
}

/// [`assert_code_chunks_equal_legacy`] for `row_chunks`.
async fn assert_row_chunks_equal_legacy(view: &VortexRdfStore, batch_rows: usize, tag: &str) {
    let rows = struct_chunk_codes(&collect_row_chunks(view, batch_rows).await);
    let legacy = {
        let _legacy = LegacyChunkPaths::set();
        struct_chunk_codes(&collect_row_chunks(view, batch_rows).await)
    };
    let tag = format!("{tag}: row_chunks at {batch_rows}");
    assert_eq!(sorted(rows.clone()), sorted(legacy), "{tag}");
    assert_eq!(
        rows,
        read_order_codes(view).await,
        "{tag}: the view's order"
    );
}

/// `rows`, sorted: a stream's rows as a multiset.
fn sorted<T: Ord>(mut rows: Vec<T>) -> Vec<T> {
    rows.sort_unstable();
    rows
}

/// The patterns the legacy matrix reads on `modular_quads(20_000, 7, 11)`:
/// one subject, a predicate and an object each bound alone, the default
/// graph (every row), the predicate and object together, the subject with
/// its predicate, and nothing bound.
const LEGACY_PATTERNS: [(&str, [Option<&str>; 4]); 7] = [
    ("S", [Some("<http://example.org/s1234>"), None, None, None]),
    ("P", [None, Some("<http://example.org/p2>"), None, None]),
    ("O", [None, None, Some("\"object 3\""), None]),
    ("G", [None, None, None, Some("")]),
    (
        "PO",
        [
            None,
            Some("<http://example.org/p2>"),
            Some("\"object 3\""),
            None,
        ],
    ),
    (
        "SP",
        [
            Some("<http://example.org/s1234>"),
            Some("<http://example.org/p2>"),
            None,
            None,
        ],
    ),
    ("all", [None; 4]),
];

/// Every chunk stream reads what its pre-fix path reads, row for row, across
/// the shapes a view takes: each index set, both residencies, bound
/// positions served or filtered, tombstones, and a window.
#[tokio::test]
async fn chunk_reads_equal_the_legacy_paths() {
    let quads = modular_quads(20_000, 7, 11);
    for (name, indexes) in [
        ("no-index", vec![]),
        ("copy", vec![IndexType::SecondaryByCopy]),
        ("reference", vec![IndexType::SecondaryByReference]),
    ] {
        let (_dir, path) =
            write_store_file(quads.clone(), LayoutStrategy::Dictionary, indexes).await;
        for in_memory in [false, true] {
            let store = if in_memory {
                VortexRdfStore::from_file_in_memory(&path).await
            } else {
                VortexRdfStore::from_file(&path).await
            }
            .unwrap();
            // About a fifth of every view's rows tombstoned: p0's and "object 0"'s.
            let tombstoned = store
                .delete_matching(None, Some(&iri("p0")), None, None)
                .await
                .unwrap()
                .delete_matching(None, None, Some(&plain_literal("object 0")), None)
                .await
                .unwrap();
            for (deleted, owner) in [(false, &store), (true, &tombstoned)] {
                for (pattern, [s, p, o, g]) in LEGACY_PATTERNS {
                    for window in [None, Some((3, Some(50)))] {
                        let mut probe = Probe::parse(s, p, o, g).unwrap();
                        if let Some((offset, limit)) = window {
                            probe = probe.window(offset, limit);
                        }
                        let view = owner.run_probe(&probe).await.unwrap();
                        let tag = format!(
                            "{name} in_memory={in_memory} deleted={deleted} {pattern} window={window:?}"
                        );
                        let rows = collect_code_chunks(&view, &QuadColumn::ALL, 65_536)
                            .await
                            .0
                            .len();
                        // One row a chunk is a pull per row: views of at most 256 rows hold the case.
                        for batch_rows in [1, 256, 65_536] {
                            if batch_rows > 1 || rows <= 256 {
                                assert_code_chunks_equal_legacy(
                                    &view,
                                    &QuadColumn::ALL,
                                    batch_rows,
                                    &tag,
                                )
                                .await;
                            }
                        }
                        assert_code_chunks_equal_legacy(
                            &view,
                            &[QuadColumn::O, QuadColumn::S],
                            256,
                            &tag,
                        )
                        .await;
                        assert_row_chunks_equal_legacy(&view, 256, &tag).await;
                    }
                }
            }
        }
    }
}

/// A file view's code-chunk stream at `batch_rows`, drained, and whether it
/// was read off its serving index's run.
async fn drain_counting_served(
    view: &VortexRdfStore,
    batch_rows: usize,
) -> (Vec<[TermCode; 4]>, bool) {
    let (rows, served, _) = drain_served_and_base_reads(view, batch_rows).await;
    (rows, served)
}

/// [`drain_counting_served`], beside how many reads of the base it chose
/// ([`Event::ExactRead`]).
async fn drain_served_and_base_reads(
    view: &VortexRdfStore,
    batch_rows: usize,
) -> (Vec<[TermCode; 4]>, bool, usize) {
    let rec = Recorder::start();
    let (rows, _) = collect_code_chunks(view, &QuadColumn::ALL, batch_rows).await;
    let served = rec.count(&Event::ServedRunChunks);
    assert!(served <= 1, "one stream, one run");
    (rows, served == 1, rec.count(&Event::ExactRead))
}

/// The matched view of a pattern of spellings.
async fn probe_view(store: &VortexRdfStore, [s, p, o, g]: [Option<&str>; 4]) -> VortexRdfStore {
    store
        .run_probe(&Probe::parse(s, p, o, g).unwrap())
        .await
        .unwrap()
}

#[tokio::test]
async fn served_runs_are_read_off_the_index() {
    let (_dir, path) = write_store_file(
        modular_quads(20_000, 7, 11),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    for (name, pattern) in [
        ("P", [None, Some("<http://example.org/p2>"), None, None]),
        ("O", [None, None, Some("\"object 3\""), None]),
    ] {
        let view = probe_view(&store, pattern).await;
        assert!(
            served_component(&view).is_some(),
            "{name}: an index serves it"
        );
        for batch_rows in [256, 65_536] {
            let (_, served, base_reads) = drain_served_and_base_reads(&view, batch_rows).await;
            assert!(served, "{name} at {batch_rows}: read off the index");
            assert_eq!(
                base_reads, 0,
                "{name} at {batch_rows}: no read of the base chosen"
            );
        }
        for batch_rows in [1, 256, 65_536] {
            assert_code_chunks_equal_legacy(&view, &QuadColumn::ALL, batch_rows, name).await;
        }
        assert_code_chunks_equal_legacy(&view, &[QuadColumn::O, QuadColumn::S], 256, name).await;
    }
}

#[tokio::test]
async fn served_runs_at_the_cap_and_with_tombstones() {
    // p0 holds every even row: a run of 4,096 rows, exactly 16 chunks of 256;
    // its objects cycle 0, 2, 1, so "object 0" is every third of its rows.
    let (_dir, path) = write_store_file(
        modular_quads(8_192, 2, 3),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let p0 = [None, Some("<http://example.org/p0>"), None, None];
    let view = probe_view(&store, p0).await;
    let (rows, served) = drain_counting_served(&view, 256).await;
    assert_eq!(rows.len(), 4_096);
    assert!(served, "a run of exactly 16 chunks is read off the index");
    let (_, served) = drain_counting_served(&view, 255).await;
    assert!(!served, "a run a row past 16 chunks keeps the legacy path");
    assert_code_chunks_equal_legacy(&view, &QuadColumn::ALL, 256, "at the cap").await;
    assert_code_chunks_equal_legacy(&view, &QuadColumn::ALL, 255, "past the cap").await;

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
    let view = probe_view(&tombstoned, p0).await;
    let (rows, served) = drain_counting_served(&view, 256).await;
    assert!(served, "tombstones keep the index path");
    assert_eq!(
        rows.len(),
        4_096 - 1_366,
        "every third row of the run deleted"
    );
    assert!(rows.iter().all(|row| row[2] != deleted), "no deleted row");
    assert_code_chunks_equal_legacy(&view, &QuadColumn::ALL, 256, "tombstoned").await;
}

#[tokio::test]
async fn other_served_views_keep_their_path() {
    let quads = modular_quads(20_000, 7, 11);
    let p2 = [None, Some("<http://example.org/p2>"), None, None];
    let (_copy_dir, copy) = write_store_file(
        quads.clone(),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    let (_reference_dir, reference) = write_store_file(
        quads,
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByReference],
    )
    .await;
    // An in-memory copy's run is read off its plan, in the index's order.
    let memory = VortexRdfStore::from_file_in_memory(&copy).await.unwrap();
    let reference = VortexRdfStore::from_file(&reference).await.unwrap();
    for (name, store, read_off_the_run) in [
        ("copy in memory", memory, true),
        ("reference on file", reference, false),
    ] {
        let view = probe_view(&store, p2).await;
        for batch_rows in [256, 65_536] {
            let (_, served) = drain_counting_served(&view, batch_rows).await;
            assert_eq!(served, read_off_the_run, "{name} at {batch_rows}");
            assert_code_chunks_equal_legacy(&view, &QuadColumn::ALL, batch_rows, name).await;
        }
    }
    // A point-sized run's ids are resolved at once and read point by point,
    // which costs less than scanning the run off the index.
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
    let view = probe_view(&store, [None, None, Some("\"rare\""), None]).await;
    let (rows, served) = drain_counting_served(&view, 256).await;
    assert_eq!(rows.len(), 2);
    assert!(!served, "a point-sized run is read by id");
    assert_code_chunks_equal_legacy(&view, &QuadColumn::ALL, 256, "point-sized").await;
}

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

/// Masked reads of kept rows, among the events since `rec` last took.
fn masked_reads(rec: &mut Recorder) -> usize {
    rec.take()
        .iter()
        .filter(|event| matches!(event, Event::MaskedRead(_)))
        .count()
}

#[tokio::test]
async fn masked_streams_of_rare_and_absent_codes() {
    let (_dir, path) = rare_tail_file(8_192).await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    // Held only by the last split's rows: the whole file is evaluated before
    // the first chunk, which holds them.
    let rare = probe_view(&store, [None, None, Some("\"rare\""), None]).await;
    let mut rec = Recorder::start();
    let mut chunks = rare.code_chunks(&QuadColumn::ALL, 256).unwrap();
    let first = chunks.next().await.unwrap().unwrap();
    assert_eq!(first[0].len(), 3, "the rare rows in the first chunk");
    assert!(chunks.next().await.is_none());
    assert_eq!(rec.take_count(&Event::MasksFirstStream), 1);
    // A dictionary code no quad holds as an object (an IRI only predicates
    // hold, which the object column's zones rule out): no chunk, no read.
    let absent = probe_view(&store, [None, None, Some("<http://example.org/p0>"), None]).await;
    rec.take();
    let mut chunks = absent.code_chunks(&QuadColumn::ALL, 256).unwrap();
    assert!(chunks.next().await.is_none(), "no chunk");
    assert_eq!(masked_reads(&mut rec), 0, "no read of kept rows");
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
    let gone = probe_view(&tombstoned, [None, None, Some("\"rare\""), None]).await;
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
        let events = rec.take();
        assert_eq!(
            events
                .iter()
                .filter(|event| **event == Event::MasksFirstStream)
                .count(),
            2,
            "both streams masked, batch {batch_rows}"
        );
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, Event::MaskedRead(_))),
            "no read of kept rows, batch {batch_rows}"
        );
    }
}

#[tokio::test]
async fn masked_streams_are_taken() {
    let (_dir, path) = write_store_file(
        modular_quads(20_000, 7, 11),
        LayoutStrategy::Dictionary,
        vec![],
    )
    .await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    for (name, pattern) in [
        ("P", [None, Some("<http://example.org/p2>"), None, None]),
        ("O", [None, None, Some("\"object 3\""), None]),
    ] {
        let view = probe_view(&store, pattern).await;
        for batch_rows in [1, 256, 65_536] {
            let mut rec = Recorder::start();
            collect_code_chunks(&view, &QuadColumn::ALL, batch_rows).await;
            assert_eq!(
                rec.take_count(&Event::MasksFirstStream),
                1,
                "{name}: code_chunks at {batch_rows}"
            );
            collect_row_chunks(&view, batch_rows).await;
            assert_eq!(
                rec.take_count(&Event::MasksFirstStream),
                1,
                "{name}: row_chunks at {batch_rows}"
            );
            drop(rec);
            assert_code_chunks_equal_legacy(&view, &QuadColumn::ALL, batch_rows, name).await;
            assert_row_chunks_equal_legacy(&view, batch_rows, name).await;
        }
    }
}
