//! The public pieces the bindings compose a chunked read from: a code pattern's probe against the store's own
//! dictionary, and a string-layout row chunk decoded as shared quads.

use std::collections::BTreeSet;

use super::*;
use crate::store::read::chunks::chunk_path_hooks::{self, ForceLegacy};
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
/// now and on its pre-fix path ([`ForceLegacy`]): the same rows, in the same
/// order.
async fn assert_code_chunks_equal_legacy<const N: usize>(
    view: &VortexRdfStore,
    columns: &[QuadColumn; N],
    batch_rows: usize,
    tag: &str,
) {
    let (rows, _) = collect_code_chunks(view, columns, batch_rows).await;
    let legacy = {
        let _legacy = ForceLegacy::set();
        assert!(chunk_path_hooks::legacy());
        collect_code_chunks(view, columns, batch_rows).await.0
    };
    assert!(!chunk_path_hooks::legacy());
    assert_eq!(
        rows, legacy,
        "{tag}: code_chunks {columns:?} at {batch_rows}"
    );
}

/// [`assert_code_chunks_equal_legacy`] for `row_chunks`.
async fn assert_row_chunks_equal_legacy(view: &VortexRdfStore, batch_rows: usize, tag: &str) {
    let rows = struct_chunk_codes(&collect_row_chunks(view, batch_rows).await);
    let legacy = {
        let _legacy = ForceLegacy::set();
        struct_chunk_codes(&collect_row_chunks(view, batch_rows).await)
    };
    assert_eq!(rows, legacy, "{tag}: row_chunks at {batch_rows}");
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
