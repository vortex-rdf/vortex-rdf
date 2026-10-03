//! A store view registered through vortex's DataFusion table provider
//! answers SQL as its gathered codes say — on each backend, with filters,
//! limits, ordering and the persisted children as tables.

use vortex_rdf_core::{IndexType, LayoutStrategy, VortexRdfStore};
use vortex_rdf_interface_tests::*;

const N: usize = 3_000;

async fn predicate_code(store: &VortexRdfStore, i: usize) -> u32 {
    store
        .dict_reader()
        .unwrap()
        .encode(&format!("<http://example.org/p{i}>"))
        .await
        .unwrap()
        .expect("a predicate the store holds")
}

/// The whole conformance suite over one store.
async fn run_suite(store: &VortexRdfStore, tag: &str) {
    let ctx = context();
    register(&ctx, "quads", store.data_source().await.unwrap());
    let all = view_codes(store).await;
    let n = store.size().await.unwrap();
    assert_eq!(all.len(), n, "{tag}");

    // COUNT(*) and the whole table in base order.
    assert_eq!(count(&ctx, "SELECT COUNT(*) FROM quads").await, n as i64, "{tag}: count");
    assert_eq!(rows_u32(&ctx, "SELECT s, p, o, g FROM quads").await, all, "{tag}: whole table");
    assert_eq!(
        rows_u32(&ctx, "SELECT g, s FROM quads").await,
        all.iter().map(|r| vec![r[3], r[0]]).collect::<Vec<_>>(),
        "{tag}: a projection"
    );

    // Filters pushed into the scan: equality, a list, a range, a conjunction.
    let p1 = predicate_code(store, 1).await;
    let p2 = predicate_code(store, 2).await;
    let by_p1: Vec<Vec<u32>> = all.iter().filter(|r| r[1] == p1).cloned().collect();
    assert_eq!(
        rows_u32(&ctx, &format!("SELECT s, p, o, g FROM quads WHERE p = {p1}")).await,
        by_p1,
        "{tag}: equality"
    );
    let in_list: Vec<Vec<u32>> = all.iter().filter(|r| r[1] == p1 || r[1] == p2).cloned().collect();
    assert_eq!(
        rows_u32(&ctx, &format!("SELECT s, p, o, g FROM quads WHERE p IN ({p1}, {p2})")).await,
        in_list,
        "{tag}: IN"
    );
    let (lo, hi) = (all[100][0], all[900][0]);
    let in_range: Vec<Vec<u32>> =
        all.iter().filter(|r| r[0] >= lo && r[0] < hi).cloned().collect();
    assert_eq!(
        rows_u32(&ctx, &format!("SELECT s, p, o, g FROM quads WHERE s >= {lo} AND s < {hi}")).await,
        in_range,
        "{tag}: range"
    );
    let both: Vec<Vec<u32>> = in_range.iter().filter(|r| r[1] == p1).cloned().collect();
    assert_eq!(
        rows_u32(&ctx, &format!("SELECT s, p, o, g FROM quads WHERE s >= {lo} AND s < {hi} AND p = {p1}")).await,
        both,
        "{tag}: conjunction"
    );

    // Limits, with and without a filter, and an ordering the base already has.
    assert_eq!(rows_u32(&ctx, "SELECT s, p, o, g FROM quads LIMIT 7").await, all[..7], "{tag}: limit");
    assert_eq!(
        rows_u32(&ctx, &format!("SELECT s, p, o, g FROM quads WHERE p = {p1} LIMIT 5")).await,
        by_p1[..5],
        "{tag}: filtered limit"
    );
    assert_eq!(
        rows_u32(&ctx, "SELECT s, p FROM quads ORDER BY s, p LIMIT 3").await,
        all[..3].iter().map(|r| vec![r[0], r[1]]).collect::<Vec<_>>(),
        "{tag}: order by the base's sort"
    );
    assert_eq!(
        count(&ctx, &format!("SELECT COUNT(*) FROM quads WHERE p = {p1}")).await,
        by_p1.len() as i64,
        "{tag}: filtered count"
    );

    // Aggregates over the codes.
    let distinct_p = {
        let mut ps: Vec<u32> = all.iter().map(|r| r[1]).collect();
        ps.sort_unstable();
        ps.dedup();
        ps
    };
    assert_eq!(
        rows_u32(&ctx, "SELECT DISTINCT p FROM quads ORDER BY p").await,
        distinct_p.iter().map(|&p| vec![p]).collect::<Vec<_>>(),
        "{tag}: distinct"
    );

    // A narrowed view is a table of its own.
    let p1_node = oxrdf::NamedNode::new("http://example.org/p1").unwrap();
    let view = store
        .match_pattern(None, Some(&p1_node), None, None)
        .await
        .unwrap();
    register(&ctx, "p1", view.data_source().await.unwrap());
    assert_eq!(count(&ctx, "SELECT COUNT(*) FROM p1").await, by_p1.len() as i64, "{tag}: view count");
    assert_eq!(rows_u32(&ctx, "SELECT s, p, o, g FROM p1").await, by_p1, "{tag}: view rows");
    let joined = count(&ctx, "SELECT COUNT(*) FROM p1 JOIN quads ON p1.s = quads.s").await;
    let expected_join: usize = by_p1
        .iter()
        .map(|r| all.iter().filter(|q| q[0] == r[0]).count())
        .sum();
    assert_eq!(joined, expected_join as i64, "{tag}: a join between two views");

    // Codes decode through the dictionary handle.
    let first = rows_u32(&ctx, &format!("SELECT s FROM quads WHERE p = {p1} LIMIT 1")).await;
    let term = store.dict_reader().unwrap().decode(first[0][0]).await.unwrap().unwrap();
    assert!(term.starts_with("<http://example.org/s"), "{tag}: decoded {term}");
}

/// The persisted children as tables: the `index:posg` copy sorted by
/// `(p, o, s, g)` with base row ids, and the dictionary's term column.
async fn run_component_suite(store: &VortexRdfStore, tag: &str) {
    let ctx = context();
    let all = view_codes(store).await;
    let posg = store
        .component_data_source("index:posg")
        .unwrap()
        .expect("a by-copy index child");
    register(&ctx, "posg", posg);
    assert_eq!(count(&ctx, "SELECT COUNT(*) FROM posg").await, all.len() as i64, "{tag}");
    let rows = rows_u32(&ctx, "SELECT p, o, s, g, rid FROM posg").await;
    assert!(rows.windows(2).all(|w| w[0][..4] <= w[1][..4]), "{tag}: sorted by (p, o, s, g)");
    for row in rows.iter().step_by(97) {
        let base = &all[row[4] as usize];
        assert_eq!([base[1], base[2], base[0], base[3]], [row[0], row[1], row[2], row[3]], "{tag}: rid addresses the base row");
    }
    if let Some(dict) = store.component_data_source("dictionary").unwrap() {
        register(&ctx, "dictionary", dict);
        let len = store.dict_reader().unwrap().len() as i64;
        assert_eq!(count(&ctx, "SELECT COUNT(*) FROM dictionary").await, len, "{tag}: one row per term");
        let first = strings(&ctx, "SELECT _dict_term FROM dictionary ORDER BY _dict_term LIMIT 1").await;
        let code0 = store.dict_reader().unwrap().decode(0).await.unwrap().unwrap();
        assert_eq!(first, [code0], "{tag}: row 0 is code 0");
        // A term-level filter against the term column, joined back on codes
        // through the rank: the planner's decode-by-join.
        let literals = count(&ctx, "SELECT COUNT(*) FROM dictionary WHERE _dict_term LIKE '\"%'").await;
        let kinds = store.dict_reader().unwrap().kind_ranges().await.unwrap();
        assert_eq!(literals as u32, kinds.literals.end - kinds.literals.start, "{tag}: literal count");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_store_through_datafusion() {
    let quads = modular_quads(N, 4, 5, 3);
    for indexes in [vec![], vec![IndexType::SecondaryByCopy]] {
        let store = memory_store(&quads, LayoutStrategy::Dictionary, indexes.clone()).await;
        run_suite(&store, &format!("memory {indexes:?}")).await;
        if !indexes.is_empty() {
            run_component_suite(&store, "memory").await;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn file_store_through_datafusion() {
    let quads = modular_quads(N, 4, 5, 3);
    let dir = tempfile::tempdir().unwrap();
    for (residency, tag) in [(u64::MAX, "file"), (0, "file/dict-in-file")] {
        let store = file_store(
            dir.path(),
            &quads,
            LayoutStrategy::Dictionary,
            vec![IndexType::SecondaryByCopy],
            residency,
        )
        .await;
        run_suite(&store, tag).await;
        run_component_suite(&store, tag).await;
    }
    let bytes = std::fs::read(dir.path().join("store.vortex")).unwrap();
    let adopted = VortexRdfStore::from_bytes_owned(bytes).await.unwrap();
    run_suite(&adopted, "bytes").await;
}

/// A string layout registers with `Utf8View` columns and answers term-level
/// predicates directly.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn string_layout_through_datafusion() {
    let quads = modular_quads(500, 4, 5, 3);
    let store = memory_store(&quads, LayoutStrategy::Default, vec![]).await;
    let ctx = context();
    register(&ctx, "quads", store.data_source().await.unwrap());
    assert_eq!(count(&ctx, "SELECT COUNT(*) FROM quads").await, 500);
    let subjects = strings(&ctx, "SELECT s FROM quads WHERE p = '<http://example.org/p1>' LIMIT 2").await;
    assert_eq!(subjects, ["<http://example.org/s00001>", "<http://example.org/s00005>"]);
    let literals = count(&ctx, "SELECT COUNT(*) FROM quads WHERE o LIKE '\"o1%'").await;
    assert_eq!(literals, 100);
}
