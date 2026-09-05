//! The order a view claims for its rows, and the statistics a planner reads
//! before touching them: every claim checked against the rows themselves.

use std::num::NonZeroUsize;

use super::*;
use crate::store::{ExportOptions, Keep, META_SORT_ORDER, SortOrder, TermEncoding};
use arrow_array::cast::AsArray;
use arrow_array::types::UInt32Type;

/// The exported codes of `column`, in export order.
async fn exported(view: &VortexRdfStore, column: QuadColumn) -> Vec<u32> {
    let batches: Vec<arrow_array::RecordBatch> = view
        .to_record_batches(&ExportOptions::new(TermEncoding::Codes).projection([column]))
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_primitive::<UInt32Type>()
                .values()
                .iter()
                .copied()
        })
        .collect()
}

/// A claimed order is true of the rows: the leading column is non-decreasing
/// over the export, the schema names the order, and every exported code of
/// every column lies in the envelope statistics report — over every shape a
/// view takes in memory.
#[tokio::test]
async fn claimed_orders_hold_and_envelopes_contain_the_rows() {
    let quads = modular_quads(1200, 3, 5);
    let store = VortexRdfStore::from_quads(
        quad_stream(quads.clone()),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await
    .unwrap();
    let dict = store.code_read_snapshot().unwrap();
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let s7 = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s07").unwrap());
    let o1 = Term::Literal(Literal::new_simple_literal("object 1"));
    let served_p = store
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    let served_o = store
        .match_pattern(None, None, Some(&o1), None)
        .await
        .unwrap();
    let subject = store
        .match_pattern(Some(&s7), None, None, None)
        .await
        .unwrap();
    let (lo, hi) = dict.prefix_range("<http://example.org/s1");
    let kept = store
        .keep(QuadColumn::S, &Keep::range(lo, hi))
        .await
        .unwrap();
    let gathered = store
        .keep(
            QuadColumn::O,
            &Keep::set([dict.encode("\"object 1\"").unwrap()]),
        )
        .await
        .unwrap();
    let parts = served_p
        .partitions(NonZeroUsize::new(3).unwrap())
        .await
        .unwrap();
    let mut views: Vec<(&str, &VortexRdfStore, Option<SortOrder>)> = vec![
        ("store", &store, Some(SortOrder::SPOG)),
        ("served p", &served_p, Some(SortOrder::POSG)),
        ("served o", &served_o, Some(SortOrder::OSPG)),
        ("subject", &subject, Some(SortOrder::SPOG)),
        ("kept s", &kept, Some(SortOrder::SPOG)),
        ("gathered o", &gathered, Some(SortOrder::SPOG)),
    ];
    views.extend(
        parts
            .iter()
            .map(|part| ("served partition", part, Some(SortOrder::POSG))),
    );
    for (tag, view, expected) in views {
        assert_eq!(view.sort_order(), expected, "{tag}");
        let stats = view.statistics().await.unwrap();
        assert_eq!(stats.rows, view.size().await.unwrap(), "{tag}: rows");
        assert_eq!(stats.sort_order, expected, "{tag}: order");
        assert_eq!(
            stats.distinct_terms,
            Some(dict.len()),
            "{tag}: distinct terms"
        );
        let schema = view
            .to_record_batches(&ExportOptions::new(TermEncoding::Codes))
            .await
            .unwrap()
            .schema();
        assert_eq!(
            schema.metadata().get(META_SORT_ORDER).map(String::as_str),
            expected.map(|order| order.to_string()).as_deref(),
            "{tag}: schema metadata"
        );
        let lead = expected.unwrap().columns()[0];
        let codes = exported(view, lead).await;
        assert!(
            codes.windows(2).all(|pair| pair[0] <= pair[1]),
            "{tag}: {lead} non-decreasing"
        );
        for column in QuadColumn::ALL {
            let codes = exported(view, column).await;
            if let Some((lo, hi)) = stats.code_bounds[column.index()] {
                assert!(
                    codes.iter().all(|code| (lo..=hi).contains(code)),
                    "{tag}: {column} within [{lo}, {hi}]"
                );
            }
        }
        assert!(
            stats.code_bounds[lead.index()].is_some(),
            "{tag}: the lead column has bounds"
        );
    }
    // A fixed key's envelope is its one value.
    let stats = served_p.statistics().await.unwrap();
    let p1_code = dict.encode(&p1.to_string()).unwrap();
    assert_eq!(
        stats.code_bounds[QuadColumn::P.index()],
        Some((p1_code, p1_code))
    );
    assert!(
        stats.code_bounds[QuadColumn::O.index()].is_some(),
        "the next key has bounds"
    );
    assert!(
        stats.code_bounds[QuadColumn::S.index()].is_none(),
        "a later key has none"
    );
}

/// No order is claimed where none holds: under an append tail, on a base
/// built without the sorted stamp — and the schema then carries no key.
#[tokio::test]
async fn no_order_is_claimed_without_a_witness() {
    let quads = modular_quads(60, 3, 5);
    let store = VortexRdfStore::from_quads(
        quad_stream(quads.clone()),
        LayoutStrategy::Dictionary,
        vec![],
    )
    .await
    .unwrap();
    let tailed = store
        .add_quads([make_quad(
            "http://example.org/tail",
            "http://example.org/p1",
            "x",
            GraphName::DefaultGraph,
        )])
        .await
        .unwrap();
    assert_eq!(tailed.sort_order(), None);
    let schema = tailed
        .to_record_batches(&ExportOptions::new(TermEncoding::Strings))
        .await
        .unwrap()
        .schema();
    assert!(!schema.metadata().contains_key(META_SORT_ORDER));
    let stats = tailed.statistics().await.unwrap();
    assert_eq!(stats.code_bounds, [None; 4]);
    assert_eq!(unstamped_store(&quads).sort_order(), None);
}

/// On a file the base's order is the file's `quads_sorted` provenance and a
/// served match's the index child's; a partition of a served file view reads
/// the base, and says so.
#[cfg(feature = "file-io")]
#[tokio::test]
async fn file_views_claim_their_order() {
    let quads = modular_quads(1200, 3, 5);
    let (_dir, path) = write_store_file(
        quads,
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let served = store
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    assert_eq!(store.sort_order(), Some(SortOrder::SPOG));
    assert_eq!(served.sort_order(), Some(SortOrder::POSG));
    for (view, lead) in [(&store, QuadColumn::S), (&served, QuadColumn::P)] {
        let codes = exported(view, lead).await;
        assert!(codes.windows(2).all(|pair| pair[0] <= pair[1]));
        let stats = view.statistics().await.unwrap();
        assert_eq!(stats.rows, view.size().await.unwrap());
        assert_eq!(
            stats.code_bounds, [None; 4],
            "a file view reads nothing for an envelope"
        );
    }
    let parts = served
        .partitions(NonZeroUsize::new(2).unwrap())
        .await
        .unwrap();
    for part in &parts {
        assert_eq!(part.sort_order(), Some(SortOrder::SPOG));
        let codes = exported(part, QuadColumn::S).await;
        assert!(codes.windows(2).all(|pair| pair[0] <= pair[1]));
    }
}
