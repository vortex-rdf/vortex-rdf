//! The `vortexrdf.value_counts` statistic in store files: written into the
//! zone maps of every sorted table's graph column, and read back as it was
//! counted.

use super::*;
use crate::io::zone_map::{ExactZoneMap, ZONE_ROWS};
use crate::store::test_hooks::ForcedValueCounts;
use crate::store::value_counts::{decode_state, value_counts};

/// Whether `column` is zoned with the statistic among its aggregates.
fn records_value_counts(column: &vortex_layout::LayoutRef) -> bool {
    use vortex_layout::layouts::zoned::Zoned;
    column.as_opt::<Zoned>().is_some_and(|zoned| {
        zoned
            .present_aggregates()
            .contains(&value_counts().to_string())
    })
}

/// Each zone's recorded state of `column`'s statistic, decoded.
async fn recorded_states(
    column: &vortex_layout::LayoutRef,
    source: &std::sync::Arc<dyn vortex_layout::segments::SegmentSource>,
) -> Vec<Option<Vec<(u64, u32)>>> {
    use vortex_array::VortexSessionExecute as _;
    use vortex_array::arrays::StructArray;
    use vortex_array::arrays::struct_::StructArrayExt as _;

    let map = ExactZoneMap::of(column).expect("a zoned column");
    let reader = map
        .table
        .new_reader(
            "zone-table".into(),
            std::sync::Arc::clone(source),
            &crate::session::VORTEX_SESSION,
            &Default::default(),
        )
        .unwrap();
    let mut ctx = crate::session::VORTEX_SESSION.create_execution_ctx();
    let table = crate::io::read::scan_all_reader(reader)
        .await
        .unwrap()
        .execute::<StructArray>(&mut ctx)
        .unwrap();
    let states = table
        .unmasked_field_by_name(value_counts().to_string())
        .expect("the statistic's column")
        .clone();
    (0..states.len())
        .map(|zone| decode_state(&states.execute_scalar(zone, &mut ctx).unwrap()))
        .collect()
}

/// 20,000 quads over three graphs, on a store with the copy index.
fn three_graph_quads() -> Vec<Quad> {
    (0..20_000)
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{i:05}"),
                "http://example.org/p",
                &format!("o{}", i % 7),
                numbered_graph(1 + i % 3),
            )
        })
        .collect()
}

/// The graph column of every sorted table records the statistic; the
/// subject, predicate and object columns don't.
#[tokio::test]
async fn graph_columns_record_value_counts() {
    let (_dir, path) = write_store_file(
        three_graph_quads(),
        LayoutStrategy::Dictionary,
        IndexConfig::Copy.types(),
    )
    .await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let file = store.debug_native_file().unwrap();
    for component in [None, Some("index:posg"), Some("index:ospg")] {
        for (column, want) in [("s", false), ("p", false), ("o", false), ("g", true)] {
            let layout = file.table_column_layout(component, column).unwrap();
            assert_eq!(
                records_value_counts(&layout),
                want,
                "{component:?} {column}"
            );
        }
    }
}

/// Zones of 2, 16 and 18 graphs (the default graph among the last): the
/// first two record each graph with its rows, the third, past the limit of
/// 16, records null.
#[tokio::test]
async fn value_counts_round_trip() {
    let graph_of = |i: usize| match i / ZONE_ROWS {
        0 => 1 + i % 2,
        1 => 1 + i % 16,
        // 0 is the default graph: 17 named graphs and the default one.
        _ => i % 18,
    };
    let quads: Vec<Quad> = (0..3 * ZONE_ROWS)
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{i:06}"),
                "http://example.org/p",
                "o",
                numbered_graph(graph_of(i)),
            )
        })
        .collect();
    let (_dir, path) = write_store_file(quads, LayoutStrategy::Dictionary, vec![]).await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let file = store.debug_native_file().unwrap();
    let column = file.table_column_layout(None, "g").unwrap();
    let states = recorded_states(&column, &file.segment_source()).await;
    let rows = row_list(&store).await;
    let counted = |zone: usize| {
        let mut counts = std::collections::BTreeMap::new();
        for row in &rows[zone * ZONE_ROWS..(zone + 1) * ZONE_ROWS] {
            *counts.entry(row[3]).or_insert(0u32) += 1;
        }
        counts.into_iter().collect::<Vec<_>>()
    };
    assert_eq!(counted(1).len(), 16);
    assert_eq!(states, vec![Some(counted(0)), Some(counted(1)), None]);
}

/// A store written under `ForcedValueCounts::set(false)` stands for one
/// written before the statistic: no column records it.
#[tokio::test]
async fn forced_off_writes_no_value_counts() {
    let (_dir, path) = {
        let _off = ForcedValueCounts::set(false);
        write_store_file(
            three_graph_quads(),
            LayoutStrategy::Dictionary,
            IndexConfig::Copy.types(),
        )
        .await
    };
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let file = store.debug_native_file().unwrap();
    for component in [None, Some("index:posg"), Some("index:ospg")] {
        let layout = file.table_column_layout(component, "g").unwrap();
        assert!(!records_value_counts(&layout), "{component:?}");
    }
}
