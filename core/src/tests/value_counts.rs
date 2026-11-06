//! The `vortexrdf.value_counts` statistic in store files: written into the
//! zone maps of every sorted table's graph column, and read back as it was
//! counted.

use super::*;
use crate::io::zone_map::{ExactZoneMap, ZONE_ROWS};
use crate::stats_rules::zones_switch;
use crate::store::test_hooks::ForcedValueCounts;
use crate::store::value_counts::{decode_state, value_counts};

/// Whether `column` is zoned with the statistic among its aggregates.
pub(super) fn records_value_counts(column: &vortex_layout::LayoutRef) -> bool {
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

/// The probe's graph of row `i`: zone 4 holds graph 2 alone; zone 6 holds 20
/// graphs around graph 2's code (1, 3 and 40 to 57, past the limit of 16);
/// the other zones alternate graphs 1 and 3, so graph 2 lies within their
/// bounds without being in them.
fn probe_graph(i: usize) -> usize {
    match i / ZONE_ROWS {
        4 => 2,
        6 => [1, 3].into_iter().chain(40..58).nth(i % 20).unwrap(),
        _ if i.is_multiple_of(2) => 1,
        _ => 3,
    }
}

/// A quad per row of `zones` zones, row `i` in graph `graph_of(i)`.
fn zoned_graph_quads(zones: usize, graph_of: impl Fn(usize) -> usize) -> Vec<Quad> {
    (0..zones * ZONE_ROWS)
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{i:06}"),
                "http://example.org/p",
                "o",
                numbered_graph(graph_of(i)),
            )
        })
        .collect()
}

/// The code of graph `n` (`numbered_graph`'s IRI) in `store`'s dictionary.
async fn graph_code(store: &VortexRdfStore, n: usize) -> TermCode {
    store
        .dict_reader()
        .unwrap()
        .encode(&format!("<http://example.org/g{n}>"))
        .await
        .unwrap()
        .unwrap()
}

/// Each zone's graph codes, from the store's rows in table order.
async fn zone_graphs(store: &VortexRdfStore) -> Vec<std::collections::BTreeSet<u64>> {
    let rows = row_list(store).await;
    rows.chunks(ZONE_ROWS)
        .map(|zone| zone.iter().map(|row| row[3]).collect())
        .collect()
}

/// For `code`: the zones a reader of the recorded graphs keeps (its bounds
/// hold `code`, and the zone holds it or recorded too many graphs to say),
/// and the zones the bounds alone keep.
fn expected_zones(
    zones: &[std::collections::BTreeSet<u64>],
    code: u64,
) -> (Vec<usize>, Vec<usize>) {
    let in_bounds = |zone: &std::collections::BTreeSet<u64>| {
        zone.first().is_some_and(|&min| min <= code) && zone.last().is_some_and(|&max| code <= max)
    };
    let bounds: Vec<usize> = (0..zones.len()).filter(|&z| in_bounds(&zones[z])).collect();
    let kept = bounds
        .iter()
        .copied()
        .filter(|&z| zones[z].len() > 16 || zones[z].contains(&code))
        .collect();
    (kept, bounds)
}

/// The zones of the quad table that Vortex's pruning of `g = code` keeps.
async fn kept_zones(store: &VortexRdfStore, code: u64) -> Vec<usize> {
    use vortex_array::expr::{eq, get_item, lit, root};
    let file = store.debug_native_file().unwrap();
    let reader = file.layout_reader().unwrap();
    let rows = file.row_count();
    let expr = eq(get_item("g", root()), lit(code))
        .bind(reader.dtype())
        .unwrap();
    let mask = reader
        .pruning_evaluation(
            &(0..rows),
            &expr,
            vortex_mask::Mask::new_true(rows as usize),
        )
        .unwrap()
        .await
        .unwrap();
    let rows = rows as usize;
    (0..rows.div_ceil(ZONE_ROWS))
        .filter(|z| {
            mask.slice(z * ZONE_ROWS..((z + 1) * ZONE_ROWS).min(rows))
                .true_count()
                > 0
        })
        .collect()
}

/// Vortex's own pruning of `g = c` keeps exactly the zones that hold `c`,
/// and the zones that recorded too many graphs to say: graph 2, inside every
/// zone's bounds, keeps two of eight. `VORTEX_RDF_ZONES=0` turns the
/// falsifier off, leaving the bounds alone.
#[tokio::test]
async fn vortex_pruning_drops_zones_without_the_graph() {
    let (_dir, path) = write_store_file(
        zoned_graph_quads(8, probe_graph),
        LayoutStrategy::Dictionary,
        vec![],
    )
    .await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let zones = zone_graphs(&store).await;
    let g2 = graph_code(&store, 2).await;
    assert_eq!(expected_zones(&zones, g2).1, (0..8).collect::<Vec<_>>());
    let (mut by_counts, mut null_kept) = (false, false);
    for graph in [2, 1, 45] {
        let code = graph_code(&store, graph).await;
        let (want, bounds) = expected_zones(&zones, code);
        by_counts |= want != bounds;
        null_kept |= want
            .iter()
            .any(|&z| zones[z].len() > 16 && !zones[z].contains(&code));
        let kept = if zones_switch() { want } else { bounds };
        assert_eq!(kept_zones(&store, code).await, kept, "g{graph}");
    }
    assert!(
        by_counts && null_kept,
        "a zone pruned by its counts; a null zone kept"
    );
}

/// The general match of a bound graph answers its rows, and its zone-map
/// envelope is narrowed to the zones the recorded graphs keep; with
/// `VORTEX_RDF_ZONES=0` the bounds, which hold graph 2 in every zone, narrow
/// nothing.
#[tokio::test]
async fn graph_bound_general_match_prunes_zones() {
    let (_dir, path) = write_store_file(
        zoned_graph_quads(8, probe_graph),
        LayoutStrategy::Dictionary,
        vec![],
    )
    .await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let graph = numbered_graph(2);
    let code = graph_code(&store, 2).await;
    let mut want: Vec<[TermCode; 4]> = row_list(&store)
        .await
        .into_iter()
        .filter(|row| row[3] == code)
        .collect();
    let view = store
        .match_pattern(None, None, None, Some(&graph))
        .await
        .unwrap();
    let mut got = row_list(&view).await;
    want.sort_unstable();
    got.sort_unstable();
    assert_eq!(got, want);
    let zone = ZONE_ROWS as u64;
    assert_eq!(
        store.debug_graph_pruning_envelope(&graph).await.unwrap(),
        zones_switch().then_some(4 * zone..7 * zone)
    );
}

/// The default graph is recorded like any other: zones alternating it and
/// graph 3 are pruned for graph 2 by their counts, and kept for the default
/// graph. `VORTEX_RDF_ZONES=0` leaves the bounds alone.
#[tokio::test]
async fn default_graph_zones_prune() {
    let graph_of = |i: usize| match i / ZONE_ROWS {
        4 => 2,
        _ if i.is_multiple_of(2) => 0,
        _ => 3,
    };
    let (_dir, path) = write_store_file(
        zoned_graph_quads(8, graph_of),
        LayoutStrategy::Dictionary,
        vec![],
    )
    .await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let zones = zone_graphs(&store).await;
    let default = row_list(&store).await[0][3];
    let g2 = graph_code(&store, 2).await;
    let (want, bounds) = expected_zones(&zones, g2);
    assert_eq!(want, vec![4]);
    assert_ne!(
        want, bounds,
        "graph 2 lies within the default graph's zones' bounds"
    );
    let kept = if zones_switch() { want } else { bounds };
    assert_eq!(kept_zones(&store, g2).await, kept);
    let (want, bounds) = expected_zones(&zones, default);
    assert_eq!(want, (0..8).filter(|&z| z != 4).collect::<Vec<_>>());
    let kept = if zones_switch() { want } else { bounds };
    assert_eq!(kept_zones(&store, default).await, kept);
}

/// An in-memory store's bytes record the statistic, and a store adopted from
/// them answers a graph-bound match as the file does.
#[tokio::test]
async fn bytes_carry_the_statistic() {
    let memory = built_store(
        zoned_graph_quads(8, probe_graph),
        LayoutStrategy::Dictionary,
        vec![],
    )
    .await;
    let bytes = memory.to_bytes().await.unwrap();
    let (_dir, path) = temp_store_file(&bytes);
    let file = VortexRdfStore::from_file(&path).await.unwrap();
    let column = file
        .debug_native_file()
        .unwrap()
        .table_column_layout(None, "g")
        .unwrap();
    assert!(records_value_counts(&column));
    let adopted = VortexRdfStore::from_bytes(&bytes).await.unwrap();
    let graph = numbered_graph(2);
    assert_eq!(
        graph_rows(&adopted, &graph).await,
        graph_rows(&file, &graph).await
    );
}

/// The rows of `store` in `graph`, sorted.
async fn graph_rows(store: &VortexRdfStore, graph: &GraphName) -> Vec<[TermCode; 4]> {
    let view = store
        .match_pattern(None, None, None, Some(graph))
        .await
        .unwrap();
    let mut rows = row_list(&view).await;
    rows.sort_unstable();
    rows
}
