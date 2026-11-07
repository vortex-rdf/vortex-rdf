//! In-memory index components' zone maps: a component's graph column keeps
//! a zone map with its zones' recorded graphs, as a file's index child
//! does, taken while a build compresses the component or read from the
//! child's zone table at load.

use super::*;

/// The components of an index configuration.
fn zone_components(index: IndexConfig) -> &'static [&'static str] {
    match index {
        IndexConfig::NoIndex => &[],
        IndexConfig::Copy => &["index:posg", "index:ospg"],
        IndexConfig::Reference => &["index:ref-p", "index:ref-o"],
    }
}

/// Component `name`'s graph column in the component's row order, from the
/// base rows `rows` in base order (row `i` holding row id `i`): a copy
/// child's rows sorted by its keys, a reference child's by `(val, rid)`.
fn component_graphs(name: &str, rows: &[[TermCode; 4]]) -> Vec<TermCode> {
    let mut order: Vec<usize> = (0..rows.len()).collect();
    match name {
        "index:posg" => order.sort_by_key(|&i| (rows[i][1], rows[i][2], rows[i][0], rows[i][3])),
        "index:ospg" => order.sort_by_key(|&i| (rows[i][2], rows[i][0], rows[i][1], rows[i][3])),
        "index:ref-p" => order.sort_by_key(|&i| (rows[i][1], i)),
        "index:ref-o" => order.sort_by_key(|&i| (rows[i][2], i)),
        other => panic!("no component {other}"),
    }
    order.into_iter().map(|i| rows[i][3]).collect()
}

/// Each in-memory component's graph column carries its file's zone map: the
/// bounds of the component's own rows zone by zone, and each zone's recorded
/// graphs, the same whether the store is built from the quads, loaded from
/// the file in memory, or adopted from its bytes. A zone past 16 graphs
/// records none.
#[tokio::test]
async fn component_graph_zones_match_the_files() {
    for graphs in [Graphs::Stretches, Graphs::RecordedByTurns] {
        for index in [IndexConfig::Copy, IndexConfig::Reference] {
            let quads = zone_quads(graphs);
            let built = built_store(quads.clone(), LayoutStrategy::Dictionary, index.types()).await;
            let (_dir, path) =
                write_store_file(quads, LayoutStrategy::Dictionary, index.types()).await;
            let loaded = VortexRdfStore::from_file_in_memory(&path).await.unwrap();
            let bytes = std::fs::read(&path).unwrap();
            let adopted = VortexRdfStore::from_bytes(&bytes).await.unwrap();
            let rows = row_list(&built).await;
            for &name in zone_components(index) {
                let tag = format!("{graphs:?} {name}");
                let column = component_graphs(name, &rows);
                let want: Vec<(TermCode, TermCode)> = column
                    .chunks(ZONE_ROWS)
                    .map(|zone| (*zone.iter().min().unwrap(), *zone.iter().max().unwrap()))
                    .collect();
                let bounds = built
                    .debug_component_zone_bounds(name)
                    .unwrap_or_else(|| panic!("{tag}: built zones"));
                assert_eq!(bounds, want, "{tag}: the component's rows' bounds");
                let records = built
                    .debug_component_zone_records(name)
                    .unwrap_or_else(|| panic!("{tag}: built records"));
                for (zone, recorded) in column.chunks(ZONE_ROWS).zip(&records) {
                    let Some(pairs) = recorded else { continue };
                    let mut counts = std::collections::BTreeMap::new();
                    for &g in zone {
                        *counts.entry(g).or_insert(0u32) += 1;
                    }
                    assert_eq!(
                        pairs,
                        &counts.into_iter().collect::<Vec<_>>(),
                        "{tag}: a zone's recorded graphs"
                    );
                }
                for (label, store) in [("loaded", &loaded), ("adopted", &adopted)] {
                    assert_eq!(
                        store.debug_component_zone_bounds(name).as_ref(),
                        Some(&bounds),
                        "{tag} {label}: bounds"
                    );
                    assert_eq!(
                        store.debug_component_zone_records(name).as_ref(),
                        Some(&records),
                        "{tag} {label}: records"
                    );
                }
                match graphs {
                    Graphs::RecordedByTurns => assert!(
                        records.iter().any(Option::is_none) && records.iter().any(Option::is_some),
                        "{tag}: zones past 16 graphs beside recorded ones"
                    ),
                    _ => assert!(
                        records.iter().all(Option::is_some),
                        "{tag}: every zone recorded"
                    ),
                }
            }
        }
    }
}

/// A `from_bytes` store adopts its components unread: each carries its graph
/// column's zone map, read from the child's zone table at adoption, while its
/// rows wait for their first use.
#[tokio::test]
async fn adopted_components_stay_deferred() {
    for index in [IndexConfig::Copy, IndexConfig::Reference] {
        let (_dir, path) = write_store_file(
            zone_quads(Graphs::Stretches),
            LayoutStrategy::Dictionary,
            index.types(),
        )
        .await;
        let bytes = std::fs::read(&path).unwrap();
        let adopted = VortexRdfStore::from_bytes(&bytes).await.unwrap();
        for &name in zone_components(index) {
            assert!(
                adopted.debug_component_zone_bounds(name).is_some(),
                "{name}: a zone map at adoption"
            );
            assert_eq!(
                adopted.debug_index_component_materialized(name),
                Some(false),
                "{name}: rows deferred"
            );
        }
        // A match through the index is a component's first use.
        let ty = NamedNode::new_unchecked("http://example.org/type");
        let view = adopted
            .match_pattern(None, Some(&ty), None, None)
            .await
            .unwrap();
        assert_eq!(view.size().await.unwrap(), RUN, "{}", index.name());
        assert!(
            zone_components(index)
                .iter()
                .any(|name| adopted.debug_index_component_materialized(name) == Some(true)),
            "{}: the probed component materializes on use",
            index.name()
        );
    }
}

/// String-valued components, the Default layout's, have no zone maps, built
/// or loaded: their graph column holds terms, not codes, and a reference
/// child there has none.
#[tokio::test]
async fn string_components_have_no_zone_maps() {
    for index in [IndexConfig::Copy, IndexConfig::Reference] {
        let quads = zone_quads(Graphs::Stretches);
        let built = built_store(quads.clone(), LayoutStrategy::Default, index.types()).await;
        let (_dir, path) = write_store_file(quads, LayoutStrategy::Default, index.types()).await;
        let loaded = VortexRdfStore::from_file_in_memory(&path).await.unwrap();
        let bytes = std::fs::read(&path).unwrap();
        let adopted = VortexRdfStore::from_bytes(&bytes).await.unwrap();
        for (label, store) in [
            ("built", &built),
            ("loaded", &loaded),
            ("adopted", &adopted),
        ] {
            for &name in zone_components(index) {
                assert!(
                    store.debug_index_component_materialized(name).is_some(),
                    "{label} {name}: the component exists"
                );
                assert!(
                    store.debug_component_zone_bounds(name).is_none(),
                    "{label} {name}: no zone map"
                );
            }
        }
    }
}
