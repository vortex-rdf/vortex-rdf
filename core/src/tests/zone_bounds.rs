//! In-memory zone bounds: an in-memory base's code columns carry the
//! smallest and largest code of each 8,192-row zone, taken in the pass that
//! compresses a build's codes or read from the zone maps of the file the store
//! was loaded from, and a residual equality on a base no index serves compares
//! only the zones whose bounds hold its code. Its answers are the full
//! compare's, over every kind of selection.

use super::*;
use crate::io::zone_map::ZONE_ROWS;

/// The rows of [`zoned_quads`]: four zones and part of a fifth.
const ROWS: usize = 4 * ZONE_ROWS + 1_000;
/// The rows holding the object `"rare"`: one in the first zone, one in the
/// last.
const RARE: [usize; 2] = [100, 4 * ZONE_ROWS + 7];

/// [`ROWS`] quads, one per subject `s{i:06}`, so in row order, with the
/// predicate `p{i % 3}`, the object `"o{i % 50}"` but on the [`RARE`] rows,
/// whose object is `"rare"`, and the default graph. Every zone holds every
/// predicate and every `"o…"` object.
fn zoned_quads() -> Vec<Quad> {
    (0..ROWS)
        .map(|i| {
            let o = if RARE.contains(&i) {
                "rare".to_string()
            } else {
                format!("o{}", i % 50)
            };
            make_quad(
                &format!("http://example.org/s{i:06}"),
                &format!("http://example.org/p{}", i % 3),
                &o,
                GraphName::DefaultGraph,
            )
        })
        .collect()
}

/// Rows per chunk of [`chunked_build`]: a size zones do not divide, so
/// zones cross chunk boundaries.
const CHUNK_ROWS: usize = 5_000;

/// `quads` built as a Dictionary store whose code columns are chunks of
/// [`CHUNK_ROWS`] rows, as a build past 100,000 rows has them.
async fn chunked_build(quads: Vec<Quad>) -> VortexRdfStore {
    use crate::store::builders::{BuildSizes, BuiltArray, sorted_stream};
    let built = sorted_stream::build_chunk_stream(
        Box::new(quad_stream(quads)),
        LayoutStrategy::Dictionary,
        vec![],
        BuildSizes::uniform(CHUNK_ROWS),
        None,
    )
    .await
    .unwrap();
    let chunks: Vec<_> = built
        .chunks
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|chunk| chunk.unwrap())
        .collect();
    assert!(chunks.len() > ROWS / ZONE_ROWS);
    let dtype = chunks[0].dtype().clone();
    let array = crate::store::array::chunked_or_single(chunks, dtype).unwrap();
    VortexRdfStore::from_built(BuiltArray {
        array,
        components: Vec::new(),
        dict: built.dict,
    })
    .unwrap()
}

/// [`zoned_quads`]' file store, the reference, and its in-memory stores:
/// built (in one chunk and in [`CHUNK_ROWS`]-row chunks), loaded from the
/// file, and loaded from its bytes. Keep the `TempDir` alive for the
/// stores' lifetime.
async fn zoned_stores() -> (
    tempfile::TempDir,
    VortexRdfStore,
    Vec<(&'static str, VortexRdfStore)>,
) {
    let quads = zoned_quads();
    let (dir, path) = write_store_file(quads.clone(), LayoutStrategy::Dictionary, vec![]).await;
    let file = VortexRdfStore::from_file(&path).await.unwrap();
    let bytes = std::fs::read(&path).unwrap();
    let stores = vec![
        (
            "built",
            built_store(quads.clone(), LayoutStrategy::Dictionary, vec![]).await,
        ),
        ("built in chunks", chunked_build(quads).await),
        (
            "loaded",
            VortexRdfStore::from_file_in_memory(&path).await.unwrap(),
        ),
        (
            "from bytes",
            VortexRdfStore::from_bytes(&bytes).await.unwrap(),
        ),
    ];
    (dir, file, stores)
}

/// Every in-memory Dictionary store has zone bounds on its four code
/// columns, the same whether taken from the build's codes (in one chunk or
/// in chunks that zones cross) or read from the file's zone maps, and a
/// compaction's rebuild has them too; a Default-layout store, whose columns
/// hold strings, has none.
#[tokio::test]
async fn in_memory_stores_carry_the_zone_bounds_of_their_code_columns() {
    let (_dir, _file, stores) = zoned_stores().await;
    let built = &stores[0].1;
    for (label, store) in &stores {
        assert_eq!(store.debug_zone_columns(), ["s", "p", "o", "g"], "{label}");
        for column in ["s", "p", "o", "g"] {
            let bounds = store.debug_zone_bounds(column).unwrap();
            assert_eq!(bounds.len(), ROWS.div_ceil(ZONE_ROWS), "{label} {column}");
            assert_eq!(
                Some(bounds),
                built.debug_zone_bounds(column),
                "{label} {column}"
            );
        }
        let extra = make_quad(
            "http://example.org/tail",
            "http://example.org/p0",
            "o1",
            GraphName::DefaultGraph,
        );
        let compacted = store
            .add_quads([extra])
            .await
            .unwrap()
            .compact()
            .await
            .unwrap();
        assert_eq!(
            compacted.debug_zone_columns(),
            ["s", "p", "o", "g"],
            "{label}: compacted"
        );
    }
    let default = built_store(zoned_quads(), LayoutStrategy::Default, vec![]).await;
    assert!(default.debug_zone_columns().is_empty());
}

/// A residual equality whose code few zones may hold compares only those:
/// the object `"rare"` alone and with each predicate. A code every zone may
/// hold compares every row. Each answer is the file store's.
#[tokio::test]
async fn a_rare_code_compares_only_the_zones_that_may_hold_it() {
    let (_dir, file, stores) = zoned_stores().await;
    let rare = plain_literal("rare");
    let (p0, p1, p2) = (iri("p0"), iri("p1"), iri("p2"));
    let cases = [
        ("O", None, &rare, 2),
        ("P0 O", Some(&p0), &rare, 1),
        ("P1 O", Some(&p1), &rare, 1),
        ("P2 O", Some(&p2), &rare, 0),
    ];
    for (label, store) in &stores {
        for (shape, p, o, rows) in cases {
            let rec = Recorder::start();
            let view = matched_view(store, None, p, Some(o), None).await;
            assert_eq!(
                rec.count(&Event::ZoneNarrowed),
                1,
                "{label} {shape}: narrowed by zone bounds"
            );
            let got = view_strings(&view).await;
            assert_eq!(got.len(), rows, "{label} {shape}");
            let want = view_strings(&matched_view(&file, None, p, Some(o), None).await).await;
            assert_eq!(got, want, "{label} {shape}");
        }
        let common = plain_literal("o7");
        let rec = Recorder::start();
        let view = matched_view(store, None, None, Some(&common), None).await;
        assert_eq!(
            rec.count(&Event::ZoneNarrowed),
            0,
            "{label}: a common object"
        );
        let want = (0..ROWS)
            .filter(|i| i % 50 == 7 && !RARE.contains(i))
            .count();
        assert_eq!(view.size().await.unwrap(), want, "{label}: a common object");
    }
}

/// The zone-narrowed answer over a window of the base (a range), a chained
/// match's rows (an id list), tombstones and a tail is the full compare's.
#[tokio::test]
async fn zone_narrowing_answers_every_selection_as_the_full_compare() {
    let (_dir, _file, stores) = zoned_stores().await;
    let rare = plain_literal("rare");
    for (label, store) in &stores {
        for (offset, limit) in [
            (0, ROWS),
            (50, 4 * ZONE_ROWS),
            (101, ROWS),
            (0, 101),
            (3 * ZONE_ROWS, ZONE_ROWS + 500),
            (ZONE_ROWS, 2 * ZONE_ROWS),
        ] {
            let windowed = store.window(offset, limit).await.unwrap();
            let got = matched_size(&windowed, None, None, Some(&rare), None).await;
            let want = RARE
                .iter()
                .filter(|&&row| row >= offset && row < offset + limit)
                .count();
            assert_eq!(got, want, "{label}: window ({offset}, {limit})");
        }
        // `p0` keeps every third row, as an id list; of the rare rows only
        // the last has it.
        let p0 = matched_view(store, None, Some(&iri("p0")), None, None).await;
        let chained = matched_view(&p0, None, None, Some(&rare), None).await;
        let subjects: Vec<String> = chained
            .quads_vec()
            .await
            .unwrap()
            .iter()
            .map(|q| q.subject.to_string())
            .collect();
        assert_eq!(
            subjects,
            [format!("<http://example.org/s{:06}>", RARE[1])],
            "{label}: chained"
        );
        // Tombstones: the first rare row deleted.
        let deleted = store
            .delete_matching(None, Some(&iri("p1")), Some(&rare), None)
            .await
            .unwrap();
        assert_eq!(
            matched_size(&deleted, None, None, Some(&rare), None).await,
            1,
            "{label}: tombstoned"
        );
        // A tail holding the object too.
        let tailed = store
            .add_quads([make_quad(
                "http://example.org/tail",
                "http://example.org/p0",
                "rare",
                GraphName::DefaultGraph,
            )])
            .await
            .unwrap();
        assert_eq!(
            matched_size(&tailed, None, None, Some(&rare), None).await,
            3,
            "{label}: tailed"
        );
    }
}

/// An in-memory store's zone maps are its file's: the four code columns'
/// bounds and `g`'s recorded graphs, the same whether built from the quads,
/// loaded from the file in memory, or adopted from its bytes. Only `g`
/// records graphs, and a zone past 16 of them records none.
#[tokio::test]
async fn in_memory_zone_maps_match_the_files() {
    use super::located_zones::{Graphs, zone_quads};

    for graphs in [Graphs::Stretches, Graphs::RecordedByTurns] {
        let quads = zone_quads(graphs);
        let built = built_store(quads.clone(), LayoutStrategy::Dictionary, vec![]).await;
        let (_dir, path) = write_store_file(quads, LayoutStrategy::Dictionary, vec![]).await;
        let loaded = VortexRdfStore::from_file_in_memory(&path).await.unwrap();
        let bytes = std::fs::read(&path).unwrap();
        let adopted = VortexRdfStore::from_bytes(&bytes).await.unwrap();
        for column in ["s", "p", "o", "g"] {
            let want = built.debug_zone_bounds(column).expect("built zones");
            for (label, store) in [("loaded", &loaded), ("adopted", &adopted)] {
                assert_eq!(
                    store.debug_zone_bounds(column).as_ref(),
                    Some(&want),
                    "{graphs:?} {label} {column}"
                );
            }
        }
        let records = built.debug_zone_records("g").expect("g's records");
        for (label, store) in [("loaded", &loaded), ("adopted", &adopted)] {
            assert_eq!(
                store.debug_zone_records("g").as_ref(),
                Some(&records),
                "{graphs:?} {label} g"
            );
        }
        for column in ["s", "p", "o"] {
            assert!(built.debug_zone_records(column).is_none(), "{column}");
        }
        match graphs {
            Graphs::RecordedByTurns => assert!(
                records.iter().any(Option::is_none) && records.iter().any(Option::is_some),
                "zones past 16 graphs beside recorded ones"
            ),
            _ => assert!(records.iter().all(Option::is_some), "every zone recorded"),
        }
    }
}

/// A residual graph whose code every zone's bounds hold, but only two zones
/// hold (`Graphs::Gapped`): the recorded graphs leave the others out, so
/// `(? ? ? g2)` in memory compares only those two zones, where bounds alone
/// compared every row.
#[tokio::test]
async fn records_skip_zones_in_memory() {
    let _zones_on = crate::store::test_hooks::ForcedZones::set(true);
    use super::located_zones::{Graphs, zone_quads};

    let store = built_store(
        zone_quads(Graphs::Gapped),
        LayoutStrategy::Dictionary,
        vec![],
    )
    .await;
    let g2 = iri_graph("g2");
    let rec = Recorder::start();
    let view = store
        .match_pattern(None, None, None, Some(&g2))
        .await
        .unwrap();
    assert_eq!(view.size().await.unwrap(), 2_000, "graph 2's rows");
    assert_eq!(rec.count(&Event::ZoneNarrowed), 1, "narrowed by zones");
    let compared: u64 = rec
        .picked(|e| match e {
            Event::ResidualCompared(rows) => Some(*rows),
            _ => None,
        })
        .iter()
        .sum();
    assert_eq!(compared, 2 * ZONE_ROWS as u64, "graph 2's two zones");
}

/// A one-graph store's graph column holds a single code, so its array
/// statistics settle a graph residual with no zone and no row compared:
/// `(? ? ? g1)` matches every row, counted and paged as the base's rows,
/// and `(? ? ? type)`, whose code no row holds as a graph, matches none.
#[tokio::test]
async fn array_statistics_settle_a_whole_column() {
    let _zones_on = crate::store::test_hooks::ForcedZones::set(true);
    use super::located_zones::{Graphs, RUN, code_of, zone_quads};

    let store = built_store(zone_quads(Graphs::One), LayoutStrategy::Dictionary, vec![]).await;
    let rows = 2 * RUN;
    let compared = |rec: &Recorder| rec.count_where(|e| matches!(e, Event::ResidualCompared(_)));
    let g1 = code_of(&store, "<http://example.org/g1>").await;
    let every = crate::CodePattern::new(None, None, None, Some(g1));

    let rec = Recorder::start();
    let view = store
        .match_pattern(None, None, None, Some(&iri_graph("g1")))
        .await
        .unwrap();
    assert_eq!(view.size().await.unwrap(), rows, "every row");
    assert_eq!(store.count_code_pattern(every).await.unwrap(), rows as u64);
    let page = store
        .match_code_pattern(every, 18_000, Some(4_096))
        .await
        .unwrap();
    let base = row_list(&store).await;
    let page: Vec<[TermCode; 4]> = (0..page[0].len())
        .map(|i| std::array::from_fn(|c| page[c][i]))
        .collect();
    assert_eq!(page, base[18_000..22_096], "the base's rows, in base order");
    assert!(
        rec.count(&Event::ResidualSettled { none: false }) >= 1,
        "settled by the statistics"
    );
    assert_eq!(compared(&rec), 0, "no row compared");

    let rec = Recorder::start();
    let none = store
        .match_pattern(None, None, None, Some(&iri_graph("type")))
        .await
        .unwrap();
    assert_eq!(none.size().await.unwrap(), 0, "no row holds the graph");
    assert_eq!(rec.count(&Event::ResidualSettled { none: true }), 1);
    assert_eq!(compared(&rec), 0, "no row compared");
}

/// Every in-memory Dictionary store stamps its four code columns with
/// their exact bounds, the same whether built from the quads, loaded from
/// the file in memory, or adopted from its bytes, and the zone maps'
/// smallest and largest codes; a Default-layout store, whose columns hold
/// strings, stamps none.
#[tokio::test]
async fn built_and_loaded_stores_stamp_the_same_bounds() {
    use super::located_zones::{Graphs, zone_quads};

    let quads = zone_quads(Graphs::Stretches);
    let built = built_store(quads.clone(), LayoutStrategy::Dictionary, vec![]).await;
    let (_dir, path) = write_store_file(quads.clone(), LayoutStrategy::Dictionary, vec![]).await;
    let loaded = VortexRdfStore::from_file_in_memory(&path).await.unwrap();
    let bytes = std::fs::read(&path).unwrap();
    let adopted = VortexRdfStore::from_bytes(&bytes).await.unwrap();
    for column in ["s", "p", "o", "g"] {
        let zones = built.debug_zone_bounds(column).expect("built zones");
        let folded = (
            zones.iter().map(|z| z.0).min().unwrap(),
            zones.iter().map(|z| z.1).max().unwrap(),
        );
        assert_eq!(
            built.debug_column_bounds(column),
            Some(folded),
            "built {column}"
        );
        for (label, store) in [("loaded", &loaded), ("adopted", &adopted)] {
            assert_eq!(
                store.debug_column_bounds(column),
                Some(folded),
                "{label} {column}"
            );
        }
    }
    let strings = built_store(quads, LayoutStrategy::Default, vec![]).await;
    for column in ["s", "p", "o", "g"] {
        assert_eq!(
            strings.debug_column_bounds(column),
            None,
            "Default {column}"
        );
    }
}

/// `(? type ? g1)` on a one-graph store with `index:posg`: the index
/// resolves the predicate, and the graph left over is settled with no row
/// compared, the run's zones of the component's graph column each holding
/// the one graph. The view still answers in base row order, as with zones
/// off (the index's serve plan dropped, as a residual drops it), and keeps
/// the store's indexes.
#[tokio::test]
async fn settled_residuals_keep_base_order() {
    use super::located_zones::{Graphs, RUN, zone_quads};
    use crate::store::test_hooks::ForcedZones;

    let store = built_store(
        zone_quads(Graphs::One),
        LayoutStrategy::Dictionary,
        IndexConfig::Copy.types(),
    )
    .await;
    let (ty, g1) = (iri("type"), iri_graph("g1"));
    let read = |on: bool| {
        let (store, ty, g1) = (&store, &ty, &g1);
        async move {
            let _zones = ForcedZones::set(on);
            let rec = Recorder::start();
            let view = store
                .match_pattern(None, Some(ty), None, Some(g1))
                .await
                .unwrap();
            let rows = row_list(&view).await;
            let compared: u64 = rec
                .picked(|e| match e {
                    Event::ResidualCompared(rows) | Event::ComponentChecked(rows) => Some(*rows),
                    _ => None,
                })
                .iter()
                .sum();
            (rows, view.indexes().to_vec(), compared)
        }
    };
    let (on, on_indexes, settled) = read(true).await;
    let (off, off_indexes, compared) = read(false).await;
    assert_eq!(on.len(), RUN, "every type row");
    assert_eq!(on, off, "the rows and their order, as with zones off");
    let mut sorted = on.clone();
    sorted.sort_unstable();
    assert_eq!(on, sorted, "base row order");
    assert_eq!(on_indexes, store.indexes(), "the view keeps its indexes");
    assert_eq!(off_indexes, store.indexes());
    assert_eq!(settled, 0, "no row compared with zones on");
    assert!(compared > 0, "the graph compared with zones off");
}
