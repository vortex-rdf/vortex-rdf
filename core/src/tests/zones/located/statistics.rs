//! What a located run asks of its file's statistics, and when: the footer,
//! the zone maps read once per file, the gap cap, the first block read
//! before any statistics, and the work counters.
use super::*;

/// Each zone map a file's reads ask for is read once per file and column,
/// whoever asks: `(? type ? g1)` counted twice reads `index:posg`'s `g` map
/// once by copy, `index:ref-p`'s once by reference, and two such counts
/// joined on one task, on a store opened afresh, read it once in all; with
/// no index, the general match's every-row step and a gap run of the quad
/// table checked on `g` share the quad table's `g` map. Every count is the
/// zones-off count.
#[tokio::test]
async fn each_zone_map_reads_once_per_file() {
    use crate::CodePattern;
    use crate::store::test_hooks::ForcedZones;

    for index in [IndexConfig::Copy, IndexConfig::Reference] {
        let tag = index.name();
        let (_dir, path) = zone_file(ZoneFile::new(Graphs::Alternating, index)).await;
        let store = VortexRdfStore::from_file(&path).await.unwrap();
        let ty = code_of(&store, "<http://example.org/type>").await;
        let g1 = code_of(&store, "<http://example.org/g1>").await;
        let pattern = CodePattern::new(None, Some(ty), None, Some(g1));
        let count = || async { store.count_code_pattern(pattern).await.unwrap() };
        let (want, rec) = zones_agree(tag, count).await;
        assert_eq!(
            zone_maps_read(&rec),
            1,
            "{tag}: the first count reads the map"
        );
        drop(rec);
        let (_, rec) = zones_agree(tag, count).await;
        assert_eq!(zone_maps_read(&rec), 0, "{tag}: a second count reads none");
        drop(rec);
        let fresh = VortexRdfStore::from_file(&path).await.unwrap();
        let _on = ForcedZones::set(true);
        let rec = Recorder::start();
        let (first, second) = futures::join!(
            fresh.count_code_pattern(pattern),
            fresh.count_code_pattern(pattern)
        );
        assert_eq!((first.unwrap(), second.unwrap()), (want, want), "{tag}");
        assert_eq!(
            zone_maps_read(&rec),
            1,
            "{tag}: joined counts read the map once"
        );
    }
    let (_dir, path) = zone_file(ZoneFile::new(Graphs::Alternating, IndexConfig::NoIndex)).await;
    let store = &VortexRdfStore::from_file(&path).await.unwrap();
    let hub = code_of(store, "<http://example.org/hub>").await;
    let g1 = code_of(store, "<http://example.org/g1>").await;
    let every_row = CodePattern::new(None, None, None, Some(g1));
    let gap = CodePattern::new(Some(hub), None, None, Some(g1));
    let count = |pattern| async move { store.count_code_pattern(pattern).await.unwrap() };
    let (_, rec) = zones_agree("the every-row step", || count(every_row)).await;
    assert_eq!(zone_maps_read(&rec), 1, "the every-row step reads g's map");
    drop(rec);
    let (_, rec) = zones_agree("the gap run", || count(gap)).await;
    assert_eq!(zone_maps_read(&rec), 0, "the gap run shares it");
}

/// Runs with no checks never ask the file's statistics, however long: by
/// copy, `(? type ? ?)` is an exact run of `index:posg`; by reference, the
/// run of `index:ref-p`'s `type` alone. Counted and streamed, each answers
/// as the term path.
#[tokio::test]
async fn runs_without_checks_ask_no_statistics() {
    use crate::CodePattern;
    use crate::store::test_hooks::ForcedZones;

    for index in [IndexConfig::Copy, IndexConfig::Reference] {
        let tag = index.name();
        let (_dir, path) = zone_file(ZoneFile::new(Graphs::Alternating, index)).await;
        let store = &VortexRdfStore::from_file(&path).await.unwrap();
        let ty = code_of(store, "<http://example.org/type>").await;
        let pattern = CodePattern::new(None, Some(ty), None, None);
        let mut want = term_rows(store, pattern).await;
        want.sort_unstable();
        assert_eq!(want.len(), RUN, "{tag}: every type row");
        let _on = ForcedZones::set(true);
        let rec = Recorder::start();
        assert_eq!(
            store.count_code_pattern(pattern).await.unwrap(),
            RUN as u64,
            "{tag}"
        );
        let mut got = streamed(store, pattern, 4096).await;
        assert_eq!(statistics_asked(&rec), 0, "{tag}: no statistics asked");
        got.sort_unstable();
        assert_eq!(got, want, "{tag}");
    }
}

/// A long checked run's stream asks the file's statistics once, on its first
/// pull, after a 64-row first page, read from the run's first block, asked
/// none: `(? type ? g1)` by copy on alternating graphs. Its rows are the
/// term path's.
#[tokio::test]
async fn streams_ask_statistics_once() {
    use crate::CodePattern;
    use crate::store::test_hooks::ForcedZones;

    if !locate_enabled() {
        return;
    }

    let (_dir, path) = zone_file(ZoneFile::new(Graphs::Alternating, IndexConfig::Copy)).await;
    let store = &VortexRdfStore::from_file(&path).await.unwrap();
    let ty = code_of(store, "<http://example.org/type>").await;
    let g1 = code_of(store, "<http://example.org/g1>").await;
    let pattern = CodePattern::new(None, Some(ty), None, Some(g1));
    let _on = ForcedZones::set(true);
    let rec = Recorder::start();
    let page = code_rows::<4>(
        &store
            .match_code_pattern(pattern, 0, Some(64))
            .await
            .unwrap(),
    );
    assert_eq!(page.len(), 64);
    assert_eq!(statistics_asked(&rec), 0, "the first page asks none");
    drop(rec);
    let rec = Recorder::start();
    let mut got = streamed(store, pattern, 4096).await;
    assert_eq!(statistics_asked(&rec), 1, "the stream asks once");
    drop(rec);
    let mut want = term_rows(store, pattern).await;
    got.sort_unstable();
    want.sort_unstable();
    assert_eq!(got, want);
}

/// The footer settles a checked slot for the whole file before any zone map
/// is read: on a one-graph store a graph check needs no reading at all, and a
/// graph code no row holds (here the `type` predicate's) matches nothing,
/// still from the located run's table.
#[tokio::test]
async fn footer_settles_a_whole_file() {
    use crate::CodePattern;
    use crate::store::test_hooks::ForcedZones;

    if !locate_enabled() {
        return;
    }

    let _on = ForcedZones::set(true);
    for fixture in zone_stores().await {
        let store = &fixture.store;
        let patterns = zone_patterns(store, 1).await;
        // The copy index's posg run; on a reference store `index:ref-p`'s
        // `type` run, checked against the graph alone; else the quad
        // table's (hub, link) run.
        let located = match fixture.index {
            IndexConfig::Copy => patterns[0],
            IndexConfig::Reference => patterns[4],
            IndexConfig::NoIndex => patterns[1],
        };
        let ty = code_of(store, "<http://example.org/type>").await;
        let held_by_none = CodePattern {
            graph: Some(ty),
            ..located
        };
        let rec = Recorder::start();
        assert_eq!(
            store.count_code_pattern(held_by_none).await.unwrap(),
            0,
            "{}",
            fixture.tag
        );
        let answered = store.locate_codes(held_by_none, 0, Some(0)).await.unwrap();
        assert_eq!(
            answered.map(|l| l.total),
            Some(0),
            "{}: still located",
            fixture.tag
        );
        if !fixture.tag.ends_with("leaves") {
            assert_eq!(
                zone_maps_read(&rec),
                0,
                "{}: the footer settles it",
                fixture.tag
            );
            assert_eq!(zone_work(&rec), (0, 0), "{}", fixture.tag);
        }
        drop(rec);
        if fixture.graphs == Graphs::One {
            let rec = Recorder::start();
            assert_eq!(store.count_code_pattern(located).await.unwrap(), RUN as u64);
            assert_eq!(zone_maps_read(&rec), 0, "{}: one graph", fixture.tag);
            assert_eq!(zone_work(&rec), (0, 0), "{}: one graph", fixture.tag);
            assert_eq!(rec.count_where(|e| matches!(e, Event::LocatedBlock(_))), 0);
        }
    }
}

/// `GAP_ROWS` caps only a run whose checks zones cannot decide: with the cap
/// forced below the run, the copy index's run is still located with zones,
/// and declines without them, as does a run whose column has no zone map.
/// Counts answer alike either way.
#[tokio::test]
async fn gap_cap_only_where_zones_cannot_decide() {
    use crate::store::test_hooks::{ForcedGapRows, ForcedZones};

    if !locate_enabled() {
        return;
    }

    let _cap = ForcedGapRows::set(1_000);
    for fixture in zone_stores().await {
        let store = &fixture.store;
        let patterns = zone_patterns(store, 1).await;
        let (pattern, zoned) = match (
            fixture.graphs,
            fixture.index,
            fixture.tag.ends_with("leaves"),
        ) {
            (Graphs::Alternating, IndexConfig::Copy, _) => (patterns[0], true),
            (_, _, true) => (patterns[1], false),
            _ => continue,
        };
        let want = term_rows(store, pattern).await.len() as u64;
        {
            let _on = ForcedZones::set(true);
            let located = store.locate_codes(pattern, 0, Some(0)).await.unwrap();
            assert_eq!(
                located.is_some(),
                zoned,
                "{}: located with zones",
                fixture.tag
            );
            assert_eq!(store.count_code_pattern(pattern).await.unwrap(), want);
        }
        let _off = ForcedZones::set(false);
        assert!(
            store
                .locate_codes(pattern, 0, Some(0))
                .await
                .unwrap()
                .is_none(),
            "{}",
            fixture.tag
        );
        assert_eq!(store.count_code_pattern(pattern).await.unwrap(), want);
    }
}

/// With `VORTEX_RDF_COUNTERS=1`, a zoned count is counted where it settles
/// zones and counts mixed ones (a no-op without the variable).
#[tokio::test]
async fn zone_counters_count_decisions() {
    use crate::counters::{ZONE_ALL, ZONE_COUNTED, ZONE_NONE, ZONE_RECORDED, snapshot};
    use crate::store::test_hooks::ForcedZones;
    use vortex_rdf_encoded_search::counters::on;

    if !on() {
        return;
    }
    let _on = ForcedZones::set(true);
    let (_dir, path) = zone_file(ZoneFile::new(Graphs::Long, IndexConfig::Copy)).await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let taxa = zone_patterns(&store, 1).await[0];
    let before = snapshot();
    assert_eq!(store.count_code_pattern(taxa).await.unwrap(), 20_000);
    let after = snapshot();
    for site in [ZONE_ALL, ZONE_NONE, ZONE_COUNTED] {
        assert!(after[site] > before[site], "site {site}");
    }
    // Alternating graphs mix every zone: the whole ones count from their
    // records.
    let (_dir, path) = zone_file(ZoneFile::new(Graphs::Alternating, IndexConfig::Copy)).await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let taxa = zone_patterns(&store, 1).await[0];
    let before = snapshot();
    assert_eq!(store.count_code_pattern(taxa).await.unwrap(), 10_000);
    assert!(snapshot()[ZONE_RECORDED] > before[ZONE_RECORDED]);
}

/// A gap run shorter than a zone checks its few rows directly: asking the
/// footer and the zone maps would cost a short probe (a join's per-binding
/// lookup) more than the rows. It reads no zone map, answers as without
/// zones, and its rows are checked.
#[tokio::test]
async fn short_runs_check_rows_without_statistics() {
    use crate::CodePattern;

    if !locate_enabled() {
        return;
    }

    for index in [IndexConfig::NoIndex, IndexConfig::Copy] {
        let (_dir, path) = zone_file(ZoneFile::new(Graphs::Long, index)).await;
        let store = VortexRdfStore::from_file(&path).await.unwrap();
        let ty = code_of(&store, "<http://example.org/type>").await;
        let g1 = code_of(&store, "<http://example.org/g1>").await;
        for (i, want) in [(5, 1), (30_000, 0)] {
            let s = code_of(&store, &format!("<http://example.org/s{i:05}>")).await;
            let pattern = CodePattern::new(Some(s), Some(ty), None, Some(g1));
            let tag = format!("{} s{i}", index.name());
            let read = || async {
                let count = store.count_code_pattern(pattern).await.unwrap();
                let rows = store.match_code_pattern(pattern, 0, None).await.unwrap();
                (count, code_rows::<4>(&rows))
            };
            let ((count, _), rec) = zones_agree(&tag, read).await;
            assert_eq!(count, want, "{tag}");
            assert_eq!(zone_maps_read(&rec), 0, "{tag}: no zone map");
            assert!(zone_work(&rec).1 > 0, "{tag}: its rows checked");
        }
    }
}

/// A first page that a long checked run's first block fills is read from
/// that block, row by row, before the file's statistics are asked, which
/// cost more than the block: on interleaved graphs a 64-row page reads
/// 4,096 rows and asks nothing. A first page the block leaves short (its
/// graph starts half-way), a later page and a count ask them. Every page
/// is the rows' own.
#[tokio::test]
async fn first_page_reads_the_first_block_before_statistics() {
    if !locate_enabled() {
        return;
    }
    for index in [IndexConfig::NoIndex, IndexConfig::Copy] {
        for (graphs, g, fills) in [(Graphs::Alternating, 1, true), (Graphs::Long, 2, false)] {
            let (_dir, path) = zone_file(ZoneFile::new(graphs, index)).await;
            let store = &VortexRdfStore::from_file(&path).await.unwrap();
            let patterns = zone_patterns(store, g).await;
            // The quad table's (hub, link) run, and on a copy store the
            // index:posg run.
            let patterns = match index {
                IndexConfig::Copy => &patterns[..2],
                _ => &patterns[1..2],
            };
            for (i, &pattern) in patterns.iter().enumerate() {
                let tag = format!("{graphs:?}/{} g{g} pattern {i}", index.name());
                let page = |offset| async move {
                    code_rows::<4>(
                        &store
                            .match_code_pattern(pattern, offset, Some(64))
                            .await
                            .unwrap(),
                    )
                };
                let (first, rec) = zones_agree(&tag, || page(0)).await;
                assert_eq!(first.len(), 64, "{tag}");
                if fills {
                    assert_eq!(statistics_asked(&rec), 0, "{tag}: none asked");
                    assert_eq!(zone_work(&rec), (0, 4096), "{tag}: the first block");
                } else {
                    assert_eq!(statistics_asked(&rec), 1, "{tag}: the block fell short");
                }
                drop(rec);
                let (_, rec) = zones_agree(&tag, || page(64)).await;
                assert_eq!(statistics_asked(&rec), 1, "{tag}: a later page asks");
                drop(rec);
                let count = || async { store.count_code_pattern(pattern).await.unwrap() };
                let (_, rec) = zones_agree(&tag, count).await;
                assert_eq!(statistics_asked(&rec), 1, "{tag}: a count asks");
            }
        }
    }
}
