//! Planner-facing metadata: generations and dictionary identities across
//! views, opens and mutations, view statistics against the views' reads, the
//! index child roster, and partitions that tile a view.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;

use super::*;
use crate::store::{
    IndexComponentInfo, Keep, QuadColumn, RowCountHint, SelectionKind, SortOrder, TermCode,
};

/// Every view of `store` — the planner views, a match kept and windowed, a
/// partition and the empty view — carries its generation and reads its
/// dictionary, through the reader and, when it is resident, its snapshot.
async fn assert_views_share_identity(store: &VortexRdfStore, tag: &str) {
    let generation = store.generation();
    let dictionary = store.dict_reader().unwrap().dictionary_id();
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let kept = store
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap()
        .keep(QuadColumn::S, &Keep::range(0..TermCode::MAX))
        .await
        .unwrap();
    let mut views = views_of(store).await;
    views.push(("kept match, windowed", kept.window(3, 10).await.unwrap()));
    views.push(("kept match", kept));
    let mut parts = store
        .partitions(NonZeroUsize::new(3).unwrap())
        .await
        .unwrap();
    views.push(("partition", parts.swap_remove(1)));
    views.push(("empty view", store.empty_view()));
    for (name, view) in &views {
        assert_eq!(
            view.generation(),
            generation,
            "{tag} {name}: a view keeps the generation"
        );
        let reader = view.dict_reader().unwrap();
        assert_eq!(
            reader.dictionary_id(),
            dictionary,
            "{tag} {name}: one vocabulary"
        );
        assert_eq!(
            reader.snapshot().is_some(),
            !reader.is_file_backed(),
            "{tag} {name}"
        );
        if let Some(snapshot) = reader.snapshot() {
            assert_eq!(
                snapshot.dictionary_id(),
                dictionary,
                "{tag} {name}: its snapshot too"
            );
        }
    }
}

/// Views share their store's generation and dictionary. A build, an open, an
/// append, a delete and a compaction each make new data; a build, an open and
/// a compaction each a new dictionary, which a delete keeps.
#[tokio::test]
async fn test_generation_and_dictionary_identity() {
    let quads = fixture_quads();
    let store = built_store(
        quads.clone(),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    let other = built_store(quads.clone(), LayoutStrategy::Dictionary, vec![]).await;
    assert_ne!(
        store.generation(),
        other.generation(),
        "two builds, two generations"
    );
    let dictionary = store.dict_reader().unwrap().dictionary_id();
    assert_ne!(
        dictionary,
        other.dict_reader().unwrap().dictionary_id(),
        "two builds, two dictionaries"
    );
    assert_ne!(
        VortexRdfStore::empty().generation(),
        VortexRdfStore::empty().generation(),
        "two empty stores, two generations"
    );
    assert_views_share_identity(&store, "built").await;

    let tailed = store.add_quads(appended_quads(5)).await.unwrap();
    assert_ne!(
        tailed.generation(),
        store.generation(),
        "an append is new data"
    );
    let deleted = store.delete_quad(&quads[0]).await.unwrap();
    assert_ne!(
        deleted.generation(),
        store.generation(),
        "a delete is new data"
    );
    assert_ne!(deleted.generation(), tailed.generation());
    assert_eq!(
        deleted.dict_reader().unwrap().dictionary_id(),
        dictionary,
        "a delete keeps the dictionary"
    );
    let compacted = tailed.compact().await.unwrap();
    assert_ne!(
        compacted.generation(),
        tailed.generation(),
        "a compaction is new data"
    );
    assert_ne!(
        compacted.dict_reader().unwrap().dictionary_id(),
        dictionary,
        "a compaction re-encodes against a new dictionary"
    );

    {
        use std::collections::BTreeSet;

        // Every open of a file is new data over its own dictionary, which
        // the open's views share.
        let stores = file_stores().await;
        let generations: BTreeSet<u64> = stores.iter().map(|(_, _, s)| s.generation()).collect();
        let dictionaries: BTreeSet<u64> = stores
            .iter()
            .map(|(_, _, s)| s.dict_reader().unwrap().dictionary_id())
            .collect();
        assert_eq!(generations.len(), stores.len(), "every open is new data");
        assert_eq!(
            dictionaries.len(),
            stores.len(),
            "every open reads its own dictionary"
        );
        for (tag, _dir, store) in &stores {
            assert_views_share_identity(store, tag).await;
        }

        // The same file opened twice; a delete on a file, which keeps its
        // dictionary, and its compaction, which rewrites the file and opens
        // it again.
        let (_dir, path) =
            write_store_file(quads.clone(), LayoutStrategy::Dictionary, vec![]).await;
        let mapped = VortexRdfStore::from_file(&path).await.unwrap();
        let again = VortexRdfStore::from_file(&path).await.unwrap();
        let dictionary = mapped.dict_reader().unwrap().dictionary_id();
        assert_ne!(
            again.generation(),
            mapped.generation(),
            "a second open is new data"
        );
        assert_ne!(
            again.dict_reader().unwrap().dictionary_id(),
            dictionary,
            "a second open reads a second dictionary"
        );
        let deleted = mapped.delete_quad(&quads[0]).await.unwrap();
        assert_ne!(
            deleted.generation(),
            mapped.generation(),
            "a delete on a file is new data"
        );
        assert_eq!(
            deleted.dict_reader().unwrap().dictionary_id(),
            dictionary,
            "a delete on a file keeps its dictionary"
        );
        let compacted = deleted.compact().await.unwrap();
        assert_ne!(
            compacted.generation(),
            deleted.generation(),
            "a file's compaction is new data"
        );
        assert_ne!(
            compacted.dict_reader().unwrap().dictionary_id(),
            dictionary,
            "a file's compaction reads a new dictionary"
        );
    }
}

/// `view`'s row count hint against its size: a bound the size stays within,
/// and the size itself wherever it is exact.
async fn assert_rows_bound(view: &VortexRdfStore, stats: &crate::store::ViewStatistics, tag: &str) {
    let size = view.size().await.unwrap();
    assert!(
        stats.rows.upper_bound >= size,
        "{tag}: {:?} bounds {size} rows",
        stats.rows
    );
    if stats.rows.exact.is_some() {
        assert_eq!(stats.rows, RowCountHint::exact(size), "{tag}");
    }
}

/// In memory every count is exact and every view keeps its store's order and
/// generation; a run of the sorted base is bounded by its subject codes, a
/// served match names the index child serving it, and tombstones and a tail
/// are counted.
#[tokio::test]
async fn test_view_statistics_in_memory() {
    let quads = fixture_quads();
    let store = built_store(
        quads.clone(),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    let mut deleted = store.clone();
    for quad in quads.iter().step_by(50).take(20) {
        deleted = deleted.delete_quad(quad).await.unwrap();
    }
    for (base, tombstones) in [(&store, 0), (&deleted, 20)] {
        for (name, view) in views_of(base).await {
            let tag = format!("{tombstones} tombstones, {name}");
            let stats = view.view_statistics().unwrap();
            assert_eq!(
                stats.rows,
                RowCountHint::exact(view.size().await.unwrap()),
                "{tag}"
            );
            assert_eq!(stats.sort_order, Some(SortOrder::Spog), "{tag}");
            assert_eq!(stats.tombstones, tombstones, "{tag}");
            assert!(!stats.pending_filter && !stats.file_backed, "{tag}");
            assert_eq!(stats.tail_rows, 0, "{tag}");
            assert_eq!(stats.generation, base.generation(), "{tag}");
        }
    }

    let views: BTreeMap<&str, VortexRdfStore> = views_of(&store).await.into_iter().collect();
    let stats_of = |name: &str| views[name].view_statistics().unwrap();
    let whole = stats_of("whole");
    assert_eq!(whole.selection, SelectionKind::All);
    assert_eq!(whole.served_component, None);
    let all = base_ordered_codes(&store).await;
    assert_eq!(
        whole.code_bounds,
        [Some((all[0][0], all[all.len() - 1][0])), None, None, None],
        "the sorted subject column's extremes"
    );
    // A subject prefix search: one run, bounded by its one subject.
    let subject = stats_of("subject");
    let s_code = store
        .dict_reader()
        .unwrap()
        .encode("<http://example.org/s0017>")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(subject.selection, SelectionKind::Range);
    assert_eq!(subject.code_bounds[0], Some((s_code, s_code)));
    // Served matches: a pending run, the index child serving it named.
    let predicate = stats_of("predicate");
    assert_eq!(predicate.selection, SelectionKind::PendingIndexRun);
    assert_eq!(predicate.served_component, Some("index:posg"));
    assert_eq!(predicate.code_bounds, [None; 4]);
    assert_eq!(stats_of("object").served_component, Some("index:ospg"));
    // A match chained onto a served one reads its ids.
    let chained = stats_of("chained");
    assert_eq!(chained.selection, SelectionKind::Ids);
    assert_eq!(chained.served_component, None);
    assert_eq!(
        deleted.view_statistics().unwrap().code_bounds,
        [None; 4],
        "tombstones leave the run's extremes unknown"
    );

    // The tail is counted after the base, and a string layout has no codes.
    // The tail's rows follow the base unsorted: a view that reads any of
    // them has no sort order, one that reads none keeps the base's.
    let base = built_store(quads.clone(), LayoutStrategy::Default, vec![]).await;
    let strings = base.add_quads(appended_quads(7)).await.unwrap();
    assert_eq!(
        base.view_statistics().unwrap().sort_order,
        Some(SortOrder::Spog),
        "a sorted base without a tail"
    );
    let stats = strings.view_statistics().unwrap();
    assert_eq!(stats.tail_rows, 7);
    assert_eq!(stats.rows, RowCountHint::exact(quads.len() + 7));
    assert_eq!(stats.sort_order, None, "a sorted base with a tail");
    assert_eq!(
        stats.code_bounds, [None; 4],
        "no codes under a string layout"
    );
    let base_rows_only = strings
        .match_pattern(Some(&subject_node(17, 4)), None, None, None)
        .await
        .unwrap()
        .view_statistics()
        .unwrap();
    assert_eq!(
        (base_rows_only.tail_rows, base_rows_only.sort_order),
        (0, Some(SortOrder::Spog)),
        "a tailed store's view that reads no tail row"
    );
}

/// On file a count is exact unless a pushed-down filter has rows still to
/// test, when the selection's live rows bound it, or the base has tombstones
/// and a pending run's ids are unread, when the run's width bounds it; a
/// pushed-down equality bounds its column by its code, and a served match
/// names the index child serving it. Every open of a file describes its views
/// as the file's own.
#[tokio::test]
async fn test_view_statistics_file_backed() {
    let quads = fixture_quads();
    let stores = file_stores().await;
    for (tag, _dir, store) in &stores {
        let mapped = tag.ends_with("/mapped");
        let mut deleted = store.clone();
        for quad in quads.iter().step_by(100).take(10) {
            deleted = deleted.delete_quad(quad).await.unwrap();
        }
        for (base, tombstones) in [(store, 0), (&deleted, 10)] {
            for (name, view) in views_of(base).await {
                let tag = format!("{tag} {tombstones} tombstones, {name}");
                let stats = view.view_statistics().unwrap();
                assert_rows_bound(&view, &stats, &tag).await;
                if !stats.pending_filter && stats.selection != SelectionKind::PendingIndexRun {
                    assert!(
                        stats.rows.exact.is_some(),
                        "{tag}: an exact selection with no filter pending counts exactly"
                    );
                }
                assert_eq!(stats.file_backed, mapped, "{tag}");
                assert_eq!(stats.sort_order, Some(SortOrder::Spog), "{tag}");
                assert_eq!(stats.tombstones, tombstones, "{tag}");
                assert_eq!(stats.generation, base.generation(), "{tag}");
            }
        }
    }

    let open = |want: &str| {
        stores
            .iter()
            .find(|(tag, _, _)| tag == want)
            .map(|(_, _, store)| store.clone())
            .unwrap()
    };
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let store = open("file/no-index/mapped");
    let whole = store.view_statistics().unwrap();
    assert_eq!(whole.rows, RowCountHint::exact(quads.len()));
    assert!(whole.file_backed && !whole.pending_filter);
    assert_eq!(
        whole.code_bounds, [None; 4],
        "bounds that would need a read"
    );
    // The tail's rows follow the file's sorted rows unsorted.
    assert_eq!(
        whole.sort_order,
        Some(SortOrder::Spog),
        "a sorted file without a tail"
    );
    let tailed = store.add_quads(appended_quads(3)).await.unwrap();
    let stats = tailed.view_statistics().unwrap();
    assert_eq!(
        (stats.tail_rows, stats.sort_order),
        (3, None),
        "a sorted file with a tail"
    );
    // Unindexed, a predicate match pushes a filter down: the rows it still
    // has to test bound the count, and its code bounds the column.
    let by_p = store
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    let stats = by_p.view_statistics().unwrap();
    let p_code = store
        .dict_reader()
        .unwrap()
        .encode("<http://example.org/p1>")
        .await
        .unwrap()
        .unwrap();
    assert!(stats.pending_filter);
    assert_eq!(stats.rows, RowCountHint::at_most(quads.len()));
    assert_eq!(
        stats.code_bounds,
        [None, Some((p_code, p_code)), None, None]
    );
    // Rows appended under the predicate add to the bound: the base
    // selection's rows, then the tail's.
    let p1_tail: Vec<Quad> = (0..3)
        .map(|i| {
            make_quad(
                &format!("http://example.org/tail{i}"),
                "http://example.org/p1",
                "late",
                GraphName::DefaultGraph,
            )
        })
        .collect();
    let by_p = store
        .add_quads(p1_tail.clone())
        .await
        .unwrap()
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    let stats = by_p.view_statistics().unwrap();
    assert!(stats.pending_filter);
    assert_eq!(stats.tail_rows, 3);
    assert_eq!(stats.rows, RowCountHint::at_most(quads.len() + 3));

    // Indexed, the match is served and its count exact.
    let copy = open("file/copy/mapped");
    let by_p = copy
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    let stats = by_p.view_statistics().unwrap();
    assert_eq!(stats.selection, SelectionKind::PendingIndexRun);
    assert_eq!(stats.served_component, Some("index:posg"));
    assert_eq!(stats.rows, RowCountHint::exact(by_p.size().await.unwrap()));
    assert!(!stats.pending_filter);
    // A bound graph rides the served run as a filter: the run's 600 rows
    // bound the count.
    let g1 = GraphName::NamedNode(NamedNode::new("http://example.org/g1").unwrap());
    let by_pg = copy
        .match_pattern(None, Some(&p1), None, Some(&g1))
        .await
        .unwrap();
    let stats = by_pg.view_statistics().unwrap();
    assert_eq!(stats.served_component, Some("index:posg"));
    assert!(stats.pending_filter);
    assert_eq!(stats.rows, RowCountHint::at_most(600));
    assert_eq!(by_pg.size().await.unwrap(), 200);
    // A tombstone in the served run: the run's width bounds its live rows
    // until its ids are read, which counts them.
    let deleted = copy.delete_quad(&quads[1]).await.unwrap();
    let by_p = deleted
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    let rows = |view: &VortexRdfStore| view.view_statistics().unwrap().rows;
    assert_eq!(rows(&by_p), RowCountHint::at_most(600));
    assert_eq!(by_p.size().await.unwrap(), 599);
    assert_eq!(rows(&by_p), RowCountHint::exact(599));
    // With rows appended under the predicate, the bound is the run's width
    // and the tail's rows.
    let by_p = deleted
        .add_quads(p1_tail)
        .await
        .unwrap()
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    assert_eq!(rows(&by_p), RowCountHint::at_most(600 + 3));
    assert_eq!(by_p.size().await.unwrap(), 602);
}

/// The children each index set holds, in the order a build emits them.
fn roster(indexes: &[IndexType], rows: usize) -> Vec<IndexComponentInfo> {
    let child = |name, index, sort_order| IndexComponentInfo {
        name,
        index,
        sort_order,
        sorted: true,
        rows,
    };
    indexes
        .iter()
        .flat_map(|&index| match index {
            IndexType::SecondaryByCopy => [
                child("index:posg", index, Some(SortOrder::Posg)),
                child("index:ospg", index, Some(SortOrder::Ospg)),
            ],
            IndexType::SecondaryByReference => [
                child("index:ref-o", index, None),
                child("index:ref-p", index, None),
            ],
        })
        .collect()
}

/// The index roster lists every child a store holds, one row per quad each:
/// built under every index set, and every open of a file.
#[tokio::test]
async fn test_index_components() {
    let quads = fixture_quads();
    for indexes in index_sets() {
        let store = built_store(quads.clone(), LayoutStrategy::Dictionary, indexes.clone()).await;
        assert_eq!(
            store.index_components().unwrap(),
            roster(&indexes, quads.len()),
            "built {indexes:?}"
        );
    }
    for (tag, _dir, store) in file_stores().await {
        let indexes = match tag.split('/').nth(1) {
            Some("copy") => vec![IndexType::SecondaryByCopy],
            Some("reference") => vec![IndexType::SecondaryByReference],
            _ => vec![],
        };
        assert_eq!(
            store.index_components().unwrap(),
            roster(&indexes, quads.len()),
            "{tag}"
        );
    }
}

/// A child under a known name but an implementation slug this version does
/// not know is no index child of it: every open lists and serves as a data
/// source only the children whose slug it knows, and no match reaches the
/// foreign one — a pattern only it would answer is scanned, while its known
/// sibling still serves.
#[tokio::test]
async fn test_index_components_classify_children_by_slug() {
    let quads = fixture_quads();
    let parts = built_store(
        quads.clone(),
        LayoutStrategy::Default,
        vec![IndexType::SecondaryByCopy],
    )
    .await
    .to_serializable_parts()
    .await
    .unwrap();
    let ospg = parts
        .built
        .components
        .iter()
        .find(|c| c.identity.name == "index:ospg")
        .unwrap()
        .to_write()
        .unwrap();
    let foreign = unknown_component("index:posg", "secondary-by-copy/posg-v2", false);
    let bytes = unstamped_store_bytes(vec![parts.built.array.clone()], vec![foreign, ospg]).await;
    let (_dir, path) = temp_store_file(&bytes);
    let want = roster(&[IndexType::SecondaryByCopy], quads.len())
        .into_iter()
        .filter(|child| child.name == "index:ospg")
        .collect::<Vec<_>>();
    // The copy index answers a bound predicate from `index:posg`, here the
    // foreign child, and a bound object from `index:ospg`.
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let o3 = Term::Literal(Literal::new_simple_literal("o3"));
    for (open, store) in reopened_stores(&path).await {
        assert_eq!(store.index_components().unwrap(), want, "{open}");
        // The data source names the children as the roster does.
        let source = |name: &str| store.component_data_source(name).unwrap();
        assert!(source("index:posg").is_none(), "{open}: index:posg");
        assert!(source("index:ospg").is_some(), "{open}: index:ospg");
        for (pattern, view, served, rows) in [
            (
                "predicate",
                store.match_pattern(None, Some(&p1), None, None).await,
                None,
                expected_strings(&quads, |i| i % 5 == 1),
            ),
            (
                "object",
                store.match_pattern(None, None, Some(&o3), None).await,
                Some("index:ospg"),
                expected_strings(&quads, |i| i % 7 == 3),
            ),
        ] {
            let view = view.unwrap();
            let stats = view.view_statistics().unwrap();
            assert_eq!(stats.served_component, served, "{open} {pattern}");
            assert_eq!(view_strings(&view).await, rows, "{open} {pattern}");
        }
    }
}

/// The partition counts every view is cut into: one (the view itself) and a
/// few.
const PARTITION_COUNTS: [usize; 5] = [1, 2, 3, 7, 64];

/// A partition count past any view's rows, which cuts the rows a view
/// selects into one-row partitions — under a pending filter, mostly
/// partitions without a live row: asked of the views of at most
/// [`PAST_THE_ROWS_VIEW_ROWS`] rows, which in [`views_of`] are the subject,
/// the window, the chained match and the empty window.
const PAST_THE_ROWS: usize = 100_000;

/// The most rows a view [`PAST_THE_ROWS`] is asked of has.
const PAST_THE_ROWS_VIEW_ROWS: usize = 256;

/// Partitions tile `view`: at each of [`PARTITION_COUNTS`], and at
/// [`PAST_THE_ROWS`] on a small view, at most that many and never none, read
/// in turn they give the view's base-ordered codes, and their sizes add up
/// to its size.
async fn assert_partitions_tile(view: &VortexRdfStore, tag: &str) {
    let expected = base_ordered_codes(view).await;
    let past_the_rows = (expected.len() <= PAST_THE_ROWS_VIEW_ROWS).then_some(PAST_THE_ROWS);
    for n in PARTITION_COUNTS.into_iter().chain(past_the_rows) {
        let parts = view
            .partitions(NonZeroUsize::new(n).unwrap())
            .await
            .unwrap();
        assert!(
            !parts.is_empty() && parts.len() <= n,
            "{tag}: {n} -> {}",
            parts.len()
        );
        let mut got = Vec::new();
        let mut total = 0;
        for part in &parts {
            got.extend(base_ordered_codes(part).await);
            total += part.size().await.unwrap();
        }
        assert_eq!(got, expected, "{tag}: partitions({n}) tile the view");
        assert_eq!(total, expected.len(), "{tag}: sizes add up");
    }
}

/// In memory, over every index set: partitions tile each planner view, with
/// and without tombstones, and a string layout's tail rides with the last.
#[tokio::test]
async fn test_partitions_in_memory() {
    let quads = fixture_quads();
    for indexes in [
        vec![],
        vec![IndexType::SecondaryByCopy],
        vec![IndexType::SecondaryByReference],
    ] {
        let store = built_store(quads.clone(), LayoutStrategy::Dictionary, indexes.clone()).await;
        let mut deleted = store.clone();
        for quad in quads.iter().step_by(9).take(40) {
            deleted = deleted.delete_quad(quad).await.unwrap();
        }
        for (base, tombstones) in [(&store, 0), (&deleted, 40)] {
            for (name, view) in views_of(base).await {
                assert_partitions_tile(&view, &format!("{indexes:?} {tombstones} {name}")).await;
            }
        }
    }

    let strings = built_store(quads.clone(), LayoutStrategy::Default, vec![])
        .await
        .add_quads(appended_quads(7))
        .await
        .unwrap();
    let parts = strings
        .partitions(NonZeroUsize::new(4).unwrap())
        .await
        .unwrap();
    assert_eq!(parts.len(), 4);
    let mut sizes = Vec::new();
    let mut subjects = Vec::new();
    for part in &parts {
        sizes.push(part.size().await.unwrap());
        subjects.extend(
            part.quads_vec()
                .await
                .unwrap()
                .into_iter()
                .map(|q| q.subject.to_string()),
        );
    }
    assert_eq!(sizes.iter().sum::<usize>(), quads.len() + 7);
    assert_eq!(parts[3].tail_size(), 7, "the tail rides with the last");
    assert!(parts[..3].iter().all(|p| p.tail_size() == 0));
    // Only the last partition reads tail rows, so only it loses the base's
    // order.
    let orders: Vec<Option<SortOrder>> = parts
        .iter()
        .map(|p| p.view_statistics().unwrap().sort_order)
        .collect();
    let spog = Some(SortOrder::Spog);
    assert_eq!(orders, [spog, spog, spog, None]);
    let expected: Vec<String> = strings
        .quads_vec()
        .await
        .unwrap()
        .iter()
        .map(|q| q.subject.to_string())
        .collect();
    assert_eq!(subjects, expected);
}

/// Every open of a file, under each index: partitions tile each planner view,
/// with and without tombstones — served runs, pushed-down filters and windows.
#[tokio::test]
async fn test_partitions_file_backed() {
    let quads = fixture_quads();
    for (tag, _dir, store) in file_stores().await {
        let mut deleted = store.clone();
        for quad in quads.iter().step_by(13).take(30) {
            deleted = deleted.delete_quad(quad).await.unwrap();
        }
        for (base, tombstones) in [(&store, 0), (&deleted, 30)] {
            for (name, view) in views_of(base).await {
                assert_partitions_tile(&view, &format!("{tag} {tombstones} {name}")).await;
            }
        }
    }
}
