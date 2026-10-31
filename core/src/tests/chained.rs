//! Matches chained on a view whose base still holds the pending matches of
//! one pattern over a wide run: each is one match of both patterns, which
//! never computes those pending ids, and answers what matching the second
//! pattern over the first view's rows answers, tombstones and an appended
//! tail included.

use super::indexes::copy_index_script_dataset;
use super::*;

/// 3,300 quads `s{i:04} p{i % 3} "o{i % 5}"` alternating between the copy
/// script's two graphs, beside the graphs: a predicate's run holds 1,100
/// rows, past the 1,024 a match names its pattern from.
fn wide_chain_dataset() -> (Vec<Quad>, [GraphName; 2]) {
    let graphs = [iri_graph("g0"), iri_graph("g1")];
    (graph_modular_quads(3_300, 4, 3, 5, &graphs), graphs)
}

/// The stores of `quads()` the chains run on, each beside its name: every
/// layout with each index, built in memory and opened from a file, each file
/// written once per process into one of `files` (see [`cached_store_file`]).
/// Keep the `TempDir`s alive while the stores are used.
async fn chain_stores(
    files: &'static [OnceLock<Vec<u8>>; 6],
    quads: fn() -> Vec<Quad>,
) -> (Vec<tempfile::TempDir>, Vec<(String, VortexRdfStore)>) {
    let indexes = [IndexType::SecondaryByCopy, IndexType::SecondaryByReference];
    let configs = LAYOUTS
        .into_iter()
        .flat_map(|layout| indexes.map(|index| (layout, index)));
    let mut dirs = Vec::new();
    let mut stores = Vec::new();
    for ((layout, index), cell) in configs.zip(files) {
        let memory = built_store(quads(), layout, vec![index]).await;
        stores.push((format!("{layout:?} {index:?} memory"), memory));
        let (dir, path) = cached_store_file(cell, quads, layout, vec![index]).await;
        let file = VortexRdfStore::from_file(&path).await.unwrap();
        stores.push((format!("{layout:?} {index:?} file"), file));
        dirs.push(dir);
    }
    (dirs, stores)
}

/// [`chain_stores`] of [`wide_chain_dataset`].
async fn wide_chain_stores() -> (Vec<tempfile::TempDir>, Vec<(String, VortexRdfStore)>) {
    static FILES: [OnceLock<Vec<u8>>; 6] = [const { OnceLock::new() }; 6];
    chain_stores(&FILES, || wide_chain_dataset().0).await
}

/// A match chained on the predicate's view of [`wide_chain_dataset`]:
/// wherever that view's ids are pending (a served copy run), every chained
/// pattern that binds no role of the first one to another term is one match
/// of both, the first view's ids never computed. The rows are what the
/// second pattern selects among the first view's, read in base order with
/// no plan. A chain that binds the predicate to another term matches the
/// patterns one after the other, and selects nothing.
#[tokio::test]
async fn test_a_chained_match_is_one_match_of_both_patterns() {
    let (quads, graphs) = wide_chain_dataset();
    let (_dirs, stores) = wide_chain_stores().await;
    let (p1, p2) = (iri("p1"), iri("p2"));
    let o1 = plain_literal("o1");
    let s04 = iri_subject("s0004");
    for (tag, store) in stores {
        let by_p = matched_view(&store, None, Some(&p1), None, None).await;
        let pending = selection_pending(&by_p);
        let chains: [(&str, VortexRdfStore, Vec<String>); 4] = [
            (
                "o",
                matched_view(&by_p, None, None, Some(&o1), None).await,
                expected_strings(&quads, |i| i % 3 == 1 && i % 5 == 1),
            ),
            (
                "s",
                matched_view(&by_p, Some(&s04), None, None, None).await,
                expected_strings(&quads, |i| i == 4),
            ),
            (
                "g",
                matched_view(&by_p, None, None, None, Some(&graphs[1])).await,
                expected_strings(&quads, |i| i % 3 == 1 && i % 2 == 1),
            ),
            (
                "p again",
                matched_view(&by_p, None, Some(&p1), Some(&o1), None).await,
                expected_strings(&quads, |i| i % 3 == 1 && i % 5 == 1),
            ),
        ];
        for (name, chained, expected) in chains {
            assert_eq!(served_component(&chained), None, "{tag} {name}");
            assert!(!selection_pending(&chained), "{tag} {name}");
            assert_eq!(view_strings(&chained).await, expected, "{tag} {name}");
            assert_eq!(
                chained.size().await.unwrap(),
                expected.len(),
                "{tag} {name}"
            );
        }
        if pending {
            assert_eq!(
                by_p.debug_row_ids_materialized(),
                Some(false),
                "{tag}: the first view's ids are never computed"
            );
        }

        let rec = Recorder::start();
        let other = matched_view(&by_p, None, Some(&p2), None, None).await;
        assert_eq!(rec.count(&Event::ChainMerged), 0, "{tag}: p2 after p1");
        assert_eq!(view_strings(&other).await, Vec::<String>::new(), "{tag}");
        assert_eq!(other.size().await.unwrap(), 0, "{tag}");
    }
}

/// How many chains are one match of both patterns: one for each chain on a
/// pending view of [`wide_chain_dataset`], none on a view whose ids the
/// first match computed (a reference index), and, over the copy script's 30
/// quads, one only on a pending run of unknown width (a file run the
/// resolver did not locate): a known run of ten rows costs less to compute
/// than to name.
#[tokio::test]
async fn test_only_a_wide_pending_view_chains_as_one_match() {
    let (p1, o1) = (iri("p1"), plain_literal("o1"));
    let (narrow, _) = copy_index_script_dataset();
    static NARROW_FILES: [OnceLock<Vec<u8>>; 6] = [const { OnceLock::new() }; 6];
    let (_narrow_dirs, narrow_stores) =
        chain_stores(&NARROW_FILES, || copy_index_script_dataset().0).await;
    let mut narrow_known = 0;
    for (tag, store) in narrow_stores {
        let by_p = matched_view(&store, None, Some(&p1), None, None).await;
        let width = by_p.debug_pending_len();
        narrow_known += usize::from(matches!(width, Some(Some(_))));
        let rec = Recorder::start();
        let chained = matched_view(&by_p, None, None, Some(&o1), None).await;
        assert_eq!(
            rec.count(&Event::ChainMerged),
            usize::from(width == Some(None)),
            "{tag}"
        );
        assert_eq!(
            view_strings(&chained).await,
            expected_strings(&narrow, |i| i % 15 == 1),
            "{tag}"
        );
    }
    assert!(
        narrow_known >= 3,
        "the narrow in-memory copy runs are pending"
    );

    let (quads, _) = wide_chain_dataset();
    let (_dirs, stores) = wide_chain_stores().await;
    let mut pending_views = 0;
    for (tag, store) in stores {
        let by_p = matched_view(&store, None, Some(&p1), None, None).await;
        let pending = selection_pending(&by_p);
        pending_views += usize::from(pending);
        let rec = Recorder::start();
        let chained = matched_view(&by_p, None, None, Some(&o1), None).await;
        assert_eq!(
            rec.count(&Event::ChainMerged),
            usize::from(pending),
            "{tag}"
        );
        // A chain on the chained view, whose ids are exact, composes.
        let again = matched_view(&chained, None, None, Some(&o1), None).await;
        assert_eq!(
            rec.count(&Event::ChainMerged),
            usize::from(pending),
            "{tag}"
        );
        assert_eq!(
            view_strings(&again).await,
            expected_strings(&quads, |i| i % 15 == 1),
            "{tag}"
        );

        // A probe on the predicate's view, counted or windowed, is one match
        // of both patterns too.
        let probe = Probe::new(None, None, Some(o1.clone()), None);
        assert_eq!(by_p.count_probe(&probe).await.unwrap(), 220, "{tag}");
        let first = Probe {
            limit: Some(1),
            ..probe
        };
        let windowed = by_p.run_probe(&first).await.unwrap();
        assert_eq!(
            view_strings(&windowed).await,
            expected_strings(&quads, |i| i == 1),
            "{tag}"
        );
        assert_eq!(
            rec.count(&Event::ChainMerged),
            3 * usize::from(pending),
            "{tag}"
        );
    }
    // Every layout's copy index, in memory and on file.
    assert_eq!(pending_views, 6);
}

/// A chained match over a store with a tombstoned row and an appended quad:
/// the deleted row stays out and the tail's row that both patterns select
/// comes in, as when the patterns match one after the other.
#[tokio::test]
async fn test_a_chained_match_keeps_tombstones_and_the_tail() {
    let (quads, graphs) = wide_chain_dataset();
    let (_dirs, stores) = wide_chain_stores().await;
    let (p1, o1) = (iri("p1"), plain_literal("o1"));
    let appended = make_quad(
        "http://example.org/s99",
        "http://example.org/p1",
        "o1",
        graphs[0].clone(),
    );
    for (tag, store) in stores {
        let store = store
            .delete_quad(&quads[16])
            .await
            .unwrap()
            .add_quad(appended.clone())
            .await
            .unwrap();
        let by_p = matched_view(&store, None, Some(&p1), None, None).await;
        let chained = matched_view(&by_p, None, None, Some(&o1), None).await;
        let mut expected = expected_strings(&quads, |i| i % 15 == 1 && i != 16);
        expected.push(appended.to_string());
        expected.sort();
        assert_eq!(view_strings(&chained).await, expected, "{tag}");
        assert_eq!(chained.size().await.unwrap(), expected.len(), "{tag}");
    }
}
