//! Views a by-copy index serves read straight off the index, in its order,
//! and the views cut from them stay served. Underneath: the lazy id caches a
//! served view carries, which count and narrow a run without sorting its ids
//! into base order.

use super::*;
use crate::store::indexes::LazyRowIds;
use vortex_buffer::Buffer;
use vortex_mask::Mask;

/// How many times `rec` saw a pending selection's ids sorted into base order.
fn ids_sorted(rec: &Recorder) -> usize {
    rec.count_where(|e| matches!(e, Event::IdsSorted(_)))
}

#[tokio::test]
async fn unsorted_ids_sort_only_when_asked() {
    let ids = LazyRowIds::from_unsorted_ids(Buffer::from(vec![5u64, 1, 3]), true);
    let mut rec = Recorder::start();
    assert_eq!(ids.len_if_known(), Some(3));
    let deleted = Mask::from_indices(8, vec![1]);
    // Live: the ids already leave every tombstone out, so the mask is not read.
    assert_eq!(ids.live_len_without_sort(&deleted).unwrap(), Some(3));
    assert_eq!(
        rec.take_picked(|e| matches!(e, Event::IdsSorted(_)).then_some(()))
            .len(),
        0
    );
    assert_eq!(ids.materialized().unwrap().as_slice(), &[1, 3, 5]);
    ids.materialized().unwrap();
    assert_eq!(
        rec.picked(|e| matches!(e, Event::IdsSorted(3)).then_some(()))
            .len(),
        1
    );
}

#[tokio::test]
async fn unsorted_ids_not_known_live_count_against_the_tombstones() {
    let ids = LazyRowIds::from_unsorted_ids(Buffer::from(vec![5u64, 1, 3]), false);
    let rec = Recorder::start();
    let deleted = Mask::from_indices(8, vec![1, 6]);
    assert_eq!(ids.live_len_without_sort(&deleted).unwrap(), Some(2));
    assert_eq!(ids_sorted(&rec), 0);
}

/// Under tombstones an in-memory served run is counted off its ids in the
/// index's order, never sorted into base order, and counts what an unindexed
/// twin counts: the tombstones fall both outside the run (`?p0??`) and inside
/// it (one object's rows).
#[tokio::test]
async fn pending_counts_without_sort_under_tombstones() {
    let quads = modular_quads(2_000, 7, 11);
    let (p0, p2, doomed) = (iri("p0"), iri("p2"), plain_literal("object 3"));
    let mut stores = Vec::new();
    for indexes in [vec![IndexType::SecondaryByCopy], vec![]] {
        let store = built_store(quads.clone(), LayoutStrategy::Dictionary, indexes).await;
        let store = store
            .delete_matching(None, Some(&p0), None, None)
            .await
            .unwrap();
        let store = store
            .delete_matching(None, None, Some(&doomed), None)
            .await
            .unwrap();
        stores.push(store);
    }
    let want = matched_size(&stores[1], None, Some(&p2), None, None).await;
    assert!(
        want > 0 && want < 2_000 / 7,
        "tombstones fall inside the run"
    );
    let view = matched_view(&stores[0], None, Some(&p2), None, None).await;
    assert_eq!(
        view.debug_row_ids_materialized(),
        Some(false),
        "a served run"
    );
    let rec = Recorder::start();
    assert_eq!(view.size().await.unwrap(), want);
    assert_eq!(ids_sorted(&rec), 0, "the count sorted no ids");
    assert_eq!(view.debug_row_ids_materialized(), Some(false));
}

/// An in-memory run's ids decode once, in the component's order, however
/// many clones ask; sorting them into base order afterwards sorts that
/// decode rather than decoding the rids again.
#[tokio::test]
async fn a_run_decodes_once_for_both_orders() {
    use vortex_array::IntoArray;
    use vortex_array::arrays::PrimitiveArray;

    let rids = PrimitiveArray::from_iter([5u64, 1, 3]).into_array();
    let ids = LazyRowIds::from_component_run(rids, false);
    let rec = Recorder::start();
    let run_order = |ids: &LazyRowIds| ids.run_order_ids().unwrap().unwrap();
    assert_eq!(run_order(&ids).as_slice(), &[5, 1, 3]);
    assert_eq!(run_order(&ids.clone()).as_slice(), &[5, 1, 3]);
    assert_eq!(ids.materialized().unwrap().as_slice(), &[1, 3, 5]);
    assert_eq!(rids_decoded(&rec), 3, "one decode of the run's three rids");
    assert_eq!(ids_sorted(&rec), 1);
}

/// The by-copy store of `modular_quads(20_000, 7, 11)` under `layout`,
/// written once per process per layout (see [`cached_store_file`]) and
/// opened in memory. Keep the `TempDir` alive while the store is used.
async fn memory_store(layout: LayoutStrategy) -> (tempfile::TempDir, VortexRdfStore) {
    static FILES: [OnceLock<Vec<u8>>; 3] = [const { OnceLock::new() }; 3];
    let cell = &FILES[LAYOUTS.iter().position(|&l| l == layout).unwrap()];
    let quads = || modular_quads(20_000, 7, 11);
    let (dir, path) =
        cached_store_file(cell, quads, layout, vec![IndexType::SecondaryByCopy]).await;
    (
        dir,
        VortexRdfStore::from_file_in_memory(&path).await.unwrap(),
    )
}

/// A quad's terms as the store spells them, `(s, p, o, g)`: their N-Triples
/// forms, the default graph as the empty string.
fn spellings(quad: &Quad) -> [String; 4] {
    let graph = match &quad.graph_name {
        GraphName::DefaultGraph => String::new(),
        graph => graph.to_string(),
    };
    [
        quad.subject.to_string(),
        quad.predicate.to_string(),
        quad.object.to_string(),
        graph,
    ]
}

/// Every stream of a served in-memory view reads its rows in its index's
/// order: a `?p??` view in `index:posg`'s, which puts the object before the
/// subject, and a `??o?` view in `index:ospg`'s, which for a bound object is
/// base order. The one-shot read already did.
#[tokio::test]
async fn memory_streams_follow_index_order() {
    let (_dir, store) = memory_store(LayoutStrategy::Dictionary).await;
    let p = matched_view(&store, None, Some(&iri("p2")), None, None).await;
    assert_eq!(served_component(&p), Some("index:posg"));
    let base = base_ordered_codes(&p).await;
    let want = posg(base.clone());
    assert_ne!(want, base, "the index's order is not base order");
    assert_eq!(collect_code_chunks(&p, &QuadColumn::ALL, 64).await.0, want);
    let objects: Vec<[TermCode; 1]> = want.iter().map(|r| [r[2]]).collect();
    assert_eq!(
        collect_code_chunks(&p, &[QuadColumn::O], 64).await.0,
        objects
    );
    assert_eq!(struct_chunk_codes(&collect_row_chunks(&p, 256).await), want);
    assert_eq!(row_list(&p).await, want);

    let o = matched_view(&store, None, None, Some(&plain_literal("object 3")), None).await;
    assert_eq!(served_component(&o), Some("index:ospg"));
    let base = base_ordered_codes(&o).await;
    assert_eq!(collect_code_chunks(&o, &QuadColumn::ALL, 64).await.0, base);
    assert_eq!(struct_chunk_codes(&collect_row_chunks(&o, 256).await), base);
}

/// The same on a string layout: a served `?p??` view's row chunks and its
/// `quads_vec` follow (p, o, s, g) over the terms' N-Triples spellings.
#[tokio::test]
async fn memory_string_streams_follow_index_order() {
    let (_dir, store) = memory_store(LayoutStrategy::Default).await;
    let p = matched_view(&store, None, Some(&iri("p2")), None, None).await;
    assert_eq!(served_component(&p), Some("index:posg"));
    let unserved = p.debug_unserved().await.unwrap().quads_vec().await.unwrap();
    let mut want: Vec<[String; 4]> = unserved.iter().map(spellings).collect();
    want.sort_by(|a, b| (&a[1], &a[2], &a[0], &a[3]).cmp(&(&b[1], &b[2], &b[0], &b[3])));
    let chunks = collect_row_chunks(&p, 256).await;
    let rows: Vec<(String, String)> = chunk_subjects(&chunks)
        .into_iter()
        .zip(chunk_strings(&chunks, "o"))
        .collect();
    let want_rows: Vec<(String, String)> =
        want.iter().map(|q| (q[0].clone(), q[2].clone())).collect();
    assert_eq!(rows, want_rows);
    let quads: Vec<[String; 4]> = p.quads_vec().await.unwrap().iter().map(spellings).collect();
    assert_eq!(quads, want);
}

/// The N-Quads export of a served in-memory view writes its rows in the
/// order `quads_vec` reads them, the index's, not base order.
#[tokio::test]
async fn memory_export_follows_index_order() {
    let (_dir, store) = memory_store(LayoutStrategy::Dictionary).await;
    let p = matched_view(&store, None, Some(&iri("p2")), None, None).await;
    let mut out = Vec::new();
    crate::export_rdf(p.clone(), &mut out, oxrdfio::RdfFormat::NQuads)
        .await
        .unwrap();
    let exported: Vec<Quad> = oxrdfio::RdfParser::from_format(oxrdfio::RdfFormat::NQuads)
        .for_reader(out.as_slice())
        .collect::<std::result::Result<_, _>>()
        .unwrap();
    let quads = p.quads_vec().await.unwrap();
    assert_eq!(exported, quads);
    let unserved = p.debug_unserved().await.unwrap().quads_vec().await.unwrap();
    assert_ne!(quads, unserved, "the index's order is not base order");
}

/// A served view's run ids decode once however many clones stream it (every
/// chunk stream reads a clone of its view), and no stream sorts them.
#[tokio::test]
async fn run_ids_decode_once_across_clones() {
    let (_dir, store) = memory_store(LayoutStrategy::Dictionary).await;
    let p = matched_view(&store, None, Some(&iri("p2")), None, None).await;
    let rec = Recorder::start();
    for _ in 0..2 {
        collect_code_chunks(&p.clone(), &QuadColumn::ALL, 64).await;
        collect_row_chunks(&p.clone(), 64).await;
    }
    assert_eq!(rec.count_where(|e| matches!(e, Event::RidsDecoded(_))), 1);
    assert_eq!(ids_sorted(&rec), 0);
}

/// The `?p2??` view of `store` and its rows in the index's order.
async fn p2_view(store: &VortexRdfStore) -> (VortexRdfStore, Vec<[TermCode; 4]>) {
    let p = matched_view(store, None, Some(&iri("p2")), None, None).await;
    let want = posg(base_ordered_codes(&p).await);
    (p, want)
}

/// The rows of a window, none when it is empty.
async fn window_rows(view: &VortexRdfStore) -> Vec<[TermCode; 4]> {
    if view.size().await.unwrap() == 0 {
        return vec![];
    }
    row_list(view).await
}

/// `rows[offset..offset + limit]`, clamped to `rows`.
fn slice_of<T: Clone>(rows: &[T], offset: usize, limit: usize) -> Vec<T> {
    let n = rows.len();
    rows[offset.min(n)..offset.saturating_add(limit).min(n)].to_vec()
}

/// A window of a served in-memory view is the matching slice of the view's
/// rows in the index's order and stays served; neither the window nor its
/// count and read sorts ids.
#[tokio::test]
async fn memory_windows_are_slices_of_index_order() {
    let (_dir, store) = memory_store(LayoutStrategy::Dictionary).await;
    let (p, want) = p2_view(&store).await;
    let n = want.len();
    for (offset, limit) in [
        (0, 10),
        (10, 100),
        (n / 2, n / 4),
        (n - 1, 5),
        (n, 5),
        (0, 0),
    ] {
        let rec = Recorder::start();
        let w = p.window(offset, limit).await.unwrap();
        let got = window_rows(&w).await;
        assert_eq!(got, slice_of(&want, offset, limit), "({offset}, {limit})");
        assert_eq!(ids_sorted(&rec), 0, "({offset}, {limit}) sorts no ids");
        if !got.is_empty() {
            assert_eq!(
                served_component(&w),
                Some("index:posg"),
                "({offset}, {limit})"
            );
        }
    }
}

/// Under tombstones inside and outside the run, a window of a served view is
/// the slice of its live rows in the index's order.
#[tokio::test]
async fn memory_windows_under_tombstones() {
    let (_dir, store) = memory_store(LayoutStrategy::Dictionary).await;
    let store = store
        .delete_matching(None, Some(&iri("p0")), None, None)
        .await
        .unwrap()
        .delete_matching(None, None, Some(&plain_literal("object 3")), None)
        .await
        .unwrap();
    let (p, want) = p2_view(&store).await;
    let n = want.len();
    assert!(n > 0 && n < 20_000 / 7, "tombstones fall inside the run");
    for (offset, limit) in [(0, 10), (10, 100), (n / 2, n / 4), (n - 1, 5), (n, 5)] {
        let w = p.window(offset, limit).await.unwrap();
        assert_eq!(
            window_rows(&w).await,
            slice_of(&want, offset, limit),
            "({offset}, {limit})"
        );
        assert_eq!(
            w.size().await.unwrap(),
            slice_of(&want, offset, limit).len()
        );
    }
}

/// Windows nest: a window of a window is the slice of the slice.
#[tokio::test]
async fn nested_windows() {
    let (_dir, store) = memory_store(LayoutStrategy::Dictionary).await;
    let (p, want) = p2_view(&store).await;
    let outer = p.window(10, 100).await.unwrap();
    let inner = outer.window(5, 50).await.unwrap();
    assert_eq!(row_list(&inner).await, want[15..65]);
    assert_eq!(served_component(&inner), Some("index:posg"));
    let past = outer.window(95, 50).await.unwrap();
    assert_eq!(row_list(&past).await, want[105..110]);
}

/// The partitions of a served in-memory view are served windows that read,
/// in turn, the view in the index's order.
#[tokio::test]
async fn memory_partitions_concatenate_to_index_order() {
    let (_dir, store) = memory_store(LayoutStrategy::Dictionary).await;
    let (p, want) = p2_view(&store).await;
    let parts = p
        .partitions(std::num::NonZeroUsize::new(4).unwrap())
        .await
        .unwrap();
    assert_eq!(parts.len(), 4);
    let mut got = vec![];
    for part in &parts {
        assert_eq!(served_component(part), Some("index:posg"));
        got.extend(row_list(part).await);
    }
    assert_eq!(got, want);
}

/// Windows of a served view with an append tail run past the base's rows,
/// in the index's order, into the tail's: each window is the slice of the
/// view's quads, and the counts add up.
#[tokio::test]
async fn window_crosses_into_the_tail() {
    let (_dir, store) = memory_store(LayoutStrategy::Default).await;
    let extra: Vec<Quad> = (0..3)
        .map(|k| {
            make_quad(
                &format!("http://example.org/t{k}"),
                "http://example.org/p2",
                "tail",
                GraphName::DefaultGraph,
            )
        })
        .collect();
    let store = store.add_quads(extra).await.unwrap();
    let p = matched_view(&store, None, Some(&iri("p2")), None, None).await;
    assert_eq!(served_component(&p), Some("index:posg"));
    let quads = p.quads_vec().await.unwrap();
    let n = quads.len() - 3;
    for offset in n - 2..=n + 2 {
        let w = p.window(offset, 4).await.unwrap();
        let want = slice_of(&quads, offset, 4);
        assert_eq!(
            quad_strings(&w.quads_vec().await.unwrap()),
            quad_strings(&want),
            "offset {offset}"
        );
        assert_eq!(
            w.quads_vec().await.unwrap(),
            want,
            "offset {offset}: the order"
        );
        assert_eq!(w.size().await.unwrap(), want.len(), "offset {offset}");
    }
}

/// Row ids past `u32::MAX` through served windows and partitions: the same
/// rows as the index-order slices of an unindexed twin's.
#[tokio::test]
async fn wide_row_ids_through_served_windows() {
    let _base = crate::store::test_hooks::RowIdBase::set((1 << 32) + 5);
    let quads = modular_quads(2_000, 7, 11);
    let store = built_store(
        quads.clone(),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    let twin = built_store(quads, LayoutStrategy::Dictionary, vec![]).await;
    let p = matched_view(&store, None, Some(&iri("p2")), None, None).await;
    assert_eq!(served_component(&p), Some("index:posg"));
    let want = posg(row_list(&matched_view(&twin, None, Some(&iri("p2")), None, None).await).await);
    for (offset, limit) in [(0, 10), (100, 50), (200, 1_000)] {
        let w = p.window(offset, limit).await.unwrap();
        assert_eq!(
            row_list(&w).await,
            slice_of(&want, offset, limit),
            "({offset}, {limit})"
        );
    }
    let parts = p
        .partitions(std::num::NonZeroUsize::new(3).unwrap())
        .await
        .unwrap();
    let mut got = vec![];
    for part in &parts {
        got.extend(row_list(part).await);
    }
    assert_eq!(got, want);
}

/// What does not depend on order reads a narrowed served view as before:
/// its bytes, reopened, hold its rows.
#[tokio::test]
async fn order_free_consumers_unchanged() {
    let (_dir, store) = memory_store(LayoutStrategy::Dictionary).await;
    let (p, _) = p2_view(&store).await;
    let w = p.window(10, 100).await.unwrap();
    let reopened = VortexRdfStore::from_bytes(&w.to_bytes().await.unwrap())
        .await
        .unwrap();
    let mut want = quad_strings(&w.quads_vec().await.unwrap());
    want.sort();
    let mut got = quad_strings(&reopened.quads_vec().await.unwrap());
    got.sort();
    assert_eq!(got, want);
}

/// The probe API, as the bindings call it: a windowed probe of a served
/// pattern is the index-order slice, and its count is the view's size.
#[tokio::test]
async fn probe_windows_follow_index_order() {
    let (_dir, store) = memory_store(LayoutStrategy::Dictionary).await;
    let (p, want) = p2_view(&store).await;
    let probe = Probe::parse(None, Some("<http://example.org/p2>"), None, None).unwrap();
    let w = store
        .run_probe(&probe.clone().window(10, Some(100)))
        .await
        .unwrap();
    assert_eq!(row_list(&w).await, want[10..110]);
    assert_eq!(
        store.count_probe(&probe).await.unwrap(),
        p.size().await.unwrap()
    );
}

/// The objects of `rows`, each once, ascending.
fn objects_of(rows: &[[TermCode; 4]]) -> Vec<TermCode> {
    let objects: std::collections::BTreeSet<TermCode> = rows.iter().map(|r| r[2]).collect();
    objects.into_iter().collect()
}

/// A keep of a served in-memory view filters its rows in the index's order
/// and stays served: its count sorts no ids, a window of it slices the
/// filtered rows, and a keep of a window filters the window.
#[tokio::test]
async fn memory_keeps_filter_index_order() {
    let (_dir, store) = memory_store(LayoutStrategy::Dictionary).await;
    let (p, want) = p2_view(&store).await;
    let sparse = Keep::set(objects_of(&want).into_iter().step_by(3));
    let kept = p.keep(QuadColumn::O, &sparse).await.unwrap();
    let filtered: Vec<_> = want
        .iter()
        .copied()
        .filter(|r| sparse.admits(r[2]))
        .collect();
    assert!(!filtered.is_empty() && filtered.len() < want.len());
    assert_eq!(row_list(&kept).await, filtered);
    assert_eq!(served_component(&kept), Some("index:posg"));
    let rec = Recorder::start();
    assert_eq!(kept.size().await.unwrap(), filtered.len());
    assert_eq!(ids_sorted(&rec), 0, "a kept view counts without a sort");
    let window = kept.window(2, 7).await.unwrap();
    assert_eq!(row_list(&window).await, filtered[2..9]);
    assert_eq!(served_component(&window), Some("index:posg"));
    let window_then_keep = p
        .window(5, 300)
        .await
        .unwrap()
        .keep(QuadColumn::O, &sparse)
        .await
        .unwrap();
    let want_window: Vec<_> = want[5..305]
        .iter()
        .copied()
        .filter(|r| sparse.admits(r[2]))
        .collect();
    assert_eq!(row_list(&window_then_keep).await, want_window);
}

/// Keeps compose: `keep_many`, partitions of a kept view, its code chunks
/// and decoded quads all follow the filtered index order, and a keep that
/// admits none of the view's rows is empty.
#[tokio::test]
async fn memory_keeps_compose() {
    let (_dir, store) = memory_store(LayoutStrategy::Dictionary).await;
    let (p, want) = p2_view(&store).await;
    let objects = objects_of(&want);
    let sparse = Keep::set(objects.iter().step_by(2).copied());
    let subjects: Vec<TermCode> = want
        .iter()
        .map(|r| r[0])
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();
    let lower = Keep::range(subjects[0]..subjects[subjects.len() / 2]);
    let both = p
        .keep_many(&[
            (QuadColumn::O, sparse.clone()),
            (QuadColumn::S, lower.clone()),
        ])
        .await
        .unwrap();
    let filtered: Vec<_> = want
        .iter()
        .copied()
        .filter(|r| sparse.admits(r[2]) && lower.admits(r[0]))
        .collect();
    assert!(!filtered.is_empty());
    assert_eq!(row_list(&both).await, filtered);
    assert_eq!(served_component(&both), Some("index:posg"));
    assert_eq!(
        collect_code_chunks(&both, &QuadColumn::ALL, 64).await.0,
        filtered
    );
    let parts = both
        .partitions(std::num::NonZeroUsize::new(3).unwrap())
        .await
        .unwrap();
    let mut got = vec![];
    for part in &parts {
        assert_eq!(served_component(part), Some("index:posg"));
        got.extend(row_list(part).await);
    }
    assert_eq!(got, filtered);
    let dict = both.dict_reader().unwrap();
    let codes: [vortex_buffer::Buffer<TermCode>; 4] =
        std::array::from_fn(|c| filtered.iter().map(|r| r[c]).collect());
    let rows: Vec<Row> = decoded_rows(&codes, &dict).await;
    let quads: Vec<Row> = both
        .quads_vec()
        .await
        .unwrap()
        .iter()
        .map(|q| {
            let [s, p, o, g] = spellings(q);
            (s, p, o, g)
        })
        .collect();
    assert_eq!(quads, rows, "the decoded quads, in order");
    // A window of the kept rows small enough to read point by point.
    let small: Vec<Row> = both
        .window(3, 10)
        .await
        .unwrap()
        .quads_vec()
        .await
        .unwrap()
        .iter()
        .map(|q| {
            let [s, p, o, g] = spellings(q);
            (s, p, o, g)
        })
        .collect();
    assert_eq!(small, rows[3..13], "a small kept window's quads, in order");

    let outside = Keep::set([objects[0] + 1_000_000]);
    let none = p.keep(QuadColumn::O, &outside).await.unwrap();
    assert_eq!(none.size().await.unwrap(), 0);
}

/// Under tombstones inside the run, a keep of a served view keeps only its
/// live rows, in the index's order.
#[tokio::test]
async fn memory_keeps_under_tombstones() {
    let (_dir, store) = memory_store(LayoutStrategy::Dictionary).await;
    let store = store
        .delete_matching(None, None, Some(&plain_literal("object 3")), None)
        .await
        .unwrap();
    let (p, want) = p2_view(&store).await;
    let sparse = Keep::set(objects_of(&want).into_iter().step_by(2));
    let kept = p.keep(QuadColumn::O, &sparse).await.unwrap();
    let filtered: Vec<_> = want
        .iter()
        .copied()
        .filter(|r| sparse.admits(r[2]))
        .collect();
    assert_eq!(row_list(&kept).await, filtered);
    assert_eq!(kept.size().await.unwrap(), filtered.len());
    assert_eq!(
        row_list(&kept.window(1, 5).await.unwrap()).await,
        filtered[1..6]
    );
}

/// Over a base whose code columns the shared cache cannot hand out (as a
/// chunked or wire-encoded one), a served view's code chunks read its rows
/// and its keeps test the serving component's column at the rows'
/// positions: both still in the index's order.
#[tokio::test]
async fn served_reads_without_the_code_cache() {
    let (_dir, store) = memory_store(LayoutStrategy::Dictionary).await;
    let (p, want) = p2_view(&store).await;
    let sparse = Keep::set(objects_of(&want).into_iter().step_by(3));
    let filtered: Vec<_> = want
        .iter()
        .copied()
        .filter(|r| sparse.admits(r[2]))
        .collect();
    let _uncached = crate::store::test_hooks::NoCodeCache::set();
    assert_eq!(collect_code_chunks(&p, &QuadColumn::ALL, 64).await.0, want);
    let kept = p.keep(QuadColumn::O, &sparse).await.unwrap();
    assert_eq!(served_component(&kept), Some("index:posg"));
    assert_eq!(
        collect_code_chunks(&kept, &QuadColumn::ALL, 64).await.0,
        filtered
    );
    assert_eq!(row_list(&kept).await, filtered);
}

/// The by-copy Dictionary store of `modular_quads(20_000, 7, 11)`, written
/// once per process (see [`cached_store_file`]) and opened from its file.
async fn file_store() -> (tempfile::TempDir, VortexRdfStore) {
    static FILE: OnceLock<Vec<u8>> = OnceLock::new();
    let quads = || modular_quads(20_000, 7, 11);
    let indexes = vec![IndexType::SecondaryByCopy];
    let (dir, path) = cached_store_file(&FILE, quads, LayoutStrategy::Dictionary, indexes).await;
    (dir, VortexRdfStore::from_file(&path).await.unwrap())
}

/// How many served streams `rec` saw read off their plans.
fn served_streams(rec: &Recorder) -> usize {
    rec.count(&Event::ServedRunChunks)
}

/// Every stream of a served file view reads its rows off the index, in its
/// order: `?p2??` over the run the index locates, `?p2?g` (the default
/// graph, every row's) over the rows the plan's filter keeps, both in
/// `index:posg`'s (o, s) order; `??o?` and `?po?`, whose index order is base
/// order, in base order.
#[tokio::test]
async fn file_streams_follow_index_order() {
    let (_dir, store) = file_store().await;
    let (p2, o3) = (iri("p2"), plain_literal("object 3"));
    let default = GraphName::DefaultGraph;
    let views = [
        (
            "?p2??",
            matched_view(&store, None, Some(&p2), None, None).await,
            true,
        ),
        (
            "?p2?g",
            matched_view(&store, None, Some(&p2), None, Some(&default)).await,
            true,
        ),
        (
            "??o?",
            matched_view(&store, None, None, Some(&o3), None).await,
            false,
        ),
        (
            "?po?",
            matched_view(&store, None, Some(&p2), Some(&o3), None).await,
            false,
        ),
    ];
    for (name, view, object_free) in views {
        assert!(served_component(&view).is_some(), "{name}: served");
        let base = base_ordered_codes(&view).await;
        let want = if object_free {
            posg(base.clone())
        } else {
            base.clone()
        };
        if object_free {
            assert_ne!(want, base, "{name}: the orders differ");
        }
        for batch in [64, 65_536] {
            let rec = Recorder::start();
            assert_eq!(
                collect_code_chunks(&view, &QuadColumn::ALL, batch).await.0,
                want,
                "{name} at {batch}"
            );
            assert_eq!(served_streams(&rec), 1, "{name} at {batch}: off the plan");
        }
        let objects: Vec<[TermCode; 1]> = want.iter().map(|r| [r[2]]).collect();
        assert_eq!(
            collect_code_chunks(&view, &[QuadColumn::O], 64).await.0,
            objects,
            "{name}: o"
        );
        let rec = Recorder::start();
        assert_eq!(
            struct_chunk_codes(&collect_row_chunks(&view, 256).await),
            want,
            "{name}: rows"
        );
        assert_eq!(served_streams(&rec), 1, "{name}: rows off the plan");
    }
}

/// On file too, a served view's streams drop its tombstoned rows and keep
/// the index's order, and a string layout's row chunks decode, in order, to
/// the quads `quads_vec` reads.
#[tokio::test]
async fn file_streams_under_tombstones_and_strings() {
    let (_dir, store) = file_store().await;
    let store = store
        .delete_matching(None, None, Some(&plain_literal("object 3")), None)
        .await
        .unwrap();
    let (p, want) = p2_view(&store).await;
    assert!(want.len() < 20_000 / 7, "tombstones fall inside the run");
    assert_eq!(collect_code_chunks(&p, &QuadColumn::ALL, 100).await.0, want);
    assert_eq!(struct_chunk_codes(&collect_row_chunks(&p, 100).await), want);

    let (_dir, path) = write_store_file(
        modular_quads(2_000, 7, 11),
        LayoutStrategy::Default,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let p = matched_view(&store, None, Some(&iri("p2")), None, None).await;
    assert_eq!(served_component(&p), Some("index:posg"));
    let mut decoded = Vec::new();
    let mut chunks = p.row_chunks(50).unwrap();
    while let Some(chunk) = chunks.next().await {
        decoded.extend(p.decode_row_chunk(&chunk.unwrap()).unwrap());
    }
    assert_eq!(decoded, p.shared_quads_vec().await.unwrap());
}

/// Task 4's window matrix on a served file view, with and without
/// tombstones inside its run: each window is the slice of the view's rows in
/// the index's order and stays served, and windows nest and stream.
#[tokio::test]
async fn file_windows_are_slices_of_index_order() {
    let (_dir, store) = file_store().await;
    let tombstoned = store
        .delete_matching(None, Some(&iri("p0")), None, None)
        .await
        .unwrap()
        .delete_matching(None, None, Some(&plain_literal("object 3")), None)
        .await
        .unwrap();
    for (tag, store) in [("plain", &store), ("tombstoned", &tombstoned)] {
        let (p, want) = p2_view(store).await;
        let n = want.len();
        for (offset, limit) in [
            (0, 10),
            (10, 100),
            (n / 2, n / 4),
            (n - 1, 5),
            (n, 5),
            (0, 0),
        ] {
            let w = p.window(offset, limit).await.unwrap();
            let got = window_rows(&w).await;
            assert_eq!(
                got,
                slice_of(&want, offset, limit),
                "{tag} ({offset}, {limit})"
            );
            assert_eq!(
                w.size().await.unwrap(),
                got.len(),
                "{tag} ({offset}, {limit}) size"
            );
            if !got.is_empty() {
                assert_eq!(
                    served_component(&w),
                    Some("index:posg"),
                    "{tag} ({offset}, {limit})"
                );
            }
        }
        let w = p.window(10, 100).await.unwrap();
        assert_eq!(
            row_list(&w.window(5, 50).await.unwrap()).await,
            want[15..65],
            "{tag} nested"
        );
        assert_eq!(
            collect_code_chunks(&w, &QuadColumn::ALL, 32).await.0,
            want[10..110],
            "{tag} chunks"
        );
        assert_eq!(
            struct_chunk_codes(&collect_row_chunks(&w, 32).await),
            want[10..110],
            "{tag} rows"
        );
        let quads = p.quads_vec().await.unwrap();
        assert_eq!(w.quads_vec().await.unwrap(), quads[10..110], "{tag} quads");
    }
}

/// The partitions of a served file view are served windows that read, in
/// turn, the view in the index's order.
#[tokio::test]
async fn file_partitions_concatenate_to_index_order() {
    let (_dir, store) = file_store().await;
    let (p, want) = p2_view(&store).await;
    let parts = p
        .partitions(std::num::NonZeroUsize::new(4).unwrap())
        .await
        .unwrap();
    assert_eq!(parts.len(), 4);
    let mut got = vec![];
    for part in &parts {
        assert_eq!(served_component(part), Some("index:posg"));
        got.extend(row_list(part).await);
    }
    assert_eq!(got, want);
}

/// Task 5's keeps on a served file view: the keep filters the view's rows in
/// the index's order and stays served, counts without a sort, and composes
/// with windows either way round; under tombstones it keeps live rows only.
#[tokio::test]
async fn file_keeps_filter_index_order() {
    let (_dir, store) = file_store().await;
    let (p, want) = p2_view(&store).await;
    let sparse = Keep::set(objects_of(&want).into_iter().step_by(3));
    let filtered: Vec<_> = want
        .iter()
        .copied()
        .filter(|r| sparse.admits(r[2]))
        .collect();
    let kept = p.keep(QuadColumn::O, &sparse).await.unwrap();
    assert_eq!(row_list(&kept).await, filtered);
    assert_eq!(served_component(&kept), Some("index:posg"));
    let rec = Recorder::start();
    assert_eq!(kept.size().await.unwrap(), filtered.len());
    assert_eq!(ids_sorted(&rec), 0, "a kept view counts without a sort");
    assert_eq!(
        collect_code_chunks(&kept, &QuadColumn::ALL, 64).await.0,
        filtered
    );
    assert_eq!(
        row_list(&kept.window(2, 7).await.unwrap()).await,
        filtered[2..9]
    );
    let window_then_keep = p
        .window(5, 300)
        .await
        .unwrap()
        .keep(QuadColumn::O, &sparse)
        .await
        .unwrap();
    let want_window: Vec<_> = want[5..305]
        .iter()
        .copied()
        .filter(|r| sparse.admits(r[2]))
        .collect();
    assert_eq!(row_list(&window_then_keep).await, want_window);
    let quads = p.quads_vec().await.unwrap();
    let kept_quads = kept.quads_vec().await.unwrap();
    let positions: Vec<usize> = want
        .iter()
        .enumerate()
        .filter(|(_, r)| sparse.admits(r[2]))
        .map(|(i, _)| i)
        .collect();
    let want_quads: Vec<Quad> = positions.iter().map(|&i| quads[i].clone()).collect();
    assert_eq!(kept_quads, want_quads, "the kept quads, in order");

    let tombstoned = store
        .delete_matching(None, None, Some(&plain_literal("object 3")), None)
        .await
        .unwrap();
    let (p, want) = p2_view(&tombstoned).await;
    let filtered: Vec<_> = want
        .iter()
        .copied()
        .filter(|r| sparse.admits(r[2]))
        .collect();
    let kept = p.keep(QuadColumn::O, &sparse).await.unwrap();
    assert_eq!(row_list(&kept).await, filtered, "tombstoned");
    assert_eq!(
        kept.size().await.unwrap(),
        filtered.len(),
        "tombstoned size"
    );
}

/// A located file run spanning several child leaves, windowed across their
/// boundaries: each window is the index-order slice.
#[tokio::test]
async fn windows_across_child_chunks() {
    let quads = modular_quads(2_000, 7, 11);
    let (_dir, path) =
        write_chunked_store(&quads, 32, Some(IndexType::SecondaryByCopy), false, None).await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let (p, want) = p2_view(&store).await;
    assert_eq!(served_component(&p), Some("index:posg"));
    let n = want.len();
    for (offset, limit) in [(31, 2), (30, 70), (0, n)] {
        let w = p.window(offset, limit).await.unwrap();
        assert_eq!(
            row_list(&w).await,
            slice_of(&want, offset, limit),
            "({offset}, {limit})"
        );
        assert_eq!(
            collect_code_chunks(&w, &QuadColumn::ALL, 16).await.0,
            slice_of(&want, offset, limit),
            "({offset}, {limit}) chunks"
        );
    }
}

/// The probe API on a served file pattern: a windowed probe is the
/// index-order slice, and its count is the view's size.
#[tokio::test]
async fn file_probe_windows_follow_index_order() {
    let (_dir, store) = file_store().await;
    let (p, want) = p2_view(&store).await;
    let probe = Probe::parse(None, Some("<http://example.org/p2>"), None, None).unwrap();
    let w = store
        .run_probe(&probe.clone().window(10, Some(100)))
        .await
        .unwrap();
    assert_eq!(row_list(&w).await, want[10..110]);
    assert_eq!(
        store.count_probe(&probe).await.unwrap(),
        p.size().await.unwrap()
    );
}

/// The quads of `quads` in `index:posg`'s order over their N-Triples
/// spellings, (p, o, s, g): a served `?p??` view's order on a string layout.
fn posg_quads(quads: &[Quad]) -> Vec<[String; 4]> {
    let mut rows: Vec<[String; 4]> = quads.iter().map(spellings).collect();
    rows.sort_by(|a, b| (&a[1], &a[2], &a[0], &a[3]).cmp(&(&b[1], &b[2], &b[0], &b[3])));
    rows
}

/// A served file run the index cannot locate (a string layout's, whose
/// keys are not binary-searched, or a run a residual graph filters) is read
/// once by row index, then narrowed: a run whose rows are contiguous in the
/// child (no residual) as a located range, a residual one's scattered rows
/// as kept rows. Two windows and a partitioning of each give the quads in
/// the index's order, and the run is located once across them.
#[tokio::test]
async fn unlocated_runs_read_once_then_narrow() {
    let quads = modular_quads(5_000, 7, 11);
    // Every third quad in a named graph: the default graph's rows of a
    // predicate are scattered over its run in (p, o, s, g) order.
    let graphed: Vec<Quad> = quads
        .iter()
        .enumerate()
        .map(|(i, quad)| {
            let mut quad = quad.clone();
            if i % 3 == 0 {
                quad.graph_name = iri_graph("g1");
            }
            quad
        })
        .collect();
    let p2 = iri("p2");
    let default = GraphName::DefaultGraph;
    // Each view beside the unindexed twin's match, sorted (p, o, s, g), and
    // whether its narrowings are located ranges.
    let mut views = Vec::new();
    let mut dirs = Vec::new();
    let twin = built_store(quads.clone(), LayoutStrategy::Default, vec![]).await;
    let want = posg_quads(
        &matched_view(&twin, None, Some(&p2), None, None)
            .await
            .quads_vec()
            .await
            .unwrap(),
    );
    for layout in [LayoutStrategy::Default, LayoutStrategy::TypedObject] {
        let (dir, path) =
            write_store_file(quads.clone(), layout, vec![IndexType::SecondaryByCopy]).await;
        let store = VortexRdfStore::from_file(&path).await.unwrap();
        views.push((
            format!("{layout:?} ?p2??"),
            matched_view(&store, None, Some(&p2), None, None).await,
            want.clone(),
            true,
        ));
        dirs.push(dir);
    }
    let (dir, path) = write_store_file(
        graphed.clone(),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let twin = built_store(graphed, LayoutStrategy::Default, vec![]).await;
    views.push((
        "Dictionary ?p2?g".into(),
        matched_view(&store, None, Some(&p2), None, Some(&default)).await,
        posg_quads(
            &matched_view(&twin, None, Some(&p2), None, Some(&default))
                .await
                .quads_vec()
                .await
                .unwrap(),
        ),
        false,
    ));
    dirs.push(dir);

    for (tag, view, want, located) in views {
        let n = want.len();
        assert!(n > 100, "{tag}: a run to narrow");
        assert!(served_component(&view).is_some(), "{tag}: served");
        assert_eq!(view.debug_serve_row_range(), None, "{tag}: unlocated");
        let rec = Recorder::start();
        for (offset, limit) in [(10, 100), (n / 2, n / 4)] {
            let w = view.window(offset, limit).await.unwrap();
            let got: Vec<[String; 4]> =
                w.quads_vec().await.unwrap().iter().map(spellings).collect();
            assert_eq!(
                got,
                slice_of(&want, offset, limit),
                "{tag} ({offset}, {limit})"
            );
            assert!(
                served_component(&w).is_some(),
                "{tag} ({offset}, {limit}): served"
            );
            assert_eq!(
                w.debug_serve_row_range().is_some(),
                located,
                "{tag} ({offset}, {limit}): a located range, else kept rows"
            );
        }
        let parts = view
            .partitions(std::num::NonZeroUsize::new(3).unwrap())
            .await
            .unwrap();
        let mut got = Vec::new();
        for part in &parts {
            assert!(
                served_component(part).is_some(),
                "{tag}: a served partition"
            );
            got.extend(part.quads_vec().await.unwrap().iter().map(spellings));
        }
        assert_eq!(got, want, "{tag}: partitions");
        assert_eq!(
            rec.count(&Event::ServedRunLocated),
            1,
            "{tag}: located once"
        );
    }
}

/// TypedObject's row chunks, in both residencies, read a served view in the
/// index's order: decoded, they are `quads_vec`'s rows in its order.
#[tokio::test]
async fn typed_object_row_chunks_follow_index_order() {
    let (_dir, path) = write_store_file(
        modular_quads(5_000, 7, 11),
        LayoutStrategy::TypedObject,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    for in_memory in [false, true] {
        let store = if in_memory {
            VortexRdfStore::from_file_in_memory(&path).await
        } else {
            VortexRdfStore::from_file(&path).await
        }
        .unwrap();
        let p = matched_view(&store, None, Some(&iri("p2")), None, None).await;
        assert_eq!(served_component(&p), Some("index:posg"));
        let mut decoded = Vec::new();
        let mut chunks = p.row_chunks(256).unwrap();
        while let Some(chunk) = chunks.next().await {
            decoded.extend(p.decode_row_chunk(&chunk.unwrap()).unwrap());
        }
        assert_eq!(
            decoded,
            p.shared_quads_vec().await.unwrap(),
            "in_memory={in_memory}"
        );
    }
}

/// A pattern chained on a narrowed served view (a window of `?p2??`) keeps
/// the window's rows its bound terms match, compared on the copy's own
/// columns (codes or strings), in the window's order, and stays served: in
/// both residencies, on the Dictionary and Default layouts, as written and
/// under tombstones. The object chain keeps one object's rows; the graph
/// chain keeps rows of every object, so its order is the index's, not
/// base order.
#[tokio::test]
async fn chained_match_on_a_window_filters_in_index_order() {
    // Every third quad in a named graph.
    let quads: Vec<Quad> = modular_quads(5_000, 7, 11)
        .into_iter()
        .enumerate()
        .map(|(i, mut quad)| {
            if i % 3 == 0 {
                quad.graph_name = iri_graph("g1");
            }
            quad
        })
        .collect();
    let (p2, o3) = (iri("p2"), plain_literal("object 3"));
    let default = GraphName::DefaultGraph;
    // Every fifth of the predicate's quads.
    let doomed: Vec<Quad> = quads
        .iter()
        .filter(|quad| quad.predicate == p2)
        .step_by(5)
        .cloned()
        .collect();
    for layout in [LayoutStrategy::Dictionary, LayoutStrategy::Default] {
        let (_dir, path) =
            write_store_file(quads.clone(), layout, vec![IndexType::SecondaryByCopy]).await;
        let file = VortexRdfStore::from_file(&path).await.unwrap();
        let memory = built_store(quads.clone(), layout, vec![IndexType::SecondaryByCopy]).await;
        let mut stores = Vec::new();
        for (residency, store) in [("file", file), ("memory", memory)] {
            stores.push((format!("{layout:?} {residency} as written"), store.clone()));
            let tombstoned = delete_all(&store, &doomed).await;
            stores.push((format!("{layout:?} {residency} tombstoned"), tombstoned));
        }
        for (tag, store) in stores {
            let window = matched_view(&store, None, Some(&p2), None, None)
                .await
                .window(10, 500)
                .await
                .unwrap();
            assert!(
                served_component(&window).is_some(),
                "{tag}: a served window"
            );
            let rows = window.quads_vec().await.unwrap();
            for (chain, object, graph) in [
                ("o3", Some(&o3), None),
                ("default graph", None, Some(&default)),
            ] {
                let tag = format!("{tag}, {chain}");
                let want: Vec<Quad> = rows
                    .iter()
                    .filter(|quad| object.is_none_or(|o| &quad.object == o))
                    .filter(|quad| graph.is_none_or(|g| &quad.graph_name == g))
                    .cloned()
                    .collect();
                assert!(
                    !want.is_empty() && want.len() < rows.len(),
                    "{tag}: the chain filters"
                );
                let chained = window
                    .match_pattern(None, None, object, graph)
                    .await
                    .unwrap();
                assert!(served_component(&chained).is_some(), "{tag}: served");
                assert_eq!(
                    chained.quads_vec().await.unwrap(),
                    want,
                    "{tag}: the window's rows, filtered, in its order"
                );
                assert_eq!(
                    chained.size().await.unwrap(),
                    want.len(),
                    "{tag}: the count"
                );
            }
            // A term no row of the window holds leaves none.
            let absent = plain_literal("object 99");
            let none = window
                .match_pattern(None, None, Some(&absent), None)
                .await
                .unwrap();
            assert_eq!(none.size().await.unwrap(), 0, "{tag}: an absent object");
            assert!(none.quads_vec().await.unwrap().is_empty(), "{tag}: no rows");
        }
    }
}

/// A pattern chained on a served view no window or keep narrowed is matched
/// as one match of both patterns, as before: the rows of the merged pattern.
/// The run is wide enough for its view to name its pattern (1,428 rows).
#[tokio::test]
async fn chained_match_on_an_unnarrowed_view_is_unchanged() {
    let quads = modular_quads(10_000, 7, 11);
    let (p2, o3) = (iri("p2"), plain_literal("object 3"));
    let (_dir, path) = write_store_file(
        quads.clone(),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    let file = VortexRdfStore::from_file(&path).await.unwrap();
    let memory = built_store(
        quads,
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    for (tag, store) in [("file", file), ("memory", memory)] {
        let view = matched_view(&store, None, Some(&p2), None, None).await;
        assert!(served_component(&view).is_some(), "{tag}: served");
        let rec = Recorder::start();
        let chained = view
            .match_pattern(None, None, Some(&o3), None)
            .await
            .unwrap();
        assert_eq!(rec.count(&Event::ChainMerged), 1, "{tag}: merged");
        let merged = matched_view(&store, None, Some(&p2), Some(&o3), None)
            .await
            .quads_vec()
            .await
            .unwrap();
        assert!(!merged.is_empty(), "{tag}: rows to chain");
        assert_eq!(
            chained.quads_vec().await.unwrap(),
            merged,
            "{tag}: the rows"
        );
    }
}
