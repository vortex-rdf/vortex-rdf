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
