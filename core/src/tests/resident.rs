//! The resident forms and their caches: a built base's canonical columns,
//! an adopted base's live canonical form, and the dictionary's Arrow
//! values — what each decodes, what it shares, and when it is freed.

use super::*;
use crate::store::TermEncoding;
#[cfg(feature = "file-io")]
use crate::store::{Keep, QuadColumn};
use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use arrow_array::types::UInt32Type;

/// `n` quads over `subjects` subjects, each subject holding a run of
/// `n / subjects` consecutive rows — wide enough that a subject's range
/// outgrows a point read.
fn wide_subject_quads(n: usize, subjects: usize) -> Vec<Quad> {
    let per_subject = n / subjects;
    (0..n)
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{:03}", i / per_subject),
                &format!("http://example.org/p{}", i % 3),
                &format!("object {i}"),
                GraphName::DefaultGraph,
            )
        })
        .collect()
}

async fn built(quads: Vec<Quad>) -> VortexRdfStore {
    VortexRdfStore::from_quads(quad_stream(quads), LayoutStrategy::Dictionary, vec![])
        .await
        .unwrap()
}

#[cfg(feature = "file-io")]
async fn adopted(store: &VortexRdfStore) -> VortexRdfStore {
    VortexRdfStore::from_bytes_owned(store.to_bytes().await.unwrap())
        .await
        .unwrap()
}

fn assert_live(store: &VortexRdfStore, alive: bool) {
    for idx in 0..4 {
        assert_eq!(
            store.debug_live_canonical_alive(idx),
            Some(alive),
            "column {idx}"
        );
    }
}

/// A built base holds canonical columns: every code read serves them
/// zero-copy, and the live canonical cache is never touched.
#[tokio::test]
async fn built_base_serves_codes_without_the_live_cache() {
    let store = built(wide_subject_quads(1000, 2)).await;
    assert!(store.debug_base_int_children_canonical());
    let direct = store
        .code_columns(&QuadColumn::ALL)
        .unwrap()
        .expect("canonical columns serve codes");
    let gathered = store.code_columns_gathered().await.unwrap().unwrap();
    for idx in 0..4 {
        assert_eq!(
            direct[idx].as_ptr(),
            gathered[idx].as_ptr(),
            "column {idx} is the base's own buffer"
        );
    }
    assert_live(&store, false);
}

/// Over an adopted base, contiguous wide reads decode each column once into
/// the live canonical form, share it with every holder, and free it with
/// the last; a subject range is a slice of the same buffers.
#[cfg(feature = "file-io")]
#[tokio::test]
async fn adopted_contiguous_reads_share_the_live_canonical_and_free_it() {
    let store = adopted(&built(wide_subject_quads(1000, 2)).await).await;
    assert!(
        !store.debug_base_int_children_canonical(),
        "an adopted base keeps its wire encodings"
    );
    assert_live(&store, false);

    let a = store.code_columns_gathered().await.unwrap().unwrap();
    assert_live(&store, true);
    let b = store.code_columns_gathered().await.unwrap().unwrap();
    for idx in 0..4 {
        assert_eq!(
            a[idx].as_ptr(),
            b[idx].as_ptr(),
            "column {idx} is shared while held"
        );
    }

    let s1 = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s001").unwrap());
    let view = store
        .match_pattern(Some(&s1), None, None, None)
        .await
        .unwrap();
    let range = view
        .debug_selection_range()
        .expect("a bound subject is a range");
    assert_eq!(range, 500..1000);
    let c = view.code_columns_gathered().await.unwrap().unwrap();
    for idx in 0..4 {
        assert_eq!(
            c[idx].as_ptr(),
            a[idx].as_slice()[500..].as_ptr(),
            "column {idx} is a slice of the shared form"
        );
    }
    let expected: Vec<u32> = a[0].as_slice()[500..].to_vec();
    assert_eq!(c[0].as_slice(), &expected[..]);

    drop(a);
    drop(b);
    assert_live(&store, true);
    drop(c);
    assert_live(&store, false);

    // A later read decodes afresh, to the same values.
    let again = store.code_columns_gathered().await.unwrap().unwrap();
    assert_eq!(&again[0].as_slice()[500..], &expected[..]);
    assert_live(&store, true);
    drop(again);
    assert_live(&store, false);
}

/// A point-sized selection over an adopted base reads point by point through
/// the probes and never decodes a column.
#[cfg(feature = "file-io")]
#[tokio::test]
async fn adopted_point_reads_never_decode_a_column() {
    let store = adopted(&built(modular_quads(50, 5, 7)).await).await;
    let s7 = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s07").unwrap());
    let view = store
        .match_pattern(Some(&s7), None, None, None)
        .await
        .unwrap();
    let cols = view.code_columns_gathered().await.unwrap().unwrap();
    assert_eq!(cols[0].len(), 1);
    assert_live(&store, false);
    let dict = store.code_read_snapshot().unwrap();
    assert_eq!(
        dict.decode(cols[0][0]).as_deref(),
        Some("<http://example.org/s07>")
    );
}

/// An id-list selection gathers from the live columns only while someone
/// holds them; on its own it takes from the encoded base and leaves the
/// cache empty.
#[cfg(feature = "file-io")]
#[tokio::test]
async fn adopted_id_reads_use_the_live_columns_only_while_held() {
    let store = adopted(&built(wide_subject_quads(1200, 2)).await).await;
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let view = store
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    let alone = view.code_columns_gathered().await.unwrap().unwrap();
    assert_eq!(alone[0].len(), 400);
    assert_live(&store, false);

    let held = store.code_columns_gathered().await.unwrap().unwrap();
    assert_live(&store, true);
    let shared = view.code_columns_gathered().await.unwrap().unwrap();
    for idx in 0..4 {
        assert_eq!(
            shared[idx].as_slice(),
            alone[idx].as_slice(),
            "column {idx}"
        );
    }
    drop(held);
    drop(shared);
    assert_live(&store, false);
}

/// A built dictionary is one canonical column that hands out its own
/// buffers: every `to_arrow` and every `terms` batch shares the dictionary's
/// views, and nothing is decoded or cached.
#[tokio::test]
async fn built_dictionary_exports_its_own_buffers() {
    let store = built(modular_quads(60, 4, 9)).await;
    let own = store
        .debug_dict_views_ptr()
        .expect("a built dictionary is canonical");
    let dict = store.code_read_snapshot().unwrap();
    let values = dict.to_arrow().unwrap();
    assert_eq!(
        values.as_string_view().views().as_ptr() as usize,
        own,
        "the Arrow values are the dictionary's own views"
    );
    assert_eq!(
        store.debug_dict_arrow_values_alive(),
        Some(false),
        "nothing to cache"
    );
    let batches: Vec<RecordBatch> = store
        .to_record_batches(TermEncoding::Terms, None)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let column = batches[0].column(0).as_dictionary::<UInt32Type>();
    assert_eq!(
        column.values().as_string_view().views().as_ptr() as usize,
        own,
        "a terms batch carries the dictionary itself"
    );
    assert_eq!(store.debug_dict_arrow_values_alive(), Some(false));
}

/// An adopted dictionary held as written decodes its Arrow values into
/// memory shared by every holder and freed with the last: a `terms` export
/// keeps it alive exactly as long as its batches live.
#[cfg(feature = "file-io")]
#[tokio::test]
async fn adopted_dictionary_arrow_values_are_freed_with_the_last_holder() {
    let store = adopted(&built(modular_quads(60, 4, 9)).await).await;
    assert!(
        store.debug_dict_views_ptr().is_none(),
        "adopted as written, the dictionary is its FSST chunks"
    );
    assert_eq!(store.debug_dict_arrow_values_alive(), Some(false));
    let dict = store.code_read_snapshot().unwrap();
    let values = dict.to_arrow().unwrap();
    assert_eq!(store.debug_dict_arrow_values_alive(), Some(true));
    assert_eq!(
        values.as_string_view().views().as_ptr(),
        dict.to_arrow().unwrap().as_string_view().views().as_ptr(),
        "a held values array shares its buffers"
    );
    drop(values);
    assert_eq!(store.debug_dict_arrow_values_alive(), Some(false));

    let batches: Vec<RecordBatch> = store
        .to_record_batches(TermEncoding::Terms, None)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert!(!batches.is_empty());
    assert_eq!(store.debug_dict_arrow_values_alive(), Some(true));
    drop(batches);
    assert_eq!(store.debug_dict_arrow_values_alive(), Some(false));
}

/// `keep` over an adopted base reads its column through the live cache and
/// leaves no decoded column behind.
#[cfg(feature = "file-io")]
#[tokio::test]
async fn keep_on_an_adopted_base_leaves_no_decoded_column_behind() {
    let base = built(wide_subject_quads(600, 2)).await;
    let store = adopted(&base).await;
    let dict = store.code_read_snapshot().unwrap();
    let p1 = dict.encode("<http://example.org/p1>").unwrap();
    let kept = store.keep(QuadColumn::P, &Keep::set([p1])).await.unwrap();
    assert_eq!(kept.size().await.unwrap(), 200);
    assert_live(&store, false);
    let from_built = base.keep(QuadColumn::P, &Keep::set([p1])).await.unwrap();
    assert_eq!(
        from_built.quads_vec().await.unwrap(),
        kept.quads_vec().await.unwrap()
    );
}
