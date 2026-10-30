//! The vortex-rdf.store.v2 wire contract. The round-trips assert it from
//! different starting states (tailed, tombstoned, file-backed, dictionary,
//! locally-sorted multi-chunk): `to_bytes` / `from_bytes` must carry the
//! store's index components and its sortedness provenance faithfully —
//! rebuilt where a mutation invalidated them, never silently dropped, never
//! over-claimed. Every writer stamps its index children with the version the
//! open-time ceiling check reads, and a Dictionary-layout primary streams only
//! beside its dictionary; what an open refuses is in `open_errors.rs`.

use super::*;
use crate::io::container;
use crate::store::schema;

// ─── Wire round-trips ──────────────────────────────────────────────────

/// A mutated owner's serialization keeps BOTH provenance channels: tombstones
/// must not silently drop the index children (they are rebuilt over the
/// surviving rows) nor the subject sortedness (a gather preserves row
/// order).
#[tokio::test]
async fn test_tombstoned_store_serialization_keeps_indexes_and_sortedness() {
    let quads = modular_quads(12, 3, 4);
    let store = built_store(
        quads.clone(),
        LayoutStrategy::Default,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    let s0 = iri_subject("s00");
    let deleted = store
        .delete_matching(Some(&s0), None, None, None)
        .await
        .unwrap();
    assert_eq!(deleted.size().await.unwrap(), 11);

    let bytes = deleted.to_bytes().await.unwrap();
    // Sortedness provenance survives the tombstoned gather on the wire.
    let (quads_sorted, _) = container::store_metadata_of_bytes(&bytes);
    assert!(
        quads_sorted,
        "tombstoned sorted store must keep quads_sorted"
    );
    let reread = VortexRdfStore::from_bytes(&bytes).await.unwrap();
    assert_eq!(
        reread.indexes(),
        &[IndexType::SecondaryByCopy],
        "tombstoned serialization must rebuild, not drop, the indexes"
    );
    assert_eq!(reread.size().await.unwrap(), 11);
    let p0 = iri("p0");
    // p0 was on rows 0, 3, 6, 9; row 0 was deleted.
    assert_eq!(matched_size(&reread, None, Some(&p0), None, None).await, 3);
}

/// A store whose every row is gone — deleted, or never in the base at all
/// (tail only) — still serializes its index roster: the components are
/// rebuilt over the empty row set, so the round-tripped store keeps the
/// index set it was built with.
#[tokio::test]
async fn test_serialize_to_empty_keeps_indexes() {
    let indexes = ALL_INDEX_TYPES.to_vec();
    for layout in [LayoutStrategy::Dictionary, LayoutStrategy::Default] {
        let store = built_store(modular_quads(6, 3, 4), layout, indexes.clone()).await;
        assert_eq!(store.indexes(), indexes.as_slice());

        // Every row tombstoned: the rebuild runs over no rows at all.
        let emptied = store.delete_matching(None, None, None, None).await.unwrap();
        assert_eq!(emptied.size().await.unwrap(), 0);
        let reread = VortexRdfStore::from_bytes(&emptied.to_bytes().await.unwrap())
            .await
            .unwrap();
        assert_eq!(reread.size().await.unwrap(), 0, "{layout:?}");
        assert_eq!(
            reread.indexes(),
            indexes.as_slice(),
            "{layout:?}: an emptied store's serialization must keep its indexes"
        );

        // Every base row tombstoned and a tail appended: the rebuild runs
        // over the tail alone.
        let tailed = emptied
            .add_quad(make_quad(
                "http://example.org/s99",
                "http://example.org/p1",
                "fresh object",
                GraphName::DefaultGraph,
            ))
            .await
            .unwrap();
        let reread = VortexRdfStore::from_bytes(&tailed.to_bytes().await.unwrap())
            .await
            .unwrap();
        assert_eq!(reread.size().await.unwrap(), 1, "{layout:?}");
        assert_eq!(
            reread.indexes(),
            indexes.as_slice(),
            "{layout:?}: a tail-only store's serialization must keep its indexes"
        );
    }
}

/// A narrowed view serializes its rows alone: the round-tripped store holds
/// exactly the matched quads and declares no indexes, whatever the source
/// carried.
#[tokio::test]
async fn test_narrowed_view_serializes_rows_only() {
    let quads = modular_quads(12, 3, 4);
    let store = built_store(
        quads.clone(),
        LayoutStrategy::Default,
        ALL_INDEX_TYPES.to_vec(),
    )
    .await;
    let p1 = iri("p1");
    let view = matched_view(&store, None, Some(&p1), None, None).await;
    assert_eq!(view.size().await.unwrap(), 4);

    let reread = VortexRdfStore::from_bytes(&view.to_bytes().await.unwrap())
        .await
        .unwrap();
    assert!(
        reread.indexes().is_empty(),
        "a narrowed view's serialization carries no index components"
    );
    assert_eq!(reread.size().await.unwrap(), 4);
    assert_eq!(
        view_strings(&reread).await,
        expected_strings(&quads, |i| i % 3 == 1)
    );
}

/// An unstamped owner passes its rows through untouched, so the wire
/// records `quads_sorted == false` and a reader neither stamps the base nor
/// loses the rows to a subject-bound match.
#[tokio::test]
async fn test_unstamped_owner_to_bytes_records_unsorted() {
    let mut quads = modular_quads(12, 3, 4);
    quads.reverse();
    let store = unstamped_store(&quads);

    let bytes = store.to_bytes().await.unwrap();
    let (quads_sorted, _) = container::store_metadata_of_bytes(&bytes);
    assert!(
        !quads_sorted,
        "an unstamped owner's serialization must not claim sorted order"
    );

    let reread = VortexRdfStore::from_bytes(&bytes).await.unwrap();
    assert_eq!(reread.view_statistics().unwrap().sort_order, None);
    assert_eq!(reread.size().await.unwrap(), 12);
    let s07 = subject_node(7, 2);
    assert_eq!(
        view_strings(&matched_view(&reread, Some(&s07), None, None, None).await).await,
        expected_strings(&quads, |i| quads[i].subject == s07)
    );
}

/// An untouched file-backed store's `to_bytes` is its file, index children
/// and all, so the byte round-trip keeps the index set.
#[tokio::test]
async fn test_file_backed_to_bytes_keeps_indexes() {
    let (_dir, path) = write_store_file(
        modular_quads(12, 3, 4),
        LayoutStrategy::Default,
        vec![IndexType::SecondaryByCopy],
    )
    .await;

    let store = VortexRdfStore::from_file(&path).await.unwrap();
    assert_eq!(store.indexes(), &[IndexType::SecondaryByCopy]);
    let bytes = store.to_bytes().await.unwrap();
    let reread = VortexRdfStore::from_bytes(&bytes).await.unwrap();
    assert_eq!(
        reread.indexes(),
        &[IndexType::SecondaryByCopy],
        "file → bytes must carry the index children across"
    );
    // And they route with correct results after the round-trip.
    let p1 = iri("p1");
    assert_eq!(matched_size(&reread, None, Some(&p1), None, None).await, 4);
}

/// A sorted store's `quads_sorted` provenance survives the file → bytes
/// round-trip: an untouched store's bytes are the file's, and a filtered
/// view that keeps every row is written again, its rows re-stamped from the
/// root metadata, never from stats a multi-chunk scan lost.
#[tokio::test]
async fn test_file_backed_to_bytes_keeps_quads_sorted() {
    let quads: Vec<Quad> = (0..500)
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{:05}", i),
                "http://example.org/p",
                &format!("o{}", i),
                GraphName::DefaultGraph,
            )
        })
        .collect();
    let (_dir, path) = write_store_file(quads, LayoutStrategy::Default, vec![]).await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let every_row = matched_view(&store, None, Some(&iri("p")), None, None).await;
    assert_eq!(every_row.size().await.unwrap(), 500);
    for view in [&store, &every_row] {
        let bytes = view.to_bytes().await.unwrap();
        let (quads_sorted, _) = container::store_metadata_of_bytes(&bytes);
        assert!(
            quads_sorted,
            "file-backed to_bytes must carry the sorted provenance across"
        );
    }
}

/// Serialization is deterministic: two `to_bytes` of the same store are
/// byte-identical.
#[tokio::test]
async fn test_to_bytes_is_byte_stable() {
    let quads: Vec<Quad> = (0..500)
        .map(|i| {
            make_quad(
                &format!("http://example.org/subject/{i:04}"),
                &format!("http://example.org/predicate/{}", i % 8),
                &format!("object value {i:04}"),
                GraphName::DefaultGraph,
            )
        })
        .collect();
    let arr =
        build_array::<SortedStreamBuilder>(quad_stream(quads), LayoutStrategy::Dictionary, vec![])
            .await
            .unwrap();
    let store = VortexRdfStore::from_built(arr).unwrap();

    let bytes_a = store.to_bytes().await.unwrap();
    let bytes_b = store.to_bytes().await.unwrap();
    assert_eq!(bytes_a, bytes_b);
}

/// Index children whose chunks carry only local sorts are not globally
/// sorted. Reading them back must NOT restore binary-search routing: the
/// descriptors carry `sorted: false`, in-memory resolution declines, and the
/// mask scan answers correctly. Re-stamping such columns sorted would make
/// this match return 5 rows instead of 4.
#[tokio::test]
async fn test_locally_sorted_children_from_bytes_match_correctly() {
    // Predicates interleave across chunks so per-chunk sorted != global.
    let quads: Vec<Quad> = (0..12)
        .map(|i| {
            make_quad(
                &format!("http://example.org/s/{i:02}"),
                &format!("http://example.org/p/{}", i % 3),
                &format!("o{}", i),
                GraphName::DefaultGraph,
            )
        })
        .collect();
    let raws: Vec<crate::store::RawQuad> =
        quads.iter().map(crate::store::RawQuad::from_quad).collect();
    // Cut the rows into three chunks and sort each family's child WITHIN a
    // chunk only: the concatenated child is not globally sorted, so its
    // descriptor must say so — the shape a chunked foreign writer could
    // produce and a reader must not binary-search.
    use crate::store::indexes::copy::CopyFamily;
    use crate::store::indexes::copy::out_of_core::{CopyKey, copy_child_chunk};
    let mut quad_chunks = Vec::new();
    let mut child_chunks: Vec<Vec<vortex_array::ArrayRef>> = vec![Vec::new(), Vec::new()];
    for (n, rows) in raws.chunks(4).enumerate() {
        quad_chunks.push(
            crate::store::builders::build_struct_array(rows, LayoutStrategy::Default, false)
                .unwrap(),
        );
        for (family_ix, family) in [CopyFamily::Posg, CopyFamily::Ospg].into_iter().enumerate() {
            let mut keys: Vec<(CopyKey<String>, crate::store::RowId)> = rows
                .iter()
                .enumerate()
                .map(|(i, q)| {
                    let spog = [q.s.clone(), q.p.clone(), q.o.clone(), q.g.clone()];
                    (
                        CopyKey::new(family, spog),
                        (n * 4 + i) as crate::store::RowId,
                    )
                })
                .collect();
            keys.sort_unstable();
            child_chunks[family_ix].push(copy_child_chunk(family, &keys).unwrap());
        }
    }
    let components: Vec<container::NativeComponentWrite> = [CopyFamily::Posg, CopyFamily::Ospg]
        .into_iter()
        .zip(child_chunks)
        .map(|(family, chunks)| {
            component_write(
                container::StoreComponentDescriptor {
                    name: family.identity().name.into(),
                    role: container::StoreComponentRole::Index,
                    implementation: family.identity().slug.into(),
                    version: 1,
                    required: false,
                    // Per-chunk sorts only: the writer may not claim global order.
                    sorted: false,
                    bracketed_iris: false,
                    dtype: copy_child_chunk::<String>(family, &[])
                        .unwrap()
                        .dtype()
                        .clone(),
                },
                chunks,
                container::default_child_strategy(),
            )
        })
        .collect();
    let bytes = unstamped_store_bytes(quad_chunks, components).await;

    let store = VortexRdfStore::from_bytes(&bytes).await.unwrap();
    assert_eq!(store.indexes(), &[IndexType::SecondaryByCopy]);
    let p = iri("p/1");
    let matched: Vec<Quad> = matched_view(&store, None, Some(&p), None, None)
        .await
        .quads()
        .try_collect()
        .await
        .unwrap();
    // Ground truth: p/1 appears for i = 1, 4, 7, 10 -> 4 quads.
    assert_eq!(
        matched.len(),
        4,
        "index-routed match after from_bytes is wrong"
    );
}

/// The file writer records its build's sortedness provenance on the wire:
/// the out-of-core sort it runs emits rows in global order, so `quads_sorted`
/// must come back true even for a stream fed in reverse — the stamp a
/// materialized read restores the subject binary search from.
#[tokio::test]
async fn test_writer_records_sortedness() {
    // Reverse subject order, so only a genuinely sorting build may claim it.
    let mut quads = modular_quads(10, 3, 4);
    quads.reverse();
    let mut bytes: Vec<u8> = Vec::new();
    crate::io::quads_stream_to_vortex_writer(
        quad_stream(quads),
        &mut bytes,
        LayoutStrategy::Default,
        vec![],
    )
    .await
    .unwrap();
    let (quads_sorted, _) = container::store_metadata_of_bytes(&bytes);
    assert!(quads_sorted);
}

/// `from_bytes` adopts index children *without* canonicalizing them: a
/// deferred component materializes on its first genuine use (here, an
/// index-routed probe) — and only the family the probe touches, so an
/// object family never queried stays un-executed. Serialization is such a
/// use too, and its output must not depend on the deferral state.
#[tokio::test]
async fn test_from_bytes_defers_index_child_materialization() {
    let bytes = built_store(
        modular_quads(12, 3, 4),
        LayoutStrategy::Default,
        ALL_INDEX_TYPES.to_vec(),
    )
    .await
    .to_bytes()
    .await
    .unwrap();

    let reread = VortexRdfStore::from_bytes(&bytes).await.unwrap();
    for name in ["index:posg", "index:ospg", "index:ref-o", "index:ref-p"] {
        assert_eq!(
            reread.debug_index_component_materialized(name),
            Some(false),
            "{name}: from_bytes must not materialize index children"
        );
    }

    // Serializing the deferred store materializes for the write without
    // changing the bytes: the wire form is deferral-independent.
    let deferred_bytes = reread.to_bytes().await.unwrap();

    let reread = VortexRdfStore::from_bytes(&bytes).await.unwrap();
    let p1 = iri("p1");
    let matched = matched_view(&reread, None, Some(&p1), None, None).await;
    // p1 is on rows 1, 4, 7, 10 — the probe answered through the index.
    assert_eq!(matched.size().await.unwrap(), 4);
    assert_eq!(
        reread.debug_index_component_materialized("index:posg"),
        Some(true),
        "the probed family materializes on first use"
    );
    for name in ["index:ospg", "index:ref-o", "index:ref-p"] {
        assert_eq!(
            reread.debug_index_component_materialized(name),
            Some(false),
            "{name}: an unprobed component stays deferred"
        );
    }
    assert_eq!(
        reread.to_bytes().await.unwrap(),
        deferred_bytes,
        "serialization output must not depend on the deferral state"
    );
}

/// `from_parts` adopts a wire-encoded base into searchable resident form —
/// every sorted column binds an encoded search probe, with children outside
/// the probe's set decoded — while carrying the subject sortedness
/// provenance honestly in both directions: a sorted build's stamp survives
/// the re-encoding, rows written without one gain none.
#[tokio::test]
async fn test_from_parts_adopts_searchable_children() {
    let quads = modular_quads(12, 3, 4);

    // Sorted Dictionary build: code columns arrive wire-encoded after a
    // bytes round-trip, and the s column carries the sorted stamp.
    let bytes = built_store(
        quads.clone(),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await
    .to_bytes()
    .await
    .unwrap();
    let adopted = VortexRdfStore::from_bytes(&bytes).await.unwrap();
    let parts = adopted.to_serializable_parts().await.unwrap();
    let store = VortexRdfStore::from_parts(parts, Default::default()).unwrap();
    assert!(
        store.debug_base_probe_resolvable(),
        "from_parts must leave every sorted column probe-bindable"
    );
    assert_eq!(
        store.view_statistics().unwrap().sort_order,
        Some(SortOrder::Spog),
        "the sorted build's subject stamp must survive the re-encoding"
    );
    for name in ["index:posg", "index:ospg"] {
        assert_eq!(
            store.debug_index_component_materialized(name),
            Some(true),
            "{name}: from_parts must adopt components in resident form"
        );
    }

    // The canonical base answers a fully-bound pattern: s01 p1 "object 1"
    // is row 1 exactly.
    let s = iri_subject("s01");
    let p = iri("p1");
    let o = plain_literal("object 1");
    assert_eq!(
        matched_size(&store, Some(&s), Some(&p), Some(&o), None).await,
        1
    );

    // A sorted column in an encoding no probe reads (pcodec):
    // adoption decodes it and keeps its stamp, so a bound subject is still
    // one searched row range, answering as the built store does.
    let decoded =
        VortexRdfStore::from_parts(pco_coded_build("s").await, Default::default()).unwrap();
    assert!(decoded.debug_base_probe_resolvable());
    assert_eq!(decoded.debug_base_child_int_canonical("s"), Some(true));
    assert_eq!(
        decoded.view_statistics().unwrap().sort_order,
        Some(SortOrder::Spog)
    );
    let reference = built_store(fixture_quads(), LayoutStrategy::Dictionary, vec![]).await;
    for i in [0, 17, 2_999] {
        let subject = subject_node(i, 4);
        let got = matched_view(&decoded, Some(&subject), None, None, None).await;
        assert!(got.debug_selection_range().is_some(), "s{i}: searched");
        let want = matched_view(&reference, Some(&subject), None, None, None).await;
        assert_eq!(view_strings(&got).await, view_strings(&want).await, "s{i}");
    }
    // An unsorted column in such an encoding is decoded too: `from_parts`
    // leaves no integer column without a probe or a primitive.
    let decoded =
        VortexRdfStore::from_parts(pco_coded_build("o").await, Default::default()).unwrap();
    assert_eq!(decoded.debug_base_child_int_canonical("o"), Some(true));
    assert_eq!(view_strings(&decoded).await, view_strings(&reference).await);

    // Rows with no sorted provenance (reversed, rotated): adoption must not
    // invent a stamp they never earned.
    let quads_unsorted = {
        let mut v: Vec<Quad> = quads.iter().rev().cloned().collect();
        v.rotate_left(3);
        v
    };
    let adopted = unstamped_store(&quads_unsorted);
    assert_eq!(
        VortexRdfStore::from_parts(
            adopted.to_serializable_parts().await.unwrap(),
            Default::default()
        )
        .unwrap()
        .view_statistics()
        .unwrap()
        .sort_order,
        None,
        "rows with no sorted provenance must stay unstamped through adoption"
    );
}

/// Serializing a store that carries an append tail emits its rows in sorted
/// order: the artifact claims `quads_sorted`, and a reader keeps the subject
/// fast path without compacting first. The index components are rebuilt over
/// the merged rows, so the round-tripped store keeps its index set under the
/// Dictionary layout (fresh dictionary, code components) and the string
/// layouts alike, and the rebuilt components answer for the base rows and the
/// tail's.
#[tokio::test]
async fn test_tailed_serialization_sorts_and_keeps_indexes() {
    for layout in LAYOUTS {
        for indexes in index_sets() {
            // `front` has a subject that sorts BEFORE the tail's own position
            // in the base, so an appended-at-the-end row must move to claim
            // order; its predicate and object are new to the base. `back`
            // sorts after every base row, under a predicate the base carries,
            // with a brand-new object (no code in the cached dictionary).
            let front = make_quad(
                "http://example.org/s00",
                "http://example.org/p9",
                "object 9",
                GraphName::DefaultGraph,
            );
            let back = make_quad(
                "http://example.org/s99",
                "http://example.org/p1",
                "fresh object",
                GraphName::DefaultGraph,
            );
            let tailed = built_store(modular_quads(12, 3, 4), layout, indexes.clone())
                .await
                .add_quads([front, back])
                .await
                .unwrap();
            assert_eq!(tailed.size().await.unwrap(), 14);

            let bytes = tailed.to_bytes().await.unwrap();
            let (quads_sorted, _) = container::store_metadata_of_bytes(&bytes);
            assert!(
                quads_sorted,
                "{layout:?}/{indexes:?}: a tailed store's serialization must claim sorted order"
            );

            let reread = VortexRdfStore::from_bytes(&bytes).await.unwrap();
            assert_eq!(reread.size().await.unwrap(), 14);
            assert_eq!(
                reread.indexes(),
                indexes.as_slice(),
                "{layout:?}/{indexes:?}: a tailed store's serialization must keep its indexes"
            );
            let subjects: Vec<String> = reread
                .quads()
                .try_collect::<Vec<Quad>>()
                .await
                .unwrap()
                .into_iter()
                .map(|q| q.subject.to_string())
                .collect();
            let mut expected = subjects.clone();
            expected.sort();
            assert_eq!(
                subjects, expected,
                "{layout:?}/{indexes:?}: rewritten rows must be in subject order"
            );
            // The appended row is reachable through the subject fast path the
            // stamp unlocks, not just through a scan.
            let s = iri_subject("s00");
            let p = iri("p9");
            assert_eq!(
                matched_size(&reread, Some(&s), Some(&p), None, None).await,
                1,
                "{layout:?}/{indexes:?}: the appended row survives the reorder"
            );
            // The rebuilt components actually answer: p1 is on base rows 1,
            // 4, 7, 10 plus the appended s99, and the tail's fresh object
            // survives the round-trip.
            let p1 = iri("p1");
            assert_eq!(
                matched_size(&reread, None, Some(&p1), None, None).await,
                5,
                "{layout:?}/{indexes:?}"
            );
            let fresh = plain_literal("fresh object");
            assert_eq!(
                matched_size(&reread, None, None, Some(&fresh), None).await,
                1,
                "{layout:?}/{indexes:?}: the tail row survives the round-trip"
            );
        }
    }
}

/// A lean `from_bytes` store — wire-encoded base, deferred components — must
/// answer every pattern class exactly like the canonical `from_built` store
/// it was serialized from, with its sorted columns bound by encoded search
/// probes rather than the generic per-scalar kernel.
#[tokio::test]
async fn test_from_bytes_and_from_parts_answer_every_pattern_like_from_built() {
    // Repeated subjects (4 quads each) over enough rows that the sorted
    // subject column keeps a compressed wire form worth probing; object
    // cardinality high enough that code columns bit-pack rather than
    // dict-encode, so index prefix probes exercise encoded sliced searches.
    let quads: Vec<Quad> = (0..20_000)
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{:05}", i / 4),
                &format!("http://example.org/p{}", i % 7),
                &format!("object {}", i % 4001),
                GraphName::DefaultGraph,
            )
        })
        .collect();
    let canonical = built_store(quads, LayoutStrategy::Dictionary, ALL_INDEX_TYPES.to_vec()).await;
    let bytes = canonical.to_bytes().await.unwrap();
    let lean = VortexRdfStore::from_bytes(&bytes).await.unwrap();
    let resident = VortexRdfStore::from_parts(
        lean.to_serializable_parts().await.unwrap(),
        Default::default(),
    )
    .unwrap();

    assert!(canonical.debug_base_probe_resolvable());
    assert!(
        lean.debug_base_probe_resolvable(),
        "the lean base's sorted columns must bind encoded probes"
    );
    assert!(
        !lean.debug_base_int_children_canonical(),
        "fixture must exercise a wire-encoded base"
    );
    assert!(
        resident.debug_base_probe_resolvable(),
        "resident adoption must stay probe-bindable"
    );
    assert!(
        !resident.debug_base_int_children_canonical(),
        "resident adoption must retain probeable children compressed"
    );

    let s = iri_subject("s02500");
    let s_absent = iri_subject("zz");
    let p = iri("p4");
    let o = plain_literal("object 7");
    let g = GraphName::DefaultGraph;

    let patterns: [Pattern<'_>; 10] = [
        ("S", Some(&s), None, None, None),
        ("S absent", Some(&s_absent), None, None, None),
        ("P", None, Some(&p), None, None),
        ("O", None, None, Some(&o), None),
        ("G", None, None, None, Some(&g)),
        ("SP", Some(&s), Some(&p), None, None),
        ("PO", None, Some(&p), Some(&o), None),
        ("SPO", Some(&s), Some(&p), Some(&o), None),
        ("SPOG", Some(&s), Some(&p), Some(&o), Some(&g)),
        ("full", None, None, None, None),
    ];
    assert_patterns_agree(
        &canonical,
        &[("lean", &lean), ("resident", &resident)],
        &patterns,
    )
    .await;

    // The subject run itself: 4 quads per subject by construction.
    let s_view = matched_view(&lean, Some(&s), None, None, None).await;
    assert_eq!(s_view.size().await.unwrap(), 4);
}

/// A low-cardinality object column dictionary-encodes on the wire; adoption
/// must retain that form (the probe's dict node binds it) and every pattern
/// over it must still answer exactly like the canonical store.
#[tokio::test]
async fn test_from_parts_retains_dict_children() {
    let quads: Vec<Quad> = (0..20_000)
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{:05}", i / 4),
                &format!("http://example.org/p{}", i % 7),
                &format!("object {}", i % 13),
                GraphName::DefaultGraph,
            )
        })
        .collect();
    let canonical = built_store(
        quads,
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    let bytes = canonical.to_bytes().await.unwrap();
    let lean = VortexRdfStore::from_bytes(&bytes).await.unwrap();
    let resident = VortexRdfStore::from_parts(
        lean.to_serializable_parts().await.unwrap(),
        Default::default(),
    )
    .unwrap();

    assert_eq!(
        resident.debug_base_child_int_canonical(schema::COL_O),
        Some(false),
        "the dictionary-encoded object column must stay compressed through adoption"
    );

    let s = iri_subject("s02500");
    let p = iri("p4");
    let o = plain_literal("object 7");
    let patterns: [Pattern<'_>; 4] = [
        ("O", None, None, Some(&o), None),
        ("PO", None, Some(&p), Some(&o), None),
        ("SPO", Some(&s), Some(&p), Some(&o), None),
        ("SO", Some(&s), None, Some(&o), None),
    ];
    assert_patterns_agree(
        &canonical,
        &[("lean", &lean), ("resident", &resident)],
        &patterns,
    )
    .await;
}

// ─── Writers and parts ─────────────────────────────────────────────────

/// Every writer stamps its index children with `INDEX_VERSION`, the number
/// the open-time ceiling check reads: the streaming file writer behind
/// `serialize_rdf`, the streaming writer into a sink, compaction of a file
/// store, and `to_bytes`. Each child carries its own stamp, so each layout
/// writes the index set that holds both families' children.
#[tokio::test]
async fn test_every_writer_stamps_index_children_with_the_index_version() {
    let index_versions = |bytes: &[u8]| -> Vec<(String, u32)> {
        let (_, components) = container::store_metadata_of_bytes(bytes);
        components
            .into_iter()
            .filter(|component| component.role == container::StoreComponentRole::Index)
            .map(|component| (component.name, component.version))
            .collect()
    };
    let indexes = ALL_INDEX_TYPES.to_vec();
    for layout in LAYOUTS {
        let label = format!("{layout:?}");
        let quads = modular_quads(12, 3, 4);

        let (_dir, path) = write_store_file(quads.clone(), layout, indexes.clone()).await;
        let written = std::fs::read(&path).unwrap();
        let mut sink: Vec<u8> = Vec::new();
        crate::io::quads_stream_to_vortex_writer(
            quad_stream(quads.clone()),
            &mut sink,
            layout,
            indexes.clone(),
        )
        .await
        .unwrap();
        let compacted = VortexRdfStore::from_file(&path)
            .await
            .unwrap()
            .compact()
            .await
            .unwrap();
        drop(compacted);
        let built = streamed_store(quads, layout, indexes.clone())
            .await
            .to_bytes()
            .await
            .unwrap();

        for (writer, bytes) in [
            ("quads_stream_to_vortex_file", written),
            ("quads_stream_to_vortex_writer", sink),
            ("compact", std::fs::read(&path).unwrap()),
            ("to_bytes", built),
        ] {
            let versions = index_versions(&bytes);
            let mut names: Vec<&str> = versions.iter().map(|(name, _)| name.as_str()).collect();
            names.sort_unstable();
            assert_eq!(
                names,
                ["index:ospg", "index:posg", "index:ref-o", "index:ref-p"],
                "{label}: {writer} wrote both families' children"
            );
            for (name, version) in versions {
                assert_eq!(
                    version,
                    container::INDEX_VERSION,
                    "{label}: {writer} stamped {name}"
                );
            }
        }
    }
}

/// `BuiltArray::into_stream` requires a Dictionary-layout primary to come
/// with its dictionary; the precondition is a debug assertion, so it is
/// pinned in debug builds only.
#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "pairs a Dictionary primary with its dictionary")]
fn test_parts_stream_requires_dictionary_beside_code_rows() {
    let parts = crate::store::BuiltArray {
        array: bare_code_quad_array(&[1, 2, 3]),
        components: Vec::new(),
        dict: None,
    };
    let _ = parts.into_stream();
}

/// A store opened from a file and left as it is serializes as that file: its
/// `to_bytes`, and an unconstrained match's, is the file's bytes, copied with
/// nothing read or written, under every layout. Once a narrowing, a tail or a
/// tombstone changes what the store holds, `to_bytes` writes it again, and
/// each reads back with its own rows.
#[tokio::test]
async fn test_untouched_file_store_to_bytes_copies_its_file() {
    use crate::store::persist::serialize::serialize_hooks;

    let quads = modular_quads(60, 3, 4);
    for layout in LAYOUTS {
        let (_dir, path) =
            write_store_file(quads.clone(), layout, vec![IndexType::SecondaryByCopy]).await;
        let file_bytes = std::fs::read(&path).unwrap();
        let store = VortexRdfStore::from_file(&path).await.unwrap();

        let copied = serialize_hooks::copied();
        let unconstrained = matched_view(&store, None, None, None, None).await;
        for view in [&store, &unconstrained] {
            assert_eq!(view.to_bytes().await.unwrap(), file_bytes, "{layout:?}");
        }
        assert_eq!(serialize_hooks::copied() - copied, 2, "{layout:?}");

        let p1 = iri("p1");
        let narrowed = matched_view(&store, None, Some(&p1), None, None).await;
        let tailed = store.add_quad(extra_quad()).await.unwrap();
        let tombstoned = store.delete_quad(&quads[0]).await.unwrap();
        for (tag, view, rows) in [
            ("narrowed", &narrowed, 20),
            ("tailed", &tailed, 61),
            ("tombstoned", &tombstoned, 59),
        ] {
            let bytes = view.to_bytes().await.unwrap();
            assert_ne!(bytes, file_bytes, "{layout:?} {tag}");
            let reread = VortexRdfStore::from_bytes(&bytes).await.unwrap();
            assert_eq!(reread.size().await.unwrap(), rows, "{layout:?} {tag}");
        }
        assert_eq!(serialize_hooks::copied() - copied, 2, "{layout:?}");
    }
}
