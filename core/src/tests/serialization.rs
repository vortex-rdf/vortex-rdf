//! The vortex-rdf.store.v1 wire contract. Every test here asserts the same
//! contract from a different starting state (tailed, tombstoned,
//! file-backed, dictionary, locally-sorted multi-chunk): `to_bytes` /
//! `from_bytes` must carry the store's index components and its sortedness
//! provenance faithfully — rebuilt where a mutation invalidated them, never
//! silently dropped, never over-claimed.

use super::*;
use crate::io::container;
use crate::store::schema;

// ─── Wire round-trips ──────────────────────────────────────────────────

/// A tailed store's `to_bytes` rebuilds its index components over the
/// merged rows, so the round-tripped store keeps its index set under the
/// Dictionary layout (fresh dictionary, code components) and the string
/// layouts alike.
#[tokio::test]
async fn test_tailed_store_serialization_keeps_indexes() {
    for layout in [LayoutStrategy::Dictionary, LayoutStrategy::Default] {
        let quads = modular_quads(12, 3, 4);
        let arr = build_array::<SortedInMemoryBuilder>(
            quad_stream(quads.clone()),
            layout,
            vec![IndexType::SecondaryByCopy, IndexType::SecondaryByReference],
        )
        .await
        .unwrap();
        let store = VortexRdfStore::from_built(arr).unwrap();

        // Tail with a brand-new term (no code in the cached dictionary).
        let added = store
            .add_quads([make_quad(
                "http://example.org/s99",
                "http://example.org/p1",
                "fresh object",
                GraphName::DefaultGraph,
            )])
            .await
            .unwrap();

        let bytes = added.to_bytes().await.unwrap();
        let reread = VortexRdfStore::from_bytes(&bytes).await.unwrap();
        assert_eq!(
            reread.indexes(),
            &[IndexType::SecondaryByCopy, IndexType::SecondaryByReference],
            "{layout:?}: a tailed store's serialization must keep its indexes"
        );
        assert_eq!(reread.size().await.unwrap(), 13);

        // The rebuilt components actually answer: p1 is on base rows 1, 4,
        // 7, 10 plus the appended s99.
        let p1 = NamedNode::new("http://example.org/p1").unwrap();
        let matched = reread
            .match_pattern(None, Some(&p1), None, None)
            .await
            .unwrap();
        assert_eq!(matched.size().await.unwrap(), 5, "{layout:?}");
        let fresh = Term::Literal(Literal::new_simple_literal("fresh object"));
        assert_eq!(
            reread
                .match_pattern(None, None, Some(&fresh), None)
                .await
                .unwrap()
                .size()
                .await
                .unwrap(),
            1,
            "{layout:?}: the tail row survives the round-trip"
        );
    }
}

/// A mutated owner's serialization keeps BOTH provenance channels: tombstones
/// must not silently drop the index children (they are rebuilt over the
/// surviving rows) nor the subject sortedness (a gather preserves row
/// order).
#[tokio::test]
async fn test_tombstoned_store_serialization_keeps_indexes_and_sortedness() {
    let quads = modular_quads(12, 3, 4);
    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(quads.clone()),
        LayoutStrategy::Default,
        vec![IndexType::SecondaryByCopy],
    )
    .await
    .unwrap();
    let store = VortexRdfStore::from_built(arr).unwrap();
    let s0 = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s00").unwrap());
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
    let p0 = NamedNode::new("http://example.org/p0").unwrap();
    // p0 was on rows 0, 3, 6, 9; row 0 was deleted.
    assert_eq!(
        reread
            .match_pattern(None, Some(&p0), None, None)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        3
    );
}

/// A store whose every row is gone — deleted, or never in the base at all
/// (tail only) — still serializes its index roster: the components are
/// rebuilt over the empty row set, so the round-tripped store keeps the
/// index set it was built with.
#[tokio::test]
async fn test_serialize_to_empty_keeps_indexes() {
    let indexes = vec![IndexType::SecondaryByCopy, IndexType::SecondaryByReference];
    for layout in [LayoutStrategy::Dictionary, LayoutStrategy::Default] {
        let arr = build_array::<SortedInMemoryBuilder>(
            quad_stream(modular_quads(6, 3, 4)),
            layout,
            indexes.clone(),
        )
        .await
        .unwrap();
        let store = VortexRdfStore::from_built(arr).unwrap();
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
    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(quads.clone()),
        LayoutStrategy::Default,
        vec![IndexType::SecondaryByCopy, IndexType::SecondaryByReference],
    )
    .await
    .unwrap();
    let store = VortexRdfStore::from_built(arr).unwrap();
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let view = store
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
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
    assert!(!reread.debug_base_subject_sorted());
    assert_eq!(reread.size().await.unwrap(), 12);
    let s07 = subject_node(7, 2);
    assert_eq!(
        view_strings(
            &reread
                .match_pattern(Some(&s07), None, None, None)
                .await
                .unwrap()
        )
        .await,
        expected_strings(&quads, |i| quads[i].subject == s07)
    );
}

/// An unrefined file-backed store's `to_bytes` reads its index children
/// wholesale, so the byte round-trip keeps the index set.
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
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    assert_eq!(
        reread
            .match_pattern(None, Some(&p1), None, None)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        4
    );
}

/// A sorted store's `quads_sorted` provenance survives the file → bytes
/// round-trip: the file arm of serialization re-stamps from the root
/// metadata, never from stats a multi-chunk scan lost.
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
    let bytes = store.to_bytes().await.unwrap();
    let (quads_sorted, _) = container::store_metadata_of_bytes(&bytes);
    assert!(
        quads_sorted,
        "file-backed to_bytes must carry the sorted provenance across"
    );
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
            let mut keys: Vec<(CopyKey<String>, u32)> = rows
                .iter()
                .enumerate()
                .map(|(i, q)| {
                    let spog = [q.s.clone(), q.p.clone(), q.o.clone(), q.g.clone()];
                    let key = match family {
                        CopyFamily::Posg => CopyKey::posg(&spog),
                        CopyFamily::Ospg => CopyKey::ospg(spog),
                    };
                    (key, (n * 4 + i) as u32)
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
            container::NativeComponentWrite::new(
                container::StoreComponentDescriptor {
                    name: family.component_name().into(),
                    role: container::StoreComponentRole::Index,
                    implementation: family.component_slug().into(),
                    version: 1,
                    required: false,
                    // Per-chunk sorts only: the writer may not claim global order.
                    sorted: false,
                    dtype: crate::store::indexes::copy::out_of_core::copy_child_dtype(false),
                },
                std::sync::Arc::new(container::BufferedComponentSource::try_new(chunks).unwrap()),
                container::default_child_strategy(),
            )
            .unwrap()
        })
        .collect();
    let bytes = unstamped_store_bytes(quad_chunks, components).await;

    let store = VortexRdfStore::from_bytes(&bytes).await.unwrap();
    assert_eq!(store.indexes(), &[IndexType::SecondaryByCopy]);
    let p = NamedNode::new("http://example.org/p/1").unwrap();
    let matched: Vec<Quad> = store
        .match_pattern(None, Some(&p), None, None)
        .await
        .unwrap()
        .quads()
        .unwrap()
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
    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(modular_quads(12, 3, 4)),
        LayoutStrategy::Default,
        vec![IndexType::SecondaryByCopy, IndexType::SecondaryByReference],
    )
    .await
    .unwrap();
    let bytes = VortexRdfStore::from_built(arr)
        .unwrap()
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
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let matched = reread
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
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
    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(quads.clone()),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await
    .unwrap();
    let bytes = VortexRdfStore::from_built(arr)
        .unwrap()
        .to_bytes()
        .await
        .unwrap();
    let adopted = VortexRdfStore::from_bytes(&bytes).await.unwrap();
    let parts = adopted.to_serializable_parts().await.unwrap();
    let store = VortexRdfStore::from_parts(parts).unwrap();
    assert!(
        store.debug_base_probe_resolvable(),
        "from_parts must leave every sorted column probe-bindable"
    );
    assert!(
        store.debug_base_subject_sorted(),
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
    let s = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s01").unwrap());
    let p = NamedNode::new("http://example.org/p1").unwrap();
    let o = Term::Literal(Literal::new_simple_literal("object 1"));
    assert_eq!(
        store
            .match_pattern(Some(&s), Some(&p), Some(&o), None)
            .await
            .unwrap()
            .size()
            .await
            .unwrap(),
        1
    );

    // Rows with no sorted provenance (reversed, rotated): adoption must not
    // invent a stamp they never earned.
    let quads_unsorted = {
        let mut v: Vec<Quad> = quads.iter().rev().cloned().collect();
        v.rotate_left(3);
        v
    };
    let adopted = unstamped_store(&quads_unsorted);
    assert!(
        !VortexRdfStore::from_parts(adopted.to_serializable_parts().await.unwrap())
            .unwrap()
            .debug_base_subject_sorted(),
        "rows with no sorted provenance must stay unstamped through adoption"
    );
}

/// Serializing a store that carries an append tail emits its rows in sorted
/// order: the artifact claims `quads_sorted`, and a reader keeps the subject
/// fast path without compacting first.
#[tokio::test]
async fn test_tailed_serialization_restores_sorted_order() {
    for layout in [
        LayoutStrategy::Default,
        LayoutStrategy::TypedObject,
        LayoutStrategy::Dictionary,
    ] {
        for indexes in [
            vec![],
            vec![IndexType::SecondaryByReference],
            vec![IndexType::SecondaryByCopy],
            vec![IndexType::SecondaryByCopy, IndexType::SecondaryByReference],
        ] {
            let arr = build_array::<SortedInMemoryBuilder>(
                quad_stream(modular_quads(12, 3, 4)),
                layout,
                indexes.clone(),
            )
            .await
            .unwrap();
            // A subject that sorts BEFORE the tail's own position in the
            // base, so an appended-at-the-end row must move to claim order.
            let appended = make_quad(
                "http://example.org/s00",
                "http://example.org/p9",
                "object 9",
                GraphName::DefaultGraph,
            );
            let tailed = VortexRdfStore::from_built(arr)
                .unwrap()
                .add_quad(appended.clone())
                .await
                .unwrap();
            assert_eq!(tailed.size().await.unwrap(), 13);

            let bytes = tailed.to_bytes().await.unwrap();
            let (quads_sorted, _) = container::store_metadata_of_bytes(&bytes);
            assert!(
                quads_sorted,
                "{layout:?}/{indexes:?}: a tailed store's serialization must claim sorted order"
            );

            let reread = VortexRdfStore::from_bytes(&bytes).await.unwrap();
            assert_eq!(reread.size().await.unwrap(), 13);
            assert_eq!(reread.indexes(), indexes.as_slice(), "{layout:?}");
            let subjects: Vec<String> = reread
                .quads()
                .unwrap()
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
            let s = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s00").unwrap());
            let p = NamedNode::new("http://example.org/p9").unwrap();
            assert_eq!(
                reread
                    .match_pattern(Some(&s), Some(&p), None, None)
                    .await
                    .unwrap()
                    .size()
                    .await
                    .unwrap(),
                1,
                "{layout:?}/{indexes:?}: the appended row survives the reorder"
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
    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(quads),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy, IndexType::SecondaryByReference],
    )
    .await
    .unwrap();
    let canonical = VortexRdfStore::from_built(arr).unwrap();
    let bytes = canonical.to_bytes().await.unwrap();
    let lean = VortexRdfStore::from_bytes(&bytes).await.unwrap();
    let resident = VortexRdfStore::from_parts(lean.to_serializable_parts().await.unwrap()).unwrap();

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

    let s = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s02500").unwrap());
    let s_absent = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/zz").unwrap());
    let p = NamedNode::new("http://example.org/p4").unwrap();
    let o = Term::Literal(Literal::new_simple_literal("object 7"));
    let g = GraphName::DefaultGraph;

    type Pattern<'a> = (
        &'a str,
        Option<&'a NamedOrBlankNode>,
        Option<&'a NamedNode>,
        Option<&'a Term>,
        Option<&'a GraphName>,
    );
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
    for (name, ps, pp, po, pg) in patterns {
        let want = canonical.match_pattern(ps, pp, po, pg).await.unwrap();
        for (kind, store) in [("lean", &lean), ("resident", &resident)] {
            let got = store.match_pattern(ps, pp, po, pg).await.unwrap();
            assert_eq!(
                got.size().await.unwrap(),
                want.size().await.unwrap(),
                "size diverged on [{name}] ({kind})"
            );
            assert_eq!(
                view_strings(&got).await,
                view_strings(&want).await,
                "results diverged on [{name}] ({kind})"
            );
        }
    }

    // The subject run itself: 4 quads per subject by construction.
    let s_view = lean
        .match_pattern(Some(&s), None, None, None)
        .await
        .unwrap();
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
    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(quads),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await
    .unwrap();
    let canonical = VortexRdfStore::from_built(arr).unwrap();
    let bytes = canonical.to_bytes().await.unwrap();
    let lean = VortexRdfStore::from_bytes(&bytes).await.unwrap();
    let resident = VortexRdfStore::from_parts(lean.to_serializable_parts().await.unwrap()).unwrap();

    assert_eq!(
        resident.debug_base_child_int_canonical(schema::COL_O),
        Some(false),
        "the dictionary-encoded object column must stay compressed through adoption"
    );

    let s = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s02500").unwrap());
    let p = NamedNode::new("http://example.org/p4").unwrap();
    let o = Term::Literal(Literal::new_simple_literal("object 7"));
    for (name, ps, pp, po) in [
        ("O", None, None, Some(&o)),
        ("PO", None, Some(&p), Some(&o)),
        ("SPO", Some(&s), Some(&p), Some(&o)),
        ("SO", Some(&s), None, Some(&o)),
    ] {
        let want = canonical.match_pattern(ps, pp, po, None).await.unwrap();
        for (kind, store) in [("lean", &lean), ("resident", &resident)] {
            let got = store.match_pattern(ps, pp, po, None).await.unwrap();
            assert_eq!(
                view_strings(&got).await,
                view_strings(&want).await,
                "results diverged on [{name}] ({kind})"
            );
        }
    }
}

// ─── Open errors ───────────────────────────────────────────────────────

/// Open `bytes` both ways — `from_bytes` over the buffer and `from_file` over
/// a temp file holding the same bytes — so every open-path contract below
/// is asserted on each reader.
async fn open_both(
    bytes: &[u8],
) -> (
    crate::error::Result<VortexRdfStore>,
    crate::error::Result<VortexRdfStore>,
) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.vortex");
    std::fs::write(&path, bytes).unwrap();
    let from_file = VortexRdfStore::from_file(&path).await;
    (VortexRdfStore::from_bytes(bytes).await, from_file)
}

/// A component descriptor of the shape a future change set could write: an
/// index child with an implementation slug this version does not know.
fn unknown_component(required: bool) -> container::NativeComponentWrite {
    let rows = bare_code_quad_array(&[1, 2, 3]);
    container::NativeComponentWrite::new(
        container::StoreComponentDescriptor {
            name: "index:future".into(),
            role: container::StoreComponentRole::Index,
            implementation: "future-index-v1".into(),
            version: 1,
            required,
            sorted: false,
            dtype: rows.dtype().clone(),
        },
        std::sync::Arc::new(container::BufferedComponentSource::try_new(vec![rows]).unwrap()),
        container::default_child_strategy(),
    )
    .unwrap()
}

/// Dictionary-dtype rows written without a dictionary component: bare codes
/// cannot self-describe, and both readers refuse to open them.
#[tokio::test]
async fn test_open_rejects_dictionary_rows_without_dictionary_component() {
    let bytes = unstamped_store_bytes(vec![bare_code_quad_array(&[0, 1, 2])], vec![]).await;
    let (from_bytes, from_file) = open_both(&bytes).await;
    for (path, result) in [("from_bytes", from_bytes), ("from_file", from_file)] {
        let err = result.err().expect("open should fail");
        assert!(
            err.to_string().contains("no dictionary"),
            "{path}: unexpected error: {err}"
        );
    }
}

/// A dictionary component of an implementation this version does not know
/// is rejected at open, before any term is read.
#[tokio::test]
async fn test_open_rejects_unknown_dictionary_implementation() {
    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(dictionary_test_quads()),
        LayoutStrategy::Dictionary,
        vec![],
    )
    .await
    .unwrap();
    let parts = VortexRdfStore::from_built(arr)
        .unwrap()
        .to_serializable_parts()
        .await
        .unwrap();
    let mut dict = parts.dict.as_ref().unwrap().to_write().unwrap();
    dict.descriptor.implementation = "not-a-dictionary-v0".into();
    let bytes = unstamped_store_bytes(vec![parts.array.clone()], vec![dict]).await;

    let (from_bytes, from_file) = open_both(&bytes).await;
    for (path, result) in [("from_bytes", from_bytes), ("from_file", from_file)] {
        let err = result.err().expect("open should fail");
        assert!(
            err.to_string()
                .contains("unsupported dictionary component implementation"),
            "{path}: unexpected error: {err}"
        );
    }
}

/// A required component this version cannot interpret makes the store
/// unopenable — skipping it could silently change query results — while an
/// optional unknown component is skipped and the store answers as if it
/// were absent.
#[tokio::test]
async fn test_open_required_unknown_component_rejected_optional_skipped() {
    let quads = modular_quads(6, 3, 4);
    let raws: Vec<crate::store::RawQuad> =
        quads.iter().map(crate::store::RawQuad::from_quad).collect();
    let rows = || {
        crate::store::builders::build_struct_array(&raws, LayoutStrategy::Default, false).unwrap()
    };

    let bytes = unstamped_store_bytes(vec![rows()], vec![unknown_component(true)]).await;
    let (from_bytes, from_file) = open_both(&bytes).await;
    for (path, result) in [("from_bytes", from_bytes), ("from_file", from_file)] {
        let err = result.err().expect("open should fail");
        assert!(
            err.to_string()
                .contains("required component this version cannot"),
            "{path}: unexpected error: {err}"
        );
    }

    let bytes = unstamped_store_bytes(vec![rows()], vec![unknown_component(false)]).await;
    let (from_bytes, from_file) = open_both(&bytes).await;
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    for (path, result) in [("from_bytes", from_bytes), ("from_file", from_file)] {
        let store = result.unwrap_or_else(|e| panic!("{path}: open should succeed: {e}"));
        assert_eq!(store.size().await.unwrap(), 6, "{path}");
        assert!(store.indexes().is_empty(), "{path}");
        assert_eq!(
            view_strings(
                &store
                    .match_pattern(None, Some(&p1), None, None)
                    .await
                    .unwrap()
            )
            .await,
            expected_strings(&quads, |i| i % 3 == 1),
            "{path}"
        );
    }
}

/// An index child holding one row fewer than the quads is a corrupt or
/// foreign file; both readers reject it at open with the row counts.
#[tokio::test]
async fn test_open_rejects_index_child_with_mismatched_rows() {
    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(modular_quads(6, 3, 4)),
        LayoutStrategy::Default,
        vec![IndexType::SecondaryByCopy],
    )
    .await
    .unwrap();
    let parts = VortexRdfStore::from_built(arr)
        .unwrap()
        .to_serializable_parts()
        .await
        .unwrap();
    let components: Vec<container::NativeComponentWrite> = parts
        .components
        .iter()
        .map(|component| {
            use vortex_array::IntoArray as _;
            let descriptor = component.to_write().unwrap().descriptor;
            let rows = component.rows().unwrap().clone().into_array();
            let short = rows.slice(0..rows.len() - 1).unwrap();
            container::NativeComponentWrite::new(
                descriptor,
                std::sync::Arc::new(
                    container::BufferedComponentSource::try_new(vec![short]).unwrap(),
                ),
                container::default_child_strategy(),
            )
            .unwrap()
        })
        .collect();
    let bytes = unstamped_store_bytes(vec![parts.array.clone()], components).await;

    let (from_bytes, from_file) = open_both(&bytes).await;
    for (path, result) in [("from_bytes", from_bytes), ("from_file", from_file)] {
        let err = result.err().expect("open should fail");
        assert!(
            err.to_string().contains("holds 5 rows against 6 quad rows"),
            "{path}: unexpected error: {err}"
        );
    }
}

/// A generic Vortex writer's output — a valid Vortex file under a plain root
/// layout — is not a store, and `from_bytes` says so by naming the expected
/// root layout.
#[tokio::test]
async fn test_from_bytes_rejects_foreign_vortex_bytes() {
    use vortex_file::WriteOptionsSessionExt as _;

    let array = bare_code_quad_array(&[1, 2, 3]);
    let dtype = array.dtype().clone();
    let stream = vortex_array::stream::ArrayStreamAdapter::new(
        dtype,
        Box::pin(futures::stream::once(async move { Ok(array) })),
    );
    let mut bytes: Vec<u8> = Vec::new();
    crate::session::VORTEX_SESSION
        .write_options()
        .write(&mut bytes, stream)
        .await
        .unwrap();

    let err = VortexRdfStore::from_bytes(&bytes)
        .await
        .err()
        .expect("open should fail");
    assert!(
        err.to_string().contains("not a vortex-rdf store file"),
        "unexpected error: {err}"
    );
}

/// `serialize_parts` requires a Dictionary-layout primary to come with its
/// dictionary; the precondition is a debug assertion, so it is pinned in
/// debug builds only.
#[cfg(debug_assertions)]
#[tokio::test]
#[should_panic(expected = "pairs a Dictionary primary with its dictionary")]
async fn test_serialize_parts_requires_dictionary_beside_code_rows() {
    let parts = crate::store::StoreParts {
        array: bare_code_quad_array(&[1, 2, 3]),
        components: Vec::new(),
        dict: None,
        quads_sorted: false,
    };
    let _ = crate::io::ser::serialize_parts(&parts, &mut Vec::new()).await;
}

// ─── Builder parity on the wire ────────────────────────────────────────

/// The push-based sink and the sorted in-memory builder serialize the same
/// quads to the same bytes: the sink's single-chunk array, dictionary and
/// components are the builder's.
#[tokio::test]
async fn test_dictionary_quad_sink_serializes_like_the_builder() {
    let quads = dictionary_test_quads();
    let mut sink = DictionaryQuadSink::new(vec![IndexType::SecondaryByCopy]);
    for quad in &quads {
        sink.push(crate::store::RawQuad::from_quad(quad));
    }
    let sunk = VortexRdfStore::from_built(sink.finish().unwrap()).unwrap();
    let built = VortexRdfStore::from_built(
        build_array::<SortedInMemoryBuilder>(
            quad_stream(quads),
            LayoutStrategy::Dictionary,
            vec![IndexType::SecondaryByCopy],
        )
        .await
        .unwrap(),
    )
    .unwrap();
    assert_eq!(
        sunk.to_bytes().await.unwrap(),
        built.to_bytes().await.unwrap(),
        "the sink's wire form must be the builder's"
    );
}

/// The store's own files keep their code columns on the encodings the
/// probes read in place: no FastLanes delta in the quad child or the index
/// children, however well it would compress a sorted column. The adoption
/// keeps wire encodings the probes bind (delta included), so the trees seen
/// here are the writer's.
#[tokio::test]
async fn test_written_code_columns_avoid_delta() {
    let graphs: Vec<GraphName> = (0..4)
        .map(|i| GraphName::NamedNode(NamedNode::new(format!("http://example.org/g{i}")).unwrap()))
        .collect();
    let quads = graph_modular_quads(32_768, 5, 32, 1024, &graphs);
    let store = VortexRdfStore::from_quads(
        quad_stream(quads),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await
    .unwrap();
    let bytes = store.to_bytes().await.unwrap();
    let adopted = VortexRdfStore::from_bytes_owned(bytes).await.unwrap();
    let parts = adopted.to_serializable_parts().await.unwrap();
    let mut trees = vec![(
        "quads".to_string(),
        format!("{}", parts.array.display_tree_encodings_only()),
    )];
    for component in &parts.components {
        use vortex_array::IntoArray as _;
        let rows = component.rows().unwrap().clone().into_array();
        trees.push((
            component.name.to_string(),
            format!("{}", rows.display_tree_encodings_only()),
        ));
    }
    for (name, tree) in &trees {
        assert!(
            !tree.contains("fastlanes.delta"),
            "{name} carries a delta-encoded column:\n{tree}"
        );
    }
    // The guard bites: a 32 Ki-row sorted subject column is exactly what the
    // stock cascade would delta-encode, and it stays on a word-addressable
    // encoding instead.
    let quads_tree = &trees[0].1;
    assert!(
        [
            "fastlanes.bitpacked",
            "fastlanes.for",
            "vortex.runend",
            "vortex.sequence"
        ]
        .iter()
        .any(|id| quads_tree.contains(id)),
        "quads tree:\n{quads_tree}"
    );
}
