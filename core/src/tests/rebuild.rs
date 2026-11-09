//! The Dictionary layout's rebuild from codes: an in-memory compaction and a
//! mutated store's serialization rebuild a store whose dictionary is in
//! memory from its base rows' codes, and write exactly what the rebuild from
//! its live quads spelled out writes: the fresh dictionary of their terms,
//! the rows in order each once, and the indexes over them.

use super::*;
use crate::store::builders::build_parts_from_raws;

/// The base of the mutated stores: twelve quads over subjects `s01..s12`,
/// two predicates and three objects, half in a named graph.
fn rebuild_base() -> Vec<Quad> {
    (1..=12)
        .map(|i| {
            let g = if i % 2 == 0 {
                iri_graph("g")
            } else {
                GraphName::DefaultGraph
            };
            make_quad(
                &format!("http://example.org/s{i:02}"),
                &format!("http://example.org/p{}", i % 2),
                &format!("object {}", i % 3),
                g,
            )
        })
        .collect()
}

/// What the tests append: terms the base lacks, sorting before, between and
/// after its own (a literal below every IRI, `s05a`, `s99`, a fresh graph),
/// one quad of base terms only, one whose subject `s03` only a deleted base
/// row holds, and a repeat of a base quad.
fn rebuild_appended(base: &[Quad]) -> Vec<Quad> {
    vec![
        make_quad(
            "http://example.org/s03",
            "http://example.org/p0",
            "object 0",
            GraphName::DefaultGraph,
        ),
        make_quad(
            "http://example.org/s05a",
            "http://example.org/p0",
            "a fresh object",
            GraphName::DefaultGraph,
        ),
        make_quad(
            "http://example.org/s99",
            "http://example.org/p9",
            "object 1",
            iri_graph("h"),
        ),
        make_quad(
            "http://example.org/s01",
            "http://example.org/p0",
            "object 2",
            GraphName::DefaultGraph,
        ),
        base[3].clone(),
    ]
}

/// `quads` as the bytes of a store rebuilt from them spelled out: sorted,
/// each once, over a dictionary of their terms, with `indexes`.
async fn spelled_bytes(quads: &[Quad], indexes: &Indexes) -> Vec<u8> {
    let mut raws: Vec<RawQuad> = quads.iter().map(RawQuad::from_quad).collect();
    raws.sort_unstable();
    raws.dedup();
    let parts = build_parts_from_raws(&raws, LayoutStrategy::Dictionary, indexes).unwrap();
    VortexRdfStore::from_built(parts)
        .unwrap()
        .to_bytes()
        .await
        .unwrap()
}

/// A Dictionary store with an appended tail and tombstones (`s03`'s and
/// `s09`'s rows, so `object 0` loses every base row that held it but one),
/// under each index: its serialization and its compaction are rebuilt from
/// its codes, and each writes the bytes of the rebuild from its live quads
/// spelled out. So does a store compacted down to nothing.
#[tokio::test]
async fn test_a_dictionary_rebuild_from_codes_writes_the_spelled_rebuild() {
    let base = rebuild_base();
    let appended = rebuild_appended(&base);
    for indexes in [
        vec![],
        vec![IndexType::SecondaryByCopy],
        vec![IndexType::SecondaryByReference],
    ] {
        let store = built_store(base.clone(), LayoutStrategy::Dictionary, indexes.clone())
            .await
            .add_quads(appended.clone())
            .await
            .unwrap()
            .delete_quad(&base[2])
            .await
            .unwrap()
            .delete_quad(&base[8])
            .await
            .unwrap();
        let live: Vec<Quad> = base
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != 2 && *i != 8)
            .map(|(_, q)| q.clone())
            .chain(appended.clone())
            .collect();
        let expected = spelled_bytes(&live, &indexes).await;

        let rec = Recorder::start();
        assert!(
            store.to_bytes().await.unwrap() == expected,
            "{indexes:?}: to_bytes"
        );
        let compacted = store.compact().await.unwrap();
        assert_eq!(rec.count(&Event::DictionaryRebuilt), 2, "{indexes:?}");
        assert!(
            compacted.to_bytes().await.unwrap() == expected,
            "{indexes:?}: compact"
        );

        let mut emptied = compacted;
        for quad in &live {
            emptied = emptied.delete_quad(quad).await.unwrap();
        }
        let empty = emptied.compact().await.unwrap();
        assert_eq!(empty.size().await.unwrap(), 0, "{indexes:?}");
        assert!(
            empty.to_bytes().await.unwrap() == spelled_bytes(&[], &indexes).await,
            "{indexes:?}: emptied"
        );
    }
}

/// Codes past `u32::MAX` (the `CodeBase` hook) rebuild as small ones do.
#[tokio::test]
async fn test_a_rebuild_from_codes_past_u32() {
    let _base = crate::store::test_hooks::CodeBase::set(u64::from(u32::MAX) + 7);
    let base = rebuild_base();
    let appended = rebuild_appended(&base);
    let store = built_store(base.clone(), LayoutStrategy::Dictionary, vec![])
        .await
        .add_quads(appended.clone())
        .await
        .unwrap();
    let live: Vec<Quad> = base.iter().cloned().chain(appended).collect();
    let rec = Recorder::start();
    let compacted = store.compact().await.unwrap();
    assert_eq!(rec.count(&Event::DictionaryRebuilt), 1);
    assert!(compacted.to_bytes().await.unwrap() == spelled_bytes(&live, &vec![]).await);
}

/// A store loaded in memory from a foreign file whose rows are out of
/// subject order and repeat one quad is rebuilt from its codes sorted, each
/// quad once, as the spelled rebuild leaves them; a derived view of a
/// file-backed store, whose dictionary stays in the file, is rebuilt from its
/// quads spelled out, to the same bytes.
#[tokio::test]
async fn test_rebuilds_of_unsorted_and_file_backed_stores() {
    let base = rebuild_base();
    let appended = rebuild_appended(&base);
    let live: Vec<Quad> = base.iter().cloned().chain(appended.clone()).collect();
    let expected = spelled_bytes(&live, &vec![]).await;

    let repeated: Vec<Quad> = base.iter().chain([&base[6]]).cloned().collect();
    let (_dir, path) = write_unsorted_store_file(&repeated, 5).await;
    let loaded = VortexRdfStore::from_file_in_memory(&path)
        .await
        .unwrap()
        .add_quads(appended.clone())
        .await
        .unwrap();
    let rec = Recorder::start();
    assert!(loaded.compact().await.unwrap().to_bytes().await.unwrap() == expected);
    assert_eq!(rec.count(&Event::DictionaryRebuilt), 1, "loaded in memory");

    let (_dir, path) = write_store_file(base.clone(), LayoutStrategy::Dictionary, vec![]).await;
    let file = VortexRdfStore::from_file(&path).await.unwrap();
    let view = matched_view(&file, None, None, None, None)
        .await
        .add_quads(appended)
        .await
        .unwrap();
    let rec = Recorder::start();
    assert!(view.compact().await.unwrap().to_bytes().await.unwrap() == expected);
    assert_eq!(
        rec.count(&Event::DictionaryRebuilt),
        0,
        "file-backed dictionary"
    );
}
