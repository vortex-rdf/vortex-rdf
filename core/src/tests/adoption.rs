//! Reading checked-in vortex-rdf.store.v2 fixtures through `from_bytes` —
//! the one open path every target has. The fixtures
//! hold [`dictionary_test_quads`]: `store-default.vortex` under the Default
//! layout with no indexes, `store-dictionary-both-indexes.vortex` under the
//! Dictionary layout with both index families. Setting
//! `VORTEX_RDF_REGEN_FIXTURES` makes `test_fixtures_match_regenerated_stores`
//! rewrite both files from the current writer before comparing.

use super::*;

const DEFAULT_FIXTURE: &[u8] = include_bytes!("fixtures/store-default.vortex");
const DICTIONARY_FIXTURE: &[u8] = include_bytes!("fixtures/store-dictionary-both-indexes.vortex");

/// The Default fixture opens with its layout, empty index roster, row count
/// and quads, and answers a predicate match by scan.
#[tokio::test]
async fn test_default_fixture_adopts() {
    let quads = dictionary_test_quads();
    let store = VortexRdfStore::from_bytes(DEFAULT_FIXTURE).await.unwrap();
    assert_eq!(store.layout(), LayoutStrategy::Default);
    assert!(store.indexes().is_empty());
    assert_eq!(store.size().await.unwrap(), quads.len());
    assert_eq!(view_strings(&store).await, quad_strings(&quads));

    let p1 = iri("p1");
    let matched = matched_view(&store, None, Some(&p1), None, None).await;
    assert_eq!(
        view_strings(&matched).await,
        expected_strings(&quads, |i| i % 3 == 1)
    );
}

/// The Dictionary fixture opens with both index families, decodes to its
/// quads, serves a predicate match through its by-copy index, and hands out
/// a resident dictionary snapshot holding every distinct term. Its
/// components are adopted deferred, probes unresolved, until a match first
/// uses one, which then searches through the probes it resolved.
#[tokio::test]
async fn test_dictionary_fixture_adopts() {
    use crate::store::scan::typed_eq::probe_hooks;

    let quads = dictionary_test_quads();
    let store = VortexRdfStore::from_bytes(DICTIONARY_FIXTURE)
        .await
        .unwrap();
    let components = ["index:posg", "index:ospg", "index:ref-o", "index:ref-p"];
    for name in components {
        assert_eq!(
            store.debug_index_component_probes_resolved(name),
            Some(false),
            "{name}"
        );
    }
    assert_eq!(store.layout(), LayoutStrategy::Dictionary);
    assert_eq!(
        store.indexes(),
        &[IndexType::SecondaryByCopy, IndexType::SecondaryByReference]
    );
    assert_eq!(store.size().await.unwrap(), quads.len());
    assert_eq!(view_strings(&store).await, quad_strings(&quads));

    let p1 = iri("p1");
    let (cached, resolved) = probe_hooks::binds();
    let served = matched_view(&store, None, Some(&p1), None, None).await;
    let after = probe_hooks::binds();
    assert!(after.0 > cached && after.1 == resolved, "{after:?}");
    for name in components {
        assert_eq!(
            store.debug_index_component_probes_resolved(name),
            Some(name == "index:posg"),
            "{name}: resolved at its first use"
        );
    }
    assert_eq!(
        served_component(&served),
        Some("index:posg"),
        "a P match routes through the index"
    );
    assert_eq!(
        view_strings(&served).await,
        expected_strings(&quads, |i| i % 3 == 1)
    );

    let snapshot = store
        .dict_reader()
        .and_then(|reader| reader.snapshot())
        .expect("a resident, untailed Dictionary store hands out a snapshot");
    let terms: std::collections::BTreeSet<String> = tuple_rows(&quads)
        .into_iter()
        .flat_map(|(s, p, o, g)| [s, p, o, g])
        .collect();
    assert_eq!(snapshot.len(), terms.len());
    for (code, term) in terms.iter().enumerate() {
        assert_eq!(
            snapshot.encode(term).unwrap(),
            Some(code as TermCode),
            "{term}"
        );
        assert_eq!(
            snapshot.decode(code as TermCode).as_deref(),
            Some(term.as_str())
        );
    }
}

/// The fixtures are what `to_bytes` writes for their quads today: a
/// regeneration opens to the same layout, index roster and quads.
#[tokio::test]
async fn test_fixtures_match_regenerated_stores() {
    let quads = dictionary_test_quads();
    for (fixture, name, layout, indexes) in [
        (
            DEFAULT_FIXTURE,
            "store-default.vortex",
            LayoutStrategy::Default,
            vec![],
        ),
        (
            DICTIONARY_FIXTURE,
            "store-dictionary-both-indexes.vortex",
            LayoutStrategy::Dictionary,
            vec![IndexType::SecondaryByCopy, IndexType::SecondaryByReference],
        ),
    ] {
        let regenerated = built_store(quads.clone(), layout, indexes).await;
        let bytes = regenerated.to_bytes().await.unwrap();
        if std::env::var_os("VORTEX_RDF_REGEN_FIXTURES").is_some() {
            let path =
                std::path::Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/src/tests/fixtures/"))
                    .join(name);
            std::fs::write(&path, &bytes).unwrap();
        }
        let reread = VortexRdfStore::from_bytes(&bytes).await.unwrap();
        let checked_in = VortexRdfStore::from_bytes(fixture).await.unwrap();
        assert_eq!(checked_in.layout(), reread.layout(), "{layout:?}");
        assert_eq!(checked_in.indexes(), reread.indexes(), "{layout:?}");
        assert_eq!(
            view_strings(&checked_in).await,
            view_strings(&reread).await,
            "{layout:?}"
        );
    }
}
