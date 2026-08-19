use super::*;
use crate::io::container::{DICT_COMPONENT_NAME, RdfStoreLayoutVTable, store_component};
use crate::store::layouts::dictionary::{FileBackedDict, TermChunks};
use crate::store::native_file::NativeStoreFile;

// ─── 7b) File-backed dictionary ─────────────────────────────────────────

/// Sorted string forms of a pattern match on `store`.
async fn matched_strings(
    store: &VortexRdfStore,
    s: Option<&NamedOrBlankNode>,
    p: Option<&NamedNode>,
    o: Option<&Term>,
    g: Option<&GraphName>,
) -> Vec<String> {
    view_strings(&store.match_pattern(s, p, o, g).await.unwrap()).await
}

/// A store opened with the dictionary forced file-backed must answer every
/// pattern family identically to the resident open of the same file.
async fn assert_file_backed_matches_resident(indexes: Indexes, tag: &str) {
    let quads = dictionary_test_quads();
    let (_dir, path) = write_store_file(quads.clone(), LayoutStrategy::Dictionary, indexes).await;

    let resident = VortexRdfStore::from_file(&path).await.unwrap();
    let fb = VortexRdfStore::from_file_with_dict_residency(&path, 0)
        .await
        .unwrap();

    // Residency is observable through the sync dictionary surface: a
    // file-backed dictionary has no snapshot and no sync code translation.
    assert!(resident.dictionary_snapshot().is_some(), "{tag}");
    assert!(fb.dictionary_snapshot().is_none(), "{tag}");
    // A forced-file-backed open must actually stay file-backed: the written
    // child's shape resolves a wire-chunk handle, so it never falls back to
    // the resident arm.
    assert!(fb.debug_dict_file_backed(), "{tag}");
    assert!(!resident.debug_dict_file_backed(), "{tag}");
    // The code-read gate includes residency: no snapshot, no code decoding.
    assert!(fb.code_read_snapshot().is_none(), "{tag}");
    // The resident open hands one out, and it translates codes both ways.
    let snapshot = resident.code_read_snapshot().expect(tag);
    let code = snapshot.encode("<http://example.org/p0>").expect(tag);
    assert_eq!(
        snapshot.decode(code).as_deref(),
        Some("<http://example.org/p0>"),
        "{tag}"
    );

    let s3 = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s03").unwrap());
    let p0 = NamedNode::new("http://example.org/p0").unwrap();
    let o1 = Term::Literal(Literal::new_simple_literal("object 1"));
    let g = GraphName::NamedNode(NamedNode::new("http://example.org/g").unwrap());
    let default_g = GraphName::DefaultGraph;
    let absent = NamedNode::new("http://example.org/absent").unwrap();

    // Full reconstruction.
    assert_eq!(
        matched_strings(&fb, None, None, None, None).await,
        matched_strings(&resident, None, None, None, None).await,
        "{tag}: full scan"
    );
    assert_eq!(fb.size().await.unwrap(), quads.len(), "{tag}");

    // One pattern per family: subject / predicate / object / graph bound,
    // multi-role, fully bound, and a term absent from the dictionary.
    assert_eq!(
        matched_strings(&fb, Some(&s3), None, None, None).await,
        matched_strings(&resident, Some(&s3), None, None, None).await,
        "{tag}: subject-bound"
    );
    assert_eq!(
        matched_strings(&fb, None, Some(&p0), None, None).await,
        matched_strings(&resident, None, Some(&p0), None, None).await,
        "{tag}: predicate-bound"
    );
    assert_eq!(
        matched_strings(&fb, None, None, Some(&o1), None).await,
        matched_strings(&resident, None, None, Some(&o1), None).await,
        "{tag}: object-bound"
    );
    assert_eq!(
        matched_strings(&fb, None, None, None, Some(&g)).await,
        matched_strings(&resident, None, None, None, Some(&g)).await,
        "{tag}: graph-bound"
    );
    assert_eq!(
        matched_strings(&fb, None, None, None, Some(&default_g)).await,
        matched_strings(&resident, None, None, None, Some(&default_g)).await,
        "{tag}: default-graph-bound"
    );
    assert_eq!(
        matched_strings(&fb, None, Some(&p0), Some(&o1), None).await,
        matched_strings(&resident, None, Some(&p0), Some(&o1), None).await,
        "{tag}: predicate+object"
    );
    let q0 = &quads[3];
    assert!(fb.contains(q0).await.unwrap(), "{tag}: contains");
    let empty = matched_strings(&fb, None, Some(&absent), None, None).await;
    assert!(empty.is_empty(), "{tag}: absent term matches nothing");
}

#[tokio::test]
async fn test_file_backed_dictionary_matches_resident() {
    assert_file_backed_matches_resident(vec![], "dict-child").await;
}

/// With a copy index present, an index-served read on a file-backed store
/// must stream through the async decode path and still agree with resident.
#[tokio::test]
async fn test_file_backed_dictionary_serves_from_copy_index() {
    assert_file_backed_matches_resident(vec![IndexType::SecondaryByCopy], "copy_index").await;

    // And explicitly confirm the serving plan engages on the file-backed
    // store (the equality above would hold even off the fallback path).
    let (_dir, path) = write_store_file(
        dictionary_test_quads(),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    let fb = VortexRdfStore::from_file_with_dict_residency(&path, 0)
        .await
        .unwrap();
    let p0 = NamedNode::new("http://example.org/p0").unwrap();
    let matched = fb.match_pattern(None, Some(&p0), None, None).await.unwrap();
    assert!(matched.debug_has_serve_plan());
    // The located run is small, so its ids resolved eagerly by rid point
    // reads at match time — no deferred rid scan remains.
    assert!(!matched.debug_selection_pending());
    let served: Vec<Quad> = matched.quads().unwrap().try_collect().await.unwrap();
    assert_eq!(served.len(), 4);
}

/// The residency threshold is inclusive and byte-based: exactly at the
/// dictionary child's on-disk size the dictionary lifts resident, one byte
/// below it stays file-backed.
#[tokio::test]
async fn test_file_backed_dictionary_threshold_boundary() {
    let quads = dictionary_test_quads();
    let (_dir, path) = write_store_file(quads, LayoutStrategy::Dictionary, vec![]).await;

    let file = NativeStoreFile::try_new(
        crate::io::native_file::open_vortex_file(&path)
            .await
            .unwrap(),
    )
    .unwrap();
    let dict_bytes = file
        .component_bytes(DICT_COMPONENT_NAME)
        .unwrap()
        .expect("dictionary child present");
    assert!(dict_bytes > 1);

    let at = VortexRdfStore::from_file_with_dict_residency(&path, dict_bytes)
        .await
        .unwrap();
    assert!(at.dictionary_snapshot().is_some());
    let below = VortexRdfStore::from_file_with_dict_residency(&path, dict_bytes - 1)
        .await
        .unwrap();
    assert!(below.dictionary_snapshot().is_none());
}

/// The operations that need the whole dictionary — serialization, mutation
/// with its tail merge, compaction — lift a file-backed dictionary
/// transiently and stay correct.
#[tokio::test]
async fn test_file_backed_dictionary_serializes_and_mutates() {
    let quads = dictionary_test_quads();
    let (_dir, path) = write_store_file(quads.clone(), LayoutStrategy::Dictionary, vec![]).await;
    let fb = VortexRdfStore::from_file_with_dict_residency(&path, 0)
        .await
        .unwrap();

    // Serialization lifts the dictionary transiently and writes it as the
    // dictionary child, which a fresh store decodes standalone.
    let bytes = fb.to_bytes().await.unwrap();
    let reread = VortexRdfStore::from_bytes(&bytes).await.unwrap();
    let expected = quad_strings(&quads);
    let got: Vec<Quad> = reread.quads().unwrap().try_collect().await.unwrap();
    assert_eq!(quad_strings(&got), expected);

    // Mutation: an added quad lands in the string tail; reads merge it with
    // the file-backed base (tail-merge re-encoding lifts transiently).
    let mut mutated = fb.clone();
    let extra = make_quad(
        "http://example.org/added",
        "http://example.org/p0",
        "added object",
        GraphName::DefaultGraph,
    );
    mutated = mutated.add_quad(extra.clone()).await.unwrap();
    assert_eq!(mutated.size().await.unwrap(), quads.len() + 1);
    assert!(mutated.contains(&extra).await.unwrap());
    let merged = mutated.to_bytes().await.unwrap();
    let merged_store = VortexRdfStore::from_bytes(&merged).await.unwrap();
    assert_eq!(merged_store.size().await.unwrap(), quads.len() + 1);

    // Deletion + compaction rewrite the source file through the lifted
    // dictionary; the reopened store serves the surviving quads.
    let doomed = quads[0].clone();
    let deleted = fb.delete_quad(&doomed).await.unwrap();
    let compacted = deleted.compact().await.unwrap();
    assert_eq!(compacted.size().await.unwrap(), quads.len() - 1);
    assert!(!compacted.contains(&doomed).await.unwrap());
}

/// Pins the rows-only read path's dictionary contract: a tombstoned,
/// *indexed* owner (compacting nothing) must answer `code_columns_gathered`
/// with codes addressing the store's cached dictionary — the one
/// `dictionary_snapshot` hands out. The old serialization-shaped read path
/// re-encoded exactly this shape against a fresh dictionary of the surviving
/// terms, silently renumbering codes the caller could only decode wrongly.
#[tokio::test]
async fn test_tombstoned_indexed_codes_address_cached_dictionary() {
    let quads = dictionary_test_quads();
    let (_dir, path) = write_store_file(
        quads.clone(),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    // The default open lifts this small dictionary resident, so the sync
    // snapshot below is available; the quad rows stay file-backed, which is
    // what routes `code_columns_gathered` off the in-memory fast path and
    // through the gathered read.
    let store = VortexRdfStore::from_file(&path).await.unwrap();

    // Tombstone the quad whose subject sorts first among the s-terms: a
    // fresh re-encode of the survivors would shift every later subject's
    // code down by one, so decoding through the cached snapshot would
    // visibly name the wrong terms.
    let deleted = store.delete_quad(&quads[0]).await.unwrap();

    let cols = deleted
        .code_columns_gathered()
        .await
        .unwrap()
        .expect("a tombstoned Dictionary view still answers codes");
    let dict = deleted.dictionary_snapshot().unwrap();
    let got: std::collections::BTreeSet<[String; 4]> = (0..cols[0].len())
        .map(|i| {
            [&cols[0], &cols[1], &cols[2], &cols[3]].map(|col| {
                dict.decode(col[i])
                    .expect("returned codes address the cached dictionary")
            })
        })
        .collect();
    let expected: std::collections::BTreeSet<[String; 4]> = quads[1..]
        .iter()
        .map(|q| {
            let raw = crate::store::RawQuad::from_quad(q);
            [raw.s, raw.p, raw.o, raw.g]
        })
        .collect();
    assert_eq!(got, expected);
}

/// Direct probe parity at multi-chunk scale: every sampled term must resolve
/// to the same code through the child's point-read probe as through the
/// resident dictionary, and mutated absent terms must come back `None`.
#[tokio::test]
async fn test_file_backed_dictionary_probe_parity() {
    // Enough unique terms to spread the dictionary across several chunk
    // leaves, so the probe's binary search genuinely crosses between them.
    // The 20k-quad serialize dominates this test's runtime, so the fixture's
    // bytes are built once per process.
    static BYTES: std::sync::OnceLock<Vec<u8>> = std::sync::OnceLock::new();
    let bytes = cached_store_bytes(&BYTES, || async {
        let quads: Vec<Quad> = (0..20_000)
            .map(|i| {
                make_quad(
                    &format!("http://example.org/s{i:06}"),
                    &format!("http://example.org/p{}", i % 3),
                    &format!("object {i:06}"),
                    GraphName::DefaultGraph,
                )
            })
            .collect();
        let mut bytes: Vec<u8> = Vec::new();
        quads_stream_to_vortex_writer(
            quad_stream(quads),
            &mut bytes,
            LayoutStrategy::Dictionary,
            vec![],
        )
        .await
        .unwrap();
        bytes
    })
    .await;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("probe.vortex");
    std::fs::write(&path, bytes).unwrap();

    // The reference answers, from a resident open of the same file.
    let resident = VortexRdfStore::from_file_with_dict_residency(&path, u64::MAX)
        .await
        .unwrap();
    let dict = resident.dictionary_snapshot().unwrap().0;

    // The probe target, built exactly as `from_file` does file-backed: the
    // dictionary child's cached layout reader plus the wire-chunk handle
    // resolved off the same child.
    let outer = NativeStoreFile::try_new(
        crate::io::native_file::open_vortex_file(&path)
            .await
            .unwrap(),
    )
    .unwrap();
    let (_, reader) = outer
        .component_reader(DICT_COMPONENT_NAME)
        .unwrap()
        .expect("dictionary child present");
    let len = dict.len() as u64;
    assert_eq!(reader.row_count(), len);
    let typed = outer.footer().layout().as_::<RdfStoreLayoutVTable>();
    let (_, dict_child) = store_component(typed, DICT_COMPONENT_NAME)
        .unwrap()
        .expect("dictionary child present");
    let chunks = TermChunks::resolve(&dict_child, outer.segment_source())
        .expect("the dictionary child's chunk shape must resolve");
    let fb = FileBackedDict::new(reader, len, chunks);

    // Every ~397th term plus both extremes, probed twice (cold + memo).
    let sample: Vec<u32> = (0..len as u32)
        .step_by(397)
        .chain([0, len as u32 - 1])
        .collect();
    for &code in &sample {
        let term = dict.term_at(code).unwrap();
        assert_eq!(fb.get_id(&term).await.unwrap(), Some(code), "{term}");
        assert_eq!(fb.get_id(&term).await.unwrap(), Some(code), "{term}");

        // A control character sorts immediately after the stored term, so
        // the search lands on it and must still report absent.
        let absent = format!("{term}\u{1}");
        assert_eq!(fb.get_id(&absent).await.unwrap(), None, "{absent}");
    }
    // Above every stored term: the search runs off the end.
    assert_eq!(fb.get_id("\u{10FFFF}").await.unwrap(), None);

    // ID→term parity under and over the point-read cap (the wide batch
    // exercises the row-index scan).
    for k in [64usize, 300] {
        let codes: Vec<u32> = (0..len as u32)
            .step_by((len as usize / k).max(1))
            .take(k)
            .collect();
        let want: Vec<String> = codes.iter().map(|&c| dict.term_at(c).unwrap()).collect();
        assert_eq!(fb.resolve_terms(&codes).await.unwrap(), want);
    }
}

/// A probe sorting below every dictionary term must come back absent rather
/// than matching row 0 — the binary search's `lo == 0` edge, where the
/// bisection never moves and the final equality check is the only thing
/// rejecting it. The fixture's lowest term is a literal (`"…`), probed with
/// `!`, which sorts before `"`.
#[tokio::test]
async fn test_file_backed_dictionary_rejects_below_first_term() {
    let g = GraphName::NamedNode(NamedNode::new("http://example.org/g").unwrap());
    let quads: Vec<Quad> = (0..3)
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{i}"),
                "http://example.org/p",
                &format!("object {i}"),
                g.clone(),
            )
        })
        .collect();

    let (_dir, path) = write_store_file(quads, LayoutStrategy::Dictionary, vec![]).await;

    let resident = VortexRdfStore::from_file_with_dict_residency(&path, u64::MAX)
        .await
        .unwrap();
    let dict = resident.dictionary_snapshot().unwrap().0;
    let first_term = dict.term_at(0).unwrap();
    assert!(
        first_term.as_str() > "!",
        "fixture must have no term sorting at or below `!`, got {first_term:?}"
    );

    let outer = NativeStoreFile::try_new(
        crate::io::native_file::open_vortex_file(&path)
            .await
            .unwrap(),
    )
    .unwrap();
    let (_, reader) = outer
        .component_reader(DICT_COMPONENT_NAME)
        .unwrap()
        .expect("dictionary child present");
    let dict_len = reader.row_count();
    let typed = outer.footer().layout().as_::<RdfStoreLayoutVTable>();
    let (_, dict_child) = store_component(typed, DICT_COMPONENT_NAME)
        .unwrap()
        .expect("dictionary child present");
    let chunks = TermChunks::resolve(&dict_child, outer.segment_source())
        .expect("the dictionary child's chunk shape must resolve");
    let fb = FileBackedDict::new(reader, dict_len, chunks);

    assert_eq!(fb.get_id("!").await.unwrap(), None);
    // And row 0 itself still resolves.
    assert_eq!(fb.get_id(&first_term).await.unwrap(), Some(0));
}
