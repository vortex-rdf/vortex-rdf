//! The Dictionary layout on files: the residency axis (a dictionary left in
//! its serialized child, probed by point reads, versus lifted resident) and
//! the native container round-trips of the dictionary child.

use super::*;
use crate::io::container::{self, DICT_COMPONENT_NAME};
use crate::store::layouts::dictionary::FileBackedDict;
use crate::store::persist::native_file::NativeStoreFile;

// ─── File-backed dictionary ────────────────────────────────────────────

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

/// A file-backed dictionary's shared reads equal the resident open's, whole
/// and index-served: each chunk's distinct codes resolve through one
/// dictionary scan into shared strings, and the chunk decodes against them.
#[tokio::test]
async fn test_file_backed_dictionary_shared_quads_match_resident() {
    let (_dir, path) = write_store_file(
        modular_quads(64, 3, 4),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    let resident = VortexRdfStore::from_file(&path).await.unwrap();
    let fb = VortexRdfStore::from_file_with_dict_residency(&path, 0)
        .await
        .unwrap();
    assert!(fb.debug_dict_file_backed());

    let p0 = NamedNode::new("http://example.org/p0").unwrap();
    let served = |store: &VortexRdfStore| {
        let store = store.clone();
        let p0 = p0.clone();
        async move {
            store
                .match_pattern(None, Some(&p0), None, None)
                .await
                .unwrap()
        }
    };
    for (tag, a, b) in [
        ("full", resident.clone(), fb.clone()),
        ("served", served(&resident).await, served(&fb).await),
    ] {
        let want = assert_shared_matches_quads(&a, &format!("resident {tag}")).await;
        let got = assert_shared_matches_quads(&b, &format!("file-backed {tag}")).await;
        assert_eq!(got, want, "{tag}");
    }
}

/// A store opened with the dictionary forced file-backed must answer every
/// pattern family identically to the resident open of the same file. Hands
/// back the temp dir guard with both opens for further probes.
async fn assert_file_backed_matches_resident(
    indexes: Indexes,
    tag: &str,
) -> (tempfile::TempDir, VortexRdfStore, VortexRdfStore) {
    let quads = dictionary_test_quads();
    let (dir, path) = write_store_file(quads.clone(), LayoutStrategy::Dictionary, indexes).await;

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
    (dir, fb, resident)
}

#[tokio::test]
async fn test_file_backed_dictionary_matches_resident() {
    assert_file_backed_matches_resident(vec![], "dict-child").await;
}

/// With a copy index present, an index-served read on a file-backed store
/// must stream through the async decode path and still agree with resident.
#[tokio::test]
async fn test_file_backed_dictionary_serves_from_copy_index() {
    let (_dir, fb, _resident) =
        assert_file_backed_matches_resident(vec![IndexType::SecondaryByCopy], "copy_index").await;

    // And explicitly confirm the serving plan engages on the file-backed
    // store (the equality above would hold even off the fallback path).
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

    let file =
        NativeStoreFile::try_new(crate::io::read::open_vortex_file(&path).await.unwrap()).unwrap();
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
    assert_shared_matches_quads(&mutated, "file-backed tailed").await;
    let merged = mutated.to_bytes().await.unwrap();
    let merged_store = VortexRdfStore::from_bytes(&merged).await.unwrap();
    assert_eq!(merged_store.size().await.unwrap(), quads.len() + 1);

    // Deletion + compaction rewrite the source file through the lifted
    // dictionary; the reopened store serves the surviving quads.
    let doomed = quads[0].clone();
    let deleted = fb.delete_quad(&doomed).await.unwrap();
    assert_shared_matches_quads(&deleted, "file-backed tombstoned").await;
    let compacted = deleted.compact().await.unwrap();
    assert_eq!(compacted.size().await.unwrap(), quads.len() - 1);
    assert!(!compacted.contains(&doomed).await.unwrap());
}

/// Pins the rows-only read path's dictionary contract: a tombstoned,
/// *indexed* owner (compacting nothing) must answer `code_columns_gathered`
/// with codes addressing the store's cached dictionary — the one
/// `dictionary_snapshot` hands out. Re-encoding this shape against a fresh
/// dictionary of the surviving terms, as a serialization-shaped read would,
/// silently renumbers codes the caller can then only decode wrongly.
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
    // Serialized once per process (see `cached_store_bytes`).
    static BYTES: OnceLock<Vec<u8>> = OnceLock::new();
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
    let outer =
        NativeStoreFile::try_new(crate::io::read::open_vortex_file(&path).await.unwrap()).unwrap();
    let len = dict.len() as u64;
    let fb = FileBackedDict::open(&outer)
        .unwrap()
        .expect("the dictionary child's chunk shape must resolve");

    // Every ~397th term plus both extremes, probed twice (cold + memo).
    let sample: Vec<u32> = (0..len as u32)
        .step_by(397)
        .chain([0, len as u32 - 1])
        .collect();
    for &code in &sample {
        let term = dict.decode(code).unwrap();
        assert_eq!(fb.encode(&term).await.unwrap(), Some(code), "{term}");
        assert_eq!(fb.encode(&term).await.unwrap(), Some(code), "{term}");

        // A control character sorts immediately after the stored term, so
        // the search lands on it and must still report absent.
        let absent = format!("{term}\u{1}");
        assert_eq!(fb.encode(&absent).await.unwrap(), None, "{absent}");
    }
    // Above every stored term: the search runs off the end.
    assert_eq!(fb.encode("\u{10FFFF}").await.unwrap(), None);

    // code → term parity under and over the point-read cap (the wide batch
    // exercises the row-index scan).
    for k in [64usize, 300] {
        let codes: Vec<u32> = (0..len as u32)
            .step_by((len as usize / k).max(1))
            .take(k)
            .collect();
        let want: Vec<String> = codes.iter().map(|&c| dict.decode(c).unwrap()).collect();
        let got: Vec<String> = fb
            .decode_many(&codes)
            .await
            .unwrap()
            .iter()
            .map(|t| t.to_string())
            .collect();
        assert_eq!(got, want);
    }
    // A code past the last term is rejected, an empty batch resolves to
    // nothing.
    assert!(matches!(
        fb.decode_many(&[len as u32]).await,
        Err(VortexRdfError::Deserialization(_))
    ));
    assert!(fb.decode_many(&[]).await.unwrap().is_empty());
}

/// A copy-served run wider than the point-read cap on a file-backed
/// dictionary is decoded chunk by chunk through the async term resolution,
/// agreeing row for row with the resident open of the same file.
#[tokio::test]
async fn test_file_backed_dictionary_serves_wide_run() {
    let quads = modular_quads(900, 3, 4);
    let (_dir, path) = write_store_file(
        quads.clone(),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    let resident = VortexRdfStore::from_file(&path).await.unwrap();
    let fb = VortexRdfStore::from_file_with_dict_residency(&path, 0)
        .await
        .unwrap();
    assert!(fb.debug_dict_file_backed());
    assert!(!resident.debug_dict_file_backed());

    // 300 rows per predicate: past the point-read cap, so the run is read
    // by range.
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let served = fb.match_pattern(None, Some(&p1), None, None).await.unwrap();
    assert!(served.debug_has_serve_plan());
    assert_eq!(served.size().await.unwrap(), 300);
    assert!(served.size().await.unwrap() > crate::store::view::selection::POINT_GATHER_MAX_ROWS);
    let want = expected_strings(&quads, |i| i % 3 == 1);
    assert_eq!(view_strings(&served).await, want);
    assert_eq!(
        view_strings(
            &resident
                .match_pattern(None, Some(&p1), None, None)
                .await
                .unwrap()
        )
        .await,
        want
    );
    let shared = assert_shared_matches_quads(&served, "file-backed wide run").await;
    assert_eq!(shared.len(), 300);
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
    let first_term = dict.decode(0).unwrap();
    assert!(
        first_term.as_str() > "!",
        "fixture must have no term sorting at or below `!`, got {first_term:?}"
    );

    let outer =
        NativeStoreFile::try_new(crate::io::read::open_vortex_file(&path).await.unwrap()).unwrap();
    let fb = FileBackedDict::open(&outer)
        .unwrap()
        .expect("the dictionary child's chunk shape must resolve");

    assert_eq!(fb.encode("!").await.unwrap(), None);
    // And row 0 itself still resolves.
    assert_eq!(fb.encode(&first_term).await.unwrap(), Some(0));
}

/// A dictionary child whose layout shape cannot be point-read — one flat
/// leaf holding the whole struct, with no struct layout to find the term
/// column under — declines the file-backed handle, and an open that asked
/// for a file-backed dictionary lifts it resident instead, answering every
/// pattern exactly.
#[tokio::test]
async fn test_file_backed_dictionary_unaddressable_child_lifts_resident() {
    use crate::io::container::{
        BufferedComponentSource, NativeComponentWrite, StoreComponentDescriptor, StoreComponentRole,
    };
    use std::sync::Arc;
    use vortex_layout::layouts::flat::writer::FlatLayoutStrategy;

    let quads = dictionary_test_quads();
    let built = build_array::<SortedInMemoryBuilder>(
        quad_stream(quads.clone()),
        LayoutStrategy::Dictionary,
        vec![],
    )
    .await
    .unwrap();
    let dict = built
        .dict
        .clone()
        .expect("a Dictionary build carries its dictionary");
    let chunks = dict.child_chunks().unwrap();
    let dtype = chunks[0].dtype().clone();
    // The dictionary component written as a single flat leaf.
    let flat = NativeComponentWrite::new(
        StoreComponentDescriptor {
            name: DICT_COMPONENT_NAME.into(),
            role: StoreComponentRole::Dictionary,
            implementation: container::DICT_IMPLEMENTATION.into(),
            version: 1,
            required: true,
            sorted: true,
            dtype,
        },
        Arc::new(BufferedComponentSource::try_new(chunks).unwrap()),
        Arc::new(FlatLayoutStrategy::default()),
    )
    .unwrap();
    let mut bytes: Vec<u8> = Vec::new();
    container::write_store(
        &crate::session::VORTEX_SESSION,
        &mut bytes,
        vortex_array::stream::ArrayStreamAdapter::new(
            built.array.dtype().clone(),
            Box::pin(stream::iter([Ok(built.array.clone())])),
        ),
        container::default_child_strategy(),
        true,
        vec![flat],
    )
    .await
    .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("flat.vortex");
    std::fs::write(&path, &bytes).unwrap();

    let outer =
        NativeStoreFile::try_new(crate::io::read::open_vortex_file(&path).await.unwrap()).unwrap();
    assert!(
        FileBackedDict::open(&outer).unwrap().is_none(),
        "a flat dictionary child must decline the point-read handle"
    );

    let store = VortexRdfStore::from_file_with_dict_residency(&path, 0)
        .await
        .unwrap();
    assert!(!store.debug_dict_file_backed());
    assert!(store.dictionary_snapshot().is_some());
    assert_eq!(view_strings(&store).await, quad_strings(&quads));
    let p0 = NamedNode::new("http://example.org/p0").unwrap();
    assert_eq!(
        matched_strings(&store, None, Some(&p0), None, None).await,
        expected_strings(&quads, |i| i % 3 == 0)
    );

    // An empty dictionary child declines the same way (nothing to
    // point-read) and opens resident — reachable only when the child still
    // occupies bytes the zero threshold cannot cover.
    let (_dir, empty_path) = write_store_file(Vec::new(), LayoutStrategy::Dictionary, vec![]).await;
    let empty_file = NativeStoreFile::try_new(
        crate::io::read::open_vortex_file(&empty_path)
            .await
            .unwrap(),
    )
    .unwrap();
    if empty_file
        .component_bytes(DICT_COMPONENT_NAME)
        .unwrap()
        .is_some_and(|bytes| bytes > 0)
    {
        assert!(FileBackedDict::open(&empty_file).unwrap().is_none());
        let empty = VortexRdfStore::from_file_with_dict_residency(&empty_path, 0)
            .await
            .unwrap();
        assert!(!empty.debug_dict_file_backed());
        assert!(empty.dictionary_snapshot().is_some());
        assert_eq!(empty.size().await.unwrap(), 0);
    }
}

// ─── Dictionary child round-trips ──────────────────────────────────────

/// Written FSST and adopted as written, the term column comes back
/// compressed — not canonicalized on open — and the terms still resolve
/// through the compressed form.
#[tokio::test]
async fn test_dictionary_terms_stay_fsst_through_bytes() {
    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(fsst_dictionary_quads()),
        LayoutStrategy::Dictionary,
        vec![],
    )
    .await
    .unwrap();
    let store = VortexRdfStore::from_built(arr).unwrap();

    let bytes = store.to_bytes().await.unwrap();
    let reread = VortexRdfStore::from_bytes_owned_as(bytes, crate::store::DictForm::AsWritten)
        .await
        .unwrap();
    assert_dictionary_terms_fsst(&reread, "reread");

    // And the terms still resolve, through the compressed representation.
    assert_eq!(reread.size().await.unwrap(), 2_000);
    let p = NamedNode::new("http://example.org/predicate/3").unwrap();
    let matched = reread
        .match_pattern(None, Some(&p), None, None)
        .await
        .unwrap();
    assert_eq!(matched.size().await.unwrap(), 125);
}

/// The same bytes adopted in the plaintext form decode into one canonical
/// chunk that answers exactly like the store they were written from.
#[tokio::test]
async fn test_plaintext_adoption_is_one_canonical_chunk() {
    let arr = build_array::<SortedInMemoryBuilder>(
        quad_stream(fsst_dictionary_quads()),
        LayoutStrategy::Dictionary,
        vec![],
    )
    .await
    .unwrap();
    let store = VortexRdfStore::from_built(arr).unwrap();

    let bytes = store.to_bytes().await.unwrap();
    let plaintext = VortexRdfStore::from_bytes_owned_as(bytes, crate::store::DictForm::Plaintext)
        .await
        .unwrap();
    assert_dictionary_canonical(&plaintext, "plaintext");

    assert_eq!(plaintext.size().await.unwrap(), 2_000);
    let p = NamedNode::new("http://example.org/predicate/3").unwrap();
    let matched = plaintext
        .match_pattern(None, Some(&p), None, None)
        .await
        .unwrap();
    assert_eq!(matched.size().await.unwrap(), 125);

    let built = store.code_read_snapshot().unwrap();
    let dict = plaintext.code_read_snapshot().unwrap();
    assert_eq!(dict.len(), built.len());
    for code in (0..dict.len() as u32).step_by(97) {
        let term = built.decode(code).unwrap();
        assert_eq!(dict.decode(code).as_deref(), Some(term.as_str()));
        assert_eq!(dict.encode(&term), Some(code), "{term}");
    }
    assert_eq!(dict.encode("\u{10FFFF}"), None);
}

/// The native container end to end through the path-based writer: one
/// self-contained file whose root carries the transparent quad-source child
/// and the dictionary child, reopening with identical results.
#[tokio::test]
async fn test_dictionary_native_file_roundtrip() {
    let quads = dictionary_test_quads();
    let (_dir, path) = write_store_file(quads.clone(), LayoutStrategy::Dictionary, vec![]).await;

    // One self-contained file: the native root with the quad-source child
    // and the dictionary as an auxiliary child.
    assert!(!path.with_extension("dict.vortex").exists());
    let file = crate::io::read::open_vortex_file(&path).await.unwrap();
    assert!(container::is_native_file(&file));
    let names: Vec<_> = file.footer().layout().child_names().collect();
    assert_eq!(names.first().map(|n| n.as_ref()), Some("quad-source"));
    assert!(names.iter().any(|n| n.as_ref() == "dictionary"));

    let store = VortexRdfStore::from_file(&path).await.unwrap();
    assert_eq!(store.layout(), LayoutStrategy::Dictionary);
    assert_eq!(store.size().await.unwrap(), quads.len());

    let p0 = NamedNode::new("http://example.org/p0").unwrap();
    let matched = store
        .match_pattern(None, Some(&p0), None, None)
        .await
        .unwrap();
    assert_eq!(matched.size().await.unwrap(), 4);
    let decoded: Vec<Quad> = store.quads().unwrap().try_collect().await.unwrap();
    assert_eq!(quad_strings(&decoded), quad_strings(&quads));
}

/// An empty Dictionary store still writes (and reopens through) the
/// dictionary segment — the key's presence is unconditional for the layout.
#[tokio::test]
async fn test_dictionary_empty_store_roundtrip() {
    let (_dir, path) = write_store_file(Vec::new(), LayoutStrategy::Dictionary, vec![]).await;

    let store = VortexRdfStore::from_file(&path).await.unwrap();
    assert_eq!(store.layout(), LayoutStrategy::Dictionary);
    assert_eq!(store.size().await.unwrap(), 0);
}

/// A dictionary big enough that the writer splits its child into several
/// chunks must survive the resident lift still FSST-compressed, chunk by
/// chunk, with probe parity — the multi-chunk arm of the term store.
#[tokio::test]
async fn test_large_dictionary_child_lift_keeps_fsst() {
    use crate::store::RawQuad;
    use crate::store::layouts::dictionary::{TermDictionary, TermDictionaryBuilder};
    use vortex_buffer::ByteBuffer;
    use vortex_file::OpenOptionsSessionExt as _;

    // ~200k unique terms → several independent FSST windows, so the child is
    // written (verbatim, through the pass-through strategy) as several flat
    // leaves and splits back into several chunks.
    let mut builder = TermDictionaryBuilder::new();
    for i in 0..50_000u32 {
        builder.insert_quad(&RawQuad {
            s: format!("<http://example.org/subject/{i:07}>"),
            p: format!("<http://example.org/predicate/{i:07}>"),
            o: format!("\"object value number {i:07}\""),
            g: format!("<http://example.org/graph/{i:07}>"),
        });
    }
    let (dict, _code_map) = builder.finish().unwrap();
    let len = dict.len() as u64;

    // As built, one canonical chunk; the FSST windows are made at write,
    // one per 64 Ki terms.
    let built_chunks = dict.term_chunks();
    assert_eq!(built_chunks.len(), 1);
    assert_eq!(
        built_chunks[0].encoding_id().to_string(),
        "vortex.varbinview"
    );
    assert_eq!(
        dict.child_chunks().unwrap().len(),
        (len as usize).div_ceil(65_536)
    );

    let bytes = write_dict_only_store(&dict).await;
    assert!(bytes.len() > 1 << 20, "file too small to force chunking");

    let file = crate::session::VORTEX_SESSION
        .open_options()
        .open_buffer(ByteBuffer::from(bytes))
        .unwrap();
    let typed = file
        .footer()
        .layout()
        .as_::<container::RdfStoreLayoutVTable>();
    let (_, child) = container::store_component(typed, container::DICT_COMPONENT_NAME)
        .unwrap()
        .unwrap();
    assert_eq!(child.row_count(), len);
    let reader = child
        .new_reader(
            container::DICT_COMPONENT_NAME.into(),
            file.segment_source(),
            file.session(),
            &Default::default(),
        )
        .unwrap();
    let lifted = TermDictionary::from_child_reader(reader, crate::store::DictForm::AsWritten)
        .await
        .unwrap();

    // Multi-chunk, and every chunk still FSST.
    let chunks = lifted.term_chunks();
    assert!(chunks.len() > 1, "large dictionary should lift multi-chunk");
    for chunk in &chunks {
        assert_eq!(chunk.encoding_id().to_string(), "vortex.fsst");
    }

    // Probe parity across chunk boundaries, both directions.
    for code in (0..len as u32).step_by(7919) {
        let term = dict.decode(code).unwrap();
        assert_eq!(lifted.encode(&term), Some(code), "{term}");
        assert_eq!(lifted.decode(code).as_deref(), Some(term.as_str()));
    }
    assert_eq!(lifted.encode("\u{10FFFF}"), None);
}
