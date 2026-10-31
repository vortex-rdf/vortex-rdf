//! File-backed stores: zone-map pruning, the bound-subject chunk probe, the
//! per-layout matching matrix over files, a file view's code columns against
//! the same quads read in memory, and `locate_codes` against the term path.
//! The reads that take a pending filter by its masks first are in
//! `masks_first/`.

use super::*;

// ─── Zone-map pruning ──────────────────────────────────────────────────

/// Predicate matches whose zone-map hits are NOT contiguous (clusters at both
/// ends of an s-sorted file, with a middle zone whose stats exclude the
/// predicate) must all survive the metadata row-range pre-pass. A first-gap
/// cutoff would silently drop the trailing cluster.
#[tokio::test]
async fn test_file_backed_non_contiguous_predicate_matches() {
    // Three 8192-row zones. The rare predicate appears only in the first
    // and last 64 rows; the middle zone holds exclusively the common
    // predicate (lexicographically greater), so its zone map excludes the
    // rare one and creates an interior prunable gap.
    const N: usize = 3 * 8192;
    const CLUSTER: usize = 64;

    // Serialized once per process (see `cached_store_file`).
    static BYTES: OnceLock<Vec<u8>> = OnceLock::new();
    let (_dir, path) = cached_store_file(
        &BYTES,
        || {
            (0..N)
                .map(|i| {
                    let p = if !(CLUSTER..N - CLUSTER).contains(&i) {
                        "http://example.org/pAAA"
                    } else {
                        "http://example.org/pMMM"
                    };
                    make_quad(
                        &format!("http://example.org/s{:06}", i),
                        p,
                        "o",
                        GraphName::DefaultGraph,
                    )
                })
                .collect()
        },
        LayoutStrategy::Default,
        vec![],
    )
    .await;

    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let p_rare = iri("pAAA");
    let matched = matched_view(&store, None, Some(&p_rare), None, None).await;
    assert_eq!(matched.size().await.unwrap(), 2 * CLUSTER);

    let results: Vec<Quad> = matched.quads().try_collect().await.unwrap();
    assert_eq!(results.len(), 2 * CLUSTER);
    // Both the leading and the trailing cluster must be present.
    assert!(
        results
            .iter()
            .any(|q| q.subject.to_string() == "<http://example.org/s000000>")
    );
    assert!(
        results
            .iter()
            .any(|q| q.subject.to_string() == format!("<http://example.org/s{:06}>", N - 1))
    );
}

/// Chained matches on a multi-zone file: a subject match narrows the store
/// to a row range from the zone maps; a subsequent indexed object match
/// must restrict its index row ids to that range (not discard the index),
/// and index-to-index chaining must intersect the two id lists.
#[tokio::test]
async fn test_file_backed_chained_subject_then_object_index() {
    const N: usize = 3 * 8192;

    // Serialized once per process (see `cached_store_file`); this shape
    // (reference index, modular predicates) cannot share the non-contiguous
    // fixture's file.
    static BYTES: OnceLock<Vec<u8>> = OnceLock::new();
    let (_dir, path) = cached_store_file(
        &BYTES,
        || graph_modular_quads(N, 6, 3, 5, &[GraphName::DefaultGraph]),
        LayoutStrategy::Default,
        vec![IndexType::SecondaryByReference],
    )
    .await;

    let store = VortexRdfStore::from_file(&path).await.unwrap();

    // The sorted subject column narrows a bound subject to a sub-range of
    // the file via zone-map pruning (row 12000 lives in the middle zone).
    let s_mid = iri_subject("s012000");
    let range = store.debug_subject_pruning_envelope(&s_mid).await.unwrap();
    let range = range.expect("sorted multi-zone file must narrow a bound subject");
    assert!(range.start <= 12000 && 12000 < range.end);
    assert!(
        range.end - range.start < N as u64,
        "envelope must exclude other zones"
    );

    // Subject match first (row range), then indexed object match: the
    // object index ids are restricted to the subject's range.
    let by_subject = matched_view(&store, Some(&s_mid), None, None, None).await;
    let o0 = plain_literal("o0");
    let chained = matched_view(&by_subject, None, None, Some(&o0), None).await;
    assert_eq!(chained.size().await.unwrap(), 1); // 12000 % 5 == 0
    let results: Vec<Quad> = chained.quads().try_collect().await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].subject.to_string(),
        "<http://example.org/s012000>"
    );
    assert_eq!(results[0].object.to_string(), "\"o0\"");

    // Same chain with an object the subject's row doesn't carry.
    let o1 = plain_literal("o1");
    let empty = matched_view(&by_subject, None, None, Some(&o1), None).await;
    assert_eq!(empty.size().await.unwrap(), 0);

    // Index-to-index chaining: object ids ∩ predicate ids.
    let p2 = iri("p2");
    let step1 = matched_view(&store, None, None, Some(&o1), None).await;
    let step2 = matched_view(&step1, None, Some(&p2), None, None).await;
    let expected = (0..N).filter(|i| i % 5 == 1 && i % 3 == 2).count();
    assert_eq!(step2.size().await.unwrap(), expected);
    let results: Vec<Quad> = step2.quads().try_collect().await.unwrap();
    assert_eq!(results.len(), expected);
    assert!(
        results.iter().all(|q| {
            q.predicate == "http://example.org/p2" && q.object.to_string() == "\"o1\""
        })
    );
}

#[tokio::test]
async fn test_file_backed_subject_metadata_range_for_missing_subject() {
    let quads = modular_quads(25, 1, 1);
    let (_dir, path) = write_store_file(quads, LayoutStrategy::Default, vec![]).await;

    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let missing = iri_subject("s99");
    let row_range = store
        .debug_subject_pruning_envelope(&missing)
        .await
        .unwrap();
    assert_eq!(row_range, Some(0..0));
    assert_eq!(
        matched_size(&store, Some(&missing), None, None, None).await,
        0
    );

    // An append's presence check, a fully bound pattern, on the missing
    // subject: its subject conjunct alone proves the envelope empty, so the
    // envelope binds that conjunct and none of the three after it.
    let fresh = VortexRdfStore::from_file(&path).await.unwrap();
    let absent = make_quad(
        "http://example.org/s99",
        "http://example.org/p0",
        "object 0",
        GraphName::DefaultGraph,
    );
    let bound = fresh.debug_bound_exprs().unwrap();
    assert!(!fresh.contains(&absent).await.unwrap());
    assert_eq!(fresh.debug_bound_exprs().unwrap(), bound + 1);
}

/// A file view's `size()` counts its pending filter's rows minus the
/// tombstones over them, so it agrees with the rows the view streams.
#[tokio::test]
async fn test_file_backed_filtered_size_skips_tombstones() {
    let quads = modular_quads(12, 3, 4);
    let (_dir, path) = write_store_file(quads.clone(), LayoutStrategy::Default, vec![]).await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();

    // Row 3: s03 p0 "object 3".
    let deleted = store.delete_quad(&quads[3]).await.unwrap();
    let p0 = iri("p0");
    let matched = matched_view(&deleted, None, Some(&p0), None, None).await;
    // p0 is on rows 0, 3, 6, 9; row 3 is gone.
    assert_eq!(matched.size().await.unwrap(), 3);
    assert_eq!(matched.size().await.unwrap(), matched.quads().count().await);
    assert_eq!(
        view_strings(&matched).await,
        expected_strings(&quads, |i| i % 3 == 0 && i != 3)
    );
}

// ─── Subject chunk probe ───────────────────────────────────────────────

/// Sorted Dictionary quads with 3-quad subject runs — the shape the encoded
/// chunk-probe fast path serves.
fn probe_path_quads(n_subjects: usize) -> Vec<Quad> {
    (0..n_subjects * 3)
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{:04}", i / 3),
                &format!("http://example.org/p{}", i % 5),
                &format!("object {}", i % 9),
                GraphName::DefaultGraph,
            )
        })
        .collect()
}

/// The fast path engages on a sorted Dictionary file — the debug hook
/// reports the exact run — and every subject-bound shape answers exactly
/// like the in-memory store over the same quads: first/mid/last subjects,
/// absent subjects, and [S]/[SP]/[SPO]/[SG] residual composition. Each
/// view's few rows decode point by point through the chunk probes, with no
/// row scan.
#[tokio::test]
async fn test_file_backed_subject_chunk_probe_matches_memory() {
    let quads = probe_path_quads(2000);
    let (_dir, path) = write_store_file(
        quads.clone(),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let memory = built_store(
        quads,
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await;

    // Engagement, pinned directly: the exact 3-row run for a mid subject.
    let mid = subject_node(777, 4);
    let range = store.debug_subject_chunk_probe_range(&mid).await.unwrap();
    assert_eq!(range, Some(777 * 3..777 * 3 + 3), "fast path must engage");

    let p = iri("p1");
    let o = plain_literal("object 3");
    let g = GraphName::DefaultGraph;
    for s in [
        subject_node(0, 4),
        subject_node(777, 4),
        subject_node(1999, 4),
    ] {
        for (pp, po, pg) in [
            (None, None, None),
            (Some(&p), None, None),
            (Some(&p), Some(&o), None),
            (None, None, Some(&g)),
        ] {
            let got = matched_view(&store, Some(&s), pp, po, pg).await;
            let want = matched_view(&memory, Some(&s), pp, po, pg).await;
            let rec = Recorder::start();
            let got = view_strings(&got).await;
            assert_eq!(
                rec.count(&Event::RowScan),
                0,
                "scanned on subject {s} pattern ({pp:?}, {po:?}, {pg:?})"
            );
            assert_eq!(
                got,
                view_strings(&want).await,
                "diverged on subject {s} pattern ({pp:?}, {po:?}, {pg:?})"
            );
        }
    }

    // Absent subject (never in the dictionary) short-circuits to empty.
    let absent = iri_subject("zz");
    let empty = matched_view(&store, Some(&absent), None, None, None).await;
    assert_eq!(empty.size().await.unwrap(), 0);
}

/// Tombstones and tails compose with the probed range: a deleted quad
/// leaves the run, an added quad joins it through the tail.
#[tokio::test]
async fn test_file_backed_subject_chunk_probe_with_mutation() {
    let quads = probe_path_quads(100);
    let (_dir, path) = write_store_file(quads.clone(), LayoutStrategy::Dictionary, vec![]).await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();

    let s50 = subject_node(50, 4);
    let doomed = quads[50 * 3].clone();
    let store = store.delete_quad(&doomed).await.unwrap();
    let matched = matched_view(&store, Some(&s50), None, None, None).await;
    assert_eq!(
        matched.size().await.unwrap(),
        2,
        "tombstone must leave the run"
    );

    let fresh = make_quad(
        "http://example.org/s0050",
        "http://example.org/pnew",
        "fresh",
        GraphName::DefaultGraph,
    );
    let store = store.add_quads([fresh]).await.unwrap();
    let matched = matched_view(&store, Some(&s50), None, None, None).await;
    assert_eq!(
        matched.size().await.unwrap(),
        3,
        "the tail must rejoin the run"
    );
}

/// A chained match composes: narrowing by predicate first, then by subject,
/// must equal the in-memory result — and the second match must not attach a
/// serve plan (the view already carries a restriction).
#[tokio::test]
async fn test_file_backed_subject_chunk_probe_chained() {
    let quads = probe_path_quads(500);
    let (_dir, path) = write_store_file(
        quads.clone(),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let memory = built_store(
        quads,
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await;

    let p = iri("p2");
    let s = subject_node(444, 4);
    let got_first = matched_view(&store, None, Some(&p), None, None).await;
    let got = matched_view(&got_first, Some(&s), None, None, None).await;
    assert_eq!(served_component(&got), None);
    let want = matched_view(
        &matched_view(&memory, None, Some(&p), None, None).await,
        Some(&s),
        None,
        None,
        None,
    )
    .await;
    assert_eq!(view_strings(&got).await, view_strings(&want).await);
}

/// A file whose rows are not globally sorted must NOT engage the fast path —
/// no sortedness, no binary search — while subject matches stay correct
/// through the scan path. The rows are encoded and written directly, rotated
/// out of order and without the sorted stamp — the shape a foreign writer's
/// file arrives in.
#[tokio::test]
async fn test_file_backed_subject_chunk_probe_requires_sorted() {
    let (_dir, path) = write_unsorted_store_file(&probe_path_quads(50), 7).await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let s = subject_node(10, 4);
    assert_eq!(
        store.debug_subject_chunk_probe_range(&s).await.unwrap(),
        None
    );
    let matched = matched_view(&store, Some(&s), None, None, None).await;
    assert_eq!(matched.size().await.unwrap(), 3);
}

// ─── File-backed matching, by layout ───────────────────────────────────

/// One cell of the 3-layout matrix: write the layout's dataset to a file,
/// open it, and hand the store to the layout's `probe` — the probes each
/// bind the term family their layout represents differently on disk.
async fn run_match_pattern_file_test<F, Fut>(layout: LayoutStrategy, quads: Vec<Quad>, probe: F)
where
    F: FnOnce(VortexRdfStore) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let (_dir, path) = write_store_file(quads, layout, vec![]).await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    probe(store).await;
}

/// Default layout over [`two_quads`]: a bound predicate over the string
/// columns, hit and miss.
async fn probe_default(store: VortexRdfStore) {
    let p1 = iri("p1");
    let filtered = matched_view(&store, None, Some(&p1), None, None).await;
    assert_eq!(filtered.size().await.unwrap(), 1);
    let results: Vec<Quad> = filtered.quads().try_collect().await.unwrap();
    assert_eq!(results[0].subject.to_string(), "<http://example.org/s1>");

    let p3 = iri("p3");
    let empty = matched_view(&store, None, Some(&p3), None, None).await;
    assert_eq!(empty.size().await.unwrap(), 0);
}

/// TypedObject layout over [`two_quads`]: a bound object literal (over the
/// typed o_kind/o_value columns), hit and miss, and a bound predicate.
async fn probe_typed_object(store: VortexRdfStore) {
    let o1 = plain_literal("o1");
    let filtered = matched_view(&store, None, None, Some(&o1), None).await;
    assert_eq!(filtered.size().await.unwrap(), 1);
    let results: Vec<Quad> = filtered.quads().try_collect().await.unwrap();
    assert_eq!(results[0].subject.to_string(), "<http://example.org/s1>");
    assert_eq!(results[0].object.to_string(), "\"o1\"");

    let p2 = iri("p2");
    let matched_p = matched_view(&store, None, Some(&p2), None, None).await;
    assert_eq!(matched_p.size().await.unwrap(), 1);

    let o3 = plain_literal("o3");
    let empty = matched_view(&store, None, None, Some(&o3), None).await;
    assert_eq!(empty.size().await.unwrap(), 0);
}

/// Dictionary layout (over `dictionary_test_quads`): a pushed-down integer
/// filter on the code columns, and a term absent from the dictionary.
async fn probe_dictionary(store: VortexRdfStore) {
    let p0 = iri("p0");
    let filtered = matched_view(&store, None, Some(&p0), None, None).await;
    assert_eq!(filtered.size().await.unwrap(), 4);
    let results: Vec<Quad> = filtered.quads().try_collect().await.unwrap();
    assert!(
        results
            .iter()
            .all(|q| q.predicate == "http://example.org/p0")
    );

    let missing_p = iri("nope");
    let empty = matched_view(&store, None, Some(&missing_p), None, None).await;
    assert_eq!(empty.size().await.unwrap(), 0);
}

#[tokio::test]
async fn test_match_file_default() {
    run_match_pattern_file_test(LayoutStrategy::Default, two_quads(), probe_default).await;
}
#[tokio::test]
async fn test_match_file_typed_object() {
    run_match_pattern_file_test(LayoutStrategy::TypedObject, two_quads(), probe_typed_object).await;
}
#[tokio::test]
async fn test_match_file_dictionary() {
    run_match_pattern_file_test(
        LayoutStrategy::Dictionary,
        dictionary_test_quads(),
        probe_dictionary,
    )
    .await;
}

/// A file store's code columns are the codes of the same quads read in
/// memory, on every open and index set: subject-bound patterns (which a
/// point-sized read by id fills straight from the chunk probes), one leaving
/// a code filter pending (`s?o`), windows past their first row, and
/// tombstoned rows. Rows compare as sorted sets: a served view returns them
/// in its index's order.
#[tokio::test]
async fn test_file_code_columns_match_memory() {
    let quads = fixture_quads();
    let memory = built_store(quads.clone(), LayoutStrategy::Dictionary, vec![]).await;
    // Patterns from the fixture's own quads, every 97th, in the four
    // subject-bound shapes, each also windowed past its first row.
    let patterns: Vec<(Option<NamedOrBlankNode>, Option<NamedNode>, Option<Term>)> = quads
        .iter()
        .step_by(97)
        .flat_map(|q| {
            let (s, p, o) = (q.subject.clone(), q.predicate.clone(), q.object.clone());
            [
                (Some(s.clone()), None, None),
                (Some(s.clone()), Some(p.clone()), None),
                (Some(s.clone()), Some(p), Some(o.clone())),
                (Some(s), None, Some(o)),
            ]
        })
        .collect();
    let check = async |tag: &str, store: &VortexRdfStore, memory: &VortexRdfStore| {
        for (s, p, o) in &patterns {
            for window in [None, Some((1, 2))] {
                let view = async |base: &VortexRdfStore| {
                    let v = matched_view(base, s.as_ref(), p.as_ref(), o.as_ref(), None).await;
                    match window {
                        Some((offset, limit)) => v.window(offset, limit).await.unwrap(),
                        None => v,
                    }
                };
                let got = sorted_rows(&view(store).await).await;
                let want = sorted_rows(&view(memory).await).await;
                assert_eq!(got, want, "{tag}: {s:?} {p:?} {o:?} window {window:?}");
            }
        }
    };
    for fixture in file_stores().await {
        check(&fixture.tag(), &fixture.store, &memory).await;
    }
    // Tombstones: the same deletions on a file store and in memory.
    let (_dir, path) = write_store_file(quads.clone(), LayoutStrategy::Dictionary, vec![]).await;
    let doomed = quads.iter().step_by(7).take(60);
    let file = delete_all(
        &VortexRdfStore::from_file(&path).await.unwrap(),
        doomed.clone(),
    )
    .await;
    let mem = delete_all(&memory, doomed).await;
    check("file/tombstoned", &file, &mem).await;
}

/// Quads whose sort-key prefixes repeat at every depth: 40 subjects, each
/// with three predicates, each predicate with two or three objects, each
/// triple in one to three graphs. So a bound `(s, p)` has several objects, a
/// `(p, o)` several subjects and a `(p, o, s)` several graphs: every key of a
/// located run's prefix narrows it.
fn multiplicity_quads() -> Vec<Quad> {
    let mut quads = Vec::new();
    for s in 0..40 {
        for p in 0..3 {
            for o in 0..(2 + (s + p) % 2) {
                for g in 0..=((s + o) % 3) {
                    quads.push(make_quad(
                        &format!("http://example.org/s{s:02}"),
                        &format!("http://example.org/p{}", (s + p) % 5),
                        &format!("o{}", (s * 3 + o) % 7),
                        numbered_graph(g),
                    ));
                }
            }
        }
    }
    quads
}

/// [`multiplicity_quads`] and a hub subject with 60 quads, each object held
/// by no other quad: under the reference index the hub's `s?o` checks the
/// object's one-row reference run, every other subject's its own run, shorter
/// than any object's.
fn hub_quads() -> Vec<Quad> {
    let mut quads = multiplicity_quads();
    for k in 0..60 {
        quads.push(make_quad(
            "http://example.org/hub",
            &format!("http://example.org/p{}", k % 5),
            &format!("rare{k}"),
            GraphName::DefaultGraph,
        ));
    }
    quads
}

/// `locate_codes` answers exactly what the term path answers: for every bound
/// subset of a store's own quads' codes, on every file open and index set
/// (the suite's fixture, and [`multiplicity_quads`] and [`hub_quads`]
/// mapped), the total is `size()` and the rows are the match's, in pages that
/// cover them once. It declines (`None`) rather than answer a tombstoned or
/// narrowed view or a non-prefix shape; a code no quad holds matches nothing.
#[tokio::test]
async fn test_locate_codes_matches_the_term_path() {
    if !crate::store::query::located::locate_enabled() {
        return; // VORTEX_RDF_LOCATE=0: locate_codes declines every pattern by design
    }
    /// The store file at `path`, written under `index`, opened mapped, its
    /// dir guard kept beside it.
    async fn mapped(
        index: IndexConfig,
        (dir, path): (tempfile::TempDir, std::path::PathBuf),
    ) -> FixtureStore {
        FixtureStore {
            index,
            open: Some(Open::Mapped),
            store: VortexRdfStore::from_file(&path).await.unwrap(),
            _dir: Some(std::sync::Arc::new(dir)),
        }
    }

    // Each store beside its tag, the quads it holds and the stride of the
    // rows checked.
    let mut stores: Vec<(String, FixtureStore, Vec<Quad>, usize)> = Vec::new();
    for fixture in file_stores().await {
        stores.push((fixture.tag(), fixture, fixture_quads(), 97));
    }
    for index in IndexConfig::ALL {
        let file = write_store_file(
            multiplicity_quads(),
            LayoutStrategy::Dictionary,
            index.types(),
        )
        .await;
        stores.push((
            format!("multiplicity/{}/mapped", index.name()),
            mapped(index, file).await,
            multiplicity_quads(),
            3,
        ));
    }
    // Seven-row flat leaves: most subject runs cross a leaf boundary, so the
    // narrowing and the reads go across leaves as well as inside one.
    let file = write_chunked_store_file(&multiplicity_quads(), 7).await;
    stores.push((
        "multiplicity/seven-row-leaves/mapped".to_string(),
        mapped(IndexConfig::NoIndex, file).await,
        multiplicity_quads(),
        1,
    ));
    // The reference index in seven-row leaves: a run of `index:ref-p` or
    // `index:ref-o` spans leaves, so the other run is walked leaf by leaf.
    let file = write_chunked_reference_store_file(&multiplicity_quads(), 7).await;
    stores.push((
        "multiplicity/reference/seven-row-leaves/mapped".to_string(),
        mapped(IndexConfig::Reference, file).await,
        multiplicity_quads(),
        3,
    ));
    // A bound subject's rows are checked when its run is the shorter one, a
    // reference run's otherwise: the hub holds both, in one leaf and in
    // seven-row leaves, where a subject's run crosses them.
    let file = write_store_file(
        hub_quads(),
        LayoutStrategy::Dictionary,
        IndexConfig::Reference.types(),
    )
    .await;
    stores.push((
        "hub/reference/mapped".to_string(),
        mapped(IndexConfig::Reference, file).await,
        hub_quads(),
        3,
    ));
    let file = write_chunked_reference_store_file(&hub_quads(), 7).await;
    stores.push((
        "hub/reference/seven-row-leaves/mapped".to_string(),
        mapped(IndexConfig::Reference, file).await,
        hub_quads(),
        3,
    ));
    for (tag, fixture, quads, step) in stores {
        check_locate_codes(&tag, &fixture, &quads, step).await;
    }
    for fixture in file_stores().await {
        check_code_patterns(&fixture).await;
    }
    // A store without a term dictionary has no code patterns.
    let (_dir, path) = write_store_file(fixture_quads(), LayoutStrategy::Default, vec![]).await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let error = store
        .count_code_pattern(crate::CodePattern::default())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("no term dictionary"), "{error}");
}

/// `count_code_pattern` and `match_code_pattern` answer every shape of a
/// store's own codes as the term path does, whether a located run answers it
/// or the general match: the count is the match's size, and pages cover its
/// rows once. A code in a position its term kind cannot take, or past the
/// dictionary, matches nothing.
async fn check_code_patterns(fixture: &FixtureStore) {
    use crate::{CodePattern, Probe};
    let tag = fixture.tag();
    let store = &fixture.store;
    let dict = store.dict_reader().unwrap();
    let all = row_list(store).await;
    let mut patterns = vec![CodePattern::default()];
    // Every bound subset of four quads spread over the store.
    for quad in all.iter().step_by(all.len() / 4) {
        for mask in 1..16u8 {
            let slot = |i: usize| (mask & (1 << i) != 0).then_some(quad[i]);
            patterns.push(CodePattern::new(slot(0), slot(1), slot(2), slot(3)));
        }
    }
    for pattern in patterns {
        let mut terms = Vec::new();
        for code in [
            pattern.subject,
            pattern.predicate,
            pattern.object,
            pattern.graph,
        ] {
            terms.push(match code {
                Some(code) => Some(dict.decode(code).await.unwrap().unwrap()),
                None => None,
            });
        }
        let probe = Probe::parse(
            terms[0].as_deref(),
            terms[1].as_deref(),
            terms[2].as_deref(),
            terms[3].as_deref(),
        )
        .unwrap();
        let want = sorted_rows(&store.run_probe(&probe).await.unwrap()).await;
        let tag = format!("{tag} {pattern:?}");
        assert_eq!(
            store.count_code_pattern(pattern).await.unwrap(),
            want.len() as u64,
            "{tag}: count"
        );
        // About five pages, the last one short.
        let page = want.len() / 4 + 1;
        let mut seen = Vec::new();
        let mut offset = 0;
        loop {
            let got = store
                .match_code_pattern(pattern, offset, Some(page))
                .await
                .unwrap();
            let n = got[0].len();
            seen.extend(code_rows::<4>(&got));
            if n < page {
                break;
            }
            offset += page;
        }
        seen.sort_unstable();
        assert_eq!(seen, want, "{tag}: pages of {page}");
    }
    let kinds = dict.kind_ranges().await.unwrap();
    let mut nothing = vec![CodePattern::new(
        Some(dict.len() as TermCode + 7),
        None,
        None,
        None,
    )];
    if !kinds.literals.is_empty() {
        let literal = Some(kinds.literals.start);
        nothing.push(CodePattern::new(literal, None, None, None));
        nothing.push(CodePattern::new(None, literal, None, None));
        nothing.push(CodePattern::new(None, None, None, literal));
    }
    if !kinds.blanks.is_empty() {
        nothing.push(CodePattern::new(None, Some(kinds.blanks.start), None, None));
    }
    if let Some(default_graph) = kinds.default_graph {
        nothing.push(CodePattern::new(Some(default_graph), None, None, None));
    }
    for pattern in nothing {
        let tag = format!("{tag} {pattern:?}");
        assert_eq!(store.count_code_pattern(pattern).await.unwrap(), 0, "{tag}");
        let got = store.match_code_pattern(pattern, 0, None).await.unwrap();
        assert!(got.iter().all(|column| column.is_empty()), "{tag}");
    }
}

async fn check_locate_codes(tag: &str, fixture: &FixtureStore, quads: &[Quad], step: usize) {
    use crate::{CodePattern, Probe};
    let store = &fixture.store;
    let dict = store.dict_reader().unwrap();
    let all = row_list(store).await;
    let mut answered = std::collections::BTreeSet::new();
    for quad in all.iter().step_by(step) {
        for mask in 1..16u8 {
            let slot = |i: usize| (mask & (1 << i) != 0).then_some(quad[i]);
            let pattern = CodePattern::new(slot(0), slot(1), slot(2), slot(3));
            let Some(full) = store.locate_codes(pattern, 0, None).await.unwrap() else {
                continue;
            };
            answered.insert(mask);
            let mut terms = Vec::new();
            for i in 0..4 {
                terms.push(match slot(i) {
                    Some(code) => Some(dict.decode(code).await.unwrap().unwrap()),
                    None => None,
                });
            }
            let probe = Probe::parse(
                terms[0].as_deref(),
                terms[1].as_deref(),
                terms[2].as_deref(),
                terms[3].as_deref(),
            )
            .unwrap();
            let view = store.run_probe(&probe).await.unwrap();
            let want = sorted_rows(&view).await;
            let tag = format!("{tag} {pattern:?}");
            assert_eq!(
                full.total,
                view.size().await.unwrap() as u64,
                "{tag}: total"
            );
            assert_eq!(
                full.total,
                full.columns[0].len() as u64,
                "{tag}: unwindowed rows"
            );
            let mut located: Vec<[TermCode; 4]> = code_rows(&full.columns);
            located.sort_unstable();
            assert_eq!(located, want, "{tag}: rows");
            for page in [1usize, 3, 64] {
                let mut seen = Vec::new();
                let mut offset = 0;
                while offset < full.total as usize {
                    let got = store
                        .locate_codes(pattern, offset, Some(page))
                        .await
                        .unwrap()
                        .unwrap();
                    assert_eq!(got.total, full.total, "{tag}: page total");
                    let n = got.columns[0].len();
                    assert_eq!(
                        n,
                        page.min(full.total as usize - offset),
                        "{tag}: page {page} at {offset}"
                    );
                    seen.extend(code_rows::<4>(&got.columns));
                    offset += page;
                }
                seen.sort_unstable();
                assert_eq!(seen, want, "{tag}: pages of {page}");
            }
        }
    }
    // Masks over (s, p, o, g) = bits 0..4. A mapped sorted file answers the
    // subject prefixes; the copy index also the posg and ospg ones (`s?o`
    // among them); the reference index every shape with `p` or `o` bound, the
    // other bound slots checked at its rows; the no-index file none of the
    // index-only shapes, and nothing answers `s??g`.
    if fixture.mapped() {
        for mask in [0b0001, 0b0011, 0b0111, 0b1111] {
            assert!(
                answered.contains(&mask),
                "{tag}: mask {mask:04b} not answered"
            );
        }
    }
    if fixture.index == IndexConfig::Copy && fixture.mapped() {
        for mask in [0b0010, 0b0110, 0b0100, 0b0101] {
            assert!(
                answered.contains(&mask),
                "{tag}: mask {mask:04b} not answered"
            );
        }
    }
    if fixture.index == IndexConfig::Reference && fixture.mapped() {
        for mask in [0b0010, 0b0110, 0b0100, 0b0101, 0b1010, 0b1100, 0b1110] {
            assert!(
                answered.contains(&mask),
                "{tag}: mask {mask:04b} not answered"
            );
        }
    }
    if fixture.index == IndexConfig::NoIndex {
        for mask in [0b0010, 0b0110, 0b0100] {
            assert!(
                !answered.contains(&mask),
                "{tag}: mask {mask:04b} has no index to answer it"
            );
        }
    }
    assert!(!answered.contains(&0b1001), "{tag}: s??g answered");
    // A code no quad holds, past the dictionary, as a subject.
    let absent = CodePattern::new(Some(dict.len() as TermCode + 7), None, None, None);
    if let Some(got) = store.locate_codes(absent, 0, None).await.unwrap() {
        assert_eq!(got.total, 0, "{tag}: absent code");
    }
    // Narrowed and tombstoned views decline.
    let s = CodePattern::new(Some(all[0][0]), None, None, None);
    let window = store.window(1, 10).await.unwrap();
    assert!(
        window.locate_codes(s, 0, None).await.unwrap().is_none(),
        "{tag}: window"
    );
    let deleted = store.delete_quad(&quads[0]).await.unwrap();
    assert!(
        deleted.locate_codes(s, 0, None).await.unwrap().is_none(),
        "{tag}: tombstones"
    );
}

/// Object codes of 12 bits but for a few past them, which the writer packs at
/// 12 bits with patches: an `O` or `PO` match over the mapped file compares a
/// zone-pruned slice of the object leaf through the store session's kernel
/// ([`crate::kernels`]), and answers as the store built in memory, whose code
/// columns pack at their full width without patches, does.
#[tokio::test]
async fn test_patched_object_codes_compare_through_the_kernel() {
    use crate::kernels::Kernel;

    // 4,096 literal objects, each 8,192-row zone holding its own 1,024 of
    // them, scattered through the zone so that no run repeats one; in the
    // last zone every 64th row's object is its subject instead, an IRI,
    // whose code passes the literals' 12 bits (literals sort first).
    const ZONE: usize = 8_192;
    let quads: Vec<Quad> = (0..4 * ZONE)
        .map(|i| {
            let subject = format!("http://example.org/s{:05}", i / 8);
            let object = if i >= 3 * ZONE && i % 64 == 0 {
                Term::NamedNode(NamedNode::new(&subject).unwrap())
            } else {
                plain_literal(&format!("v{:04}", (i / ZONE) * 1_024 + (i * 37) % 1_024))
            };
            Quad::new(
                NamedOrBlankNode::NamedNode(NamedNode::new(&subject).unwrap()),
                iri(&format!("p{}", i % 4)),
                object,
                GraphName::DefaultGraph,
            )
        })
        .collect();
    let (_dir, path) = write_store_file(quads.clone(), LayoutStrategy::Dictionary, vec![]).await;
    let file = VortexRdfStore::from_file(&path).await.unwrap();
    let memory = built_store(quads, LayoutStrategy::Dictionary, vec![]).await;
    let rec = Recorder::start();
    for (object, predicate) in [
        (plain_literal("v0000"), None),
        (plain_literal("v0000"), Some(iri("p0"))),
        (plain_literal("v1234"), None),
        (plain_literal("v1234"), Some(iri("p2"))),
        (plain_literal("v3500"), Some(iri("p0"))),
        (Term::NamedNode(iri("s03200")), None),
        (Term::NamedNode(iri("s03200")), Some(iri("p0"))),
    ] {
        let label = format!("{object} {predicate:?}");
        let probe = Probe::new(None, predicate, Some(object), None);
        let mut got = file
            .run_probe(&probe)
            .await
            .unwrap()
            .quads_vec()
            .await
            .unwrap();
        let mut want = memory
            .run_probe(&probe)
            .await
            .unwrap()
            .quads_vec()
            .await
            .unwrap();
        got.sort_by_key(ToString::to_string);
        want.sort_by_key(ToString::to_string);
        assert!(!want.is_empty(), "{label}: the pattern matches");
        assert_eq!(got, want, "{label}: the rows");
    }
    assert!(
        rec.count(&Event::Kernel(Kernel::CompareSlicedPatches)) > 0,
        "the object leaf's slices compared through the kernel"
    );
}

/// Subjects in runs of ten rows, which the writer run-end encodes, and the
/// graph `gd` on every ninth row: a `G` match over the mapped file projects
/// its kept rows (more than one per 16 rows of the split) and filters the
/// subject column by its mask through the store session's run-end filter
/// ([`crate::kernels`]), and answers as the store built in memory does.
#[tokio::test]
async fn test_run_end_subjects_filter_through_the_kernel() {
    use crate::kernels::Kernel;

    let gd = iri_graph("gd");
    let quads: Vec<Quad> = (0..16_384usize)
        .map(|i| {
            let graph = if i % 9 == 0 {
                gd.clone()
            } else {
                GraphName::DefaultGraph
            };
            make_quad(
                &format!("http://example.org/s{:05}", i / 10),
                &format!("http://example.org/p{}", i % 4),
                &format!("o{}", i % 7),
                graph,
            )
        })
        .collect();
    let (_dir, path) = write_store_file(quads.clone(), LayoutStrategy::Dictionary, vec![]).await;
    let file = VortexRdfStore::from_file(&path).await.unwrap();
    let memory = built_store(quads, LayoutStrategy::Dictionary, vec![]).await;
    let rec = Recorder::start();
    for probe in [
        Probe::new(None, None, None, Some(gd.clone())),
        Probe::new(None, Some(iri("p0")), None, Some(gd)),
    ] {
        let label = format!("{probe:?}");
        let mut got = file
            .run_probe(&probe)
            .await
            .unwrap()
            .quads_vec()
            .await
            .unwrap();
        let mut want = memory
            .run_probe(&probe)
            .await
            .unwrap()
            .quads_vec()
            .await
            .unwrap();
        got.sort_by_key(ToString::to_string);
        want.sort_by_key(ToString::to_string);
        assert!(!want.is_empty(), "{label}: the pattern matches");
        assert_eq!(got, want, "{label}: the rows");
    }
    assert!(
        rec.count(&Event::Kernel(Kernel::RunEndFilter)) > 0,
        "the subject column filtered through the kernel"
    );
}
