//! File-backed stores: zone-map pruning, the bound-subject chunk probe,
//! open validation, and the per-layout matching matrix over files.

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
    let p_rare = NamedNode::new("http://example.org/pAAA").unwrap();
    let matched = store
        .match_pattern(None, Some(&p_rare), None, None)
        .await
        .unwrap();
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
    let s_mid = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s012000").unwrap());
    let range = store.debug_subject_pruning_envelope(&s_mid).await.unwrap();
    let range = range.expect("sorted multi-zone file must narrow a bound subject");
    assert!(range.start <= 12000 && 12000 < range.end);
    assert!(
        range.end - range.start < N as u64,
        "envelope must exclude other zones"
    );

    // Subject match first (row range), then indexed object match: the
    // object index ids are restricted to the subject's range.
    let by_subject = store
        .match_pattern(Some(&s_mid), None, None, None)
        .await
        .unwrap();
    let o0 = Term::Literal(Literal::new_simple_literal("o0"));
    let chained = by_subject
        .match_pattern(None, None, Some(&o0), None)
        .await
        .unwrap();
    assert_eq!(chained.size().await.unwrap(), 1); // 12000 % 5 == 0
    let results: Vec<Quad> = chained.quads().try_collect().await.unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].subject.to_string(),
        "<http://example.org/s012000>"
    );
    assert_eq!(results[0].object.to_string(), "\"o0\"");

    // Same chain with an object the subject's row doesn't carry.
    let o1 = Term::Literal(Literal::new_simple_literal("o1"));
    let empty = by_subject
        .match_pattern(None, None, Some(&o1), None)
        .await
        .unwrap();
    assert_eq!(empty.size().await.unwrap(), 0);

    // Index-to-index chaining: object ids ∩ predicate ids.
    let p2 = NamedNode::new("http://example.org/p2").unwrap();
    let step1 = store
        .match_pattern(None, None, Some(&o1), None)
        .await
        .unwrap();
    let step2 = step1
        .match_pattern(None, Some(&p2), None, None)
        .await
        .unwrap();
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
    let missing = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s99").unwrap());
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
    let p0 = NamedNode::new("http://example.org/p0").unwrap();
    let matched = deleted
        .match_pattern(None, Some(&p0), None, None)
        .await
        .unwrap();
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
    use crate::store::scan::file_reads::scan_hooks;
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

    let p = NamedNode::new("http://example.org/p1").unwrap();
    let o = Term::Literal(Literal::new_simple_literal("object 3"));
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
            let got = store.match_pattern(Some(&s), pp, po, pg).await.unwrap();
            let want = memory.match_pattern(Some(&s), pp, po, pg).await.unwrap();
            let scans = scan_hooks::scans();
            let got = view_strings(&got).await;
            assert_eq!(
                scan_hooks::scans(),
                scans,
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
    let absent = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/zz").unwrap());
    let empty = store
        .match_pattern(Some(&absent), None, None, None)
        .await
        .unwrap();
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
    let matched = store
        .match_pattern(Some(&s50), None, None, None)
        .await
        .unwrap();
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
    let matched = store
        .match_pattern(Some(&s50), None, None, None)
        .await
        .unwrap();
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

    let p = NamedNode::new("http://example.org/p2").unwrap();
    let s = subject_node(444, 4);
    let got_first = store
        .match_pattern(None, Some(&p), None, None)
        .await
        .unwrap();
    let got = got_first
        .match_pattern(Some(&s), None, None, None)
        .await
        .unwrap();
    assert_eq!(served_component(&got), None);
    let want = memory
        .match_pattern(None, Some(&p), None, None)
        .await
        .unwrap()
        .match_pattern(Some(&s), None, None, None)
        .await
        .unwrap();
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
    let matched = store
        .match_pattern(Some(&s), None, None, None)
        .await
        .unwrap();
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

/// Dictionary layout (over `dictionary_test_quads`): a pushed-down integer
/// filter on the code columns, and a term absent from the dictionary.
async fn probe_dictionary(store: VortexRdfStore) {
    let p0 = NamedNode::new("http://example.org/p0").unwrap();
    let filtered = store
        .match_pattern(None, Some(&p0), None, None)
        .await
        .unwrap();
    assert_eq!(filtered.size().await.unwrap(), 4);
    let results: Vec<Quad> = filtered.quads().try_collect().await.unwrap();
    assert!(
        results
            .iter()
            .all(|q| q.predicate == "http://example.org/p0")
    );

    let missing_p = NamedNode::new("http://example.org/nope").unwrap();
    let empty = store
        .match_pattern(None, Some(&missing_p), None, None)
        .await
        .unwrap();
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
    let rows = |c: [vortex_buffer::Buffer<TermCode>; 4]| -> Vec<[TermCode; 4]> {
        let mut rows: Vec<[TermCode; 4]> = (0..c[0].len())
            .map(|i| [c[0][i], c[1][i], c[2][i], c[3][i]])
            .collect();
        rows.sort_unstable();
        rows
    };
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
                let view = |base: &VortexRdfStore| {
                    let base = base.clone();
                    let (s, p, o) = (s.clone(), p.clone(), o.clone());
                    async move {
                        let v = base
                            .match_pattern(s.as_ref(), p.as_ref(), o.as_ref(), None)
                            .await
                            .unwrap();
                        match window {
                            Some((offset, limit)) => v.window(offset, limit).await.unwrap(),
                            None => v,
                        }
                    }
                };
                let got = view(store)
                    .await
                    .code_columns_gathered()
                    .await
                    .unwrap()
                    .unwrap();
                let want = view(memory)
                    .await
                    .code_columns_gathered()
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    rows(got),
                    rows(want),
                    "{tag}: {s:?} {p:?} {o:?} window {window:?}"
                );
            }
        }
    };
    for (tag, _dir, store) in file_stores().await {
        check(&tag, &store, &memory).await;
    }
    // Tombstones: the same deletions on a file store and in memory.
    let (_dir, path) = write_store_file(quads.clone(), LayoutStrategy::Dictionary, vec![]).await;
    let (mut file, mut mem) = (
        VortexRdfStore::from_file(&path).await.unwrap(),
        memory.clone(),
    );
    for quad in quads.iter().step_by(7).take(60) {
        file = file.delete_quad(quad).await.unwrap();
        mem = mem.delete_quad(quad).await.unwrap();
    }
    check("file/tombstoned", &file, &mem).await;
}

/// Quads whose sort-key prefixes repeat at every depth: 40 subjects, each
/// with three predicates, each predicate with two or three objects, each
/// triple in one to three graphs. So a bound `(s, p)` has several objects, a
/// `(p, o)` several subjects and a `(p, o, s)` several graphs: every key of a
/// located run's prefix narrows it.
fn multiplicity_quads() -> Vec<Quad> {
    let graph = |k: usize| match k {
        0 => GraphName::DefaultGraph,
        k => GraphName::NamedNode(NamedNode::new(format!("http://example.org/g{k}")).unwrap()),
    };
    let mut quads = Vec::new();
    for s in 0..40 {
        for p in 0..3 {
            for o in 0..(2 + (s + p) % 2) {
                for g in 0..=((s + o) % 3) {
                    quads.push(make_quad(
                        &format!("http://example.org/s{s:02}"),
                        &format!("http://example.org/p{}", (s + p) % 5),
                        &format!("o{}", (s * 3 + o) % 7),
                        graph(g),
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
    // The temp dirs stay alive until the stores are checked.
    let mut guards: Vec<Box<dyn std::any::Any>> = Vec::new();
    let mut stores: Vec<(String, VortexRdfStore, Vec<Quad>, usize)> = Vec::new();
    for (tag, dir, store) in file_stores().await {
        guards.push(Box::new(dir));
        stores.push((tag, store, fixture_quads(), 97));
    }
    for (name, indexes) in [
        ("no-index", vec![]),
        ("copy", vec![IndexType::SecondaryByCopy]),
        ("reference", vec![IndexType::SecondaryByReference]),
    ] {
        let (dir, path) =
            write_store_file(multiplicity_quads(), LayoutStrategy::Dictionary, indexes).await;
        let store = VortexRdfStore::from_file(&path).await.unwrap();
        guards.push(Box::new(dir));
        stores.push((
            format!("multiplicity/{name}/mapped"),
            store,
            multiplicity_quads(),
            3,
        ));
    }
    // Seven-row flat leaves: most subject runs cross a leaf boundary, so the
    // narrowing and the reads go across leaves as well as inside one.
    let (dir, path) = write_chunked_store_file(&multiplicity_quads(), 7).await;
    guards.push(Box::new(dir));
    stores.push((
        "multiplicity/seven-row-leaves/mapped".to_string(),
        VortexRdfStore::from_file(&path).await.unwrap(),
        multiplicity_quads(),
        1,
    ));
    // The reference index in seven-row leaves: a run of `index:ref-p` or
    // `index:ref-o` spans leaves, so the other run is walked leaf by leaf.
    let (dir, path) = write_chunked_reference_store_file(&multiplicity_quads(), 7).await;
    guards.push(Box::new(dir));
    stores.push((
        "multiplicity/reference/seven-row-leaves/mapped".to_string(),
        VortexRdfStore::from_file(&path).await.unwrap(),
        multiplicity_quads(),
        3,
    ));
    // A bound subject's rows are checked when its run is the shorter one, a
    // reference run's otherwise: the hub holds both, in one leaf and in
    // seven-row leaves, where a subject's run crosses them.
    let (dir, path) = write_store_file(
        hub_quads(),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByReference],
    )
    .await;
    guards.push(Box::new(dir));
    stores.push((
        "hub/reference/mapped".to_string(),
        VortexRdfStore::from_file(&path).await.unwrap(),
        hub_quads(),
        3,
    ));
    let (dir, path) = write_chunked_reference_store_file(&hub_quads(), 7).await;
    guards.push(Box::new(dir));
    stores.push((
        "hub/reference/seven-row-leaves/mapped".to_string(),
        VortexRdfStore::from_file(&path).await.unwrap(),
        hub_quads(),
        3,
    ));
    for (tag, store, quads, step) in stores {
        check_locate_codes(&tag, &store, &quads, step).await;
    }
}

async fn check_locate_codes(tag: &str, store: &VortexRdfStore, quads: &[Quad], step: usize) {
    use crate::{CodePattern, Probe};
    let sorted = |c: &[vortex_buffer::Buffer<TermCode>; 4]| -> Vec<[TermCode; 4]> {
        let mut rows: Vec<[TermCode; 4]> = (0..c[0].len())
            .map(|i| [c[0][i], c[1][i], c[2][i], c[3][i]])
            .collect();
        rows.sort_unstable();
        rows
    };
    let dict = store.dict_reader().unwrap();
    let all = store.code_columns_gathered().await.unwrap().unwrap();
    let mut answered = std::collections::BTreeSet::new();
    for row in (0..all[0].len()).step_by(step) {
        let quad = [all[0][row], all[1][row], all[2][row], all[3][row]];
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
            let want = view.code_columns_gathered().await.unwrap().unwrap();
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
            assert_eq!(sorted(&full.columns), sorted(&want), "{tag}: rows");
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
                    seen.extend((0..n).map(|i| {
                        [
                            got.columns[0][i],
                            got.columns[1][i],
                            got.columns[2][i],
                            got.columns[3][i],
                        ]
                    }));
                    offset += page;
                }
                seen.sort_unstable();
                assert_eq!(seen, sorted(&want), "{tag}: pages of {page}");
            }
        }
    }
    // Masks over (s, p, o, g) = bits 0..4. A mapped sorted file answers the
    // subject prefixes; the copy index also the posg and ospg ones (`s?o`
    // among them); the reference index every shape with `p` or `o` bound, the
    // other bound slots checked at its rows; the no-index file none of the
    // index-only shapes, and nothing answers `s??g`.
    if tag.ends_with("/mapped") {
        for mask in [0b0001, 0b0011, 0b0111, 0b1111] {
            assert!(
                answered.contains(&mask),
                "{tag}: mask {mask:04b} not answered"
            );
        }
    }
    if tag.contains("/copy/") && tag.ends_with("/mapped") {
        for mask in [0b0010, 0b0110, 0b0100, 0b0101] {
            assert!(
                answered.contains(&mask),
                "{tag}: mask {mask:04b} not answered"
            );
        }
    }
    if tag.contains("/reference/") && tag.ends_with("/mapped") {
        for mask in [0b0010, 0b0110, 0b0100, 0b0101, 0b1010, 0b1100, 0b1110] {
            assert!(
                answered.contains(&mask),
                "{tag}: mask {mask:04b} not answered"
            );
        }
    }
    if tag.contains("/no-index/") {
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
