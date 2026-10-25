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
    let by_subject = matched_view(&store, Some(&s_mid), None, None, None).await;
    let o0 = Term::Literal(Literal::new_simple_literal("o0"));
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
    let o1 = Term::Literal(Literal::new_simple_literal("o1"));
    let empty = matched_view(&by_subject, None, None, Some(&o1), None).await;
    assert_eq!(empty.size().await.unwrap(), 0);

    // Index-to-index chaining: object ids ∩ predicate ids.
    let p2 = NamedNode::new("http://example.org/p2").unwrap();
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
    use crate::store::scan::file_scans::scan_hooks;
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
            let got = matched_view(&store, Some(&s), pp, po, pg).await;
            let want = matched_view(&memory, Some(&s), pp, po, pg).await;
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

    let p = NamedNode::new("http://example.org/p2").unwrap();
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

/// Dictionary layout (over `dictionary_test_quads`): a pushed-down integer
/// filter on the code columns, and a term absent from the dictionary.
async fn probe_dictionary(store: VortexRdfStore) {
    let p0 = NamedNode::new("http://example.org/p0").unwrap();
    let filtered = matched_view(&store, None, Some(&p0), None, None).await;
    assert_eq!(filtered.size().await.unwrap(), 4);
    let results: Vec<Quad> = filtered.quads().try_collect().await.unwrap();
    assert!(
        results
            .iter()
            .all(|q| q.predicate == "http://example.org/p0")
    );

    let missing_p = NamedNode::new("http://example.org/nope").unwrap();
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
                let view = async |base: &VortexRdfStore| {
                    let v = matched_view(base, s.as_ref(), p.as_ref(), o.as_ref(), None).await;
                    match window {
                        Some((offset, limit)) => v.window(offset, limit).await.unwrap(),
                        None => v,
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

// ─── Filtered one-shot reads: the filter's masks first ───────────────────

/// A view's rows in read order, as the bindings' one-shot reads return them:
/// its code columns, or the quads of a view with a tail, whose rows have no
/// codes.
#[derive(Debug, PartialEq)]
enum ReadRows {
    Codes(Vec<[TermCode; 4]>),
    Quads(Vec<Quad>),
}

async fn read_rows(view: &VortexRdfStore) -> ReadRows {
    match view.code_columns_gathered().await.unwrap() {
        Some(c) => ReadRows::Codes(
            (0..c[0].len())
                .map(|i| [c[0][i], c[1][i], c[2][i], c[3][i]])
                .collect(),
        ),
        None => ReadRows::Quads(view.quads_vec().await.unwrap()),
    }
}

/// The masked reads chosen on this thread since `before`, by kind.
fn masked_reads_since(before: [usize; 3]) -> [usize; 3] {
    let now = crate::store::scan::read_plan::masked_read_hooks::reads();
    std::array::from_fn(|kind| now[kind] - before[kind])
}

/// One masked read of kind `read`, as [`masked_reads_since`] counts them.
fn one_masked_read(read: crate::store::scan::read_plan::MaskedRead) -> [usize; 3] {
    let mut reads = [0; 3];
    reads[read as usize] = 1;
    reads
}

/// Each of [`masks_probes`] on `file` against `memory`: a view with a pending
/// filter whose one-shot code read chooses the probe's masked read once and
/// answers the rows, in order, of the view's scan (`row_chunks`) and of
/// `memory`'s view.
async fn assert_masks_first_reads(file: &VortexRdfStore, memory: &VortexRdfStore, tag: &str) {
    use crate::store::scan::read_plan::masked_read_hooks;
    for (shape, probe, read) in masks_probes() {
        let label = format!("{tag} {shape}");
        let view = file.run_probe(&probe).await.unwrap();
        assert!(
            view.view_statistics().unwrap().pending_filter,
            "{label}: a pending filter"
        );
        let before = masked_read_hooks::reads();
        let rows = row_list(&view).await;
        assert_eq!(
            masked_reads_since(before),
            one_masked_read(read),
            "{label}: read as {read:?}"
        );
        assert!(!rows.is_empty(), "{label}: the pattern matches");
        assert_eq!(
            struct_chunk_codes(&collect_row_chunks(&view, 4_096).await),
            rows,
            "{label}: the scan's rows, in order"
        );
        assert_eq!(
            row_list(&memory.run_probe(&probe).await.unwrap()).await,
            rows,
            "{label}: the in-memory store's rows"
        );
    }
}

/// A filtered file view's one-shot reads against its scan and `memory`, an
/// in-memory view of the same rows: the code read chooses `read` once and
/// answers the scan's rows, in order, and `memory`'s; `quads_vec` answers the
/// stream's quads.
async fn assert_masked_reads_match(
    view: &VortexRdfStore,
    memory: &VortexRdfStore,
    read: crate::store::scan::read_plan::MaskedRead,
    label: &str,
) {
    let before = crate::store::scan::read_plan::masked_read_hooks::reads();
    let rows = row_list(view).await;
    assert_eq!(
        masked_reads_since(before),
        one_masked_read(read),
        "{label}: read as {read:?}"
    );
    assert!(!rows.is_empty(), "{label}: rows to read");
    assert_eq!(
        struct_chunk_codes(&collect_row_chunks(view, 4_096).await),
        rows,
        "{label}: the scan's rows, in order"
    );
    assert_eq!(
        row_list(memory).await,
        rows,
        "{label}: the in-memory store's rows"
    );
    let streamed: Vec<Quad> = view.quads().try_collect().await.unwrap();
    assert_eq!(
        view.quads_vec().await.unwrap(),
        streamed,
        "{label}: quads_vec"
    );
}

/// 40,000 quads `s{i:05} p{i % 3}` in the default graph whose objects `"cp"`,
/// `"ct"` and `"cd"` lie in rows 10,000 to 30,000 alone: on every 1,000th row
/// there, every other 20th and every other even one (point reads, a take, a
/// projection). Every other row's object is `"o{i % 7}"`.
fn clustered_quads() -> Vec<Quad> {
    (0..40_000usize)
        .map(|i| {
            let o = match i {
                i if !(10_000..30_000).contains(&i) => format!("o{}", i % 7),
                i if i % 1_000 == 0 => "cp".to_string(),
                i if i % 20 == 0 => "ct".to_string(),
                i if i % 2 == 0 => "cd".to_string(),
                i => format!("o{}", i % 7),
            };
            make_quad(
                &format!("http://example.org/s{i:05}"),
                &format!("http://example.org/p{}", i % 3),
                &o,
                GraphName::DefaultGraph,
            )
        })
        .collect()
}

/// The masks first over a selection that starts and ends inside a split: a
/// zone-map envelope inside a written file's splits (the clustered objects of
/// [`clustered_quads`]), and a located subject range across [`masks_file`]'s
/// sixteen splits. Every read kind answers the scan's rows and the in-memory
/// store's.
#[tokio::test]
async fn test_masks_first_reads_a_selection_clamped_inside_splits() {
    use crate::store::Keep;
    use crate::store::scan::read_plan::MaskedRead;

    let literal = |value: &str| Term::Literal(Literal::new_simple_literal(value));
    let (_envelope_dir, path) =
        write_store_file(clustered_quads(), LayoutStrategy::Dictionary, vec![]).await;
    let splits = open_mapped(&path).await.splits().unwrap();
    let inside = |row: u64| {
        splits
            .iter()
            .any(|split| split.start < row && row < split.end)
    };
    let file = VortexRdfStore::from_file(&path).await.unwrap();
    let memory = VortexRdfStore::from_file_in_memory(&path).await.unwrap();
    for (object, read) in [
        ("cp", MaskedRead::Points),
        ("ct", MaskedRead::Take),
        ("cd", MaskedRead::Project),
    ] {
        let label = format!("envelope {object}");
        let probe = Probe::new(None, None, Some(literal(object)), None);
        let view = file.run_probe(&probe).await.unwrap();
        assert!(
            view.view_statistics().unwrap().pending_filter,
            "{label}: a pending filter"
        );
        let range = view.debug_selection_range().expect("a zone-map envelope");
        assert!(
            inside(range.start) && inside(range.end),
            "{label}: {range:?} inside the splits {splits:?}"
        );
        let twin = memory.run_probe(&probe).await.unwrap();
        assert_masked_reads_match(&view, &twin, read, &label).await;
    }
    let (_masks_dir, path) = masks_file().await;
    let splits = open_mapped(&path).await.splits().unwrap();
    let file = VortexRdfStore::from_file(&path).await.unwrap();
    let memory = VortexRdfStore::from_file_in_memory(&path).await.unwrap();
    let dict = file.dict_reader().unwrap();
    let lo = dict
        .encode("<http://example.org/s01500>")
        .await
        .unwrap()
        .unwrap();
    let hi = dict
        .encode("<http://example.org/s13500>")
        .await
        .unwrap()
        .unwrap();
    let subjects = Keep::range(lo..hi);
    let inside = |row: u64| {
        splits
            .iter()
            .any(|split| split.start < row && row < split.end)
    };
    assert!(inside(1_500) && inside(13_500), "{splits:?}");
    for (shape, probe, read) in masks_probes() {
        let label = format!("subject range {shape}");
        let view = file
            .run_probe(&probe)
            .await
            .unwrap()
            .keep(QuadColumn::S, &subjects)
            .await
            .unwrap();
        assert!(
            view.view_statistics().unwrap().pending_filter,
            "{label}: the filter still pending"
        );
        assert_eq!(
            view.debug_selection_range(),
            Some(1_500..13_500),
            "{label}: the located range"
        );
        let twin = memory
            .run_probe(&probe)
            .await
            .unwrap()
            .keep(QuadColumn::S, &subjects)
            .await
            .unwrap();
        assert_masked_reads_match(&view, &twin, read, &label).await;
    }
}

/// A filtered no-index file view's one-shot code read evaluates the filter's
/// masks first and reads the rows they keep as `MaskedRead` chooses: point by
/// point, by one take, or each split projected at its mask. Over sixteen
/// splits, one of them empty among the matched, the rows and their order are
/// the scan's and the in-memory store's.
#[tokio::test]
async fn test_masks_first_reads_each_kind() {
    let (_dir, path) = masks_file().await;
    assert_eq!(
        open_mapped(&path).await.splits().unwrap().len(),
        MASKS_SPLITS
    );
    let file = VortexRdfStore::from_file(&path).await.unwrap();
    let memory = VortexRdfStore::from_file_in_memory(&path).await.unwrap();
    assert_masks_first_reads(&file, &memory, "file").await;
}

/// The masks first skip tombstoned rows: with some of every pattern's rows
/// deleted from the file store and the in-memory store alike, each pattern
/// still reads as its kind and answers the in-memory store's live rows.
#[tokio::test]
async fn test_masks_first_skips_tombstones() {
    let (_dir, path) = masks_file().await;
    let file = masks_deleted(&VortexRdfStore::from_file(&path).await.unwrap()).await;
    let memory = masks_deleted(&VortexRdfStore::from_file_in_memory(&path).await.unwrap()).await;
    assert!(file.view_statistics().unwrap().tombstones > 0);
    assert_masks_first_reads(&file, &memory, "tombstoned").await;
}

/// A filtered no-index file view's `quads_vec` and `shared_quads_vec` read
/// the filter's masks first: the quads, in order, are the `quads()` stream's,
/// the tail's after the base's, for each read kind, under a file-backed and
/// a resident dictionary, with the masks evaluated inline and, the split
/// limit forced to zero, spawned. Rows read point by point are read inline;
/// a take or a projection reads each split with a match in a task of its
/// own, spawned since there is more than one, and polled inline, one after
/// another, where no tokio runtime is current.
#[tokio::test]
async fn test_quads_vec_over_a_filtered_file_matches_quads() {
    use crate::store::scan::{
        file_scans::driver_hooks,
        read_plan::{MaskedRead, masked_read_hooks},
    };

    let (_file_backed_dir, file_backed) = masks_file().await;
    let (_resident_dir, resident) = masks_file_resident_dictionary().await;
    for (dictionary, path, file_backed) in [
        ("file-backed", &file_backed, true),
        ("resident", &resident, false),
    ] {
        let store = VortexRdfStore::from_file(path).await.unwrap();
        assert_eq!(
            store.dict_reader().unwrap().is_file_backed(),
            file_backed,
            "{dictionary}"
        );
        let tailed = store.add_quads(masks_tail()).await.unwrap();
        for forced in [None, Some(0)] {
            let _limit = forced.map(driver_hooks::ForcedLimit::set);
            for (shape, probe, read) in masks_probes() {
                let label = format!("{dictionary} {shape} limit {forced:?}");
                let view = tailed.run_probe(&probe).await.unwrap();
                let streamed: Vec<Quad> = view.quads().try_collect().await.unwrap();
                let tail_rows = if matches!(shape, "P" | "G") { 12 } else { 6 };
                assert!(
                    streamed[streamed.len() - tail_rows..].iter().all(|q| q
                        .subject
                        .to_string()
                        .starts_with("<http://example.org/tail")),
                    "{label}: the tail's rows last"
                );
                let before = masked_read_hooks::reads();
                driver_hooks::take_spawned();
                let quads = view.quads_vec().await.unwrap();
                let spawned = driver_hooks::take_spawned();
                assert_eq!(
                    masked_reads_since(before),
                    one_masked_read(read),
                    "{label}: read as {read:?}"
                );
                assert_eq!(quads, streamed, "{label}: the stream's quads, in order");
                // The masks spawn every split under the forced limit; a take
                // or a projection spawns each split with a match.
                let masks = if forced.is_some() { MASKS_SPLITS } else { 0 };
                let reads = match read {
                    MaskedRead::Points => 0,
                    MaskedRead::Take | MaskedRead::Project => MASKS_SPLITS - 1,
                };
                assert_eq!(spawned, masks + reads, "{label}: spawned splits");
                let shared = view.shared_quads_vec().await.unwrap();
                assert_eq!(
                    shared_tuple_rows(&shared),
                    tuple_rows(&streamed),
                    "{label}: shared_quads_vec"
                );
            }
        }
        // A bare executor on a plain thread: no runtime to spawn onto.
        for (shape, probe, read) in masks_probes() {
            if read == MaskedRead::Points {
                continue;
            }
            let label = format!("{dictionary} {shape} outside a runtime");
            let view = tailed.run_probe(&probe).await.unwrap();
            let streamed: Vec<Quad> = view.quads().try_collect().await.unwrap();
            let (quads, spawned) = std::thread::spawn(move || {
                futures::executor::block_on(async {
                    driver_hooks::take_spawned();
                    let quads = view.quads_vec().await.unwrap();
                    (quads, driver_hooks::take_spawned())
                })
            })
            .join()
            .expect("quads_vec must not panic without a tokio runtime");
            assert_eq!(spawned, 0, "{label}: nothing spawned");
            assert_eq!(quads, streamed, "{label}: the stream's quads, in order");
        }
    }
}

/// On no-index file stores, opened mapped and loaded whole, as written, with
/// tombstones and with a tail, each of `O`, `PO`, `P` and `G` reads the same
/// rows in base order plain and windowed to a limit that cannot bind: limits
/// between the match's rows and their bound (the window route) and at or
/// past the bound (the plain view loaded; mapped, whose reads take these
/// filters by their masks first, the window route too). They are the rows
/// the loaded open, an in-memory store, reads plain.
#[tokio::test]
async fn test_plain_and_unbound_limit_reads_return_the_same_rows() {
    let (_dir, path) = masks_file().await;
    let mapped = VortexRdfStore::from_file(&path).await.unwrap();
    let loaded = VortexRdfStore::from_file_in_memory(&path).await.unwrap();
    let variants = [
        ("as written", mapped.clone(), loaded.clone()),
        (
            "tombstoned",
            masks_deleted(&mapped).await,
            masks_deleted(&loaded).await,
        ),
        (
            "tailed",
            mapped.add_quads(masks_tail()).await.unwrap(),
            loaded.add_quads(masks_tail()).await.unwrap(),
        ),
    ];
    for (variant, mapped, loaded) in &variants {
        for (shape, probe, _) in masks_probes() {
            let want = read_rows(&loaded.run_probe(&probe).await.unwrap()).await;
            for (open, store) in [("mapped", mapped), ("loaded", loaded)] {
                let label = format!("{variant} {open} {shape}");
                let plain = store.run_probe(&probe).await.unwrap();
                let n = plain.size().await.unwrap();
                let bound = plain.view_statistics().unwrap().rows.upper_bound;
                assert_eq!(read_rows(&plain).await, want, "{label}: plain");
                let mut limits = vec![bound, bound + 1, usize::MAX];
                if open == "mapped" {
                    assert!(n + 1 < bound, "{label}: {n} rows under a bound of {bound}");
                    limits.extend([n + 1, n + (bound - n) / 2, bound - 1]);
                }
                for limit in limits {
                    let windowed = store
                        .run_probe(&probe.clone().window(0, Some(limit)))
                        .await
                        .unwrap();
                    assert_eq!(read_rows(&windowed).await, want, "{label} limit {limit}");
                }
            }
        }
    }
}

/// A window that cuts a filtered file view, a limit below its rows, is the
/// first `limit` matches in base order, read as exact ids with the filter
/// evaluated.
#[tokio::test]
async fn test_window_that_cuts_a_filter_keeps_base_order() {
    let (_dir, path) = masks_file().await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    for (shape, probe, _) in masks_probes() {
        let rows = row_list(&store.run_probe(&probe).await.unwrap()).await;
        let n = rows.len();
        for limit in [1, n / 2, n - 1] {
            let label = format!("{shape}: limit {limit} of {n} rows");
            let windowed = store
                .run_probe(&probe.clone().window(0, Some(limit)))
                .await
                .unwrap();
            let stats = windowed.view_statistics().unwrap();
            assert!(!stats.pending_filter, "{label}: the filter evaluated");
            assert_eq!(stats.selection, SelectionKind::Ids, "{label}: exact ids");
            assert_eq!(stats.rows, RowCountHint::exact(limit), "{label}");
            assert_eq!(
                row_list(&windowed).await,
                rows[..limit],
                "{label}: the first matches"
            );
        }
    }
}

/// A window that cuts a filtered file view evaluates its splits in file order
/// and stops at the first that fills it. On a file of more splits than the
/// fold's lookahead it consumes exactly the splits up to that one, and starts
/// at most a lookahead's worth of splits past them: inline (the split limit
/// forced to the file's splits), evaluated as the fold polls them; spawned
/// (the limit forced to zero), counted as they are spawned, since a spawned
/// split may or may not run before its abort. The window's rows are the
/// first matches.
#[tokio::test]
async fn test_cutting_window_over_a_filter_stops_early() {
    use crate::io::read::available_parallelism;
    use crate::store::scan::file_filter::split_hooks;
    use crate::store::scan::file_scans::driver_hooks;

    let (_dir, path) = many_splits_file().await;
    assert_eq!(
        open_mapped(&path).await.splits().unwrap().len(),
        MANY_SPLITS
    );
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let p0 = NamedNode::new("http://example.org/p0").unwrap();
    let view = matched_view(&store, None, Some(&p0), None, None).await;
    let rows = row_list(&view).await;
    let limit = 64;
    // `p0` is on every third row: the splits whose matches reach the limit.
    let filled = (1..=MANY_SPLITS)
        .find(|&splits| (0..splits * MANY_SPLIT_ROWS).filter(|i| i % 3 == 0).count() >= limit)
        .unwrap();
    let reach = (filled + available_parallelism().max(2) - 1).min(MANY_SPLITS);
    for (forced, inline) in [(MANY_SPLITS, true), (0, false)] {
        let _limit = driver_hooks::ForcedLimit::set(forced);
        let (evaluated, consumed) = (split_hooks::evaluated(), split_hooks::consumed());
        driver_hooks::take_spawned();
        let windowed = view.window(0, limit).await.unwrap();
        let spawned = driver_hooks::take_spawned();
        let evaluated = split_hooks::evaluated() - evaluated;
        assert_eq!(
            split_hooks::consumed() - consumed,
            filled,
            "limit {forced}: the splits up to the one that fills the window"
        );
        if inline {
            assert_eq!(spawned, 0, "inline: nothing spawned");
            assert!(
                (filled..=reach).contains(&evaluated),
                "inline: {evaluated} splits evaluated, {filled} to {reach} expected"
            );
        } else {
            assert_eq!(spawned, reach, "spawned: a lookahead past the consumed");
        }
        assert!(filled < MANY_SPLITS, "the window stops early");
        assert_eq!(
            row_list(&windowed).await,
            rows[..limit],
            "limit {forced}: the first matches"
        );
    }
}

/// The masks of a one-shot read, and a capped count, evaluate their splits
/// under the row-scan driver: with the split limit forced below the file's
/// sixteen splits every split is spawned, at sixteen none. A projection's own
/// split reads follow the same rule over the fifteen splits with a match:
/// inline at fifteen, every one spawned at zero. The rows and counts agree
/// under every limit.
#[tokio::test]
async fn test_masks_driver_follows_the_row_scan_rule() {
    use crate::store::scan::{file_scans::driver_hooks, read_plan::MaskedRead};

    let (_dir, path) = masks_file().await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    for (shape, probe, read) in masks_probes() {
        let view = store.run_probe(&probe).await.unwrap();
        let mut answers = Vec::new();
        for limit in [MASKS_SPLITS - 1, MASKS_SPLITS, 0] {
            let label = format!("{shape} limit {limit}");
            let masks = if limit < MASKS_SPLITS {
                MASKS_SPLITS
            } else {
                0
            };
            let projection = if read == MaskedRead::Project && limit < MASKS_SPLITS - 1 {
                MASKS_SPLITS - 1
            } else {
                0
            };
            let _limit = driver_hooks::ForcedLimit::set(limit);
            driver_hooks::take_spawned();
            let rows = row_list(&view).await;
            assert_eq!(
                driver_hooks::take_spawned(),
                masks + projection,
                "{label}: the read's masks and split reads"
            );
            let count = view.size_capped(usize::MAX).await.unwrap();
            assert_eq!(
                driver_hooks::take_spawned(),
                masks,
                "{label}: the capped count's masks"
            );
            answers.push((rows, count));
        }
        for answer in &answers[1..] {
            assert_eq!(*answer, answers[0], "{shape}: every driver");
        }
        assert_eq!(answers[0].0.len(), answers[0].1, "{shape}: the count");
    }
}

/// Dropping a capped count whose masks are spawned (the split limit forced to
/// zero) aborts the split evaluations in flight: its first poll spawns a
/// lookahead of splits, which on this single-threaded runtime have not run
/// yet, and once the count is dropped they never run.
#[tokio::test]
async fn test_dropping_a_spawned_capped_count_aborts_its_splits() {
    use crate::io::read::available_parallelism;
    use crate::store::scan::file_filter::split_hooks;
    use crate::store::scan::file_scans::driver_hooks;

    let (_dir, path) = many_splits_file().await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let p0 = NamedNode::new("http://example.org/p0").unwrap();
    let view = matched_view(&store, None, Some(&p0), None, None).await;
    let _limit = driver_hooks::ForcedLimit::set(0);
    driver_hooks::take_spawned();
    let evaluated = split_hooks::evaluated();
    {
        let mut count = std::pin::pin!(view.size_capped(usize::MAX));
        assert!(
            futures::poll!(count.as_mut()).is_pending(),
            "the spawned splits have not run"
        );
        assert_eq!(
            driver_hooks::take_spawned(),
            available_parallelism().clamp(2, MANY_SPLITS),
            "a lookahead of splits spawned"
        );
    }
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
    assert_eq!(
        split_hooks::evaluated(),
        evaluated,
        "the dropped count's splits never ran"
    );
}

// ─── A window that cuts no row keeps the filter's evaluation ─────────────

/// A window over a filtered file view that finds fewer matches than its limit
/// cuts no base row: it reads the plain rows and knows its size and
/// statistics exactly, no filter left pending, without evaluating a split
/// again. Under tombstones its counts are the live rows'; under a tail longer
/// than the rest of the limit it reads the base's rows, then the tail's first
/// rows up to the limit. Under a string layout, whose filter compares strings
/// that no read by id checks, the window keeps the matches' ids, not masks.
#[tokio::test]
async fn test_window_that_finds_it_cuts_nothing_keeps_the_view() {
    use crate::store::scan::file_filter::split_hooks;

    let (_dir, path) = masks_file().await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let tombstoned = masks_deleted(&store).await;
    for (variant, store) in [("as written", &store), ("tombstoned", &tombstoned)] {
        for (shape, probe, _) in masks_probes() {
            let label = format!("{variant} {shape}");
            let plain = store.run_probe(&probe).await.unwrap();
            let rows = row_list(&plain).await;
            let n = rows.len();
            let bound = plain.view_statistics().unwrap().rows.upper_bound;
            assert!(n + 1 < bound, "{label}: {n} rows under a bound of {bound}");
            let windowed = store
                .run_probe(&probe.clone().window(0, Some(n + 1)))
                .await
                .unwrap();
            let evaluated = split_hooks::evaluated();
            let stats = windowed.view_statistics().unwrap();
            assert!(!stats.pending_filter, "{label}: no filter pending");
            assert_eq!(stats.rows, RowCountHint::exact(n), "{label}: exact rows");
            assert_eq!(windowed.size().await.unwrap(), n, "{label}: size");
            assert_eq!(
                windowed.size_capped(n + 1).await.unwrap(),
                n,
                "{label}: capped"
            );
            assert_eq!(row_list(&windowed).await, rows, "{label}: the plain rows");
            assert_eq!(
                split_hooks::evaluated(),
                evaluated,
                "{label}: no split evaluated again"
            );
        }
    }
    let tailed = store.add_quads(masks_tail()).await.unwrap();
    for (shape, probe, _) in masks_probes() {
        let label = format!("tailed {shape}");
        let quads = tailed
            .run_probe(&probe)
            .await
            .unwrap()
            .quads_vec()
            .await
            .unwrap();
        let base = quads
            .iter()
            .take_while(|q| {
                !q.subject
                    .to_string()
                    .starts_with("<http://example.org/tail")
            })
            .count();
        assert!(quads.len() - base > 2, "{label}: a tail past the limit");
        let limit = base + 2;
        let windowed = tailed
            .run_probe(&probe.clone().window(0, Some(limit)))
            .await
            .unwrap();
        let stats = windowed.view_statistics().unwrap();
        assert!(!stats.pending_filter, "{label}: no filter pending");
        assert_eq!(
            stats.rows,
            RowCountHint::exact(limit),
            "{label}: exact rows"
        );
        assert_eq!(windowed.size().await.unwrap(), limit, "{label}: size");
        assert_eq!(
            windowed.quads_vec().await.unwrap(),
            quads[..limit],
            "{label}: the base's rows, then the tail's first"
        );
    }
    // The fixture's graph `g1`, on every third row: dense enough for masks.
    let g1 = GraphName::NamedNode(NamedNode::new("http://example.org/g1").unwrap());
    let probe = Probe::new(None, None, None, Some(g1));
    for layout in [LayoutStrategy::Default, LayoutStrategy::TypedObject] {
        let label = format!("{layout:?} G");
        let (_dir, path) = write_store_file(fixture_quads(), layout, vec![]).await;
        let store = VortexRdfStore::from_file(&path).await.unwrap();
        let plain = store.run_probe(&probe).await.unwrap();
        let quads = plain.quads_vec().await.unwrap();
        let n = quads.len();
        let stats = plain.view_statistics().unwrap();
        assert!(stats.pending_filter, "{label}: a pending filter");
        let bound = stats.rows.upper_bound;
        assert!(n + 1 < bound, "{label}: {n} rows under a bound of {bound}");
        let windowed = store
            .run_probe(&probe.clone().window(0, Some(n + 1)))
            .await
            .unwrap();
        assert_eq!(windowed.debug_evaluated_len(), None, "{label}: no masks");
        let stats = windowed.view_statistics().unwrap();
        assert!(!stats.pending_filter, "{label}: the filter evaluated");
        assert_eq!(stats.rows, RowCountHint::exact(n), "{label}: exact rows");
        assert_eq!(
            windowed.quads_vec().await.unwrap(),
            quads,
            "{label}: the plain rows"
        );
    }
}

/// On a Dictionary file one of whose primary columns has no chunk probes (the
/// object column written dictionary-encoded with its values under a chunked
/// node, a shape the probes decline), the reads by id cannot check a filter
/// of code equalities, so a filtered view's reads scan, and a window that
/// finds fewer matches than its limit keeps the matches' ids, not the
/// filter's masks: the rows an in-memory open reads, known exactly, no filter
/// pending.
#[tokio::test]
async fn test_window_over_a_column_without_chunk_probes_keeps_ids() {
    let (_dir, path) =
        write_chunked_store(&masks_quads(), MASKS_SPLIT_ROWS, false, false, Some("o")).await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let twin = VortexRdfStore::from_file_in_memory(&path).await.unwrap();
    assert_eq!(store.debug_quad_column_chunks("o"), None, "o: no probes");
    assert!(store.debug_quad_column_chunks("p").is_some(), "p: probes");
    let dense = masks_probes()
        .into_iter()
        .filter(|(shape, ..)| matches!(*shape, "P" | "G"));
    for (shape, probe, _) in dense {
        let plain = store.run_probe(&probe).await.unwrap();
        assert!(
            plain.view_statistics().unwrap().pending_filter,
            "{shape}: a pending filter"
        );
        assert!(!plain.debug_reads_masks_first(), "{shape}: no masks first");
        let rows = row_list(&twin.run_probe(&probe).await.unwrap()).await;
        assert_eq!(row_list(&plain).await, rows, "{shape}: the twin's rows");
        let n = rows.len();
        let windowed = store
            .run_probe(&probe.clone().window(0, Some(n + 1)))
            .await
            .unwrap();
        assert_eq!(windowed.debug_evaluated_len(), None, "{shape}: no masks");
        let stats = windowed.view_statistics().unwrap();
        assert!(!stats.pending_filter, "{shape}: the filter evaluated");
        assert_eq!(stats.selection, SelectionKind::Ids, "{shape}: the ids");
        assert_eq!(stats.rows, RowCountHint::exact(n), "{shape}: exact rows");
        assert_eq!(row_list(&windowed).await, rows, "{shape}: the plain rows");
    }
}

/// A window that cuts no row of a filtered file view keeps the filter's masks
/// for its reads where they are smaller than the matches' ids (a bit a row of
/// each split a mask keeps some rows of, against 64 a match), and the ids
/// otherwise: the dense predicate and graph keep their masks, while the sparse
/// object and predicate-object pair, spread over the splits, keep their ids
/// and drop the filter. Either way the view knows its rows exactly and reads
/// the plain read's rows in base order, with and without a tail.
#[tokio::test]
async fn test_window_keeps_masks_only_when_smaller_than_ids() {
    let (_dir, path) = masks_file().await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let tailed = store.add_quads(masks_tail()).await.unwrap();
    for (variant, store) in [("plain", &store), ("tailed", &tailed)] {
        for (shape, probe, _) in masks_probes() {
            let label = format!("{variant} {shape}");
            let plain = store.run_probe(&probe).await.unwrap();
            let want = read_rows(&plain).await;
            let n = plain.size().await.unwrap();
            let tail = plain.view_statistics().unwrap().tail_rows;
            let windowed = store
                .run_probe(&probe.clone().window(0, Some(n + 1)))
                .await
                .unwrap();
            let dense = matches!(shape, "P" | "G");
            assert_eq!(
                windowed.debug_evaluated_len(),
                dense.then_some(n - tail),
                "{label}: masks kept only where smaller than the ids"
            );
            let stats = windowed.view_statistics().unwrap();
            assert!(!stats.pending_filter, "{label}: no filter pending");
            assert_eq!(stats.rows, RowCountHint::exact(n), "{label}: exact rows");
            assert_eq!(read_rows(&windowed).await, want, "{label}: the plain rows");
        }
    }
}

/// The masks a window keeps belong to its view alone: every view derived from
/// it (a located subject keep, a streamed one, an object keep, a chained
/// match, windows that cut or skip, pieces, an owned copy) starts without
/// them and answers what the same derivation of an in-memory twin answers.
/// An owner's later delete leaves the view's own rows as they were, and the
/// probe run again sees it, as the twin's owner does.
#[tokio::test]
async fn test_kept_masks_never_outlive_a_derivation() {
    use crate::store::Keep;
    use std::num::NonZeroUsize;

    let (_dir, path) = masks_file().await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let memory = VortexRdfStore::from_file_in_memory(&path).await.unwrap();
    let dict = store.dict_reader().unwrap();
    let o0 = dict.encode("\"o0\"").await.unwrap().unwrap();
    let o5 = dict.encode("\"o5\"").await.unwrap().unwrap();
    let (s_lo, s_hi) = dict.prefix_range("<http://example.org/s").await.unwrap();
    let o0_term = Term::Literal(Literal::new_simple_literal("o0"));
    let dense = masks_probes()
        .into_iter()
        .filter(|(shape, ..)| matches!(*shape, "P" | "G"));
    for (shape, probe, _) in dense {
        let n = store.run_probe(&probe).await.unwrap().size().await.unwrap();
        let windowed = probe.clone().window(0, Some(n + 1));
        let view = store.run_probe(&windowed).await.unwrap();
        let twin = memory.run_probe(&windowed).await.unwrap();
        assert_eq!(view.debug_evaluated_len(), Some(n), "{shape}: kept masks");
        let rows = row_list(&twin).await;
        assert_eq!(row_list(&view).await, rows, "{shape}: the twin's rows");
        // A subject set of one code per sixteen of the view's rows is
        // located: the rule counts a set's codes against the selection's
        // rows, here every row of the file. One with the codes of two in
        // every three of the store's subjects, odd and even rows' alike, more
        // than one per sixteen of those rows, is streamed.
        let located = Keep::set(rows.iter().step_by(16).map(|row| row[0]));
        let streamed = Keep::set((s_lo..s_hi).filter(|code| (code - s_lo) % 3 != 0));
        let objects = Keep::set([o0, o5]);
        let streams = || view.debug_column_streams().unwrap();
        let before = streams();
        let located_view = view.keep(QuadColumn::S, &located).await.unwrap();
        assert_eq!(streams(), before, "{shape}: the sparse subject set located");
        let streamed_view = view.keep(QuadColumn::S, &streamed).await.unwrap();
        assert!(
            streams() > before,
            "{shape}: the dense subject set streamed"
        );
        let derived = [
            (
                "located subject keep",
                located_view,
                twin.keep(QuadColumn::S, &located).await.unwrap(),
            ),
            (
                "streamed subject keep",
                streamed_view,
                twin.keep(QuadColumn::S, &streamed).await.unwrap(),
            ),
            (
                "object keep",
                view.keep(QuadColumn::O, &objects).await.unwrap(),
                twin.keep(QuadColumn::O, &objects).await.unwrap(),
            ),
            (
                "chained match",
                matched_view(&view, None, None, Some(&o0_term), None).await,
                matched_view(&twin, None, None, Some(&o0_term), None).await,
            ),
            (
                "cutting window",
                view.window(0, n / 2).await.unwrap(),
                twin.window(0, n / 2).await.unwrap(),
            ),
            (
                "skipping window",
                view.window(1, usize::MAX).await.unwrap(),
                twin.window(1, usize::MAX).await.unwrap(),
            ),
        ];
        for (tag, derived, twin_derived) in &derived {
            let label = format!("{shape} {tag}");
            let want = row_list(twin_derived).await;
            assert!(!want.is_empty(), "{label}: rows to check");
            assert_eq!(derived.debug_evaluated_len(), None, "{label}: no masks");
            assert_eq!(row_list(derived).await, want, "{label}: the twin's rows");
            assert_eq!(derived.size().await.unwrap(), want.len(), "{label}: size");
        }
        let pieces = view
            .partitions(NonZeroUsize::new(3).unwrap())
            .await
            .unwrap();
        assert!(pieces.len() > 1, "{shape}: pieces");
        let mut pieced = Vec::new();
        for piece in &pieces {
            assert_eq!(piece.debug_evaluated_len(), None, "{shape}: a piece");
            pieced.extend(row_list(piece).await);
        }
        assert_eq!(pieced, rows, "{shape}: the pieces' rows");
        let quads = twin.quads_vec().await.unwrap();
        let owned = view.owned().await.unwrap();
        assert_eq!(owned.debug_evaluated_len(), None, "{shape}: the owned copy");
        assert_eq!(
            owned.quads_vec().await.unwrap(),
            quads,
            "{shape}: owned quads"
        );
        // The store, the view's owner, deletes three of the view's rows, and
        // so does the twin's.
        let (mut owner, mut twin_owner) = (store.clone(), memory.clone());
        for quad in &quads[..3] {
            owner = owner.delete_quad(quad).await.unwrap();
            twin_owner = twin_owner.delete_quad(quad).await.unwrap();
        }
        assert_eq!(row_list(&view).await, rows, "{shape}: the view's own rows");
        let again = owner.run_probe(&windowed).await.unwrap();
        let twin_again = twin_owner.run_probe(&windowed).await.unwrap();
        let want = twin_again.quads_vec().await.unwrap();
        assert_eq!(want.len(), quads.len() - 3, "{shape}: three rows deleted");
        assert_eq!(
            again.quads_vec().await.unwrap(),
            want,
            "{shape}: the probe after the delete"
        );
    }
}
