//! The pushdown primitives a query layer builds on: batch matching, row
//! windows, keep constraints and dictionary predicates — each checked
//! against the plain match it must agree with.

use super::*;
use crate::common::terms::Pattern;
use crate::store::Keep;
use crate::store::QuadColumn;

fn subject(i: usize) -> NamedOrBlankNode {
    NamedOrBlankNode::NamedNode(NamedNode::new(format!("http://example.org/s{i:02}")).unwrap())
}

fn predicate(i: usize) -> NamedNode {
    NamedNode::new(format!("http://example.org/p{i}")).unwrap()
}

fn object(i: usize) -> Term {
    Term::Literal(Literal::new_simple_literal(format!("object {i}")))
}

/// A spread of patterns over the modular dataset: free, one bound position
/// of each kind, two bound, and one that matches nothing.
fn patterns() -> Vec<Pattern> {
    vec![
        (None, None, None, None),
        (None, Some(predicate(1)), None, None),
        (Some(subject(7)), None, None, None),
        (None, None, Some(object(2)), None),
        (Some(subject(7)), Some(predicate(3)), None, None),
        (None, Some(predicate(2)), Some(object(2)), None),
        (Some(subject(99)), None, None, None),
    ]
}

async fn assert_many_equals_singles(store: &VortexRdfStore, tag: &str) {
    let patterns = patterns();
    let many = store.match_pattern_many(&patterns).await.unwrap();
    assert_eq!(many.len(), patterns.len(), "{tag}");
    for (view, (s, p, o, g)) in many.iter().zip(&patterns) {
        let single = store
            .match_pattern(s.as_ref(), p.as_ref(), o.as_ref(), g.as_ref())
            .await
            .unwrap();
        assert_eq!(
            view.quads_vec().await.unwrap(),
            single.quads_vec().await.unwrap(),
            "{tag}: {p:?} {o:?}"
        );
        assert_eq!(
            view.size().await.unwrap(),
            single.size().await.unwrap(),
            "{tag}"
        );
    }
}

/// A batch of matches is the matches, in order — on every layout, in memory
/// and off a file (where the scans overlap).
#[tokio::test]
async fn match_pattern_many_equals_the_singles() {
    for layout in [LayoutStrategy::Default, LayoutStrategy::Dictionary] {
        let store =
            VortexRdfStore::from_quads(quad_stream(modular_quads(40, 4, 5)), layout, vec![])
                .await
                .unwrap();
        assert_many_equals_singles(&store, &format!("{layout} in memory")).await;
        let tailed = store
            .add_quads([make_quad(
                "http://example.org/s07",
                "http://example.org/p1",
                "appended",
                GraphName::DefaultGraph,
            )])
            .await
            .unwrap();
        assert_many_equals_singles(&tailed, &format!("{layout} tailed")).await;
    }
    assert!(
        VortexRdfStore::from_quads(quad_stream(vec![]), LayoutStrategy::Dictionary, vec![])
            .await
            .unwrap()
            .match_pattern_many(&[])
            .await
            .unwrap()
            .is_empty()
    );
}

/// Every window of `view` is the matching slice of its rows, and the capped
/// size the slice's length.
async fn assert_windows(view: &VortexRdfStore, tag: &str) {
    let all = view.quads_vec().await.unwrap();
    let n = all.len();
    for (offset, limit) in [
        (0, 5),
        (3, 10),
        (n.saturating_sub(2), 5),
        (n, 3),
        (0, 0),
        (0, n + 10),
    ] {
        let window = view.window(offset, limit).await.unwrap();
        let end = (offset + limit).min(n);
        let expected: &[Quad] = if offset >= n { &[] } else { &all[offset..end] };
        assert_eq!(
            window.quads_vec().await.unwrap(),
            expected,
            "{tag}: window({offset}, {limit})"
        );
        assert_eq!(
            window.size().await.unwrap(),
            expected.len(),
            "{tag}: size of window({offset}, {limit})"
        );
        assert_eq!(
            view.size_capped(limit).await.unwrap(),
            limit.min(n),
            "{tag}: size_capped({limit})"
        );
    }
}

/// Windows over a store, a match, and a tailed view with tombstones in the
/// base and the tail — on both string-capable layouts.
#[tokio::test]
async fn windows_are_row_slices() {
    for layout in [LayoutStrategy::Default, LayoutStrategy::Dictionary] {
        let quads = modular_quads(40, 4, 5);
        let store = VortexRdfStore::from_quads(quad_stream(quads.clone()), layout, vec![])
            .await
            .unwrap();
        assert_windows(&store, &format!("{layout} store")).await;
        let p1 = predicate(1);
        let matched = store
            .match_pattern(None, Some(&p1), None, None)
            .await
            .unwrap();
        assert_windows(&matched, &format!("{layout} match")).await;

        let appended: Vec<Quad> = (0..6)
            .map(|i| {
                make_quad(
                    &format!("http://example.org/tail{i}"),
                    "http://example.org/p1",
                    "appended",
                    GraphName::DefaultGraph,
                )
            })
            .collect();
        let tailed = store
            .add_quads(appended.clone())
            .await
            .unwrap()
            .delete_quad(&quads[5])
            .await
            .unwrap()
            .delete_quad(&quads[6])
            .await
            .unwrap()
            .delete_quad(&appended[1])
            .await
            .unwrap();
        assert_eq!(tailed.size().await.unwrap(), 43);
        assert_windows(&tailed, &format!("{layout} tailed")).await;
        let tailed_match = tailed
            .match_pattern(None, Some(&p1), None, None)
            .await
            .unwrap();
        assert_windows(&tailed_match, &format!("{layout} tailed match")).await;
    }
}

/// Every keep of `view` on `column` is the rows whose term `admits` — as a
/// set: a keep that reads the rows yields them in base order, a served view
/// in its index's.
async fn assert_keep(
    view: &VortexRdfStore,
    column: QuadColumn,
    keep: &Keep,
    admits: impl Fn(&Quad) -> bool,
    tag: &str,
) {
    let spelled = |quads: Vec<Quad>| {
        let mut rows: Vec<String> = quads.iter().map(|q| q.to_string()).collect();
        rows.sort();
        rows
    };
    let expected: Vec<Quad> = view
        .quads_vec()
        .await
        .unwrap()
        .into_iter()
        .filter(admits)
        .collect();
    let kept = view.keep(column, keep).await.unwrap();
    assert_eq!(kept.size().await.unwrap(), expected.len(), "{tag}: size");
    assert_eq!(
        spelled(kept.quads_vec().await.unwrap()),
        spelled(expected),
        "{tag}"
    );
}

/// A set keeps exactly its codes' rows and a range exactly the terms of a
/// spelling prefix, on a store and composed over a match; non-code layouts
/// and tailed views are rejected.
#[tokio::test]
async fn keep_narrows_by_code_set_and_range() {
    let quads = modular_quads(40, 4, 5);
    let store = VortexRdfStore::from_quads(
        quad_stream(quads.clone()),
        LayoutStrategy::Dictionary,
        vec![],
    )
    .await
    .unwrap();
    let dict = store.code_read_snapshot().unwrap();
    let objects = ["\"object 1\"", "\"object 3\""];
    let set = Keep::set(objects.iter().map(|o| dict.encode(o).unwrap()));
    let in_set = |q: &Quad| objects.contains(&q.object.to_string().as_str());
    let (lo, hi) = dict.prefix_range("<http://example.org/s1");
    let range = Keep::range(lo, hi);
    let in_range = |q: &Quad| q.subject.to_string().starts_with("<http://example.org/s1");
    let p1 = predicate(1);
    let matched = store
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    for (tag, view) in [("store", &store), ("match", &matched)] {
        assert_keep(view, QuadColumn::O, &set, in_set, &format!("{tag} set")).await;
        assert_keep(
            view,
            QuadColumn::S,
            &range,
            in_range,
            &format!("{tag} range"),
        )
        .await;
        let both = view.keep(QuadColumn::O, &set).await.unwrap();
        assert_keep(
            &both,
            QuadColumn::S,
            &range,
            in_range,
            &format!("{tag} set then range"),
        )
        .await;
    }
    assert!(
        store
            .keep(QuadColumn::O, &Keep::set([u32::MAX]))
            .await
            .unwrap()
            .quads_vec()
            .await
            .unwrap()
            .is_empty()
    );

    let plain =
        VortexRdfStore::from_quads(quad_stream(quads.clone()), LayoutStrategy::Default, vec![])
            .await
            .unwrap();
    assert!(plain.keep(QuadColumn::O, &set).await.is_err());
    let tailed = store
        .add_quads([
            quads[0].clone(),
            make_quad(
                "http://example.org/tail",
                "http://example.org/p1",
                "x",
                GraphName::DefaultGraph,
            ),
        ])
        .await
        .unwrap();
    assert!(tailed.keep(QuadColumn::O, &set).await.is_err());
}

#[cfg(feature = "file-io")]
#[tokio::test]
async fn windows_and_keeps_agree_on_a_file() {
    let quads = modular_quads(3_000, 7, 11);
    for indexes in [vec![], vec![IndexType::SecondaryByReference]] {
        let (_dir, path) =
            write_store_file(quads.clone(), LayoutStrategy::Dictionary, indexes.clone()).await;
        let store = VortexRdfStore::from_file(&path).await.unwrap();
        let tag = format!("file indexes={indexes:?}");
        assert_windows(&store, &tag).await;
        // An object-bound pattern: a pushed-down filter without an index,
        // an index-resolved selection with one.
        let o3 = object(3);
        let matched = store
            .match_pattern(None, None, Some(&o3), None)
            .await
            .unwrap();
        assert_windows(&matched, &format!("{tag} object match")).await;
        let s = subject(2_345);
        let by_subject = store
            .match_pattern(Some(&s), None, None, None)
            .await
            .unwrap();
        assert_windows(&by_subject, &format!("{tag} subject match")).await;
        let deleted = store
            .delete_quad(&quads[10])
            .await
            .unwrap()
            .delete_quad(&quads[11])
            .await
            .unwrap();
        assert_windows(&deleted, &format!("{tag} deleted")).await;

        let dict = store.code_read_snapshot().unwrap();
        let objects = ["\"object 1\"", "\"object 4\"", "\"object 9\""];
        let set = Keep::set(objects.iter().map(|o| dict.encode(o).unwrap()));
        let in_set = |q: &Quad| objects.contains(&q.object.to_string().as_str());
        let (lo, hi) = dict.prefix_range("<http://example.org/s12");
        let range = Keep::range(lo, hi);
        let in_range = |q: &Quad| q.subject.to_string().starts_with("<http://example.org/s12");
        let p2 = predicate(2);
        let by_predicate = store
            .match_pattern(None, Some(&p2), None, None)
            .await
            .unwrap();
        for (view_tag, view) in [
            ("store", &store),
            ("object match", &matched),
            ("predicate match", &by_predicate),
            ("deleted", &deleted),
        ] {
            assert_keep(
                view,
                QuadColumn::O,
                &set,
                in_set,
                &format!("{tag} {view_tag} set"),
            )
            .await;
            assert_keep(
                view,
                QuadColumn::S,
                &range,
                in_range,
                &format!("{tag} {view_tag} range"),
            )
            .await;
            let both = view.keep(QuadColumn::S, &range).await.unwrap();
            assert_keep(
                &both,
                QuadColumn::O,
                &set,
                in_set,
                &format!("{tag} {view_tag} range then set"),
            )
            .await;
            assert_windows(&both, &format!("{tag} {view_tag} kept window")).await;
        }
    }
}

/// The dictionary's predicate scan partitions its terms as the predicate
/// rules say, feeds `keep`, and memoizes; `encode` resolves a canonical
/// spelling on a miss.
#[tokio::test]
async fn filter_codes_partition_the_dictionary() {
    use crate::store::TermPredicate;
    let xsd_int = NamedNode::new("http://www.w3.org/2001/XMLSchema#integer").unwrap();
    let s = |i: usize| {
        NamedOrBlankNode::NamedNode(NamedNode::new(format!("http://example.org/s{i}")).unwrap())
    };
    let p = predicate(0);
    let quads = vec![
        Quad::new(
            s(0),
            p.clone(),
            Term::Literal(Literal::new_typed_literal("42", xsd_int.clone())),
            GraphName::DefaultGraph,
        ),
        Quad::new(
            s(1),
            p.clone(),
            Term::Literal(Literal::new_typed_literal("7", xsd_int)),
            GraphName::DefaultGraph,
        ),
        Quad::new(
            s(2),
            p.clone(),
            Term::Literal(Literal::new_language_tagged_literal("Bob", "en").unwrap()),
            GraphName::DefaultGraph,
        ),
        Quad::new(
            s(3),
            p.clone(),
            Term::Literal(Literal::new_simple_literal("Alice")),
            GraphName::DefaultGraph,
        ),
        Quad::new(
            s(4),
            p.clone(),
            Term::NamedNode(NamedNode::new("http://example.org/o").unwrap()),
            GraphName::DefaultGraph,
        ),
        Quad::new(
            NamedOrBlankNode::BlankNode(oxrdf::BlankNode::new("b0").unwrap()),
            p,
            Term::Literal(Literal::new_simple_literal("Anon")),
            GraphName::DefaultGraph,
        ),
    ];
    let store = VortexRdfStore::from_quads(
        quad_stream(quads.clone()),
        LayoutStrategy::Dictionary,
        vec![],
    )
    .await
    .unwrap();
    let dict = store.code_read_snapshot().unwrap();
    let terms: Vec<String> = (0..dict.len() as u32)
        .map(|c| dict.decode(c).unwrap())
        .collect();
    let spelled = |codes: &vortex_buffer::Buffer<u32>| -> Vec<&str> {
        codes.iter().map(|&c| terms[c as usize].as_str()).collect()
    };
    let check = |kind: &str, arg: &str, holds: &[&str], unknown: &[&str]| {
        let predicate = TermPredicate::parse(kind, arg).unwrap();
        let (got_holds, got_unknown) = dict.filter_codes(&predicate).unwrap();
        assert_eq!(spelled(&got_holds), holds, "{kind}({arg}) holds");
        assert_eq!(spelled(&got_unknown), unknown, "{kind}({arg}) unknown");
    };
    let int = |v: &str| format!("\"{v}\"^^<http://www.w3.org/2001/XMLSchema#integer>");
    let (forty_two, seven) = (int("42"), int("7"));
    // Codes are byte-order ranks: "" < '"' literals (digits before letters)
    // < '<' IRIs < '_:' blanks.
    check(
        "is_literal",
        "",
        &[&forty_two, &seven, "\"Alice\"", "\"Anon\"", "\"Bob\"@en"],
        &[""],
    );
    check(
        "num_gt",
        "10",
        &[&forty_two, "\"Alice\"", "\"Anon\"", "\"Bob\"@en"],
        &[
            "",
            "<http://example.org/o>",
            "<http://example.org/p0>",
            "<http://example.org/s0>",
            "<http://example.org/s1>",
            "<http://example.org/s2>",
            "<http://example.org/s3>",
            "<http://example.org/s4>",
            "_:b0",
        ],
    );
    check("num_eq", "7", &[&seven], &[""]);
    check(
        "lang_matches",
        "en",
        &["\"Bob\"@en"],
        &[
            "",
            "<http://example.org/o>",
            "<http://example.org/p0>",
            "<http://example.org/s0>",
            "<http://example.org/s1>",
            "<http://example.org/s2>",
            "<http://example.org/s3>",
            "<http://example.org/s4>",
            "_:b0",
        ],
    );
    check(
        "str_prefix",
        "http://example.org/s",
        &[
            "<http://example.org/s0>",
            "<http://example.org/s1>",
            "<http://example.org/s2>",
            "<http://example.org/s3>",
            "<http://example.org/s4>",
        ],
        &["", &forty_two, &seven],
    );

    // The sets feed `keep`: the rows whose object the predicate holds for,
    // in base (subject) order.
    let (holds, _) = dict
        .filter_codes(&TermPredicate::parse("num_gt", "10").unwrap())
        .unwrap();
    let kept = store.keep(QuadColumn::O, &Keep::Set(holds)).await.unwrap();
    let objects: Vec<String> = kept
        .quads_vec()
        .await
        .unwrap()
        .iter()
        .map(|q| q.object.to_string())
        .collect();
    assert_eq!(
        objects,
        [&forty_two, "\"Bob\"@en", "\"Alice\"", "\"Anon\""].map(String::from)
    );

    // Memoized: the second call hands back the same buffers.
    let predicate = TermPredicate::parse("is_literal", "").unwrap();
    let first = dict.filter_codes(&predicate).unwrap();
    let second = dict.filter_codes(&predicate).unwrap();
    assert_eq!(first.0.as_slice().as_ptr(), second.0.as_slice().as_ptr());

    // A tolerant encode: the xsd:string spelling of a plain literal.
    let alice = dict.encode("\"Alice\"").unwrap();
    assert_eq!(
        dict.encode("\"Alice\"^^<http://www.w3.org/2001/XMLSchema#string>"),
        Some(alice)
    );
    assert_eq!(dict.encode("\"nope\""), None);
    assert_eq!(dict.encode("not a term"), None);
    assert_eq!(
        dict.encode_many(&["\"Alice\"", "not a term", "_:b0"]),
        vec![Some(alice), None, dict.encode("_:b0")]
    );
}

#[cfg(feature = "file-io")]
#[tokio::test]
async fn match_pattern_many_equals_the_singles_on_a_file() {
    for indexes in [vec![], vec![IndexType::SecondaryByReference]] {
        let (_dir, path) = write_store_file(
            modular_quads(2_000, 4, 5),
            LayoutStrategy::Dictionary,
            indexes,
        )
        .await;
        let store = VortexRdfStore::from_file(&path).await.unwrap();
        assert_many_equals_singles(&store, "file").await;
    }
}

/// A term-code constraint on a served run narrows the run in place — the
/// view keeps its plan and its deferred ids — where the plan can say how: on
/// a key the resolution fixed it is all or nothing; on the next key, or a
/// later one ascending within the run, it is a binary search; anywhere else
/// the rows are read. Every answer equals the linear pass.
#[tokio::test]
async fn keep_narrows_a_served_run_and_keeps_its_plan() {
    let quads = modular_quads(4000, 5, 40);
    let store = VortexRdfStore::from_quads(
        quad_stream(quads.clone()),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await
    .unwrap();
    let dict = store.code_read_snapshot().unwrap();
    let p1 = predicate(1);
    let served = store
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    assert!(served.debug_has_serve_plan() && served.debug_selection_pending());

    // `o` is the next key of a `p` run: a code range is a sub-run.
    let (lo, hi) = dict.prefix_range("\"object 1");
    let range = Keep::range(lo, hi);
    let in_range = |q: &Quad| q.object.to_string().starts_with("\"object 1");
    assert_keep(&served, QuadColumn::O, &range, in_range, "served o range").await;
    let narrowed = served.keep(QuadColumn::O, &range).await.unwrap();
    assert!(
        narrowed.debug_has_serve_plan() && narrowed.debug_selection_pending(),
        "a narrowed run keeps its plan and its deferred ids"
    );
    // The run holds objects 1, 6, 11, 16, … (i ≡ 1 mod 5, i mod 40): a code
    // set whose runs touch coalesces into one sub-run and keeps the plan; one
    // with a run of the view between its runs reads the rows.
    let touching = ["\"object 1\"", "\"object 11\""];
    let set = Keep::set(touching.iter().map(|o| dict.encode(o).unwrap()));
    let in_set = |q: &Quad| touching.contains(&q.object.to_string().as_str());
    assert_keep(
        &served,
        QuadColumn::O,
        &set,
        in_set,
        "served o touching set",
    )
    .await;
    assert!(
        served
            .keep(QuadColumn::O, &set)
            .await
            .unwrap()
            .debug_has_serve_plan()
    );
    let apart = ["\"object 1\"", "\"object 16\""];
    let set = Keep::set(apart.iter().map(|o| dict.encode(o).unwrap()));
    let in_set = |q: &Quad| apart.contains(&q.object.to_string().as_str());
    assert_keep(&served, QuadColumn::O, &set, in_set, "served o set apart").await;
    assert!(
        !served
            .keep(QuadColumn::O, &set)
            .await
            .unwrap()
            .debug_has_serve_plan()
    );
    // The fixed key: all or nothing, plan kept.
    let p1_code = dict.encode(&p1.to_string()).unwrap();
    let same = served
        .keep(QuadColumn::P, &Keep::set([p1_code]))
        .await
        .unwrap();
    assert!(same.debug_has_serve_plan());
    assert_eq!(same.size().await.unwrap(), served.size().await.unwrap());
    let none = served
        .keep(QuadColumn::P, &Keep::range(p1_code + 1, p1_code + 2))
        .await
        .unwrap();
    assert_eq!(none.size().await.unwrap(), 0);
    // A later key that is not ascending within the run reads the rows.
    let (slo, shi) = dict.prefix_range("<http://example.org/s1");
    let s_range = Keep::range(slo, shi);
    let in_s = |q: &Quad| q.subject.to_string().starts_with("<http://example.org/s1");
    assert_keep(&served, QuadColumn::S, &s_range, in_s, "served s range").await;
    assert!(
        !served
            .keep(QuadColumn::S, &s_range)
            .await
            .unwrap()
            .debug_has_serve_plan()
    );
    // With two keys fixed, `s` is the next key.
    let po = store
        .match_pattern(None, Some(&p1), Some(&object(1)), None)
        .await
        .unwrap();
    assert!(po.debug_has_serve_plan());
    assert_keep(&po, QuadColumn::S, &s_range, in_s, "served po, s range").await;
    assert!(
        po.keep(QuadColumn::S, &s_range)
            .await
            .unwrap()
            .debug_has_serve_plan()
    );
}

/// On the sorted base a keep on `s` — or on a later column while every
/// column before it is constant over a contiguous selection — narrows by
/// binary search and keeps the selection a range, so the code export stays a
/// slice of the base; a selection spanning several subjects reads the rows.
#[tokio::test]
async fn keep_searches_a_sorted_prefix_column() {
    // Forty subjects, ten rows each.
    let quads: Vec<Quad> = (0..400)
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{:03}", i / 10),
                &format!("http://example.org/p{}", i % 4),
                &format!("object {}", i % 5),
                GraphName::DefaultGraph,
            )
        })
        .collect();
    let store = VortexRdfStore::from_quads(
        quad_stream(quads.clone()),
        LayoutStrategy::Dictionary,
        vec![],
    )
    .await
    .unwrap();
    let dict = store.code_read_snapshot().unwrap();
    let (lo, hi) = dict.prefix_range("<http://example.org/s01");
    let s_range = Keep::range(lo, hi);
    let in_s = |q: &Quad| q.subject.to_string().starts_with("<http://example.org/s01");
    assert_keep(&store, QuadColumn::S, &s_range, in_s, "store s range").await;
    let kept = store.keep(QuadColumn::S, &s_range).await.unwrap();
    let run = kept
        .debug_selection_range()
        .expect("a keep on the sorted subject column is a range");
    let base = store.code_columns(&QuadColumn::ALL).unwrap().unwrap();
    let sliced = kept.code_columns(&QuadColumn::ALL).unwrap().unwrap();
    assert_eq!(
        sliced[0].as_slice().as_ptr(),
        base[0].as_slice()[run.start as usize..].as_ptr(),
        "the kept range is a slice of the base"
    );

    // Within one subject's run `p` is ascending: still a range.
    let s = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/s003").unwrap());
    let one = store
        .match_pattern(Some(&s), None, None, None)
        .await
        .unwrap();
    assert!(one.debug_selection_range().is_some());
    let (plo, phi) = dict.prefix_range("<http://example.org/p1");
    let p_range = Keep::range(plo, phi);
    let in_p = |q: &Quad| {
        q.predicate
            .to_string()
            .starts_with("<http://example.org/p1")
    };
    assert_keep(&one, QuadColumn::P, &p_range, in_p, "subject run, p range").await;
    assert!(
        one.keep(QuadColumn::P, &p_range)
            .await
            .unwrap()
            .debug_selection_range()
            .is_some()
    );

    // A window over several subjects: `p` is not ascending there, so the
    // rows are read and the answer is an id list.
    let span = store.window(3, 50).await.unwrap();
    assert_keep(
        &span,
        QuadColumn::P,
        &p_range,
        in_p,
        "spanning window, p range",
    )
    .await;
    assert!(
        span.keep(QuadColumn::P, &p_range)
            .await
            .unwrap()
            .debug_selection_range()
            .is_none()
    );
}
