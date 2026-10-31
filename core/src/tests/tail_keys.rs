//! The tail's row keys (`TailKeys`): a fully bound match on the tail reads
//! the rows its key names instead of comparing every tail row, and finds what
//! the comparison finds, through single appends and batches, tombstones,
//! views and keys that two rows share.

use super::*;
use crate::store::layouts::ResolvedLayout;
use crate::store::view::tail_keys::TailKeys;
use vortex_array::scalar::Scalar;

/// A quad `http://example.org/{s} p9 {o}` in the default graph.
fn quad_with(s: &str, o: Term) -> Quad {
    Quad::new(
        NamedNode::new_unchecked(format!("http://example.org/{s}")),
        iri("p9"),
        o,
        GraphName::DefaultGraph,
    )
}

/// Every layout's tail answers its fully bound matches off its keys, never
/// comparing its rows: through more single appends than the tail keeps as
/// chunks and a batch past the flatten floor, each append keying its own rows
/// alone, for quads present and absent, a re-added batch adding nothing. A
/// tombstoned quad is absent and is added back once, the append that drops
/// its row keying the tail again; a view's tail answers only for the rows its
/// selection holds.
#[tokio::test]
async fn test_tail_keys_answer_fully_bound_matches() {
    // The tail rows keyed, and the tail matches made by a scan, that `rec`
    // saw.
    let rows_keyed = |rec: &Recorder| -> usize {
        rec.picked(|e| match e {
            Event::TailKeyed(rows) => Some(*rows),
            _ => None,
        })
        .iter()
        .sum()
    };
    let scanned = |rec: &Recorder| rec.count(&Event::TailMatch { keyed: false });
    let fresh = appended_quads(1_100);
    let absent = quad_with("tail-absent", plain_literal("late"));
    for layout in LAYOUTS {
        let matches = Recorder::start();
        let mut store = built_store(fixture_quads(), layout, vec![]).await;
        let mut keys = Recorder::start();
        for quad in &fresh[..70] {
            store = store.add_quad(quad.clone()).await.unwrap();
        }
        assert_eq!(rows_keyed(&keys), 70, "{layout:?}");
        let store = store.add_quads(fresh[70..].iter().cloned()).await.unwrap();
        assert_eq!(rows_keyed(&keys), fresh.len(), "{layout:?}");
        assert_eq!(store.tail_len(), fresh.len(), "{layout:?}");
        // The first add had no tail to look in; every later presence check
        // read the keys.
        assert_eq!(
            matches.count(&Event::TailMatch { keyed: true }),
            fresh.len() - 1,
            "{layout:?}"
        );

        for quad in fresh.iter().step_by(97) {
            assert!(store.contains(quad).await.unwrap(), "{layout:?} {quad}");
        }
        assert!(!store.contains(&absent).await.unwrap(), "{layout:?}");
        let size = store.size().await.unwrap();
        let again = store.add_quads(fresh.iter().cloned()).await.unwrap();
        assert_eq!(again.size().await.unwrap(), size, "{layout:?}");

        // Deleting a tail quad tombstones its row; the append after it drops
        // the row, renumbering the rows after it, whose keys are taken again.
        let doomed = &fresh[5];
        let deleted = store.delete_quad(doomed).await.unwrap();
        assert!(!deleted.contains(doomed).await.unwrap(), "{layout:?}");
        keys.take();
        let restored = deleted.add_quad(doomed.clone()).await.unwrap();
        assert_eq!(rows_keyed(&keys), fresh.len(), "{layout:?}");
        assert_eq!(restored.size().await.unwrap(), size, "{layout:?}");
        for quad in fresh.iter().step_by(97).chain([doomed]) {
            assert!(restored.contains(quad).await.unwrap(), "{layout:?} {quad}");
        }
        let twice = restored.add_quads(fresh.iter().cloned()).await.unwrap();
        assert_eq!(twice.size().await.unwrap(), size, "{layout:?}");

        assert_eq!(scanned(&matches), 0, "{layout:?}");

        // A view's tail holds only the rows its match kept: a match of the
        // subject alone, which compares the tail's rows.
        let tail1 =
            NamedOrBlankNode::NamedNode(NamedNode::new_unchecked("http://example.org/tail1"));
        let view = matched_view(&store, Some(&tail1), None, None, None).await;
        assert!(view.contains(&fresh[1]).await.unwrap(), "{layout:?}");
        assert!(!view.contains(&fresh[2]).await.unwrap(), "{layout:?}");
        assert_eq!(scanned(&matches), 1, "{layout:?}");
    }
}

/// Under TypedObject a key holds the object's kind and value but not its
/// datatype or language, so two literals that differ only there share a key:
/// the check of each row a key names tells them apart, as the comparison of
/// every row did. The other layouts key the whole object.
#[tokio::test]
async fn test_tail_keys_check_what_the_key_leaves_out() {
    let objects = [
        Term::Literal(Literal::new_typed_literal("1", oxrdf::vocab::xsd::INTEGER)),
        Term::Literal(Literal::new_typed_literal("1", oxrdf::vocab::xsd::DECIMAL)),
        Term::Literal(Literal::new_language_tagged_literal_unchecked("one", "en")),
        Term::Literal(Literal::new_language_tagged_literal_unchecked("one", "fr")),
    ];
    let quads: Vec<Quad> = objects
        .iter()
        .map(|o| quad_with("typed", o.clone()))
        .collect();
    for layout in LAYOUTS {
        let store = built_store(fixture_quads(), layout, vec![])
            .await
            .add_quads([quads[0].clone(), quads[2].clone()])
            .await
            .unwrap();
        for (quad, held) in quads.iter().zip([true, false, true, false]) {
            assert_eq!(
                store.contains(quad).await.unwrap(),
                held,
                "{layout:?} {quad}"
            );
        }
        let store = store.add_quads(quads.iter().cloned()).await.unwrap();
        assert_eq!(store.tail_len(), 4, "{layout:?}");
        for quad in &quads {
            let (s, p, o, g) = (
                &quad.subject,
                &quad.predicate,
                &quad.object,
                &quad.graph_name,
            );
            let matched = matched_view(&store, Some(s), Some(p), Some(o), Some(g)).await;
            assert_eq!(
                matched.quads_vec().await.unwrap(),
                vec![quad.clone()],
                "{layout:?}"
            );
        }
    }
}

/// The keys of a run of single appends: each append's key joins a run of its
/// own, merged into the run before while it holds at least half that run's
/// rows, so the runs shrink by more than half from oldest to newest, and
/// every row is found under its key.
#[test]
fn test_tail_keys_merge_their_runs() {
    let raws: Vec<RawQuad> = appended_quads(300).iter().map(RawQuad::from_quad).collect();
    let row = |raw: &RawQuad| {
        crate::store::builders::build_struct_array(
            std::slice::from_ref(raw),
            LayoutStrategy::Default,
            false,
        )
        .unwrap()
    };
    let mut keys = TailKeys::of(&row(&raws[0]), &ResolvedLayout::Default)
        .unwrap()
        .unwrap();
    for raw in &raws[1..] {
        keys = keys.extended(&row(raw)).unwrap();
        let runs = keys.run_lengths();
        assert!(
            runs.windows(2).all(|pair| pair[1] * 2 < pair[0]),
            "{runs:?}"
        );
    }
    assert_eq!(keys.rows(), raws.len());
    assert_eq!(keys.run_lengths().iter().sum::<usize>(), raws.len());
    for (i, raw) in raws.iter().enumerate() {
        let eqs = [
            ("s", Scalar::from(raw.s.as_str())),
            ("p", Scalar::from(raw.p.as_str())),
            ("o", Scalar::from(raw.o.as_str())),
            ("g", Scalar::from(raw.g.as_str())),
        ];
        assert_eq!(keys.candidates(&eqs), Some(vec![i as u64]), "{}", raw.s);
        assert_eq!(keys.candidates(&eqs[..3]), None, "a free graph has no key");
    }
}
