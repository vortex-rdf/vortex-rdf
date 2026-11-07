//! The reference indexes' graph column: under the Dictionary layout each
//! reference child holds, beside each row id, the graph of the quad that row
//! id names, so a by-reference run checks its own graph rows.

use super::*;
use crate::store::indexes::IndexComponent;
use crate::store::test_hooks::ForcedReferenceGraphs;

/// 60 quads over three graphs, the default graph among them, four
/// predicates and five objects: each predicate and object meets every graph.
fn three_graph_sixty() -> Vec<Quad> {
    let graphs = [GraphName::DefaultGraph, iri_graph("g1"), iri_graph("g2")];
    graph_modular_quads(60, 2, 4, 5, &graphs)
}

/// A Dictionary reference child's columns by name, each as its codes or row
/// ids, in the child's order.
fn child_columns(component: &IndexComponent) -> Vec<(String, Vec<u64>)> {
    use crate::store::array::field_as;
    use vortex_array::VortexSessionExecute as _;
    use vortex_array::arrays::PrimitiveArray;
    use vortex_array::arrays::struct_::StructArrayExt as _;

    let mut ctx = crate::session::VORTEX_SESSION.create_execution_ctx();
    let rows = component.rows().unwrap();
    rows.names()
        .iter()
        .map(|name| {
            let column: PrimitiveArray = field_as(rows, name.as_ref(), &mut ctx).unwrap();
            (name.to_string(), column.as_slice::<u64>().to_vec())
        })
        .collect()
}

/// A child's column names, in order.
fn child_names(component: &IndexComponent) -> Vec<String> {
    use vortex_array::arrays::struct_::StructArrayExt as _;
    let rows = component.rows().unwrap();
    rows.names().iter().map(ToString::to_string).collect()
}

/// Each Dictionary reference child built in memory holds `{val, rid, g}`:
/// `g` is the graph of the quad at `rid`, the default graph's code included,
/// and the rows stay in `(val, rid)` order. String layouts' children keep
/// `{val, rid}`.
#[tokio::test]
async fn reference_children_hold_each_rows_graph() {
    let built = build_array::<SortedInMemoryBuilder>(
        quad_stream(three_graph_sixty()),
        LayoutStrategy::Dictionary,
        IndexConfig::Reference.types(),
    )
    .await
    .unwrap();
    let components = built.components.clone();
    let store = VortexRdfStore::from_built(built).unwrap();
    let rows = row_list(&store).await;
    assert_eq!(components.len(), 2, "ref-o and ref-p");
    for component in &components {
        let name = component.identity.name;
        let columns = child_columns(component);
        let names: Vec<&str> = columns.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, ["val", "rid", "g"], "{name}");
        let (val, rid, g) = (&columns[0].1, &columns[1].1, &columns[2].1);
        assert_eq!(rid.len(), rows.len(), "{name}");
        for i in 0..rid.len() {
            assert_eq!(g[i], rows[rid[i] as usize][3], "{name}: row {i}");
            if i > 0 {
                assert!(
                    (val[i - 1], rid[i - 1]) < (val[i], rid[i]),
                    "{name}: row {i}"
                );
            }
        }
    }
    // The default graph's code (its spelling is the empty one) is among the
    // graphs the children hold.
    let default_graph = store
        .dict_reader()
        .unwrap()
        .encode("")
        .await
        .unwrap()
        .unwrap();
    for component in &components {
        let graphs = &child_columns(component)[2].1;
        assert!(
            graphs.contains(&default_graph),
            "{}",
            component.identity.name
        );
    }
    for layout in [LayoutStrategy::Default, LayoutStrategy::TypedObject] {
        let built = build_array::<SortedInMemoryBuilder>(
            quad_stream(three_graph_sixty()),
            layout,
            IndexConfig::Reference.types(),
        )
        .await
        .unwrap();
        for component in &built.components {
            assert_eq!(child_names(component), ["val", "rid"], "{layout:?}");
        }
    }
}

/// An in-memory store's bytes, opened as a file: each reference child's `g`
/// records the zones' graphs (`vortexrdf.value_counts`); `val` and `rid`
/// don't.
#[tokio::test]
async fn written_reference_graphs_record_value_counts() {
    let memory = built_store(
        three_graph_sixty(),
        LayoutStrategy::Dictionary,
        IndexConfig::Reference.types(),
    )
    .await;
    let bytes = memory.to_bytes().await.unwrap();
    let (_dir, path) = temp_store_file(&bytes);
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let file = store.debug_native_file().unwrap();
    for name in ["index:ref-p", "index:ref-o"] {
        for (column, recorded) in [("val", false), ("rid", false), ("g", true)] {
            let layout = file
                .table_column_layout(Some(name), column)
                .unwrap_or_else(|| panic!("{name} has a {column} column"));
            assert_eq!(records_value_counts(&layout), recorded, "{name} {column}");
        }
    }
}

/// A store written without the column, standing for one written before it:
/// its children are `{val, rid}`, and a by-reference graph-bound pattern
/// counts and reads as its rows say.
#[tokio::test]
async fn older_reference_children_read_as_before() {
    let memory = {
        let _before = ForcedReferenceGraphs::set(false);
        built_store(
            three_graph_sixty(),
            LayoutStrategy::Dictionary,
            IndexConfig::Reference.types(),
        )
        .await
    };
    let bytes = memory.to_bytes().await.unwrap();
    let (_dir, path) = temp_store_file(&bytes);
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let file = store.debug_native_file().unwrap();
    for name in ["index:ref-p", "index:ref-o"] {
        assert!(
            file.table_column_layout(Some(name), "rid").is_some(),
            "{name}"
        );
        assert!(
            file.table_column_layout(Some(name), "g").is_none(),
            "{name}: no g"
        );
    }
    let rows = row_list(&store).await;
    let (p, g) = (rows[0][1], rows[0][3]);
    let mut want: Vec<[TermCode; 4]> = rows
        .iter()
        .filter(|row| row[1] == p && row[3] == g)
        .copied()
        .collect();
    want.sort_unstable();
    let of_p = rows.iter().filter(|row| row[1] == p).count();
    assert!(
        !want.is_empty() && want.len() < of_p,
        "the graph check keeps some rows"
    );
    let pattern = crate::CodePattern::new(None, Some(p), None, Some(g));
    assert!(
        store
            .locate_codes(pattern, 0, Some(0))
            .await
            .unwrap()
            .is_some()
    );
    assert_eq!(
        store.count_code_pattern(pattern).await.unwrap(),
        want.len() as u64
    );
    let mut got = code_rows::<4>(&store.match_code_pattern(pattern, 0, None).await.unwrap());
    got.sort_unstable();
    assert_eq!(got, want);
}

/// The out-of-core build, its spill runs three entries long, carries each
/// row's graph through the spill and the merge: every child row's `g` is the
/// graph of the quad at its `rid`, as the in-memory build gives it.
#[tokio::test]
async fn spilled_reference_graphs_cross_run_boundaries() {
    use crate::store::builders::{BuildSizes, sorted_stream};

    let graphs = [
        GraphName::DefaultGraph,
        iri_graph("g1"),
        iri_graph("g2"),
        iri_graph("g3"),
    ];
    let built = sorted_stream::build_array(
        Box::new(quad_stream(graph_modular_quads(40, 2, 3, 7, &graphs))),
        LayoutStrategy::Dictionary,
        IndexConfig::Reference.types(),
        BuildSizes::uniform(3),
    )
    .await
    .unwrap();
    let components = built.components.clone();
    let store = VortexRdfStore::from_built(built).unwrap();
    let rows = row_list(&store).await;
    for component in &components {
        let name = component.identity.name;
        let columns = child_columns(component);
        let names: Vec<&str> = columns.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, ["val", "rid", "g"], "{name}");
        assert_eq!(columns[1].1.len(), rows.len(), "{name}");
        for (i, (&rid, &g)) in columns[1].1.iter().zip(&columns[2].1).enumerate() {
            let row = crate::store::indexes::base_row(rid) as usize;
            assert_eq!(g, rows[row][3], "{name}: row {i}");
        }
    }
}

/// The out-of-core writer follows the test hook too: a file written under
/// `ForcedReferenceGraphs::set(false)` has `{val, rid}` children, one written
/// without it has `g`.
#[tokio::test]
async fn stream_written_children_follow_the_hook() {
    for graphs in [true, false] {
        let (_dir, path) = {
            let _hook = ForcedReferenceGraphs::set(graphs);
            write_store_file(
                three_graph_sixty(),
                LayoutStrategy::Dictionary,
                IndexConfig::Reference.types(),
            )
            .await
        };
        let store = VortexRdfStore::from_file(&path).await.unwrap();
        let file = store.debug_native_file().unwrap();
        for name in ["index:ref-p", "index:ref-o"] {
            let g = file.table_column_layout(Some(name), "g");
            assert_eq!(g.is_some(), graphs, "{name}, graphs {graphs}");
            if let Some(g) = g {
                assert!(records_value_counts(&g), "{name}: g records its graphs");
            }
        }
    }
}

/// A by-reference run checked against the graph reads its child's own `g`
/// rows, contiguous, and never the quad table's `g` at its scattered rows:
/// `(? type ? g1)`, `(? ? Taxon g1)` and `(? type Taxon g1)` on alternating
/// graphs, read row by row (zones off; zones deciding these runs are pinned
/// in `located`). A store written without the child `g`
/// reads the quad table's; both answer every read alike, as their rows say.
#[tokio::test]
async fn reference_runs_read_their_own_graphs() {
    use crate::CodePattern;
    use crate::store::test_hooks::ForcedZones;

    let _rows = ForcedZones::set(false);
    let mut answers = Vec::new();
    for graphs in [true, false] {
        let (_dir, path) = zone_file(
            ZoneFile::new(Graphs::Alternating, IndexConfig::Reference).reference_graphs(graphs),
        )
        .await;
        let store = &VortexRdfStore::from_file(&path).await.unwrap();
        let dict = store.dict_reader().unwrap();
        let code = |term: &'static str| {
            let dict = dict.clone();
            async move { dict.encode(term).await.unwrap().unwrap() }
        };
        let (ty, taxon, g1) = (
            code("<http://example.org/type>").await,
            code("\"Taxon\"").await,
            code("<http://example.org/g1>").await,
        );
        let rows = row_list(store).await;
        let mut store_answers = Vec::new();
        for pattern in [
            CodePattern::new(None, Some(ty), None, Some(g1)),
            CodePattern::new(None, None, Some(taxon), Some(g1)),
            CodePattern::new(None, Some(ty), Some(taxon), Some(g1)),
        ] {
            let tag = format!("graphs {graphs} {pattern:?}");
            let slots = [
                pattern.subject,
                pattern.predicate,
                pattern.object,
                pattern.graph,
            ];
            let want = rows
                .iter()
                .filter(|row| (0..4).all(|i| slots[i].is_none_or(|code| row[i] == code)))
                .count() as u64;
            assert!(
                store
                    .locate_codes(pattern, 0, Some(0))
                    .await
                    .unwrap()
                    .is_some(),
                "{tag}"
            );
            let rec = Recorder::start();
            assert_eq!(
                store.count_code_pattern(pattern).await.unwrap(),
                want,
                "{tag}"
            );
            let taken: u64 = rec.rows(|e| match e {
                Event::CheckTaken { slot: 3, rows } => Some(*rows),
                _ => None,
            });
            let own: u64 = rec.rows(|e| match e {
                Event::ReferenceGraphRead(rows) => Some(*rows),
                _ => None,
            });
            match graphs {
                true => assert_eq!((taken, own), (0, RUN as u64), "{tag}: the child's g"),
                false => assert!(taken > 0 && own == 0, "{tag}: the quad table's g"),
            }
            drop(rec);
            store_answers.push(code_answers(store, pattern).await);
        }
        answers.push(store_answers);
    }
    assert!(
        answers[0] == answers[1],
        "the child's g and the quad table's answer alike"
    );
}
