//! Built stores hold each quad once, and each term once in its canonical
//! spelling.
//!
//! The probe is the one that found the gap in vortex-rdf 0.11.0: eight
//! N-Quads lines that encode three distinct RDF triples. The parser (oxrdf)
//! already rewrites every spelling of a term to one canonical N-Triples form,
//! so the dictionary held six terms; but both builders sorted the rows
//! without dropping the repeats, and the store kept all eight.
//!
//! Every test here builds from N-Quads text, so the terms go through the same
//! parser a file or a string does, and compares the store with a brute-force
//! answer computed from the distinct set of the parsed quads.

use super::*;
use crate::common::terms::{canonical_spelling, parse_pattern_checked, parse_quads_from_reader};
use crate::store::RawQuad;
use crate::store::builders::{BuiltArray, sorted_stream};
use oxrdfio::RdfFormat;
use std::collections::BTreeSet;
use std::io::Cursor;
use vortex_array::VortexSessionExecute as _;
use vortex_array::arrays::PrimitiveArray;

/// Three RDF triples written eight ways: `"x"` four times (twice plain, once
/// typed `xsd:string`, once through a `\u` escape), `"y"@en` twice (`@EN`
/// and `@en`), and `"z"` under a subject written two ways.
const PROBE: &str = r#"<http://ex.org/s> <http://ex.org/p> "x" .
<http://ex.org/s> <http://ex.org/p> "x" .
<http://ex.org/s> <http://ex.org/p> "x"^^<http://www.w3.org/2001/XMLSchema#string> .
<http://ex.org/s> <http://ex.org/p> "\u0078" .
<http://ex.org/s> <http://ex.org/p> "y"@EN .
<http://ex.org/s> <http://ex.org/p> "y"@en .
<http://ex.org/s> <http://ex.org/p> "z" .
<http://ex.org/\u0073> <http://ex.org/p> "z" .
"#;

/// The probe plus distinct triples, a named graph, and repeats of both: the
/// shape of a real dataset with duplicates scattered through it.
const DATASET: &str = r#"<http://ex.org/s> <http://ex.org/p> "x" .
<http://ex.org/s> <http://ex.org/p> "x" .
<http://ex.org/s> <http://ex.org/p> "x"^^<http://www.w3.org/2001/XMLSchema#string> .
<http://ex.org/s> <http://ex.org/p> "\u0078" .
<http://ex.org/s> <http://ex.org/p> "y"@EN .
<http://ex.org/s> <http://ex.org/p> "y"@en .
<http://ex.org/s> <http://ex.org/p> "z" .
<http://ex.org/\u0073> <http://ex.org/p> "z" .
<http://ex.org/s> <http://ex.org/p> "x" <http://ex.org/g> .
<http://ex.org/s> <http://ex.org/p> "x" <http://ex.org/g> .
<http://ex.org/s2> <http://ex.org/p> "x" .
<http://ex.org/s2> <http://ex.org/q> "y"@en .
<http://ex.org/s3> <http://ex.org/q> <http://ex.org/o> .
<http://ex.org/s3> <http://ex.org/q> <http://ex.org/o> .
"#;

/// Lines of [`DATASET`] and its distinct quads: the duplicate count is the
/// difference.
const DATASET_LINES: usize = 14;
const DATASET_DISTINCT: usize = 7;

const ALL_INDEXES: [IndexType; 2] = [IndexType::SecondaryByCopy, IndexType::SecondaryByReference];
const LAYOUTS: [LayoutStrategy; 3] = [
    LayoutStrategy::Default,
    LayoutStrategy::TypedObject,
    LayoutStrategy::Dictionary,
];

/// Every index set worth building: none, each family, both.
fn index_sets() -> Vec<Indexes> {
    vec![
        vec![],
        vec![IndexType::SecondaryByCopy],
        vec![IndexType::SecondaryByReference],
        ALL_INDEXES.to_vec(),
    ]
}

type Row = (String, String, String, String);

fn row(q: &RawQuad) -> Row {
    (q.s.clone(), q.p.clone(), q.o.clone(), q.g.clone())
}

/// `text` parsed the way a file or a string is: through the oxrdfio parser,
/// into the canonical N-Triples spelling the builders consume.
fn raw_stream(
    text: &'static str,
) -> impl futures::Stream<Item = crate::error::Result<RawQuad>> + Unpin + Send + 'static {
    parse_quads_from_reader(Cursor::new(text.as_bytes()), RdfFormat::NQuads)
}

async fn raws(text: &'static str) -> Vec<RawQuad> {
    raw_stream(text).map(|q| q.unwrap()).collect().await
}

/// The distinct quads of `text`, computed independently of any builder.
async fn distinct(text: &'static str) -> BTreeSet<Row> {
    raws(text).await.iter().map(row).collect()
}

/// The terms of `text` in their canonical spelling: what a dictionary built
/// from it must hold, once each.
async fn distinct_terms(text: &'static str) -> BTreeSet<String> {
    raws(text)
        .await
        .into_iter()
        .flat_map(|q| [q.s, q.p, q.o, q.g])
        .collect()
}

/// A quad pattern in the spelling the columns store: each of subject,
/// predicate, object and graph bound (`Some`) or free (`None`).
type Pattern = (
    Option<&'static str>,
    Option<&'static str>,
    Option<&'static str>,
    Option<&'static str>,
);

/// The patterns every store is asked, each a mix of subject, predicate,
/// object and graph in the spelling the columns store (`""` is the default
/// graph): by object (plain, language-tagged, IRI), by predicate, by
/// predicate and object, by subject, by graph, and unconstrained. With an
/// index built, the object, predicate and predicate-object ones are the ones
/// it serves.
const PATTERNS: &[Pattern] = &[
    (None, None, None, None),
    (None, None, Some("\"x\""), None),
    (None, None, Some("\"y\"@en"), None),
    (None, None, Some("<http://ex.org/o>"), None),
    (None, Some("<http://ex.org/p>"), None, None),
    (None, Some("<http://ex.org/p>"), Some("\"x\""), None),
    (None, Some("<http://ex.org/q>"), Some("\"y\"@en"), None),
    (Some("<http://ex.org/s>"), None, None, None),
    (
        Some("<http://ex.org/s>"),
        Some("<http://ex.org/p>"),
        None,
        None,
    ),
    (None, None, None, Some("<http://ex.org/g>")),
    (None, None, None, Some("")),
    (None, None, Some("\"x\""), Some("<http://ex.org/g>")),
];

/// Whether `quad` is an answer to a pattern, by plain string comparison.
fn answers(quad: &Row, (s, p, o, g): &Pattern) -> bool {
    let free_or_equal =
        |bound: &Option<&str>, field: &String| bound.is_none_or(|b| b == field.as_str());
    free_or_equal(s, &quad.0)
        && free_or_equal(p, &quad.1)
        && free_or_equal(o, &quad.2)
        && free_or_equal(g, &quad.3)
}

/// `store` holds exactly the `expected` quads, each once, and answers every
/// pattern — through its indexes where it has them — with the brute-force
/// answer over that set, also each row once.
async fn assert_holds_each_quad_once(store: &VortexRdfStore, expected: &BTreeSet<Row>, who: &str) {
    assert_eq!(store.size().await.unwrap(), expected.len(), "{who}: size");

    let rows = tuple_rows(&store.quads_vec().await.unwrap());
    assert_eq!(rows.len(), expected.len(), "{who}: rows read back");
    assert_eq!(
        rows.iter().cloned().collect::<BTreeSet<_>>(),
        *expected,
        "{who}: rows"
    );

    for pattern in PATTERNS {
        let (s, p, o, g) =
            parse_pattern_checked(pattern.0, pattern.1, pattern.2, pattern.3).unwrap();
        let view = store
            .match_pattern(s.as_ref(), p.as_ref(), o.as_ref(), g.as_ref())
            .await
            .unwrap();
        let want: BTreeSet<Row> = expected
            .iter()
            .filter(|q| answers(q, pattern))
            .cloned()
            .collect();
        let got = tuple_rows(&view.quads_vec().await.unwrap());
        assert_eq!(got.len(), want.len(), "{who}: {pattern:?}: rows matched");
        assert_eq!(
            got.into_iter().collect::<BTreeSet<_>>(),
            want,
            "{who}: {pattern:?}: matched quads"
        );
        assert_eq!(
            view.size().await.unwrap(),
            expected.iter().filter(|q| answers(q, pattern)).count(),
            "{who}: {pattern:?}: matched count"
        );
    }
}

/// A build of [`DATASET`] through every pipeline a store can come out of,
/// handed to `check` with a label: the in-memory sort, the out-of-core one
/// (resident, then forced through several spilled runs), `from_quads`, the
/// byte writer, the interning sink the wasm array path feeds, and a file
/// opened mapped and loaded whole.
async fn for_each_build<F, Fut>(layout: LayoutStrategy, indexes: &Indexes, mut check: F)
where
    F: FnMut(String, VortexRdfStore) -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let tag = |pipeline: &str| format!("{pipeline} / {layout:?} / {indexes:?}");

    let built = build_array::<SortedInMemoryBuilder>(raw_stream(DATASET), layout, indexes.clone())
        .await
        .unwrap();
    check(
        tag("SortedInMemoryBuilder"),
        VortexRdfStore::from_built(built).unwrap(),
    )
    .await;

    let built = build_array::<SortedStreamBuilder>(raw_stream(DATASET), layout, indexes.clone())
        .await
        .unwrap();
    check(
        tag("SortedStreamBuilder"),
        VortexRdfStore::from_built(built).unwrap(),
    )
    .await;

    // A run capacity of 3 spills the 14 lines into five runs, so the repeats
    // sit in different runs.
    let built =
        sorted_stream::build_array(Box::new(raw_stream(DATASET)), layout, indexes.clone(), 3)
            .await
            .unwrap();
    check(
        tag("SortedStreamBuilder, spilled"),
        VortexRdfStore::from_built(built).unwrap(),
    )
    .await;

    let store = VortexRdfStore::from_quads(raw_stream(DATASET), layout, indexes.clone())
        .await
        .unwrap();
    check(tag("from_quads"), store).await;

    if layout == LayoutStrategy::Dictionary {
        let mut sink = DictionaryQuadSink::new(indexes.clone());
        for quad in raws(DATASET).await {
            sink.push(quad);
        }
        check(
            tag("DictionaryQuadSink"),
            VortexRdfStore::from_built(sink.finish().unwrap()).unwrap(),
        )
        .await;
    }

    #[cfg(feature = "file-io")]
    {
        // The in-memory builder's chunk stream, written out the way the wasm
        // bindings serialize it.
        let built = crate::store::builders::sorted_in_memory::build_chunk_stream(
            Box::new(raw_stream(DATASET)),
            layout,
            indexes.clone(),
            3,
        )
        .await
        .unwrap();
        let mut bytes: Vec<u8> = Vec::new();
        crate::io::ser::built_stream_to_vortex_writer(built, &mut bytes)
            .await
            .unwrap();
        check(
            tag("SortedInMemoryBuilder chunk stream, from bytes"),
            VortexRdfStore::from_bytes(&bytes).await.unwrap(),
        )
        .await;

        let mut bytes: Vec<u8> = Vec::new();
        quads_stream_to_vortex_writer(raw_stream(DATASET), &mut bytes, layout, indexes.clone())
            .await
            .unwrap();
        check(
            tag("quads_stream_to_vortex_writer, from bytes"),
            VortexRdfStore::from_bytes(&bytes).await.unwrap(),
        )
        .await;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dataset.vortex");
        crate::io::quads_stream_to_vortex_file(raw_stream(DATASET), &path, layout, indexes.clone())
            .await
            .unwrap();
        check(
            tag("file, mapped"),
            VortexRdfStore::from_file(&path).await.unwrap(),
        )
        .await;
        check(
            tag("file, loaded whole"),
            VortexRdfStore::from_file_in_memory(&path).await.unwrap(),
        )
        .await;
    }
}

// ─── Rows ──────────────────────────────────────────────────────────────

/// The dataset really has repeats (and the distinct count is the one the
/// tests below hold stores to), so a pass is not a vacuous one.
#[tokio::test]
async fn test_the_dataset_has_repeats() {
    assert_eq!(raws(DATASET).await.len(), DATASET_LINES);
    assert_eq!(distinct(DATASET).await.len(), DATASET_DISTINCT);
    // The probe alone: eight lines, three RDF triples.
    assert_eq!(raws(PROBE).await.len(), 8);
    assert_eq!(distinct(PROBE).await.len(), 3);
}

/// Whichever pipeline builds it, under every layout and with every index
/// set, a store from the dataset holds each distinct quad once: its size,
/// the rows it reads back, and every pattern's matches (served by an index
/// where there is one) agree with the brute-force answer over the distinct
/// set.
#[tokio::test]
async fn test_every_pipeline_builds_each_quad_once() {
    let expected = distinct(DATASET).await;
    for layout in LAYOUTS {
        for indexes in index_sets() {
            for_each_build(layout, &indexes, |who, store| {
                let expected = expected.clone();
                let indexes = indexes.clone();
                async move {
                    assert_eq!(store.layout(), layout, "{who}");
                    assert_eq!(store.indexes(), indexes.as_slice(), "{who}");
                    assert_holds_each_quad_once(&store, &expected, &who).await;
                }
            })
            .await;
        }
    }
}

/// The index lookups are what the patterns above go through when an index is
/// built: a predicate pattern over a copy-indexed store is planned onto its
/// sorted copy, so the agreement with the brute-force answer is the index's.
#[tokio::test]
async fn test_index_served_matches_ignore_the_repeats() {
    let expected = distinct(DATASET).await;
    let p = NamedNode::new("http://ex.org/p").unwrap();
    for layout in [LayoutStrategy::Default, LayoutStrategy::Dictionary] {
        let built = build_array::<SortedStreamBuilder>(
            raw_stream(DATASET),
            layout,
            vec![IndexType::SecondaryByCopy],
        )
        .await
        .unwrap();
        let store = VortexRdfStore::from_built(built).unwrap();
        let served = store
            .match_pattern(None, Some(&p), None, None)
            .await
            .unwrap();
        assert!(
            served.debug_has_serve_plan(),
            "{layout:?}: P is index-served"
        );
        assert_eq!(
            served.size().await.unwrap(),
            expected
                .iter()
                .filter(|q| q.1 == "<http://ex.org/p>")
                .count(),
            "{layout:?}"
        );
    }
}

// ─── The spill path ────────────────────────────────────────────────────

/// Seven distinct quads, three copies each, arranged so that no two copies
/// of a quad share a run of four: the repeats can only collapse in the merge.
async fn spread_repeats() -> (Vec<RawQuad>, BTreeSet<Row>) {
    let distinct_quads: Vec<RawQuad> = (0..7)
        .map(|i| RawQuad {
            s: format!("<http://ex.org/s{}>", (i * 3) % 7),
            p: format!("<http://ex.org/p{}>", i % 3),
            o: format!("\"o{}\"", i % 4),
            g: if i % 2 == 0 {
                String::new()
            } else {
                "<http://ex.org/g>".to_string()
            },
        })
        .collect();
    let mut stream = Vec::new();
    for _ in 0..3 {
        stream.extend(distinct_quads.iter().cloned());
    }
    let expected = distinct_quads.iter().map(row).collect();
    (stream, expected)
}

fn raw_vec_stream(
    quads: Vec<RawQuad>,
) -> impl futures::Stream<Item = crate::error::Result<RawQuad>> + Unpin + Send + 'static {
    stream::iter(quads.into_iter().map(Ok))
}

/// The row ids of an index child, which a deduplicated build numbers
/// `0..rows` with each exactly once — so the `(value, row id)` records are
/// unique because their ids are, and none describes a row that was dropped.
fn child_row_ids(component: &crate::store::indexes::IndexComponent) -> Vec<crate::store::RowId> {
    let mut ctx = crate::session::VORTEX_SESSION.create_execution_ctx();
    let rid: PrimitiveArray =
        crate::store::array::field_as(component.rows().unwrap(), "rid", &mut ctx).unwrap();
    rid.as_slice::<crate::store::RowId>().to_vec()
}

fn assert_children_describe_exactly(built: &BuiltArray, rows: usize, who: &str) {
    for component in &built.components {
        let mut ids = child_row_ids(component);
        assert_eq!(ids.len(), rows, "{who}: {} child rows", component.name);
        ids.sort_unstable();
        assert_eq!(
            ids,
            (0..rows as crate::store::RowId).collect::<Vec<_>>(),
            "{who}: {} child row ids are the deduplicated rows, once each",
            component.name
        );
    }
}

/// Copies of one quad that land in different spilled runs collapse in the
/// K-way merge, and the index children — spilled and merged on their own —
/// describe the deduplicated rows: one record per row, each row id once.
#[tokio::test]
async fn test_spilled_runs_collapse_repeats_across_runs() {
    let (stream, expected) = spread_repeats().await;
    assert_eq!(stream.len(), 21);
    for layout in LAYOUTS {
        for run_capacity in [2, 4, 5] {
            let who = format!("{layout:?}, run capacity {run_capacity}");
            let built = sorted_stream::build_array(
                Box::new(raw_vec_stream(stream.clone())),
                layout,
                ALL_INDEXES.to_vec(),
                run_capacity,
            )
            .await
            .unwrap();
            assert_eq!(built.array.len(), expected.len(), "{who}: primary rows");
            assert_eq!(built.components.len(), 4, "{who}");
            assert_children_describe_exactly(&built, expected.len(), &who);

            let store = VortexRdfStore::from_built(built).unwrap();
            let rows = tuple_rows(&store.quads_vec().await.unwrap());
            assert_eq!(rows.len(), expected.len(), "{who}");
            assert_eq!(rows.into_iter().collect::<BTreeSet<_>>(), expected, "{who}");
        }
    }
}

/// The lazy stream a file is written from cuts its chunks from the merged,
/// deduplicated output: seven distinct quads in runs of four are chunks of
/// four and three, and the dictionary holds the terms of the distinct quads.
#[tokio::test]
async fn test_spilled_chunk_stream_is_cut_from_deduplicated_rows() {
    let (stream, expected) = spread_repeats().await;
    for layout in LAYOUTS {
        for indexes in [vec![], ALL_INDEXES.to_vec()] {
            let who = format!("{layout:?} / {indexes:?}");
            let built = sorted_stream::build_chunk_stream(
                Box::new(raw_vec_stream(stream.clone())),
                layout,
                indexes,
                4,
                None,
            )
            .await
            .unwrap();
            let terms = built.dict.as_ref().map(|dict| dict.len());
            let chunks: Vec<_> = built.chunks.try_collect().await.unwrap();
            assert_eq!(
                chunks.iter().map(|c| c.len()).collect::<Vec<_>>(),
                [4, 3],
                "{who}"
            );
            if let Some(terms) = terms {
                let distinct_terms: BTreeSet<String> = expected
                    .iter()
                    .flat_map(|(s, p, o, g)| [s.clone(), p.clone(), o.clone(), g.clone()])
                    .collect();
                assert_eq!(terms, distinct_terms.len(), "{who}: dictionary terms");
            }
        }
    }
}

/// The in-memory sort drops the repeats too, with the index children built
/// over the deduplicated rows.
#[tokio::test]
async fn test_in_memory_sort_children_describe_deduplicated_rows() {
    let (stream, expected) = spread_repeats().await;
    for layout in LAYOUTS {
        let who = format!("{layout:?}");
        let built = build_array::<SortedInMemoryBuilder>(
            raw_vec_stream(stream.clone()),
            layout,
            ALL_INDEXES.to_vec(),
        )
        .await
        .unwrap();
        assert_eq!(built.array.len(), expected.len(), "{who}");
        assert_children_describe_exactly(&built, expected.len(), &who);
    }
}

// ─── Terms ─────────────────────────────────────────────────────────────

/// The probe's dictionary holds one code per RDF term — six: the subject,
/// the predicate, `"x"`, `"y"@en`, `"z"` and the default graph's empty
/// string — whichever spelling of a term the input used, and every spelling
/// of a term encodes to its one code.
#[tokio::test]
async fn test_probe_dictionary_holds_one_code_per_rdf_term() {
    let expected_terms = distinct_terms(PROBE).await;
    assert_eq!(
        expected_terms
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        [
            "",
            "\"x\"",
            "\"y\"@en",
            "\"z\"",
            "<http://ex.org/p>",
            "<http://ex.org/s>"
        ],
        "the parser rewrites every spelling to one"
    );

    // Each group holds spellings of one RDF term: stored form first.
    let spellings: [&[&str]; 6] = [
        &[
            "\"x\"",
            "\"x\"^^<http://www.w3.org/2001/XMLSchema#string>",
            "\"\\u0078\"",
        ],
        &["\"y\"@en", "\"y\"@EN"],
        &["\"z\""],
        &["<http://ex.org/s>", "http://ex.org/s"],
        &["<http://ex.org/p>", "http://ex.org/p"],
        &["", "default", "[]"],
    ];

    let mut stores = Vec::new();
    let layout = LayoutStrategy::Dictionary;
    for indexes in index_sets() {
        for builder in ["in memory", "streamed", "spilled"] {
            let store = match builder {
                "in memory" => VortexRdfStore::from_built(
                    build_array::<SortedInMemoryBuilder>(
                        raw_stream(PROBE),
                        layout,
                        indexes.clone(),
                    )
                    .await
                    .unwrap(),
                ),
                "streamed" => VortexRdfStore::from_built(
                    build_array::<SortedStreamBuilder>(raw_stream(PROBE), layout, indexes.clone())
                        .await
                        .unwrap(),
                ),
                _ => VortexRdfStore::from_built(
                    sorted_stream::build_array(
                        Box::new(raw_stream(PROBE)),
                        layout,
                        indexes.clone(),
                        2,
                    )
                    .await
                    .unwrap(),
                ),
            }
            .unwrap();
            stores.push((format!("{builder} / {indexes:?}"), store));
        }
    }
    // The file-backed dictionary answers through its reader too; the
    // directory outlives the assertions below.
    #[cfg(feature = "file-io")]
    let dir = tempfile::tempdir().unwrap();
    #[cfg(feature = "file-io")]
    {
        let path = dir.path().join("probe.vortex");
        crate::io::quads_stream_to_vortex_file(raw_stream(PROBE), &path, layout, vec![])
            .await
            .unwrap();
        let mapped = VortexRdfStore::from_file(&path).await.unwrap();
        assert!(mapped.dict_reader().unwrap().is_file_backed());
        stores.push(("file, mapped".to_string(), mapped));
    }

    for (who, store) in &stores {
        let dict = store.dict_reader().unwrap();
        assert_eq!(dict.len(), 6, "{who}: one code per RDF term");
        assert_eq!(store.size().await.unwrap(), 3, "{who}");

        let mut codes = BTreeSet::new();
        for group in spellings {
            let mut group_codes = BTreeSet::new();
            for spelling in group {
                let stored = canonical_spelling(spelling).unwrap();
                let code = dict.encode(&stored).await.unwrap();
                assert!(code.is_some(), "{who}: {spelling:?} is in the dictionary");
                group_codes.insert(code.unwrap());
            }
            assert_eq!(
                group_codes.len(),
                1,
                "{who}: {group:?} are one term, one code"
            );
            codes.extend(group_codes);
        }
        assert_eq!(codes.len(), 6, "{who}: six terms, six distinct codes");
    }
}

/// The other entry points that build from oxrdf values rather than text —
/// the `Quad` a Rust caller holds, and what the JS bindings decode from an
/// RDF/JS term — reach the builders through `RawQuad::from_quad`, which
/// renders one canonical spelling per RDF term: `xsd:string` typing dropped,
/// the language tag lower-cased.
#[test]
fn test_quads_from_oxrdf_values_render_one_spelling_per_term() {
    use oxrdf::vocab::xsd;
    let quad = |object: Literal| {
        RawQuad::from_quad(&Quad::new(
            NamedOrBlankNode::NamedNode(NamedNode::new("http://ex.org/s").unwrap()),
            NamedNode::new("http://ex.org/p").unwrap(),
            Term::Literal(object),
            GraphName::DefaultGraph,
        ))
    };
    let plain = quad(Literal::new_simple_literal("x"));
    assert_eq!(plain.o, "\"x\"");
    assert_eq!(
        row(&quad(Literal::new_typed_literal("x", xsd::STRING))),
        row(&plain)
    );
    assert_eq!(
        row(&quad(
            Literal::new_language_tagged_literal("y", "EN").unwrap()
        )),
        row(&quad(
            Literal::new_language_tagged_literal("y", "en").unwrap()
        ))
    );
    assert_eq!(
        quad(Literal::new_language_tagged_literal("y", "EN").unwrap()).o,
        "\"y\"@en"
    );
}

/// Builders intern the spelling they are given — re-parsing every term of
/// every build to canonicalize it is ruled out — so two hand-built quads that
/// spell one term two ways are two quads with two terms, and the same two
/// through [`RawQuad::canonical`] are one quad with one. Inputs to a build
/// come from the parser, [`RawQuad::from_quad`] or [`RawQuad::canonical`].
#[tokio::test]
async fn test_builders_intern_the_spelling_they_are_given() {
    let typed = "\"x\"^^<http://www.w3.org/2001/XMLSchema#string>";
    let by_hand = |o: &str| RawQuad {
        s: "<http://ex.org/s>".to_string(),
        p: "<http://ex.org/p>".to_string(),
        o: o.to_string(),
        g: String::new(),
    };
    let canonical =
        |o: &str| RawQuad::canonical("<http://ex.org/s>", "<http://ex.org/p>", o, "").unwrap();

    for layout in LAYOUTS {
        let who = format!("{layout:?}");
        let hand_built = build_array::<SortedInMemoryBuilder>(
            raw_vec_stream(vec![by_hand("\"x\""), by_hand(typed)]),
            layout,
            vec![],
        )
        .await
        .unwrap();
        assert_eq!(hand_built.array.len(), 2, "{who}: two spellings, two quads");

        let built = build_array::<SortedInMemoryBuilder>(
            raw_vec_stream(vec![canonical("\"x\""), canonical(typed)]),
            layout,
            vec![],
        )
        .await
        .unwrap();
        assert_eq!(built.array.len(), 1, "{who}: one RDF quad, one row");
        let store = VortexRdfStore::from_built(built).unwrap();
        assert_eq!(
            tuple_rows(&store.quads_vec().await.unwrap()).len(),
            1,
            "{who}"
        );
        if layout == LayoutStrategy::Dictionary {
            // The subject, the predicate, "x" and the default graph.
            assert_eq!(store.code_read_snapshot().unwrap().len(), 4);
        }
    }
}

// ─── Compaction ────────────────────────────────────────────────────────

fn quad_of(s: &str, p: &str, o: &str) -> Quad {
    make_quad(
        &format!("http://ex.org/{s}"),
        &format!("http://ex.org/{p}"),
        o,
        GraphName::DefaultGraph,
    )
}

/// Appends skip quads the store already holds, and compaction folds the tail
/// into the base: the compacted store, in memory and on disk, holds every
/// quad once.
#[tokio::test]
async fn test_compaction_after_appends_holds_each_quad_once() {
    let base: Vec<Quad> = vec![
        quad_of("s1", "p", "x"),
        quad_of("s2", "p", "y"),
        quad_of("s3", "q", "x"),
    ];
    // Two held already, one repeated inside the batch, two new.
    let appended: Vec<Quad> = vec![
        quad_of("s1", "p", "x"),
        quad_of("s4", "p", "z"),
        quad_of("s4", "p", "z"),
        quad_of("s2", "p", "y"),
        quad_of("s5", "q", "w"),
    ];
    let mut everything = base.clone();
    everything.extend(appended.iter().cloned());
    let expected: BTreeSet<Row> = tuple_rows(&everything).into_iter().collect();
    assert_eq!(expected.len(), 5);

    for layout in LAYOUTS {
        for indexes in index_sets() {
            let who = format!("{layout:?} / {indexes:?}");
            let built = build_array::<SortedInMemoryBuilder>(
                quad_stream(base.clone()),
                layout,
                indexes.clone(),
            )
            .await
            .unwrap();
            let store = VortexRdfStore::from_built(built).unwrap();
            let tailed = store.add_quads(appended.clone()).await.unwrap();
            assert_eq!(
                tailed.tail_len(),
                2,
                "{who}: only the new quads are appended"
            );
            let compacted = tailed.compact().await.unwrap();
            assert_eq!(compacted.tail_len(), 0, "{who}");
            assert_holds_each_quad_once_in(&compacted, &expected, &who).await;

            // And its serialization.
            #[cfg(feature = "file-io")]
            {
                let reread = VortexRdfStore::from_bytes(&tailed.to_bytes().await.unwrap())
                    .await
                    .unwrap();
                assert_holds_each_quad_once_in(&reread, &expected, &who).await;
            }
        }
    }

    #[cfg(feature = "file-io")]
    for layout in LAYOUTS {
        let who = format!("{layout:?} / file");
        let (_dir, path) = write_store_file(base.clone(), layout, ALL_INDEXES.to_vec()).await;
        let store = VortexRdfStore::from_file(&path).await.unwrap();
        let compacted = store
            .add_quads(appended.clone())
            .await
            .unwrap()
            .compact()
            .await
            .unwrap();
        assert_holds_each_quad_once_in(&compacted, &expected, &who).await;
        let reopened = VortexRdfStore::from_file(&path).await.unwrap();
        assert_holds_each_quad_once_in(&reopened, &expected, &who).await;
    }
}

/// Rows only (no pattern matrix): the store holds exactly `expected`, each
/// once.
async fn assert_holds_each_quad_once_in(
    store: &VortexRdfStore,
    expected: &BTreeSet<Row>,
    who: &str,
) {
    assert_eq!(store.size().await.unwrap(), expected.len(), "{who}: size");
    let rows = tuple_rows(&store.quads_vec().await.unwrap());
    assert_eq!(rows.len(), expected.len(), "{who}: rows read back");
    assert_eq!(
        rows.into_iter().collect::<BTreeSet<_>>(),
        *expected,
        "{who}"
    );
}

/// A store whose base was assembled without a builder — rows from a foreign
/// writer, repeats and all — comes out of compaction (which rebuilds through
/// the sorted builders) and out of a rebuilding serialization (a tail
/// appended to it) with each quad once.
#[tokio::test]
async fn test_rebuilds_drop_repeats_a_base_carries() {
    let with_repeats: Vec<Quad> = vec![
        quad_of("s2", "p", "y"),
        quad_of("s1", "p", "x"),
        quad_of("s2", "p", "y"),
        quad_of("s1", "p", "x"),
        quad_of("s3", "q", "x"),
        quad_of("s1", "p", "x"),
    ];
    let expected: BTreeSet<Row> = tuple_rows(&with_repeats).into_iter().collect();
    assert_eq!(expected.len(), 3);
    let store = unstamped_store(&with_repeats);
    assert_eq!(
        store.size().await.unwrap(),
        6,
        "the base carries its repeats"
    );

    for indexes in index_sets() {
        let who = format!("in memory / {indexes:?}");
        let compacted = store.compact_with_indexes(indexes.clone()).await.unwrap();
        assert_holds_each_quad_once_in(&compacted, &expected, &who).await;
        assert_eq!(compacted.indexes(), indexes.as_slice(), "{who}");
    }

    // A tail makes the serialization rebuild the rows: it drops them too.
    #[cfg(feature = "file-io")]
    {
        let tailed = store.add_quads([quad_of("s4", "p", "z")]).await.unwrap();
        let mut expected_tailed = expected.clone();
        expected_tailed.extend(tuple_rows(&[quad_of("s4", "p", "z")]));
        let reread = VortexRdfStore::from_bytes(&tailed.to_bytes().await.unwrap())
            .await
            .unwrap();
        assert_holds_each_quad_once_in(&reread, &expected_tailed, "tailed serialization").await;
    }

    #[cfg(feature = "file-io")]
    for indexes in index_sets() {
        let who = format!("file / {indexes:?}");
        let (_dir, path) = write_unsorted_store_file(&with_repeats, 0).await;
        let on_disk = VortexRdfStore::from_file(&path).await.unwrap();
        assert_eq!(
            on_disk.size().await.unwrap(),
            6,
            "{who}: the file carries its repeats"
        );
        let compacted = on_disk.compact_with_indexes(indexes.clone()).await.unwrap();
        assert_holds_each_quad_once_in(&compacted, &expected, &who).await;
        let reopened = VortexRdfStore::from_file(&path).await.unwrap();
        assert_holds_each_quad_once_in(&reopened, &expected, &who).await;
    }
}
