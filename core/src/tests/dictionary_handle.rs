//! The dictionary handle ([`DictReader`]) under both residencies, with the
//! spelling-tolerant encoder and the byte-order range probes it carries.

use super::*;
use crate::store::{CaseMap, DictReader, TermPredicate, TextOptions};
use oxrdf::{BlankNode, Literal};

/// Quads mixing every term kind the dictionary sorts: IRIs, blank nodes,
/// plain, language-tagged and typed literals, a named graph beside the
/// default graph.
fn mixed_kind_quads() -> Vec<Quad> {
    let p = |name: &str| NamedNode::new(format!("http://example.org/{name}")).unwrap();
    let s = |i: usize| NamedOrBlankNode::NamedNode(p(&format!("s{i:02}")));
    let g = NamedNode::new("http://example.org/g").unwrap();
    let mut quads = vec![
        Quad::new(
            s(0),
            p("name"),
            Term::Literal(Literal::new_language_tagged_literal_unchecked(
                "hallo", "de",
            )),
            GraphName::DefaultGraph,
        ),
        Quad::new(
            s(0),
            p("age"),
            Term::Literal(Literal::new_typed_literal("42", oxrdf::vocab::xsd::INTEGER)),
            GraphName::DefaultGraph,
        ),
        Quad::new(
            s(1),
            p("name"),
            Term::Literal(Literal::new_simple_literal("plain \"quoted\" value")),
            GraphName::NamedNode(g.clone()),
        ),
        Quad::new(
            s(1),
            p("knows"),
            Term::BlankNode(BlankNode::new("b1").unwrap()),
            GraphName::NamedNode(g),
        ),
        Quad::new(
            NamedOrBlankNode::BlankNode(BlankNode::new("b1").unwrap()),
            p("age"),
            Term::Literal(Literal::new_typed_literal(
                "3.5",
                oxrdf::vocab::xsd::DECIMAL,
            )),
            GraphName::DefaultGraph,
        ),
    ];
    quads.extend(modular_quads(12, 3, 4));
    quads
}

async fn memory_dictionary_store(quads: Vec<Quad>) -> VortexRdfStore {
    VortexRdfStore::from_quads(quad_stream(quads), LayoutStrategy::Dictionary, vec![])
        .await
        .unwrap()
}

/// Every term of the dictionary, in code order.
async fn all_terms(reader: &DictReader) -> Vec<String> {
    let codes: Vec<TermCode> = (0..reader.len() as TermCode).collect();
    reader
        .decode_many(&codes)
        .await
        .unwrap()
        .into_iter()
        .map(|term| term.expect("every code below len decodes"))
        .collect()
}

/// `lower_bound` by brute force over the sorted terms.
fn brute_lower_bound(terms: &[String], probe: &str) -> TermCode {
    terms.partition_point(|t| t.as_bytes() < probe.as_bytes()) as TermCode
}

/// The spellings every term is probed with: itself, byte-order neighbours
/// and the kind markers.
fn probe_spellings(terms: &[String]) -> Vec<String> {
    let mut probes: Vec<String> = vec![
        String::new(),
        "\"".into(),
        "\"object".into(),
        "<".into(),
        "<http://example.org/".into(),
        "<http://example.org/s".into(),
        "<http://example.org/s0".into(),
        "<http://example.org/zz".into(),
        "_:".into(),
        "_:b".into(),
        "~".into(),
    ];
    for term in terms {
        probes.push(term.clone());
        probes.push(format!("{term} "));
        if let Some(stripped) = term.strip_suffix('>') {
            probes.push(stripped.to_owned());
        }
    }
    probes
}

/// Spelling variants `encode` tolerates, each beside the stored form it must
/// resolve to.
fn tolerated_spellings() -> Vec<(&'static str, &'static str)> {
    vec![
        ("http://example.org/s00", "<http://example.org/s00>"),
        (
            "\"object 0\"^^<http://www.w3.org/2001/XMLSchema#string>",
            "\"object 0\"",
        ),
        ("\"hallo\"@DE", "\"hallo\"@de"),
        ("\"hallo\"@de", "\"hallo\"@de"),
        ("default", ""),
        ("[]", ""),
        ("", ""),
        (
            "\"42\"^^<http://www.w3.org/2001/XMLSchema#integer>",
            "\"42\"^^<http://www.w3.org/2001/XMLSchema#integer>",
        ),
        ("_:b1", "_:b1"),
        (
            "\"plain \\\"quoted\\\" value\"",
            "\"plain \\\"quoted\\\" value\"",
        ),
    ]
}

/// `decode_many_with` over `codes` collected by position, with how often each
/// position was visited.
async fn visited(reader: &DictReader, codes: &[TermCode]) -> (Vec<Option<String>>, Vec<usize>) {
    let mut terms = vec![None; codes.len()];
    let mut visits = vec![0usize; codes.len()];
    reader
        .decode_many_with(codes, |i, term| {
            visits[i] += 1;
            terms[i] = term.map(str::to_owned);
        })
        .await
        .unwrap();
    (terms, visits)
}

/// [`visited`] through the snapshot's synchronous visitor.
fn snapshot_visited(
    oracle: &DictSnapshot,
    codes: &[TermCode],
) -> (Vec<Option<String>>, Vec<usize>) {
    let mut terms = vec![None; codes.len()];
    let mut visits = vec![0usize; codes.len()];
    oracle.decode_many_with(codes, |i, term| {
        visits[i] += 1;
        terms[i] = term.map(str::to_owned);
    });
    (terms, visits)
}

/// The visitor delivers every position once with the term `decode_many`
/// returns for it — any order, repeats, out-of-range codes, empty — through
/// the reader and through the snapshot's synchronous twin.
async fn assert_visitor_matches_decode_many(reader: &DictReader, oracle: &DictSnapshot, tag: &str) {
    let len = reader.len() as TermCode;
    // A fixed pseudo-random batch: many repeats, a tenth out of range.
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    let scattered: Vec<TermCode> = (0..4 * len)
        .map(|_| {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (state >> 33) % (len + len / 10 + 1)
        })
        .collect();
    let batches: [Vec<TermCode>; 6] = [
        Vec::new(),
        vec![len],
        vec![len + 3, len, u64::MAX],
        vec![len - 1, 0, 3, 3, len, 1, len + 7, 1, 0],
        (0..len).rev().collect(),
        scattered,
    ];
    for batch in &batches {
        let want = reader.decode_many(batch).await.unwrap();
        let (terms, visits) = visited(reader, batch).await;
        assert_eq!(terms, want, "{tag}: visitor over {} codes", batch.len());
        assert!(visits.iter().all(|&n| n == 1), "{tag}: visits {visits:?}");
        let (terms, visits) = snapshot_visited(oracle, batch);
        assert_eq!(
            terms,
            want,
            "{tag}: snapshot visitor over {} codes",
            batch.len()
        );
        assert!(visits.iter().all(|&n| n == 1), "{tag}: snapshot visits");
    }
}

/// Every method of `reader` answers like the resident `DictSnapshot` of the
/// same dictionary (`oracle`), over every term and probe.
async fn assert_reader_matches_snapshot(reader: &DictReader, oracle: &DictSnapshot, tag: &str) {
    assert_eq!(reader.len(), oracle.len(), "{tag}: len");
    // A handle is empty only when it holds no term: the fixture's dictionary
    // holds some, so neither handle is (the empty store's are, in
    // `test_dictionary_empty_store_roundtrip`).
    assert_ne!(
        reader.len(),
        0,
        "{tag}: the fixture's dictionary holds terms"
    );
    assert!(!reader.is_empty(), "{tag}: is_empty");
    assert!(!oracle.is_empty(), "{tag}: snapshot is_empty");
    let terms = all_terms(reader).await;
    assert_eq!(terms.len(), oracle.len(), "{tag}: term count");
    for (code, term) in (0u64..).zip(&terms) {
        assert_eq!(
            oracle.decode(code).as_deref(),
            Some(term.as_str()),
            "{tag}: decode {code}"
        );
        assert_eq!(
            reader.decode(code).await.unwrap().as_deref(),
            Some(term.as_str()),
            "{tag}"
        );
        assert_eq!(
            reader.encode(term).await.unwrap(),
            Some(code),
            "{tag}: encode {term}"
        );
        assert_eq!(
            oracle.encode(term).unwrap(),
            Some(code),
            "{tag}: oracle encode {term}"
        );
    }
    // Out-of-range codes decode to `None`, singly and inside a batch of any
    // order with repeats.
    let len = reader.len() as TermCode;
    assert_eq!(reader.decode(len).await.unwrap(), None, "{tag}");
    let batch: Vec<TermCode> = [len, 3, 0, 3, len - 1, len + 7, 1, 1, 0]
        .into_iter()
        .filter(|&c| c == len || c == len + 7 || c < len)
        .collect();
    let want: Vec<Option<String>> = batch.iter().map(|&c| oracle.decode(c)).collect();
    assert_eq!(
        reader.decode_many(&batch).await.unwrap(),
        want,
        "{tag}: decode_many"
    );
    assert_eq!(
        oracle.decode_many(&batch),
        want,
        "{tag}: snapshot decode_many"
    );

    assert_visitor_matches_decode_many(reader, oracle, tag).await;

    // Byte-order probes: lower bounds and prefix ranges against brute force.
    let probes = probe_spellings(&terms);
    for probe in &probes {
        let want = brute_lower_bound(&terms, probe);
        assert_eq!(
            reader.lower_bound(probe).await.unwrap(),
            want,
            "{tag}: lower_bound {probe:?}"
        );
        assert_eq!(
            oracle.lower_bound(probe),
            want,
            "{tag}: snapshot lower_bound {probe:?}"
        );
        let lo = want;
        let hi = terms.partition_point(|t| {
            t.as_bytes() < probe.as_bytes() || t.as_bytes().starts_with(probe.as_bytes())
        }) as TermCode;
        assert_eq!(
            reader.prefix_range(probe).await.unwrap(),
            (lo, hi),
            "{tag}: prefix_range {probe:?}"
        );
        assert_eq!(
            oracle.prefix_range(probe),
            (lo, hi),
            "{tag}: snapshot prefix_range {probe:?}"
        );
    }
    // A batch encode over every term, variants and absent terms mixed in,
    // in order.
    let mut batch: Vec<String> = terms.clone();
    batch.extend(
        tolerated_spellings()
            .into_iter()
            .map(|(variant, _)| variant.to_owned()),
    );
    batch.push("http://example.org/absent".to_owned());
    batch.push("_:absent".to_owned());
    let batch_refs: Vec<&str> = batch.iter().map(String::as_str).collect();
    let want: Vec<Option<TermCode>> = batch_refs
        .iter()
        .map(|t| oracle.encode(t).unwrap())
        .collect();
    assert_eq!(
        reader.encode_many(&batch_refs).await.unwrap(),
        want,
        "{tag}: encode_many"
    );
    assert_eq!(
        oracle.encode_many(&batch_refs).unwrap(),
        want,
        "{tag}: snapshot encode_many"
    );
    assert!(
        reader
            .encode_many(&["<ok>", "\"unterminated"])
            .await
            .is_err(),
        "{tag}"
    );

    // Tolerated spellings resolve to the stored form's code.
    for (variant, stored) in tolerated_spellings() {
        let want = oracle.encode(stored).unwrap();
        assert_eq!(
            reader.encode(variant).await.unwrap(),
            want,
            "{tag}: tolerant {variant:?}"
        );
        assert_eq!(
            oracle.encode(variant).unwrap(),
            want,
            "{tag}: snapshot tolerant {variant:?}"
        );
    }
    for malformed in ["\"unterminated", "<no closing", "\"x\"^^", "\"x\"@"] {
        assert!(
            reader.encode(malformed).await.is_err(),
            "{tag}: {malformed:?} must be an error"
        );
        assert!(
            oracle.encode(malformed).is_err(),
            "{tag}: snapshot {malformed:?}"
        );
    }

    assert_eq!(
        reader.kind_ranges().await.unwrap(),
        oracle.kind_ranges(),
        "{tag}: kind_ranges"
    );
    for (kind, arg) in [
        ("is_literal", ""),
        ("is_iri", ""),
        ("is_blank", ""),
        ("datatype", "http://www.w3.org/2001/XMLSchema#string"),
        (
            "datatype",
            "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString",
        ),
        ("lang", "de"),
        ("lang_matches", "DE"),
        ("str_prefix", "object"),
        ("str_prefix", "http://example.org/s0"),
        ("num_lt", "10"),
        ("num_ne", "42"),
    ] {
        let predicate = TermPredicate::parse(kind, arg).unwrap();
        let all: Vec<TermCode> = (0..oracle.len() as TermCode).collect();
        let every_third: Vec<TermCode> = all.iter().copied().step_by(3).collect();
        for codes in [&all, &every_third] {
            assert_eq!(
                reader.filter_codes(&predicate, codes).await.unwrap(),
                oracle.filter_codes(&predicate, codes).unwrap(),
                "{tag}: filter_codes {kind} {arg:?}"
            );
        }
    }

    // The string kinds read a term's text as `string()` or as `STR()`, with
    // and without a case wrapper: the file-backed reader reads the windows
    // holding the candidates and answers as the resident snapshot does.
    for (kind, arg) in [
        ("str_prefix", "http://example.org/s0"),
        ("str_prefix", "object"),
        ("contains", "\"object\""),
        ("contains", "\"o\"@de"),
        ("strstarts", "\"hallo\"@de"),
        ("strends", "\"3\""),
        ("strends", "<http://example.org/s00>"),
        ("regex", "^o"),
        ("regex", "3$"),
        ("regex", r"\d"),
        ("regex", "(?=o)"),
    ] {
        for (as_str, case) in [
            (false, None),
            (true, None),
            (false, Some(CaseMap::Lower)),
            (true, Some(CaseMap::Upper)),
        ] {
            let options = TextOptions {
                as_str,
                case,
                ..TextOptions::default()
            };
            let predicate = TermPredicate::parse_with(kind, arg, &options).unwrap();
            let all: Vec<TermCode> = (0..oracle.len() as TermCode).collect();
            let every_third: Vec<TermCode> = all.iter().copied().step_by(3).collect();
            for codes in [&all, &every_third] {
                assert_eq!(
                    reader.filter_codes(&predicate, codes).await.unwrap(),
                    oracle.filter_codes(&predicate, codes).unwrap(),
                    "{tag}: filter_codes {kind} {arg:?} {options:?}"
                );
            }
        }
    }

    // Candidates must be ascending, unique and inside the dictionary, under
    // either residency, else invalid input; none at all is two empty answers.
    let is_iri = TermPredicate::parse("is_iri", "").unwrap();
    let len = oracle.len() as TermCode;
    for codes in [vec![3u64, 1], vec![1, 1], vec![0, len], vec![len + 7]] {
        assert!(
            matches!(
                reader.filter_codes(&is_iri, &codes).await,
                Err(crate::VortexRdfError::InvalidInput(_))
            ),
            "{tag}: filter_codes over {codes:?}"
        );
        assert!(
            matches!(
                oracle.filter_codes(&is_iri, &codes),
                Err(crate::VortexRdfError::InvalidInput(_))
            ),
            "{tag}: snapshot filter_codes over {codes:?}"
        );
    }
    let (passed, undecided) = reader.filter_codes(&is_iri, &[]).await.unwrap();
    assert!(passed.is_empty() && undecided.is_empty(), "{tag}");
}

/// A resident reader is the snapshot behind an async surface: every method
/// agrees with the snapshot's, and the snapshot is reachable from it.
#[tokio::test]
async fn test_dict_reader_resident_matches_snapshot() {
    let store = memory_dictionary_store(mixed_kind_quads()).await;
    let reader = store.dict_reader().expect("Dictionary layout, no tail");
    assert!(!reader.is_file_backed());
    let snapshot = reader
        .snapshot()
        .expect("resident readers carry a snapshot");
    assert_eq!(snapshot.len(), reader.len());
    assert_reader_matches_snapshot(&reader, &snapshot, "resident").await;
}

/// The handle is gated on the layout and the tail: never on a string layout,
/// and not while a tail holds quads outside the dictionary, though the
/// resident dictionary stays held; compaction restores it.
#[tokio::test]
async fn test_dict_reader_gates_on_layout_and_tail() {
    let strings = VortexRdfStore::from_quads(
        quad_stream(mixed_kind_quads()),
        LayoutStrategy::Default,
        vec![],
    )
    .await
    .unwrap();
    assert!(
        strings.dict_reader().is_none(),
        "string layouts have no dictionary"
    );

    let store = memory_dictionary_store(mixed_kind_quads()).await;
    let tailed = store
        .add_quad(make_quad(
            "http://example.org/new",
            "http://example.org/p0",
            "object 0",
            GraphName::DefaultGraph,
        ))
        .await
        .unwrap();
    assert_ne!(tailed.tail_len(), 0);
    assert!(
        tailed.debug_dictionary_snapshot().is_some(),
        "the resident dictionary stays held behind the closed gate"
    );
    assert!(tailed.dict_reader().is_none(), "a tail disables the handle");
    let compacted = tailed.compact().await.unwrap();
    let reader = compacted
        .dict_reader()
        .expect("compaction re-encodes the tail");
    assert!(
        reader
            .encode("http://example.org/new")
            .await
            .unwrap()
            .is_some()
    );
}

/// The kind ranges partition the codes in byte order — default graph,
/// literals, IRIs, blank nodes — and every term lands in its kind's range.
#[tokio::test]
async fn test_kind_ranges_partition_codes() {
    let store = memory_dictionary_store(mixed_kind_quads()).await;
    let reader = store.dict_reader().unwrap();
    let terms = all_terms(&reader).await;
    let kinds = reader.kind_ranges().await.unwrap();
    assert_eq!(kinds.start, 0);
    assert_eq!(kinds.len as usize, terms.len());
    assert_eq!(
        kinds.default_graph,
        Some(0),
        "default-graph quads put \"\" at code 0"
    );
    assert_eq!(
        kinds.gaps().collect::<Vec<_>>(),
        vec![0],
        "only the default graph is outside a kind"
    );
    assert_eq!(kinds.literals.start, 1);
    assert_eq!(kinds.literals.end, kinds.iris.start);
    assert_eq!(kinds.iris.end, kinds.blanks.start);
    assert_eq!(kinds.blanks.end, kinds.len);
    for (code, term) in (0u64..).zip(&terms) {
        let expected = match term.as_bytes().first() {
            None => None,
            Some(b'"') => Some(&kinds.literals),
            Some(b'<') => Some(&kinds.iris),
            Some(b'_') => Some(&kinds.blanks),
            other => panic!("unexpected leading byte {other:?} in {term:?}"),
        };
        match expected {
            None => assert_eq!(code, 0),
            Some(range) => assert!(
                range.contains(&code),
                "{term:?} at {code} outside {range:?}"
            ),
        }
    }
    // The kind ranges are the prefix ranges of the kind markers.
    assert_eq!(
        reader.prefix_range("\"").await.unwrap(),
        (kinds.literals.start, kinds.literals.end)
    );
    assert_eq!(
        reader.prefix_range("<").await.unwrap(),
        (kinds.iris.start, kinds.iris.end)
    );
    assert_eq!(
        reader.prefix_range("_:").await.unwrap(),
        (kinds.blanks.start, kinds.blanks.end)
    );
    assert_eq!(reader.prefix_range("").await.unwrap(), (0, kinds.len));
    // A namespace is one range: exactly the subjects.
    let (lo, hi) = reader.prefix_range("<http://example.org/s").await.unwrap();
    let subjects: Vec<&String> = terms
        .iter()
        .filter(|t| t.starts_with("<http://example.org/s"))
        .collect();
    assert_eq!((hi - lo) as usize, subjects.len());
    assert_eq!(
        &terms[lo as usize..hi as usize],
        subjects.into_iter().cloned().collect::<Vec<_>>().as_slice()
    );
}

/// A variant spelling resolves the same before and after its stored form was
/// looked up, and absent terms stay absent either way.
#[tokio::test]
async fn test_encode_variant_spellings_agree_with_the_stored_form() {
    let store = memory_dictionary_store(mixed_kind_quads()).await;
    let snapshot = store.dict_reader().unwrap().snapshot().unwrap();
    for (variant, stored) in tolerated_spellings() {
        let first = snapshot.encode(variant).unwrap();
        let stored_code = snapshot.encode(stored).unwrap();
        let again = snapshot.encode(variant).unwrap();
        assert_eq!(first, stored_code, "{variant:?}");
        assert_eq!(again, stored_code, "{variant:?}");
        assert!(stored_code.is_some(), "{stored:?} is in the fixture");
    }
    assert_eq!(snapshot.encode("http://example.org/absent").unwrap(), None);
    assert_eq!(snapshot.encode("\"absent\"@fr").unwrap(), None);
    assert_eq!(snapshot.encode("_:absent").unwrap(), None);
}

/// A file-backed reader answers every method like the resident open of the
/// same file, and the codes a file-backed view gathers decode through it.
#[tokio::test]
async fn test_dict_reader_file_backed_matches_resident() {
    let quads = mixed_kind_quads();
    let (_dir, path) = write_store_file(
        quads.clone(),
        LayoutStrategy::Dictionary,
        vec![IndexType::SecondaryByCopy],
    )
    .await;
    let resident = VortexRdfStore::from_file_in_memory(&path).await.unwrap();
    let fb = VortexRdfStore::from_file(&path).await.unwrap();
    let reader = fb
        .dict_reader()
        .expect("file-backed dictionaries get a handle");
    assert!(reader.is_file_backed());

    let oracle = resident.dict_reader().unwrap().snapshot().unwrap();
    assert!(
        reader.snapshot().is_none(),
        "the sync snapshot stays resident-only"
    );
    assert_reader_matches_snapshot(&reader, &oracle, "file-backed").await;
    // The kind ranges stay stable on re-ask, and a predicate over the
    // candidates answers the same each time (nothing is memoized).
    assert_eq!(reader.kind_ranges().await.unwrap(), oracle.kind_ranges());
    let literal = TermPredicate::parse("is_literal", "").unwrap();
    let all: Vec<TermCode> = (0..oracle.len() as TermCode).collect();
    assert_eq!(
        reader.filter_codes(&literal, &all).await.unwrap(),
        oracle.filter_codes(&literal, &all).unwrap()
    );

    // A served and a scanned view's gathered codes decode through the
    // file-backed handle to the quads the view holds.
    let p0 = NamedNode::new("http://example.org/p0").unwrap();
    let name = NamedNode::new("http://example.org/name").unwrap();
    for (tag, view) in [
        (
            "served",
            matched_view(&fb, None, Some(&p0), None, None).await,
        ),
        (
            "scanned",
            matched_view(&fb, None, Some(&name), None, None).await,
        ),
        ("whole", fb.clone()),
    ] {
        let columns = view
            .code_columns_gathered()
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("{tag}: codes are the store's vocabulary"));
        let mut got = decoded_rows(&columns, &reader).await;
        got.sort();
        let held = view.quads_vec().await.unwrap();
        assert_eq!(got, expected_rows(&held, |_| true), "{tag}");
    }
}
