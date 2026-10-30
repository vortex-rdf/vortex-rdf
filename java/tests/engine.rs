//! The engine against a naive scan of the quads it was built from: every pattern shape, by terms and by codes,
//! on each way a store can be built, with windows, absent terms and the errors a caller can cause.

mod common;

use std::collections::BTreeSet;

use common::{Built, Quad, builds, drain, quads as make_quads, reference_quads, write};
use vortex_rdf_core::{CodePattern, LayoutStrategy};
use vortex_rdf_jni::cursor::QuadCursor;
use vortex_rdf_jni::engine::{ROW_BYTES, Store};
use vortex_rdf_jni::payload::Limits;

fn quads() -> Vec<Quad> {
    make_quads(400)
}

/// The quads matching a pattern of spellings, by scan.
fn scan(quads: &[Quad], pattern: &[Option<&str>; 4]) -> BTreeSet<Quad> {
    quads
        .iter()
        .filter(|q| {
            pattern
                .iter()
                .zip(q.iter())
                .all(|(want, have)| want.is_none_or(|w| w == have))
        })
        .cloned()
        .collect()
}

/// Patterns: every subset of positions bound to a stored quad's own terms, for a spread of stored quads, plus
/// a pattern per position bound to a well-formed term no quad holds.
fn patterns(quads: &[Quad]) -> Vec<[Option<String>; 4]> {
    let mut out = Vec::new();
    for quad in quads.iter().step_by(29) {
        for mask in 0..16u8 {
            out.push(std::array::from_fn(|i| {
                (mask & (1 << i) != 0).then(|| quad[i].clone())
            }));
        }
    }
    let absent = [
        "<http://example.org/absent>",
        "<http://example.org/absent>",
        "<http://example.org/absent>",
        "<http://example.org/absent>",
    ];
    for i in 0..4 {
        out.push(std::array::from_fn(|j| {
            (i == j).then(|| absent[j].to_string())
        }));
    }
    out
}

fn as_refs(pattern: &[Option<String>; 4]) -> [Option<&str>; 4] {
    std::array::from_fn(|i| pattern[i].as_deref())
}

/// The code at `position` of a row [`Store::match_codes`] wrote.
fn code_at(row: &[u8; ROW_BYTES], position: usize) -> u64 {
    u64::from_ne_bytes(row.as_chunks::<8>().0[position])
}

#[test]
fn term_patterns_match_a_scan_on_every_layout() {
    let quads = quads();
    let dir = tempfile::tempdir().unwrap();
    for built in builds() {
        let path = write(dir.path(), &built, &quads);
        for in_memory in [false, true] {
            let store = Store::open(&path, in_memory)
                .unwrap_or_else(|e| panic!("{}: open: {e}", built.name));
            let tag = format!("{} in_memory={in_memory}", built.name);
            assert_eq!(store.size(), quads.len() as u64, "{tag}: size");
            assert_eq!(
                store.has_dictionary(),
                built.layout == LayoutStrategy::Dictionary,
                "{tag}: dictionary"
            );
            for pattern in patterns(&quads) {
                let r = as_refs(&pattern);
                let want = scan(&quads, &r);
                let tag = format!("{tag} {pattern:?}");
                assert_eq!(
                    store.count(r[0], r[1], r[2], r[3]).unwrap(),
                    want.len() as u64,
                    "{tag}: count"
                );
                let all = store.quads(r[0], r[1], r[2], r[3], 0, None).unwrap();
                assert_eq!(
                    all.iter().cloned().collect::<BTreeSet<_>>(),
                    want,
                    "{tag}: quads"
                );
                assert_eq!(all.len(), want.len(), "{tag}: no duplicate rows");
                for page in [1usize, 7]
                    .into_iter()
                    .filter(|&page| page == 7 || want.len() < 10)
                {
                    let mut seen = Vec::new();
                    let mut offset = 0u64;
                    while (offset as usize) < want.len() {
                        let rows = store
                            .quads(r[0], r[1], r[2], r[3], offset, Some(page))
                            .unwrap();
                        assert_eq!(
                            rows.len(),
                            page.min(want.len() - offset as usize),
                            "{tag}: page {page} at {offset}"
                        );
                        seen.extend(rows);
                        offset += page as u64;
                    }
                    assert_eq!(
                        seen.into_iter().collect::<BTreeSet<_>>(),
                        want,
                        "{tag}: pages of {page}"
                    );
                }
            }
        }
    }
}

/// The per-term read the tests compare the packed transfer against is itself right: on every build, both
/// residencies and every pattern it gives the scan's rows, once each.
#[test]
fn reference_reads_the_scan_on_every_build() {
    let quads = quads();
    // The oracle carries the hard spellings: an astral character only they hold.
    assert!(quads.iter().any(|q| q[2].contains('\u{10348}')));
    let dir = tempfile::tempdir().unwrap();
    let all = builds();
    assert_eq!(all.len(), 5, "five builds");
    for built in all {
        let path = write(dir.path(), &built, &quads);
        if built.name == "typed" {
            assert_eq!(Store::open(&path, false).unwrap().layout(), "typed-object");
        }
        for in_memory in [false, true] {
            for pattern in patterns(&quads) {
                let r = as_refs(&pattern);
                let want = scan(&quads, &r);
                let tag = format!("{} in_memory={in_memory} {pattern:?}", built.name);
                let rows = reference_quads(&path, in_memory, r, 0, None);
                assert_eq!(rows.len(), want.len(), "{tag}: no duplicate rows");
                assert_eq!(rows.into_iter().collect::<BTreeSet<_>>(), want, "{tag}");
            }
        }
    }
}

/// The windows the cursor tests read: whole, inside, and a point read.
const WINDOWS: [(u64, Option<usize>); 3] = [(0, None), (3, Some(7)), (0, Some(1))];

/// The cursor's rows against the per-term read's, row for row, on every build, residency, pattern and window,
/// under each of `limits`.
fn cursor_matches_the_reference(limits: &[Limits]) {
    let quads = quads();
    let dir = tempfile::tempdir().unwrap();
    for built in builds() {
        let path = write(dir.path(), &built, &quads);
        for in_memory in [false, true] {
            let store = Store::open(&path, in_memory).unwrap();
            for pattern in patterns(&quads) {
                let r = as_refs(&pattern);
                for (offset, limit) in WINDOWS {
                    let want = reference_quads(&path, in_memory, r, offset, limit);
                    for &limits in limits {
                        let tag = format!(
                            "{} in_memory={in_memory} {pattern:?} window ({offset}, {limit:?}) {limits:?}",
                            built.name
                        );
                        let cursor = store
                            .quads_cursor(r[0], r[1], r[2], r[3], offset, limit, limits)
                            .unwrap();
                        assert_eq!(cursor.rows(), want.len(), "{tag}: rows");
                        assert_eq!(drain(cursor), want, "{tag}");
                    }
                }
            }
        }
    }
}

#[test]
fn cursor_reads_equal_the_reference_row_for_row() {
    cursor_matches_the_reference(&[Limits::DEFAULT]);
}

#[test]
fn cursor_reads_survive_every_chunk_cut() {
    cursor_matches_the_reference(&[
        Limits::new(1, 64, 16).unwrap(),
        Limits::new(7, 256, 64).unwrap(),
    ]);
}

#[test]
fn cursors_can_cross_threads() {
    fn send<T: Send>() {}
    send::<QuadCursor>();
}

/// After a chunk the byte budget cut, the next one offers the rows that fit, growing back at most twofold a
/// chunk: rows of long literals followed by short ones never jump from a few rows a chunk to many.
#[test]
fn cursor_paces_chunks_by_the_last_fit() {
    let long = "x".repeat(200);
    let quads: Vec<Quad> = (0..40)
        .map(|i| format!("\"{long} {i}\""))
        .chain((0..400).map(|i| format!("\"{i}\"")))
        .enumerate()
        .map(|(i, o)| {
            [
                format!("<http://example.org/s{i:03}>"),
                "<http://example.org/p>".to_string(),
                o,
                String::new(),
            ]
        })
        .collect();
    let dir = tempfile::tempdir().unwrap();
    let limits = Limits::new(1024, 2048, 512).unwrap();
    for (name, layout) in [
        ("dictionary", LayoutStrategy::Dictionary),
        ("default", LayoutStrategy::Default),
    ] {
        let built = Built {
            name,
            layout,
            indexes: vec![],
        };
        let store = Store::open(&write(dir.path(), &built, &quads), false).unwrap();
        let cursor = store
            .quads_cursor(None, None, None, None, 0, None, limits)
            .unwrap();
        let mut held = Vec::new();
        for payload in cursor {
            let payload = payload.unwrap();
            held.push(payload.positions() / 4);
        }
        assert_eq!(held.iter().sum::<usize>(), quads.len(), "{name}");
        assert!(held[0] < 10, "{name}: long rows cut early: {held:?}");
        for pair in held.windows(2) {
            assert!(pair[1] <= 2 * pair[0], "{name}: {held:?}");
        }
        assert!(
            held.iter().any(|&n| n > 20),
            "{name}: short rows grow back: {held:?}"
        );
    }
}

#[test]
fn decode_payload_matches_decode() {
    let quads = quads();
    let dir = tempfile::tempdir().unwrap();
    for built in builds()
        .into_iter()
        .filter(|b| b.layout == LayoutStrategy::Dictionary)
    {
        let path = write(dir.path(), &built, &quads);
        for in_memory in [false, true] {
            let store = Store::open(&path, in_memory).unwrap();
            let tag = format!("{} in_memory={in_memory}", built.name);
            let distinct: BTreeSet<&String> = quads.iter().flat_map(|q| q.iter()).collect();
            let mut codes: Vec<u64> = distinct
                .iter()
                .map(|t| store.encode(t).unwrap().unwrap())
                .collect();
            codes.extend([u64::MAX, codes[0], codes[3], codes[0]]);
            let check = |from: usize, payload: &vortex_rdf_jni::payload::Payload, n: usize| {
                for i in 0..n {
                    let want = store.decode(codes[from + i]).unwrap();
                    assert_eq!(payload.term(i), want.as_deref(), "{tag}: code {}", from + i);
                }
            };
            let (payload, n) = store.decode_payload(&codes, Limits::DEFAULT).unwrap();
            assert_eq!(n, codes.len(), "{tag}: one call");
            check(0, &payload, n);
            let three = Limits::new(3, 16 << 20, 1 << 20).unwrap();
            let mut from = 0;
            while from < codes.len() {
                let (payload, n) = store.decode_payload(&codes[from..], three).unwrap();
                assert_eq!(n, 3.min(codes.len() - from), "{tag}: three a call");
                check(from, &payload, n);
                from += n;
            }
        }
    }
}

#[test]
fn term_patterns_report_what_a_caller_can_get_wrong() {
    let quads = quads();
    let dir = tempfile::tempdir().unwrap();
    let path = write(dir.path(), &builds()[0], &quads);
    let store = Store::open(&path, false).unwrap();
    // A literal subject cannot be parsed as a pattern: an error naming the position, not an empty answer.
    let error = store
        .count(Some("\"literal\""), None, None, None)
        .unwrap_err()
        .to_string();
    assert!(error.contains("pattern subject"), "{error}");
    let error = store
        .quads(None, Some("not a term"), None, None, 0, None)
        .unwrap_err()
        .to_string();
    assert!(error.contains("pattern predicate"), "{error}");
    assert!(Store::open(dir.path().join("missing.vortex").to_str().unwrap(), false).is_err());
    // The default graph is `""`, a free graph is `None`.
    let default_graph = quads.iter().filter(|q| q[3].is_empty()).count() as u64;
    assert!(default_graph > 0 && default_graph < quads.len() as u64);
    assert_eq!(
        store.count(None, None, None, Some("")).unwrap(),
        default_graph
    );
    assert_eq!(
        store.count(None, None, None, None).unwrap(),
        quads.len() as u64
    );
}

#[test]
fn code_patterns_match_a_scan_on_dictionary_stores() {
    let quads = quads();
    let dir = tempfile::tempdir().unwrap();
    for built in builds()
        .into_iter()
        .filter(|b| b.layout == LayoutStrategy::Dictionary)
    {
        let path = write(dir.path(), &built, &quads);
        for in_memory in [false, true] {
            let store = Store::open(&path, in_memory).unwrap();
            let tag = format!("{} in_memory={in_memory}", built.name);
            let terms = store.term_count().unwrap();
            let ranges = store.ranges().unwrap();
            let default_graph = store.encode("").unwrap();
            assert_eq!(
                ranges.default_graph, default_graph,
                "{tag}: default graph code"
            );
            // The kind ranges and the default graph's name tile the codes.
            let kinds: u64 = [&ranges.literals, &ranges.iris, &ranges.blanks]
                .iter()
                .map(|r| r.end - r.start)
                .sum();
            assert_eq!(
                kinds + u64::from(default_graph.is_some()),
                terms,
                "{tag}: ranges tile the dictionary"
            );

            // encode / decode round trip over every distinct stored term.
            let distinct: BTreeSet<&String> = quads.iter().flat_map(|q| q.iter()).collect();
            let codes: Vec<u64> = distinct
                .iter()
                .map(|t| {
                    store
                        .encode(t)
                        .unwrap()
                        .unwrap_or_else(|| panic!("{tag}: {t} has no code"))
                })
                .collect();
            let decoded = store.decode_many(&codes).unwrap();
            for (term, back) in distinct.iter().zip(&decoded) {
                assert_eq!(back.as_deref(), Some(term.as_str()), "{tag}: round trip");
            }
            assert_eq!(
                store.decode(codes[0]).unwrap().as_deref(),
                Some(distinct.iter().next().unwrap().as_str())
            );
            assert_eq!(store.encode("<http://example.org/absent>").unwrap(), None);
            assert_eq!(store.decode(terms + 5).unwrap(), None);

            for pattern in patterns(&quads) {
                let r = as_refs(&pattern);
                let want = scan(&quads, &r);
                // An absent term: a code past the dictionary.
                let code = |i: usize| r[i].map(|t| store.encode(t).unwrap().unwrap_or(terms + 7));
                let code_pattern = CodePattern::new(code(0), code(1), code(2), code(3));
                let tag = format!("{tag} {pattern:?}");
                assert_eq!(
                    store.count_codes(code_pattern).unwrap(),
                    want.len() as u64,
                    "{tag}: count_codes"
                );
                let mut seen = BTreeSet::new();
                let mut offset = 0u64;
                let page = 5usize;
                let mut out = vec![[0u8; ROW_BYTES]; page];
                loop {
                    let n = store.match_codes(code_pattern, offset, &mut out).unwrap();
                    for row in &out[..n] {
                        let spelled: Quad = std::array::from_fn(|i| {
                            let code = code_at(row, i);
                            store.decode(code).unwrap().expect("a stored code decodes")
                        });
                        assert!(seen.insert(spelled), "{tag}: repeated row");
                    }
                    if n < page {
                        break;
                    }
                    offset += page as u64;
                }
                assert_eq!(seen, want, "{tag}: match_codes pages");
            }
        }
    }
}

#[test]
fn code_patterns_that_cannot_match_are_empty_not_errors() {
    let quads = quads();
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&write(dir.path(), &builds()[0], &quads), false).unwrap();
    // A literal's code in the subject position, a blank node's in the predicate position.
    let literal = store
        .encode(&quads.iter().find(|q| q[2].starts_with("\"plain")).unwrap()[2])
        .unwrap()
        .unwrap();
    let blank = store.encode("_:b0").unwrap().unwrap();
    let mut out = [[0u8; ROW_BYTES]; 2];
    for pattern in [
        CodePattern::new(Some(literal), None, None, None),
        CodePattern::new(None, Some(blank), None, None),
    ] {
        assert_eq!(store.count_codes(pattern).unwrap(), 0);
        assert_eq!(store.match_codes(pattern, 0, &mut out).unwrap(), 0);
    }
}

#[test]
fn a_store_without_a_dictionary_refuses_code_calls_only() {
    let quads = quads();
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&write(dir.path(), &builds()[3], &quads), false).unwrap();
    assert!(!store.has_dictionary());
    assert_eq!(store.layout(), "default");
    for result in [
        store.term_count().map(|_| ()),
        store.encode("<http://example.org/s1>").map(|_| ()),
        store.count_codes(CodePattern::default()).map(|_| ()),
        store.decode_payload(&[0], Limits::DEFAULT).map(|_| ()),
    ] {
        let error = result.unwrap_err().to_string();
        assert!(error.contains("no term dictionary"), "{error}");
    }
    assert_eq!(
        store.count(None, None, None, None).unwrap(),
        quads.len() as u64
    );
}

#[test]
fn indexes_and_layout_are_reported() {
    let quads = quads();
    let dir = tempfile::tempdir().unwrap();
    let all = builds();
    let plain = Store::open(&write(dir.path(), &all[0], &quads), false).unwrap();
    assert_eq!(
        (plain.layout().as_str(), plain.indexes()),
        ("dictionary", vec![])
    );
    let copy = Store::open(&write(dir.path(), &all[1], &quads), false).unwrap();
    assert_eq!(copy.indexes(), vec!["secondary-by-copy".to_string()]);
    let reference = Store::open(&write(dir.path(), &all[2], &quads), false).unwrap();
    assert_eq!(
        reference.indexes(),
        vec!["secondary-by-reference".to_string()]
    );
}
