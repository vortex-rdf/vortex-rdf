//! The native term and code streams against the stores' one-shot reads (which tests/engine.rs holds to a naive
//! scan of the quads the stores were built from, in base order but a pattern the copy index serves in its order),
//! row for row, on every build, chunk size and payload limit; the empty stream of a pattern no quad can match, the
//! errors a caller can cause, and a stream that outlives its store's handle.

mod common;

use common::{
    JAVA_LIMITS, Quad, as_refs, builds, drain, patterns, quads as make_quads, reference_quads,
    scan, write,
};
use vortex_rdf_core::{CodePattern, LayoutStrategy};
use vortex_rdf_jni::engine::Store;
use vortex_rdf_jni::payload::{Limits, ROW_BYTES};
use vortex_rdf_jni::streams::{CodeStream, TermStream};

fn quads() -> Vec<Quad> {
    make_quads(400)
}

/// Every row a code stream writes, `rows` at a time, as its four codes.
fn drain_codes(mut stream: CodeStream, rows: usize) -> Vec<[u64; 4]> {
    let mut out = vec![[0u8; ROW_BYTES]; rows];
    let mut codes = Vec::new();
    loop {
        let n = stream.next(&mut out).unwrap();
        assert!(n <= rows);
        if n == 0 {
            break;
        }
        for row in &out[..n] {
            codes.push(std::array::from_fn(|i| {
                u64::from_ne_bytes(row.as_chunks::<8>().0[i])
            }));
        }
    }
    assert_eq!(
        stream.next(&mut out).unwrap(),
        0,
        "an ended stream stays ended"
    );
    codes
}

#[test]
fn term_streams_yield_the_match_in_read_order() {
    let quads = quads();
    let dir = tempfile::tempdir().unwrap();
    for built in builds() {
        let path = write(dir.path(), &built, &quads);
        for in_memory in [false, true] {
            let store = Store::open(&path, in_memory).unwrap();
            for pattern in patterns(&quads) {
                let r = as_refs(&pattern);
                let want = reference_quads(&path, in_memory, r);
                for batch_rows in [1, 7, 65_536] {
                    // One row a chunk is a pull per row: small matches hold the case.
                    if batch_rows == 1 && want.len() > 64 {
                        continue;
                    }
                    for limits in [JAVA_LIMITS, Limits::new(3, 128, 32).unwrap()] {
                        let tag = format!(
                            "{} in_memory={in_memory} {pattern:?} batch {batch_rows} {limits:?}",
                            built.name
                        );
                        let stream = store
                            .term_stream(r[0], r[1], r[2], r[3], batch_rows, limits)
                            .unwrap();
                        assert_eq!(drain(stream, limits), want, "{tag}");
                    }
                }
            }
        }
    }
}

#[test]
fn code_streams_yield_the_match_in_read_order() {
    let quads = quads();
    let dir = tempfile::tempdir().unwrap();
    for built in builds()
        .into_iter()
        .filter(|b| b.layout == LayoutStrategy::Dictionary)
    {
        let path = write(dir.path(), &built, &quads);
        for in_memory in [false, true] {
            let store = Store::open(&path, in_memory).unwrap();
            let terms = store.term_count().unwrap();
            for pattern in patterns(&quads) {
                let r = as_refs(&pattern);
                let want = reference_quads(&path, in_memory, r);
                // An absent term: a code past the dictionary.
                let code = |i: usize| r[i].map(|t| store.encode(t).unwrap().unwrap_or(terms + 7));
                let code_pattern = CodePattern::new(code(0), code(1), code(2), code(3));
                for batch_rows in [7, 65_536] {
                    for rows in [1, 5, 4_096] {
                        let tag = format!(
                            "{} in_memory={in_memory} {pattern:?} batch {batch_rows} out {rows}",
                            built.name
                        );
                        let stream = store.code_stream(code_pattern, batch_rows).unwrap();
                        let spelled: Vec<Quad> = drain_codes(stream, rows)
                            .iter()
                            .map(|codes| {
                                codes.map(|c| {
                                    store.decode(c).unwrap().expect("a stored code decodes")
                                })
                            })
                            .collect();
                        assert_eq!(spelled, want, "{tag}");
                    }
                }
            }
        }
    }
}

#[test]
fn code_streams_of_impossible_codes_are_empty() {
    let quads = quads();
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&write(dir.path(), &builds()[0], &quads), false).unwrap();
    let literal = store
        .encode(&quads.iter().find(|q| q[2].starts_with("\"plain")).unwrap()[2])
        .unwrap()
        .unwrap();
    let past = store.term_count().unwrap() + 5;
    for pattern in [
        CodePattern::new(Some(literal), None, None, None),
        CodePattern::new(None, None, Some(past), None),
    ] {
        let mut stream = store.code_stream(pattern, 7).unwrap();
        let mut out = [[0u8; ROW_BYTES]; 4];
        assert_eq!(stream.next(&mut out).unwrap(), 0, "{pattern:?}");
    }
}

#[test]
fn streams_refuse_what_their_store_cannot_serve() {
    let quads = quads();
    let dir = tempfile::tempdir().unwrap();
    let flat = Store::open(&write(dir.path(), &builds()[3], &quads), false).unwrap();
    let error = flat
        .code_stream(CodePattern::default(), 7)
        .err()
        .expect("no code stream without a dictionary")
        .to_string();
    assert!(error.contains("no term dictionary"), "{error}");
    let store = Store::open(&write(dir.path(), &builds()[0], &quads), false).unwrap();
    let error = store
        .term_stream(None, Some("not a term"), None, None, 7, JAVA_LIMITS)
        .err()
        .expect("a malformed predicate")
        .to_string();
    assert!(error.contains("pattern predicate"), "{error}");
    assert!(
        store
            .term_stream(None, None, None, None, 0, JAVA_LIMITS)
            .is_err()
    );
    assert!(store.code_stream(CodePattern::default(), 0).is_err());
}

#[test]
fn streams_outlive_their_store_handle() {
    let quads = quads();
    let want: Vec<Quad> = scan(&quads, &[None; 4]).into_iter().collect();
    let dir = tempfile::tempdir().unwrap();
    for built in builds() {
        let path = write(dir.path(), &built, &quads);
        let store = Store::open(&path, false).unwrap();
        let terms = store
            .term_stream(None, None, None, None, 64, JAVA_LIMITS)
            .unwrap();
        let codes = (built.layout == LayoutStrategy::Dictionary)
            .then(|| store.code_stream(CodePattern::default(), 64).unwrap());
        drop(store);
        assert_eq!(drain(terms, JAVA_LIMITS), want, "{}", built.name);
        if let Some(codes) = codes {
            let reader = Store::open(&path, false).unwrap();
            let spelled: Vec<Quad> = drain_codes(codes, 100)
                .iter()
                .map(|codes| codes.map(|c| reader.decode(c).unwrap().unwrap()))
                .collect();
            assert_eq!(spelled, want, "{} codes", built.name);
        }
    }
}

#[test]
fn streams_can_cross_threads() {
    fn send<T: Send>() {}
    send::<TermStream>();
    send::<CodeStream>();
}
