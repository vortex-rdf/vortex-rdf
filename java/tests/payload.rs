//! The payload encoder against the dictionary and the matched rows it encodes: every position decodes to its
//! term in order, repeats share a slot, and the limits cut chunks where they say, huge terms included.

mod common;

use std::path::Path;
use std::sync::Arc;

use common::{Quad, builds, quads, write};
use vortex_rdf_core::{DictReader, LayoutStrategy, Probe, SharedQuad, TermCode, VortexRdfStore};
use vortex_rdf_jni::payload::{Limits, Pace, Payload, encode_codes, encode_lookup, encode_shared};

fn block_on<F: Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(future)
}

/// The stores of the fixture builds with `layout`, written into `dir`, each opened in place and loaded whole:
/// a file-backed dictionary visits codes in code order, a resident one in the order given.
fn stores(dir: &Path, layout: LayoutStrategy) -> Vec<(String, VortexRdfStore)> {
    let quads = quads(400);
    let mut out = Vec::new();
    for built in builds().into_iter().filter(|b| b.layout == layout) {
        let path = write(dir, &built, &quads);
        for in_memory in [false, true] {
            let store = block_on(async {
                if in_memory {
                    VortexRdfStore::from_file_in_memory(&path).await
                } else {
                    VortexRdfStore::from_file(&path).await
                }
            })
            .unwrap();
            out.push((format!("{} in_memory={in_memory}", built.name), store));
        }
    }
    out
}

/// The code columns a pattern matches.
async fn columns(store: &VortexRdfStore, pattern: [Option<&str>; 4]) -> [Vec<TermCode>; 4] {
    let [s, p, o, g] = pattern;
    let view = store
        .run_probe(&Probe::parse(s, p, o, g).unwrap())
        .await
        .unwrap();
    let columns = view
        .code_columns_gathered()
        .await
        .unwrap()
        .expect("a code view");
    columns.map(|column| column.to_vec())
}

/// The rows of the columns, each code decoded alone.
async fn decoded(dict: &DictReader, columns: &[Vec<TermCode>; 4]) -> Vec<Quad> {
    let mut spelled = Vec::new();
    for column in columns {
        spelled.push(dict.decode_many(column).await.unwrap());
    }
    (0..columns[0].len())
        .map(|row| std::array::from_fn(|c| spelled[c][row].clone().expect("a stored code")))
        .collect()
}

/// Every payload `encode_codes` cuts the columns into, each with the rows it holds, checked against the
/// limits as it comes.
async fn drain_codes(
    dict: &DictReader,
    columns: &[Vec<TermCode>; 4],
    limits: Limits,
) -> Vec<(Payload, usize)> {
    let mut out = Vec::new();
    let mut at = 0;
    while at < columns[0].len() {
        let rest: [&[TermCode]; 4] = std::array::from_fn(|c| &columns[c][at..]);
        let (payload, rows) = encode_codes(dict, rest, limits).await.unwrap();
        assert!((1..=limits.rows).contains(&rows), "{rows} rows");
        assert_eq!(payload.positions(), 4 * rows);
        assert!(payload.text.len() <= limits.chunk_bytes);
        at += rows;
        out.push((payload, rows));
    }
    out
}

/// The rows the payloads spell.
fn spelled(payloads: &[(Payload, usize)]) -> Vec<Quad> {
    payloads
        .iter()
        .flat_map(|(payload, rows)| {
            (0..*rows).map(move |row| {
                std::array::from_fn(|c| {
                    payload
                        .term(4 * row + c)
                        .expect("a row position holds a term")
                        .to_string()
                })
            })
        })
        .collect()
}

#[test]
fn codes_round_trip_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let stores = stores(dir.path(), LayoutStrategy::Dictionary);
    for (tag, store) in &stores {
        block_on(async {
            let dict = store.dict_reader().unwrap();
            let columns = columns(store, [None; 4]).await;
            let payloads = drain_codes(&dict, &columns, Limits::DEFAULT).await;
            assert_eq!(spelled(&payloads), decoded(&dict, &columns).await, "{tag}");
        });
    }
}

#[test]
fn repeats_share_a_slot() {
    let quads = quads(400);
    let dir = tempfile::tempdir().unwrap();
    let stores = stores(dir.path(), LayoutStrategy::Dictionary);
    for (tag, store) in &stores {
        block_on(async {
            let dict = store.dict_reader().unwrap();
            let columns = columns(store, [None, Some(quads[0][1].as_str()), None, None]).await;
            assert!(
                columns[0].len() > 1,
                "{tag}: the predicate holds several rows"
            );
            let rest = std::array::from_fn(|c| &columns[c][..]);
            let (payload, _) = encode_codes(&dict, rest, Limits::DEFAULT).await.unwrap();
            assert_eq!(payload.terms[1], payload.terms[5], "{tag}: one slot");
            assert!(payload.bounds.len() / 2 < payload.terms.len(), "{tag}");
        });
    }
}

#[test]
fn limits_cut_chunks() {
    let dir = tempfile::tempdir().unwrap();
    let stores = stores(dir.path(), LayoutStrategy::Dictionary);
    for (tag, store) in &stores {
        block_on(async {
            let dict = store.dict_reader().unwrap();
            let columns = columns(store, [None; 4]).await;
            let want = decoded(&dict, &columns).await;

            let rows = Limits::new(7, 1 << 20, 1 << 18).unwrap();
            let payloads = drain_codes(&dict, &columns, rows).await;
            assert!(payloads.iter().all(|(_, n)| *n <= 7), "{tag}");
            assert_eq!(spelled(&payloads), want, "{tag}: by rows");

            let bytes = Limits::new(65_536, 256, 64).unwrap();
            let payloads = drain_codes(&dict, &columns, bytes).await;
            assert!(payloads.len() > 1, "{tag}: the budget cuts");
            assert_eq!(spelled(&payloads), want, "{tag}: by bytes");
            // Each chunk is cut where the next row would pass the budget, not before: offered one row more
            // than it holds, the encoder holds the same rows again.
            let mut at = 0;
            for (payload, n) in &payloads {
                assert!(payload.bytes() <= 256 || *n == 1, "{tag}: within budget");
                if at + n < columns[0].len() {
                    let rest = std::array::from_fn(|c| &columns[c][at..]);
                    let tighter = Limits {
                        rows: n + 1,
                        ..bytes
                    };
                    let (_, again) = encode_codes(&dict, rest, tighter).await.unwrap();
                    assert_eq!(again, *n, "{tag}: chunk at row {at} is full");
                }
                at += n;
            }
        });
    }
}

#[test]
fn huge_terms_take_the_large_list() {
    let dir = tempfile::tempdir().unwrap();
    let stores = stores(dir.path(), LayoutStrategy::Dictionary);
    let limits = Limits::new(65_536, 64, 16).unwrap();
    for (tag, store) in &stores {
        block_on(async {
            let dict = store.dict_reader().unwrap();
            let columns = columns(store, [None; 4]).await;
            let want = decoded(&dict, &columns).await;
            let payloads = drain_codes(&dict, &columns, limits).await;
            assert_eq!(spelled(&payloads), want, "{tag}");
            for (payload, _) in &payloads {
                for (position, &term) in payload.terms.iter().enumerate() {
                    let length = payload.term(position).unwrap().len();
                    if term <= -2 {
                        assert!(length > 16, "{tag}: a large term is long");
                    } else {
                        assert!(length <= 16, "{tag}: a long term is large");
                    }
                }
            }
            // A row whose terms pass the budget alone is a chunk of its own.
            let row = want
                .iter()
                .position(|q| q.iter().map(String::len).sum::<usize>() > 64)
                .expect("a long row");
            let rest = std::array::from_fn(|c| &columns[c][row..]);
            let (payload, n) = encode_codes(&dict, rest, limits).await.unwrap();
            assert_eq!(n, 1, "{tag}");
            assert_eq!(spelled(&[(payload, 1)]), [want[row].clone()], "{tag}");
        });
    }
}

/// Every payload `encode_shared` cuts the rows into, checked against the limits as it comes.
fn drain_shared(rows: &[SharedQuad], limits: Limits) -> Vec<(Payload, usize)> {
    let mut out = Vec::new();
    let mut at = 0;
    while at < rows.len() {
        let (payload, n) = encode_shared(&rows[at..], limits);
        assert!((1..=limits.rows).contains(&n), "{n} rows");
        assert_eq!(payload.positions(), 4 * n);
        assert!(payload.text.len() <= limits.chunk_bytes);
        assert!(payload.bytes() <= limits.chunk_bytes || n == 1);
        at += n;
        out.push((payload, n));
    }
    out
}

#[test]
fn shared_rows_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let mut all = stores(dir.path(), LayoutStrategy::Default);
    all.extend(stores(dir.path(), LayoutStrategy::TypedObject));
    assert_eq!(all.len(), 4);
    for (tag, store) in &all {
        let rows = block_on(async {
            let view = store
                .run_probe(&Probe::parse(None, None, None, None).unwrap())
                .await;
            view.unwrap().shared_quads_vec().await.unwrap()
        });
        let want: Vec<Quad> = rows
            .iter()
            .map(|q| [&q.s, &q.p, &q.o, &q.g].map(|t| t.to_string()))
            .collect();
        for limits in [Limits::DEFAULT, Limits::new(3, 128, 32).unwrap()] {
            let payloads = drain_shared(&rows, limits);
            assert_eq!(spelled(&payloads), want, "{tag} {limits:?}");
            // Positions holding one Arc hold one slot.
            let mut at = 0;
            for (payload, n) in &payloads {
                let terms: Vec<&Arc<str>> = rows[at..at + n]
                    .iter()
                    .flat_map(|q| [&q.s, &q.p, &q.o, &q.g])
                    .collect();
                for i in 0..terms.len() {
                    for j in i + 1..terms.len() {
                        if Arc::ptr_eq(terms[i], terms[j]) {
                            assert_eq!(payload.terms[i], payload.terms[j], "{tag}");
                        }
                    }
                }
                at += n;
            }
        }
    }
    // Two rows sharing their predicate's Arc share its slot; equal spellings in separate Arcs need not.
    let p: Arc<str> = Arc::from("<http://example.org/p>");
    let quad = |s: &str| SharedQuad {
        s: Arc::from(s),
        p: p.clone(),
        o: Arc::from("\"o\""),
        g: Arc::from(""),
    };
    let rows = [
        quad("<http://example.org/a>"),
        quad("<http://example.org/b>"),
    ];
    let (payload, n) = encode_shared(&rows, Limits::DEFAULT);
    assert_eq!(n, 2);
    assert_eq!(payload.terms[1], payload.terms[5]);
    assert_eq!(payload.term(5), Some("<http://example.org/p>"));
}

#[test]
fn lookup_maps_misses_to_null() {
    let dir = tempfile::tempdir().unwrap();
    let stores = stores(dir.path(), LayoutStrategy::Dictionary);
    for (tag, store) in &stores {
        block_on(async {
            let dict = store.dict_reader().unwrap();
            let c0 = dict
                .encode("<http://example.org/p0>")
                .await
                .unwrap()
                .unwrap();
            let len = dict.len() as u64;
            let codes = [c0, u64::MAX, c0, len + 3];
            let (payload, n) = encode_lookup(&dict, &codes, Limits::DEFAULT).await.unwrap();
            assert_eq!((n, payload.positions()), (4, 4), "{tag}");
            let want = dict.decode(c0).await.unwrap();
            assert_eq!(payload.term(0), want.as_deref(), "{tag}");
            assert_eq!(payload.term(2), want.as_deref(), "{tag}");
            assert_eq!(payload.terms[0], payload.terms[2], "{tag}");
            assert_eq!((payload.term(1), payload.term(3)), (None, None), "{tag}");
            assert_eq!(
                encode_lookup(&dict, &[], Limits::DEFAULT).await.unwrap(),
                (Payload::default(), 0),
                "{tag}"
            );
            // A budget cut: one code at a time once the terms pass it.
            let tight = Limits::new(65_536, 4 * 23, 23).unwrap();
            let all: Vec<TermCode> = (0..len).collect();
            let mut at = 0;
            while at < all.len() {
                let (payload, n) = encode_lookup(&dict, &all[at..], tight).await.unwrap();
                assert!(n >= 1 && (payload.bytes() <= 4 * 23 || n == 1), "{tag}");
                for i in 0..n {
                    let want = dict.decode(all[at + i]).await.unwrap();
                    assert_eq!(payload.term(i), want.as_deref(), "{tag}");
                }
                at += n;
            }
        });
    }
}

#[test]
fn a_code_outside_the_dictionary_fails_a_row_read() {
    let dir = tempfile::tempdir().unwrap();
    let stores = stores(dir.path(), LayoutStrategy::Dictionary);
    for (tag, store) in &stores {
        block_on(async {
            let dict = store.dict_reader().unwrap();
            let bad = dict.len() as u64 + 5;
            let columns: [&[TermCode]; 4] = [&[0, 1], &[2, 2], &[3, bad], &[0, 0]];
            let error = encode_codes(&dict, columns, Limits::DEFAULT)
                .await
                .unwrap_err()
                .to_string();
            assert!(error.contains("outside the dictionary"), "{tag}: {error}");
        });
    }
}

#[test]
fn limits_new_refuses_bad_values() {
    assert!(Limits::new(0, 64, 16).is_err());
    assert!(Limits::new(1, 63, 16).is_err());
    assert!(Limits::new(1, 64, 0).is_err());
    assert!(Limits::new(1, 64, 16).is_ok());
    // The largest values the int-coded payload addresses.
    assert!(Limits::new(i32::MAX as usize / 4 + 1, 64, 16).is_err());
    assert!(Limits::new(1, i32::MAX as usize + 1, 16).is_err());
    assert_eq!(
        Limits::new(65_536, 16 << 20, 1 << 20).unwrap(),
        Limits::DEFAULT
    );
}

#[test]
fn pace_follows_the_cut_and_grows_back() {
    let limits = Limits::new(100, 1_000, 100).unwrap();
    let mut pace = Pace::new(limits);
    assert_eq!(pace.limits(), limits);
    // The budget held 10 of 100 offered rows: the next chunk offers 10.
    pace.learn(100, 10, 990);
    assert_eq!(pace.limits(), Limits { rows: 10, ..limits });
    // 10 offered, 10 held, the budget about full: unchanged.
    pace.learn(10, 10, 900);
    assert_eq!(pace.limits().rows, 10);
    // Under half the budget: doubled, up to the read's limit.
    for want in [20, 40, 80, 100, 100] {
        pace.learn(pace.limits().rows, pace.limits().rows, 400);
        assert_eq!(pace.limits().rows, want);
    }
    // A short input held whole is no cut.
    pace.learn(3, 3, 900);
    assert_eq!(pace.limits().rows, 100);
    // The same rule without state, for a lookup whose slices its Java caller cuts: no cap but the caller's.
    assert_eq!(Pace::next_slice(100, 10, 990, 1_000), 10);
    assert_eq!(Pace::next_slice(10, 10, 900, 1_000), 10);
    assert_eq!(Pace::next_slice(10, 10, 400, 1_000), 20);
    assert_eq!(Pace::next_slice(65_536, 65_536, 0, 1_000), 131_072);
}
