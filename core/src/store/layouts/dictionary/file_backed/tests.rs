use super::fixtures::{native_over, windowed_handle};
use super::*;
use vortex_array::arrays::VarBinViewArray;

/// The public handle on `fbd`, which shares its window cache.
fn reader(fbd: &FileBackedDict) -> crate::DictReader {
    super::super::handles::DictAccess::FileBacked(fbd.clone()).reader()
}

/// A window's first and last term are answered from the bounds alone, an
/// interior term rebuilds exactly one window, and a term between two
/// windows, below the first or past the last reads no leaf at all.
#[tokio::test]
async fn window_search_reads_one_window_at_most() {
    let (fbd, terms) = windowed_handle(600, 100).await;
    for w in 0..6 {
        let (before, asked) = (fbd.debug_windows_rebuilt(), fbd.debug_windows_requested());
        assert_eq!(
            fbd.encode_exact(&terms[w * 100]).await.unwrap(),
            Some((w * 100) as TermCode)
        );
        assert_eq!(
            fbd.encode_exact(&terms[w * 100 + 99]).await.unwrap(),
            Some((w * 100 + 99) as TermCode)
        );
        assert_eq!(fbd.debug_windows_rebuilt(), before, "window {w}'s edges");
        assert_eq!(fbd.debug_windows_requested(), asked, "window {w}'s edges");
        assert_eq!(
            fbd.encode_exact(&terms[w * 100 + 37]).await.unwrap(),
            Some((w * 100 + 37) as TermCode)
        );
        assert_eq!(
            fbd.debug_windows_rebuilt(),
            before + 1,
            "window {w}'s interior"
        );
        assert_eq!(
            fbd.debug_windows_requested(),
            asked + 1,
            "window {w}'s interior"
        );
    }
    // All six windows are cached here, so a stray read would be a hit that
    // moves no rebuild count: the request count is what pins "no leaf".
    let (before, asked) = (fbd.debug_windows_rebuilt(), fbd.debug_windows_requested());
    let between = format!("{}~", terms[99]);
    assert_eq!(fbd.encode_exact(&between).await.unwrap(), None);
    assert_eq!(fbd.lower_bound(between.as_bytes()).await.unwrap(), 100);
    assert_eq!(fbd.encode_exact("!").await.unwrap(), None);
    assert_eq!(fbd.lower_bound(b"!").await.unwrap(), 0);
    assert_eq!(fbd.encode_exact("~").await.unwrap(), None);
    assert_eq!(fbd.lower_bound(b"~").await.unwrap(), 600);
    assert_eq!(fbd.debug_windows_rebuilt(), before);
    assert_eq!(fbd.debug_windows_requested(), asked);
    let inside = format!("{}~", terms[150]);
    assert_eq!(fbd.lower_bound(inside.as_bytes()).await.unwrap(), 151);
    assert_eq!(fbd.debug_windows_requested(), asked + 1);
}

/// A decode rebuilds only the windows holding its codes.
#[tokio::test]
async fn decode_rebuilds_only_the_windows_holding_codes() {
    let (fbd, terms) = windowed_handle(600, 100).await;
    let codes = [105u64, 150, 199, 401, 450];
    let before = fbd.debug_windows_rebuilt();
    let got = fbd.decode_many(&codes).await.unwrap();
    assert_eq!(fbd.debug_windows_rebuilt(), before + 2);
    for (code, term) in codes.iter().zip(&got) {
        assert_eq!(&**term, terms[*code as usize].as_str());
    }
    assert!(matches!(
        fbd.decode_many(&[600]).await,
        Err(VortexRdfError::Deserialization(_))
    ));
}

/// Probes inside one window rebuild it once: a repeated term, another
/// term of the window and a lower bound inside it read the same window.
#[tokio::test]
async fn probes_of_one_window_rebuild_it_once() {
    let (fbd, terms) = windowed_handle(600, 100).await;
    let before = fbd.debug_windows_rebuilt();
    for _ in 0..3 {
        assert_eq!(fbd.encode_exact(&terms[137]).await.unwrap(), Some(137));
    }
    assert_eq!(fbd.debug_windows_rebuilt() - before, 1);
    assert_eq!(fbd.encode_exact(&terms[142]).await.unwrap(), Some(142));
    let inside = format!("{}~", terms[150]);
    assert_eq!(fbd.lower_bound(inside.as_bytes()).await.unwrap(), 151);
    assert_eq!(fbd.debug_windows_rebuilt() - before, 1);

    fbd.debug_clear_cache();
    assert_eq!(fbd.encode_exact(&terms[137]).await.unwrap(), Some(137));
    assert_eq!(fbd.debug_windows_rebuilt() - before, 2);
}

/// Repeated decodes of the same codes rebuild their windows once, through
/// both decode entry points.
#[tokio::test]
async fn repeated_decodes_rebuild_once() {
    let (fbd, terms) = windowed_handle(600, 100).await;
    let codes = [105u64, 150, 199, 401, 450];
    let before = fbd.debug_windows_rebuilt();
    for _ in 0..3 {
        let got = fbd.decode_many(&codes).await.unwrap();
        for (code, term) in codes.iter().zip(&got) {
            assert_eq!(&**term, terms[*code as usize].as_str());
        }
    }
    assert_eq!(fbd.debug_windows_rebuilt() - before, 2);
    let any = reader(&fbd)
        .decode_many(&[450, 105, 450, 9999])
        .await
        .unwrap();
    assert_eq!(any[0].as_deref(), Some(terms[450].as_str()));
    assert_eq!(any[1].as_deref(), Some(terms[105].as_str()));
    assert_eq!(any[3], None);
    assert_eq!(fbd.debug_windows_rebuilt() - before, 2);
}

/// A cached window's 32-bit code offsets are the leaf's own bytes: a
/// fresh rebuild of the same leaf has its offsets at the address the
/// cached chunk's offsets sit at.
#[tokio::test]
async fn cached_windows_view_their_leafs_offsets() {
    use vortex_array::arrays::PrimitiveArray;
    use vortex_fsst::{FSST, FSSTArraySlotsExt as _};

    let (fbd, _) = windowed_handle(600, 100).await;
    for w in [0, 2, 5] {
        let chunk = fbd.0.window_chunk(w).await.unwrap();
        let leaf = fetch_flat(&fbd.0.windows[w].layout, &fbd.0.source, &VORTEX_SESSION)
            .await
            .unwrap();
        let fsst = leaf.try_downcast::<FSST>().expect("an FSST leaf");
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let stored = fsst
            .codes_offsets()
            .clone()
            .execute::<PrimitiveArray>(&mut ctx)
            .unwrap();
        assert_eq!(
            chunk.offsets_addr(),
            Some(stored.as_slice::<i32>().as_ptr() as usize),
            "window {w}"
        );
    }
}

/// Windows holding terms of widely varying lengths resolve every probe,
/// positional read and batch, however the codes are ordered.
#[tokio::test]
async fn windows_of_widely_varying_term_lengths_resolve() {
    let terms = crate::tests::widely_varying_terms();
    let n = terms.len();
    for window in [n, 6] {
        let (native, d) = native_over(&terms, window).await;
        let fbd = FileBackedDict::open(&native).await.unwrap().unwrap();
        for (i, term) in terms.iter().enumerate() {
            assert_eq!(fbd.encode_exact(term).await.unwrap(), Some(i as TermCode));
            assert_eq!(
                fbd.lower_bound(term.as_bytes()).await.unwrap(),
                i as TermCode
            );
        }
        let past_the_longest = format!("{}~", terms[9]);
        assert_eq!(fbd.encode_exact(&past_the_longest).await.unwrap(), None);
        assert_eq!(
            fbd.lower_bound(past_the_longest.as_bytes()).await.unwrap(),
            d.lower_bound(past_the_longest.as_bytes())
        );
        // Ascending codes decode short, long and short terms through one
        // cursor per window.
        let codes: Vec<TermCode> = (0..n as TermCode).collect();
        let decoded = fbd.decode_many(&codes).await.unwrap();
        assert!(decoded.iter().zip(&terms).all(|(got, want)| &**got == want));
        // Any order, repeats included.
        let scattered: Vec<TermCode> = [9, 0, 5, 9, 15, 2, 1, 5, 7, 12, 3, 9].into();
        let got = reader(&fbd).decode_many(&scattered).await.unwrap();
        for (code, term) in scattered.iter().zip(&got) {
            assert_eq!(term.as_deref(), Some(terms[*code as usize].as_str()));
        }
    }
}

/// The visitor delivers each position once, in any order with repeats and
/// out-of-range codes, reading each window holding a code once.
#[tokio::test]
async fn decode_many_with_visits_each_position_once_across_windows() {
    let (fbd, terms) = windowed_handle(600, 100).await;
    let codes: [TermCode; 12] = [450, 5, 450, 105, 9999, 5, 599, 0, 105, 600, 450, u64::MAX];
    let mut got = vec![None; codes.len()];
    let mut visits = vec![0usize; codes.len()];
    let before = fbd.debug_windows_rebuilt();
    fbd.decode_many_with(&codes, |i, term| {
        visits[i] += 1;
        got[i] = term.map(str::to_owned);
    })
    .await
    .unwrap();
    // Windows 0, 1, 4 and 5 hold codes.
    assert_eq!(fbd.debug_windows_rebuilt() - before, 4);
    assert!(visits.iter().all(|&n| n == 1), "{visits:?}");
    let want: Vec<Option<String>> = codes
        .iter()
        .map(|&code| terms.get(code as usize).cloned())
        .collect();
    assert_eq!(got, want);
    assert_eq!(reader(&fbd).decode_many(&codes).await.unwrap(), want);
}

/// A code list that is not strictly ascending is an error, however far
/// the last code is in range: `[700, 800, 5]` once passed the last-code
/// check and then looped on a run of nothing, `[150, 50]` underflowed
/// `code - window.start`.
#[tokio::test]
async fn unsorted_code_lists_are_errors_not_loops() {
    let (fbd, terms) = windowed_handle(600, 100).await;
    for codes in [
        vec![700u64, 800, 5],
        vec![150, 50],
        vec![5, 5],
        vec![0, 450, 449],
        vec![599, 0],
    ] {
        assert!(
            matches!(
                fbd.decode_many(&codes).await,
                Err(VortexRdfError::InvalidInput(_))
            ),
            "{codes:?}"
        );
    }
    // Ascending, unique and in range still decodes, window edges included.
    let codes = [0u64, 99, 100, 599];
    let got = fbd.decode_many(&codes).await.unwrap();
    for (code, term) in codes.iter().zip(&got) {
        assert_eq!(&**term, terms[*code as usize].as_str());
    }
    assert!(fbd.decode_many(&[]).await.unwrap().is_empty());
}

/// Searches that cross windows — lower bounds, prefix ranges, the kind
/// ranges and predicates over candidate codes — answer exactly like the
/// resident dictionary they were written from, over terms of every kind:
/// window boundaries fall inside the literal and IRI runs, and each change
/// of kind falls inside a window.
#[tokio::test]
async fn window_searches_match_the_resident_dictionary() {
    let mut terms: Vec<String> = vec![String::new()];
    terms.extend((0..150).map(|i| format!("\"lit {i:03}\"")));
    terms.extend((0..30).map(|i| format!("\"lit {i:03}\"@en")));
    terms.extend(
        (0..30).map(|i| format!("\"{}\"^^<http://www.w3.org/2001/XMLSchema#integer>", i * 10)),
    );
    terms.extend((0..300).map(|i| format!("<http://example.org/iri/{i:04}>")));
    terms.extend((0..80).map(|i| format!("_:b{i:03}")));
    terms.sort();
    terms.dedup();
    assert_eq!(terms.len(), 591);
    let plain = VarBinViewArray::from_iter_str(terms.iter().map(String::as_str));
    let d = TermDictionary::compress_windowed(plain, 100).unwrap();
    let native = crate::tests::open_native_bytes(crate::tests::write_dict_only_store(&d).await);
    let fbd = FileBackedDict::open(&native).await.unwrap().unwrap();
    assert_eq!(fbd.debug_window_count(), 6);

    assert_eq!(fbd.kind_ranges().await.unwrap(), *d.kind_ranges());
    let all: Vec<TermCode> = (0..terms.len() as TermCode).collect();
    let sparse: Vec<TermCode> = all.iter().copied().step_by(7).collect();
    let mut probes: Vec<String> = [
        "",
        "\"",
        "\"lit 1",
        "<",
        "<http://example.org/iri/00",
        "_:",
        "_:b0",
        "~",
    ]
    .map(String::from)
    .into();
    for term in &terms {
        probes.push(term.clone());
        probes.push(format!("{term} "));
        probes.push(term.strip_suffix('>').unwrap_or(term).to_owned());
    }
    for probe in &probes {
        assert_eq!(
            fbd.lower_bound(probe.as_bytes()).await.unwrap(),
            d.lower_bound(probe.as_bytes()),
            "lower_bound {probe:?}"
        );
        assert_eq!(
            fbd.prefix_range(probe).await.unwrap(),
            d.prefix_range(probe),
            "prefix_range {probe:?}"
        );
    }
    for (kind, arg) in [
        ("is_literal", ""),
        ("is_iri", ""),
        ("is_blank", ""),
        ("str_prefix", "lit 1"),
        ("lang", "en"),
        ("datatype", "http://www.w3.org/2001/XMLSchema#integer"),
        ("num_lt", "95"),
    ] {
        let predicate = TermPredicate::parse(kind, arg).unwrap();
        for codes in [&all, &sparse] {
            assert_eq!(
                fbd.filter_codes(&predicate, codes).await.unwrap(),
                d.filter_codes(&predicate, codes).unwrap(),
                "filter_codes {kind} {arg:?}"
            );
        }
    }
}

/// A candidate evaluation rebuilds only the windows holding the codes it
/// must read.
#[tokio::test]
async fn filter_codes_rebuilds_only_candidate_windows() {
    let (fbd, _) = windowed_handle(600, 100).await;
    fbd.kind_ranges().await.unwrap();
    let (before, asked) = (fbd.debug_windows_rebuilt(), fbd.debug_windows_requested());
    // An IRI is a text only under `STR()`.
    let predicate = TermPredicate::parse_with(
        "str_prefix",
        "http://example.org/term/01",
        &super::super::predicates::TextOptions {
            as_str: true,
            ..Default::default()
        },
    )
    .unwrap();
    let (passed, undecided) = fbd
        .filter_codes(&predicate, &[105, 150, 450])
        .await
        .unwrap();
    assert_eq!(fbd.debug_windows_rebuilt(), before + 2);
    assert_eq!(fbd.debug_windows_requested(), asked + 2);
    assert_eq!(passed.as_slice(), &[105, 150]);
    assert!(undecided.is_empty());

    // The two windows are cached: the request count, which a hit moves as
    // a miss does, is what pins that the next two steps read nothing.
    // A kind test is decided by the kind ranges: nothing is read.
    let (before, asked) = (fbd.debug_windows_rebuilt(), fbd.debug_windows_requested());
    let is_iri = TermPredicate::parse("is_iri", "").unwrap();
    let (passed, undecided) = fbd.filter_codes(&is_iri, &[105, 150, 450]).await.unwrap();
    assert_eq!(fbd.debug_windows_rebuilt(), before);
    assert_eq!(fbd.debug_windows_requested(), asked);
    assert_eq!(passed.as_slice(), &[105, 150, 450]);
    assert!(undecided.is_empty());

    // Without `STR()` an IRI has no text: its kind fails it, nothing is read.
    let (before, asked) = (fbd.debug_windows_rebuilt(), fbd.debug_windows_requested());
    let plain = TermPredicate::parse("str_prefix", "http://example.org/term/01").unwrap();
    let (passed, undecided) = fbd.filter_codes(&plain, &[105, 150, 450]).await.unwrap();
    assert_eq!(fbd.debug_windows_rebuilt(), before);
    assert_eq!(fbd.debug_windows_requested(), asked);
    assert!(passed.is_empty() && undecided.is_empty());
}

/// A candidate list that is not ascending, unique and inside the
/// dictionary is refused before any window is read.
#[tokio::test]
async fn filter_codes_refuses_bad_candidates_before_reading() {
    let (fbd, _) = windowed_handle(600, 100).await;
    fbd.kind_ranges().await.unwrap();
    let (before, asked) = (fbd.debug_windows_rebuilt(), fbd.debug_windows_requested());
    let predicate = TermPredicate::parse("str_prefix", "http://").unwrap();
    for codes in [
        vec![700u64, 800, 5],
        vec![150, 50],
        vec![5, 5],
        vec![0, 600],
    ] {
        assert!(
            matches!(
                fbd.filter_codes(&predicate, &codes).await,
                Err(VortexRdfError::InvalidInput(_))
            ),
            "{codes:?}"
        );
    }
    assert_eq!(fbd.debug_windows_rebuilt(), before);
    assert_eq!(fbd.debug_windows_requested(), asked);
}

/// Spellings no writer of this crate produces — a lone `<`, a `<` and a
/// multi-byte character with no closing `>`, a bare `_`, `_:` or `"` — in
/// a foreign file's dictionary: every string kind reads them without
/// panicking and passes none. One in the IRI or blank-node range fails
/// unless the text is `STR()`, where it is undecided; a `_` between the
/// ranges and a `"` that is no literal are undecided throughout. Both
/// residencies agree.
#[tokio::test]
async fn filter_codes_survives_foreign_spellings() {
    use super::super::predicates::{CaseMap, TextOptions};

    let mut terms: Vec<String> = [
        "\"",
        "\"a\"",
        "<",
        "<é",
        "<http://ex.org/a>",
        "_",
        "_:",
        "_:b0",
    ]
    .map(String::from)
    .into();
    terms.sort();
    let code = |spelling: &str| terms.iter().position(|t| t == spelling).unwrap() as TermCode;
    let plain = VarBinViewArray::from_iter_str(terms.iter().map(String::as_str));
    let d = TermDictionary::compress_windowed(plain, 3).unwrap();
    let native = crate::tests::open_native_bytes(crate::tests::write_dict_only_store(&d).await);
    let fbd = FileBackedDict::open(&native).await.unwrap().unwrap();
    assert_eq!(fbd.debug_window_count(), 3);
    assert_eq!(fbd.kind_ranges().await.unwrap(), *d.kind_ranges());

    let all: Vec<TermCode> = (0..terms.len() as TermCode).collect();
    let (quote, lt, lt_e, underscore, blank) =
        (code("\""), code("<"), code("<é"), code("_"), code("_:"));
    let foreign = [quote, lt, lt_e, underscore, blank];
    for (kind, arg) in [
        ("str_prefix", ""),
        ("str_prefix", "a"),
        ("contains", "\"\""),
        ("contains", "\"a\""),
        ("strstarts", "\"\""),
        ("strends", "\"a\"@en"),
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
                ..Default::default()
            };
            let predicate = TermPredicate::parse_with(kind, arg, &options).unwrap();
            let (passed, undecided) = fbd.filter_codes(&predicate, &all).await.unwrap();
            assert_eq!(
                (passed.clone(), undecided.clone()),
                d.filter_codes(&predicate, &all).unwrap(),
                "{kind} {arg:?} {options:?}: residencies"
            );
            let undecided_foreign: &[TermCode] = if as_str {
                &foreign
            } else {
                &[quote, underscore]
            };
            for term in foreign {
                let why = format!("{kind} {arg:?} {options:?} on {:?}", terms[term as usize]);
                assert!(!passed.as_slice().contains(&term), "{why}: passed");
                assert_eq!(
                    undecided.as_slice().contains(&term),
                    undecided_foreign.contains(&term),
                    "{why}: undecided"
                );
            }
        }
    }
    // The well-formed terms beside them are still decided.
    let literal = TermPredicate::parse("str_prefix", "a").unwrap();
    let (passed, _) = fbd.filter_codes(&literal, &all).await.unwrap();
    assert_eq!(passed.as_slice(), &[code("\"a\"")]);
    let iri = TermPredicate::parse_with(
        "str_prefix",
        "http",
        &TextOptions {
            as_str: true,
            ..Default::default()
        },
    )
    .unwrap();
    let (passed, _) = fbd.filter_codes(&iri, &all).await.unwrap();
    assert_eq!(passed.as_slice(), &[code("<http://ex.org/a>")]);
}
