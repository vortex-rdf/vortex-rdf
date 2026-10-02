//! Term predicates against the rules a Python query layer's fast path
//! applies (vortex-rdflib's `filters.py`, validated there against rdflib):
//! a native `True`/`False` must agree with that layer's `True`/`False`
//! (its `ERROR` folds to `False` under a FILTER), and wherever that layer
//! defers (`UNKNOWN`) the native verdict must be `Unknown` too. The
//! spellings are that layer's own test corpus.

use super::*;
use crate::store::{TermPredicate, Verdict};

const XSD: &str = "http://www.w3.org/2001/XMLSchema#";

fn typed(lexical: &str, datatype: &str) -> String {
    format!("\"{lexical}\"^^<{XSD}{datatype}>")
}

/// What the native verdict must be: exactly `Verdict` (`Definite`), the
/// Python layer's answer or a deferral (`Allowed`), or a deferral
/// (`Deferred`).
#[derive(Clone, Copy)]
enum Expect {
    Definite(bool),
    Allowed(bool),
    Deferred,
}
use Expect::{Allowed, Deferred, Definite};

fn check(spelling: &str, kind: &str, arg: &str, expect: Expect) {
    let predicate = TermPredicate::parse(kind, arg).unwrap();
    let got = predicate.eval(spelling);
    let ok = match expect {
        Definite(b) => got == Verdict::from(b),
        Allowed(b) => got == Verdict::from(b) || got == Verdict::Unknown,
        Deferred => got == Verdict::Unknown,
    };
    assert!(
        ok,
        "{kind} {arg:?} on {spelling}: got {got:?}, expected {}",
        match expect {
            Definite(b) => format!("exactly {b}"),
            Allowed(b) => format!("{b} or Unknown"),
            Deferred => "Unknown".to_string(),
        }
    );
}

/// `?v < 5` over the corpus: the numeric fast path where the lexical form
/// is well-formed, datatype-IRI order across XSD datatypes, errors on
/// non-literals, deferrals everywhere the Python layer defers.
#[test]
fn num_lt_integer_five() {
    let cases: Vec<(String, Expect)> = vec![
        (typed("5", "integer"), Definite(false)),
        (typed("01", "integer"), Definite(true)),
        (typed("+7", "integer"), Allowed(false)),
        (typed(" 7", "integer"), Allowed(false)),
        (typed("1_0", "integer"), Allowed(false)),
        (typed("1.5", "integer"), Deferred),
        (typed("abc", "integer"), Deferred),
        (typed("0x10", "integer"), Deferred),
        (typed("99999999999999999999", "integer"), Definite(false)),
        (typed("-3", "integer"), Definite(true)),
        (typed("1.5", "decimal"), Definite(true)),
        (typed("5", "decimal"), Definite(false)),
        (typed("NaN", "double"), Allowed(false)),
        (typed("INF", "double"), Allowed(false)),
        (typed("-INF", "double"), Allowed(true)),
        (typed("1e2", "double"), Definite(false)),
        (typed("5", "float"), Definite(false)),
        (typed("7", "byte"), Definite(false)),
        // Out of range for its datatype: no value, so the datatypes order.
        (typed("300", "byte"), Definite(true)),
        (typed("5", "int"), Definite(false)),
        (typed("5", "short"), Definite(false)),
        (typed("5", "unsignedByte"), Definite(false)),
        (typed("-1", "nonNegativeInteger"), Definite(false)),
        (typed("5", "positiveInteger"), Definite(false)),
        (typed("true", "boolean"), Definite(true)),
        (typed("maybe", "boolean"), Definite(true)),
        (typed("2020-01-01T00:00:00", "dateTime"), Definite(true)),
        ("\"5\"".into(), Definite(false)),
        ("\"\"".into(), Definite(false)),
        ("\"abc\"@en".into(), Definite(false)),
        ("\"5\"@en".into(), Definite(false)),
        ("\"x\"^^<http://ex.org/dt>".into(), Allowed(true)),
        ("<http://ex.org/x>".into(), Definite(false)),
        ("_:b0".into(), Definite(false)),
    ];
    for (spelling, expect) in &cases {
        check(spelling, "num_lt", "5", *expect);
    }
    // The mirror orderings follow from the same comparison.
    check(&typed("-3", "integer"), "num_ge", "5", Definite(false));
    check(&typed("-3", "integer"), "num_gt", "5", Definite(false));
    check(&typed("-3", "integer"), "num_le", "5", Definite(true));
    check(&typed("5", "integer"), "num_le", "5", Definite(true));
    check(&typed("5", "integer"), "num_ge", "5", Definite(true));
    check("\"abc\"@en", "num_gt", "5", Definite(true));
    check("\"5\"", "num_gt", "5", Definite(true));
    check(&typed("true", "boolean"), "num_gt", "5", Definite(false));
}

/// Equality and inequality: value equality for numbers, term inequality for
/// non-literals, deferral where only an engine's canonicalization decides.
#[test]
fn num_eq_and_ne() {
    check(&typed("5", "integer"), "num_eq", "5", Definite(true));
    check(&typed("05", "integer"), "num_eq", "5", Definite(true));
    check(&typed("5", "decimal"), "num_eq", "5", Definite(true));
    check(&typed("5.0", "decimal"), "num_eq", "5", Definite(true));
    check(&typed("5", "float"), "num_eq", "5", Definite(true));
    check(&typed("1e2", "double"), "num_eq", "1e1", Definite(false));
    check(&typed("1e1", "double"), "num_eq", "10", Definite(true));
    check(&typed("5", "integer"), "num_eq", "1e1", Definite(false));
    check(&typed("5", "decimal"), "num_eq", "1e1", Definite(false));
    check(&typed("1.5", "decimal"), "num_eq", "1e1", Allowed(false));
    check(&typed("NaN", "double"), "num_eq", "5", Allowed(false));
    check(&typed("abc", "integer"), "num_eq", "5", Deferred);
    check(&typed("1.5", "integer"), "num_eq", "5", Deferred);
    check("\"5\"", "num_eq", "5", Allowed(false));
    check("\"5\"@en", "num_eq", "5", Allowed(false));
    check(&typed("true", "boolean"), "num_eq", "5", Allowed(false));
    check("<http://ex.org/x>", "num_eq", "5", Definite(false));
    check("_:b0", "num_eq", "5", Definite(false));

    check(&typed("5", "integer"), "num_ne", "5", Definite(false));
    check(&typed("-3", "integer"), "num_ne", "5", Definite(true));
    check(&typed("1e2", "double"), "num_ne", "1e1", Definite(true));
    check("\"5\"", "num_ne", "5", Allowed(true));
    check(&typed("abc", "integer"), "num_ne", "5", Deferred);
    check("<http://ex.org/x>", "num_ne", "5", Definite(true));
    check("_:b0", "num_ne", "5", Definite(true));
}

/// Mixed-model comparisons: integers against decimals exactly, floats
/// against small integers, and nothing where exactness is lost.
#[test]
fn num_mixed_models() {
    check(&typed("5", "integer"), "num_lt", "5.5", Definite(true));
    check(&typed("6", "integer"), "num_lt", "5.5", Definite(false));
    check(&typed("1.5", "decimal"), "num_lt", "5.5", Definite(true));
    check(&typed("1e2", "double"), "num_lt", "5.5", Allowed(false));
    check(&typed("1e2", "double"), "num_lt", "-1", Definite(false));
    check(&typed("-3", "integer"), "num_lt", "-1", Definite(true));
    check(&typed("1e2", "double"), "num_gt", "1e100", Definite(false));
    check(
        &typed("99999999999999999999", "integer"),
        "num_gt",
        "1e100",
        Allowed(false),
    );
    check(&typed("5", "integer"), "num_gt", "1e100", Definite(false));
    check(&typed("5", "decimal"), "num_gt", "1e100", Definite(false));
    check(&typed("-INF", "double"), "num_lt", "1e100", Allowed(true));
}

/// Datatype and language predicates are pure spelling tests over literals.
#[test]
fn datatype_and_lang() {
    let integer = format!("{XSD}integer");
    let string = format!("{XSD}string");
    let lang_string = "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString";
    for (spelling, is_integer, is_string, is_lang) in [
        (typed("5", "integer"), true, false, false),
        (typed("abc", "integer"), true, false, false),
        (typed("5", "decimal"), false, false, false),
        (typed("5", "string"), false, true, false),
        ("\"5\"".to_string(), false, true, false),
        ("\"\"".to_string(), false, true, false),
        ("\"abc\"@en".to_string(), false, false, true),
        ("\"5\"@en".to_string(), false, false, true),
        ("\"x\"^^<http://ex.org/dt>".to_string(), false, false, false),
    ] {
        check(&spelling, "datatype", &integer, Definite(is_integer));
        check(
            &spelling,
            "datatype",
            &format!("<{integer}>"),
            Definite(is_integer),
        );
        check(&spelling, "datatype", &string, Definite(is_string));
        check(&spelling, "datatype", lang_string, Definite(is_lang));
    }
    for (spelling, en, empty, matches_en, any, en_us) in [
        ("\"abc\"@en", true, false, true, true, false),
        ("\"abc\"@en-us", false, false, true, true, true),
        ("\"5\"@en", true, false, true, true, false),
        ("\"abc\"", false, true, false, false, false),
        ("\"\"", false, true, false, false, false),
    ] {
        check(spelling, "lang", "en", Definite(en));
        check(spelling, "lang", "", Definite(empty));
        check(spelling, "lang_matches", "EN", Definite(matches_en));
        check(spelling, "lang_matches", "*", Definite(any));
        check(spelling, "lang_matches", "en-US", Definite(en_us));
    }
    let five = typed("5", "integer");
    check(&five, "lang", "en", Definite(false));
    check(&five, "lang", "", Definite(true));
    check(&five, "lang_matches", "*", Definite(false));
    // Non-literals are outside the domain: errors in the engine, which a
    // scan never asks about (the kind ranges decide them).
    check("<http://ex.org/x>", "lang", "", Definite(false));
    check("_:b0", "lang_matches", "*", Definite(false));
}

/// Kind predicates read the first byte; the default graph's empty spelling
/// is no kind.
#[test]
fn kinds_and_str_prefix() {
    for (spelling, iri, blank, literal) in [
        ("<http://ex.org/x>", true, false, false),
        ("_:b0", false, true, false),
        ("\"abc\"", false, false, true),
        ("\"abc\"@en", false, false, true),
        (&typed("5", "integer"), false, false, true),
    ] {
        check(spelling, "is_iri", "", Definite(iri));
        check(spelling, "is_blank", "", Definite(blank));
        check(spelling, "is_literal", "", Definite(literal));
    }
    for kind in ["is_iri", "is_blank", "is_literal"] {
        check("", kind, "", Deferred);
    }
    // `str_prefix` is `strstarts(str(?v), p)`: the unescaped lexical form of
    // a string-like literal, the IRI itself, a blank node's label; other
    // typed literals depend on the engine's canonical form.
    check("\"abc\"", "str_prefix", "a", Definite(true));
    check("\"Abc\"", "str_prefix", "a", Definite(false));
    check("\"abc\"@en", "str_prefix", "ab", Definite(true));
    check("\"a\\\"b\"", "str_prefix", "a\"", Definite(true));
    check("\"a\\nb\"", "str_prefix", "a\n", Definite(true));
    check("\"a\\nb\"", "str_prefix", "a\\", Definite(false));
    check(&typed("abc", "string"), "str_prefix", "ab", Definite(true));
    check("\"\"", "str_prefix", "", Definite(true));
    check("\"\"", "str_prefix", "a", Definite(false));
    check(&typed("5", "integer"), "str_prefix", "5", Deferred);
    check("<http://ex.org/x>", "str_prefix", "http", Definite(true));
    check("<http://ex.org/x>", "str_prefix", "a", Definite(false));
    check("_:b0", "str_prefix", "b", Definite(true));
    check("_:b0", "str_prefix", "_", Definite(false));
}

/// Malformed predicates are refused at parse time, with the kind named.
#[test]
fn parse_rejects_malformed() {
    for (kind, arg) in [
        ("no_such", ""),
        ("datatype", ""),
        ("lang_matches", ""),
        ("num_lt", ""),
        ("num_lt", "abc"),
        ("num_eq", "1.5.2"),
        ("num_gt", "\"5\"^^<http://ex.org/dt>"),
    ] {
        assert!(TermPredicate::parse(kind, arg).is_err(), "{kind} {arg:?}");
    }
    // A numeric argument is a bare lexical form or a typed literal.
    assert!(TermPredicate::parse("num_lt", "5").is_ok());
    assert!(TermPredicate::parse("num_lt", "-5.5").is_ok());
    assert!(TermPredicate::parse("num_lt", "1e3").is_ok());
    assert!(TermPredicate::parse("num_lt", &typed("5", "byte")).is_ok());
}

/// The dictionary partitions its codes by every predicate exactly as
/// `eval` answers term by term.
#[tokio::test]
async fn filter_codes_matches_eval() {
    let mut quads = dictionary_test_quads();
    let s = NamedOrBlankNode::NamedNode(NamedNode::new("http://example.org/typed").unwrap());
    let p = NamedNode::new("http://example.org/value").unwrap();
    for spelling in [
        typed("5", "integer"),
        typed("-3", "integer"),
        typed("abc", "integer"),
        typed("1.5", "decimal"),
        typed("1e2", "double"),
        typed("300", "byte"),
        typed("true", "boolean"),
        "\"abc\"@en".to_string(),
        "\"abc\"@en-us".to_string(),
        "\"Abc\"".to_string(),
        "\"a\\\"b\"".to_string(),
        "\"x\"^^<http://ex.org/dt>".to_string(),
    ] {
        let term = crate::common::terms::parse_term(&spelling).unwrap();
        quads.push(Quad::new(
            s.clone(),
            p.clone(),
            term,
            GraphName::DefaultGraph,
        ));
    }
    let store = VortexRdfStore::from_quads(quad_stream(quads), LayoutStrategy::Dictionary, vec![])
        .await
        .unwrap();
    let dict = store.code_read_snapshot().unwrap();
    let terms: Vec<String> = (0..dict.len() as u32)
        .map(|c| dict.decode(c).unwrap())
        .collect();
    let kinds = dict.kind_ranges();
    for (kind, arg) in [
        ("is_literal", ""),
        ("is_iri", ""),
        ("is_blank", ""),
        ("datatype", &format!("{XSD}integer")),
        ("datatype", &format!("{XSD}string")),
        ("lang", "en"),
        ("lang", ""),
        ("lang_matches", "en"),
        ("lang_matches", "*"),
        ("str_prefix", "a"),
        ("str_prefix", "http://example.org/s0"),
        ("num_lt", "5"),
        ("num_le", "5"),
        ("num_gt", "0"),
        ("num_ge", "1e2"),
        ("num_eq", "5"),
        ("num_ne", "5"),
    ] {
        let predicate = TermPredicate::parse(kind, arg).unwrap();
        let (truth, unknown) = dict.filter_codes(&predicate);
        let in_domain = |code: u32| match predicate.domain() {
            crate::store::Domain::All => true,
            crate::store::Domain::Literals => kinds.literals.contains(&code),
        };
        let mut want_true = Vec::new();
        let mut want_unknown = Vec::new();
        for (code, term) in (0u32..).zip(&terms) {
            if !in_domain(code) {
                continue;
            }
            match predicate.eval(term) {
                Verdict::True => want_true.push(code),
                Verdict::Unknown => want_unknown.push(code),
                Verdict::False => {}
            }
        }
        assert_eq!(
            truth.as_slice(),
            &want_true[..],
            "{kind} {arg:?}: true codes"
        );
        assert_eq!(
            unknown.as_slice(),
            &want_unknown[..],
            "{kind} {arg:?}: unknown codes"
        );
        // Memoized: the same answer on re-ask.
        assert_eq!(dict.filter_codes(&predicate).0.as_slice(), &want_true[..]);
    }
}
