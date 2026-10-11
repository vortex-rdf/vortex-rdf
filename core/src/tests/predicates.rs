//! Term predicates against the rules a Python query layer's fast path
//! applies (vortex-rdflib's `filters.py`, validated there against rdflib):
//! a native `True`/`False` must agree with that layer's `True`/`False`
//! (its `ERROR` folds to `False` under a FILTER), and wherever that layer
//! defers (`UNKNOWN`) the native verdict must be `Unknown` too. The
//! spellings are that layer's own test corpus.

use super::*;
use crate::store::{CaseMap, TermPredicate, TextOptions, Verdict};

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
    // Non-literals: errors in the engine, which `filter_codes` never asks
    // about (the kind ranges decide them).
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
    // `str_prefix` is a string kind: under `string()` only string literals
    // have a text; IRIs, blank nodes and other typed literals fail.
    check("\"abc\"", "str_prefix", "a", Definite(true));
    check("\"Abc\"", "str_prefix", "a", Definite(false));
    check("\"abc\"@en", "str_prefix", "ab", Definite(true));
    check("\"a\\\"b\"", "str_prefix", "a\"", Definite(true));
    check("\"a\\nb\"", "str_prefix", "a\n", Definite(true));
    check("\"a\\nb\"", "str_prefix", "a\\", Definite(false));
    check(&typed("abc", "string"), "str_prefix", "ab", Definite(true));
    check("\"\"", "str_prefix", "", Definite(true));
    check("\"\"", "str_prefix", "a", Definite(false));
    check(&typed("5", "integer"), "str_prefix", "5", Definite(false));
    check("<http://ex.org/x>", "str_prefix", "http", Definite(false));
    check("_:b0", "str_prefix", "b", Definite(false));
}

fn check_text(spelling: &str, kind: &str, arg: &str, options: TextOptions, expect: Verdict) {
    let got = TermPredicate::parse_with(kind, arg, &options)
        .unwrap()
        .eval(spelling);
    assert_eq!(got, expect, "{kind} {arg:?} {options:?} on {spelling}");
}

fn opts(case: Option<CaseMap>, as_str: bool) -> TextOptions {
    TextOptions {
        flags: String::new(),
        case,
        as_str,
    }
}

/// The string kinds against rdflib 7.6's `_compatibleStrings`,
/// `Builtin_STR`, `LCASE`/`UCASE` and `in`/`startswith`/`endswith`.
#[test]
fn string_kinds_follow_rdflib() {
    let none = || opts(None, false);
    let str_ = || opts(None, true);
    let xsd_string = format!("\"A\"^^<{XSD}string>");
    for (spelling, kind, arg, options, expect) in [
        ("\"Ab\"@en", "contains", "\"b\"", none(), Verdict::True),
        ("\"Ab\"@en", "contains", "\"b\"@en", none(), Verdict::True),
        ("\"Ab\"@en", "contains", "\"b\"@EN", none(), Verdict::False),
        ("\"Ab\"", "contains", "\"b\"@en", none(), Verdict::False),
        (
            "\"Ab\"@en",
            "strstarts",
            xsd_string.as_str(),
            none(),
            Verdict::True,
        ),
        ("<http://ex/a>", "contains", "\"a\"", none(), Verdict::False),
        ("<http://ex/a>", "contains", "\"a\"", str_(), Verdict::True),
        (
            "<http://ex/a>",
            "contains",
            "\"a\"@en",
            str_(),
            Verdict::False,
        ),
        (
            "\"Ab\"@en",
            "contains",
            "\"ab\"",
            opts(Some(CaseMap::Lower), false),
            Verdict::True,
        ),
        (
            "\"ab\"",
            "contains",
            "\"A\"",
            opts(Some(CaseMap::Upper), false),
            Verdict::True,
        ),
        (
            "\"Áb\"",
            "contains",
            "\"ab\"",
            opts(Some(CaseMap::Lower), false),
            Verdict::Unknown,
        ),
        ("\"ab\\n\"", "strends", "\"\\n\"", none(), Verdict::True),
        ("\"caf\\u00E9\"", "contains", "\"é\"", none(), Verdict::True),
        (
            "\"5\"",
            "contains",
            &format!("\"5\"^^<{XSD}integer>"),
            none(),
            Verdict::False,
        ),
        ("\"a\"", "contains", "<http://ex/a>", none(), Verdict::False),
        ("\"anything\"", "contains", "\"\"", none(), Verdict::True),
        (
            "\"x\"^^<http://ex/dt>",
            "str_prefix",
            "x",
            none(),
            Verdict::False,
        ),
        (
            "\"x\"^^<http://ex/dt>",
            "str_prefix",
            "x",
            str_(),
            Verdict::True,
        ),
        // rdflib STR(_:b0) is "b0"
        ("_:b0", "str_prefix", "b", str_(), Verdict::Unknown),
        ("\"Ab\"@en", "strends", "\"b\"@fr", none(), Verdict::False),
    ] {
        check_text(spelling, kind, arg, options, expect);
    }
}

/// `STR()` of a literal rdflib parses into a value is its canonical form,
/// not the stored lexical form: undecided, never decided on the spelling.
#[test]
fn str_of_rdflib_normalized_datatypes_is_undecided() {
    for spelling in [
        typed("01", "integer"),
        typed("1", "boolean"),
        typed("1e2", "double"),
        typed("1.50", "decimal"),
    ] {
        check_text(
            &spelling,
            "contains",
            "\"0\"",
            opts(None, true),
            Verdict::Unknown,
        );
    }
    check_text(
        &typed("0001", "gYear"),
        "contains",
        "\"0\"",
        opts(None, true),
        Verdict::True,
    );
    check_text(
        "\"01\"^^<http://ex/dt>",
        "contains",
        "\"0\"",
        opts(None, true),
        Verdict::True,
    );
    // `xsd:string` is the text itself, however a foreign writer spells it.
    check_text(
        &typed("01", "string"),
        "contains",
        "\"0\"",
        opts(None, true),
        Verdict::True,
    );
    // Every datatype with an entry in rdflib 7.6's `XSDToPython` but
    // `xsd:string`, and the two non-XSD ones it parses (`rdf:HTML` only when
    // its optional `html5rdf` is installed).
    for local in [
        "anyURI",
        "base64Binary",
        "boolean",
        "byte",
        "date",
        "dateTime",
        "dayTimeDuration",
        "decimal",
        "double",
        "duration",
        "float",
        "hexBinary",
        "int",
        "integer",
        "language",
        "long",
        "negativeInteger",
        "nonNegativeInteger",
        "nonPositiveInteger",
        "normalizedString",
        "positiveInteger",
        "short",
        "time",
        "token",
        "unsignedByte",
        "unsignedInt",
        "unsignedLong",
        "unsignedShort",
        "yearMonthDuration",
    ] {
        check_text(
            &typed("0", local),
            "contains",
            "\"0\"",
            opts(None, true),
            Verdict::Unknown,
        );
    }
    for local in ["XMLLiteral", "HTML"] {
        let spelling =
            format!("\"<b>0</b >\"^^<http://www.w3.org/1999/02/22-rdf-syntax-ns#{local}>");
        check_text(
            &spelling,
            "contains",
            "\"0\"",
            opts(None, true),
            Verdict::Unknown,
        );
    }
}

/// `regex` reads the term's text like the other string kinds — `string()` by
/// default, `STR()` with `as_str`, after the case wrapper — and its flags are
/// rdflib's `re` flags.
#[test]
fn regex_follows_the_string_kind_rules() {
    let with = |flags: &str, case, as_str| TextOptions {
        flags: flags.into(),
        case,
        as_str,
    };
    let none = || with("", None, false);
    let str_ = || with("", None, true);
    let lower = || with("", Some(CaseMap::Lower), false);
    for (spelling, pattern, options, expect) in [
        // `string()`: string literals only, the language tag not looked at.
        ("\"Ab\"@en", "b$", none(), Verdict::True),
        ("\"Ab\"@en", "^b", none(), Verdict::False),
        ("\"Ab\"", "^a", with("i", None, false), Verdict::True),
        ("\"Ab\"", "^a", none(), Verdict::False),
        ("\"Ab\"", "^a", with("I", None, false), Verdict::False),
        ("<http://ex/a>", "a", none(), Verdict::False),
        ("_:b0", "b", none(), Verdict::False),
        (&typed("5", "integer"), "5", none(), Verdict::False),
        ("\"5\"^^<http://ex/dt>", "5", none(), Verdict::False),
        (&typed("a5", "string"), "5$", none(), Verdict::True),
        // `STR()`: an IRI's string or a lexical form; a blank node, or a
        // literal whose datatype rdflib normalizes, is undecided.
        ("<http://ex/a>", "^http://ex/", str_(), Verdict::True),
        ("<http://ex/a>", "^<", str_(), Verdict::False),
        ("\"5\"^^<http://ex/dt>", "5", str_(), Verdict::True),
        ("_:b0", "b", str_(), Verdict::Unknown),
        (&typed("01", "integer"), "0", str_(), Verdict::Unknown),
        (&typed("0", "boolean"), "0", str_(), Verdict::Unknown),
        // The case wrapper: ASCII text only, applied after `STR()`.
        ("\"Ab\"", "^ab$", lower(), Verdict::True),
        ("\"Ab\"", "^Ab$", lower(), Verdict::False),
        (
            "\"ab\"",
            "^AB$",
            with("", Some(CaseMap::Upper), false),
            Verdict::True,
        ),
        ("\"Áb\"", "ab", lower(), Verdict::Unknown),
        ("\"Áb\"", "^$", lower(), Verdict::Unknown),
        (
            "<http://EX/a>",
            "^http://ex/",
            with("", Some(CaseMap::Lower), true),
            Verdict::True,
        ),
        (
            "<http://ex/a>",
            "^HTTP://EX/",
            with("", Some(CaseMap::Upper), true),
            Verdict::True,
        ),
        (
            "<http://ex/a>",
            "^http://ex/",
            with("", Some(CaseMap::Upper), true),
            Verdict::False,
        ),
        (
            "<http://ex/é>",
            "ex",
            with("", Some(CaseMap::Lower), true),
            Verdict::Unknown,
        ),
        ("<http://ex/é>", "ex", str_(), Verdict::True),
        // The text is unescaped before it is read.
        ("\"a\\nb\"", "a.b", none(), Verdict::False),
        ("\"a\\nb\"", "a.b", with("s", None, false), Verdict::True),
        ("\"a\\nb\"", "^b", with("m", None, false), Verdict::True),
        ("\"caf\\u00E9\"", "\u{e9}$", none(), Verdict::True),
        ("\"ab\\n\"", "b$", none(), Verdict::Unknown),
        ("\"ab\\n\"", "b$", with("m", None, false), Verdict::True),
        // Outside the pattern subset: a text is undecided, a non-text is not.
        ("\"ab\"", "(?=a)", none(), Verdict::Unknown),
        ("\"ab\"@en", "(?=a)", lower(), Verdict::Unknown),
        ("<http://ex/a>", "(?=a)", none(), Verdict::False),
        ("<http://ex/a>", "(?=a)", str_(), Verdict::Unknown),
        ("_:b0", "(?=a)", none(), Verdict::False),
        ("_:b0", "(?=a)", str_(), Verdict::Unknown),
        (&typed("5", "integer"), "(?=a)", none(), Verdict::False),
        // Spellings that are no term.
        ("", "a", none(), Verdict::Unknown),
        ("\"", "a", none(), Verdict::Unknown),
        ("<", "a", str_(), Verdict::Unknown),
    ] {
        let got = TermPredicate::parse_with("regex", pattern, &options)
            .unwrap()
            .eval(spelling);
        assert_eq!(got, expect, "regex {pattern:?} {options:?} on {spelling}");
    }
}

#[test]
fn string_kind_options_are_validated() {
    for (kind, arg, options) in [
        ("num_lt", "5", opts(None, true)),
        ("lang", "en", opts(Some(CaseMap::Lower), false)),
        (
            "contains",
            "\"a\"",
            TextOptions {
                flags: "i".into(),
                case: None,
                as_str: false,
            },
        ),
        ("contains", "not a spelling", opts(None, false)),
        ("contains", "\"x\"@", opts(None, false)),
    ] {
        assert!(
            TermPredicate::parse_with(kind, arg, &options).is_err(),
            "{kind} {arg:?}"
        );
    }
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

/// Spellings covering every kind, escapes and the 64-bit long rule, in a
/// Dictionary store.
async fn predicate_store() -> VortexRdfStore {
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
        typed("99999999999999999999", "long"),
        typed("-1", "unsignedLong"),
        typed("7", "long"),
        "\"abc\"@en".to_string(),
        "\"abc\"@en-us".to_string(),
        "\"Abc\"".to_string(),
        "\"a\\\"b\"".to_string(),
        "\"x\"^^<http://ex.org/dt>".to_string(),
        "_:b0".to_string(),
        "_:ab".to_string(),
    ] {
        let term = crate::common::terms::parse_term(&spelling).unwrap();
        quads.push(Quad::new(
            s.clone(),
            p.clone(),
            term,
            GraphName::DefaultGraph,
        ));
    }
    VortexRdfStore::from_quads(quad_stream(quads), LayoutStrategy::Dictionary, vec![])
        .await
        .unwrap()
}

/// Over any candidate subset, `filter_codes` answers exactly what `eval`
/// answers code by code: passed = True, undecided = Unknown, the rest False.
#[tokio::test]
async fn filter_codes_matches_eval() {
    let store = predicate_store().await;
    let dict = store.code_read_snapshot().unwrap();
    let terms: Vec<String> = (0..dict.len() as TermCode)
        .map(|c| dict.decode(c).unwrap())
        .collect();
    let all: Vec<TermCode> = (0..terms.len() as TermCode).collect();
    let odd: Vec<TermCode> = all.iter().copied().filter(|c| c % 2 == 1).collect();
    let mut cases: Vec<(&str, String, TextOptions)> = [
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
    ]
    .map(|(kind, arg)| (kind, arg.to_owned(), TextOptions::default()))
    .into();
    // The string kinds under each reading of the text and each case wrapper:
    // IRIs are read only under `STR()`, blank nodes are never decided there.
    for options in [
        opts(None, false),
        opts(None, true),
        opts(Some(CaseMap::Lower), false),
        opts(Some(CaseMap::Upper), true),
    ] {
        for (kind, arg) in [
            ("str_prefix", "a"),
            ("str_prefix", "http://example.org/s0"),
            ("contains", "\"b\""),
            ("contains", "\"b\"@en"),
            ("contains", "<http://example.org/s0>"),
            ("strstarts", "\"a\""),
            ("strstarts", "\"http\""),
            ("strends", "\"c\""),
            (
                "strends",
                "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
            ),
            ("regex", "^a"),
            ("regex", "c$"),
            ("regex", r"\d"),
            ("regex", "(?=a)"),
        ] {
            cases.push((kind, arg.to_owned(), options.clone()));
        }
    }
    cases.push((
        "regex",
        "^A".to_owned(),
        TextOptions {
            flags: "i".into(),
            ..TextOptions::default()
        },
    ));
    for (kind, arg, options) in &cases {
        let predicate = TermPredicate::parse_with(kind, arg, options).unwrap();
        for codes in [&all, &odd, &vec![]] {
            let (passed, undecided) = dict.filter_codes(&predicate, codes).unwrap();
            let (mut want_passed, mut want_undecided) = (Vec::new(), Vec::new());
            for &code in codes.iter() {
                match predicate.eval(&terms[code as usize]) {
                    Verdict::True => want_passed.push(code),
                    Verdict::Unknown => want_undecided.push(code),
                    Verdict::False => {}
                }
            }
            assert_eq!(
                passed.as_slice(),
                &want_passed[..],
                "{kind} {arg:?} {options:?}: passed"
            );
            assert_eq!(
                undecided.as_slice(),
                &want_undecided[..],
                "{kind} {arg:?} {options:?}: undecided"
            );
        }
    }
}

/// A spelling that is no term at all — what a foreign writer's dictionary can
/// hold in a kind's range — never passes and never panics, under every string
/// kind and every reading of the text. A bare `"` has no literal to parse and
/// a bare `<`, `_` or `_:` no IRI or label to read, so they are undecided;
/// `string()` still rejects the kind such a spelling looks like (an IRI or a
/// blank node), so there it fails.
#[test]
fn malformed_spellings_are_never_true() {
    for (kind, arg) in [
        ("str_prefix", "a"),
        ("str_prefix", ""),
        ("contains", "\"a\""),
        ("contains", "\"\""),
        ("strstarts", "\"a\"@en"),
        ("strends", "\"a\""),
        ("strends", "<http://ex.org/a>"),
        ("regex", "a"),
        ("regex", ""),
        ("regex", "(?=a)"),
    ] {
        for options in [
            opts(None, false),
            opts(None, true),
            opts(Some(CaseMap::Lower), false),
            opts(Some(CaseMap::Upper), true),
        ] {
            let predicate = TermPredicate::parse_with(kind, arg, &options).unwrap();
            for spelling in ["<", "<é", "_", "_:", "\"", ""] {
                let want = match spelling {
                    // No term, or a literal that does not parse.
                    "" | "\"" => Verdict::Unknown,
                    _ if options.as_str => Verdict::Unknown,
                    _ => Verdict::False,
                };
                assert_eq!(
                    predicate.eval(spelling),
                    want,
                    "{kind} {arg:?} {options:?} on {spelling:?}"
                );
            }
        }
    }
}

/// A `xsd:long` / `xsd:unsignedLong` beyond its 64-bit bounds reaches the
/// caller undecided under every comparison — rdflib compares such a value,
/// which the model refuses — while a long within bounds is decided.
#[tokio::test]
async fn filter_codes_leaves_wide_longs_undecided() {
    let store = predicate_store().await;
    let dict = store.code_read_snapshot().unwrap();
    let all: Vec<TermCode> = (0..dict.len() as TermCode).collect();
    let code = |spelling: String| dict.encode(&spelling).expect(&spelling);
    let wide = [
        code(typed("99999999999999999999", "long")),
        code(typed("-1", "unsignedLong")),
    ];
    let narrow = code(typed("7", "long"));
    for kind in ["num_lt", "num_le", "num_gt", "num_ge", "num_eq", "num_ne"] {
        let predicate = TermPredicate::parse(kind, "9").unwrap();
        let (passed, undecided) = dict.filter_codes(&predicate, &all).unwrap();
        for wide_code in wide {
            assert!(
                undecided.as_slice().contains(&wide_code),
                "{kind}: {wide_code} undecided"
            );
            assert!(
                !passed.as_slice().contains(&wide_code),
                "{kind}: {wide_code} not passed"
            );
        }
        assert!(
            !undecided.as_slice().contains(&narrow),
            "{kind}: 7 is decided"
        );
    }
}

/// Candidates must be ascending, unique and inside the dictionary.
#[tokio::test]
async fn filter_codes_rejects_bad_candidates() {
    let store = predicate_store().await;
    let dict = store.code_read_snapshot().unwrap();
    let predicate = TermPredicate::parse("is_iri", "").unwrap();
    let len = dict.len() as TermCode;
    for codes in [vec![3u64, 1], vec![1, 1], vec![0, len]] {
        assert!(
            matches!(
                dict.filter_codes(&predicate, &codes),
                Err(crate::VortexRdfError::InvalidOperation(_))
            ),
            "{codes:?}"
        );
    }
    let (passed, undecided) = dict.filter_codes(&predicate, &[]).unwrap();
    assert!(passed.is_empty() && undecided.is_empty());
}
