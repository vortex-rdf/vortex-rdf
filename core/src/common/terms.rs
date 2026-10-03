//! RDF term parsing and reconstruction: N-Triples spellings into `oxrdf`
//! terms, under the trust level of their source, and RDF documents into
//! [`RawQuad`] streams.

use crate::common::quad::RawQuad;
use crate::error::{Result, VortexRdfError};

use std::borrow::Cow;

use futures::{Stream, stream};
use oxrdf::{BlankNode, GraphName, Literal, NamedNode, NamedOrBlankNode, Quad, Term};
use oxrdfio::{RdfFormat, RdfParser};

/// Where a spelling comes from. `Stored` spellings are the store's own
/// columns, validated at ingest: they decode through the `new_unchecked`
/// constructors and a malformed literal reads leniently. `Input` spellings
/// are user-typed (pattern arguments, [`canonical_spelling`]): every
/// component is validated, a bare IRI without angle brackets is a named node,
/// and the default graph may be spelled `""`, `default` (any case) or `[]`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Trust {
    Stored,
    Input,
}

fn invalid(what: &str, spelling: &str, error: impl std::fmt::Display) -> VortexRdfError {
    VortexRdfError::Deserialization(format!("invalid {what} {spelling:?}: {error}"))
}

/// `<iri>` (the brackets optional) as a named node.
pub(crate) fn named_node(s: &str, trust: Trust) -> Result<NamedNode> {
    let iri = s.trim_matches(|c| c == '<' || c == '>');
    match trust {
        Trust::Stored => Ok(NamedNode::new_unchecked(iri)),
        Trust::Input => NamedNode::new(iri).map_err(|e| invalid("IRI", iri, e)),
    }
}

/// `_:id` as a blank node.
fn blank_node(s: &str, trust: Trust) -> Result<BlankNode> {
    let id = s.trim_start_matches("_:");
    match trust {
        Trust::Stored => Ok(BlankNode::new_unchecked(id)),
        Trust::Input => BlankNode::new(id).map_err(|e| invalid("blank node", s, e)),
    }
}

/// A named node or a `_:` blank node.
pub(crate) fn subject(s: &str, trust: Trust) -> Result<NamedOrBlankNode> {
    if s.starts_with("_:") {
        Ok(NamedOrBlankNode::BlankNode(blank_node(s, trust)?))
    } else {
        Ok(NamedOrBlankNode::NamedNode(named_node(s, trust)?))
    }
}

/// The three N-Triples literal shapes, `value` still in its escaped lexical
/// form (the slice between the quotes).
enum LiteralForm<'a> {
    Simple { value: &'a str },
    Language { value: &'a str, lang: &'a str },
    Typed { value: &'a str, datatype: &'a str },
}

/// Byte offset of the literal's closing quote, honouring `\` escapes; `None`
/// if `s` does not start with `"` or is unterminated. A quote closes the
/// literal when the run of backslashes directly before it has even length.
fn closing_quote(s: &str) -> Option<usize> {
    let b = s.as_bytes();
    if b.first() != Some(&b'"') {
        return None;
    }
    // Both delimiters are ASCII, so every index here is a char boundary.
    let mut from = 1;
    loop {
        let quote = s[from..].find('"')? + from;
        let mut run = quote;
        while run > 1 && b[run - 1] == b'\\' {
            run -= 1;
        }
        if (quote - run) % 2 == 0 {
            return Some(quote);
        }
        from = quote + 1;
    }
}

/// `s` split into its escaped value and its suffix: none means simple,
/// `^^<dt>` typed, `@lang` language-tagged. `None` for a malformed form
/// (unterminated, or trailing text that is neither suffix). The suffix is
/// read only after the closing quote.
fn split_literal(s: &str) -> Option<LiteralForm<'_>> {
    let end = closing_quote(s)?;
    let value = &s[1..end];
    let rest = &s[end + 1..];
    if rest.is_empty() {
        return Some(LiteralForm::Simple { value });
    }
    if let Some(datatype) = rest.strip_prefix("^^") {
        return Some(LiteralForm::Typed { value, datatype });
    }
    rest.strip_prefix('@')
        .map(|lang| LiteralForm::Language { value, lang })
}

/// Decodes the N-Triples escapes of a literal's lexical value: `\\`, `\"`,
/// `\'`, `\n`, `\r`, `\t`, `\b`, `\f`, `\uXXXX` and `\UXXXXXXXX`. Borrows
/// when there is no backslash; a backslash starting no recognized escape (or
/// a truncated or invalid `\u`) is kept verbatim.
fn unescape_literal_value(s: &str) -> Cow<'_, str> {
    let b = s.as_bytes();
    if !b.contains(&b'\\') {
        return Cow::Borrowed(s);
    }
    let mut out = String::with_capacity(s.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] != b'\\' {
            let start = i;
            while i < b.len() && b[i] != b'\\' {
                i += 1;
            }
            out.push_str(&s[start..i]);
            continue;
        }
        let decoded = match b.get(i + 1) {
            Some(b't') => Some(('\t', 2)),
            Some(b'b') => Some(('\u{8}', 2)),
            Some(b'n') => Some(('\n', 2)),
            Some(b'r') => Some(('\r', 2)),
            Some(b'f') => Some(('\u{c}', 2)),
            Some(b'"') => Some(('"', 2)),
            Some(b'\'') => Some(('\'', 2)),
            Some(b'\\') => Some(('\\', 2)),
            Some(b'u') => hex_escape(b, i + 2, 4).map(|c| (c, 6)),
            Some(b'U') => hex_escape(b, i + 2, 8).map(|c| (c, 10)),
            _ => None,
        };
        match decoded {
            Some((c, width)) => {
                out.push(c);
                i += width;
            }
            None => {
                out.push('\\');
                i += 1;
            }
        }
    }
    Cow::Owned(out)
}

/// The character `len` hex digits at `at` encode; `None` if truncated, not
/// hex, or not a scalar value.
fn hex_escape(b: &[u8], at: usize, len: usize) -> Option<char> {
    let digits = b.get(at..at + len)?;
    let mut cp: u32 = 0;
    for &d in digits {
        cp = cp * 16 + (d as char).to_digit(16)?;
    }
    char::from_u32(cp)
}

/// A simple (`"v"`), language-tagged (`"v"@lang`) or typed (`"v"^^<dt>`)
/// literal. A malformed form reads as a quote-trimmed simple literal when
/// `Stored` and is an error when `Input`.
fn literal(s: &str, trust: Trust) -> Result<Literal> {
    match split_literal(s) {
        Some(LiteralForm::Simple { value }) => {
            Ok(Literal::new_simple_literal(unescape_literal_value(value)))
        }
        Some(LiteralForm::Language { value, lang }) => match trust {
            Trust::Stored => Ok(Literal::new_language_tagged_literal_unchecked(
                unescape_literal_value(value),
                lang,
            )),
            Trust::Input => {
                Literal::new_language_tagged_literal(unescape_literal_value(value), lang)
                    .map_err(|e| invalid("language tag", lang, e))
            }
        },
        Some(LiteralForm::Typed { value, datatype }) => Ok(Literal::new_typed_literal(
            unescape_literal_value(value),
            named_node(datatype, trust)?,
        )),
        None => match trust {
            Trust::Stored => Ok(Literal::new_simple_literal(s.trim_matches('"'))),
            Trust::Input => Err(VortexRdfError::Deserialization(format!(
                "malformed literal {:?}",
                s
            ))),
        },
    }
}

/// Whether `s` is a user-typed default-graph spelling: `""`, `default` (any
/// case) or `[]`.
fn is_default_graph_spelling(s: &str) -> bool {
    s.is_empty() || s.eq_ignore_ascii_case("default") || s == "[]"
}

/// A graph name as the columns store it: `""` is the default graph (`Input`
/// also takes `default` and `[]`), else a named or blank node.
pub(crate) fn graph_name(s: &str, trust: Trust) -> Result<GraphName> {
    let default = match trust {
        Trust::Stored => s.is_empty(),
        Trust::Input => is_default_graph_spelling(s),
    };
    if default {
        Ok(GraphName::DefaultGraph)
    } else if s.starts_with("_:") {
        Ok(GraphName::BlankNode(blank_node(s, trust)?))
    } else {
        Ok(GraphName::NamedNode(named_node(s, trust)?))
    }
}

/// `<iri>`, `_:id` or a literal; `Input` also reads a bare IRI as a named
/// node. Any other spelling is an error.
pub(crate) fn term(s: &str, trust: Trust) -> Result<Term> {
    if s.starts_with("_:") {
        Ok(Term::BlankNode(blank_node(s, trust)?))
    } else if s.starts_with('"') {
        Ok(Term::Literal(literal(s, trust)?))
    } else if trust == Trust::Input || s.starts_with('<') {
        Ok(Term::NamedNode(named_node(s, trust)?))
    } else {
        Err(VortexRdfError::Deserialization(format!(
            "invalid term {s:?}: not an IRI, blank node or literal"
        )))
    }
}

/// A quad from its four stored N-Triples term strings (`g` empty for the
/// default graph).
pub(crate) fn quad_from_terms(s: &str, p: &str, o: &str, g: &str) -> Result<Quad> {
    Ok(Quad::new(
        subject(s, Trust::Stored)?,
        named_node(p, Trust::Stored)?,
        term(o, Trust::Stored)?,
        graph_name(g, Trust::Stored)?,
    ))
}

/// A parsed quad pattern: the four term positions, each bound (`Some`) or
/// free (`None`); what [`parse_pattern_checked`] returns and
/// `VortexRdfStore::match_pattern` borrows.
pub type Pattern = (
    Option<NamedOrBlankNode>,
    Option<NamedNode>,
    Option<Term>,
    Option<GraphName>,
);

/// Parses a user-typed quad pattern, the four optional term strings every
/// frontend's match surface accepts, as validated input: a subject is a
/// named or blank node, a predicate a named node, an object any term (a bare
/// IRI included), a graph a named or blank node or a default-graph spelling
/// (`""`, `default`, `[]`). A `None` slot stays free; the first invalid
/// slot's error is returned as is.
pub fn parse_pattern_checked(
    s: Option<&str>,
    p: Option<&str>,
    o: Option<&str>,
    g: Option<&str>,
) -> Result<Pattern> {
    Ok((
        s.map(|s| subject(s, Trust::Input)).transpose()?,
        p.map(|p| named_node(p, Trust::Input)).transpose()?,
        o.map(|o| term(o, Trust::Input)).transpose()?,
        g.map(|g| graph_name(g, Trust::Input)).transpose()?,
    ))
}

/// The spelling the dictionary and the columns hold for a user-typed term:
/// the validated parse of `spelling` (an IRI with or without angle brackets,
/// a `_:` blank node, or an escape-aware literal) rendered back in storage
/// form — `xsd:string` typing dropped, the language tag lowercased, escapes
/// normalized — and the default graph's spellings (`""`, `default`, `[]`) as
/// the empty string the `g` column stores. Malformed input is an error.
pub fn canonical_spelling(spelling: &str) -> Result<String> {
    if is_default_graph_spelling(spelling) {
        return Ok(String::new());
    }
    Ok(term(spelling, Trust::Input)?.to_string())
}

/// Parses a stream of RDF quads from any reader in `format`, as
/// [`RawQuad`]s.
pub fn parse_quads_from_reader<R: std::io::Read + Send + 'static>(
    reader: R,
    format: RdfFormat,
) -> impl Stream<Item = Result<RawQuad>> {
    let parser = RdfParser::from_format(format);
    let iter = parser.for_reader(reader).map(|x| {
        x.map(|q| RawQuad::from_quad(&q))
            .map_err(|e| VortexRdfError::Deserialization(format!("Parse error: {}", e)))
    });
    stream::iter(iter)
}

/// Term parsing from serialized N-Triples strings and the escape-aware
/// structure scan the literal decoders are built on. Store-level
/// escaped-literal round trips live in `crate::tests::escaping`, whose case
/// list the parser-agreement test borrows.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::escaping::escaped_literal_cases;
    use futures::StreamExt;

    #[test]
    fn stored_term_reads_each_literal_shape() {
        let dt = NamedNode::new("http://www.w3.org/2001/XMLSchema#integer").unwrap();
        for (serialized, expected) in [
            (
                "\"Alice\"",
                Term::Literal(Literal::new_simple_literal("Alice")),
            ),
            (
                "\"Bob\"@en",
                Term::Literal(Literal::new_language_tagged_literal("Bob", "en").unwrap()),
            ),
            (
                "\"42\"^^<http://www.w3.org/2001/XMLSchema#integer>",
                Term::Literal(Literal::new_typed_literal("42", dt)),
            ),
        ] {
            assert_eq!(
                term(serialized, Trust::Stored).unwrap(),
                expected,
                "{}",
                serialized
            );
        }
    }

    #[test]
    fn stored_term_named_and_blank_nodes() {
        assert_eq!(
            term("<http://example.org/x>", Trust::Stored).unwrap(),
            Term::NamedNode(NamedNode::new("http://example.org/x").unwrap())
        );
        assert!(matches!(
            term("_:b0", Trust::Stored).unwrap(),
            Term::BlankNode(b) if b.as_str() == "b0"
        ));
        // Only the stored forms are terms; anything else is an error.
        assert!(term("http://example.org/x", Trust::Stored).is_err());
        assert!(term("not a term", Trust::Stored).is_err());
    }

    #[test]
    fn parse_pattern_checked_binds_each_slot() {
        let (s, p, o, g) = parse_pattern_checked(
            Some("_:b0"),
            Some("<http://example.org/p>"),
            Some("\"v\"@en"),
            Some("http://example.org/g"),
        )
        .unwrap();
        assert!(matches!(s, Some(NamedOrBlankNode::BlankNode(b)) if b.as_str() == "b0"));
        assert_eq!(p, Some(NamedNode::new("http://example.org/p").unwrap()));
        assert_eq!(
            o,
            Some(Term::Literal(
                Literal::new_language_tagged_literal("v", "en").unwrap()
            ))
        );
        assert_eq!(
            g,
            Some(GraphName::NamedNode(
                NamedNode::new("http://example.org/g").unwrap()
            ))
        );

        // A bare IRI (no angle brackets) in the object slot takes the
        // named-node arm.
        assert_eq!(
            term("http://example.org/x", Trust::Input).unwrap(),
            Term::NamedNode(NamedNode::new("http://example.org/x").unwrap())
        );

        // A `None` slot stays free; "default" names the default graph.
        let (s, p, o, g) = parse_pattern_checked(None, None, None, Some("default")).unwrap();
        assert!(s.is_none() && p.is_none() && o.is_none());
        assert_eq!(g, Some(GraphName::DefaultGraph));
    }

    /// The user-typed default-graph spellings belong to the input form
    /// only; the stored form reads the `""` the columns store.
    #[test]
    fn default_graph_spellings_split_between_stored_and_input() {
        for s in ["", "default", "DEFAULT", "[]"] {
            assert_eq!(
                graph_name(s, Trust::Input).unwrap(),
                GraphName::DefaultGraph,
                "{s:?}"
            );
        }
        assert_eq!(
            graph_name("", Trust::Stored).unwrap(),
            GraphName::DefaultGraph
        );
        for s in ["default", "[]"] {
            assert!(
                matches!(
                    graph_name(s, Trust::Stored).unwrap(),
                    GraphName::NamedNode(_)
                ),
                "{s:?}"
            );
        }
    }

    /// Pattern slots are user-typed: an invalid term in any slot must error
    /// rather than silently match nothing.
    #[test]
    fn parse_pattern_checked_rejects_invalid_slots() {
        assert!(parse_pattern_checked(Some("no spaces allowed"), None, None, None).is_err());
        assert!(parse_pattern_checked(None, Some("not an iri"), None, None).is_err());
        assert!(parse_pattern_checked(None, None, Some("\"unterminated"), None).is_err());
        assert!(parse_pattern_checked(None, None, None, Some("bad graph iri")).is_err());
    }

    #[test]
    fn stored_and_input_parses_agree_on_escaped_literals() {
        for expected in escaped_literal_cases() {
            let s = expected.to_string();
            assert_eq!(
                term(&s, Trust::Stored).unwrap(),
                expected,
                "stored parse of {}",
                s
            );
            assert_eq!(
                term(&s, Trust::Input).unwrap(),
                expected,
                "input parse of {}",
                s
            );
        }
    }

    #[test]
    fn structure_scan_ignores_suffix_lookalikes_inside_the_value() {
        // `"@` and `^^` inside the value must not be read as structure: both
        // parses see a simple literal, and the input one does not error.
        for (serialized, value) in [
            ("\"say \\\"hi\\\"@home\"", "say \"hi\"@home"),
            ("\"a ^^ b\"", "a ^^ b"),
            (
                "\"\\\"^^<http://example.org/nope>\"",
                "\"^^<http://example.org/nope>",
            ),
        ] {
            let expected = Term::Literal(Literal::new_simple_literal(value));
            assert_eq!(term(serialized, Trust::Stored).unwrap(), expected);
            assert_eq!(term(serialized, Trust::Input).unwrap(), expected);
        }
    }

    #[test]
    fn escape_sequences_decode_in_both_paths() {
        for (serialized, value) in [
            ("\"a\\u0041b\"", "aAb"),
            ("\"\\U0001F600\"", "\u{1F600}"),
            ("\"\\b\\f\"", "\u{8}\u{c}"),
            ("\"\\'\"", "'"),
            ("\"\\t\\n\\r\"", "\t\n\r"),
            ("\"\\\\\\\"\"", "\\\""),
        ] {
            let expected = Term::Literal(Literal::new_simple_literal(value));
            assert_eq!(
                term(serialized, Trust::Stored).unwrap(),
                expected,
                "{}",
                serialized
            );
            assert_eq!(
                term(serialized, Trust::Input).unwrap(),
                expected,
                "{}",
                serialized
            );
        }
    }

    /// A backslash that starts no recognized escape, or a truncated or
    /// non-scalar `\u`, is kept as written.
    #[test]
    fn unrecognized_escapes_are_preserved_verbatim() {
        for (serialized, value) in [
            ("\"\\q\"", "\\q"),
            ("\"\\u12\"", "\\u12"),
            ("\"\\uD800\"", "\\uD800"),
        ] {
            let expected = Term::Literal(Literal::new_simple_literal(value));
            assert_eq!(
                term(serialized, Trust::Stored).unwrap(),
                expected,
                "{}",
                serialized
            );
            assert_eq!(
                term(serialized, Trust::Input).unwrap(),
                expected,
                "{}",
                serialized
            );
        }
    }

    #[test]
    fn escaped_suffixes_are_read_from_after_the_closing_quote() {
        let dt = NamedNode::new("http://example.org/dt").unwrap();
        assert_eq!(
            term("\"a\\\"b\"^^<http://example.org/dt>", Trust::Stored).unwrap(),
            Term::Literal(Literal::new_typed_literal("a\"b", dt))
        );
        assert_eq!(
            term("\"a\\\"b\"@en", Trust::Stored).unwrap(),
            Term::Literal(Literal::new_language_tagged_literal("a\"b", "en").unwrap())
        );
    }

    /// A quote closes the literal only when the backslash run directly
    /// before it has even length.
    #[test]
    fn backslash_run_parity_decides_the_closing_quote() {
        // Serialized form -> the value it denotes, over runs of length 1..=3
        // ending at a quote.
        let cases = [
            ("\"a\\\\\"", "a\\"),           // "a\\"      -> a\
            ("\"a\\\"b\"", "a\"b"),         // "a\"b"     -> a"b
            ("\"a\\\\\\\"b\"", "a\\\"b"),   // "a\\\"b"   -> a\"b
            ("\"\\\\\\\\\"", "\\\\"),       // "\\\\"     -> \\
            ("\"\\\\\\\\\\\"\"", "\\\\\""), // "\\\\\""   -> \\"
        ];
        for (serialized, value) in cases {
            let expected = Term::Literal(Literal::new_simple_literal(value));
            assert_eq!(
                term(serialized, Trust::Stored).unwrap(),
                expected,
                "{}",
                serialized
            );
            assert_eq!(
                term(serialized, Trust::Input).unwrap(),
                expected,
                "{}",
                serialized
            );
            // And the value must render back to exactly the form we started from.
            assert_eq!(expected.to_string(), serialized);
        }
    }

    /// Every tolerated variant of a term lands on the one spelling the
    /// columns store.
    #[test]
    fn canonical_spelling_normalizes_tolerated_variants() {
        for (typed, stored) in [
            ("<http://example.org/x>", "<http://example.org/x>"),
            ("http://example.org/x", "<http://example.org/x>"),
            ("_:b0", "_:b0"),
            ("\"v\"", "\"v\""),
            ("\"v\"^^<http://www.w3.org/2001/XMLSchema#string>", "\"v\""),
            ("\"v\"@EN-gb", "\"v\"@en-gb"),
            ("\"a\\u0041b\"", "\"aAb\""),
            ("\"q\\'\"", "\"q'\""),
            (
                "\"42\"^^<http://www.w3.org/2001/XMLSchema#integer>",
                "\"42\"^^<http://www.w3.org/2001/XMLSchema#integer>",
            ),
            ("", ""),
            ("default", ""),
            ("DEFAULT", ""),
            ("[]", ""),
        ] {
            assert_eq!(canonical_spelling(typed).unwrap(), stored, "{typed:?}");
        }
        for bad in ["not an iri", "\"unterminated", "\"v\"@not a tag"] {
            assert!(canonical_spelling(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn malformed_literals_are_lenient_when_stored_and_rejected_when_input() {
        for s in ["\"unterminated", "\"a\" trailing", "\"a\"\\"] {
            // The stored decode must not panic and must yield a term.
            assert!(
                matches!(term(s, Trust::Stored).unwrap(), Term::Literal(_)),
                "{}",
                s
            );
            assert!(term(s, Trust::Input).is_err(), "{}", s);
        }
    }

    const NQUADS: &str = "\
<http://example.org/s> <http://example.org/p> \"say \\\"hi\\\"\\n\" .
<http://example.org/s> <http://example.org/p> \"bonjour\"@fr <http://example.org/g> .
_:b0 <http://example.org/p> \"7\"^^<http://www.w3.org/2001/XMLSchema#integer> .
<http://example.org/s> <http://example.org/p> _:b0 <http://example.org/g> .
";

    /// The textual ingest boundary yields exactly what oxrdfio's own parse
    /// does, in `RawQuad` form.
    #[tokio::test]
    async fn parse_quads_from_reader_matches_oxrdfio() {
        let fields = |q: RawQuad| (q.s, q.p, q.o, q.g);
        let expected: Vec<_> = RdfParser::from_format(RdfFormat::NQuads)
            .for_reader(NQUADS.as_bytes())
            .map(|q| fields(RawQuad::from_quad(&q.unwrap())))
            .collect();
        assert_eq!(expected.len(), 4);
        let parsed: Vec<_> = parse_quads_from_reader(NQUADS.as_bytes(), RdfFormat::NQuads)
            .map(|q| fields(q.unwrap()))
            .collect()
            .await;
        assert_eq!(parsed, expected);
        assert_eq!(parsed[0].2, "\"say \\\"hi\\\"\\n\"");
        assert_eq!(parsed[1].3, "<http://example.org/g>");
        assert_eq!(parsed[2].0, "_:b0");
    }

    #[tokio::test]
    async fn parse_quads_from_reader_reports_a_malformed_document() {
        let results: Vec<Result<RawQuad>> = parse_quads_from_reader(
            "<http://example.org/s> nonsense".as_bytes(),
            RdfFormat::NQuads,
        )
        .collect()
        .await;
        let err = results
            .into_iter()
            .find_map(|r| r.err())
            .expect("a malformed document must yield an error");
        match err {
            VortexRdfError::Deserialization(msg) => {
                assert!(msg.contains("Parse error"), "{msg}")
            }
            other => panic!("expected a Deserialization error, got {other:?}"),
        }
    }
}
