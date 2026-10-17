//! RDF term parsing and reconstruction: N-Triples term spellings — the
//! store's own columns or user-typed input — into `oxrdf` terms, and RDF
//! documents into [`RawQuad`] streams.

use crate::common::quad::RawQuad;
use crate::error::{Result, VortexRdfError};

use std::borrow::Cow;

use futures::{Stream, stream};
use oxrdf::{BlankNode, GraphName, Literal, NamedNode, NamedOrBlankNode, Quad, Term};
use oxrdfio::{RdfFormat, RdfParser};

/// Where a term spelling comes from, which decides how much of it the
/// parsers below validate.
///
/// The parsers that take a `Trust` are `#[inline(always)]`: a call site's
/// constant trust then folds away every branch on it, so the `Stored` decode
/// path carries none of the `Input` validation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Trust {
    /// A spelling from the store's *own* serialized columns (see
    /// [`crate::store::layouts`]), whose terms oxrdf's constructors validated
    /// at ingestion; `.vortex` files are likewise trusted to have been
    /// checked when written. Nodes are built with the `new_unchecked`
    /// constructors, so no IRI is re-parsed on decode, and a malformed
    /// literal (which the store's writer cannot produce) reads as the
    /// quote-trimmed simple literal. Only `""` is the default graph, and a
    /// term is `<iri>`, `_:id` or a literal.
    Stored,
    /// A spelling that did not come out of the store's columns: CLI pattern
    /// arguments, binding call arguments, anything a user typed. Every
    /// component goes through a validating constructor ([`NamedNode::new`],
    /// [`BlankNode::new`], [`Literal::new_language_tagged_literal`]), a bare
    /// IRI (no angle brackets) is a named node, the default graph may also be
    /// spelled `default` (any case) or `[]`, and a malformed literal is an
    /// error. Every refusal is an
    /// [`InvalidInput`](VortexRdfError::InvalidInput) error.
    Input,
}

/// The error for the user-typed `spelling`, whose `what` (an IRI, a blank
/// node, a language tag) a validating constructor refused with `error`.
fn invalid(what: &str, spelling: &str, error: impl std::fmt::Display) -> VortexRdfError {
    VortexRdfError::InvalidInput(format!("invalid {what} {spelling:?}: {error}"))
}

/// `parsed` with its [`InvalidInput`](VortexRdfError::InvalidInput) message
/// led by `place`: the term parsers report an invalid term without saying
/// which position of a quad or a pattern held it.
pub(crate) fn at<T>(place: &str, parsed: Result<T>) -> Result<T> {
    parsed.map_err(|error| match error {
        VortexRdfError::InvalidInput(message) => {
            VortexRdfError::InvalidInput(format!("{place}: {message}"))
        }
        other => other,
    })
}

/// A named node from `<iri>`, the angle brackets optional.
#[inline(always)]
pub(crate) fn named_node(s: &str, trust: Trust) -> Result<NamedNode> {
    let iri = s.trim_matches(|c| c == '<' || c == '>');
    match trust {
        Trust::Stored => Ok(NamedNode::new_unchecked(iri)),
        Trust::Input => NamedNode::new(iri).map_err(|e| invalid("IRI", iri, e)),
    }
}

/// A blank node from `_:id`, the prefix optional.
#[inline(always)]
fn blank_node(s: &str, trust: Trust) -> Result<BlankNode> {
    let id = s.trim_start_matches("_:");
    match trust {
        Trust::Stored => Ok(BlankNode::new_unchecked(id)),
        Trust::Input => BlankNode::new(id).map_err(|e| invalid("blank node", s, e)),
    }
}

/// A subject: a `_:` blank node, otherwise a named node.
#[inline(always)]
pub(crate) fn subject(s: &str, trust: Trust) -> Result<NamedOrBlankNode> {
    if s.starts_with("_:") {
        Ok(NamedOrBlankNode::BlankNode(blank_node(s, trust)?))
    } else {
        Ok(NamedOrBlankNode::NamedNode(named_node(s, trust)?))
    }
}

/// The three N-Triples literal shapes, with `value` still in its *escaped*
/// lexical form — the slice between the opening and closing quote.
pub(crate) enum LiteralForm<'a> {
    Simple { value: &'a str },
    Language { value: &'a str, lang: &'a str },
    Typed { value: &'a str, datatype: &'a str },
}

/// Byte offset of the literal's closing quote, honouring `\` escapes, or
/// `None` if `s` does not start with `"` or is unterminated.
///
/// A quote terminates the literal only when the run of backslashes directly
/// before it has even length; an odd run means the last one escapes it. The
/// scan jumps quote to quote with the memchr-accelerated `str::find` and
/// counts the backslash run behind each candidate.
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

/// Splits a serialized literal into its escaped value and its suffix
/// interpretation: empty suffix => simple, `^^<dt>` => typed, `@lang` =>
/// language-tagged.
///
/// `None` means the form is malformed — unterminated, or trailing text after
/// the closing quote that is neither suffix. The suffix is read only from
/// *after* the closing quote, so `^^` or `"@` occurring inside the value
/// cannot be mistaken for structure.
pub(crate) fn split_literal(s: &str) -> Option<LiteralForm<'_>> {
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
/// `\'`, `\n`, `\r`, `\t`, `\b`, `\f`, `\uXXXX` and `\UXXXXXXXX`.
///
/// Borrows when there is no backslash — the common case on the hot
/// [`Trust::Stored`] decode path, which must stay allocation-free. A
/// backslash that starts no recognized escape (or a truncated/invalid `\u`)
/// is preserved verbatim, so this never loses input.
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

/// The character `len` hex digits at `at` encode, or `None` if they are
/// truncated, not hex, or not a scalar value.
fn hex_escape(b: &[u8], at: usize, len: usize) -> Option<char> {
    let digits = b.get(at..at + len)?;
    let mut cp: u32 = 0;
    for &d in digits {
        cp = cp * 16 + (d as char).to_digit(16)?;
    }
    char::from_u32(cp)
}

/// A literal from its serialized N-Triples form: simple (`"v"`),
/// language-tagged (`"v"@lang`), or typed (`"v"^^<dt>`), its value
/// unescaped.
///
/// Both trust levels share the escape-aware structure scan and the
/// unescaping, so they yield the same literal for the same well-formed
/// input; [`Trust::Input`] also validates the language tag and the datatype
/// IRI. A malformed form reads as the quote-trimmed simple literal under
/// [`Trust::Stored`] (never a panic) and is an error under [`Trust::Input`].
#[inline(always)]
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
            Trust::Input => Err(VortexRdfError::InvalidInput(format!(
                "malformed literal {s:?}"
            ))),
        },
    }
}

/// Whether `s` is a user-typed spelling of the default graph: `""`,
/// `default` (any case) or `[]`.
fn is_default_graph_spelling(s: &str) -> bool {
    s.is_empty() || s.eq_ignore_ascii_case("default") || s == "[]"
}

/// A graph name: the default graph, a `_:` blank node, otherwise a named
/// node. The columns store the default graph as `""`, the one spelling
/// [`Trust::Stored`] reads as it; [`Trust::Input`] takes every
/// [`is_default_graph_spelling`].
#[inline(always)]
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

/// A term from its N-Triples form: `<iri>`, `_:id`, or a simple,
/// language-tagged or typed [`literal`]. [`Trust::Input`] also reads a bare
/// IRI (no angle brackets) as a named node; under [`Trust::Stored`] a
/// spelling in none of those forms is a
/// [`Deserialization`](VortexRdfError::Deserialization) error.
#[inline(always)]
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
/// default graph), parsed as [`Trust::Stored`] spellings.
pub(crate) fn quad_from_terms(s: &str, p: &str, o: &str, g: &str) -> Result<Quad> {
    Ok(Quad::new(
        subject(s, Trust::Stored)?,
        named_node(p, Trust::Stored)?,
        term(o, Trust::Stored)?,
        graph_name(g, Trust::Stored)?,
    ))
}

/// The spelling the dictionary and the columns hold for a user-typed term:
/// the validated parse of `spelling` (an IRI with or without angle brackets,
/// a `_:` blank node, or an escape-aware literal) rendered back through the
/// storage form — `xsd:string` typing dropped, the language tag lowercased,
/// escapes normalized — and the default graph's spellings (`""`, `default`,
/// `[]`) as the empty string the `g` column stores. Malformed input is an
/// error; a dictionary that holds the term under *some* spelling holds it
/// under this one.
pub fn canonical_spelling(spelling: &str) -> Result<String> {
    if is_default_graph_spelling(spelling) {
        return Ok(String::new());
    }
    Ok(term(spelling, Trust::Input)?.to_string())
}

/// A quad from four term spellings in any of the forms a person types or a
/// foreign system writes, rendered in the one canonical spelling the builders
/// intern ([`RawQuad::canonical`]).
///
/// Each position is parsed as [`Trust::Input`], as a pattern's slots are, and
/// so takes the same tolerant spellings as [`canonical_spelling`] (an IRI with
/// or without angle brackets, an `xsd:string`-typed or escaped literal, an
/// upper-case language tag, `""`, `default` or `[]` for the default graph),
/// and the quad is rendered by [`RawQuad::from_quad`], so a term comes out
/// exactly as `canonical_spelling` renders it. Unlike `canonical_spelling` it
/// also requires each term to be one its position can hold: a literal is no
/// subject, predicate or graph, and a blank node is no predicate. A failure
/// names the position.
pub(crate) fn canonical_raw_quad(s: &str, p: &str, o: &str, g: &str) -> Result<RawQuad> {
    Ok(RawQuad::from_quad(&Quad::new(
        at("quad subject", subject(s, Trust::Input))?,
        at("quad predicate", named_node(p, Trust::Input))?,
        at("quad object", term(o, Trust::Input))?,
        at("quad graph", graph_name(g, Trust::Input))?,
    )))
}

/// Parses a stream of RDF quads from any reader using the specified RDF format.
///
/// Yields [`RawQuad`]: every builder converts to
/// `RawQuad` as its first act, so handing back the parsed `Quad` would keep a
/// second owned copy of every term alive for no purpose. Converting here lets
/// the `Quad` die inside the map.
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

/// Mechanism tests for the parsers above: term parsing from serialized
/// N-Triples strings, and the escape-aware structure scan the literal
/// decoders are built on (a documented perf-sensitive area — see
/// [`closing_quote`]). Store-level escaped-literal round-trips live in the
/// central suite (`crate::tests::escaping`), whose shared case list the
/// parser-agreement test below borrows.
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
        // Only the stored forms are terms; anything else is a store that does
        // not read, never invalid input.
        for s in ["http://example.org/x", "not a term"] {
            assert!(
                matches!(
                    term(s, Trust::Stored),
                    Err(VortexRdfError::Deserialization(_))
                ),
                "{s:?}"
            );
        }
    }

    /// The user-typed default-graph spellings belong to the `Input` form
    /// only; the `Stored` form reads the `""` the columns store.
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
        // parses see a simple literal, and the `Input` one does not error.
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
    /// non-scalar `\u`, is kept as written — the decoder never loses input.
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

    /// A quote closes the literal only when the backslash run directly before it
    /// has even length — the property the closing-quote scan is built on.
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

    /// The canonical spelling is what the columns store: every tolerated
    /// variant of a term lands on the one spelling the dictionary holds.
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
            // The `Stored` decode must not panic and must yield a term.
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
