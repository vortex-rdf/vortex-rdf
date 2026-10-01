//! RDF term parsing and reconstruction: the store's serialized N-Triples
//! strings back into `oxrdf` terms, and RDF documents into [`RawQuad`]
//! streams.

use crate::common::quad::RawQuad;
use crate::error::{Result, VortexRdfError};

use std::borrow::Cow;

use futures::{Stream, stream};
use oxrdf::{BlankNode, GraphName, Literal, NamedNode, NamedOrBlankNode, Quad, Term};
use oxrdfio::{RdfFormat, RdfParser};

/// Parses a string representation of an RDF named node (URI), stripping optional `<` and `>` boundaries.
///
/// **Trusted-input decode path.** Every caller reconstructs a term from the
/// store's *own* serialized columns (see [`crate::store::layouts`]), whose
/// IRIs were validated by oxrdf's constructors at ingestion, so the node is
/// built with [`NamedNode::new_unchecked`] and no IRI is re-parsed on decode.
/// `.vortex` files are likewise trusted to have been checked when written.
/// The `Result` is kept so the decode call sites (which `?` on genuinely
/// fallible neighbours like `buf_as_str`) stay uniform.
pub(crate) fn parse_named_node(s: &str) -> Result<NamedNode> {
    let s = s.trim_matches(|c| c == '<' || c == '>');
    Ok(NamedNode::new_unchecked(s))
}

/// Parses a string representation of an RDF blank node, stripping the `_:` prefix
/// if present. Trusted-input decode path — see [`parse_named_node`].
fn parse_blank_node(s: &str) -> Result<BlankNode> {
    let s = s.trim_start_matches("_:");
    Ok(BlankNode::new_unchecked(s))
}

/// Parses an RDF subject node, which can either be a NamedNode (URI) or a BlankNode.
pub(crate) fn parse_subject(s: &str) -> Result<NamedOrBlankNode> {
    if s.starts_with("_:") {
        Ok(NamedOrBlankNode::BlankNode(parse_blank_node(s)?))
    } else {
        Ok(NamedOrBlankNode::NamedNode(parse_named_node(s)?))
    }
}

/// The three N-Triples literal shapes, with `value` still in its *escaped*
/// lexical form — the slice between the opening and closing quote.
enum LiteralForm<'a> {
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
/// `\'`, `\n`, `\r`, `\t`, `\b`, `\f`, `\uXXXX` and `\UXXXXXXXX`.
///
/// Borrows when there is no backslash — the common case on the hot trusted
/// decode path, which must stay allocation-free. A backslash that starts no
/// recognized escape (or a truncated/invalid `\u`) is preserved verbatim, so
/// this never loses input.
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

/// Reconstructs a literal from its serialized N-Triples form: simple
/// (`"v"`), language-tagged (`"v"@lang`), or typed (`"v"^^<dt>`). Trusted
/// decode path — see [`parse_named_node`].
///
/// Infallible: a malformed form (which our own writer cannot produce) falls
/// back to the lenient quote-trimming read rather than panicking.
fn literal_from_serialized(s: &str) -> Literal {
    match split_literal(s) {
        Some(LiteralForm::Simple { value }) => {
            Literal::new_simple_literal(unescape_literal_value(value))
        }
        Some(LiteralForm::Language { value, lang }) => {
            Literal::new_language_tagged_literal_unchecked(unescape_literal_value(value), lang)
        }
        Some(LiteralForm::Typed { value, datatype }) => Literal::new_typed_literal(
            unescape_literal_value(value),
            NamedNode::new_unchecked(datatype.trim_matches(|c| c == '<' || c == '>')),
        ),
        None => Literal::new_simple_literal(s.trim_matches('"')),
    }
}

/// Parses an RDF graph name as the columns store it: `""` for the default
/// graph, otherwise a named node or a blank node. Trusted decode path — see
/// [`parse_named_node`].
pub(crate) fn parse_graph_name(s: &str) -> Result<GraphName> {
    if s.is_empty() {
        Ok(GraphName::DefaultGraph)
    } else if s.starts_with("_:") {
        Ok(GraphName::BlankNode(parse_blank_node(s)?))
    } else {
        Ok(GraphName::NamedNode(parse_named_node(s)?))
    }
}

/// The untrusted-boundary counterpart of [`parse_named_node`]: the IRI is
/// validated by [`NamedNode::new`] instead of being trusted.
///
/// The checked family (`*_checked`) is for strings that did NOT come out of
/// the store's own columns — CLI pattern arguments, binding call arguments,
/// anything a user typed. The trusted family above stays the decode path for
/// terms the store itself serialized, where re-validation is pure cost.
fn parse_named_node_checked(s: &str) -> Result<NamedNode> {
    let s = s.trim_matches(|c| c == '<' || c == '>');
    NamedNode::new(s)
        .map_err(|e| VortexRdfError::Deserialization(format!("invalid IRI {:?}: {}", s, e)))
}

/// Validating counterpart of `parse_blank_node`, used by the checked family
/// wherever a `_:` form is accepted.
fn parse_blank_node_checked(s: &str) -> Result<BlankNode> {
    let id = s.trim_start_matches("_:");
    BlankNode::new(id)
        .map_err(|e| VortexRdfError::Deserialization(format!("invalid blank node {:?}: {}", s, e)))
}

/// The untrusted-boundary counterpart of [`parse_subject`] — see
/// [`parse_named_node_checked`].
fn parse_subject_checked(s: &str) -> Result<NamedOrBlankNode> {
    if s.starts_with("_:") {
        Ok(NamedOrBlankNode::BlankNode(parse_blank_node_checked(s)?))
    } else {
        Ok(NamedOrBlankNode::NamedNode(parse_named_node_checked(s)?))
    }
}

/// The untrusted-boundary counterpart of [`parse_graph_name`] — see
/// [`parse_named_node_checked`]. Accepts the user-typed default-graph
/// spellings `""`, `"default"` (case-insensitive) and `"[]"`; the trusted
/// form takes only the `""` the columns store.
fn parse_graph_name_checked(s: &str) -> Result<GraphName> {
    if s.is_empty() || s.eq_ignore_ascii_case("default") || s == "[]" {
        Ok(GraphName::DefaultGraph)
    } else if s.starts_with("_:") {
        Ok(GraphName::BlankNode(parse_blank_node_checked(s)?))
    } else {
        Ok(GraphName::NamedNode(parse_named_node_checked(s)?))
    }
}

/// The untrusted-boundary counterpart of [`parse_term`]: the same N-Triples
/// forms — `<iri>`, `_:id`, `"v"`, `"v"@lang`, `"v"^^<dt>` — plus a bare IRI
/// (no angle brackets), which is read as a named node; every component is
/// built through a validating constructor (including the language tag and
/// the literal's datatype IRI).
///
/// Deliberately does not delegate to [`parse_term`], which is a trusted
/// decode path built on `new_unchecked` — see [`parse_named_node_checked`].
fn parse_term_checked(s: &str) -> Result<Term> {
    if s.starts_with("_:") {
        Ok(Term::BlankNode(parse_blank_node_checked(s)?))
    } else if s.starts_with('"') {
        Ok(Term::Literal(literal_checked(s)?))
    } else {
        Ok(Term::NamedNode(parse_named_node_checked(s)?))
    }
}

/// Validating counterpart of [`literal_from_serialized`]: the datatype IRI
/// goes through [`NamedNode::new`] and the language tag through
/// [`Literal::new_language_tagged_literal`].
/// Shares [`literal_from_serialized`]'s escape-aware structure scan and
/// unescaping, so both paths yield the same term for the same input; only the
/// malformed case differs, which is an error here rather than a lenient read.
fn literal_checked(s: &str) -> Result<Literal> {
    match split_literal(s) {
        Some(LiteralForm::Simple { value }) => {
            Ok(Literal::new_simple_literal(unescape_literal_value(value)))
        }
        Some(LiteralForm::Language { value, lang }) => {
            Literal::new_language_tagged_literal(unescape_literal_value(value), lang).map_err(|e| {
                VortexRdfError::Deserialization(format!("invalid language tag {:?}: {}", lang, e))
            })
        }
        Some(LiteralForm::Typed { value, datatype }) => Ok(Literal::new_typed_literal(
            unescape_literal_value(value),
            parse_named_node_checked(datatype)?,
        )),
        None => Err(VortexRdfError::Deserialization(format!(
            "malformed literal {:?}",
            s
        ))),
    }
}

/// A parsed quad pattern: the four term positions, each bound (`Some`) or
/// free (`None`) — what [`parse_pattern_checked`] returns and
/// `VortexRdfStore::match_pattern` borrows.
pub type Pattern = (
    Option<NamedOrBlankNode>,
    Option<NamedNode>,
    Option<Term>,
    Option<GraphName>,
);

/// Parses a user-typed quad pattern — the four optional term strings every
/// frontend's match surface accepts — through the checked family above:
/// subject via `parse_subject_checked`, predicate via
/// `parse_named_node_checked`, object via `parse_term_checked`, graph via
/// `parse_graph_name_checked`. A `None` slot stays free; the first invalid
/// slot's error is returned as-is (callers wanting per-slot context wrap the
/// error themselves).
pub fn parse_pattern_checked(
    s: Option<&str>,
    p: Option<&str>,
    o: Option<&str>,
    g: Option<&str>,
) -> Result<Pattern> {
    Ok((
        s.map(parse_subject_checked).transpose()?,
        p.map(parse_named_node_checked).transpose()?,
        o.map(parse_term_checked).transpose()?,
        g.map(parse_graph_name_checked).transpose()?,
    ))
}

/// The spelling the dictionary and the columns hold for a user-typed term:
/// the checked parse of `term` (an IRI with or without angle brackets, a
/// `_:` blank node, or an escape-aware literal) rendered back through the
/// storage form — `xsd:string` typing dropped, the language tag lowercased,
/// escapes normalized — and the default graph's spellings (`""`,
/// `default`, `[]`) as the empty string the `g` column stores. Malformed
/// input is an error; a dictionary that holds `term` under *some* spelling
/// holds it under this one.
pub fn canonical_spelling(term: &str) -> Result<String> {
    if term.is_empty() || term.eq_ignore_ascii_case("default") || term == "[]" {
        return Ok(String::new());
    }
    Ok(parse_term_checked(term)?.to_string())
}

/// [`parse_term`] as a decode step: an object string the columns store,
/// parsed on the trusted path, with an unrecognized form reported as a
/// deserialization error.
pub(crate) fn parse_object(o: &str) -> Result<Term> {
    parse_term(o).ok_or_else(|| VortexRdfError::Deserialization(format!("Invalid object: {o}")))
}

/// A quad from its four stored N-Triples term strings (`g` empty for the
/// default graph), parsed on the trusted decode path.
pub(crate) fn quad_from_terms(s: &str, p: &str, o: &str, g: &str) -> Result<Quad> {
    Ok(Quad::new(
        parse_subject(s)?,
        parse_named_node(p)?,
        parse_object(o)?,
        parse_graph_name(g)?,
    ))
}

/// Reconstructs an oxrdf [`Term`] from its stored N-Triples form: `<iri>`,
/// `_:id`, or a simple, language-tagged or typed literal. `None` for a string
/// in none of those forms. Trusted decode path — see [`parse_named_node`].
pub(crate) fn parse_term(s: &str) -> Option<Term> {
    if s.starts_with('<') {
        // Trusted-input decode path — see `parse_named_node`; `new_unchecked`
        // skips the `oxiri::Iri::parse` re-validation of an already-validated,
        // stored IRI.
        Some(Term::NamedNode(NamedNode::new_unchecked(
            s.trim_matches(|c| c == '<' || c == '>'),
        )))
    } else if s.starts_with("_:") {
        Some(Term::BlankNode(BlankNode::new_unchecked(
            s.trim_start_matches("_:"),
        )))
    } else if s.starts_with('"') {
        Some(Term::Literal(literal_from_serialized(s)))
    } else {
        None
    }
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
    fn parse_term_reads_each_literal_shape() {
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
            assert_eq!(parse_term(serialized).unwrap(), expected, "{}", serialized);
        }
    }

    #[test]
    fn parse_term_named_and_blank_nodes() {
        assert_eq!(
            parse_term("<http://example.org/x>").unwrap(),
            Term::NamedNode(NamedNode::new("http://example.org/x").unwrap())
        );
        assert!(matches!(
            parse_term("_:b0").unwrap(),
            Term::BlankNode(b) if b.as_str() == "b0"
        ));
        // Only the stored forms are terms; anything else is `None`.
        assert_eq!(parse_term("http://example.org/x"), None);
        assert_eq!(parse_term("not a term"), None);
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
            parse_term_checked("http://example.org/x").unwrap(),
            Term::NamedNode(NamedNode::new("http://example.org/x").unwrap())
        );

        // A `None` slot stays free; "default" names the default graph.
        let (s, p, o, g) = parse_pattern_checked(None, None, None, Some("default")).unwrap();
        assert!(s.is_none() && p.is_none() && o.is_none());
        assert_eq!(g, Some(GraphName::DefaultGraph));
    }

    /// The user-typed default-graph spellings belong to the checked form
    /// only; the trusted form reads the `""` the columns store.
    #[test]
    fn default_graph_spellings_split_between_trusted_and_checked() {
        for s in ["", "default", "DEFAULT", "[]"] {
            assert_eq!(
                parse_graph_name_checked(s).unwrap(),
                GraphName::DefaultGraph,
                "{s:?}"
            );
        }
        assert_eq!(parse_graph_name("").unwrap(), GraphName::DefaultGraph);
        for s in ["default", "[]"] {
            assert!(
                matches!(parse_graph_name(s).unwrap(), GraphName::NamedNode(_)),
                "{s:?}"
            );
        }
    }

    /// Pattern slots are user-typed, so they take the *checked* parse family: an
    /// invalid term in any slot must error rather than silently match nothing.
    #[test]
    fn parse_pattern_checked_rejects_invalid_slots() {
        assert!(parse_pattern_checked(Some("no spaces allowed"), None, None, None).is_err());
        assert!(parse_pattern_checked(None, Some("not an iri"), None, None).is_err());
        assert!(parse_pattern_checked(None, None, Some("\"unterminated"), None).is_err());
        assert!(parse_pattern_checked(None, None, None, Some("bad graph iri")).is_err());
    }

    #[test]
    fn trusted_and_checked_parses_agree_on_escaped_literals() {
        for term in escaped_literal_cases() {
            let s = term.to_string();
            assert_eq!(parse_term(&s).unwrap(), term, "trusted parse of {}", s);
            assert_eq!(
                parse_term_checked(&s).unwrap(),
                term,
                "checked parse of {}",
                s
            );
        }
    }

    #[test]
    fn structure_scan_ignores_suffix_lookalikes_inside_the_value() {
        // `"@` and `^^` inside the value must not be read as structure: both
        // parses see a simple literal, and the checked one does not error.
        for (serialized, value) in [
            ("\"say \\\"hi\\\"@home\"", "say \"hi\"@home"),
            ("\"a ^^ b\"", "a ^^ b"),
            (
                "\"\\\"^^<http://example.org/nope>\"",
                "\"^^<http://example.org/nope>",
            ),
        ] {
            let expected = Term::Literal(Literal::new_simple_literal(value));
            assert_eq!(parse_term(serialized).unwrap(), expected);
            assert_eq!(parse_term_checked(serialized).unwrap(), expected);
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
            assert_eq!(parse_term(serialized).unwrap(), expected, "{}", serialized);
            assert_eq!(
                parse_term_checked(serialized).unwrap(),
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
            assert_eq!(parse_term(serialized).unwrap(), expected, "{}", serialized);
            assert_eq!(
                parse_term_checked(serialized).unwrap(),
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
            parse_term("\"a\\\"b\"^^<http://example.org/dt>").unwrap(),
            Term::Literal(Literal::new_typed_literal("a\"b", dt))
        );
        assert_eq!(
            parse_term("\"a\\\"b\"@en").unwrap(),
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
            assert_eq!(parse_term(serialized).unwrap(), expected, "{}", serialized);
            assert_eq!(
                parse_term_checked(serialized).unwrap(),
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
    fn malformed_literals_are_lenient_when_trusted_and_rejected_when_checked() {
        for s in ["\"unterminated", "\"a\" trailing", "\"a\"\\"] {
            // Trusted decode is infallible: it must not panic and must yield a term.
            assert!(matches!(parse_term(s).unwrap(), Term::Literal(_)), "{}", s);
            assert!(parse_term_checked(s).is_err(), "{}", s);
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
