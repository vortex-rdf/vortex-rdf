//! Literal spellings taken apart: the lexical form, language tag and
//! datatype of an N-Triples literal, the kind of a spelling, and the
//! `langMatches` and escape rules the predicates read them with.

use crate::common::vocab::{RDF_LANG_STRING, XSD_STRING};

use super::numeric::{Num, digits_value, numeric_kind, parse_number};

/// A literal spelling split into its parts, borrowed from the spelling.
pub(super) struct LiteralView<'a> {
    /// The lexical form as spelled, escapes intact.
    pub(super) lexical: &'a str,
    /// The language tag, as stored.
    pub(super) lang: Option<&'a str>,
    /// The datatype IRI, without angle brackets.
    pub(super) datatype: Option<&'a str>,
}

impl<'a> LiteralView<'a> {
    /// Split `"lex"`, `"lex"@tag` or `"lex"^^<dt>`; `None` for anything
    /// else. The only unescaped `"` in a literal spelling are its two
    /// delimiters, so the terminator is found from the end.
    pub(super) fn parse(spelling: &'a str) -> Option<Self> {
        let body = spelling.strip_prefix('"')?;
        if let Some(lexical) = body.strip_suffix('"') {
            return Some(Self {
                lexical,
                lang: None,
                datatype: None,
            });
        }
        if spelling.ends_with('>') {
            let at = spelling.rfind("\"^^<")?;
            return Some(Self {
                lexical: &spelling[1..at],
                lang: None,
                datatype: Some(&spelling[at + 4..spelling.len() - 1]),
            });
        }
        let at = spelling.rfind("\"@")?;
        Some(Self {
            lexical: &spelling[1..at],
            lang: Some(&spelling[at + 2..]),
            datatype: None,
        })
    }

    /// The datatype SPARQL's `datatype()` reports: `rdf:langString` for a
    /// tagged literal, `xsd:string` for a plain one.
    pub(super) fn effective_datatype(&self) -> &str {
        match (self.lang, self.datatype) {
            (Some(_), _) => RDF_LANG_STRING,
            (None, Some(dt)) => dt,
            (None, None) => XSD_STRING,
        }
    }

    /// Plain, language-tagged, or typed `xsd:string`.
    pub(super) fn is_string_like(&self) -> bool {
        self.lang.is_some() || self.datatype.is_none_or(|dt| dt == XSD_STRING)
    }

    /// The numeric value, when the datatype is numeric and the lexical form
    /// parses under the model.
    pub(super) fn number(&self) -> Option<Num> {
        let dt = self.datatype?;
        parse_number(self.lexical, numeric_kind(dt)?)
    }

    /// Whether the lexical form parses under the datatype's kind ignoring the
    /// datatype's bounds: an out-of-range `xsd:byte` does, `NaN` does not.
    pub(super) fn parses_unbounded(&self) -> bool {
        self.datatype
            .and_then(numeric_kind)
            .is_some_and(|kind| parse_number(self.lexical, kind.unbounded()).is_some())
    }
}

/// The kind of a spelling, from its first byte.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Kind {
    /// The empty spelling of the default graph.
    DefaultGraph,
    Literal,
    Iri,
    Blank,
    /// A spelling no dictionary of this crate holds.
    Other,
}

pub(super) fn kind_of(spelling: &str) -> Kind {
    match spelling.as_bytes().first() {
        None => Kind::DefaultGraph,
        Some(b'"') => Kind::Literal,
        Some(b'<') => Kind::Iri,
        Some(b'_') => Kind::Blank,
        Some(_) => Kind::Other,
    }
}

/// BCP 47 basic filtering (RFC 4647 §3.3.1), as SPARQL's `langMatches`.
pub(super) fn lang_matches(tag: &str, range: &str) -> bool {
    if range == "*" {
        return !tag.is_empty();
    }
    let tag = tag.to_ascii_lowercase();
    let range = range.to_ascii_lowercase();
    tag == range || (tag.starts_with(&range) && tag.as_bytes().get(range.len()) == Some(&b'-'))
}

/// The first `want` characters of a lexical form with its N-Triples escapes
/// undone; `None` for a malformed escape.
pub(super) fn unescape_prefix(lexical: &str, want: usize) -> Option<String> {
    if !lexical.contains('\\') {
        return Some(lexical.chars().take(want).collect());
    }
    let mut out = String::with_capacity(want.min(lexical.len()));
    let mut chars = lexical.chars();
    let mut count = 0;
    while count < want {
        let Some(c) = chars.next() else { break };
        let unescaped = if c != '\\' {
            c
        } else {
            match chars.next()? {
                't' => '\t',
                'b' => '\u{8}',
                'n' => '\n',
                'r' => '\r',
                'f' => '\u{c}',
                '"' => '"',
                '\'' => '\'',
                '\\' => '\\',
                'u' => hex_char(&mut chars, 4)?,
                'U' => hex_char(&mut chars, 8)?,
                _ => return None,
            }
        };
        out.push(unescaped);
        count += 1;
    }
    Some(out)
}

/// The character spelled by the next `len` hex digits of `chars`.
fn hex_char(chars: &mut std::str::Chars<'_>, len: usize) -> Option<char> {
    let v = digits_value((0..len).map(|_| chars.next()?.to_digit(16)), 16)?;
    char::from_u32(u32::try_from(v).ok()?)
}
