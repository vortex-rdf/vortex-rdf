//! Literal spellings taken apart: the lexical form, language tag and
//! datatype of an N-Triples literal, the kind of a spelling, and the
//! `langMatches` and escape rules the predicates read them with.

use crate::common::vocab::{RDF_LANG_STRING, XSD_STRING};

use super::numeric::{
    Num, NumKind, numeric_kind, parse_decimal, parse_float, parse_integer, parse_number,
};

/// A literal spelling split into its parts, borrowed from the spelling.
pub(super) struct LiteralView<'a> {
    /// The lexical form as spelled (escapes intact).
    pub(super) lexical: &'a str,
    /// The language tag, as stored.
    pub(super) lang: Option<&'a str>,
    /// The datatype IRI without angle brackets, as spelled.
    pub(super) datatype: Option<&'a str>,
}

impl<'a> LiteralView<'a> {
    /// Split `"lex"`, `"lex"@tag` or `"lex"^^<dt>`; `None` for anything else.
    ///
    /// The only unescaped `"` in a literal spelling are its two delimiters,
    /// so searching the terminator from the end is unambiguous: `"^^<` and
    /// `"@` inside the lexical form can only occur escaped (`\"^^<`), and the
    /// real terminator always comes later.
    pub(super) fn parse(spelling: &'a str) -> Option<Self> {
        let body = spelling.strip_prefix('"')?;
        if let Some(lexical) = body.strip_suffix('"') {
            // Guard against the one-character spelling `"`.
            if spelling.len() < 2 {
                return None;
            }
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

    /// The datatype the SPARQL `datatype()` function reports.
    pub(super) fn effective_datatype(&self) -> &str {
        match (self.lang, self.datatype) {
            (Some(_), _) => RDF_LANG_STRING,
            (None, Some(dt)) => dt,
            (None, None) => XSD_STRING,
        }
    }

    /// Whether the literal is string-like: plain, language-tagged, or typed
    /// `xsd:string` (which the ingest never writes, but a foreign file may).
    pub(super) fn is_string_like(&self) -> bool {
        self.lang.is_some() || self.datatype.is_none_or(|dt| dt == XSD_STRING)
    }

    /// The numeric value, when the datatype is numeric and the lexical form
    /// parses under the model.
    pub(super) fn number(&self) -> Option<Num> {
        let dt = self.datatype?;
        parse_number(self.lexical, numeric_kind(dt)?)
    }

    /// Whether a numeric literal's lexical form parses disregarding the
    /// datatype's own bounds — an out-of-range `xsd:byte`, say. An engine
    /// parses such a form to a value it then rejects, and orders the
    /// literal by its datatype; a form the grammar rejects (whitespace,
    /// `NaN`) its own, more lenient parser may still accept, so that one is
    /// left to it.
    pub(super) fn parses_unbounded(&self) -> bool {
        let Some(dt) = self.datatype else {
            return false;
        };
        match numeric_kind(dt) {
            Some(NumKind::Int(..)) => parse_integer(self.lexical).is_some(),
            Some(NumKind::Decimal) => parse_decimal(self.lexical).is_some(),
            Some(NumKind::Float) => parse_float(self.lexical).is_some(),
            None => false,
        }
    }
}

/// The kind of a spelling from its first byte, which the sorted dictionary
/// shares with the kind ranges.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Kind {
    /// The empty spelling of the default graph.
    DefaultGraph,
    Literal,
    Iri,
    Blank,
    /// Anything a dictionary of this crate never holds.
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

/// BCP 47 basic filtering (RFC 4647 §3.3.1) as SPARQL's `langMatches`.
pub(super) fn lang_matches(tag: &str, range: &str) -> bool {
    if range == "*" {
        return !tag.is_empty();
    }
    let tag = tag.to_ascii_lowercase();
    let range = range.to_ascii_lowercase();
    tag == range || (tag.starts_with(&range) && tag.as_bytes().get(range.len()) == Some(&b'-'))
}

/// Unescape at least the first `want` characters of a lexical form (as
/// spelled, with N-Triples escapes), stopping early so a long literal costs
/// only its prefix. `None` for a malformed escape.
pub(super) fn unescape_prefix(lexical: &str, want: usize) -> Option<String> {
    if !lexical.contains('\\') {
        return Some(lexical.chars().take(want).collect());
    }
    let mut out = String::with_capacity(want.min(lexical.len()));
    let mut chars = lexical.chars();
    while out.chars().count() < want {
        let Some(c) = chars.next() else { break };
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next()? {
            't' => out.push('\t'),
            'b' => out.push('\u{8}'),
            'n' => out.push('\n'),
            'r' => out.push('\r'),
            'f' => out.push('\u{c}'),
            '"' => out.push('"'),
            '\'' => out.push('\''),
            '\\' => out.push('\\'),
            'u' => out.push(hex_char(&mut chars, 4)?),
            'U' => out.push(hex_char(&mut chars, 8)?),
            _ => return None,
        }
    }
    Some(out)
}

fn hex_char(chars: &mut std::str::Chars<'_>, len: usize) -> Option<char> {
    let mut v: u32 = 0;
    for _ in 0..len {
        v = v
            .checked_mul(16)?
            .checked_add(chars.next()?.to_digit(16)?)?;
    }
    char::from_u32(v)
}
