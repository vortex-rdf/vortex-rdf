//! A spelling taken apart: its kind from the first byte ([`kind_of`]), a
//! literal's lexical form (its escapes decoded [strictly](Escapes::Strict)),
//! language tag and datatype ([`LiteralView`]), and the `langMatches` rule
//! they are read with.

use std::borrow::Cow;

use crate::common::terms::{Escapes, LiteralForm, split_literal, unescape};
use crate::common::vocab::{RDF_LANG_STRING, XSD, XSD_STRING};

use super::Verdict;
use super::numeric::{Num, numeric_kind, parse_number};

/// A literal spelling split into its parts, borrowed from the spelling.
pub(super) struct LiteralView<'a> {
    /// The lexical form as spelled (N-Triples escapes intact).
    raw_lexical: &'a str,
    /// The language tag, as stored.
    pub(super) lang: Option<&'a str>,
    /// The datatype IRI without angle brackets, as spelled.
    pub(super) datatype: Option<&'a str>,
}

impl<'a> LiteralView<'a> {
    /// Split `"lex"`, `"lex"@tag` or `"lex"^^<dt>` through the escape-aware
    /// reading the decode path uses; `None` for anything else.
    pub(super) fn parse(spelling: &'a str) -> Option<Self> {
        Some(match split_literal(spelling)? {
            LiteralForm::Simple { value } => Self {
                raw_lexical: value,
                lang: None,
                datatype: None,
            },
            LiteralForm::Language { value, lang } => Self {
                raw_lexical: value,
                lang: Some(lang),
                datatype: None,
            },
            LiteralForm::Typed { value, datatype } => Self {
                raw_lexical: value,
                lang: None,
                datatype: Some(datatype.strip_prefix('<')?.strip_suffix('>')?),
            },
        })
    }

    /// The lexical form: escapes decoded, `None` for a malformed escape
    /// ([`Escapes::Strict`]), which no two readers agree on.
    pub(super) fn lexical(&self) -> Option<Cow<'a, str>> {
        unescape(self.raw_lexical, Escapes::Strict)
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
        parse_number(&self.lexical()?, numeric_kind(dt)?)
    }

    /// Whether a numeric literal's lexical form parses disregarding the
    /// datatype's own bounds — an out-of-range `xsd:byte`, say. An engine
    /// parses such a form to a value it then rejects, and orders the
    /// literal by its datatype; a form the grammar rejects (whitespace,
    /// `NaN`) its own, more lenient parser may still accept, so that one is
    /// left to it.
    pub(super) fn parses_unbounded(&self) -> bool {
        self.datatype.and_then(numeric_kind).is_some_and(|kind| {
            self.lexical()
                .is_some_and(|lexical| parse_number(&lexical, kind.unbounded()).is_some())
        })
    }

    /// Whether this is an `xsd:long` or `xsd:unsignedLong` whose lexical form
    /// is an integer outside the type's 64-bit bounds — a value rdflib holds
    /// to no bound and compares, which this model refuses.
    pub(super) fn wide_64(&self) -> bool {
        let Some(local) = self.datatype.and_then(|dt| dt.strip_prefix(XSD)) else {
            return false;
        };
        (local == "long" || local == "unsignedLong")
            && self.parses_unbounded()
            && self.number().is_none()
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

/// BCP 47 basic filtering (RFC 4647 §3.3.1) as SPARQL's `langMatches`,
/// decided for `*` and for a range of ASCII letters, digits and hyphens,
/// where it and rdflib's `_lang_range_check` agree. rdflib may read any other
/// range differently: a `*` in any subtag is a wildcard (`en-*` matches
/// `en-gb`), whitespace around the range is stripped, and Python lower-cases
/// it (the Kelvin sign to `k`). A range other than `*` with a character
/// outside that alphabet is `Unknown` whether or not rdflib happens to agree
/// (for `en_gb` it does). So is a tag outside it, which only a foreign writer's file can
/// hold (the N-Triples ingest rejects one), so that check is defensive.
pub(super) fn lang_matches(tag: &str, range: &str) -> Verdict {
    let plain = |s: &str| s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-');
    if range == "*" {
        return Verdict::from(!tag.is_empty());
    }
    if !plain(range) || !plain(tag) {
        return Verdict::Unknown;
    }
    let tag = tag.to_ascii_lowercase();
    let range = range.to_ascii_lowercase();
    Verdict::from(
        tag == range || (tag.starts_with(&range) && tag.as_bytes().get(range.len()) == Some(&b'-')),
    )
}
