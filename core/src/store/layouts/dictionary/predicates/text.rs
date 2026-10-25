//! The string kinds (`str_prefix`, `contains`, `strstarts`, `strends` and
//! `regex`): their options ([`TextOptions`]), their constants as rdflib
//! reads them, and the text of a term each test reads.

use std::borrow::Cow;

use crate::common::vocab::{XSD, XSD_STRING};
use crate::error::{Result, VortexRdfError};

use super::Verdict;
use super::literal::{Kind, LiteralView};
use super::regex_filter::RegexTest;

/// The options of a string kind beyond `(kind, arg)`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TextOptions {
    /// SPARQL `REGEX` flags (`regex` only): `i`, `s` and `m` are honoured,
    /// any other flag ignored, as rdflib does.
    pub flags: String,
    /// A case wrapper applied to the text first (`LCASE`/`UCASE`).
    pub case: Option<CaseMap>,
    /// Whether the text is SPARQL `STR(term)` (an IRI's string, a literal's
    /// lexical form as a simple literal, undecided for a datatype rdflib
    /// normalizes, and a blank node undecided) rather than rdflib's
    /// `string()` (string literals only).
    pub as_str: bool,
}

/// A case wrapper on a string kind's text, with Python `str.lower()` /
/// `str.upper()` semantics; decided on ASCII text only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaseMap {
    Lower,
    Upper,
}

/// A string kind: its test and how a term's text is read. The payload of
/// [`TermPredicate::Text`](super::TermPredicate::Text), built by
/// [`TermPredicate::parse_with`](super::TermPredicate::parse_with).
#[derive(Debug, Clone, PartialEq)]
pub struct TextPredicate {
    pub(super) test: TextTest,
    pub(super) case: Option<CaseMap>,
    pub(super) as_str: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub(super) enum TextTest {
    /// `str_prefix`: the text starts with the prefix (a simple-literal constant).
    Prefix(String),
    /// `CONTAINS(text, constant)`.
    Contains(TextConstant),
    /// `STRSTARTS(text, constant)`.
    StartsWith(TextConstant),
    /// `STRENDS(text, constant)`.
    EndsWith(TextConstant),
    /// `REGEX(text, pattern, flags)`.
    Regex(RegexTest),
}

/// A string constant as rdflib's `_compatibleStrings` reads it.
#[derive(Debug, Clone, PartialEq)]
pub(super) enum TextConstant {
    /// A string literal: its lexical form and language tag as spelled (rdflib
    /// compares tags exactly).
    Text { text: String, lang: Option<String> },
    /// Anything rdflib's `string()` rejects (a non-string typed literal, an
    /// IRI, a blank node): every candidate fails.
    NotAString,
}

impl TextConstant {
    /// A string constant from its strict N-Triples spelling: a literal
    /// (`"text"`, `"text"@tag` or `"text"^^<datatype>`), an `<iri>` or a
    /// `_:blank` node. Anything else (an unclosed bracket, a prefixed
    /// datatype, a language tag outside `[A-Za-z]+(-[A-Za-z0-9]+)*`, text
    /// after the term) is an `InvalidInput` error, never a constant that
    /// fails every candidate unnoticed. A well-formed spelling that is no
    /// string literal is [`NotAString`](TextConstant::NotAString).
    pub(super) fn parse(arg: &str) -> Result<Self> {
        let invalid = || {
            VortexRdfError::InvalidInput(format!(
                "a string constant is a strict N-Triples spelling (\"text\", \"text\"@tag, \
                 \"text\"^^<datatype>, <iri> or _:label), got {arg:?}"
            ))
        };
        if arg.starts_with('<') {
            return if is_iri_ref(arg) {
                Ok(TextConstant::NotAString)
            } else {
                Err(invalid())
            };
        }
        if let Some(label) = arg.strip_prefix("_:") {
            return if oxrdf::BlankNode::new(label).is_ok() {
                Ok(TextConstant::NotAString)
            } else {
                Err(invalid())
            };
        }
        let lit = LiteralView::parse(arg).ok_or_else(invalid)?;
        if lit.lang.is_some_and(|tag| !is_language_tag(tag))
            || lit.datatype.is_some_and(|dt| dt.contains(['<', '>']))
        {
            return Err(invalid());
        }
        let text = lit.lexical().ok_or_else(invalid)?.into_owned();
        if !lit.is_string_like() {
            return Ok(TextConstant::NotAString);
        }
        Ok(TextConstant::Text {
            text,
            lang: lit.lang.map(str::to_owned),
        })
    }

    fn render(&self) -> String {
        match self {
            TextConstant::Text { text, lang: None } => format!("{text:?}"),
            TextConstant::Text {
                text,
                lang: Some(lang),
            } => format!("{text:?}@{lang}"),
            TextConstant::NotAString => "<not a string>".to_owned(),
        }
    }
}

/// Whether `s` is an IRI reference of N-Triples' `IRIREF` shape: `<` and `>`
/// around text holding neither.
fn is_iri_ref(s: &str) -> bool {
    s.strip_prefix('<')
        .and_then(|rest| rest.strip_suffix('>'))
        .is_some_and(|iri| !iri.contains(['<', '>']))
}

/// Whether `tag` is an N-Triples `LANGTAG` (without the `@`):
/// `[A-Za-z]+(-[A-Za-z0-9]+)*`.
fn is_language_tag(tag: &str) -> bool {
    let mut subtags = tag.split('-');
    subtags
        .next()
        .is_some_and(|first| !first.is_empty() && first.bytes().all(|b| b.is_ascii_alphabetic()))
        && subtags.all(|sub| !sub.is_empty() && sub.bytes().all(|b| b.is_ascii_alphanumeric()))
}

/// The local names of the XSD datatypes rdflib 7 parses into a Python value
/// and writes back in canonical form (`rdflib.term.XSDToPython`), so its
/// `STR()` of such a literal can differ from the stored lexical form
/// (`STR("01"^^xsd:integer)` is `"1"`). `xsd:string` is not among them.
const RDFLIB_NORMALIZED_XSD: &[&str] = &[
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
];

/// The non-XSD datatypes rdflib 7 parses and writes back in canonical form:
/// `rdf:XMLLiteral`, and `rdf:HTML` when rdflib's optional `html5rdf` is
/// installed.
const RDFLIB_NORMALIZED_RDF: &[&str] = &[
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#XMLLiteral",
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#HTML",
];

/// Whether rdflib's `STR()` of a literal typed `dt` may not be its stored
/// lexical form.
fn rdflib_normalizes(dt: &str) -> bool {
    RDFLIB_NORMALIZED_RDF.contains(&dt)
        || dt
            .strip_prefix(XSD)
            .is_some_and(|local| RDFLIB_NORMALIZED_XSD.contains(&local))
}

impl TextPredicate {
    /// The term's text and language under `string()` (`as_str` off) or
    /// `STR()` (on); `Err(verdict)` when there is no text (`False`: rdflib
    /// raises and the FILTER fails) or none this layer reads exactly
    /// (`Unknown`).
    fn text_of<'s>(
        &self,
        spelling: &'s str,
        kind: Kind,
    ) -> std::result::Result<(Cow<'s, str>, Option<&'s str>), Verdict> {
        match kind {
            Kind::Iri if self.as_str => spelling
                .strip_prefix('<')
                .and_then(|iri| iri.strip_suffix('>'))
                .map(|iri| (Cow::Borrowed(iri), None))
                .ok_or(Verdict::Unknown),
            // rdflib's `STR()` of a blank node is its label: not decided here.
            Kind::Blank if self.as_str => Err(Verdict::Unknown),
            // `string()` of an IRI or a blank node raises: the FILTER fails.
            Kind::Iri | Kind::Blank => Err(Verdict::False),
            Kind::Other => Err(Verdict::Unknown),
            Kind::Literal => {
                let lit = LiteralView::parse(spelling).ok_or(Verdict::Unknown)?;
                let lexical = lit.lexical().ok_or(Verdict::Unknown)?;
                if self.as_str {
                    match lit.datatype {
                        Some(dt) if dt != XSD_STRING && rdflib_normalizes(dt) => {
                            Err(Verdict::Unknown)
                        }
                        _ => Ok((lexical, None)),
                    }
                } else if lit.is_string_like() {
                    Ok((lexical, lit.lang))
                } else {
                    Err(Verdict::False)
                }
            }
        }
    }

    pub(super) fn eval(&self, spelling: &str, kind: Kind) -> Verdict {
        let (text, lang) = match self.text_of(spelling, kind) {
            Ok(text) => text,
            Err(verdict) => return verdict,
        };
        let text = match self.case {
            None => text,
            Some(_) if !text.is_ascii() => return Verdict::Unknown,
            Some(CaseMap::Lower) => Cow::Owned(text.to_ascii_lowercase()),
            Some(CaseMap::Upper) => Cow::Owned(text.to_ascii_uppercase()),
        };
        let test = |constant: &TextConstant, holds: fn(&str, &str) -> bool| match constant {
            TextConstant::NotAString => Verdict::False,
            TextConstant::Text {
                lang: Some(tag), ..
            } if lang != Some(tag.as_str()) => Verdict::False,
            TextConstant::Text { text: c, .. } => Verdict::from(holds(&text, c)),
        };
        match &self.test {
            TextTest::Prefix(prefix) => Verdict::from(text.starts_with(prefix.as_str())),
            TextTest::Contains(c) => test(c, |t, c| t.contains(c)),
            TextTest::StartsWith(c) => test(c, |t, c| t.starts_with(c)),
            TextTest::EndsWith(c) => test(c, |t, c| t.ends_with(c)),
            TextTest::Regex(regex) => regex.eval(&text),
        }
    }

    pub(super) fn canonical(&self) -> (&'static str, String) {
        let (kind, mut arg) = match &self.test {
            TextTest::Prefix(prefix) => ("str_prefix", prefix.clone()),
            TextTest::Contains(c) => ("contains", c.render()),
            TextTest::StartsWith(c) => ("strstarts", c.render()),
            TextTest::EndsWith(c) => ("strends", c.render()),
            TextTest::Regex(regex) => ("regex", regex.render()),
        };
        match self.case {
            Some(CaseMap::Lower) => arg.push_str(" [lower]"),
            Some(CaseMap::Upper) => arg.push_str(" [upper]"),
            None => {}
        }
        if self.as_str {
            arg.push_str(" [str]");
        }
        (kind, arg)
    }
}
