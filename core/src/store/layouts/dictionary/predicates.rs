//! Term predicates evaluated on a dictionary's N-Triples spellings — the
//! native half of a query layer's `FILTER` fast path.
//!
//! A [`TermPredicate`] answers a single-variable SPARQL test (`isIRI(?x)`,
//! `datatype(?x) = <dt>`, `lang(?x) = "en"`, `langMatches(lang(?x), "en")`,
//! `?x < 5`, `strstarts(str(?x), "http://…")`) for one term, read straight
//! off its spelling, with a three-valued [`Verdict`]. The rules are
//! deliberately **conservative**: a verdict is `True` or `False` only where
//! the SPARQL semantics over the stored spelling are total and cheap to
//! decide; everything else — a lexical form the XSD grammar does not cover,
//! a comparison the value model cannot settle exactly, a datatype the rules
//! do not know — is `Unknown`, for the caller to resolve with a full SPARQL
//! engine. A caller therefore never gets a wrong definite answer, only a
//! slower one.
//!
//! A predicate is evaluated over the *candidate* codes a query produced
//! (`filter_codes`): a code whose kind range decides it
//! ([`TermPredicate::kind_verdict`]) is not read; every other one is read and
//! its spelling parsed — the lexical form unescaped, the language tag and
//! datatype taken from the spelling — before the predicate looks at it.

use std::borrow::Cow;
use std::cmp::Ordering;
use std::fmt;
use std::ops::Range;

use crate::common::terms::{LiteralForm, split_literal};
use crate::common::vocab::{RDF_LANG_STRING, XSD, XSD_STRING};
use crate::error::{Result, VortexRdfError};

/// The code ranges of a sorted dictionary's term kinds. Codes are
/// lexicographic ranks of the N-Triples spelling, so every kind is one
/// contiguous range, in byte order: the empty spelling of the default graph
/// (code 0, when any quad is in the default graph), then literals (`"`),
/// IRIs (`<`), blank nodes (`_:`). Anything a dictionary of this crate never
/// holds falls in the gaps between those ranges.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KindRanges {
    /// The code of `""`, the default graph's name, when present.
    pub default_graph: Option<u32>,
    /// Codes of the literals.
    pub literals: Range<u32>,
    /// Codes of the IRIs.
    pub iris: Range<u32>,
    /// Codes of the blank nodes.
    pub blanks: Range<u32>,
    /// The dictionary's size: every code is below it.
    pub len: u32,
}

impl KindRanges {
    /// The codes in none of the three kind ranges, ascending — a foreign
    /// writer's spellings, and the default graph's `""`.
    pub fn gaps(&self) -> impl Iterator<Item = u32> + '_ {
        (0..self.literals.start)
            .chain(self.literals.end..self.iris.start)
            .chain(self.iris.end..self.blanks.start)
            .chain(self.blanks.end..self.len)
    }
}

impl KindRanges {
    /// The kind of `code`'s spelling, read off the ranges.
    pub(crate) fn kind_of_code(&self, code: u32) -> CodeKind {
        if self.literals.contains(&code) {
            CodeKind::Literal
        } else if self.iris.contains(&code) {
            CodeKind::Iri
        } else if self.blanks.contains(&code) {
            CodeKind::Blank
        } else {
            CodeKind::Other
        }
    }
}

/// What the kind ranges say about a code before its spelling is read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CodeKind {
    Literal,
    Iri,
    Blank,
    /// The default graph's `""`, or a foreign spelling in the gaps.
    Other,
}

/// The three-valued answer of a predicate for one term.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// The predicate holds.
    True,
    /// The predicate does not hold, or evaluating it is a SPARQL type error
    /// (which a `FILTER` treats as false).
    False,
    /// Not decidable from the spelling under these rules.
    Unknown,
}

/// A numeric comparison operator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NumOp {
    Lt,
    Le,
    Gt,
    Ge,
    Eq,
    Ne,
}

impl NumOp {
    fn kind(self) -> &'static str {
        match self {
            NumOp::Lt => "num_lt",
            NumOp::Le => "num_le",
            NumOp::Gt => "num_gt",
            NumOp::Ge => "num_ge",
            NumOp::Eq => "num_eq",
            NumOp::Ne => "num_ne",
        }
    }

    /// The verdict for an ordering `ord` of the term against the constant.
    fn apply(self, ord: Ordering) -> Verdict {
        let holds = match self {
            NumOp::Lt => ord == Ordering::Less,
            NumOp::Le => ord != Ordering::Greater,
            NumOp::Gt => ord == Ordering::Greater,
            NumOp::Ge => ord != Ordering::Less,
            NumOp::Eq => ord == Ordering::Equal,
            NumOp::Ne => ord != Ordering::Equal,
        };
        if holds { Verdict::True } else { Verdict::False }
    }

    fn is_equality(self) -> bool {
        matches!(self, NumOp::Eq | NumOp::Ne)
    }
}

/// A single-variable term predicate, parsed from a `(kind, arg)` pair.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TermPredicate {
    /// `isLITERAL(?x)`.
    IsLiteral,
    /// `isIRI(?x)` / `isURI(?x)`.
    IsIri,
    /// `isBLANK(?x)`.
    IsBlank,
    /// `datatype(?x) = <iri>` — a plain literal has `xsd:string`, a
    /// language-tagged one `rdf:langString`, a typed one its datatype; the
    /// lexical form is never inspected.
    Datatype(String),
    /// `lang(?x) = "tag"` — exact comparison against the stored tag (which
    /// the ingest lowercases); an untagged literal has the empty tag.
    Lang(String),
    /// `langMatches(lang(?x), "range")` — BCP 47 basic filtering: `*`
    /// matches any non-empty tag, otherwise a case-insensitive match of the
    /// range to the tag or to a `-`-delimited prefix of it.
    LangMatches(String),
    /// `?x <op> <numeric constant>` under the SPARQL operator mapping.
    Num(NumOp, Number),
    /// `strstarts(str(?x), "prefix")` — on the IRI string, the blank node
    /// label, or the unescaped lexical form of a string-like literal.
    StrPrefix(String),
}

/// A numeric constant, as a parsed XSD numeric literal.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Number {
    value: Num,
    /// The constant's datatype IRI (without angle brackets), for the
    /// cross-datatype ordering rule.
    datatype: String,
}

/// The exact value model: integers as `i128`, decimals as a scaled `i128`
/// mantissa, floats as `f64`. Everything the XSD grammars accept but this
/// model cannot hold exactly (an integer over 38 digits, a decimal with more
/// digits than that, `INF`/`NaN`) is left out of the model and answers
/// `Unknown`.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Num {
    Int(i128),
    /// `mantissa / 10^scale`.
    Dec {
        mantissa: i128,
        scale: u32,
    },
    Float(f64),
}

// `f64` has no `Eq`/`Hash`; the derived traits on `Number` key memo tables,
// so compare and hash floats by their bit pattern (the parsed constant is
// always finite).
impl Eq for Num {}

impl std::hash::Hash for Num {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Num::Int(i) => {
                0u8.hash(state);
                i.hash(state);
            }
            Num::Dec { mantissa, scale } => {
                1u8.hash(state);
                mantissa.hash(state);
                scale.hash(state);
            }
            Num::Float(f) => {
                2u8.hash(state);
                f.to_bits().hash(state);
            }
        }
    }
}

/// The XSD numeric datatypes the value model covers, by local name, with the
/// inclusive bounds of the bounded integer types (`None` = unbounded).
/// `unsignedLong` and `long` are bounded too, by `u64`/`i64`.
const NUMERIC_TYPES: &[(&str, NumKind)] = &[
    ("integer", NumKind::Int(None, None)),
    ("nonPositiveInteger", NumKind::Int(None, Some(0))),
    ("negativeInteger", NumKind::Int(None, Some(-1))),
    ("nonNegativeInteger", NumKind::Int(Some(0), None)),
    ("positiveInteger", NumKind::Int(Some(1), None)),
    (
        "long",
        NumKind::Int(Some(i64::MIN as i128), Some(i64::MAX as i128)),
    ),
    (
        "int",
        NumKind::Int(Some(i32::MIN as i128), Some(i32::MAX as i128)),
    ),
    (
        "short",
        NumKind::Int(Some(i16::MIN as i128), Some(i16::MAX as i128)),
    ),
    (
        "byte",
        NumKind::Int(Some(i8::MIN as i128), Some(i8::MAX as i128)),
    ),
    (
        "unsignedLong",
        NumKind::Int(Some(0), Some(u64::MAX as i128)),
    ),
    ("unsignedInt", NumKind::Int(Some(0), Some(u32::MAX as i128))),
    (
        "unsignedShort",
        NumKind::Int(Some(0), Some(u16::MAX as i128)),
    ),
    ("unsignedByte", NumKind::Int(Some(0), Some(u8::MAX as i128))),
    ("decimal", NumKind::Decimal),
    ("float", NumKind::Float),
    ("double", NumKind::Float),
];

#[derive(Clone, Copy)]
enum NumKind {
    Int(Option<i128>, Option<i128>),
    Decimal,
    Float,
}

/// The numeric kind of a datatype IRI, or `None` for a non-numeric one.
fn numeric_kind(datatype: &str) -> Option<NumKind> {
    let local = datatype.strip_prefix(XSD)?;
    NUMERIC_TYPES
        .iter()
        .find(|(name, _)| *name == local)
        .map(|(_, kind)| *kind)
}

/// Parse an XSD numeric lexical form of `kind` into the value model, or
/// `None` when the grammar rejects it or the model cannot hold it exactly.
fn parse_number(lexical: &str, kind: NumKind) -> Option<Num> {
    match kind {
        NumKind::Int(lo, hi) => {
            let v = parse_integer(lexical)?;
            if lo.is_some_and(|lo| v < lo) || hi.is_some_and(|hi| v > hi) {
                return None;
            }
            Some(Num::Int(v))
        }
        NumKind::Decimal => parse_decimal(lexical),
        NumKind::Float => parse_float(lexical),
    }
}

/// XSD `integer`: `[+-]?[0-9]+`.
fn parse_integer(lexical: &str) -> Option<i128> {
    let (neg, digits) = split_sign(lexical)?;
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let mut v: i128 = 0;
    for b in digits.bytes() {
        v = v.checked_mul(10)?.checked_add(i128::from(b - b'0'))?;
    }
    Some(if neg { -v } else { v })
}

/// XSD `decimal`: `[+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)`, held as a scaled
/// mantissa; trailing fractional zeros are kept (they do not change the
/// value, and the comparison aligns scales anyway).
fn parse_decimal(lexical: &str) -> Option<Num> {
    let (neg, body) = split_sign(lexical)?;
    let (int_part, frac_part) = match body.split_once('.') {
        Some((i, f)) => (i, f),
        None => (body, ""),
    };
    if int_part.is_empty() && frac_part.is_empty() {
        return None;
    }
    if !int_part.bytes().all(|b| b.is_ascii_digit())
        || !frac_part.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let mut mantissa: i128 = 0;
    for b in int_part.bytes().chain(frac_part.bytes()) {
        mantissa = mantissa
            .checked_mul(10)?
            .checked_add(i128::from(b - b'0'))?;
    }
    let scale = u32::try_from(frac_part.len()).ok()?;
    Some(Num::Dec {
        mantissa: if neg { -mantissa } else { mantissa },
        scale,
    })
}

/// XSD `float`/`double` without the special values:
/// `[+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)([eE][+-]?[0-9]+)?`. `INF`, `-INF` and
/// `NaN` are grammatical but outside the model (`None`).
fn parse_float(lexical: &str) -> Option<Num> {
    let (neg, body) = split_sign(lexical)?;
    let (mantissa, exponent) = match body.find(['e', 'E']) {
        Some(i) => (&body[..i], Some(&body[i + 1..])),
        None => (body, None),
    };
    let (int_part, frac_part) = match mantissa.split_once('.') {
        Some((i, f)) => (i, f),
        None => (mantissa, ""),
    };
    if int_part.is_empty() && frac_part.is_empty() {
        return None;
    }
    if !int_part.bytes().all(|b| b.is_ascii_digit())
        || !frac_part.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    if let Some(exp) = exponent {
        let (_, digits) = split_sign(exp)?;
        if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
    }
    // The grammar above is a subset of what Rust's parser accepts, so this
    // cannot fail; a value past f64's range parses to infinity, which the
    // model excludes.
    let v: f64 = body.parse().ok()?;
    if !v.is_finite() {
        return None;
    }
    Some(Num::Float(if neg { -v } else { v }))
}

/// Split an optional leading sign off a lexical form.
fn split_sign(lexical: &str) -> Option<(bool, &str)> {
    if let Some(rest) = lexical.strip_prefix('-') {
        Some((true, rest))
    } else if let Some(rest) = lexical.strip_prefix('+') {
        Some((false, rest))
    } else {
        Some((false, lexical))
    }
}

/// Integers whose `f64` conversion is exact.
const F64_EXACT_INT: i128 = 1 << 53;

/// Compare two values of the model exactly, or `None` when the model cannot
/// settle the comparison (mixed float/decimal, an integer too wide to hold
/// exactly in an `f64`, an overflow while aligning scales).
fn compare_nums(a: Num, b: Num) -> Option<Ordering> {
    match (a, b) {
        (Num::Int(x), Num::Int(y)) => Some(x.cmp(&y)),
        (Num::Int(x), Num::Dec { mantissa, scale }) => {
            let scaled = x.checked_mul(10i128.checked_pow(scale)?)?;
            Some(scaled.cmp(&mantissa))
        }
        (Num::Dec { .. }, Num::Int(_)) => compare_nums(b, a).map(Ordering::reverse),
        (
            Num::Dec {
                mantissa: ma,
                scale: sa,
            },
            Num::Dec {
                mantissa: mb,
                scale: sb,
            },
        ) => {
            let scale = sa.max(sb);
            let xa = ma.checked_mul(10i128.checked_pow(scale - sa)?)?;
            let xb = mb.checked_mul(10i128.checked_pow(scale - sb)?)?;
            Some(xa.cmp(&xb))
        }
        (Num::Float(x), Num::Float(y)) => x.partial_cmp(&y),
        (Num::Float(x), Num::Int(y)) | (Num::Int(y), Num::Float(x)) => {
            if y.abs() >= F64_EXACT_INT {
                return None;
            }
            let ord = x.partial_cmp(&(y as f64))?;
            Some(if matches!(a, Num::Float(_)) {
                ord
            } else {
                ord.reverse()
            })
        }
        // A decimal with no fractional digits is an integer, and so is an
        // integral float below the exact-integer bound; any other mix of
        // the two models has no exact comparison here.
        (Num::Float(_), Num::Dec { mantissa, scale: 0 }) => compare_nums(a, Num::Int(mantissa)),
        (Num::Dec { mantissa, scale: 0 }, Num::Float(_)) => compare_nums(Num::Int(mantissa), b),
        (Num::Float(x), Num::Dec { .. }) => compare_nums(Num::Int(integral_float(x)?), b),
        (Num::Dec { .. }, Num::Float(y)) => compare_nums(a, Num::Int(integral_float(y)?)),
    }
}

/// A finite, integral float below the exact-integer bound as the integer it
/// is; `None` for anything a decimal cannot be compared with exactly.
fn integral_float(x: f64) -> Option<i128> {
    if x.is_finite() && x.fract() == 0.0 && (x.abs() as i128) < F64_EXACT_INT {
        Some(x as i128)
    } else {
        None
    }
}

/// A literal spelling split into its parts, borrowed from the spelling.
struct LiteralView<'a> {
    /// The lexical form as spelled (N-Triples escapes intact).
    raw_lexical: &'a str,
    /// The language tag, as stored.
    lang: Option<&'a str>,
    /// The datatype IRI without angle brackets, as spelled.
    datatype: Option<&'a str>,
}

impl<'a> LiteralView<'a> {
    /// Split `"lex"`, `"lex"@tag` or `"lex"^^<dt>` through the escape-aware
    /// reading the decode path uses; `None` for anything else.
    fn parse(spelling: &'a str) -> Option<Self> {
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

    /// The lexical form: escapes decoded, `None` for a malformed escape.
    fn lexical(&self) -> Option<Cow<'a, str>> {
        unescape_lexical(self.raw_lexical)
    }

    /// The datatype the SPARQL `datatype()` function reports.
    fn effective_datatype(&self) -> &str {
        match (self.lang, self.datatype) {
            (Some(_), _) => RDF_LANG_STRING,
            (None, Some(dt)) => dt,
            (None, None) => XSD_STRING,
        }
    }

    /// Whether the literal is string-like: plain, language-tagged, or typed
    /// `xsd:string` (which the ingest never writes, but a foreign file may).
    fn is_string_like(&self) -> bool {
        self.lang.is_some() || self.datatype.is_none_or(|dt| dt == XSD_STRING)
    }

    /// The numeric value, when the datatype is numeric and the lexical form
    /// parses under the model.
    fn number(&self) -> Option<Num> {
        let dt = self.datatype?;
        parse_number(&self.lexical()?, numeric_kind(dt)?)
    }

    /// Whether a numeric literal's lexical form parses disregarding the
    /// datatype's own bounds — an out-of-range `xsd:byte`, say. An engine
    /// parses such a form to a value it then rejects, and orders the
    /// literal by its datatype; a form the grammar rejects (whitespace,
    /// `NaN`) its own, more lenient parser may still accept, so that one is
    /// left to it.
    fn parses_unbounded(&self) -> bool {
        let (Some(dt), Some(lexical)) = (self.datatype, self.lexical()) else {
            return false;
        };
        match numeric_kind(dt) {
            Some(NumKind::Int(..)) => parse_integer(&lexical).is_some(),
            Some(NumKind::Decimal) => parse_decimal(&lexical).is_some(),
            Some(NumKind::Float) => parse_float(&lexical).is_some(),
            None => false,
        }
    }

    /// Whether this is an `xsd:long` or `xsd:unsignedLong` whose lexical form
    /// is an integer outside the type's 64-bit bounds — a value rdflib holds
    /// to no bound and compares, which this model refuses.
    fn wide_64(&self) -> bool {
        let Some(local) = self.datatype.and_then(|dt| dt.strip_prefix(XSD)) else {
            return false;
        };
        (local == "long" || local == "unsignedLong")
            && self
                .lexical()
                .is_some_and(|lexical| parse_integer(&lexical).is_some())
            && self.number().is_none()
    }
}

/// The kind of a spelling from its first byte, which the sorted dictionary
/// shares with the kind ranges.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    /// The empty spelling of the default graph.
    DefaultGraph,
    Literal,
    Iri,
    Blank,
    /// Anything a dictionary of this crate never holds.
    Other,
}

fn kind_of(spelling: &str) -> Kind {
    match spelling.as_bytes().first() {
        None => Kind::DefaultGraph,
        Some(b'"') => Kind::Literal,
        Some(b'<') => Kind::Iri,
        Some(b'_') => Kind::Blank,
        Some(_) => Kind::Other,
    }
}

impl TermPredicate {
    /// Parse a `(kind, arg)` pair.
    ///
    /// Kinds: `is_literal`, `is_iri`, `is_blank` (no argument), `datatype`
    /// (an IRI, with or without angle brackets), `lang` (a tag), `lang_matches`
    /// (a BCP 47 language range), `num_lt`, `num_le`, `num_gt`, `num_ge`,
    /// `num_eq`, `num_ne` (an N-Triples numeric literal such as
    /// `"5"^^<http://www.w3.org/2001/XMLSchema#integer>`, or a bare number
    /// typed by its syntax: `5` is an integer, `1.5` a decimal, `1e3` a
    /// double), and `str_prefix` (the raw prefix string).
    pub fn parse(kind: &str, arg: &str) -> Result<Self> {
        let invalid = |msg: String| VortexRdfError::InvalidOperation(msg);
        let predicate = match kind {
            "is_literal" => TermPredicate::IsLiteral,
            "is_iri" => TermPredicate::IsIri,
            "is_blank" => TermPredicate::IsBlank,
            "datatype" => {
                let iri = arg.trim_matches(|c| c == '<' || c == '>');
                if iri.is_empty() {
                    return Err(invalid("datatype predicate needs an IRI argument".into()));
                }
                TermPredicate::Datatype(iri.to_owned())
            }
            "lang" => TermPredicate::Lang(arg.to_owned()),
            "lang_matches" => {
                if arg.is_empty() {
                    return Err(invalid(
                        "lang_matches predicate needs a language range".into(),
                    ));
                }
                TermPredicate::LangMatches(arg.to_owned())
            }
            "num_lt" | "num_le" | "num_gt" | "num_ge" | "num_eq" | "num_ne" => {
                let op = match kind {
                    "num_lt" => NumOp::Lt,
                    "num_le" => NumOp::Le,
                    "num_gt" => NumOp::Gt,
                    "num_ge" => NumOp::Ge,
                    "num_eq" => NumOp::Eq,
                    _ => NumOp::Ne,
                };
                let number = Number::parse(arg).ok_or_else(|| {
                    invalid(format!(
                        "{kind} predicate needs a numeric constant, got {arg:?}"
                    ))
                })?;
                TermPredicate::Num(op, number)
            }
            "str_prefix" => TermPredicate::StrPrefix(arg.to_owned()),
            other => {
                return Err(invalid(format!(
                    "unknown term predicate kind {other:?}; expected one of is_literal, is_iri, \
                     is_blank, datatype, lang, lang_matches, num_lt, num_le, num_gt, num_ge, \
                     num_eq, num_ne, str_prefix"
                )));
            }
        };
        Ok(predicate)
    }

    /// Evaluate the predicate on one N-Triples spelling.
    pub fn eval(&self, spelling: &str) -> Verdict {
        let kind = kind_of(spelling);
        match kind {
            // The default graph's name is not a term the predicates speak of.
            Kind::DefaultGraph | Kind::Other => return Verdict::Unknown,
            Kind::Literal | Kind::Iri | Kind::Blank => {}
        }
        match self {
            TermPredicate::IsLiteral => Verdict::from(kind == Kind::Literal),
            TermPredicate::IsIri => Verdict::from(kind == Kind::Iri),
            TermPredicate::IsBlank => Verdict::from(kind == Kind::Blank),
            TermPredicate::Datatype(dt) => match LiteralView::parse(spelling) {
                Some(lit) => Verdict::from(lit.effective_datatype() == dt),
                // `datatype()` of a non-literal is a type error.
                None if kind != Kind::Literal => Verdict::False,
                None => Verdict::Unknown,
            },
            TermPredicate::Lang(tag) => match LiteralView::parse(spelling) {
                Some(lit) => Verdict::from(lit.lang.unwrap_or("") == tag),
                None if kind != Kind::Literal => Verdict::False,
                None => Verdict::Unknown,
            },
            TermPredicate::LangMatches(range) => match LiteralView::parse(spelling) {
                Some(lit) => match lit.lang {
                    Some(tag) => Verdict::from(lang_matches(tag, range)),
                    // `langMatches("", range)` is false for every range.
                    None => Verdict::False,
                },
                None if kind != Kind::Literal => Verdict::False,
                None => Verdict::Unknown,
            },
            TermPredicate::Num(op, number) => {
                if kind != Kind::Literal {
                    // Comparing a non-literal is a type error; `=` and `!=`
                    // fall back to term (in)equality, which cannot hold.
                    return match op {
                        NumOp::Ne => Verdict::True,
                        _ => Verdict::False,
                    };
                }
                let Some(lit) = LiteralView::parse(spelling) else {
                    return Verdict::Unknown;
                };
                number.compare(op, &lit)
            }
            TermPredicate::StrPrefix(prefix) => match kind {
                Kind::Iri => {
                    Verdict::from(spelling[1..spelling.len() - 1].starts_with(prefix.as_str()))
                }
                Kind::Blank => Verdict::from(spelling[2..].starts_with(prefix.as_str())),
                _ => match LiteralView::parse(spelling) {
                    Some(lit) if lit.is_string_like() => match lit.lexical() {
                        Some(text) => Verdict::from(text.starts_with(prefix.as_str())),
                        None => Verdict::Unknown,
                    },
                    // `str()` of another typed literal is its lexical form
                    // only after the engine's own canonicalization.
                    _ => Verdict::Unknown,
                },
            },
        }
    }

    /// The verdict every code of `kind` gets without its spelling being
    /// read, or `None` when the spelling decides. Always agrees with `eval`
    /// on a spelling of that kind.
    pub(crate) fn kind_verdict(&self, kind: CodeKind) -> Option<Verdict> {
        match (self, kind) {
            (_, CodeKind::Other) => Some(Verdict::Unknown),
            (TermPredicate::IsLiteral, kind) => Some(Verdict::from(kind == CodeKind::Literal)),
            (TermPredicate::IsIri, kind) => Some(Verdict::from(kind == CodeKind::Iri)),
            (TermPredicate::IsBlank, kind) => Some(Verdict::from(kind == CodeKind::Blank)),
            (_, CodeKind::Literal) => None,
            // Term inequality holds for every non-literal.
            (TermPredicate::Num(NumOp::Ne, _), _) => Some(Verdict::True),
            (TermPredicate::StrPrefix(_), _) => None,
            (
                TermPredicate::Datatype(_)
                | TermPredicate::Lang(_)
                | TermPredicate::LangMatches(_)
                | TermPredicate::Num(..),
                _,
            ) => Some(Verdict::False),
        }
    }

    /// The `(kind, arg)` pair this predicate round-trips to, in canonical
    /// form — the identity a memo keys on.
    pub fn canonical(&self) -> (&'static str, String) {
        match self {
            TermPredicate::IsLiteral => ("is_literal", String::new()),
            TermPredicate::IsIri => ("is_iri", String::new()),
            TermPredicate::IsBlank => ("is_blank", String::new()),
            TermPredicate::Datatype(dt) => ("datatype", dt.clone()),
            TermPredicate::Lang(tag) => ("lang", tag.clone()),
            TermPredicate::LangMatches(range) => ("lang_matches", range.clone()),
            TermPredicate::Num(op, number) => (op.kind(), number.canonical()),
            TermPredicate::StrPrefix(prefix) => ("str_prefix", prefix.clone()),
        }
    }
}

impl fmt::Display for TermPredicate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (kind, arg) = self.canonical();
        if arg.is_empty() {
            write!(f, "{kind}")
        } else {
            write!(f, "{kind}({arg})")
        }
    }
}

impl From<bool> for Verdict {
    fn from(b: bool) -> Self {
        if b { Verdict::True } else { Verdict::False }
    }
}

impl Number {
    /// Parse a numeric constant: an N-Triples numeric literal, or a bare
    /// number typed by its syntax (integer, decimal, or double).
    fn parse(arg: &str) -> Option<Self> {
        let arg = arg.trim();
        if arg.starts_with('"') {
            let lit = LiteralView::parse(arg)?;
            let datatype = lit.datatype?;
            let value = parse_number(&lit.lexical()?, numeric_kind(datatype)?)?;
            return Some(Self {
                value,
                datatype: datatype.to_owned(),
            });
        }
        let (local, kind) = if arg.contains(['e', 'E']) {
            ("double", NumKind::Float)
        } else if arg.contains('.') {
            ("decimal", NumKind::Decimal)
        } else {
            ("integer", NumKind::Int(None, None))
        };
        let value = parse_number(arg, kind)?;
        Some(Self {
            value,
            datatype: format!("{XSD}{local}"),
        })
    }

    /// The constant as the N-Triples literal it parsed from (normalized).
    fn canonical(&self) -> String {
        let lexical = match self.value {
            Num::Int(i) => i.to_string(),
            Num::Dec { mantissa, scale } => {
                let digits = mantissa.unsigned_abs().to_string();
                let sign = if mantissa < 0 { "-" } else { "" };
                let scale = scale as usize;
                if scale == 0 {
                    format!("{sign}{digits}")
                } else if digits.len() > scale {
                    let (int_part, frac_part) = digits.split_at(digits.len() - scale);
                    format!("{sign}{int_part}.{frac_part}")
                } else {
                    format!("{sign}0.{:0>width$}", digits, width = scale)
                }
            }
            Num::Float(f) => format!("{f:?}"),
        };
        format!("\"{lexical}\"^^<{}>", self.datatype)
    }

    /// `lit <op> self` under the SPARQL operator mapping.
    fn compare(&self, op: &NumOp, lit: &LiteralView<'_>) -> Verdict {
        if let Some(value) = lit.number() {
            return match compare_nums(value, self.value) {
                Some(ord) => op.apply(ord),
                None => Verdict::Unknown,
            };
        }
        if lit.wide_64() {
            // `xsd:long` / `xsd:unsignedLong` beyond 64 bits: rdflib holds
            // neither to its bound and compares the value, which this model
            // refuses — the verdict is the caller's.
            return Verdict::Unknown;
        }
        // Not a numeric value: a numeric datatype whose lexical form the
        // model does not hold, or a non-numeric literal. For ordering, a
        // language-tagged literal counts as `xsd:string` (its datatype is
        // implicit, and that is the datatype the engine orders it by), not
        // the `rdf:langString` that `datatype()` reports.
        let dt = if lit.lang.is_some() {
            XSD_STRING
        } else {
            lit.effective_datatype()
        };
        if op.is_equality() {
            // Value equality is undefined; term equality would decide, but
            // only an engine knows how it canonicalizes the lexical form.
            return Verdict::Unknown;
        }
        if dt == self.datatype {
            // Same datatype, no value: the engine's own parsing decides.
            return Verdict::Unknown;
        }
        if lit.lang.is_none() && !dt.starts_with(XSD) {
            // A non-XSD datatype against an XSD one: an engine orders them
            // by datatype IRI only when both are XSD; otherwise a type error
            // — which a FILTER folds to false — or an implementation-defined
            // order. Leave it to the engine.
            return Verdict::Unknown;
        }
        if numeric_kind(dt).is_some() && !lit.parses_unbounded() {
            // A numeric datatype whose lexical form the grammar rejects: the
            // engine's own parser may still give it a value (`NaN`, a padded
            // integer) and compare by value, which this model cannot.
            return Verdict::Unknown;
        }
        // Different XSD datatypes with no comparable values — a non-numeric
        // datatype, or a value outside its datatype's bounds, which the
        // engine rejects too — order by their datatype IRIs: the rule the
        // Python fast path applies.
        op.apply(dt.cmp(self.datatype.as_str()))
    }
}

/// BCP 47 basic filtering (RFC 4647 §3.3.1) as SPARQL's `langMatches`.
fn lang_matches(tag: &str, range: &str) -> bool {
    if range == "*" {
        return !tag.is_empty();
    }
    let tag = tag.to_ascii_lowercase();
    let range = range.to_ascii_lowercase();
    tag == range || (tag.starts_with(&range) && tag.as_bytes().get(range.len()) == Some(&b'-'))
}

/// The lexical form of a literal spelled with N-Triples escapes (`\t \b \n
/// \r \f \" \' \\ \uXXXX \UXXXXXXXX`), borrowed when there is none; `None`
/// for any other backslash sequence, which no two readers agree on.
fn unescape_lexical(raw: &str) -> Option<Cow<'_, str>> {
    if !raw.contains('\\') {
        return Some(Cow::Borrowed(raw));
    }
    let mut out = String::with_capacity(raw.len());
    let mut chars = raw.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        out.push(match chars.next()? {
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
        });
    }
    Some(Cow::Owned(out))
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

#[cfg(test)]
mod tests {
    use super::*;

    fn p(kind: &str, arg: &str) -> TermPredicate {
        TermPredicate::parse(kind, arg).expect("valid predicate")
    }

    const INT: &str = "http://www.w3.org/2001/XMLSchema#integer";
    const DEC: &str = "http://www.w3.org/2001/XMLSchema#decimal";
    const DBL: &str = "http://www.w3.org/2001/XMLSchema#double";

    fn typed(lex: &str, dt: &str) -> String {
        format!("\"{lex}\"^^<{dt}>")
    }

    #[test]
    fn kind_predicates_follow_the_first_byte() {
        for (kind, spelling, expect) in [
            ("is_literal", "\"a\"", Verdict::True),
            ("is_literal", "<http://x>", Verdict::False),
            ("is_literal", "_:b", Verdict::False),
            ("is_iri", "<http://x>", Verdict::True),
            ("is_iri", "\"a\"@en", Verdict::False),
            ("is_blank", "_:b", Verdict::True),
            ("is_blank", "<http://x>", Verdict::False),
            ("is_iri", "", Verdict::Unknown),
        ] {
            assert_eq!(p(kind, "").eval(spelling), expect, "{kind} on {spelling:?}");
        }
    }

    #[test]
    fn datatype_reads_the_suffix_only() {
        let s = p("datatype", "<http://www.w3.org/2001/XMLSchema#string>");
        assert_eq!(s.eval("\"a\""), Verdict::True);
        assert_eq!(s.eval("\"a\"@en"), Verdict::False);
        assert_eq!(s.eval(&typed("a", INT)), Verdict::False);
        let i = p("datatype", INT);
        assert_eq!(
            i.eval(&typed("abc", INT)),
            Verdict::True,
            "ill-typed still has the datatype"
        );
        assert_eq!(i.eval("<http://x>"), Verdict::False);
        let ls = p("datatype", RDF_LANG_STRING);
        assert_eq!(ls.eval("\"a\"@en"), Verdict::True);
    }

    #[test]
    fn lang_and_lang_matches() {
        assert_eq!(p("lang", "en").eval("\"a\"@en"), Verdict::True);
        assert_eq!(p("lang", "en").eval("\"a\"@en-gb"), Verdict::False);
        assert_eq!(p("lang", "").eval("\"a\""), Verdict::True);
        assert_eq!(p("lang", "en").eval("<http://x>"), Verdict::False);
        let m = p("lang_matches", "EN");
        assert_eq!(m.eval("\"a\"@en"), Verdict::True);
        assert_eq!(m.eval("\"a\"@en-gb"), Verdict::True);
        assert_eq!(m.eval("\"a\"@eng"), Verdict::False);
        assert_eq!(m.eval("\"a\""), Verdict::False);
        let any = p("lang_matches", "*");
        assert_eq!(any.eval("\"a\"@fr"), Verdict::True);
        assert_eq!(any.eval("\"a\""), Verdict::False);
    }

    #[test]
    fn numeric_values_compare_exactly() {
        let lt5 = p("num_lt", "5");
        assert_eq!(lt5.eval(&typed("4", INT)), Verdict::True);
        assert_eq!(lt5.eval(&typed("5", INT)), Verdict::False);
        assert_eq!(lt5.eval(&typed("4.99", DEC)), Verdict::True);
        assert_eq!(lt5.eval(&typed("4.5e0", DBL)), Verdict::True);
        assert_eq!(lt5.eval(&typed("-0", INT)), Verdict::True);
        assert_eq!(lt5.eval(&typed("+4", INT)), Verdict::True);
        // Bounded types: in range compares by value; out of range has no
        // value, so the datatypes order (`byte` sorts below `integer`).
        assert_eq!(
            lt5.eval(&typed("300", "http://www.w3.org/2001/XMLSchema#byte")),
            Verdict::True
        );
        assert_eq!(
            p("num_gt", "5").eval(&typed("300", "http://www.w3.org/2001/XMLSchema#byte")),
            Verdict::False
        );
        // Grammatical special values are outside the model and deferred.
        for lex in ["NaN", "INF", "-INF"] {
            assert_eq!(lt5.eval(&typed(lex, DBL)), Verdict::Unknown, "{lex}");
        }
        assert_eq!(
            lt5.eval(&typed("3", "http://www.w3.org/2001/XMLSchema#byte")),
            Verdict::True
        );
        // Lexical forms outside the XSD grammar.
        for lex in ["abc", " 4", "4 ", "1_000", "0x10", "4.5", ""] {
            assert_eq!(lt5.eval(&typed(lex, INT)), Verdict::Unknown, "{lex:?}");
        }
        // Specials are unknown; an integral float compares with a decimal
        // exactly, a fractional one does not.
        assert_eq!(lt5.eval(&typed("NaN", DBL)), Verdict::Unknown);
        assert_eq!(lt5.eval(&typed("INF", DBL)), Verdict::Unknown);
        assert_eq!(p("num_lt", "4.5").eval(&typed("4.0", DBL)), Verdict::True);
        assert_eq!(p("num_lt", "4.5").eval(&typed("4", DBL)), Verdict::True);
        assert_eq!(
            p("num_lt", "4.5").eval(&typed("4.25", DBL)),
            Verdict::Unknown
        );
        // Equality and inequality.
        assert_eq!(p("num_eq", "5").eval(&typed("5.0", DEC)), Verdict::True);
        assert_eq!(p("num_ne", "5").eval(&typed("5.0", DEC)), Verdict::False);
        assert_eq!(p("num_eq", "1e2").eval(&typed("100", INT)), Verdict::True);
    }

    #[test]
    fn non_literals_and_other_datatypes() {
        assert_eq!(p("num_ne", "5").eval("<http://x>"), Verdict::True);
        assert_eq!(p("num_eq", "5").eval("_:b"), Verdict::False);
        assert_eq!(p("num_lt", "5").eval("<http://x>"), Verdict::False);
        // A plain string against an integer orders by datatype IRI
        // ("…#string" > "…#integer"), as the Python fast path does.
        assert_eq!(p("num_gt", "5").eval("\"abc\""), Verdict::True);
        assert_eq!(p("num_lt", "5").eval("\"abc\""), Verdict::False);
        assert_eq!(p("num_lt", "5").eval("\"abc\"@en"), Verdict::False);
        // "…#string" < "…#unsignedInt".
        let u = typed("5", "http://www.w3.org/2001/XMLSchema#unsignedInt");
        assert_eq!(p("num_lt", &u).eval("\"abc\""), Verdict::True);
        // Equality against a non-numeric literal is left to the engine.
        assert_eq!(p("num_eq", "5").eval("\"abc\""), Verdict::Unknown);
        // Same datatype, unparseable lexical: unknown.
        assert_eq!(p("num_lt", "5").eval(&typed("abc", INT)), Verdict::Unknown);
        // Different numeric datatype, unparseable lexical: still unknown —
        // the engine's own parser may accept forms this grammar rejects
        // and compare by value.
        assert_eq!(p("num_lt", "5").eval(&typed("abc", DEC)), Verdict::Unknown);
        // Non-XSD datatype: unknown.
        assert_eq!(
            p("num_lt", "5").eval(&typed("1", "http://ex/dt")),
            Verdict::Unknown
        );
    }

    #[test]
    fn str_prefix_over_every_kind() {
        let http = p("str_prefix", "http://ex/");
        assert_eq!(http.eval("<http://ex/a>"), Verdict::True);
        assert_eq!(http.eval("<http://other/a>"), Verdict::False);
        assert_eq!(p("str_prefix", "b").eval("_:b0"), Verdict::True);
        assert_eq!(p("str_prefix", "ab").eval("\"abc\""), Verdict::True);
        assert_eq!(p("str_prefix", "ab").eval("\"abc\"@en"), Verdict::True);
        assert_eq!(
            p("str_prefix", "a\"").eval("\"a\\\"b\""),
            Verdict::True,
            "escaped quote"
        );
        assert_eq!(p("str_prefix", "a\tb").eval("\"a\\tb\""), Verdict::True);
        assert_eq!(p("str_prefix", "é").eval("\"\\u00E9x\""), Verdict::True);
        assert_eq!(p("str_prefix", "ab").eval("\"xab\""), Verdict::False);
        assert_eq!(
            p("str_prefix", "4").eval(&typed("42", INT)),
            Verdict::Unknown
        );
        assert_eq!(p("str_prefix", "").eval("\"\""), Verdict::True);
    }

    #[test]
    fn parse_rejects_bad_input_and_canonicalizes() {
        assert!(TermPredicate::parse("nope", "").is_err());
        assert!(TermPredicate::parse("num_lt", "abc").is_err());
        assert!(TermPredicate::parse("datatype", "").is_err());
        assert_eq!(
            p("num_lt", "5").to_string(),
            format!("num_lt(\"5\"^^<{INT}>)")
        );
        assert_eq!(
            p("num_lt", "1.50").to_string(),
            format!("num_lt(\"1.50\"^^<{DEC}>)")
        );
        assert_eq!(
            p("num_lt", "-.5").to_string(),
            format!("num_lt(\"-0.5\"^^<{DEC}>)")
        );
        assert_eq!(
            p("num_lt", &typed("7", INT)),
            p("num_lt", "7"),
            "a typed literal and a bare integer parse alike"
        );
        assert_eq!(p("datatype", "<http://x>"), p("datatype", "http://x"));
        assert_eq!(p("is_iri", "").to_string(), "is_iri");
    }

    #[test]
    fn wide_64_bit_longs_are_undecided() {
        let long = "http://www.w3.org/2001/XMLSchema#long";
        let ulong = "http://www.w3.org/2001/XMLSchema#unsignedLong";
        for op in ["num_lt", "num_gt", "num_le", "num_ge"] {
            assert_eq!(
                p(op, "5").eval(&typed("99999999999999999999", long)),
                Verdict::Unknown,
                "{op}"
            );
            assert_eq!(
                p(op, "5").eval(&typed("-1", ulong)),
                Verdict::Unknown,
                "{op}"
            );
        }
        assert_eq!(p("num_lt", "5").eval(&typed("7", long)), Verdict::False);
        assert_eq!(p("num_lt", "9").eval(&typed("7", ulong)), Verdict::True);
        // An unbounded integer still compares by value.
        assert_eq!(
            p("num_lt", "5").eval(&typed("99999999999999999999", INT)),
            Verdict::False
        );
    }

    #[test]
    fn lexical_forms_are_read_unescaped() {
        assert_eq!(
            p("num_eq", "5").eval("\"\\u0035\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
            Verdict::True
        );
        assert_eq!(
            p("num_lt", "5").eval("\"\\q\"^^<http://www.w3.org/2001/XMLSchema#integer>"),
            Verdict::Unknown
        );
        assert_eq!(p("str_prefix", "a\"").eval("\"a\\\"b\""), Verdict::True);
        assert_eq!(
            p("lang", "en").eval("\"a\\\"@fr\"@en"),
            Verdict::True,
            "escaped \"@ is not the tag"
        );
    }

    #[test]
    fn kind_verdicts_agree_with_eval() {
        let kinds = KindRanges {
            default_graph: Some(0),
            literals: 1..3,
            iris: 3..4,
            blanks: 4..5,
            len: 5,
        };
        let spellings = [
            "",
            "\"a\"",
            "\"5\"^^<http://www.w3.org/2001/XMLSchema#integer>",
            "<http://x>",
            "_:b",
        ];
        for (kind, arg) in [
            ("is_iri", ""),
            ("is_literal", ""),
            ("is_blank", ""),
            ("datatype", INT),
            ("lang", "en"),
            ("lang_matches", "*"),
            ("num_lt", "5"),
            ("num_ne", "5"),
            ("str_prefix", "h"),
        ] {
            let predicate = p(kind, arg);
            for (code, spelling) in spellings.iter().enumerate() {
                if let Some(verdict) = predicate.kind_verdict(kinds.kind_of_code(code as u32)) {
                    assert_eq!(verdict, predicate.eval(spelling), "{kind} on {spelling}");
                }
            }
        }
    }
}
