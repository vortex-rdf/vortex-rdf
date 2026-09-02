//! Term predicates over N-Triples spellings: what a SPARQL `FILTER`'s
//! single-variable conjuncts decide about one term, evaluated once per
//! dictionary term by [`TermDictionary::filter_codes`](super::TermDictionary::filter_codes).
//!
//! Every predicate answers with a [`Verdict`]: definitely true, definitely
//! false, or *unknown* — the term is outside the domain the predicate decides
//! exactly (rdflib would raise, parse the lexical form its own way, or
//! compare values this evaluator does not model), and the caller resolves
//! it with the full SPARQL machinery. The rules mirror the ones a SPARQL
//! engine over rdflib applies: `datatype`/`lang` of a non-literal is an
//! error, literals of different datatypes order by their datatype IRI,
//! numeric literals compare by value with the datatype's well-formedness
//! bounds, a NaN meeting a decimal is undecidable.

use std::borrow::Cow;
use std::cmp::Ordering;
use std::fmt;

use oxrdf::Term;

use crate::common::terms::parse_term;
use crate::error::{Result, VortexRdfError};

const XSD: &str = "http://www.w3.org/2001/XMLSchema#";
const XSD_STRING: &str = "http://www.w3.org/2001/XMLSchema#string";
const XSD_INTEGER: &str = "http://www.w3.org/2001/XMLSchema#integer";
const XSD_DECIMAL: &str = "http://www.w3.org/2001/XMLSchema#decimal";
const XSD_DOUBLE: &str = "http://www.w3.org/2001/XMLSchema#double";
const RDF_LANG_STRING: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#langString";

/// What a predicate concludes about one term.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    /// The predicate holds.
    True,
    /// The predicate does not hold.
    False,
    /// Outside the domain decided here: the caller evaluates the term itself.
    Unknown,
}

impl Verdict {
    fn of(holds: bool) -> Self {
        if holds { Verdict::True } else { Verdict::False }
    }

    fn not(self) -> Self {
        match self {
            Verdict::True => Verdict::False,
            Verdict::False => Verdict::True,
            Verdict::Unknown => Verdict::Unknown,
        }
    }
}

/// A numeric comparison operator.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NumOp {
    Lt,
    Le,
    Gt,
    Ge,
    Eq,
    Ne,
}

/// A predicate on one term, parsed from a kind name and its argument.
#[derive(Clone, Debug, PartialEq)]
pub enum TermPredicate {
    /// `isLiteral(?x)`.
    IsLiteral,
    /// `isIRI(?x)`.
    IsIri,
    /// `isBlank(?x)`.
    IsBlank,
    /// `datatype(?x) = <iri>` (a plain literal's datatype is `xsd:string`, a
    /// language-tagged one's `rdf:langString`).
    Datatype(String),
    /// `lang(?x) = "tag"` (exact; `""` for an untagged literal).
    Lang(String),
    /// `langMatches(lang(?x), "range")`.
    LangMatches(String),
    /// `strStarts(str(?x), "prefix")`.
    StrPrefix(String),
    /// `?x <op> constant` for a numeric constant.
    Num(NumOp, NumConst),
}

impl TermPredicate {
    /// The predicate named `kind` with argument `arg`. Kinds: `is_literal`,
    /// `is_iri`, `is_blank` (no argument); `datatype` (an IRI, bare or in
    /// `<>`); `lang` (a tag); `lang_matches` (a language range);
    /// `str_prefix` (a string); `num_lt`, `num_le`, `num_gt`, `num_ge`,
    /// `num_eq`, `num_ne` (a numeric constant, as an N-Triples literal
    /// spelling or a bare number typed by its syntax: integer, decimal, or
    /// double).
    pub fn parse(kind: &str, arg: &str) -> Result<Self> {
        let num = |op| NumConst::parse(arg).map(|constant| TermPredicate::Num(op, constant));
        Ok(match kind {
            "is_literal" => TermPredicate::IsLiteral,
            "is_iri" => TermPredicate::IsIri,
            "is_blank" => TermPredicate::IsBlank,
            "datatype" => TermPredicate::Datatype(
                arg.strip_prefix('<')
                    .and_then(|s| s.strip_suffix('>'))
                    .unwrap_or(arg)
                    .to_string(),
            ),
            "lang" => TermPredicate::Lang(arg.to_string()),
            "lang_matches" => TermPredicate::LangMatches(arg.to_string()),
            "str_prefix" => TermPredicate::StrPrefix(arg.to_string()),
            "num_lt" => num(NumOp::Lt)?,
            "num_le" => num(NumOp::Le)?,
            "num_gt" => num(NumOp::Gt)?,
            "num_ge" => num(NumOp::Ge)?,
            "num_eq" => num(NumOp::Eq)?,
            "num_ne" => num(NumOp::Ne)?,
            other => {
                return Err(VortexRdfError::InvalidOperation(format!(
                    "unknown term predicate kind {other:?}; expected is_literal, is_iri, \
                     is_blank, datatype, lang, lang_matches, str_prefix or num_lt/le/gt/ge/eq/ne"
                )));
            }
        })
    }

    /// The predicate's verdict on the term spelled `spelling`.
    pub fn eval(&self, spelling: &str) -> Verdict {
        let Some(view) = View::of(spelling) else {
            return Verdict::Unknown;
        };
        match self {
            TermPredicate::IsLiteral => Verdict::of(matches!(view, View::Literal { .. })),
            TermPredicate::IsIri => Verdict::of(matches!(view, View::Iri(_))),
            TermPredicate::IsBlank => Verdict::of(matches!(view, View::Blank(_))),
            TermPredicate::Datatype(iri) => match view {
                View::Literal { lang, datatype, .. } => {
                    let effective = match (lang, datatype) {
                        (Some(_), _) => RDF_LANG_STRING,
                        (None, Some(datatype)) => datatype,
                        (None, None) => XSD_STRING,
                    };
                    Verdict::of(effective == iri)
                }
                _ => Verdict::Unknown,
            },
            TermPredicate::Lang(tag) => match view {
                View::Literal { lang, .. } => Verdict::of(lang.unwrap_or("") == tag),
                _ => Verdict::Unknown,
            },
            TermPredicate::LangMatches(range) => match view {
                View::Literal { lang, .. } => match lang {
                    None | Some("") => Verdict::False,
                    Some(lang) => Verdict::of(lang_range_check(range, lang)),
                },
                _ => Verdict::Unknown,
            },
            TermPredicate::StrPrefix(prefix) => match view {
                View::Iri(s) | View::Blank(s) => Verdict::of(s.starts_with(prefix.as_str())),
                View::Literal { lex, datatype, .. } => {
                    if !datatype.is_none_or(|dt| dt == XSD_STRING) {
                        return Verdict::Unknown;
                    }
                    match lexical(spelling, lex) {
                        Some(lex) => Verdict::of(lex.starts_with(prefix.as_str())),
                        None => Verdict::Unknown,
                    }
                }
            },
            TermPredicate::Num(op, constant) => num_verdict(*op, constant, &view),
        }
    }
}

/// The cache key: one canonical spelling per predicate.
impl fmt::Display for TermPredicate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TermPredicate::IsLiteral => f.write_str("is_literal"),
            TermPredicate::IsIri => f.write_str("is_iri"),
            TermPredicate::IsBlank => f.write_str("is_blank"),
            TermPredicate::Datatype(iri) => write!(f, "datatype(<{iri}>)"),
            TermPredicate::Lang(tag) => write!(f, "lang({tag:?})"),
            TermPredicate::LangMatches(range) => write!(f, "lang_matches({range:?})"),
            TermPredicate::StrPrefix(prefix) => write!(f, "str_prefix({prefix:?})"),
            TermPredicate::Num(op, constant) => {
                write!(f, "num_{op:?}(\"{}\"^^<{}>)", constant.lex, constant.datatype)
            }
        }
    }
}

/// A numeric constant: its value, and the lexical form and datatype the
/// comparison rules also consult.
#[derive(Clone, Debug, PartialEq)]
pub struct NumConst {
    lex: String,
    datatype: String,
    value: Num,
}

impl NumConst {
    /// From an N-Triples literal spelling with a numeric datatype, or a bare
    /// number typed by its syntax: an integer, a decimal (with a fraction
    /// point), or a double (anything else that parses as a float).
    fn parse(arg: &str) -> Result<Self> {
        let bad = || {
            VortexRdfError::InvalidOperation(format!(
                "{arg:?} is not a numeric constant: expected an N-Triples numeric literal or a \
                 bare number"
            ))
        };
        if arg.starts_with('"') {
            let Some(View::Literal {
                lex,
                datatype: Some(datatype),
                lang: None,
            }) = View::of(arg)
            else {
                return Err(bad());
            };
            if !is_numeric_datatype(datatype) {
                return Err(bad());
            }
            let Parse::Value(value) = parse_lexical(datatype, lex) else {
                return Err(bad());
            };
            return Ok(Self {
                lex: lex.to_string(),
                datatype: datatype.to_string(),
                value,
            });
        }
        let lex = arg.trim();
        let (datatype, value) = if let Some(int) = parse_int(lex) {
            (XSD_INTEGER, Num::Int(int))
        } else if !lex.contains(['e', 'E']) && parse_decimal(lex).is_some() {
            (XSD_DECIMAL, parse_decimal(lex).ok_or_else(bad)?)
        } else if let Ok(float) = lex.parse::<f64>() {
            (XSD_DOUBLE, Num::Float(float))
        } else {
            return Err(bad());
        };
        Ok(Self {
            lex: lex.to_string(),
            datatype: datatype.to_string(),
            value,
        })
    }
}

/// A numeric value as the comparison rules model it: an integer, a decimal
/// as an unscaled integer and a scale, or a double.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Num {
    Int(i128),
    Dec { unscaled: i128, scale: u32 },
    Float(f64),
}

/// How two numbers relate — or that the relation is not decided here.
enum Cmp {
    Ordered(Ordering),
    /// A NaN against an integer or float: every comparison is false.
    Unordered,
    /// Beyond exact arithmetic (an overflow, a float against a fractional
    /// decimal, a NaN against a decimal).
    Undecidable,
}

/// Compare two numbers exactly.
fn compare(a: Num, b: Num) -> Cmp {
    match (a, b) {
        (Num::Int(x), Num::Int(y)) => Cmp::Ordered(x.cmp(&y)),
        (Num::Float(x), Num::Float(y)) => x.partial_cmp(&y).map_or(Cmp::Unordered, Cmp::Ordered),
        (Num::Float(x), other) => match float_vs(x, other) {
            Some(Cmp::Ordered(ordering)) => Cmp::Ordered(ordering),
            Some(cmp) => cmp,
            None => Cmp::Undecidable,
        },
        (other, Num::Float(y)) => match float_vs(y, other) {
            Some(Cmp::Ordered(ordering)) => Cmp::Ordered(ordering.reverse()),
            Some(cmp) => cmp,
            None => Cmp::Undecidable,
        },
        (a, b) => {
            let (ua, sa) = unscaled(a);
            let (ub, sb) = unscaled(b);
            let scale = sa.max(sb);
            match (rescale(ua, sa, scale), rescale(ub, sb, scale)) {
                (Some(x), Some(y)) => Cmp::Ordered(x.cmp(&y)),
                _ => Cmp::Undecidable,
            }
        }
    }
}

/// `x` (a float) against an integer or decimal, in that order.
fn float_vs(x: f64, other: Num) -> Option<Cmp> {
    if x.is_nan() {
        return Some(match other {
            Num::Dec { .. } => Cmp::Undecidable,
            _ => Cmp::Unordered,
        });
    }
    let y = exact_f64(other)?;
    Some(x.partial_cmp(&y).map_or(Cmp::Unordered, Cmp::Ordered))
}

/// An integer, or an integral decimal, as the float it converts to exactly.
fn exact_f64(n: Num) -> Option<f64> {
    const EXACT: i128 = 1 << 53;
    let int = match n {
        Num::Int(i) => i,
        Num::Dec { unscaled, scale: 0 } => unscaled,
        Num::Dec { .. } | Num::Float(_) => return None,
    };
    (-EXACT..=EXACT).contains(&int).then_some(int as f64)
}

fn unscaled(n: Num) -> (i128, u32) {
    match n {
        Num::Int(i) => (i, 0),
        Num::Dec { unscaled, scale } => (unscaled, scale),
        Num::Float(_) => unreachable!("floats never reach the decimal path"),
    }
}

fn rescale(unscaled: i128, scale: u32, target: u32) -> Option<i128> {
    (scale..target).try_fold(unscaled, |acc, _| acc.checked_mul(10))
}

/// The outcome of reading a numeric datatype's lexical form.
enum Parse {
    Value(Num),
    /// Not a form this parser reads; the reference implementation may read
    /// it, so nothing is concluded from it.
    Unsure,
}

fn is_numeric_datatype(datatype: &str) -> bool {
    datatype.strip_prefix(XSD).is_some_and(|name| {
        matches!(
            name,
            "integer"
                | "nonPositiveInteger"
                | "negativeInteger"
                | "nonNegativeInteger"
                | "positiveInteger"
                | "long"
                | "unsignedLong"
                | "int"
                | "short"
                | "byte"
                | "unsignedInt"
                | "unsignedShort"
                | "unsignedByte"
                | "decimal"
                | "float"
                | "double"
        )
    })
}

/// The value of `lex` under a numeric `datatype`, read the way the
/// reference converters read it.
fn parse_lexical(datatype: &str, lex: &str) -> Parse {
    if lex.contains('\\') {
        return Parse::Unsure;
    }
    let name = datatype.strip_prefix(XSD).unwrap_or(datatype);
    let value = match name {
        "decimal" => parse_decimal(lex.trim()),
        "float" | "double" => lex.trim().parse::<f64>().ok().map(Num::Float),
        _ => parse_int(lex.trim()).map(Num::Int),
    };
    value.map_or(Parse::Unsure, Parse::Value)
}

/// Whether an integer datatype's value is within its bounds — the
/// well-formedness the reference numeric fast path requires.
fn within_bounds(datatype: &str, n: Num) -> bool {
    let Num::Int(v) = n else {
        return true;
    };
    match datatype.strip_prefix(XSD).unwrap_or(datatype) {
        "int" => (-2_147_483_648..=2_147_483_647).contains(&v),
        "short" => (-32_768..=32_767).contains(&v),
        "byte" => (-128..=127).contains(&v),
        "unsignedInt" => (0..=4_294_967_295).contains(&v),
        "unsignedShort" => (0..=65_535).contains(&v),
        "unsignedByte" => (0..=255).contains(&v),
        "nonNegativeInteger" | "unsignedLong" => v >= 0,
        "positiveInteger" => v > 0,
        "nonPositiveInteger" => v <= 0,
        "negativeInteger" => v < 0,
        _ => true,
    }
}

/// An integer lexical form: an optional sign and digits, single underscores
/// allowed between digits.
fn parse_int(s: &str) -> Option<i128> {
    let (negative, digits) = match s.as_bytes().first()? {
        b'-' => (true, &s[1..]),
        b'+' => (false, &s[1..]),
        _ => (false, s),
    };
    let mut value: i128 = 0;
    let mut previous_digit = false;
    for byte in digits.bytes() {
        match byte {
            b'0'..=b'9' => {
                value = value.checked_mul(10)?.checked_add(i128::from(byte - b'0'))?;
                previous_digit = true;
            }
            b'_' if previous_digit => previous_digit = false,
            _ => return None,
        }
    }
    if !previous_digit {
        return None;
    }
    Some(if negative { -value } else { value })
}

/// A decimal lexical form: an optional sign, digits with an optional
/// fraction, an optional exponent.
fn parse_decimal(s: &str) -> Option<Num> {
    let (negative, rest) = match s.as_bytes().first()? {
        b'-' => (true, &s[1..]),
        b'+' => (false, &s[1..]),
        _ => (false, s),
    };
    let (mantissa, exponent) = match rest.split_once(['e', 'E']) {
        Some((mantissa, exponent)) => (mantissa, exponent.parse::<i32>().ok()?),
        None => (rest, 0),
    };
    let (int_part, frac_part) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    if int_part.is_empty() && frac_part.is_empty() {
        return None;
    }
    let mut unscaled: i128 = 0;
    for byte in int_part.bytes().chain(frac_part.bytes()) {
        match byte {
            b'0'..=b'9' => unscaled = unscaled.checked_mul(10)?.checked_add(i128::from(byte - b'0'))?,
            _ => return None,
        }
    }
    let scale = i64::from(u32::try_from(frac_part.len()).ok()?) - i64::from(exponent);
    let (unscaled, scale) = if scale < 0 {
        (rescale(unscaled, 0, u32::try_from(-scale).ok()?)?, 0)
    } else {
        (unscaled, u32::try_from(scale).ok()?)
    };
    Some(Num::Dec {
        unscaled: if negative { -unscaled } else { unscaled },
        scale,
    })
}

/// `?x <op> constant` for a term: the reference `_relational` over
/// `Literal.__gt__`/`Literal.eq`, with a non-literal an error for the
/// ordering operators and simply unequal for `=`/`!=`.
fn num_verdict(op: NumOp, constant: &NumConst, view: &View<'_>) -> Verdict {
    let View::Literal {
        lex,
        lang,
        datatype,
    } = view
    else {
        return match op {
            NumOp::Eq => Verdict::False,
            NumOp::Ne => Verdict::True,
            _ => Verdict::Unknown,
        };
    };
    let datatype = datatype.unwrap_or(XSD_STRING);
    let parsed = if is_numeric_datatype(datatype) {
        Some(parse_lexical(datatype, lex))
    } else {
        None
    };
    // The reference numeric value: a converting lexical form within the
    // datatype's bounds. A form this parser cannot read decides nothing.
    let converted = match parsed {
        Some(Parse::Value(n)) => Some(n),
        Some(Parse::Unsure) => return Verdict::Unknown,
        None => None,
    };
    let numeric = converted.filter(|&n| within_bounds(datatype, n));
    let num_compare = |a: Num, wanted: Ordering| match compare(a, constant.value) {
        Cmp::Ordered(ordering) => Verdict::of(ordering == wanted),
        Cmp::Unordered => Verdict::False,
        Cmp::Undecidable => Verdict::Unknown,
    };
    // `Literal.__gt__`: by value when both are numeric, else by datatype IRI
    // when the datatypes differ, else not decided here.
    let gt = || match numeric {
        Some(a) => num_compare(a, Ordering::Greater),
        None if datatype != constant.datatype => Verdict::of(datatype > constant.datatype.as_str()),
        None => Verdict::Unknown,
    };
    // `Literal.eq`: by value when both are numeric; a language tag or a
    // different datatype is unequal; the same datatype without a value
    // compares spellings, and differing spellings are an error.
    let eq = || match numeric {
        Some(a) => num_compare(a, Ordering::Equal),
        None if lang.is_some_and(|tag| !tag.is_empty()) => Verdict::False,
        None if datatype != constant.datatype => Verdict::False,
        None => match converted {
            Some(a) => num_compare(a, Ordering::Equal),
            None if *lex == constant.lex => Verdict::True,
            None => Verdict::Unknown,
        },
    };
    match op {
        NumOp::Eq => eq(),
        NumOp::Ne => eq().not(),
        NumOp::Gt => gt(),
        NumOp::Lt | NumOp::Le | NumOp::Ge => match gt() {
            Verdict::True => Verdict::of(op == NumOp::Ge),
            Verdict::Unknown => Verdict::Unknown,
            Verdict::False => {
                let eq = eq();
                match op {
                    NumOp::Lt => eq.not(),
                    NumOp::Ge => eq,
                    _ => match eq {
                        Verdict::Unknown => Verdict::Unknown,
                        _ => Verdict::True,
                    },
                }
            }
        },
    }
}

/// The reference `langMatches` range check: lowercase subtags, `*`
/// matching any, the range no longer than the tag.
fn lang_range_check(range: &str, lang: &str) -> bool {
    let range: Vec<String> = range.to_lowercase().split('-').map(str::to_string).collect();
    let lang: Vec<String> = lang.to_lowercase().split('-').map(str::to_string).collect();
    let matches = |r: &str, l: &str| r == "*" || r == l;
    if !matches(&range[0], &lang[0]) || range.len() > lang.len() {
        return false;
    }
    range.iter().zip(&lang).all(|(r, l)| matches(r, l))
}

/// One term's spelling, split by kind.
enum View<'a> {
    Iri(&'a str),
    Blank(&'a str),
    Literal {
        /// The lexical form as spelled — escapes still in place.
        lex: &'a str,
        lang: Option<&'a str>,
        datatype: Option<&'a str>,
    },
}

impl<'a> View<'a> {
    /// `None` for anything that is not an N-Triples term — including the
    /// empty string the default graph is spelled as.
    fn of(spelling: &'a str) -> Option<Self> {
        match spelling.as_bytes().first()? {
            b'<' => spelling
                .strip_prefix('<')
                .and_then(|s| s.strip_suffix('>'))
                .map(View::Iri),
            b'_' => spelling.strip_prefix("_:").map(View::Blank),
            b'"' => {
                let close = closing_quote(spelling.as_bytes())?;
                let lex = &spelling[1..close];
                let rest = &spelling[close + 1..];
                if rest.is_empty() {
                    Some(View::Literal {
                        lex,
                        lang: None,
                        datatype: None,
                    })
                } else if let Some(tag) = rest.strip_prefix('@') {
                    Some(View::Literal {
                        lex,
                        lang: Some(tag),
                        datatype: None,
                    })
                } else {
                    let datatype = rest.strip_prefix("^^<")?.strip_suffix('>')?;
                    Some(View::Literal {
                        lex,
                        lang: None,
                        datatype: Some(datatype),
                    })
                }
            }
            _ => None,
        }
    }
}

/// The index of the quote closing the literal opened at byte 0.
fn closing_quote(bytes: &[u8]) -> Option<usize> {
    let mut i = 1;
    while i < bytes.len() {
        match bytes[i] {
            b'\\' => i += 2,
            b'"' => return Some(i),
            _ => i += 1,
        }
    }
    None
}

/// The literal's lexical form with its escapes decoded — as spelled when
/// it has none, else through the term parser.
fn lexical<'a>(spelling: &'a str, lex: &'a str) -> Option<Cow<'a, str>> {
    if !lex.contains('\\') {
        return Some(Cow::Borrowed(lex));
    }
    match parse_term(spelling)? {
        Term::Literal(literal) => Some(Cow::Owned(literal.value().to_string())),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pred(kind: &str, arg: &str) -> TermPredicate {
        TermPredicate::parse(kind, arg).unwrap()
    }

    const INT: &str = "http://www.w3.org/2001/XMLSchema#integer";

    #[test]
    fn kinds_are_decided_for_every_term() {
        for (kind, iri, blank, literal) in [
            ("is_iri", Verdict::True, Verdict::False, Verdict::False),
            ("is_blank", Verdict::False, Verdict::True, Verdict::False),
            ("is_literal", Verdict::False, Verdict::False, Verdict::True),
        ] {
            let p = pred(kind, "");
            assert_eq!(p.eval("<http://x>"), iri, "{kind}");
            assert_eq!(p.eval("_:b0"), blank, "{kind}");
            assert_eq!(p.eval("\"x\"@en"), literal, "{kind}");
            assert_eq!(p.eval(""), Verdict::Unknown, "{kind}: the default graph");
        }
    }

    #[test]
    fn datatype_lang_and_prefix_follow_the_reference() {
        let dt = pred("datatype", "<http://www.w3.org/2001/XMLSchema#string>");
        assert_eq!(dt.eval("\"x\""), Verdict::True);
        assert_eq!(dt.eval(&format!("\"1\"^^<{INT}>")), Verdict::False);
        assert_eq!(dt.eval("\"x\"@en"), Verdict::False);
        assert_eq!(pred("datatype", RDF_LANG_STRING).eval("\"x\"@en"), Verdict::True);
        assert_eq!(dt.eval("<http://x>"), Verdict::Unknown);

        let lang = pred("lang", "en");
        assert_eq!(lang.eval("\"x\"@en"), Verdict::True);
        assert_eq!(lang.eval("\"x\"@EN"), Verdict::False);
        assert_eq!(lang.eval("\"x\""), Verdict::False);
        assert_eq!(pred("lang", "").eval("\"x\""), Verdict::True);
        assert_eq!(lang.eval("_:b"), Verdict::Unknown);

        let matches = pred("lang_matches", "EN");
        assert_eq!(matches.eval("\"x\"@en-GB"), Verdict::True);
        assert_eq!(matches.eval("\"x\"@en"), Verdict::True);
        assert_eq!(matches.eval("\"x\"@eng"), Verdict::False);
        assert_eq!(matches.eval("\"x\""), Verdict::False);
        assert_eq!(pred("lang_matches", "*").eval("\"x\"@fr"), Verdict::True);
        assert_eq!(pred("lang_matches", "*").eval("\"x\""), Verdict::False);
        assert_eq!(pred("lang_matches", "en-gb-x").eval("\"x\"@en-GB"), Verdict::False);
        assert_eq!(matches.eval("<http://x>"), Verdict::Unknown);

        let prefix = pred("str_prefix", "http://ex.org/");
        assert_eq!(prefix.eval("<http://ex.org/a>"), Verdict::True);
        assert_eq!(prefix.eval("<https://ex.org/a>"), Verdict::False);
        assert_eq!(pred("str_prefix", "b").eval("_:b0"), Verdict::True);
        assert_eq!(pred("str_prefix", "A").eval("\"Alice\""), Verdict::True);
        assert_eq!(pred("str_prefix", "A").eval("\"Alice\"@en"), Verdict::True);
        assert_eq!(pred("str_prefix", "A").eval("\"alice\""), Verdict::False);
        assert_eq!(pred("str_prefix", "a\"b").eval("\"a\\\"bc\""), Verdict::True);
        assert_eq!(pred("str_prefix", "4").eval(&format!("\"42\"^^<{INT}>")), Verdict::Unknown);
    }

    #[test]
    fn numbers_compare_by_value_and_datatypes_order_by_iri() {
        let gt40 = pred("num_gt", "40");
        assert_eq!(gt40.eval(&format!("\"42\"^^<{INT}>")), Verdict::True);
        assert_eq!(gt40.eval(&format!("\"40\"^^<{INT}>")), Verdict::False);
        assert_eq!(gt40.eval("\"41.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>"), Verdict::True);
        assert_eq!(gt40.eval("\"4e1\"^^<http://www.w3.org/2001/XMLSchema#double>"), Verdict::False);
        assert_eq!(gt40.eval("\"NaN\"^^<http://www.w3.org/2001/XMLSchema#double>"), Verdict::False);
        // A different datatype orders by its IRI: xsd:string > xsd:integer.
        assert_eq!(gt40.eval("\"Alice\""), Verdict::True);
        assert_eq!(pred("num_lt", "40").eval("\"Alice\""), Verdict::False);
        // Same datatype, an unreadable form: decided by nothing here.
        assert_eq!(gt40.eval(&format!("\"forty\"^^<{INT}>")), Verdict::Unknown);
        // Out of the datatype's bounds: not a numeric value; same datatype as
        // an integer constant? No — xsd:byte orders below xsd:integer.
        assert_eq!(gt40.eval("\"300\"^^<http://www.w3.org/2001/XMLSchema#byte>"), Verdict::False);
        assert_eq!(gt40.eval("<http://x>"), Verdict::Unknown);

        let eq42 = pred("num_eq", &format!("\"42\"^^<{INT}>"));
        assert_eq!(eq42.eval(&format!("\"42\"^^<{INT}>")), Verdict::True);
        assert_eq!(eq42.eval(&format!("\"042\"^^<{INT}>")), Verdict::True);
        assert_eq!(eq42.eval("\"42.0\"^^<http://www.w3.org/2001/XMLSchema#decimal>"), Verdict::True);
        assert_eq!(eq42.eval("\"42\""), Verdict::False);
        assert_eq!(eq42.eval("\"42\"@en"), Verdict::False);
        assert_eq!(eq42.eval("<http://x>"), Verdict::False);
        assert_eq!(pred("num_ne", "42").eval("<http://x>"), Verdict::True);
        assert_eq!(eq42.eval(&format!("\"forty-two\"^^<{INT}>")), Verdict::Unknown);

        let le = pred("num_le", "1.5");
        assert_eq!(le.eval(&format!("\"1\"^^<{INT}>")), Verdict::True);
        assert_eq!(le.eval(&format!("\"2\"^^<{INT}>")), Verdict::False);
        assert_eq!(pred("num_ge", "1.5").eval("\"1.5\"^^<http://www.w3.org/2001/XMLSchema#decimal>"), Verdict::True);
        // A float against a fractional decimal is not decided exactly here.
        assert_eq!(pred("num_lt", "0.1").eval("\"0.1\"^^<http://www.w3.org/2001/XMLSchema#double>"), Verdict::Unknown);
        assert!(TermPredicate::parse("num_gt", "forty").is_err());
        assert!(TermPredicate::parse("is_prime", "").is_err());
    }
}
