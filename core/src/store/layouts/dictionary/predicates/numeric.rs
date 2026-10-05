//! The numeric value model behind the `num_*` predicates: XSD numeric
//! lexical forms parsed exactly (integers, scaled decimals, finite floats),
//! their comparison, and a predicate's parsed constant.

use std::cmp::Ordering;

use crate::common::vocab::{XSD, XSD_STRING};

use super::literal::LiteralView;
use super::{NumOp, Verdict};

/// A numeric constant: a parsed XSD numeric literal and its datatype.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Number {
    value: Num,
    /// The datatype IRI, without angle brackets.
    datatype: String,
}

/// The exact value model: integers as `i128`, decimals as a scaled `i128`
/// mantissa, floats as `f64`. Anything the XSD grammars accept but the model
/// cannot hold exactly (more than 38 digits, `INF`, `NaN`) is outside it and
/// answers `Unknown`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(super) enum Num {
    Int(i128),
    /// `mantissa / 10^scale`.
    Dec {
        mantissa: i128,
        scale: u32,
    },
    Float(f64),
}

// `f64` has no `Eq`/`Hash`: floats compare and hash by bit pattern (a parsed
// constant is always finite).
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

/// The XSD numeric datatypes of the model by local name, with the inclusive
/// bounds of the bounded integer types (`None` = unbounded).
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
pub(super) enum NumKind {
    Int(Option<i128>, Option<i128>),
    Decimal,
    Float,
}

impl NumKind {
    /// The kind without its datatype's bounds.
    pub(super) fn unbounded(self) -> Self {
        match self {
            NumKind::Int(..) => NumKind::Int(None, None),
            other => other,
        }
    }
}

/// The numeric kind of a datatype IRI; `None` for a non-numeric one.
pub(super) fn numeric_kind(datatype: &str) -> Option<NumKind> {
    let local = datatype.strip_prefix(XSD)?;
    NUMERIC_TYPES
        .iter()
        .find(|(name, _)| *name == local)
        .map(|(_, kind)| *kind)
}

/// An XSD numeric lexical form of `kind` in the model; `None` when the
/// grammar rejects it or the model cannot hold it exactly.
pub(super) fn parse_number(lexical: &str, kind: NumKind) -> Option<Num> {
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
    if digits.is_empty() {
        return None;
    }
    let v = digits_value(digits.bytes().map(|b| (b as char).to_digit(10)), 10)?;
    Some(if neg { -v } else { v })
}

/// XSD `decimal`: `[+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)`, as a scaled mantissa;
/// trailing fractional zeros are kept.
fn parse_decimal(lexical: &str) -> Option<Num> {
    let (neg, body) = split_sign(lexical)?;
    let (int_part, frac_part) = split_decimal(body)?;
    let digits = int_part.bytes().chain(frac_part.bytes());
    let mantissa = digits_value(digits.map(|b| (b as char).to_digit(10)), 10)?;
    let scale = u32::try_from(frac_part.len()).ok()?;
    Some(Num::Dec {
        mantissa: if neg { -mantissa } else { mantissa },
        scale,
    })
}

/// XSD `float`/`double` without the special values:
/// `[+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)([eE][+-]?[0-9]+)?`; `INF`, `-INF` and
/// `NaN` are outside the model.
fn parse_float(lexical: &str) -> Option<Num> {
    let (neg, body) = split_sign(lexical)?;
    let (mantissa, exponent) = match body.find(['e', 'E']) {
        Some(i) => (&body[..i], Some(&body[i + 1..])),
        None => (body, None),
    };
    split_decimal(mantissa)?;
    if let Some(exp) = exponent {
        let (_, digits) = split_sign(exp)?;
        if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
    }
    // A value past f64's range parses to infinity, which the model excludes.
    let v: f64 = body.parse().ok()?;
    if !v.is_finite() {
        return None;
    }
    Some(Num::Float(if neg { -v } else { v }))
}

/// The optional leading sign (`true` = negative) and the rest of a lexical
/// form.
fn split_sign(lexical: &str) -> Option<(bool, &str)> {
    if let Some(rest) = lexical.strip_prefix('-') {
        Some((true, rest))
    } else if let Some(rest) = lexical.strip_prefix('+') {
        Some((false, rest))
    } else {
        Some((false, lexical))
    }
}

/// The `(integer, fraction)` digits of an unsigned decimal body
/// (`[0-9]+(\.[0-9]*)?|\.[0-9]+`); `None` when the grammar rejects it.
fn split_decimal(body: &str) -> Option<(&str, &str)> {
    let (int_part, frac_part) = body.split_once('.').unwrap_or((body, ""));
    let digits = |s: &str| s.bytes().all(|b| b.is_ascii_digit());
    let accepted =
        !(int_part.is_empty() && frac_part.is_empty()) && digits(int_part) && digits(frac_part);
    accepted.then_some((int_part, frac_part))
}

/// The value of `digits` in `radix`; `None` for a non-digit or an overflow.
pub(super) fn digits_value(
    mut digits: impl Iterator<Item = Option<u32>>,
    radix: u32,
) -> Option<i128> {
    digits.try_fold(0i128, |v, digit| {
        v.checked_mul(i128::from(radix))?
            .checked_add(i128::from(digit?))
    })
}

/// Integers whose `f64` conversion is exact.
const F64_EXACT_INT: i128 = 1 << 53;

/// Compare two values exactly; `None` when the model cannot settle it (a
/// fractional float against a decimal, an integer at or past
/// [`F64_EXACT_INT`] against a float, an overflow aligning scales).
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
        // A decimal without fractional digits is an integer, and so is an
        // integral float below the exact-integer bound; no other float/decimal
        // mix compares exactly.
        (Num::Float(_), Num::Dec { mantissa, scale: 0 }) => compare_nums(a, Num::Int(mantissa)),
        (Num::Dec { mantissa, scale: 0 }, Num::Float(_)) => compare_nums(Num::Int(mantissa), b),
        (Num::Float(x), Num::Dec { .. }) => compare_nums(Num::Int(integral_float(x)?), b),
        (Num::Dec { .. }, Num::Float(y)) => compare_nums(a, Num::Int(integral_float(y)?)),
    }
}

/// A finite, integral float below [`F64_EXACT_INT`] as an integer; `None`
/// otherwise.
fn integral_float(x: f64) -> Option<i128> {
    if x.is_finite() && x.fract() == 0.0 && (x.abs() as i128) < F64_EXACT_INT {
        Some(x as i128)
    } else {
        None
    }
}

impl Number {
    /// Parse a constant: an N-Triples numeric literal, or a bare number typed
    /// by its syntax (integer, decimal, or double).
    pub(super) fn parse(arg: &str) -> Option<Self> {
        let arg = arg.trim();
        if arg.starts_with('"') {
            let lit = LiteralView::parse(arg)?;
            let datatype = lit.datatype?;
            let value = parse_number(lit.lexical, numeric_kind(datatype)?)?;
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

    /// The constant as a normalized N-Triples literal.
    pub(super) fn canonical(&self) -> String {
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
    pub(super) fn compare(&self, op: NumOp, lit: &LiteralView<'_>) -> Verdict {
        if let Some(value) = lit.number() {
            return match compare_nums(value, self.value) {
                Some(ord) => op.apply(ord),
                None => Verdict::Unknown,
            };
        }
        // Without a value the literal orders by datatype; a language-tagged
        // literal orders as `xsd:string`.
        let dt = if lit.lang.is_some() {
            XSD_STRING
        } else {
            lit.effective_datatype()
        };
        if op.is_equality() {
            // Equality without a value is left to the engine.
            return Verdict::Unknown;
        }
        if dt == self.datatype {
            // Same datatype, no value: the engine's own parsing decides.
            return Verdict::Unknown;
        }
        if lit.lang.is_none() && !dt.starts_with(XSD) {
            // A non-XSD datatype has no datatype-IRI order against an XSD one.
            return Verdict::Unknown;
        }
        if numeric_kind(dt).is_some() && !lit.parses_unbounded() {
            // A numeric lexical form the grammar rejects may still parse for
            // the engine.
            return Verdict::Unknown;
        }
        // Distinct XSD datatypes with no comparable values order by their
        // datatype IRIs.
        op.apply(dt.cmp(self.datatype.as_str()))
    }
}
