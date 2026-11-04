//! The numeric value model behind the `num_*` predicates, after SPARQL 1.1.
//! Its §17.3 maps `<`, `<=`, `>`, `>=`, `=` and `!=` between numbers to
//! XPath's `op:numeric-*` operators, which first promote both operands to
//! one type: an integer or a decimal meets a float (single precision) as a
//! float and a double as a double, and a float meets a double as a double.
//! An integer and a decimal compare exactly.
//!
//! A literal's lexical form is read into the model ([`parse_number`]) and
//! classified ([`Operand`]): a number the model decides, no numeric value
//! at all (ordering it against a number is a type error), or a value an
//! engine may hold apart from the spec, which the caller's engine decides.
//! A definite verdict is one the spec gives and Oxigraph, which passes the
//! W3C SPARQL test suite, gives too; where they can part, the verdict is
//! `Unknown`. [`Number`] is a predicate's parsed constant.

use std::borrow::Cow;
use std::cmp::Ordering;

use crate::common::vocab::XSD;

use super::literal::LiteralView;
use super::{NumOp, Verdict};

/// A numeric constant, as a parsed XSD numeric literal: the payload of
/// [`TermPredicate::Num`](super::TermPredicate::Num), built by
/// [`TermPredicate::parse_with`](super::TermPredicate::parse_with).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Number {
    value: Num,
}

/// A value of the model: an integer of any integer type as an `i128`, a
/// decimal as a scaled `i128` mantissa, an `xsd:float` as a finite `f32`
/// and an `xsd:double` as a finite `f64`. What an XSD grammar accepts but
/// the model cannot hold (an integer or a mantissa past `i128`, a float
/// past its type's range, `INF`, `NaN`) is outside it.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Num {
    Int(i128),
    /// `mantissa / 10^scale`, without trailing fractional zeros: `mantissa`
    /// is no multiple of 10 when `scale` is above 0.
    Dec {
        mantissa: i128,
        scale: u32,
    },
    Float(f32),
    Double(f64),
}

// Floats have no `Eq`/`Hash`: a parsed constant is always finite (never NaN),
// so the derived `PartialEq` is reflexive, and a float hashes by its bit
// pattern, `-0.0` as the `0.0` it equals.
impl Eq for Num {}

impl std::hash::Hash for Num {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match *self {
            Num::Int(i) => (0u8, i).hash(state),
            Num::Dec { mantissa, scale } => (1u8, mantissa, scale).hash(state),
            Num::Float(f) => (2u8, if f == 0.0 { 0.0 } else { f }.to_bits()).hash(state),
            Num::Double(f) => (3u8, if f == 0.0 { 0.0 } else { f }.to_bits()).hash(state),
        }
    }
}

/// The XSD numeric datatypes, by local name, with the inclusive bounds of
/// the bounded integer types (`None` = unbounded). `unsignedLong` and
/// `long` are bounded too, by `u64`/`i64`.
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
    ("double", NumKind::Double),
];

#[derive(Clone, Copy)]
enum NumKind {
    Int(Option<i128>, Option<i128>),
    Decimal,
    Float,
    Double,
}

/// The numeric kind of a datatype IRI, or `None` for a non-numeric one.
fn numeric_kind(datatype: &str) -> Option<NumKind> {
    let local = datatype.strip_prefix(XSD)?;
    NUMERIC_TYPES
        .iter()
        .find(|(name, _)| *name == local)
        .map(|(_, kind)| *kind)
}

/// A lexical form read under a numeric datatype.
enum Parsed {
    /// A value of the model.
    Value(Num),
    /// No value of the model, though an engine may give it one: an integer
    /// past `i128` or outside its type's bounds (Oxigraph reads every
    /// integer type as `xsd:integer`), a decimal whose mantissa is past
    /// `i128`, a float past its type's range, or a special value (`INF`,
    /// `-INF`, `NaN`, or a spelling of one that Rust's float parser, which
    /// Oxigraph reads floats with, takes, such as `inf` or `-nan`).
    Unheld,
    /// Outside the datatype's grammar: the literal is ill-typed, with no
    /// value.
    IllTyped,
}

/// Read an XSD numeric lexical form of `kind` into the model.
fn parse_number(lexical: &str, kind: NumKind) -> Parsed {
    match kind {
        NumKind::Int(lo, hi) => match parse_integer(lexical) {
            Parsed::Value(Num::Int(v))
                if lo.is_some_and(|lo| v < lo) || hi.is_some_and(|hi| v > hi) =>
            {
                Parsed::Unheld
            }
            parsed => parsed,
        },
        NumKind::Decimal => parse_decimal(lexical),
        NumKind::Float => parse_float(lexical, |s| s.parse().ok().map(Num::Float)),
        NumKind::Double => parse_float(lexical, |s| s.parse().ok().map(Num::Double)),
    }
}

/// XSD `integer`: `[+-]?[0-9]+`.
fn parse_integer(lexical: &str) -> Parsed {
    let (neg, digits) = split_sign(lexical);
    if digits.is_empty() || !all_digits(digits) {
        return Parsed::IllTyped;
    }
    match digits_value(digits.bytes()) {
        Some(v) => Parsed::Value(Num::Int(if neg { -v } else { v })),
        None => Parsed::Unheld,
    }
}

/// XSD `decimal`: `[+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)`, held as a scaled
/// mantissa without the trailing fractional zeros, which do not change the
/// value.
fn parse_decimal(lexical: &str) -> Parsed {
    let (neg, body) = split_sign(lexical);
    let Some((int_part, frac_part)) = split_decimal(body) else {
        return Parsed::IllTyped;
    };
    let frac_part = frac_part.trim_end_matches('0');
    let mantissa = digits_value(int_part.bytes().chain(frac_part.bytes()));
    match (mantissa, u32::try_from(frac_part.len())) {
        (Some(mantissa), Ok(scale)) => Parsed::Value(Num::Dec {
            mantissa: if neg { -mantissa } else { mantissa },
            scale,
        }),
        _ => Parsed::Unheld,
    }
}

/// XSD `float`/`double`: the special values, or
/// `[+-]?([0-9]+(\.[0-9]*)?|\.[0-9]+)([eE][+-]?[0-9]+)?` rounded once to
/// the nearest value of the type, by `parse`.
fn parse_float(lexical: &str, parse: impl FnOnce(&str) -> Option<Num>) -> Parsed {
    let (_, body) = split_sign(lexical);
    if ["inf", "infinity", "nan"]
        .iter()
        .any(|special| body.eq_ignore_ascii_case(special))
    {
        return Parsed::Unheld;
    }
    let (mantissa, exponent) = match body.find(['e', 'E']) {
        Some(i) => (&body[..i], Some(&body[i + 1..])),
        None => (body, None),
    };
    let exponent_ok = exponent.is_none_or(|exp| {
        let (_, digits) = split_sign(exp);
        !digits.is_empty() && all_digits(digits)
    });
    if split_decimal(mantissa).is_none() || !exponent_ok {
        return Parsed::IllTyped;
    }
    // The grammar above is a subset of what Rust's parser accepts, and it
    // rounds to nearest; a value past the type's range parses to infinity,
    // which the model excludes.
    match parse(lexical) {
        Some(value @ (Num::Float(_) | Num::Double(_))) if value.is_finite() => Parsed::Value(value),
        _ => Parsed::Unheld,
    }
}

/// The optional leading sign of a lexical form (`true` for `-`) and the
/// rest.
fn split_sign(lexical: &str) -> (bool, &str) {
    if let Some(rest) = lexical.strip_prefix('-') {
        (true, rest)
    } else {
        (false, lexical.strip_prefix('+').unwrap_or(lexical))
    }
}

/// The `(integer, fraction)` digits of an unsigned decimal body
/// (`[0-9]+(\.[0-9]*)?|\.[0-9]+`), or `None` when the grammar rejects it.
fn split_decimal(body: &str) -> Option<(&str, &str)> {
    let (int_part, frac_part) = body.split_once('.').unwrap_or((body, ""));
    let accepted = !(int_part.is_empty() && frac_part.is_empty())
        && all_digits(int_part)
        && all_digits(frac_part);
    accepted.then_some((int_part, frac_part))
}

fn all_digits(s: &str) -> bool {
    s.bytes().all(|b| b.is_ascii_digit())
}

/// The value of ASCII decimal `digits`, most significant first, or `None`
/// on an overflow.
fn digits_value(mut digits: impl Iterator<Item = u8>) -> Option<i128> {
    digits.try_fold(0i128, |v, digit| {
        v.checked_mul(10)?.checked_add(i128::from(digit - b'0'))
    })
}

/// The magnitude below which every integer is an exact double.
const F64_EXACT_INT: u128 = 1 << 53;

/// The bound below which a decimal's mantissa, at most 15 significant
/// digits, is an exact double.
const F64_EXACT_MANTISSA: u128 = 10u128.pow(15);

/// The most fractional digits a decided decimal has: Oxigraph holds a
/// decimal as an `i128` multiple of 10^-18.
const MAX_DECIDED_SCALE: u32 = 18;

/// 10^0 to 10^18, each an exact double.
const POW10_F64: [f64; MAX_DECIDED_SCALE as usize + 1] = [
    1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16,
    1e17, 1e18,
];

impl Num {
    fn is_finite(self) -> bool {
        match self {
            Num::Float(f) => f.is_finite(),
            Num::Double(f) => f.is_finite(),
            Num::Int(_) | Num::Dec { .. } => true,
        }
    }

    /// Whether Oxigraph holds this value as the spec does: an integer
    /// within 64 bits, a decimal of at most 18 fractional digits whose
    /// multiple of 10^-18 is within `i128` (a magnitude below about
    /// 1.7 × 10^20), or a float. These are Oxigraph's own models: past them
    /// it raises an error, and XSD lets an engine support no more (its
    /// minimum is 16 digits).
    fn decided(self) -> bool {
        match self {
            Num::Int(i) => i64::try_from(i).is_ok(),
            Num::Dec { mantissa, scale } => {
                scale <= MAX_DECIDED_SCALE
                    && mantissa
                        .checked_mul(10i128.pow(MAX_DECIDED_SCALE - scale))
                        .is_some()
            }
            Num::Float(_) | Num::Double(_) => true,
        }
    }

    /// An integer or a decimal as `(mantissa, scale)`.
    fn scaled(self) -> Option<(i128, u32)> {
        match self {
            Num::Int(i) => Some((i, 0)),
            Num::Dec { mantissa, scale } => Some((mantissa, scale)),
            Num::Float(_) | Num::Double(_) => None,
        }
    }
}

/// The orderings `a` may take against `b` once XPath has promoted both:
/// two where engines may round a promoted value to two different floats,
/// the one ordering twice otherwise. `None` where the model leaves the
/// comparison to the engine.
fn compare_nums(a: Num, b: Num) -> Option<[Ordering; 2]> {
    if !a.decided() || !b.decided() {
        return None;
    }
    let once = |ord: Ordering| [ord, ord];
    match (a, b) {
        (Num::Float(x), Num::Float(y)) => x.partial_cmp(&y).map(once),
        (Num::Double(x), Num::Double(y)) => x.partial_cmp(&y).map(once),
        // A float widens to a double exactly.
        (Num::Float(x), Num::Double(y)) => f64::from(x).partial_cmp(&y).map(once),
        (Num::Double(x), Num::Float(y)) => x.partial_cmp(&f64::from(y)).map(once),
        (Num::Int(_) | Num::Dec { .. }, Num::Float(y)) => {
            let [p, q] = promote_to_float(a)?;
            Some([p.partial_cmp(&y)?, q.partial_cmp(&y)?])
        }
        (Num::Int(_) | Num::Dec { .. }, Num::Double(y)) => {
            promote_to_double(a)?.partial_cmp(&y).map(once)
        }
        (Num::Float(_) | Num::Double(_), Num::Int(_) | Num::Dec { .. }) => {
            compare_nums(b, a).map(|[p, q]| [p.reverse(), q.reverse()])
        }
        (Num::Int(_) | Num::Dec { .. }, Num::Int(_) | Num::Dec { .. }) => {
            compare_exact(a, b).map(once)
        }
    }
}

/// An integer or a decimal against another, exactly, at the larger scale;
/// `None` on an overflow, which the decided range rules out.
fn compare_exact(a: Num, b: Num) -> Option<Ordering> {
    let (ma, sa) = a.scaled()?;
    let (mb, sb) = b.scaled()?;
    let scale = sa.max(sb);
    let xa = ma.checked_mul(10i128.checked_pow(scale - sa)?)?;
    let xb = mb.checked_mul(10i128.checked_pow(scale - sb)?)?;
    Some(xa.cmp(&xb))
}

/// The double an integer or a decimal promotes to, where the spec and
/// Oxigraph compute the same one: an integer below 2^53, an exact double, or a
/// decimal of at most 15 significant digits, whose mantissa and power of
/// ten are exact doubles, so that dividing one by the other rounds their
/// quotient once, to the nearest double, as the spec does. Oxigraph
/// divides a longer mantissa the same way, which rounds twice. `None` past
/// those bounds, and for a float.
fn promote_to_double(n: Num) -> Option<f64> {
    match n {
        Num::Int(i) => (i.unsigned_abs() < F64_EXACT_INT).then_some(i as f64),
        Num::Dec { mantissa, scale: 0 } => promote_to_double(Num::Int(mantissa)),
        Num::Dec { mantissa, scale } => {
            let pow = POW10_F64.get(scale as usize)?;
            (mantissa.unsigned_abs() < F64_EXACT_MANTISSA).then(|| mantissa as f64 / pow)
        }
        Num::Float(_) | Num::Double(_) => None,
    }
}

/// The floats an integer or a decimal may promote to. The spec rounds the
/// value once, to the nearest float; Oxigraph rounds the double of
/// [`promote_to_double`] to the nearest float, which lands elsewhere only
/// when that double, being no exact value, is the midpoint of two floats.
/// Both floats are given then, and the one float twice otherwise. An
/// integer's double is exact, so its float is rounded once either way.
fn promote_to_float(n: Num) -> Option<[f32; 2]> {
    let double = promote_to_double(n)?;
    let twice = double as f32;
    match n {
        Num::Dec { mantissa, scale } if scale > 0 && halfway_between_floats(double) => {
            let once: f32 = format!("{mantissa}e-{scale}").parse().ok()?;
            Some([once, twice])
        }
        _ => Some([twice, twice]),
    }
}

/// Whether `x` lies exactly halfway between two adjacent floats.
fn halfway_between_floats(x: f64) -> bool {
    let near = x as f32;
    let far = if f64::from(near) < x {
        near.next_up()
    } else {
        near.next_down()
    };
    // Two adjacent floats sum exactly as doubles, and halving is exact.
    f64::from(near) != x && (f64::from(near) + f64::from(far)) / 2.0 == x
}

/// What a literal is to a numeric comparison.
enum Operand {
    /// A number of the model, in the range the spec and Oxigraph decide
    /// alike.
    Number(Num),
    /// No numeric value: a literal of a non-numeric datatype (a string, a
    /// language-tagged literal, a boolean, a datatype outside XSD), or one
    /// whose lexical form its numeric datatype's grammar rejects. Ordering
    /// it against a number is a type error.
    NoValue,
    /// A value an engine may hold apart from the spec or the model cannot
    /// hold ([`Parsed::Unheld`], past [`Num::decided`]), a lexical form
    /// whose escapes do not decode, which readers spell apart, or a
    /// datatype IRI spelled with escapes.
    Undecided,
}

/// What `lit` is to a numeric comparison.
fn operand(lit: &LiteralView<'_>) -> Operand {
    let Some(datatype) = lit.datatype else {
        // A plain or a language-tagged literal.
        return Operand::NoValue;
    };
    if datatype.contains('\\') {
        // An IRI spelled with escapes, as only a foreign writer's file holds
        // one, may name a numeric datatype once decoded.
        return Operand::Undecided;
    }
    let Some(kind) = numeric_kind(datatype) else {
        return Operand::NoValue;
    };
    let Some(lexical) = lit.lexical() else {
        return Operand::Undecided;
    };
    match parse_number(&lexical, kind) {
        Parsed::Value(value) if value.decided() => Operand::Number(value),
        Parsed::Value(_) | Parsed::Unheld => Operand::Undecided,
        Parsed::IllTyped => Operand::NoValue,
    }
}

impl Number {
    /// Parse a numeric constant: an N-Triples numeric literal, or a bare
    /// number typed by its syntax (integer, decimal, or double). `None` for
    /// anything but a value of the model.
    pub(super) fn parse(arg: &str) -> Option<Self> {
        let arg = arg.trim();
        let (lexical, kind) = if arg.starts_with('"') {
            let lit = LiteralView::parse(arg)?;
            let kind = numeric_kind(lit.datatype?)?;
            (lit.lexical()?, kind)
        } else if arg.contains(['e', 'E']) {
            (Cow::Borrowed(arg), NumKind::Double)
        } else if arg.contains('.') {
            (Cow::Borrowed(arg), NumKind::Decimal)
        } else {
            (Cow::Borrowed(arg), NumKind::Int(None, None))
        };
        match parse_number(&lexical, kind) {
            Parsed::Value(value) => Some(Self { value }),
            Parsed::Unheld | Parsed::IllTyped => None,
        }
    }

    /// The constant as an N-Triples literal of its model's datatype
    /// (normalized), which parses back to it.
    pub(super) fn canonical(&self) -> String {
        let (lexical, local) = match self.value {
            Num::Int(i) => (i.to_string(), "integer"),
            Num::Dec { mantissa, scale } => {
                let digits = mantissa.unsigned_abs().to_string();
                let sign = if mantissa < 0 { "-" } else { "" };
                let scale = scale as usize;
                let lexical = if scale == 0 {
                    format!("{sign}{digits}")
                } else if digits.len() > scale {
                    let (int_part, frac_part) = digits.split_at(digits.len() - scale);
                    format!("{sign}{int_part}.{frac_part}")
                } else {
                    format!("{sign}0.{:0>width$}", digits, width = scale)
                };
                (lexical, "decimal")
            }
            Num::Float(f) => (format!("{f:?}"), "float"),
            Num::Double(f) => (format!("{f:?}"), "double"),
        };
        format!("\"{lexical}\"^^<{XSD}{local}>")
    }

    /// `lit <op> self` under SPARQL 1.1.
    pub(super) fn compare(&self, op: NumOp, lit: &LiteralView<'_>) -> Verdict {
        match operand(lit) {
            Operand::Number(value) => match compare_nums(value, self.value) {
                // Where the engines' roundings part, a verdict both give.
                Some([p, q]) if op.apply(p) == op.apply(q) => op.apply(p),
                _ => Verdict::Unknown,
            },
            // `=` and `!=` fall back to RDF term equality, which the spec
            // makes a type error between two literals that are not the same
            // term, and which an extended engine may answer instead (§17.3.1
            // lets it replace a type error with a value). Oxigraph does for
            // datatypes it knows: the terms differ, so `!=` holds there. The
            // engine decides.
            Operand::NoValue if op.is_equality() => Verdict::Unknown,
            // No operator orders a number against a non-number: a type
            // error.
            Operand::NoValue => Verdict::False,
            Operand::Undecided => Verdict::Unknown,
        }
    }
}
