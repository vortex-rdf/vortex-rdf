//! Term predicates evaluated on N-Triples spellings: single-variable SPARQL
//! tests (`isIRI(?x)`, `datatype(?x) = <dt>`, `lang(?x) = "en"`,
//! `langMatches`, `?x < 5`, `strstarts`, `contains`, `regex`, …) read off one
//! term's spelling, with a three-valued [`Verdict`]. Verdicts are
//! conservative: `True` or `False` only where the SPARQL semantics over the
//! stored spelling are total, `Unknown` otherwise (a lexical form outside
//! the XSD grammar, a comparison the value model cannot settle exactly, an
//! unknown datatype), for a full SPARQL engine to decide. A caller never gets
//! a wrong definite answer. Over candidate codes, a code whose kind range
//! decides it ([`TermPredicate::kind_verdict`]) is not read.

use std::cmp::Ordering;
use std::fmt;
use std::ops::Range;

use crate::error::{Result, VortexRdfError};
use crate::store::schema::TermCode;

mod literal;
mod numeric;
mod regex_filter;
mod text;

use self::literal::{Kind, LiteralView, kind_of, lang_matches};
pub use self::numeric::Number;
use self::regex_filter::RegexTest;
pub use self::text::{CaseMap, TextOptions, TextPredicate};
use self::text::{TextConstant, TextTest};

/// The code ranges of a sorted dictionary's term kinds. Codes are the ranks
/// of the N-Triples spellings from the dictionary's first code, so each kind
/// is one contiguous range, in byte order: the default graph's empty
/// spelling (the first code, when any quad is in the default graph), then
/// literals (`"`), IRIs (`<`) and blank nodes (`_:`). Spellings no dictionary
/// of this crate holds fall in the gaps.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KindRanges {
    /// The dictionary's first code: every code is at or above it. 0 in every
    /// dictionary this crate builds or opens.
    pub start: TermCode,
    /// The code of `""`, the default graph's name, when present.
    pub default_graph: Option<TermCode>,
    /// Codes of the literals.
    pub literals: Range<TermCode>,
    /// Codes of the IRIs.
    pub iris: Range<TermCode>,
    /// Codes of the blank nodes.
    pub blanks: Range<TermCode>,
    /// One past the dictionary's last code: every code is below it. With
    /// `start` 0 it is the dictionary's size.
    pub len: TermCode,
}

impl KindRanges {
    /// The ranges of a dictionary whose codes run from `start` up to `len`,
    /// and whose first term is the default graph's `""` when
    /// `first_is_empty`.
    pub(super) fn new(
        start: TermCode,
        first_is_empty: bool,
        literals: Range<TermCode>,
        iris: Range<TermCode>,
        blanks: Range<TermCode>,
        len: TermCode,
    ) -> Self {
        Self {
            start,
            default_graph: first_is_empty.then_some(start),
            literals,
            iris,
            blanks,
            len,
        }
    }

    /// The codes in none of the three kind ranges, ascending from `start`:
    /// the default graph's `""` and a foreign writer's spellings.
    pub fn gaps(&self) -> impl Iterator<Item = TermCode> + '_ {
        (self.start..self.literals.start)
            .chain(self.literals.end..self.iris.start)
            .chain(self.iris.end..self.blanks.start)
            .chain(self.blanks.end..self.len)
    }
}

impl KindRanges {
    /// The kind of `code`'s spelling, read off the ranges.
    pub(crate) fn kind_of_code(&self, code: TermCode) -> Kind {
        if self.literals.contains(&code) {
            Kind::Literal
        } else if self.iris.contains(&code) {
            Kind::Iri
        } else if self.blanks.contains(&code) {
            Kind::Blank
        } else {
            Kind::Other
        }
    }
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

/// Every numeric operator: `parse` finds a `num_*` kind's operator here by
/// its [`kind`](NumOp::kind).
const NUM_OPS: [NumOp; 6] = [
    NumOp::Lt,
    NumOp::Le,
    NumOp::Gt,
    NumOp::Ge,
    NumOp::Eq,
    NumOp::Ne,
];

impl NumOp {
    /// The `num_*` predicate kind of this operator.
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
        Verdict::from(match self {
            NumOp::Lt => ord == Ordering::Less,
            NumOp::Le => ord != Ordering::Greater,
            NumOp::Gt => ord == Ordering::Greater,
            NumOp::Ge => ord != Ordering::Less,
            NumOp::Eq => ord == Ordering::Equal,
            NumOp::Ne => ord != Ordering::Equal,
        })
    }

    fn is_equality(self) -> bool {
        matches!(self, NumOp::Eq | NumOp::Ne)
    }
}

/// A single-variable term predicate, parsed from `(kind, arg)` and, for the
/// string kinds, [`TextOptions`].
#[derive(Debug, Clone, PartialEq)]
pub enum TermPredicate {
    /// `isLITERAL(?x)`.
    IsLiteral,
    /// `isIRI(?x)` / `isURI(?x)`.
    IsIri,
    /// `isBLANK(?x)`.
    IsBlank,
    /// `datatype(?x) = <iri>`: a plain literal has `xsd:string`, a
    /// language-tagged one `rdf:langString`, a typed one its datatype; the
    /// lexical form is never inspected.
    Datatype(String),
    /// `lang(?x) = "tag"`: an exact comparison with the stored tag (which the
    /// ingest lowercases); an untagged literal has the empty tag.
    Lang(String),
    /// `langMatches(lang(?x), "range")`: BCP 47 basic filtering, `*` matching
    /// any non-empty tag and any other range matching the tag, or a
    /// `-`-delimited prefix of it, case-insensitively. Any range but `*` with
    /// a character outside ASCII letters, digits and hyphens is undecided on
    /// a tagged literal: rdflib may read it differently.
    LangMatches(String),
    /// `?x <op> <numeric constant>` under the SPARQL operator mapping.
    Num(NumOp, Number),
    /// A string kind: `str_prefix`, `contains`, `strstarts`, `strends`
    /// (and `regex`), read on the term's text under the [`TextOptions`] it
    /// was parsed with.
    Text(TextPredicate),
}

/// `f` on the literal `spelling` parses to; `False` for a spelling of
/// another kind (a type error), `Unknown` for a literal that does not parse.
fn on_literal(kind: Kind, spelling: &str, f: impl FnOnce(&LiteralView<'_>) -> Verdict) -> Verdict {
    match LiteralView::parse(spelling) {
        Some(lit) => f(&lit),
        None if kind != Kind::Literal => Verdict::False,
        None => Verdict::Unknown,
    }
}

impl TermPredicate {
    /// Parse a `(kind, arg)` pair with default [`TextOptions`].
    pub fn parse(kind: &str, arg: &str) -> Result<Self> {
        Self::parse_with(kind, arg, &TextOptions::default())
    }

    /// Parse a `(kind, arg)` pair and a string kind's options. String kinds:
    /// `str_prefix` (arg: the raw prefix), `contains`, `strstarts`,
    /// `strends` (arg: the constant's strict N-Triples spelling, a literal,
    /// `<iri>` or `_:blank`; a malformed one is an error), `regex` (arg: the
    /// pattern as written, with the SPARQL `flags` option; a pattern outside
    /// the supported subset, or too large once compiled, parses and leaves
    /// every text undecided). Other kinds: `is_literal`, `is_iri`,
    /// `is_blank` (no argument), `datatype` (an IRI, with or without angle
    /// brackets), `lang` (a tag), `lang_matches` (a BCP 47 language range),
    /// `num_lt` … `num_ne` (an N-Triples numeric literal such as
    /// `"5"^^<http://www.w3.org/2001/XMLSchema#integer>`, or a bare number
    /// typed by its syntax: `5` is an integer, `1.5` a decimal, `1e3` a
    /// double). `case`/`as_str` on a non-string kind and `flags` on any kind
    /// but `regex` are errors. Every refusal is an
    /// [`InvalidInput`](VortexRdfError::InvalidInput) error.
    pub fn parse_with(kind: &str, arg: &str, options: &TextOptions) -> Result<Self> {
        let invalid = |msg: String| VortexRdfError::InvalidInput(msg);
        if !options.flags.is_empty() && kind != "regex" {
            return Err(invalid(format!(
                "flags apply to the regex kind, not {kind:?}"
            )));
        }
        let text = |test: TextTest| {
            TermPredicate::Text(TextPredicate {
                test,
                case: options.case,
                as_str: options.as_str,
            })
        };
        let predicate = match kind {
            "str_prefix" => text(TextTest::Prefix(arg.to_owned())),
            "contains" => text(TextTest::Contains(TextConstant::parse(arg)?)),
            "strstarts" => text(TextTest::StartsWith(TextConstant::parse(arg)?)),
            "strends" => text(TextTest::EndsWith(TextConstant::parse(arg)?)),
            "regex" => text(TextTest::Regex(RegexTest::new(arg, &options.flags))),
            _ if options.case.is_some() || options.as_str => {
                return Err(invalid(format!(
                    "case and as_str apply to the string kinds, not {kind:?}"
                )));
            }
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
            other => {
                let Some(op) = NUM_OPS.into_iter().find(|op| op.kind() == other) else {
                    return Err(invalid(format!(
                        "unknown term predicate kind {other:?}; expected one of is_literal, \
                         is_iri, is_blank, datatype, lang, lang_matches, num_lt, num_le, num_gt, \
                         num_ge, num_eq, num_ne, str_prefix, contains, strstarts, strends, regex"
                    )));
                };
                let number = Number::parse(arg).ok_or_else(|| {
                    invalid(format!(
                        "{other} predicate needs a numeric constant, got {arg:?}"
                    ))
                })?;
                TermPredicate::Num(op, number)
            }
        };
        Ok(predicate)
    }

    /// Evaluate the predicate on one N-Triples spelling.
    pub fn eval(&self, spelling: &str) -> Verdict {
        let kind = kind_of(spelling);
        match kind {
            // Neither the default graph's name nor a foreign spelling is a
            // term the predicates speak of.
            Kind::Other => return Verdict::Unknown,
            Kind::Literal | Kind::Iri | Kind::Blank => {}
        }
        match self {
            TermPredicate::IsLiteral => Verdict::from(kind == Kind::Literal),
            TermPredicate::IsIri => Verdict::from(kind == Kind::Iri),
            TermPredicate::IsBlank => Verdict::from(kind == Kind::Blank),
            // `datatype()`, `lang()` and `langMatches()` of a non-literal are
            // type errors.
            TermPredicate::Datatype(dt) => on_literal(kind, spelling, |lit| {
                Verdict::from(lit.effective_datatype() == dt)
            }),
            TermPredicate::Lang(tag) => on_literal(kind, spelling, |lit| {
                Verdict::from(lit.lang.unwrap_or("") == tag)
            }),
            TermPredicate::LangMatches(range) => on_literal(kind, spelling, |lit| match lit.lang {
                Some(tag) => lang_matches(tag, range),
                // `langMatches("", range)` is false for every range.
                None => Verdict::False,
            }),
            TermPredicate::Num(op, number) => {
                if kind != Kind::Literal {
                    // Comparing a non-literal is a type error; `=` and `!=`
                    // fall back to term (in)equality, which cannot hold.
                    return match op {
                        NumOp::Ne => Verdict::True,
                        _ => Verdict::False,
                    };
                }
                on_literal(kind, spelling, |lit| number.compare(*op, lit))
            }
            TermPredicate::Text(text) => text.eval(spelling, kind),
        }
    }

    /// The verdict every code of `kind` gets without its spelling being
    /// read, or `None` when the spelling decides. Always agrees with `eval`
    /// on a spelling of that kind.
    pub(crate) fn kind_verdict(&self, kind: Kind) -> Option<Verdict> {
        match (self, kind) {
            (_, Kind::Other) => Some(Verdict::Unknown),
            (TermPredicate::IsLiteral, kind) => Some(Verdict::from(kind == Kind::Literal)),
            (TermPredicate::IsIri, kind) => Some(Verdict::from(kind == Kind::Iri)),
            (TermPredicate::IsBlank, kind) => Some(Verdict::from(kind == Kind::Blank)),
            (_, Kind::Literal) => None,
            // Term inequality holds for every non-literal.
            (TermPredicate::Num(NumOp::Ne, _), _) => Some(Verdict::True),
            // `STR()` of an IRI is its string: read it. rdflib's `STR()` of a
            // blank node is its label, which this layer does not decide.
            (TermPredicate::Text(text), Kind::Iri) if text.as_str => None,
            (TermPredicate::Text(text), Kind::Blank) if text.as_str => Some(Verdict::Unknown),
            (
                TermPredicate::Datatype(_)
                | TermPredicate::Lang(_)
                | TermPredicate::LangMatches(_)
                | TermPredicate::Num(..)
                | TermPredicate::Text(_),
                _,
            ) => Some(Verdict::False),
        }
    }

    /// The `(kind, arg)` pair [`Display`](fmt::Display) prints. The kind
    /// and numeric predicates print a pair `parse` accepts back; a string
    /// kind's `arg` also names its options (` [lower]`, ` [upper]`,
    /// ` [str]`) and quotes its constant for reading, not for parsing.
    fn canonical(&self) -> (&'static str, String) {
        match self {
            TermPredicate::IsLiteral => ("is_literal", String::new()),
            TermPredicate::IsIri => ("is_iri", String::new()),
            TermPredicate::IsBlank => ("is_blank", String::new()),
            TermPredicate::Datatype(dt) => ("datatype", dt.clone()),
            TermPredicate::Lang(tag) => ("lang", tag.clone()),
            TermPredicate::LangMatches(range) => ("lang_matches", range.clone()),
            TermPredicate::Num(op, number) => (op.kind(), number.canonical()),
            TermPredicate::Text(text) => text.canonical(),
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

#[cfg(test)]
mod tests;
