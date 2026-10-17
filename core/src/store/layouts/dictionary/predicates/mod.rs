//! Term predicates evaluated on a dictionary's N-Triples spellings — the
//! native half of a query layer's `FILTER` fast path.
//!
//! A [`TermPredicate`] answers a single-variable SPARQL test (`isIRI(?x)`,
//! `datatype(?x) = <dt>`, `lang(?x) = "en"`, `langMatches(lang(?x), "en")`,
//! `?x < 5`, `strstarts(str(?x), "http://…")`, `contains(lcase(?x), "ab")`,
//! `regex(str(?x), "^http")`) for one term, read straight off its spelling,
//! with a three-valued [`Verdict`]. The rules are deliberately
//! **conservative**: a verdict is `True` or `False` only where the SPARQL
//! semantics over the stored spelling are total and cheap to decide;
//! everything else — a lexical form the XSD grammar does not cover, a
//! comparison the value model cannot settle exactly, a datatype the rules do
//! not know — is `Unknown`, for the caller to resolve with a full SPARQL
//! engine. A caller therefore never gets a wrong definite answer, only a
//! slower one.
//!
//! A predicate is evaluated over the *candidate* codes a query produced
//! (`filter_codes`): a code whose kind range decides it
//! ([`TermPredicate::kind_verdict`]) is not read; every other one is read and
//! its spelling parsed — the lexical form unescaped, the language tag and
//! datatype taken from the spelling — before the predicate looks at it.

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

/// The code ranges of a sorted dictionary's term kinds. Codes are
/// lexicographic ranks of the N-Triples spelling, offset by the dictionary's
/// first code, so every kind is one contiguous range, in byte order: the
/// empty spelling of the default graph (the first code, when any quad is in
/// the default graph), then literals (`"`), IRIs (`<`), blank nodes (`_:`).
/// Anything a dictionary of this crate never holds falls in the gaps between
/// those ranges.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KindRanges {
    /// The dictionary's first code: every code is at or above it. 0 in every
    /// dictionary this version builds.
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

    /// The codes in none of the three kind ranges, ascending from `start` —
    /// a foreign writer's spellings, and the default graph's `""`.
    pub fn gaps(&self) -> impl Iterator<Item = TermCode> + '_ {
        (self.start..self.literals.start)
            .chain(self.literals.end..self.iris.start)
            .chain(self.iris.end..self.blanks.start)
            .chain(self.blanks.end..self.len)
    }
}

impl KindRanges {
    /// The kind of `code`'s spelling, read off the ranges.
    pub(crate) fn kind_of_code(&self, code: TermCode) -> CodeKind {
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
    /// `datatype(?x) = <iri>` — a plain literal has `xsd:string`, a
    /// language-tagged one `rdf:langString`, a typed one its datatype; the
    /// lexical form is never inspected.
    Datatype(String),
    /// `lang(?x) = "tag"` — exact comparison against the stored tag (which
    /// the ingest lowercases); an untagged literal has the empty tag.
    Lang(String),
    /// `langMatches(lang(?x), "range")` — BCP 47 basic filtering: `*`
    /// matches any non-empty tag, otherwise a case-insensitive match of the
    /// range to the tag or to a `-`-delimited prefix of it. Any range but
    /// `*` with a character outside ASCII letters, digits and hyphens is
    /// undecided on a tagged literal: rdflib may read it differently.
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
    /// `strends` (arg: the constant's strict N-Triples spelling — a literal,
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
            // The default graph's name is not a term the predicates speak of.
            Kind::DefaultGraph | Kind::Other => return Verdict::Unknown,
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
    pub(crate) fn kind_verdict(&self, kind: CodeKind) -> Option<Verdict> {
        match (self, kind) {
            (_, CodeKind::Other) => Some(Verdict::Unknown),
            (TermPredicate::IsLiteral, kind) => Some(Verdict::from(kind == CodeKind::Literal)),
            (TermPredicate::IsIri, kind) => Some(Verdict::from(kind == CodeKind::Iri)),
            (TermPredicate::IsBlank, kind) => Some(Verdict::from(kind == CodeKind::Blank)),
            (_, CodeKind::Literal) => None,
            // Term inequality holds for every non-literal.
            (TermPredicate::Num(NumOp::Ne, _), _) => Some(Verdict::True),
            // `STR()` of an IRI is its string: read it. rdflib's `STR()` of a
            // blank node is its label, which this layer does not decide.
            (TermPredicate::Text(text), CodeKind::Iri) if text.as_str => None,
            (TermPredicate::Text(text), CodeKind::Blank) if text.as_str => Some(Verdict::Unknown),
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
    pub fn canonical(&self) -> (&'static str, String) {
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
mod tests {
    use super::*;
    use crate::common::vocab::{RDF_LANG_STRING, XSD};

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

    /// rdflib's `langMatches` may read a range differently from basic
    /// filtering: a `*` in any subtag is a wildcard (`en-*` matches `en-gb`),
    /// whitespace around the range is stripped, and its case mapping is
    /// Python's (the Kelvin sign lower-cases to `k`). A tagged literal is left
    /// undecided for every range but `*` that holds a character outside ASCII
    /// letters, digits and hyphens, and for a tag outside that alphabet.
    #[test]
    fn lang_matches_leaves_ranges_outside_the_basic_alphabet_undecided() {
        for range in [
            "en-*",
            "*-gb",
            "*-*",
            "e*",
            " en",
            "en ",
            "\ten",
            "\u{3000}en",
            "\u{a0}en-gb",
            "\u{212a}ab",
            "en_gb",
            "en.*",
        ] {
            let m = p("lang_matches", range);
            assert_eq!(m.eval("\"a\"@en-gb"), Verdict::Unknown, "{range:?}");
            assert_eq!(m.eval("\"a\"@kab"), Verdict::Unknown, "{range:?}");
            // An untagged literal and a non-literal match no range, whatever it is.
            assert_eq!(m.eval("\"a\""), Verdict::False, "{range:?}");
            assert_eq!(m.eval("<http://x>"), Verdict::False, "{range:?}");
        }
        for (range, want) in [
            ("en", true),
            ("en-GB", true),
            ("EN-gb", true),
            ("e", false),
            ("en-g", false),
            ("1996", false),
            ("*", true),
            ("en-", false),
            ("-gb", false),
            ("en--gb", false),
        ] {
            assert_eq!(
                p("lang_matches", range).eval("\"a\"@en-gb"),
                Verdict::from(want),
                "{range:?}"
            );
        }
        // A tag outside the alphabet is only in a foreign writer's file (the
        // N-Triples ingest rejects it), but the literal view carries it:
        // undecided for every range but `*`, which looks at no character of it.
        for spelling in ["\"a\"@en gb", "\"a\"@ en", "\"a\"@en_gb", "\"a\"@é"] {
            for range in ["en", "en-gb", "e"] {
                assert_eq!(
                    p("lang_matches", range).eval(spelling),
                    Verdict::Unknown,
                    "{range:?} on {spelling}"
                );
            }
            assert_eq!(
                p("lang_matches", "*").eval(spelling),
                Verdict::True,
                "{spelling}"
            );
        }
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
        assert_eq!(p("num_ne", "5").eval("_:b"), Verdict::True);
        // The default graph's empty spelling is no term the predicates speak of.
        assert_eq!(p("num_ne", "5").eval(""), Verdict::Unknown);
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
        // Equality and inequality against a non-numeric literal, plain or
        // language-tagged, are left to the engine.
        assert_eq!(p("num_eq", "5").eval("\"abc\""), Verdict::Unknown);
        assert_eq!(p("num_ne", "5").eval("\"abc\""), Verdict::Unknown);
        assert_eq!(p("num_ne", "5").eval("\"abc\"@en"), Verdict::Unknown);
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
        assert_eq!(
            http.eval("<http://ex/a>"),
            Verdict::False,
            "string() rejects an IRI"
        );
        let as_str = TermPredicate::parse_with(
            "str_prefix",
            "http://ex/",
            &TextOptions {
                as_str: true,
                ..TextOptions::default()
            },
        )
        .unwrap();
        assert_eq!(as_str.eval("<http://ex/a>"), Verdict::True);
        assert_eq!(as_str.eval("<http://other/a>"), Verdict::False);
        assert_eq!(p("str_prefix", "b").eval("_:b0"), Verdict::False);
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
        assert_eq!(p("str_prefix", "4").eval(&typed("42", INT)), Verdict::False);
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
    fn string_kinds_parse_and_display_with_their_options() {
        assert_eq!(p("str_prefix", "ab").to_string(), "str_prefix(ab)");
        assert_eq!(
            p("contains", "\"a\\nb\"").to_string(),
            "contains(\"a\\nb\")"
        );
        assert_eq!(
            p("strstarts", "\"b\"@en").to_string(),
            "strstarts(\"b\"@en)"
        );
        assert_eq!(
            p("strends", "<http://x>").to_string(),
            "strends(<not a string>)",
            "an IRI constant is no string"
        );
        let both = TextOptions {
            case: Some(CaseMap::Upper),
            as_str: true,
            ..TextOptions::default()
        };
        assert_eq!(
            TermPredicate::parse_with("contains", "\"b\"", &both)
                .unwrap()
                .to_string(),
            "contains(\"b\" [upper] [str])"
        );
        // A typed `xsd:string` constant is the plain one; the options and
        // the language tag tell predicates apart.
        let xsd_string = format!("\"a\"^^<{XSD}string>");
        assert_eq!(p("contains", &xsd_string), p("contains", "\"a\""));
        assert_ne!(p("contains", "\"a\"@en"), p("contains", "\"a\""));
        assert_ne!(
            TermPredicate::parse_with("contains", "\"a\"", &both).unwrap(),
            p("contains", "\"a\"")
        );
        // `regex` shows its pattern and flags, and the options after them.
        assert_eq!(p("regex", "^a").to_string(), "regex(\"^a\" flags=\"\")");
        let flagged = TextOptions {
            flags: "i".into(),
            case: Some(CaseMap::Lower),
            as_str: true,
        };
        assert_eq!(
            TermPredicate::parse_with("regex", "^a", &flagged)
                .unwrap()
                .to_string(),
            "regex(\"^a\" flags=\"i\" [lower] [str])"
        );
        assert_eq!(p("regex", "a"), p("regex", "a"));
        assert_ne!(p("regex", "a"), p("regex", "b"));
        assert_ne!(
            p("regex", "a"),
            TermPredicate::parse_with(
                "regex",
                "a",
                &TextOptions {
                    flags: "i".into(),
                    ..TextOptions::default()
                }
            )
            .unwrap()
        );
        // The pattern is raw text, whatever it holds: nothing here refuses it.
        for pattern in ["", "(", "\"a\"^^xsd:string", "[z-a]", "\\"] {
            assert!(
                TermPredicate::parse("regex", pattern).is_ok(),
                "{pattern:?}"
            );
        }
        // A bad escape in a constant is refused; one in a prefix is just text.
        assert!(TermPredicate::parse("contains", "\"a\\q\"").is_err());
        assert!(TermPredicate::parse("str_prefix", "a\\q").is_ok());
        // The message of an unknown kind names the string kinds.
        let message = TermPredicate::parse("nope", "").unwrap_err().to_string();
        assert!(
            message.contains("contains, strstarts, strends, regex"),
            "{message}"
        );
    }

    /// A string constant is a strict N-Triples spelling: a malformed one is
    /// refused at parse time — never accepted only to fail every candidate —
    /// and a well-formed one that is no string literal matches nothing.
    #[test]
    fn string_constants_are_strict_n_triples() {
        let xsd_string = format!("{XSD}string");
        let mut wrong = Vec::new();
        for kind in ["contains", "strstarts", "strends"] {
            for malformed in [
                // A datatype that is no `<iri>`: a prefixed name, an unclosed
                // bracket, nothing at all, a stray bracket.
                "\"a\"^^xsd:string",
                "\"a\"^^<http://www.w3.org/2001/XMLSchema#string",
                "\"a\"^^",
                "\"a\"^^<x>>",
                "\"a\"^^<<x>",
                "\"a\"^^<>>",
                // An IRI that is not `<...>`, and anything that is no term.
                "<http://ex/a",
                "http://ex/a>",
                "<<http://ex/a>>",
                "<a>b>",
                "http://ex/a",
                "a",
                "",
                // A language tag outside `[A-Za-z]+(-[A-Za-z0-9]+)*`.
                "\"a\"@e n",
                "\"a\"@en^^<x>",
                "\"a\"@",
                "\"a\"@-en",
                "\"a\"@en-",
                "\"a\"@en--gb",
                "\"a\"@1en",
                "\"a\"@en_gb",
                "\"a\"@é",
                // A blank node without a valid label.
                "_:",
                "_:a b",
                // Text after the literal, an unterminated literal, a bad
                // escape (in a string constant or in one that is no string).
                "\"a\" ",
                "\"a",
                "\"a\\q\"",
                "\"a\\q\"^^<http://ex/dt>",
            ] {
                if !matches!(
                    TermPredicate::parse(kind, malformed),
                    Err(VortexRdfError::InvalidInput(_))
                ) {
                    wrong.push(format!("{kind} {malformed:?} was not refused"));
                }
            }
            for well_formed in [
                "\"a\"",
                "\"\"",
                "\"a\"@en",
                "\"a\"@en-GB",
                "\"a\"@en-x-1",
                "\"a\"@EN",
                "\"a\\\"@fr\"@en",
                &format!("\"a\"^^<{xsd_string}>"),
                // No string literal: well formed, so it matches nothing.
                &format!("\"4\"^^<{INT}>"),
                "\"a\"^^<http://ex/dt>",
                "<http://ex/a>",
                "<>",
                "_:b0",
                "_:b-0.1",
            ] {
                if let Err(e) = TermPredicate::parse(kind, well_formed) {
                    wrong.push(format!("{kind} {well_formed:?} was refused: {e}"));
                }
            }
        }
        assert!(wrong.is_empty(), "{wrong:#?}");
        // A prefix is raw text, not a spelling.
        assert!(TermPredicate::parse("str_prefix", "\"a\"^^xsd:string").is_ok());
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
            start: 0,
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
            ("contains", "\"h\""),
            ("strstarts", "\"h\""),
            ("strends", "\"h\""),
            ("regex", "h"),
            ("regex", "(?=h)"),
        ] {
            let predicate = p(kind, arg);
            for (code, spelling) in spellings.iter().enumerate() {
                if let Some(verdict) = predicate.kind_verdict(kinds.kind_of_code(code as TermCode))
                {
                    assert_eq!(verdict, predicate.eval(spelling), "{kind} on {spelling}");
                }
            }
        }
    }

    /// The kind a code of this range would be given: what the first byte of
    /// its spelling says.
    fn code_kind(spelling: &str) -> CodeKind {
        match kind_of(spelling) {
            Kind::Literal => CodeKind::Literal,
            Kind::Iri => CodeKind::Iri,
            Kind::Blank => CodeKind::Blank,
            Kind::DefaultGraph | Kind::Other => CodeKind::Other,
        }
    }

    /// Every spelling up to four characters over the characters that carry a
    /// term's structure — quotes, brackets, escapes, `@`, `^`, `_`, `:` and a
    /// multi-byte letter — which a foreign writer's dictionary may hold in
    /// any kind's range: no predicate panics on one, a string kind never
    /// passes one that is no term, and a verdict the kind alone gives is the
    /// one `eval` gives.
    #[test]
    fn malformed_spellings_never_panic() {
        let alphabet = ['"', '<', '>', '_', ':', '\\', '@', '^', 'é', 'a'];
        let mut spellings = vec![String::new()];
        let mut layer = vec![String::new()];
        for _ in 0..4 {
            layer = layer
                .iter()
                .flat_map(|s| alphabet.iter().map(move |c| format!("{s}{c}")))
                .collect();
            spellings.extend(layer.iter().cloned());
        }
        assert_eq!(spellings.len(), 11_111);

        let string_kinds = [
            ("str_prefix", "a"),
            ("str_prefix", ""),
            ("contains", "\"a\""),
            ("contains", "\"\""),
            ("contains", "\"é\"@en"),
            ("strstarts", "\"a\""),
            (
                "strends",
                "\"a\"^^<http://www.w3.org/2001/XMLSchema#string>",
            ),
            ("strends", "<http://x>"),
            ("regex", "a"),
            ("regex", "^$"),
            ("regex", r"\w$"),
            ("regex", "(?=a)"),
        ];
        let options = [
            TextOptions::default(),
            TextOptions {
                as_str: true,
                ..TextOptions::default()
            },
            TextOptions {
                case: Some(CaseMap::Lower),
                ..TextOptions::default()
            },
            TextOptions {
                as_str: true,
                case: Some(CaseMap::Upper),
                ..TextOptions::default()
            },
        ];
        let mut predicates: Vec<TermPredicate> = string_kinds
            .iter()
            .flat_map(|&(kind, arg)| {
                options
                    .iter()
                    .map(move |o| TermPredicate::parse_with(kind, arg, o).unwrap())
            })
            .collect();
        predicates.extend(
            [
                ("is_literal", ""),
                ("is_iri", ""),
                ("is_blank", ""),
                ("datatype", INT),
                ("lang", "en"),
                ("lang", ""),
                ("lang_matches", "*"),
                ("lang_matches", "en"),
                ("num_lt", "5"),
                ("num_eq", "5"),
                ("num_ne", "5"),
            ]
            .map(|(kind, arg)| p(kind, arg)),
        );
        for predicate in &predicates {
            for spelling in &spellings {
                let verdict = predicate.eval(spelling);
                if let Some(by_kind) = predicate.kind_verdict(code_kind(spelling)) {
                    assert_eq!(by_kind, verdict, "{predicate} on {spelling:?}");
                }
                if matches!(predicate, TermPredicate::Text(_))
                    && !spelling.starts_with('"')
                    && verdict == Verdict::True
                {
                    // Beside literals, only an IRI under `STR()` has a text:
                    // whatever else passes must be spelled `<…>`.
                    assert!(
                        spelling.starts_with('<') && spelling.ends_with('>'),
                        "{predicate} passed {spelling:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn as_str_kind_verdicts_agree_with_eval() {
        let kinds = KindRanges {
            start: 0,
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
        for options in [
            TextOptions {
                as_str: true,
                ..TextOptions::default()
            },
            TextOptions {
                as_str: true,
                case: Some(CaseMap::Lower),
                ..TextOptions::default()
            },
        ] {
            for (kind, arg) in [
                ("str_prefix", "b"),
                ("contains", "\"b\""),
                ("strstarts", "\"b\""),
                ("strends", "\"b\""),
                ("regex", "b"),
                ("regex", "(?=b)"),
            ] {
                let predicate = TermPredicate::parse_with(kind, arg, &options).unwrap();
                for (code, spelling) in spellings.iter().enumerate() {
                    if let Some(verdict) =
                        predicate.kind_verdict(kinds.kind_of_code(code as TermCode))
                    {
                        assert_eq!(
                            verdict,
                            predicate.eval(spelling),
                            "{kind} {options:?} on {spelling}"
                        );
                    }
                }
            }
        }
    }
}
