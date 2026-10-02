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
//! The predicate's *domain* is the code range a dictionary scan has to cover
//! to find every `True`: the literal range for the literal predicates,
//! everything for the kind tests and `str_prefix` (see
//! [`TermPredicate::domain`]). Codes outside the domain are neither true nor
//! unknown; a caller decides them from the term's kind alone, which the code
//! ranges already tell it.

use std::cmp::Ordering;
use std::fmt;
use std::ops::Range;

use vortex_buffer::Buffer;

use crate::error::{Result, VortexRdfError};

mod literal;
mod numeric;

use self::literal::{Kind, LiteralView, kind_of, lang_matches, unescape_prefix};
pub use self::numeric::Number;

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

/// What a dictionary has to do to partition its codes by a predicate's
/// verdicts: the code range to scan term by term (`None` when the predicate
/// is answered by ranges alone), and the spelling prefixes whose
/// [`prefix_range`] is a run of `True` codes without any scan.
///
/// [`prefix_range`]: super::term_dict::TermDictionary::prefix_range
pub(crate) struct ScanPlan {
    pub(crate) scan: Option<Range<u32>>,
    pub(crate) true_prefixes: Vec<String>,
}

/// The verdicts a scan collected, code by code.
#[derive(Default)]
pub(crate) struct Scanned {
    pub(crate) truth: Vec<u32>,
    pub(crate) unknown: Vec<u32>,
}

impl Scanned {
    /// Record `predicate`'s verdict for the term `spelling` with code `code`.
    #[inline]
    pub(crate) fn visit(&mut self, predicate: &TermPredicate, code: u32, spelling: &str) {
        match predicate.eval(spelling) {
            Verdict::True => self.truth.push(code),
            Verdict::Unknown => self.unknown.push(code),
            Verdict::False => {}
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

impl NumOp {
    pub(super) fn kind(self) -> &'static str {
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
    pub(super) fn apply(self, ord: Ordering) -> Verdict {
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

    pub(super) fn is_equality(self) -> bool {
        matches!(self, NumOp::Eq | NumOp::Ne)
    }
}

/// The part of a dictionary's code space a predicate's definite answers can
/// come from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Domain {
    /// Every code: the kind tests, and `str_prefix` (IRIs and blank nodes
    /// answer through `str()`).
    All,
    /// Only literals can be true; every other kind is false by
    /// construction, or a type error, which a caller decides from the kind.
    Literals,
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

    /// The code range a dictionary scan must cover for this predicate's
    /// definite answers.
    pub fn domain(&self) -> Domain {
        match self {
            TermPredicate::IsLiteral
            | TermPredicate::IsIri
            | TermPredicate::IsBlank
            | TermPredicate::StrPrefix(_) => Domain::All,
            TermPredicate::Datatype(_)
            | TermPredicate::Lang(_)
            | TermPredicate::LangMatches(_)
            | TermPredicate::Num(..) => Domain::Literals,
        }
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
                Kind::Iri => Verdict::from(spelling[1..spelling.len() - 1].starts_with(prefix)),
                Kind::Blank => Verdict::from(spelling[2..].starts_with(prefix)),
                _ => match LiteralView::parse(spelling) {
                    Some(lit) if lit.is_string_like() => {
                        match unescape_prefix(lit.lexical, prefix.len()) {
                            Some(head) => Verdict::from(head.starts_with(prefix)),
                            None => Verdict::Unknown,
                        }
                    }
                    // `str()` of another typed literal is its lexical form
                    // only after the engine's own canonicalization.
                    Some(_) => Verdict::Unknown,
                    None => Verdict::Unknown,
                },
            },
        }
    }

    /// How a dictionary with `kinds` partitions its codes by this
    /// predicate: see [`ScanPlan`].
    pub(crate) fn scan_plan(&self, kinds: &KindRanges) -> ScanPlan {
        match self {
            TermPredicate::IsLiteral | TermPredicate::IsIri | TermPredicate::IsBlank => ScanPlan {
                scan: None,
                true_prefixes: Vec::new(),
            },
            // Literals are scanned; an IRI or blank node answers `str()`
            // with its own spelling, so its prefix test is a code range.
            TermPredicate::StrPrefix(prefix) => ScanPlan {
                scan: Some(kinds.literals.clone()),
                true_prefixes: vec![format!("<{prefix}"), format!("_:{prefix}")],
            },
            TermPredicate::Datatype(_)
            | TermPredicate::Lang(_)
            | TermPredicate::LangMatches(_)
            | TermPredicate::Num(..) => ScanPlan {
                scan: Some(kinds.literals.clone()),
                true_prefixes: Vec::new(),
            },
        }
    }

    /// Assemble the `(true_codes, unknown_codes)` partition from what the
    /// [`ScanPlan`] produced: `scanned` holds the verdicts of the scanned
    /// range and `true_ranges` the code ranges of the plan's `true_prefixes`,
    /// in the plan's order. Both outputs are ascending.
    ///
    /// Codes outside the predicate's [`domain`](Self::domain) appear in
    /// neither list; codes inside it that belong to no kind (the default
    /// graph's `""`, a foreign writer's spelling) are unknown.
    pub(crate) fn assemble(
        &self,
        kinds: &KindRanges,
        scanned: Scanned,
        true_ranges: &[Range<u32>],
    ) -> (Buffer<u32>, Buffer<u32>) {
        let range = |r: &Range<u32>| Buffer::from_iter(r.clone());
        let gaps = || Buffer::from_iter(kinds.gaps());
        match self {
            TermPredicate::IsLiteral => (range(&kinds.literals), gaps()),
            TermPredicate::IsIri => (range(&kinds.iris), gaps()),
            TermPredicate::IsBlank => (range(&kinds.blanks), gaps()),
            TermPredicate::StrPrefix(_) => {
                // Literals < IRIs < blank nodes, so appending the prefix
                // ranges in kind order keeps the list ascending.
                let mut truth = scanned.truth;
                for r in true_ranges {
                    truth.extend(r.clone());
                }
                let mut unknown: Vec<u32> = (0..kinds.literals.start).collect();
                unknown.extend(scanned.unknown);
                unknown.extend(kinds.literals.end..kinds.iris.start);
                unknown.extend(kinds.iris.end..kinds.blanks.start);
                unknown.extend(kinds.blanks.end..kinds.len);
                (Buffer::from(truth), Buffer::from(unknown))
            }
            TermPredicate::Datatype(_)
            | TermPredicate::Lang(_)
            | TermPredicate::LangMatches(_)
            | TermPredicate::Num(..) => {
                (Buffer::from(scanned.truth), Buffer::from(scanned.unknown))
            }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::vocab::RDF_LANG_STRING;

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
    fn domains() {
        assert_eq!(p("is_iri", "").domain(), Domain::All);
        assert_eq!(p("str_prefix", "x").domain(), Domain::All);
        assert_eq!(p("num_lt", "1").domain(), Domain::Literals);
        assert_eq!(p("lang", "en").domain(), Domain::Literals);
    }
}
