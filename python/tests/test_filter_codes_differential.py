r"""`TermDict.filter_codes` checked against rdflib's own evaluator.

Every kind's verdicts are compared code by code with what rdflib 7.6's SPARQL
operators answer for the same term, held the way an rdflib store holds it:
each stored spelling is parsed by rdflib's N-Triples parser into a `Graph`
(one document per corpus, one line per code), and so is every term-spelled
constant (`contains`, `strstarts`, `strends`, the numeric kinds). rdflib's N3
shorthand reader is not used: it rewrites a hex escape that follows an
escaped backslash (`"C:\\x41"`), which the N-Triples parser and the native
layer both read as written. A passed code must be true for rdflib, a failed
one false; an undecided one is the caller's. Regex patterns drawn at random
from the native allow-list grammar are compared with Python's `re.search` (the
call rdflib makes), and the native layer must decide every text bar the
documented gaps listed in `deliberate_gap`. The corpus covers language tags,
`xsd:string`, other datatypes, IRIs, blank nodes, N-Triples escapes, trailing
newlines, non-ASCII text and control characters; a table of 64-bit integers
around the edges of `xsd:long` and `xsd:unsignedLong` checks the numeric
kinds beyond f64's exact range. Set `VORTEX_RDF_BSBM10K_STORE` to a BSBM 10K
Dictionary store to also compare its literals.

`STR()` of a blank node is rdflib's label, and `STR()` of a literal whose
datatype rdflib normalizes (the keys of `rdflib.term.XSDToPython`, plus
`rdf:HTML`) is its canonical form rather than the stored lexical form: the
native layer leaves both undecided, so the `as_str` comparisons assert exactly
that instead of comparing them. `test_normalized_datatypes_are_undecided_under_str`
ties the native list to rdflib's table, one literal per datatype.
"""

import os
import random
import re
from collections import Counter
from decimal import InvalidOperation
from functools import cache
from pathlib import Path
from types import SimpleNamespace as NS
from typing import Any, NamedTuple

import pytest
from rdflib import BNode, Graph, Literal, URIRef
from rdflib.plugins.sparql import operators as op
from rdflib.plugins.sparql.sparql import SPARQLError
from rdflib.term import XSDToPython

from vortex_rdf import U64Column, VortexRdfStore, serialize_rdf

XSD = "http://www.w3.org/2001/XMLSchema#"
RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
TEXT_KINDS = {"str_prefix", "contains", "strstarts", "strends", "regex"}
NUM_OPS = {"num_lt": "<", "num_le": "<=", "num_gt": ">", "num_ge": ">=", "num_eq": "=", "num_ne": "!="}
STR_FNS = {
    "str_prefix": op.Builtin_STRSTARTS,
    "strstarts": op.Builtin_STRSTARTS,
    "strends": op.Builtin_STRENDS,
    "contains": op.Builtin_CONTAINS,
}
RE_FLAGS = {"i": re.IGNORECASE, "s": re.DOTALL, "m": re.MULTILINE}

#: The datatypes whose literals rdflib parses into a Python value and writes
#: back in canonical form, so its `STR()` of one can differ from the stored
#: lexical form (`STR("01"^^xsd:integer)` is `"1"`): the keys of rdflib's own
#: table, minus `xsd:string`, plus the two `rdf:` datatypes the native layer
#: names (`rdf:XMLLiteral` is in the table already; `rdf:HTML` joins it when
#: rdflib's optional `html5rdf` is installed, and is undecided either way).
NORMALIZED = {str(dt) for dt in XSDToPython if dt is not None} - {XSD + "string"}
NORMALIZED |= {RDF + "XMLLiteral", RDF + "HTML"}


# --- Reading spellings the way rdflib holds them -----------------------------


def parse_terms(spellings: list[str]) -> list[Any]:
    """The rdflib term of every N-Triples spelling, as rdflib's own parser
    reads it: one document with a line `<urn:s:POSITION> <urn:p> SPELLING .`
    per spelling, parsed into a `Graph` once and mapped back through the
    subject. The empty default-graph spelling has no term (`None`)."""
    lines = [f"<urn:s:{i}> <urn:p> {spelling} ." for i, spelling in enumerate(spellings) if spelling]
    graph = Graph()
    graph.parse(data="\n".join(lines) + "\n", format="nt")
    terms: list[Any] = [None] * len(spellings)
    for subject, _, obj in graph:
        terms[int(subject[len("urn:s:") :])] = obj
    unread = [s for s, term in zip(spellings, terms, strict=True) if s and term is None]
    assert not unread, f"rdflib's parser read no term for {unread[:5]}"
    return terms


@cache
def constant(spelling: str) -> Any:
    """The rdflib term of a term-spelled constant, parsed like a stored spelling."""
    return parse_terms([spelling])[0]


class Built(NamedTuple):
    """A serialized corpus: its file, and its terms decoded once."""

    path: Path
    spellings: list[str]
    terms: list[Any]


def build(tmp_path_factory, name: str, objects: list[str]) -> Built:
    """Serialize one triple per N-Triples object spelling into a Dictionary
    store, and decode its terms."""
    directory = tmp_path_factory.mktemp(name)
    nt = directory / f"{name}.nt"
    nt.write_text("".join(f"<urn:s:{i}> <urn:p> {obj} .\n" for i, obj in enumerate(objects)), encoding="utf-8")
    out = directory / f"{name}.vortex"
    serialize_rdf(nt, out, format="ntriples", layout="dictionary")
    term_dict = VortexRdfStore(out, in_memory=True).term_dict()
    spellings = term_dict.decode_many(list(range(len(term_dict))))
    return Built(out, spellings, parse_terms(spellings))


class Corpus(NamedTuple):
    """Candidate `codes` with their spellings and rdflib terms, by position."""

    store: Any  # keeps the dictionary's mapping or memory alive
    term_dict: Any
    codes: list[int]
    spellings: list[str]
    terms: list[Any]  # None for the default graph's empty spelling


def open_corpus(built: Built, residency: str) -> Corpus:
    store = VortexRdfStore(built.path, in_memory=residency == "in-memory")
    term_dict = store.term_dict()
    assert term_dict.file_backed == (residency == "file")
    codes = list(range(len(term_dict)))
    assert term_dict.decode_many(codes) == built.spellings
    return Corpus(store, term_dict, codes, built.spellings, built.terms)


# --- The oracle ---------------------------------------------------------------


def oracle(kind, arg, term, flags="", case=None, as_str=False) -> bool | None:
    """rdflib 7.6's FILTER verdict on `term`; an evaluation error is false.
    None when rdflib itself raises something that is no evaluation error: it
    orders a NaN double against a decimal with `decimal.InvalidOperation`,
    which no FILTER catches, so there is no verdict to agree with."""
    try:
        if kind in TEXT_KINDS:
            text = term
            if as_str:
                text = op.Builtin_STR(NS(arg=text), None)
            if case == "lower":
                text = op.Builtin_LCASE(NS(arg=text), None)
            elif case == "upper":
                text = op.Builtin_UCASE(NS(arg=text), None)
            if kind == "regex":
                result = op.Builtin_REGEX(
                    NS(text=text, pattern=Literal(arg), flags=Literal(flags) if flags else None), None
                )
            else:
                const = Literal(arg) if kind == "str_prefix" else constant(arg)
                result = STR_FNS[kind](NS(arg1=text, arg2=const), None)
        elif kind in NUM_OPS:
            result = op.RelationalExpression(NS(expr=term, op=NUM_OPS[kind], other=constant(arg)), None)
        elif kind == "lang":
            lang = op.Builtin_LANG(NS(arg=term), None)
            result = op.RelationalExpression(NS(expr=lang, op="=", other=Literal(arg)), None)
        elif kind == "lang_matches":
            lang = op.Builtin_LANG(NS(arg=term), None)
            result = op.Builtin_LANGMATCHES(NS(arg1=lang, arg2=Literal(arg)), None)
        elif kind == "datatype":
            datatype = op.Builtin_DATATYPE(NS(arg=term), None)
            result = op.RelationalExpression(NS(expr=datatype, op="=", other=URIRef(arg)), None)
        else:
            fn = {"is_literal": op.Builtin_isLITERAL, "is_iri": op.Builtin_isIRI, "is_blank": op.Builtin_isBLANK}[kind]
            result = fn(NS(arg=term), None)
        return bool(op.EBV(result))
    except SPARQLError:
        return False
    except InvalidOperation:
        return None


def str_undecided(term) -> bool:
    """Whether `STR(term)` is one the native layer never decides: a blank node
    (rdflib's label is its parser's own) or a literal of a normalized datatype."""
    if isinstance(term, BNode):
        return True
    return isinstance(term, Literal) and term.datatype is not None and str(term.datatype) in NORMALIZED


def compare(corpus: Corpus, kind, arg, *, flags="", case=None, as_str=False):
    """Native vs rdflib for every candidate; returns the native `(passed,
    undecided)` as sets of codes. Every disagreement is reported at once."""
    passed, undecided = corpus.term_dict.filter_codes(
        kind, arg, U64Column(corpus.codes), flags=flags, case=case, as_str=as_str
    )
    passed = set(memoryview(passed).cast("Q"))
    undecided = set(memoryview(undecided).cast("Q"))
    where = f"{kind} {arg!r} flags={flags!r} case={case} as_str={as_str}"
    problems = []
    if passed & undecided:
        problems.append(f"{where}: codes {sorted(passed & undecided)[:5]} are both passed and undecided")
    if not (passed | undecided) <= set(corpus.codes):
        problems.append(f"{where}: codes outside the candidates were answered")
    for code, spelling, term in zip(corpus.codes, corpus.spellings, corpus.terms, strict=True):
        if term is None:
            if code not in undecided:
                problems.append(f"{where}: the default graph's code {code} must be undecided")
        elif as_str and kind in TEXT_KINDS and str_undecided(term):
            if code not in undecided:
                verdict = "passed" if code in passed else "failed"
                problems.append(f"{where}: native {verdict} STR() of {spelling}, which is undecided by contract")
        else:
            want = oracle(kind, arg, term, flags, case, as_str)
            if want is None:
                if code not in undecided:
                    problems.append(f"{where}: native decided {spelling}, where rdflib raises")
            elif code in passed and not want:
                problems.append(f"{where}: native passed {spelling}, rdflib says false")
            elif code not in passed and code not in undecided and want:
                problems.append(f"{where}: native failed {spelling}, rdflib says true")
    assert not problems, f"{len(problems)} disagreements:\n" + "\n".join(problems[:20])
    return passed, undecided


# --- The corpus -----------------------------------------------------------------


def nt_escape(text: str) -> str:
    out = []
    for ch in text:
        if ch == "\\":
            out.append("\\\\")
        elif ch == '"':
            out.append('\\"')
        elif ch == "\n":
            out.append("\\n")
        elif ch == "\r":
            out.append("\\r")
        elif ch == "\t":
            out.append("\\t")
        elif ord(ch) < 0x20 or ch == "\x7f":
            out.append(f"\\u{ord(ch):04X}")
        else:
            out.append(ch)
    return "".join(out)


def literal(text: str, lang: str | None = None, datatype: str | None = None) -> str:
    body = f'"{nt_escape(text)}"'
    if lang:
        return f"{body}@{lang}"
    if datatype:
        return f"{body}^^<{datatype}>"
    return body


TEXTS = [
    "", "a", "ab", "Ab", "abc", "ABC", "b", "xaby", "a.b", "a/b", "a|b", "a$", "1.5", "42",
    "a b", "a\tb", "a\nb", "ab\n", "x\n\n", "a\x1cb", "\x1d", "café", "CAFÉ", "naïve", "ſ",
    "\u212a", "日本", 'say "hi"', "back\\slash", "a\r", "a&&b", "-", "[x]", "{1}",
    "http://ex.org/a", "word foo bar", "foo", "AFOOB",
    # A hex escape after an escaped backslash: the N-Triples spellings
    # "C:\\x41" and "a\\x7e b" hold a backslash, an x and hex digits, not the
    # "A" and "~" an N3 shorthand reader makes of them.
    "C:\\x41", "a\\x7e b",
]
DATATYPED = [
    ("42", "integer"), ("01", "integer"), ("+7", "integer"), (" 7", "integer"), ("abc", "integer"),
    ("1.5", "decimal"), ("1.50", "decimal"), ("1e2", "double"), ("NaN", "double"),
    ("true", "boolean"), ("1", "boolean"), ("2008-05-03T00:00:00", "dateTime"), ("300", "byte"),
    ("99999999999999999999", "long"), ("-1", "unsignedLong"), ("5", "long"), ("ab", "string"),
    ("0001", "gYear"),
]


def corpus_terms() -> list[str]:
    terms = [literal(t) for t in TEXTS]
    terms += [literal(t, lang=lang) for t in TEXTS[:12] for lang in ("en", "en-gb", "fr")]
    terms += [literal(t, datatype=XSD + dt) for t, dt in DATATYPED]
    terms += [literal("ab", datatype="http://ex.org/dt"), literal("a\nb", datatype="http://ex.org/dt")]
    return terms + [
        "<http://ex.org/a>", "<http://ex.org/ab>", "<http://ex.org/b/c>", "<urn:x:caf\u00e9>",
        "_:b0", "_:ab",
    ]


@pytest.fixture(scope="session")
def built_corpus(tmp_path_factory):
    return build(tmp_path_factory, "corpus", corpus_terms())


@pytest.fixture(scope="module", params=["file", "in-memory"])
def corpus(request, built_corpus):
    return open_corpus(built_corpus, request.param)


def test_oracle_reads_the_stored_texts_as_written(corpus):
    """The oracle's input is right before anything is compared with it: the
    plain and tagged literals read back as exactly the texts that were written,
    N-Triples escapes, controls and backslash-x sequences included."""
    texts = {str(t) for t in corpus.terms if isinstance(t, Literal) and (t.language or t.datatype is None)}
    assert texts == set(TEXTS)
    by_spelling = dict(zip(corpus.spellings, corpus.terms, strict=True))
    assert len(by_spelling[literal("C:\\x41")]) == 6  # C : \ x 4 1
    assert str(by_spelling[literal("a\\x7e b")]) == "a\\x7e b"
    assert corpus.terms[0] is None and corpus.spellings[0] == ""


# --- String kinds -----------------------------------------------------------------

STRING_CASES = [
    ("str_prefix", "a"), ("str_prefix", "A"), ("str_prefix", ""), ("str_prefix", "http://ex.org/"),
    ("str_prefix", "C:\\x"), ("str_prefix", "a\\x7e"),
    ("contains", '"b"'), ("contains", '"b"@en'), ("contains", '"b"@EN'),
    ("contains", f'"b"^^<{XSD}string>'), ("contains", '"\\n"'), ("contains", '"\u00e9"'),
    ("contains", f'"5"^^<{XSD}integer>'), ("contains", "<http://ex.org/a>"), ("contains", '"0"'),
    ("contains", r'"x41"'), ("contains", r'"\\x"'), ("contains", r'"C:\\x41"'), ("contains", r'"\\"'),
    # The same characters spelled with other N-Triples escapes.
    ("contains", r'"caf\u00E9"'), ("contains", r'"CAF\U000000c9"'), ("strends", r'"b\t"'), ("contains", r'"\""'),
    ("strstarts", '"a"'), ("strstarts", '"A"@en'), ("strstarts", '"http"'), ("strstarts", r'"a\\x7e"'),
    ("strends", '"b"'), ("strends", '"\\n"'), ("strends", '"b"@fr'), ("strends", r'"x41"'),
    ("strends", r'"7e b"'),
]


def string_kind_is_decided(term, case, as_str) -> bool:
    """Whether the pinned contract obliges the native layer to decide a
    non-regex string kind for `term`. Everything is decided but the default
    graph, a blank node or a normalized literal under `STR()`, and a non-ASCII
    text under a case wrapper. Without `as_str` only a plain, tagged or
    `xsd:string` literal has a text; every other term fails, whatever the case."""
    if term is None:
        return False
    if isinstance(term, BNode):
        return not as_str
    if as_str:
        if str_undecided(term):
            return False
    elif not (isinstance(term, Literal) and (term.datatype is None or str(term.datatype) == XSD + "string")):
        return True
    return case is None or str(term).isascii()


@pytest.mark.parametrize("kind, arg", STRING_CASES)
@pytest.mark.parametrize("case", [None, "lower", "upper"])
@pytest.mark.parametrize("as_str", [False, True])
def test_string_kinds_agree_with_rdflib(corpus, kind, arg, case, as_str):
    _, undecided = compare(corpus, kind, arg, case=case, as_str=as_str)
    wrongly_undecided = [
        spelling
        for code, spelling, term in zip(corpus.codes, corpus.spellings, corpus.terms, strict=True)
        if code in undecided and string_kind_is_decided(term, case, as_str)
    ]
    assert not wrongly_undecided, f"{kind} {arg!r} case={case} as_str={as_str}: undecided {wrongly_undecided[:5]}"


# --- Kinds that read the term's shape ---------------------------------------------

#: Kinds that decide every term but the default graph's empty spelling.
TOTAL_CASES = [
    ("is_literal", ""), ("is_iri", ""), ("is_blank", ""),
    ("datatype", XSD + "integer"), ("datatype", XSD + "string"),
    ("datatype", RDF + "langString"), ("datatype", XSD + "gYear"), ("datatype", "http://ex.org/dt"),
    ("lang", "en"), ("lang", ""), ("lang", "en-gb"), ("lang", "EN"),
    ("lang_matches", "EN"), ("lang_matches", "*"), ("lang_matches", "en-gb"), ("lang_matches", "fr"),
    ("lang_matches", "EN-GB"), ("lang_matches", "e"), ("lang_matches", "en-"), ("lang_matches", "-gb"),
    ("lang_matches", "en--gb"), ("lang_matches", "en-gb-x"),
]
#: Integer-valued literals in range, which compare exactly with every numeric
#: constant below (whatever its type) and so are always decided.
ALWAYS_DECIDED = [
    f'"{lex}"^^<{XSD}{dt}>'
    for lex, dt in [("42", "integer"), ("01", "integer"), ("+7", "integer"), ("5", "long"), ("1e2", "double")]
]
#: Numeric constants, each run under all six operators.
NUMERIC_CONSTANTS = [
    f'"5"^^<{XSD}integer>', f'"42"^^<{XSD}integer>', f'"-0"^^<{XSD}integer>', f'"100"^^<{XSD}integer>',
    f'"1.5"^^<{XSD}decimal>', f'"1.50"^^<{XSD}decimal>', f'"4.5"^^<{XSD}decimal>',
    f'"1e2"^^<{XSD}double>', f'"2.5"^^<{XSD}double>', f'"5"^^<{XSD}long>', f'"1"^^<{XSD}unsignedLong>',
    f'"7"^^<{XSD}byte>',
]


@pytest.mark.parametrize("kind, arg", TOTAL_CASES)
def test_total_kinds_agree_with_rdflib(corpus, kind, arg):
    _, undecided = compare(corpus, kind, arg)
    assert undecided == {0}, f"{kind} {arg!r}: undecided {sorted(undecided)[:5]}"


#: Language ranges with a character outside ASCII letters, digits and hyphens
#: (and not `*` alone). rdflib's `_lang_range_check` reads a `*` in any subtag
#: as a wildcard (`en-*` matches `en-gb`), strips the whitespace around a range
#: and lower-cases it the Unicode way (a Kelvin sign is `k`); basic filtering
#: does none of that. The native layer leaves a tagged literal undecided for
#: every such range, also where rdflib happens to agree (`e*`, `en_gb`),
#: instead of failing what rdflib passes.
NON_BASIC_RANGES = [
    "en-*", "*-gb", "*-*", "e*", " en", "en ", "\ten", "\u3000en", "\u00a0en-gb", "\u212aab", "en_gb", "en.*",
]


@pytest.mark.parametrize("range_", NON_BASIC_RANGES)
def test_lang_matches_ranges_outside_the_basic_alphabet_are_undecided(corpus, range_):
    _, undecided = compare(corpus, "lang_matches", range_)
    tagged = {
        code
        for code, term in zip(corpus.codes, corpus.terms, strict=True)
        if isinstance(term, Literal) and term.language
    }
    assert tagged and undecided == tagged | {0}, f"lang_matches {range_!r}: undecided {sorted(undecided)}"


@pytest.mark.parametrize("kind", NUM_OPS)
@pytest.mark.parametrize("arg", NUMERIC_CONSTANTS)
def test_numeric_kinds_agree_with_rdflib(corpus, kind, arg):
    _, undecided = compare(corpus, kind, arg)
    for spelling in ALWAYS_DECIDED:  # so the comparison above is not vacuous
        assert corpus.spellings.index(spelling) not in undecided, f"{kind} {arg!r}: undecided on {spelling}"


# --- Regex over the corpus ----------------------------------------------------------

REGEX_CASES = [
    ("b$", ""), ("b$", "m"), (r"\s", ""), (r"^\S+$", ""), ("AB", "i"), ("\u00e9", "i"),
    ("a.b", "s"), ("^a", ""), (r"\bfoo\b", ""), (r"[^a-z]", ""), ("(?=a)", ""), ("", ""),
    (r"\\x41", ""), (r"\\x7e b", ""), (r"x[0-9a-f]{2}", ""), (r"[\\/]", ""),
]
#: An unbounded repeat inside an unbounded repeat, which the random generator
#: does not draw (Python's `re` backtracks exponentially on overlapping ones).
#: Each body here has one way to match, so `re` stays fast.
NESTED_REPEATS = ["(a*b)*$", r"(?:\w+ )+\w", "(?:ab+)+c", "^(?:[a-c]+-)*x"]
REGEX_CASES += [(pattern, "") for pattern in NESTED_REPEATS]


@pytest.mark.parametrize("case", [None, "lower", "upper"])
@pytest.mark.parametrize("as_str", [False, True])
def test_regex_cases_agree_with_rdflib(corpus, case, as_str):
    abc = corpus.spellings.index('"abc"')
    for pattern, flags in REGEX_CASES:
        _, undecided = compare(corpus, "regex", pattern, flags=flags, case=case, as_str=as_str)
        if pattern in NESTED_REPEATS:  # inside the subset, so a plain ASCII text is decided
            assert abc not in undecided, f"{pattern!r}: undecided on a plain ASCII text"


#: Quantifiers that repeat without bound.
LOOPS = {"*", "+", "{1,}"}
#: The most unbounded repeats one random pattern holds.
MAX_LOOPS = 3


class PatternGenerator:
    """Random patterns from the native allow-list grammar. `uses` collects the
    constructs a pattern holds that leave some texts undecided on purpose: the
    `$` end anchor, `\\b` and `\\B`, and the class shorthands `\\d \\w \\s \\D
    \\W \\S` (as `"shorthand"`, inside a class or outside). Unbounded repeats never
    nest, and a pattern holds at most `MAX_LOOPS` of them: Python's
    backtracking is exponential in the text on a shape like `((x|.*?){1,})*?`,
    which the grammar allows and the native layer decides in microseconds (one
    seed of this generator produced a pattern that ran for minutes)."""

    def __init__(self, rng: random.Random) -> None:
        self.rng = rng
        self.uses: set[str] = set()
        self.loops = 0

    def shorthand(self) -> str:
        self.uses.add("shorthand")
        return self.rng.choice([r"\d", r"\w", r"\s", r"\D", r"\W", r"\S"])

    def klass(self) -> str:
        rng = self.rng
        items = []
        for _ in range(rng.randint(1, 3)):
            r = rng.random()
            if r < 0.3:
                items.append(rng.choice(["a-c", "0-9", "A-Z", "x-z"]))
            elif r < 0.5:
                items.append(self.shorthand())
            elif r < 0.6:
                items.append("\\" + rng.choice(".-]\\^["))
            else:
                items.append(rng.choice("abA\u00e9 .:"))
        return "[" + ("^" if rng.random() < 0.3 else "") + "".join(items) + "]"

    def atom(self, depth: int, may_loop: bool) -> str:
        rng = self.rng
        r = rng.random()
        if depth < 2 and r < 0.12:
            return "(" + ("?:" if rng.random() < 0.5 else "") + self.pattern(depth + 1, may_loop) + ")"
        if r < 0.24:
            return self.klass()
        if r < 0.32:
            return self.shorthand()
        if r < 0.40:
            return "."
        if r < 0.48:
            return "\\" + rng.choice(".*+?()[]{}|^$\\/-")
        return rng.choice(list("abAB x\u00e91-/:") + ["\n"])

    def quantifier(self, may_loop: bool) -> tuple[str, bool]:
        """A quantifier (possibly none) and whether it repeats without bound."""
        rng = self.rng
        if rng.random() < 0.6:
            return "", False
        q = rng.choice(["*", "+", "?", "{2}", "{1,}", "{0,2}"])
        loop = q in LOOPS
        if loop and (not may_loop or self.loops >= MAX_LOOPS):
            q, loop = rng.choice(["?", "{2}", "{0,2}"]), False
        self.loops += loop
        return q + ("?" if rng.random() < 0.3 else ""), loop

    def sequence(self, depth: int, may_loop: bool) -> str:
        rng = self.rng
        parts = [rng.choice(["^", r"\A"])] if rng.random() < 0.15 else []
        for _ in range(rng.randint(1, 4)):
            quantifier, loop = self.quantifier(may_loop)
            parts.append(self.atom(depth, may_loop and not loop) + quantifier)
            if rng.random() < 0.08:
                boundary = rng.choice([r"\b", r"\B"])
                self.uses.add(boundary)
                parts.append(boundary)
        if rng.random() < 0.15:
            self.uses.add("$")
            parts.append("$")
        return "".join(parts)

    def pattern(self, depth: int = 0, may_loop: bool = True) -> str:
        count = 1 if self.rng.random() < 0.8 else 2
        return "|".join(self.sequence(depth, may_loop) for _ in range(count))


def deliberate_gap(text: str, pattern: str, flags: str, uses: set[str]) -> str | None:
    """Why the native layer may leave `text` undecided for an allow-listed
    `pattern` under `flags`, or None when it has to decide it. These are the
    gaps `_native.pyi` documents for a pattern inside the subset; any other
    undecided text is a bug.

    The generator does not reach the other deliberate gaps: group nesting
    deeper than 64 and the class shapes `[a||b]` and `[+--]` are outside its
    grammar, and its patterns stay far below the compiled-size bound (150
    seeds of 300 patterns under six flag sets never left one wholly
    undecided). A pattern that hit one would fail here; add the gap to this
    function with its reason, since it is deliberate and no native bug."""
    if "i" in flags and not pattern.isascii():
        return "flag i on a non-ASCII pattern leaves every text undecided"
    if not text.isascii() and ("i" in flags or uses & {"shorthand", r"\b", r"\B"}):
        return r"shorthands, \b, \B and flag i decide ASCII texts only"
    if text.endswith("\n") and "$" in uses and "m" not in flags:
        return "Python's $ also matches before a final newline"
    if text == "" and r"\B" in uses:
        return r"\B against the empty text differs between Python 3.14 and 3.11-3.13"
    return None


#: Beyond the corpus texts: an astral character, a combining accent, a line
#: separator, a next-line, a no-break space and Python's other whitespace controls.
EXTRA_TEXTS = [
    "\U0001f600", "e\u0301", "a\u2028b", "\x85", "\xa0x", "a\x0bb", "a\x0cb", "\x1e\x1f", "_", "a_1", "A1 b2",
]


def random_texts(rng: random.Random, count: int) -> list[str]:
    """Short texts over the alphabet the random patterns draw from, mostly
    ASCII (the half the shorthands, `\\b` and flag `i` decide) with the odd
    letter that Unicode case-folds across it."""
    ascii_chars = list("abAB xyzXYZ0159-/:_.\n\t\r\x0b\x1c\x1f")
    others = list("\u00e9\u017f\u212a")
    return [
        "".join(
            rng.choice(ascii_chars) if rng.random() < 0.93 else rng.choice(others)
            for _ in range(rng.randint(0, 8))
        )
        for _ in range(count)
    ]


@pytest.fixture(scope="session")
def built_regex_corpus(tmp_path_factory):
    texts = TEXTS + EXTRA_TEXTS + random_texts(random.Random(20261008), 600)
    return build(tmp_path_factory, "regex", [literal(t) for t in dict.fromkeys(texts)])


@pytest.fixture(scope="module", params=["file", "in-memory"])
def regex_corpus(request, built_regex_corpus):
    return open_corpus(built_regex_corpus, request.param)


def check_random_patterns(corpus: Corpus, seed: int, count: int) -> Counter:
    """`count` random allow-list patterns, each under every flag set, against
    every plain or tagged literal of `corpus`, ASCII or not. A passed text must
    match, a failed one must not, and an undecided one must be one of the
    `deliberate_gap`s; anything else raises. Returns the tallies: pairs
    `compared`, `decided`, the non-ASCII pairs the native layer had to decide
    and did (`non-ASCII decided`), and the gaps met by reason."""
    texts = {
        code: str(term)
        for code, term in zip(corpus.codes, corpus.terms, strict=True)
        if isinstance(term, Literal) and (term.language or term.datatype is None)
    }
    candidates = sorted(texts)
    rng = random.Random(seed)
    tally: Counter = Counter()
    for _ in range(count):
        generator = PatternGenerator(rng)
        pattern, uses = generator.pattern(), generator.uses
        for flags in ["", "i", "s", "m", "im", "ism"]:
            cflags = 0
            for f in flags:
                cflags |= RE_FLAGS[f]
            compiled = re.compile(pattern, cflags)  # the generator only emits valid patterns
            passed, undecided = corpus.term_dict.filter_codes("regex", pattern, U64Column(candidates), flags=flags)
            passed = set(memoryview(passed).cast("Q"))
            undecided = set(memoryview(undecided).cast("Q"))
            for code in candidates:
                text = texts[code]
                want = compiled.search(text) is not None
                if code in passed:
                    assert want, f"{pattern!r} {flags!r}: passed {text!r}"
                elif code not in undecided:
                    assert not want, f"{pattern!r} {flags!r}: failed {text!r}"
                else:
                    gap = deliberate_gap(text, pattern, flags, uses)
                    assert gap, f"{pattern!r} {flags!r}: an allow-listed pattern left {text!r} undecided"
                    tally[gap] += 1
                tally["compared"] += 1
                if code not in undecided:
                    tally["decided"] += 1
                    if not text.isascii() and deliberate_gap(text, pattern, flags, uses) is None:
                        tally["non-ASCII decided"] += 1
    return tally


def test_random_allow_list_patterns_agree_with_python_re(regex_corpus):
    tally = check_random_patterns(regex_corpus, seed=20261007, count=600)
    assert tally["decided"] > 1_000_000, tally
    assert tally["non-ASCII decided"] > 50_000, tally  # the non-ASCII half of the promise is exercised


# --- The wide 64-bit integer table ------------------------------------------------

#: The inclusive bounds the native value model holds `xsd:long` and
#: `xsd:unsignedLong` to. rdflib holds a `long` to none and an `unsignedLong`
#: to no upper bound, so beyond them rdflib compares and the native layer
#: must not decide. Values this wide are where a comparison through f64
#: would equate neighbours.
BOUNDS = {"long": (-(2**63), 2**63 - 1), "unsignedLong": (0, 2**64 - 1)}
WIDE_LEXICALS = [
    "-9223372036854775809",  # -2^63 - 1: one past long's lower edge
    "-9223372036854775808",  # -2^63
    "-9223372036854775807",
    "-1", "-0", "0", "1",
    "9223372036854775806",
    "9223372036854775807",  # 2^63 - 1
    "9223372036854775808",  # 2^63: one past long's upper edge
    "18446744073709551614",
    "18446744073709551615",  # 2^64 - 1
    "18446744073709551616",  # 2^64: one past unsignedLong's upper edge
]


def wide_constants() -> list[str]:
    """`"5"^^xsd:integer`, and each in-range boundary value of a table type
    with its in-range neighbour, typed as that type."""
    constants = [f'"5"^^<{XSD}integer>']
    for datatype, (lo, hi) in BOUNDS.items():
        constants += [f'"{v}"^^<{XSD}{datatype}>' for v in (lo, lo + 1, hi - 1, hi)]
    return constants + [f'"-0"^^<{XSD}unsignedLong>']


@pytest.fixture(scope="session")
def built_wide_corpus(tmp_path_factory):
    objects = [literal(lex, datatype=XSD + dt) for dt in BOUNDS for lex in WIDE_LEXICALS]
    # Unbounded integers of the same magnitudes: decided or not, they must agree.
    objects += [literal(lex, datatype=XSD + "integer") for lex in WIDE_LEXICALS]
    return build(tmp_path_factory, "wide", objects)


@pytest.fixture(scope="module", params=["file", "in-memory"])
def wide_corpus(request, built_wide_corpus):
    return open_corpus(built_wide_corpus, request.param)


@pytest.mark.parametrize("kind", NUM_OPS)
@pytest.mark.parametrize("arg", wide_constants())
def test_wide_long_boundaries_agree_with_rdflib(wide_corpus, kind, arg):
    """An in-range value is decided exactly as rdflib decides it; one past the
    type's 64-bit bounds is undecided."""
    bounded = {}
    for code, term in zip(wide_corpus.codes, wide_corpus.terms, strict=True):
        if isinstance(term, Literal) and str(term.datatype) in (XSD + "long", XSD + "unsignedLong"):
            lo, hi = BOUNDS[str(term.datatype)[len(XSD) :]]
            bounded[code] = lo <= term.value <= hi
    assert len(bounded) == 2 * len(WIDE_LEXICALS) and list(bounded.values()).count(False) == 10
    _, undecided = compare(wide_corpus, kind, arg)
    expected = {code for code, in_range in bounded.items() if not in_range}
    got = undecided & bounded.keys()
    assert got == expected, (
        f"{kind} {arg}: wrongly undecided {[wide_corpus.spellings[c] for c in sorted(got - expected)]}, "
        f"wrongly decided {[wide_corpus.spellings[c] for c in sorted(expected - got)]}"
    )


# --- The normalized datatypes -------------------------------------------------------


def test_normalized_datatypes_are_undecided_under_str(tmp_path):
    """Under `as_str`, a literal of every datatype rdflib normalizes comes
    back undecided for every string kind, whatever its lexical form: an
    rdflib upgrade that adds a key to `XSDToPython` fails this test until the
    native list names it. A plain, tagged or `xsd:string` literal, a datatype
    rdflib leaves alone (`xsd:gYear`) and an unknown one stay decided."""
    datatypes = sorted(NORMALIZED)
    assert XSD + "integer" in datatypes and RDF + "XMLLiteral" in datatypes and RDF + "HTML" in datatypes
    # The lexical form is immaterial (the native rule goes by datatype), and
    # free of letters so that a case wrapper leaves it alone.
    lexical = "1-2"
    normalized = [f'"{lexical}"^^<{dt}>' for dt in datatypes]
    plain, tagged = f'"{lexical}"', f'"{lexical}"@en'
    gyear, unknown = f'"{lexical}"^^<{XSD}gYear>', f'"{lexical}"^^<http://ex.org/dt>'
    nt = tmp_path / "datatypes.nt"
    nt.write_text(
        "".join(f"<urn:s:{i}> <urn:p> {s} .\n" for i, s in enumerate(normalized + [plain, tagged, gyear, unknown])),
        encoding="utf-8",
    )
    out = tmp_path / "datatypes.vortex"
    serialize_rdf(nt, out, format="ntriples", layout="dictionary")
    for in_memory in (False, True):
        term_dict = VortexRdfStore(out, in_memory=in_memory).term_dict()
        assert term_dict.file_backed == (not in_memory)
        coded = term_dict.encode_many(normalized + [plain, tagged, gyear, unknown])
        assert None not in coded
        universe = set(coded)  # the dictionary also holds the subject and predicate IRIs
        lits = set(coded[: len(normalized)])
        texts = {coded[-4], coded[-3]}  # plain, tagged: strings, whatever the option
        typed_texts = {coded[-2], coded[-1]}  # gYear, unknown: strings under STR() only
        every = U64Column(range(len(term_dict)))
        for kind, arg in [
            ("str_prefix", "1"), ("contains", '"-"'), ("strstarts", '"1"'), ("strends", '"2"'), ("regex", "^1-2$"),
        ]:
            for case in (None, "lower", "upper"):
                where = f"{kind} case={case} in_memory={in_memory}"
                passed, undecided = term_dict.filter_codes(kind, arg, every, case=case, as_str=True)
                passed, undecided = set(memoryview(passed).cast("Q")), set(memoryview(undecided).cast("Q"))
                assert undecided & universe == lits, f"{where}: undecided {sorted(undecided & universe)}"
                assert passed & universe == texts | typed_texts, f"{where}: passed {sorted(passed & universe)}"
                # Without STR() a typed literal is no string: it fails, decided.
                passed, undecided = term_dict.filter_codes(kind, arg, every, case=case)
                passed, undecided = set(memoryview(passed).cast("Q")), set(memoryview(undecided).cast("Q"))
                assert not undecided & universe, f"{where}: undecided {sorted(undecided & universe)} without as_str"
                assert passed & universe == texts, f"{where}: passed {sorted(passed & universe)} without as_str"


# --- BSBM 10K ---------------------------------------------------------------------

BSBM10K = os.environ.get("VORTEX_RDF_BSBM10K_STORE")


@pytest.mark.skipif(not BSBM10K, reason="set VORTEX_RDF_BSBM10K_STORE to a BSBM 10K Dictionary store")
def test_bsbm_10k_literals_agree_with_rdflib():
    store = VortexRdfStore(BSBM10K)
    term_dict = store.term_dict()
    lo, hi = term_dict.prefix_range('"')
    codes = list(range(lo, hi, max(1, (hi - lo) // 6000)))
    spellings = term_dict.decode_many(codes)
    sample = Corpus(store, term_dict, codes, spellings, parse_terms(spellings))
    words = sorted({w for s in spellings[:3000] for w in re.findall(r"[a-z]{5,}", s)})[:10]
    for word in words:
        compare(sample, "regex", word)
        compare(sample, "regex", word.upper(), flags="i")
        compare(sample, "contains", f'"{word}"', case="lower")
        compare(sample, "strstarts", f'"{word}"', as_str=True)
    compare(sample, "lang", "en")
    compare(sample, "num_lt", f'"500"^^<{XSD}integer>')
    compare(sample, "datatype", XSD + "dateTime")
