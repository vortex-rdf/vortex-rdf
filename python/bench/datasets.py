"""Dataset generation for the Python comparative benchmark.

A deliberate port of `js/bench/datasets.ts`, kept algorithmically identical --
same moduli, same term spellings, same probe selection -- so a row on the
Python dashboard tab measures the *same data* as the corresponding row on the
JavaScript tab. Any drift here silently makes the two tabs incomparable, so the
uniqueness argument and the cardinality defaults are reproduced verbatim rather
than re-derived.

Two things differ from the JS original, both forced by what the Python
libraries accept:

  * The output is a serialized N-Triples/N-Quads *file*, not an array of term
    objects. Every library under test ingests from a file (`serialize_rdf`,
    `bulk_load`, `rdf2cottas`, `Graph.parse`), and lightrdf has no store at all
    -- a file is the only input all five share.
  * Generation streams to disk. Materializing 2M quads as Python objects first
    would cost more memory than any store under test, and this module runs in
    the same process as the adapter whose peak RSS is being attributed.

PURITY CONTRACT: standard library only. This module is imported by every
adapter's isolated virtualenv, which by construction does not have the other
adapters' dependencies installed.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from math import gcd, log
from typing import Iterator, Optional

BASE = "http://data.example.org"


# ─── Cardinality-controlled dataset ──────────────────────────────────────────
#
# Term cardinality is an explicit knob, independent of row count: drawing D**3
# rows from a namespace of D IRIs would make every store's term handling --
# dictionaries, interning, string storage -- invisible to the benchmark.
#
# Uniqueness: term indices are `i % k` per role, so quad i maps to the residue
# tuple (i mod nSubj, i mod nPred, i mod nObj, i mod nGraph). By the Chinese
# Remainder Theorem that map is injective over `i < lcm(...)`, so making the
# four moduli pairwise coprime (their lcm is then their product) and checking
# the product covers `n` guarantees every generated quad is distinct, with no
# dedupe set -- which at these row counts would itself dominate memory.
#
# Deliberate consequence: index 0 exists for every role, so row 0 satisfies all
# four single-term probes and every pattern below matches at least one row.

SUBJECT_RATIO_DEFAULT = 0.1


@dataclass(frozen=True)
class DatasetOpts:
    """Distinct terms per role. `subject_ratio` is the reciprocal of how many
    triples describe each resource -- 0.1 is ten triples per subject, which is
    what makes the dictionary a meaningful fraction of the dataset."""

    subject_ratio: float = SUBJECT_RATIO_DEFAULT
    predicates: int = 32
    object_ratio: float = 0.5
    literal_frac: float = 0.4
    graphs: int = 1


def dataset_opts(**overrides) -> DatasetOpts:
    """Env-overridable defaults, so a sweep can vary cardinality without code."""
    base = DatasetOpts(
        subject_ratio=float(os.environ.get("BENCH_SUBJ_RATIO", SUBJECT_RATIO_DEFAULT)),
        predicates=int(os.environ.get("BENCH_PREDICATES", 32)),
        object_ratio=float(os.environ.get("BENCH_OBJ_RATIO", 0.5)),
        literal_frac=float(os.environ.get("BENCH_LITERAL_FRAC", 0.4)),
        graphs=int(os.environ.get("BENCH_GRAPHS", 1)),
    )
    return DatasetOpts(**{**base.__dict__, **{k: v for k, v in overrides.items() if v is not None}})


@dataclass(frozen=True)
class Moduli:
    n_subj: int
    n_pred: int
    n_obj: int
    n_graph: int
    terms: int


def moduli(n: int, opts: Optional[DatasetOpts] = None) -> Moduli:
    """Per-role term counts: the requested values nudged up until pairwise coprime."""
    o = opts or dataset_opts()
    want = [
        max(1, round(n * o.subject_ratio)),
        max(1, round(o.predicates)),
        max(1, round(n * o.object_ratio)),
        max(1, round(o.graphs)),
    ]
    got: list[int] = []
    for w in want:
        k = w
        while any(gcd(g, k) != 1 for g in got):
            k += 1
        got.append(k)
    n_subj, n_pred, n_obj, n_graph = got

    # Product == lcm because they are pairwise coprime; see the CRT note above.
    # Use logs: the product overflows long before it matters numerically.
    if sum(log(k) for k in got) < log(n):
        raise ValueError(
            f"dataset cardinality too low for {n} distinct quads: "
            f"{n_subj}x{n_pred}x{n_obj}x{n_graph} cannot cover it -- raise a ratio"
        )
    # The default graph is a term too, and it is not one of the named graphs.
    terms = n_subj + n_pred + n_obj + (1 if n_graph == 1 else n_graph)
    return Moduli(n_subj, n_pred, n_obj, n_graph, terms)


# ─── Term spellings (must match js/bench/datasets.ts exactly) ────────────────

def subject_term(i: int) -> str:
    return f"<{BASE}/resource/2026/subject/{i:09d}>"


def predicate_term(i: int) -> str:
    return f"<{BASE}/ontology/2026/property/{i:04d}>"


def graph_term(i: int) -> str:
    return f"<{BASE}/graph/2026/named/{i:06d}>"


def object_term(i: int, literal_frac: float) -> str:
    """Objects alternate IRI/literal deterministically, in `literal_frac` proportion.

    The literal body is drawn from the same fixed alphabet as the JS generator
    and contains no quote, backslash, or newline, so N-Triples quoting here is
    a bare wrap rather than a general escaper -- keep it that way, or escaping
    cost lands in every adapter's parse timing.
    """
    if i % 10 < round(literal_frac * 10):
        return f'"descriptive object value number {i:09d}"'
    return f"<{BASE}/resource/2026/object/{i:09d}>"


def quad_at(i: int, m: Moduli, literal_frac: float) -> tuple[str, str, str, Optional[str]]:
    """Quad `i` of the dataset, as N-Triples-ready term strings. Pure in `i`, so
    probes and prefixes derive without materializing the dataset."""
    return (
        subject_term(i % m.n_subj),
        predicate_term(i % m.n_pred),
        object_term(i % m.n_obj, literal_frac),
        None if m.n_graph == 1 else graph_term(i % m.n_graph),
    )


def iter_quads(n: int, opts: Optional[DatasetOpts] = None) -> Iterator[tuple[str, str, str, Optional[str]]]:
    o = opts or dataset_opts()
    m = moduli(n, o)
    for i in range(n):
        yield quad_at(i, m, o.literal_frac)


def write_dataset(path: str, n: int, opts: Optional[DatasetOpts] = None) -> str:
    """Stream `n` distinct quads to an N-Triples (or N-Quads) file at `path`.

    Written once by the orchestrator and shared by every adapter process: the
    file is the common input, so no adapter pays for another's generation, and
    generation cost never lands inside a measured region.
    """
    o = opts or dataset_opts()
    tmp = path + ".partial"
    with open(tmp, "w", encoding="utf-8", buffering=1 << 20) as f:
        for s, p, ob, g in iter_quads(n, o):
            f.write(f"{s} {p} {ob} .\n" if g is None else f"{s} {p} {ob} {g} .\n")
    os.replace(tmp, path)  # never leave a half-written dataset that looks complete
    return path


def fresh_quads(n: int) -> list[tuple[str, str, str]]:
    """A batch of quads in a namespace disjoint from `write_dataset`'s, so the
    add phase inserts rows the store cannot already hold."""
    return [
        (
            f"<{BASE}/fresh/2026/subject/{i:09d}>",
            f"<{BASE}/fresh/2026/property/0000>",
            f"<{BASE}/fresh/2026/object/{i:09d}>",
        )
        for i in range(n)
    ]


def dataset_prefix(n: int, take: int, opts: Optional[DatasetOpts] = None) -> list[tuple[str, str, str, Optional[str]]]:
    """The first `take` quads `write_dataset(n)` would produce. The delete phase
    needs a batch-sized slice of quads that actually exist in the store."""
    o = opts or dataset_opts()
    m = moduli(n, o)
    return [quad_at(i, m, o.literal_frac) for i in range(min(take, n))]


# ─── Query patterns ─────────────────────────────────────────────────────────
#
# Probe terms are fixed at index 0, so every probe matches row 0 at minimum and
# no pattern can silently measure a zero-row query. Selectivity follows the
# data: at the defaults `S` matches ~10 rows, `P` ~n/32, `O` ~2, and the
# conjunctions narrow to a handful. Each worker records the matched-row count
# alongside the timing so the figures stay interpretable.


@dataclass(frozen=True)
class Pat:
    name: str
    s: Optional[str]
    p: Optional[str]
    o: Optional[str]
    g: Optional[str]


FULL_SCAN_PATTERN = Pat("full", None, None, None, None)


def dataset_probes(n: int, opts: Optional[DatasetOpts] = None) -> dict[str, list[Pat]]:
    o = opts or dataset_opts()
    m = moduli(n, o)
    s0 = subject_term(0)
    p0 = predicate_term(0)
    o0 = object_term(0, o.literal_frac)
    g0 = None if m.n_graph == 1 else graph_term(0)
    # A single-graph dataset has no graph term to bind, which would quietly turn
    # `G` into a second full scan and `SPOG` into `SPO` -- two probes measuring
    # something other than what their names claim. Return no quad probes instead,
    # so a caller that forgot to pass the quads options gets nothing rather than
    # plausible-looking wrong rows.
    quads = (
        []
        if g0 is None
        else [Pat("G", None, None, None, g0), Pat("SPOG", s0, p0, o0, g0)]
    )
    return {
        "triples": [
            Pat("S", s0, None, None, None),
            Pat("P", None, p0, None, None),
            Pat("O", None, None, o0, None),
            Pat("PO", None, p0, o0, None),
            Pat("SPO", s0, p0, o0, None),
        ],
        "quads": quads,
        "full": [FULL_SCAN_PATTERN],
    }


# ─── Dense-cube datasets (the instrumented suite's routing-pattern data) ─────
#
# A d**3 / d**4 cube over a tiny closed namespace: every routing decision is
# exercised while the dataset stays small enough to instrument. Deliberately
# NOT cardinality-realistic -- 3*d distinct terms over d**3 rows is exactly the
# pathology the generator above exists to avoid -- so term-handling-sensitive
# tasks must use `write_dataset` instead; the cube is for tasks where routing,
# not term volume, is what is being measured.
#
# Ported from `js/bench/datasets.ts` alongside the generator above, so the
# instrumented Python and JavaScript suites measure the same shapes.

EX = "http://example.org/#"

XSD_INTEGER = "<http://www.w3.org/2001/XMLSchema#integer>"


def nn(n) -> str:
    return f"<{EX}{n}>"


def escape_literal(value: str) -> str:
    """N-Triples escaping for a literal's lexical form.

    Needed only by `write_literal_triples`, whose escape-bearing case carries
    quotes, backslashes and newlines on purpose; every other generator here
    emits values that pass through unchanged.
    """
    out = value.replace("\\", "\\\\").replace('"', '\\"')
    return out.replace("\n", "\\n").replace("\r", "\\r").replace("\t", "\\t")


def write_triples_cube(path: str, d: int) -> str:
    """d**3 triples in the default graph: (ex#s, ex#p, ex#o)."""
    with open(path, "w", encoding="utf-8", buffering=1 << 20) as f:
        for s in range(d):
            for p in range(d):
                for o in range(d):
                    f.write(f"{nn(s)} {nn(p)} {nn(o)} .\n")
    return path


def write_quads_cube(path: str, d: int) -> str:
    """d**4 quads across named graphs: (ex#s, ex#p, ex#o, ex#g)."""
    with open(path, "w", encoding="utf-8", buffering=1 << 20) as f:
        for s in range(d):
            for p in range(d):
                for o in range(d):
                    for g in range(d):
                        f.write(f"{nn(s)} {nn(p)} {nn(o)} {nn(g)} .\n")
    return path


def write_literal_triples(path: str, d: int) -> str:
    """d**3 triples whose objects are literals -- plain, language-tagged, typed,
    and escape-bearing (quotes, backslashes, newlines, and `"@`/`^^` lookalikes
    inside the value). The other cube datasets are named-node-only, so tasks
    over this one are the only cube coverage of literal escaping."""
    i = 0
    with open(path, "w", encoding="utf-8", buffering=1 << 20) as f:
        for s in range(d):
            for p in range(d):
                for o in range(d):
                    if i % 4 == 0:
                        obj = f'"plain value {o}"'
                    elif i % 4 == 1:
                        obj = f'"tagged value {o}"@en'
                    elif i % 4 == 2:
                        obj = f'"{o}"^^{XSD_INTEGER}'
                    else:
                        raw = f'say "hi"@home ^^ line\nbreak back\\slash {o}'
                        obj = f'"{escape_literal(raw)}"'
                    f.write(f"{nn(s)} {nn(p)} {obj} .\n")
                    i += 1
    return path
