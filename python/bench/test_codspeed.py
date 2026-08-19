"""CodSpeed benchmark for the Python bindings — library-only.

The Python counterpart of `js/bench/codspeed.bench.ts`, which is itself the
counterpart of the Rust suite (`core/benches/benchmark.rs`): the same "star"
(one-factor-at-a-time) design over layout x secondary index, swept across the
query routing patterns, plus the build and read-back paths. Task
names and dataset shapes match the JavaScript suite wherever the binding
surfaces line up, so the two are read side by side on CodSpeed.

Unlike `run.py` (wall-clock, comparative against other libraries, feeds the
Pages dashboard, NEVER uploaded), THIS file IS uploaded: it runs under
`pytest --codspeed` in instrumentation mode, so every task gets deterministic
instruction counts. Without the plugin `pytest` just runs them as tests, which
also keeps them honest.

Where the two suites cannot correspond:

* `mutate::*` has no Python counterpart — the bindings expose no add/delete.
* `readback::toRdf_*` has none — there is no export API.
* `readpath::match_stream` has none — there is no streaming read.
* Building takes a *file*, so every `build::*` task is parse-and-build. That
  makes it the analogue of the JS suite's `build::fromString_nquads` as much as
  of its `fromQuads` variants, and the two are not comparable in absolute
  terms — only against themselves over time.
* `match_columns` is a Python-only read path and has no JavaScript row.

Run locally (after `maturin develop`):
    uv run pytest bench/test_codspeed.py --codspeed
    CODSPEED_BENCH_DIM=24 uv run pytest bench/test_codspeed.py --codspeed
"""

from __future__ import annotations

import os
import sys
from array import array
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from datasets import (  # noqa: E402
    FULL_SCAN_PATTERN,
    Pat,
    dataset_probes,
    nn,
    write_dataset,
    write_literal_triples,
    write_quads_cube,
    write_triples_cube,
)

from vortex_rdf import VortexRdfStore, serialize_rdf  # noqa: E402

# ─── Dataset shape (env-tunable, same knobs and defaults as the JS suite) ────
# Small by default: instrumentation runs under Valgrind, and a representative
# size catches regressions on every path while a larger one only multiplies
# cost.
#
# Two shapes, per task sensitivity: the dense cube for tasks measuring routing
# and boundary cost, and the cardinality-realistic generator for the
# term-handling-sensitive tasks, where the cube's few distinct terms would make
# dictionary build and per-distinct-term decode invisible.
DIM = int(os.environ.get("CODSPEED_BENCH_DIM", 16))  # triples: DIM**3 rows
DIM_QUADS = int(os.environ.get("CODSPEED_BENCH_DIM_QUADS", 8))  # quads: DIM_QUADS**4 rows

# ─── Query patterns (probe terms fixed at index 0, so they always hit rows) ──
T0 = nn(0)
G0 = nn(0)
TRIPLE_PATTERNS = [
    Pat("S", T0, None, None, None),
    Pat("P", None, T0, None, None),
    Pat("O", None, None, T0, None),
    Pat("PO", None, T0, T0, None),
    Pat("SPO", T0, T0, T0, None),
]
QUAD_PATTERNS = [
    Pat("G", None, None, None, G0),
    Pat("SPOG", T0, T0, T0, G0),
]

# ─── Store variants (mirror the Rust and JS star-design axes) ───────────────
#: Build (write) path, one factor at a time around a Dictionary baseline.
#: Same set and same slugs as the JS suite's BUILD_VARIANTS.
BUILD_VARIANTS = {
    "dict": dict(layout="dictionary"),
    "default": dict(layout="default"),
    "typedobject": dict(layout="typed-object"),
    "dict_byref": dict(layout="dictionary", indexes=["secondary-by-reference"]),
    "dict_bycopy": dict(layout="dictionary", indexes=["secondary-by-copy"]),
}

#: Query (read) path: the two representative configs the JS suite uses — the
#: unindexed default and the fully-indexed fast path.
QUERY_VARIANTS = ("dict", "dict_bycopy")


def _build(source: Path, out: Path, variant: str) -> str:
    serialize_rdf(str(source), str(out), **BUILD_VARIANTS[variant])
    return str(out)


@pytest.fixture(scope="session")
def data(tmp_path_factory) -> dict[str, Path]:
    """The four source datasets, written once per session.

    `cube`/`quads`/`literals` are the dense-cube shapes; `realistic` has the
    same row count as the cube but term cardinality that scales with it.
    """
    root = tmp_path_factory.mktemp("codspeed-data")
    cube = root / "cube.nt"
    quads = root / "cube.nq"
    literals = root / "literals.nt"
    realistic = root / "realistic.nt"
    write_triples_cube(str(cube), DIM)
    write_quads_cube(str(quads), DIM_QUADS)
    write_literal_triples(str(literals), DIM)
    write_dataset(str(realistic), DIM**3)
    return {"cube": cube, "quads": quads, "literals": literals, "realistic": realistic}


@pytest.fixture(scope="session")
def stores(tmp_path_factory, data) -> dict[str, VortexRdfStore]:
    """Stores built once per session for the read-path tasks (build is timed
    separately, so it must not be paid inside a read measurement)."""
    root = tmp_path_factory.mktemp("codspeed-stores")
    built: dict[str, VortexRdfStore] = {}
    for variant in QUERY_VARIANTS:
        built[f"triples::{variant}"] = VortexRdfStore(
            _build(data["cube"], root / f"t-{variant}.vortex", variant)
        )
        built[f"quads::{variant}"] = VortexRdfStore(
            _build(data["quads"], root / f"q-{variant}.vortex", variant)
        )
    built["realistic"] = VortexRdfStore(
        _build(data["realistic"], root / "realistic.vortex", "dict")
    )
    built["literals"] = VortexRdfStore(
        _build(data["literals"], root / "literals.vortex", "dict")
    )
    return built


# ─── build::<config> ────────────────────────────────────────────────────────


@pytest.mark.benchmark
@pytest.mark.parametrize("variant", list(BUILD_VARIANTS))
def test_build(benchmark, tmp_path_factory, data, variant):
    """Parse an RDF file and write the `.vortex` store, per star variant.

    `dict` runs over the cardinality-realistic dataset rather than the
    cube, as in the JS suite: it is the guard on dictionary construction, and on
    the cube the dictionary is a few dozen terms and its build cost invisible.
    Its number is therefore NOT comparable with the cube variants beside it.
    """
    source = data["realistic"] if variant == "dict" else data["cube"]
    out = tmp_path_factory.mktemp("build") / "out.vortex"
    benchmark(lambda: _build(source, out, variant))


@pytest.mark.benchmark
def test_build_literals(benchmark, tmp_path_factory, data):
    """Build over literal-bearing data — the escaping path on ingest."""
    out = tmp_path_factory.mktemp("build-lit") / "out.vortex"
    benchmark(lambda: _build(data["literals"], out, "dict"))


# ─── query_<config>::<pattern> ──────────────────────────────────────────────


@pytest.mark.benchmark
@pytest.mark.parametrize("variant", QUERY_VARIANTS)
@pytest.mark.parametrize(
    "pattern", TRIPLE_PATTERNS + [FULL_SCAN_PATTERN], ids=lambda p: p.name
)
def test_query_triples(benchmark, stores, variant, pattern):
    store = stores[f"triples::{variant}"]
    benchmark(lambda: store.get_quads(pattern.s, pattern.p, pattern.o, pattern.g))


@pytest.mark.benchmark
@pytest.mark.parametrize("variant", QUERY_VARIANTS)
@pytest.mark.parametrize("pattern", QUAD_PATTERNS, ids=lambda p: p.name)
def test_query_quads(benchmark, stores, variant, pattern):
    store = stores[f"quads::{variant}"]
    benchmark(lambda: store.get_quads(pattern.s, pattern.p, pattern.o, pattern.g))


# ─── readpath::<variant> ────────────────────────────────────────────────────


@pytest.mark.benchmark
@pytest.mark.parametrize("op", ["get_quads", "match_codes", "match_columns"])
def test_readpath(benchmark, stores, op):
    """The read entry points on the unindexed store for one selective pattern
    (S), isolating the boundary cost each carries.

    `match_codes` is the lazy one — u32 columns, no term strings — so it is the
    Python analogue of the JS suite's `readpath::matchCodes`. The other two
    materialize terms, which in JS is `readpath::getQuads_decoded`; the
    bindings have no lazy quad object, so there is no undecoded `get_quads`.
    """
    store = stores["triples::dict"]
    p = TRIPLE_PATTERNS[0]  # S
    call = getattr(store, op)
    benchmark(lambda: call(p.s, p.p, p.o, p.g))


@pytest.mark.benchmark
def test_readpath_full_decoded(benchmark, stores):
    """Every row, every term — every distinct code resolved once.

    Runs over the cardinality-realistic dataset for the same reason the JS suite
    does: on the cube it would resolve a few dozen codes and the per-distinct-
    term dictionary cost it exists to guard would be invisible. Do not compare
    it row-for-row with the selective cube tasks.
    """
    store = stores["realistic"]
    f = FULL_SCAN_PATTERN
    benchmark(lambda: store.get_quads(f.s, f.p, f.o, f.g))


@pytest.mark.benchmark
def test_readpath_full_decoded_literals(benchmark, stores):
    """The same worst case over literal-bearing data: every literal's
    serialized form is rebuilt on the way out."""
    store = stores["literals"]
    f = FULL_SCAN_PATTERN
    benchmark(lambda: store.get_quads(f.s, f.p, f.o, f.g))


# ─── decode::<shape> (Python-only) ──────────────────────────────────────────
#
# No JavaScript counterpart: these guard `TermDict::decode_owned`'s sharing of
# one Python string across repeats of a code. The three shapes are not
# variations on one workload — each is a different way a column can repeat, and
# the sharing covers them unequally:
#
#   constant  — a bound position, one code for every row.
#   distinct  — no repetition at all, where sharing can only cost.
#   scattered — a small vocabulary spread across rows. This is the one that
#               matters: a decoder that only coalesces adjacent repeats scores
#               like `distinct` here while one that tracks recent codes scores
#               near `constant`, and nothing else tells the two apart.

RDF_TYPE = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"


def _assert_shape(shape: str, codes) -> None:
    """Fail if a column is not the repetition shape its task name claims.

    Each task exists to measure one shape, and a dataset or ordering change can
    quietly turn one into another — a column labelled `distinct` that is really
    clustered measures sharing instead of its absence, and the task keeps
    reporting a plausible number.
    """
    rows, distinct = len(codes), len(set(codes))
    runs = 1 + sum(1 for a, b in zip(codes, codes[1:]) if a != b)
    if shape == "constant":
        assert distinct == 1, f"constant column has {distinct} distinct codes"
    elif shape == "distinct":
        assert distinct == rows, f"distinct column repeats: {distinct} of {rows}"
    elif shape == "scattered":
        assert distinct * 10 < rows, f"scattered column is not low-cardinality: {distinct}/{rows}"
        assert runs > rows // 2, f"scattered column is clustered: {runs} runs over {rows} rows"
    assert rows > 0


@pytest.fixture(scope="session")
def scattered_store(tmp_path_factory) -> VortexRdfStore:
    root = tmp_path_factory.mktemp("codspeed-typed")
    source = root / "typed.nt"
    classes = 20
    source.write_text(
        "".join(
            f"<http://ex.org/s{i:08d}> {RDF_TYPE} <http://ex.org/Class{i % classes:02d}> .\n"
            for i in range(DIM**3)
        ),
        encoding="utf-8",
    )
    out = root / "typed.vortex"
    serialize_rdf(str(source), str(out), layout="dictionary")
    return VortexRdfStore(str(out))


@pytest.mark.benchmark
@pytest.mark.parametrize("shape", ["constant", "distinct"])
def test_decode_many(benchmark, stores, shape):
    """Runs over the cardinality-realistic dataset, not the cube: a P match on
    the cube draws its subjects from DIM values in runs, so `distinct` would
    hold DIM distinct codes over DIM**2 rows and measure sharing rather than
    the absence of it."""
    store = stores["realistic"]
    dictionary = store.term_dict()
    p = next(x for x in dataset_probes(DIM**3)["triples"] if x.name == "P")
    columns = store.match_codes(p.s, p.p, p.o, p.g)
    codes = array("I", memoryview(columns[1 if shape == "constant" else 0]).cast("I"))
    _assert_shape(shape, codes)
    benchmark(lambda: dictionary.decode_many(codes))


@pytest.mark.benchmark
def test_decode_many_scattered(benchmark, scattered_store):
    codes = array(
        "I", memoryview(scattered_store.match_codes(None, RDF_TYPE, None, None)[2]).cast("I")
    )
    _assert_shape("scattered", codes)
    benchmark(lambda: scattered_store.term_dict().decode_many(codes))


# ─── readback::<op> ─────────────────────────────────────────────────────────


@pytest.mark.benchmark
def test_readback_to_bytes(benchmark, stores):
    store = stores["triples::dict_bycopy"]
    benchmark(store.to_bytes)


@pytest.mark.benchmark
def test_readback_from_bytes(benchmark, stores):
    data = stores["triples::dict_bycopy"].to_bytes()
    benchmark(lambda: VortexRdfStore.from_bytes(data))


@pytest.mark.benchmark
def test_readback_open(benchmark, tmp_path_factory, data):
    """Opening an already-built file — the operation with no JS counterpart,
    since the wasm bindings have no file-backed store."""
    out = _build(data["cube"], tmp_path_factory.mktemp("open") / "out.vortex", "dict_bycopy")
    benchmark(lambda: VortexRdfStore(out))
