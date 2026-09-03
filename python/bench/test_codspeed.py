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
* `match_arrow` reads the codes back through pyarrow, the boundary a query
  layer crosses; JavaScript's `readpath::matchArrow` is the zero-copy twin.

Run locally (after `maturin develop`):
    uv run pytest bench/test_codspeed.py --codspeed
    CODSPEED_BENCH_DIM=48 uv run pytest bench/test_codspeed.py --codspeed
"""

from __future__ import annotations

import os
import sys
from array import array
from pathlib import Path

import pyarrow as pa
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
#
# The defaults are the size all three CodSpeed suites share -- 32**3 = 32,768,
# which core/benches/support/mod.rs takes as its BENCH_SIZE default -- so one
# shared-core regression lands in all three tabs at comparable magnitude. It
# is also 4 zones of 8,192 rows, the smallest round size at which zone pruning
# has anything to prune.
DIM = int(os.environ.get("CODSPEED_BENCH_DIM", 32))  # triples: DIM**3 rows
DIM_QUADS = int(os.environ.get("CODSPEED_BENCH_DIM_QUADS", 13))  # quads: DIM_QUADS**4 rows

# ─── Query patterns (probe terms fixed at index 0, so they always hit rows) ──
T0 = nn(0)
G0 = nn(0)
TRIPLE_PATTERNS = [
    Pat("S", T0, None, None, None),
    Pat("P", None, T0, None, None),
    Pat("O", None, None, T0, None),
    Pat("SP", T0, T0, None, None),
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
#: unindexed Dictionary baseline and the fully-indexed (secondary-by-copy)
#: fast path.
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
def store_paths(tmp_path_factory, data) -> dict[str, str]:
    """The `.vortex` artifacts, written once per session. Separate from
    `stores` because the cold query tasks need the path to reopen, not an
    already-opened handle."""
    root = tmp_path_factory.mktemp("codspeed-stores")
    paths: dict[str, str] = {}
    for variant in QUERY_VARIANTS:
        paths[f"triples::{variant}"] = _build(
            data["cube"], root / f"t-{variant}.vortex", variant
        )
        paths[f"quads::{variant}"] = _build(
            data["quads"], root / f"q-{variant}.vortex", variant
        )
    paths["realistic"] = _build(data["realistic"], root / "realistic.vortex", "dict")
    paths["literals"] = _build(data["literals"], root / "literals.vortex", "dict")
    return paths


@pytest.fixture(scope="session")
def stores(store_paths) -> dict[str, VortexRdfStore]:
    """Stores opened once per session for the warm read-path tasks (build is
    timed separately, so it must not be paid inside a read measurement)."""
    return {key: VortexRdfStore(path) for key, path in store_paths.items()}


# ─── build::<config> ────────────────────────────────────────────────────────


@pytest.mark.benchmark
@pytest.mark.parametrize("variant", list(BUILD_VARIANTS))
def test_build(benchmark, tmp_path_factory, data, variant):
    """Parse an RDF file and write the `.vortex` store, per star variant.

    `dict` runs over the cardinality-realistic dataset, not the cube, as in
    the JS suite: it is the guard on dictionary construction, and on
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
def test_query_warm_triples(benchmark, stores, variant, pattern):
    """A repeat query on an open store — the steady state of a long-lived
    process, with probes resolved and chunk/dictionary caches populated."""
    store = stores[f"triples::{variant}"]
    benchmark(lambda: store.get_quads(pattern.s, pattern.p, pattern.o, pattern.g))


@pytest.mark.benchmark
@pytest.mark.parametrize("variant", QUERY_VARIANTS)
@pytest.mark.parametrize("pattern", QUAD_PATTERNS, ids=lambda p: p.name)
def test_query_warm_quads(benchmark, stores, variant, pattern):
    store = stores[f"quads::{variant}"]
    benchmark(lambda: store.get_quads(pattern.s, pattern.p, pattern.o, pattern.g))


def _cold_query(benchmark, path: str, pattern: Pat) -> None:
    """The FIRST query on a freshly opened store — nothing decoded, no probe
    resolved, no dictionary memo. The counterpart of the Rust suite's
    `match_cold_*` groups and the JS suite's `query_cold_*` tasks.

    The open is outside the measured callable, so this isolates the query
    against empty caches, excluding the open; opening is
    its own task (`test_open`). That works because instrumentation measures a
    single invocation — which is genuinely the store's first query. Under a
    harness that called the function repeatedly the later calls would be warm,
    so read this task only from an instrumented run.
    """
    store = VortexRdfStore(path)
    benchmark(lambda: store.get_quads(pattern.s, pattern.p, pattern.o, pattern.g))


@pytest.mark.benchmark
@pytest.mark.parametrize("variant", QUERY_VARIANTS)
@pytest.mark.parametrize(
    "pattern", TRIPLE_PATTERNS + [FULL_SCAN_PATTERN], ids=lambda p: p.name
)
def test_query_cold_triples(benchmark, store_paths, variant, pattern):
    _cold_query(benchmark, store_paths[f"triples::{variant}"], pattern)


@pytest.mark.benchmark
@pytest.mark.parametrize("variant", QUERY_VARIANTS)
@pytest.mark.parametrize("pattern", QUAD_PATTERNS, ids=lambda p: p.name)
def test_query_cold_quads(benchmark, store_paths, variant, pattern):
    _cold_query(benchmark, store_paths[f"quads::{variant}"], pattern)


@pytest.mark.benchmark
@pytest.mark.parametrize("variant", QUERY_VARIANTS)
def test_open(benchmark, store_paths, variant):
    """Opening the artifact into a queryable store, with no query behind it —
    the other half of a cold start, so the cold query tasks above can be read
    without their number silently containing this one."""
    path = store_paths[f"triples::{variant}"]
    benchmark(lambda: VortexRdfStore(path))


# ─── readpath::<variant> ────────────────────────────────────────────────────


@pytest.mark.benchmark
@pytest.mark.parametrize("op", ["get_quads", "match_arrow"])
def test_readpath(benchmark, stores, op):
    """The read entry points on the unindexed store for one selective pattern
    (S), isolating the boundary cost each carries.

    `match_arrow` is the lazy one — u32 code columns read back through
    pyarrow, no term strings — the Python analogue of the JS suite's
    `readpath::matchArrow`. `get_quads` materializes terms, which in JS is
    `readpath::getQuads_decoded`; the bindings have no lazy quad object, so
    there is no undecoded `get_quads`.
    """
    store = stores["triples::dict"]
    p = TRIPLE_PATTERNS[0]  # S
    if op == "get_quads":
        benchmark(lambda: store.get_quads(p.s, p.p, p.o, p.g))
    else:
        benchmark(
            lambda: pa.RecordBatchReader.from_stream(store.match_arrow(p.s, p.p, p.o, p.g)).read_all()
        )


def _code_columns(store, s, p, o, g):
    """The matched rows' code columns as `array("I")` buffers."""
    table = pa.RecordBatchReader.from_stream(store.match_arrow(s, p, o, g)).read_all()
    return [array("I", column.to_pylist()) for column in table.columns]


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
# No JavaScript counterpart: these guard `TermDict::decode_slice`'s sharing of
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
    codes = _code_columns(store, p.s, p.p, p.o, p.g)[1 if shape == "constant" else 0]
    _assert_shape(shape, codes)
    benchmark(lambda: dictionary.decode_many(codes))


@pytest.mark.benchmark
def test_decode_many_scattered(benchmark, scattered_store):
    codes = _code_columns(scattered_store, None, RDF_TYPE, None, None)[2]
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
    since the wasm bindings have no file-backed store.

    Measures the same operation as `test_open[dict_bycopy]` on a file built
    inside the task; both ids are tracked on CodSpeed and kept for continuity.
    """
    out = _build(data["cube"], tmp_path_factory.mktemp("open") / "out.vortex", "dict_bycopy")
    benchmark(lambda: VortexRdfStore(out))
