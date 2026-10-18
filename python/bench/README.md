# Python benchmarks

`run.py` measures the bindings against [pyoxigraph](https://pypi.org/project/pyoxigraph/), [pycottas](https://pypi.org/project/pycottas/), [rdflib](https://pypi.org/project/rdflib/) and [lightrdf](https://pypi.org/project/lightrdf/) on a file → store → query workload and writes `target/bench/python.json` at the repository root for [the dashboard's Python tab](https://vortex-rdf.github.io/vortex-rdf/#py); `test_codspeed.py` is the instrumented suite CodSpeed runs. `scripts/refresh.sh` runs every suite and renders the dashboard (`--only python,render` re-measures this one); it needs GNU coreutils (`sha256sum`, `sort -z`).

```bash
python3 python/bench/run.py                 # full run, from the repository root
BENCH_DIM=32 python3 python/bench/run.py    # quick pilot
cd python && uv run pytest bench/test_codspeed.py --codspeed
```

Harness design (per-library virtualenvs, dataset parity with `js/bench/datasets.ts`, `unsupported` cells where a library lacks the operation, matched-row counts cross-checked and any disagreement recorded in `config.countWarnings`) is documented in `run.py`, `worker.py` and `adapters.py`. Configuration variables:

| Var | Default | Meaning |
| --- | --- | --- |
| `BENCH_SIZE` | 1,048,576 | rows (value shared with the Rust and JS suites) |
| `BENCH_DIM` | unset | optional cube shorthand, `D³` rows; ignored if `BENCH_SIZE` is set |
| `BENCH_GRAPHS_QUADS` | 8 | named graphs the comparative bench asks for |
| `MUT_BATCH` | 10000 | quads per add/delete batch |
| `BENCH_PYTHON` | 3.13 | Python version the per-library virtualenvs are provisioned with |
| `BENCH_SUBJ_RATIO` / `BENCH_OBJ_RATIO` | 0.1 / 0.5 | distinct subjects / objects per row |
| `BENCH_PREDICATES` | 32 | distinct predicates |
| `BENCH_GRAPHS` | 1 | distinct named graphs in the generator; 1 means default graph only |
| `BENCH_LITERAL_FRAC` | 0.4 | fraction of objects that are literals |
| `BENCH_SLOW_PHASE_MS` | 30000 | a phase slower than this runs once, without warmup |
| `PY_BENCH_QUERY_ITERS` / `PY_BENCH_QUERY_WARMUP` | 10 / 5 | measured / warmup iterations per query |
| `PY_BENCH_HEAVY_ITERS` / `PY_BENCH_FULL_SCAN_ITERS` | 3 / 3 | iterations for the heavy and full-scan phases |
| `CODSPEED_BENCH_DIM` / `CODSPEED_BENCH_DIM_QUADS` | 32 / 13 | CodSpeed suite: `D³` triples / `D⁴` quads |
