# Python benchmarks

`run.py` measures the bindings against [pyoxigraph](https://pypi.org/project/pyoxigraph/), [pycottas](https://pypi.org/project/pycottas/), [rdflib](https://pypi.org/project/rdflib/) and [lightrdf](https://pypi.org/project/lightrdf/) on a file → store → query workload and writes `target/bench/python.json` at the repository root for [the dashboard's Python tab](https://vortex-rdf.github.io/vortex-rdf/#py); `test_codspeed.py` is the instrumented suite CodSpeed runs. `scripts/refresh.sh` runs every suite and renders the dashboard (`--only python,render` re-measures this one); it needs GNU coreutils (`sha256sum`, `sort -z`).

```bash
python3 python/bench/run.py                 # full run, from the repository root
BENCH_DIM=32 python3 python/bench/run.py    # quick pilot
cd python && uv run pytest bench/test_codspeed.py --codspeed
```

Harness design (per-library virtualenvs, dataset parity with `js/bench/datasets.ts`, `unsupported` cells where a library lacks the operation, matched-row counts cross-checked and any disagreement recorded in `config.countWarnings`) is documented in `run.py`, `worker.py` and `adapters.py`; `suite.py` holds the orchestration this suite shares with the Java one (`java/bench/run.py`).

The knobs this suite shares with the JS and Rust ones (`BENCH_SIZE`, `BENCH_DIM`, `BENCH_GRAPHS_QUADS`, `MUT_BATCH`, `BENCH_SLOW_PHASE_MS`, the dataset shape and the CodSpeed sizes) are in [the shared table](../../js/bench/README.md#shared-knobs), with the suites that read each. This suite's own:

| Var | Default | Meaning |
| --- | --- | --- |
| `BENCH_PYTHON` | 3.13 | Python version the per-library virtualenvs are provisioned with |
| `PY_BENCH_QUERY_ITERS` / `PY_BENCH_QUERY_WARMUP` | 10 / 5 | measured / warmup iterations per query |
| `PY_BENCH_HEAVY_ITERS` / `PY_BENCH_FULL_SCAN_ITERS` | 3 / 3 | iterations for the heavy and full-scan phases |
