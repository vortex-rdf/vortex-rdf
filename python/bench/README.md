# Python benchmarks

`run.py` measures the bindings against [pyoxigraph](https://pypi.org/project/pyoxigraph/), [pycottas](https://pypi.org/project/pycottas/) and [rdflib](https://pypi.org/project/rdflib/) on a file → store → query workload and writes `target/bench/python.json` at the repository root for [the dashboard's Python tab](https://vortex-rdf.github.io/vortex-rdf/#py); `test_codspeed.py` is the instrumented suite CodSpeed runs. `scripts/refresh.sh` runs every suite and renders the dashboard (`--only python,render` re-measures this one); it needs GNU coreutils (`sha256sum`, `sort -z`).

```bash
python3 python/bench/run.py                 # full run, from the repository root
BENCH_DIM=32 python3 python/bench/run.py    # quick pilot
cd python && uv run pytest bench/test_codspeed.py --codspeed
```

Harness design (per-library virtualenvs, dataset parity with `js/bench/datasets.ts`, `unsupported` cells where a library lacks the operation, matched-row counts cross-checked and any disagreement recorded in `config.countWarnings`) is documented in `run.py`, `worker.py` and `adapters.py`; `suite.py` holds the orchestration this suite shares with the Java one (`java/bench/run.py`).

The knobs this suite shares with the JS and Rust ones (`BENCH_SIZE`, `BENCH_DIM`, `BENCH_GRAPHS_QUADS`, `MUT_BATCH`, `BENCH_TIMEOUT_MS`, the dataset shape and the CodSpeed sizes) are in [the shared table](../../js/bench/README.md#shared-knobs), with the suites that read each. This suite's own:

| Var | Default | Meaning |
| --- | --- | --- |
| `BENCH_PYTHON` | 3.13 | Python version the per-library virtualenvs are provisioned with |
| `PY_BENCH_QUERY_ITERS` / `PY_BENCH_QUERY_WARMUP` | 10 / 5 | measured / warmup iterations per query |
| `PY_BENCH_HEAVY_ITERS` / `PY_BENCH_FULL_SCAN_ITERS` | 3 / 3 | iterations for the heavy and full-scan phases |

## Memory runs

Each adapter's memory role runs in a fresh process of its own, `BENCH_MEMORY_RUNS` times (default 3; the shared section in [the JS README](../../js/bench/README.md#memory-runs) describes the panels). A run opens the store, reads every probe pattern once, then disposes the store, and reports:

- **Store:** the live bytes released by disposing it, read after the reads and again after the dispose;
- **two peaks:** `RssAnon` and `RssFile`, the sample with the highest sum out of a 2 ms `/proc/self/status` sampler during the reads, minus a baseline taken before the store was opened.

The worker runs with `PYTHONMALLOC=malloc` (`run.py` sets it for this role only) and counts glibc's `mallinfo2` (`uordblks + hblkhd`), so Python's own objects are counted too: rdflib then carries glibc's per-allocation overhead rather than pymalloc's, and `keep_freed_heap` stays on. Before opening anything, the run holds 8 MiB and checks the counter saw it; on a host without glibc's `mallinfo2` it exits non-zero, naming the counter on stderr, and the suite goes on with that adapter's runs empty.

The store has no `close()`, so the dispose drops the reference and collects. The Vortex read goes through `iter_quads`, a chunk at a time; pycottas's `search()` collects its whole result, so its read peak includes that list.
