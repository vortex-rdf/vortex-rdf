"""The orchestration the comparative binding suites share.

`python/bench/run.py` (the Python tab) and `java/bench/run.py` (the Java tab)
each bring their own workers and provenance line; this module holds the rest:
the scale knobs, the one dataset every library builds from, the loop that runs
every adapter's query role, then its mutate role, then its memory runs, the
cross-library count check, and the dashboard-shaped JSON `scripts/render_bench_dashboard.py` reads.
"""

from __future__ import annotations

import json
import os
import platform
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Optional, Sequence

from datasets import DatasetOpts, Moduli, dataset_opts, moduli, write_dataset


def log(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)


# ─── Knobs ──────────────────────────────────────────────────────────────────


def bench_rows() -> int:
    """Scale, as a row count: BENCH_SIZE -- the same env name every suite reads
    (the Rust benches included), so one knob sets the whole dashboard's scale.
    BENCH_DIM remains as cube shorthand for quick pilots (BENCH_DIM=16 -> 4,096
    rows) and loses to an explicit row count. Default is the indicative-overview
    scale, 2**20."""
    dim = int(os.environ.get("BENCH_DIM", 0))
    return int(os.environ.get("BENCH_SIZE", 0)) or (dim**3 if dim else 1_048_576)


def bench_graphs() -> int:
    """The named graphs the quad dataset asks for, before the generator's
    coprimality nudge."""
    return int(os.environ.get("BENCH_GRAPHS_QUADS", 8))


def mut_batch() -> int:
    """Quads per add/delete batch."""
    return int(os.environ.get("MUT_BATCH", 10_000))


def bench_memory_runs() -> int:
    """Memory runs per adapter: BENCH_MEMORY_RUNS, default 3. 0 runs none."""
    return int(os.environ.get("BENCH_MEMORY_RUNS", 3))


# ─── Machine stamps ─────────────────────────────────────────────────────────


def meminfo_mb() -> dict[str, int]:
    """Total and available system memory at the start of the run.

    Recorded so the dashboard can compare each library's peak RSS against the
    memory the machine had free at the start: a peak close to that figure means
    the run was under swap pressure, and its timings describe this machine, not
    the library. Such cells are labelled machine-bound.
    """
    out = {}
    try:
        with open("/proc/meminfo", encoding="utf-8") as f:
            for line in f:
                m = re.match(r"^(MemTotal|MemAvailable):\s+(\d+) kB", line)
                if m:
                    out[m.group(1)] = round(int(m.group(2)) / 1024)
    except OSError:
        pass
    return out


def cpu_model() -> str:
    try:
        with open("/proc/cpuinfo", encoding="utf-8") as f:
            for line in f:
                if line.lower().startswith("model name"):
                    return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return platform.processor() or "unknown CPU"


def measured_now() -> str:
    """UTC, to the minute — the format every dashboard tab dates itself in (see
    `scripts/render_bench_dashboard.py`, `js/bench/compare.bench.ts`), so one
    page never shows three formats or three timezones for one run."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


# ─── The dataset ────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Dataset:
    """The run's one dataset: the quads file every library whose model has
    graphs builds from, and its triples projection for the ones without."""

    n: int
    opts: DatasetOpts
    moduli: Moduli
    key: str
    quads: Path
    triples: Path


def prepare_dataset(data_dir: Path, n: int, graphs: int) -> Dataset:
    """Write the quads file and its triples projection into `data_dir`, or
    reuse them when a file for the same key is already there."""
    opts = dataset_opts(graphs=graphs)
    qm = moduli(n, opts)
    # Keyed on the resolved cardinality as well as the row count: the env knobs
    # `dataset_opts` reads change the file's contents at the same row count.
    key = f"{n}_{qm.n_subj}x{qm.n_pred}x{qm.n_obj}x{qm.n_graph}_{opts.literal_frac:g}"
    quads = data_dir / f"quads_{key}.nq"
    triples = data_dir / f"triples_{key}.nt"
    data_dir.mkdir(parents=True, exist_ok=True)
    for path, drop in ((quads, False), (triples, True)):
        if path.exists():
            log(f"reusing dataset {path.name} ({path.stat().st_size / 1e6:.1f} MB)")
        else:
            log(f"generating {path.name} ({n:,} rows)…")
            t0 = time.perf_counter()
            write_dataset(str(path), n, opts, drop_graph=drop)
            log(f"  wrote {path.stat().st_size / 1e6:.1f} MB in {time.perf_counter() - t0:.1f}s")
    return Dataset(n, opts, qm, key, quads, triples)


# ─── Running the adapters ───────────────────────────────────────────────────


def check_count_agreement(counts_by_slug: dict[str, dict[str, int]]) -> list[str]:
    """Cross-check every adapter's matched-row counts against the others.

    This is the harness's correctness gate. Libraries with different term
    spellings are being asked the same question; if two disagree on how many
    rows a pattern matches, at least one is being queried wrongly and its
    timing measures the wrong work. Disagreements are reported into the
    dashboard's provenance.
    """
    warnings: list[str] = []
    patterns: set[str] = set()
    for c in counts_by_slug.values():
        patterns.update(c)
    for pat in sorted(patterns):
        seen: dict[int, list[str]] = {}
        for slug, c in counts_by_slug.items():
            if pat in c:
                seen.setdefault(c[pat], []).append(slug)
        if len(seen) > 1:
            detail = "; ".join(f"{n} rows: {', '.join(sorted(s))}" for n, s in sorted(seen.items()))
            warnings.append(f"pattern {pat} -- {detail}")
    return warnings


#: Each memory run's key, mapped from the worker key it is read from.
MEMORY_FIGURES: dict[str, str] = {
    "storeMb": "storeBytes",
    "peakAnonMb": "peakAnonBytes",
    "peakFileMb": "peakFileBytes",
}


def to_mb(n: Optional[int]) -> Optional[float]:
    """Bytes to MiB at one decimal. A figure that does not apply stays None; a
    negative difference (allocator noise) reads 0."""
    return None if n is None else round(max(n, 0) / 2**20, 1)


@dataclass
class SuiteRun:
    """What the workers reported, aggregated in dashboard row order."""

    rows: list = field(default_factory=list)
    memory: list = field(default_factory=list)
    sizes: list = field(default_factory=list)
    counts_by_slug: dict = field(default_factory=dict)
    labels: dict = field(default_factory=dict)
    iters: dict = field(default_factory=dict)
    failures: list = field(default_factory=list)


def run_roles(
    slugs: Sequence[str], run_worker: Callable[[str, str], Optional[dict]], *, memory_runs: int = 0
) -> SuiteRun:
    """Run every adapter's query role, then its mutate role, then its memory runs.

    `run_worker(slug, role)` returns the worker's result, or None when the
    worker failed: one library failing must not discard the rest of the run.
    A query result's `memory_extra` joins that phase's memory row.

    Each adapter's memory role runs up to `memory_runs` times, and only when
    its query role reported counts (otherwise its artifact is missing or
    stale). The runs end at the adapter's first failed run, or at its first run
    whose counts differ from its query role's: that run is dropped and reported
    in `failures`. Every adapter gets a `memory` entry, even with no runs, so
    its dashboard row reads "—" rather than vanishing. With `memory_runs` 0 the
    phase is skipped altogether and adds no entries.
    """
    run = SuiteRun()
    for slug in slugs:
        res = run_worker(slug, "query")
        if not res:
            continue
        run.rows.extend(res["rows"])
        run.labels[slug] = res["label"]
        if res.get("counts"):
            run.counts_by_slug[slug] = res["counts"]
        # Every worker runs the same counts; keep the first that reports them so
        # the dashboard can state the repetition count the way the JS tab does.
        run.iters = run.iters or res.get("iters") or {}
        if res.get("peak_rss_mb") is not None:
            run.memory.append(
                {"slug": slug, "label": res["label"], "role": "query", "peakRssMb": res["peak_rss_mb"],
                 **res.get("memory_extra", {})}
            )
        run.sizes.append({"slug": slug, "label": res["label"], "bytes": res.get("artifact_bytes")})
        run.failures.extend(res.get("failures", []))

    # Every adapter, including the ones that cannot mutate: a worker that finds
    # the operation unsupported exits before building anything, so the sweep is
    # cheap, and the rows it emits are what the dashboard renders as an
    # explained `unsupported` cell rather than a blank one.
    for slug in slugs:
        res = run_worker(slug, "mutate")
        if not res:
            continue
        run.rows.extend(res["rows"])
        if res.get("peak_rss_mb") is not None:
            run.memory.append(
                {"slug": slug, "label": res["label"], "role": "mutate", "peakRssMb": res["peak_rss_mb"]}
            )
        run.failures.extend(res.get("failures", []))

    for slug in slugs if memory_runs > 0 else ():
        label = run.labels.get(slug, slug)
        expected = run.counts_by_slug.get(slug)
        runs: list = []
        obtained = None
        for _ in range(memory_runs if expected else 0):
            res = run_worker(slug, "memory")
            if not res:
                break
            if res["counts"] != expected:
                error = f"the memory run counted {res['counts']} where the query role counted {expected}"
                log(f"!! [{slug}] memory {error}")
                run.failures.append(
                    {"slug": slug, "label": label, "role": "memory", "phase": "counts", "keys": [], "error": error}
                )
                break
            obtained = obtained or res.get("obtained")
            runs.append({k: to_mb(res[src]) for k, src in MEMORY_FIGURES.items()})
        run.memory.append({"slug": slug, "label": label, "role": "memory", "obtained": obtained, "runs": runs})
    return run


# ─── The output ─────────────────────────────────────────────────────────────


def suite_output(
    run: SuiteRun,
    provenance: str,
    dataset: Dataset,
    mut_batch: int,
    mem: dict[str, int],
    warnings: list[str],
    extra_config: Optional[dict] = None,
    failures: Optional[list] = None,
) -> dict:
    """The suite's results file, in the shape `load_suite` in
    `scripts/render_bench_dashboard.py` reads. `extra_config` adds a suite's
    own config keys; `failures` is written only when the suite reports them."""
    matched: dict[str, int] = {}
    for c in run.counts_by_slug.values():
        matched.update(c)
    out = {
        "provenance": provenance,
        "results": run.rows,
        "memory": run.memory,
        "sizes": run.sizes,
        "config": {
            "triplesCount": dataset.n,
            "cardinality": dataset.moduli.__dict__,
            "matchedRows": matched,
            "mutBatch": mut_batch,
            # Same key names the JS harness emits, so the tabs state their
            # repetition counts identically.
            "queryIterations": run.iters.get("query"),
            "heavyIterations": run.iters.get("heavy"),
            "fullScanIterations": run.iters.get("fullScan"),
            "countWarnings": warnings,
            "systemMemoryMb": mem.get("MemTotal"),
            "availableMemoryMb": mem.get("MemAvailable"),
            **(extra_config or {}),
        },
    }
    if failures is not None:
        out["failures"] = failures
    return out


def write_output(dest: Path, out: dict) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(out, indent=2), encoding="utf-8")
    log(
        f"\nWrote {len(out['results'])} benchmark rows, {len(out['memory'])} memory readings, "
        f"{len(out['sizes'])} artifact sizes → {dest}"
    )
    warnings = out["config"]["countWarnings"]
    if warnings:
        log(f"NOTE: {len(warnings)} pattern count disagreement(s) recorded in config.countWarnings")
