"""Measure ONE adapter, in its own process and its own virtualenv.

Run by `run.py`, never directly in a full run. One adapter per process buys
two things: peak RSS is attributable to a single library rather than to
whichever ran first, and no library's garbage or import-time allocation taints
another's timings. It also lets each adapter keep an incompatible dependency
set -- see the pyoxigraph pin note in `adapters.py`.

Results go to stdout as one JSON object; progress goes to stderr, so the
orchestrator can stream progress to the terminal while parsing the result.
"""

from __future__ import annotations

import argparse
import gc
import json
import os
import re
import statistics
import sys
import time
from typing import Callable, Optional

from adapters import build_adapter
from datasets import DatasetOpts, dataset_prefix, dataset_probes, fresh_quads

# ─── Timing ─────────────────────────────────────────────────────────────────
#
# Iteration counts mirror js/bench/shared.ts: a selective query is cheap enough
# to repeat, while a build or a full-table materialization is not. Warmup runs
# are discarded, not averaged in.

QUERY_ITERS = int(os.environ.get("PY_BENCH_QUERY_ITERS", 10))
QUERY_WARMUP = int(os.environ.get("PY_BENCH_QUERY_WARMUP", 5))
HEAVY_ITERS = int(os.environ.get("PY_BENCH_HEAVY_ITERS", 3))
FULL_SCAN_ITERS = int(os.environ.get("PY_BENCH_FULL_SCAN_ITERS", 3))


def fmt_ns(ns: float) -> str:
    """Match the JS harness's formatting so both tabs read identically."""
    if ns < 1_000:
        return f"{ns:.4g} ns"
    if ns < 1_000_000:
        return f"{ns / 1_000:.4g} µs"
    if ns < 1_000_000_000:
        return f"{ns / 1_000_000:.4g} ms"
    return f"{ns / 1_000_000_000:.4g} s"


def measure(
    ident: str,
    fn: Callable[[], object],
    rows: list,
    iters: int,
    warmup: int = 0,
    setup: Optional[Callable[[], None]] = None,
    teardown: Optional[Callable[[object], None]] = None,
) -> None:
    """Time `fn` and append one dashboard-shaped row.

    `setup`/`teardown` run outside the timed region -- a build phase must
    dispose the store it just made before the next iteration, or the process
    accumulates one full store per iteration and the peak RSS attributed to
    this adapter becomes a multiple of what it actually needs.

    Everything already alive is frozen into the permanent generation before
    the loop, so the per-iteration collection walks only what an iteration
    itself created. A full collection traverses the interpreter's entire
    object graph; that traversal evicts the CPU caches and leaves the next
    timed call running cold, a near-constant per-sample cost that swamps a
    microsecond-scale query and, being constant, compresses the ratios
    between fast and slow adapters. Freezing is scoped to this measurement:
    objects the collector ignores are still reclaimed by reference counting,
    but leaving them frozen would exempt later cyclic garbage for the rest of
    the process.
    """
    samples: list[float] = []
    gc.collect()
    gc.freeze()
    try:
        for i in range(warmup + iters):
            if setup:
                setup()
            # Keeps an automatic collection from firing inside a timed call.
            # Cheap now: the frozen objects are out of the collector's reach.
            gc.collect()
            t0 = time.perf_counter_ns()
            out = fn()
            elapsed = time.perf_counter_ns() - t0
            if teardown:
                teardown(out)
            else:
                del out
            if i >= warmup:
                samples.append(float(elapsed))
    finally:
        gc.unfreeze()

    samples.sort()
    median = statistics.median(samples)
    mean = statistics.fmean(samples)
    group, _, variant = ident.partition("::")
    rows.append(
        {
            "group": group,
            "variant": variant or None,
            "id": ident,
            "fastest": fmt_ns(samples[0]),
            "slowest": fmt_ns(samples[-1]),
            "median": fmt_ns(median),
            "mean": fmt_ns(mean),
            "fastest_ns": samples[0],
            "slowest_ns": samples[-1],
            "median_ns": median,
            "mean_ns": mean,
            "samples": str(len(samples)),
        }
    )


def unsupported_row(ident: str, reason: str) -> dict:
    """A row the dashboard renders as 'unsupported' rather than a missing cell.

    `median_ns` is None so the panel excludes it from the column's best/ratio
    arithmetic: an operation a library cannot perform is not a slow operation.
    """
    group, _, variant = ident.partition("::")
    return {
        "group": group,
        "variant": variant or None,
        "id": ident,
        "unsupported": True,
        "reason": reason,
        "fastest": "unsupported",
        "slowest": "unsupported",
        "median": "unsupported",
        "mean": "unsupported",
        "fastest_ns": None,
        "slowest_ns": None,
        "median_ns": None,
        "mean_ns": None,
        "samples": "0",
    }


# ─── Memory ─────────────────────────────────────────────────────────────────


def peak_rss_mb() -> Optional[int]:
    """The kernel-tracked high-water mark for this process's whole lifetime.

    VmHWM, not a sampled VmRSS: a point-in-time reading misses the spike a
    build transient causes between samples, which for several of these
    libraries is the largest number in the run.
    """
    try:
        with open("/proc/self/status", encoding="utf-8") as f:
            m = re.search(r"^VmHWM:\s+(\d+)\s+kB", f.read(), re.MULTILINE)
        return round(int(m.group(1)) / 1024) if m else None
    except OSError:
        return None


# ─── Phases ─────────────────────────────────────────────────────────────────


def run_query(a, args) -> dict:
    rows: list = []
    counts: dict[str, int] = {}
    log = lambda msg: print(f"  [{a.label}] {msg}", file=sys.stderr, flush=True)

    workdir = args.workdir
    artifact = a.artifact_path(workdir, args.triples)

    # --- build (ingest) ---
    log("build…")
    handle_box: list = []

    def do_build():
        h = a.build(args.triples, artifact)
        handle_box.append(h)
        return h

    measure(
        f"ingest::{a.slug}",
        do_build,
        rows,
        HEAVY_ITERS,
        teardown=lambda h: (a.dispose(h), handle_box.clear()),
    )

    artifact_bytes = a.artifact_bytes(artifact)

    # --- open ---
    # For an in-memory store this is a full re-parse of the source file, which
    # is the honest cost of getting that library queryable in a fresh process.
    log("open…")
    measure(
        f"open::{a.slug}",
        lambda: a.open(artifact, args.triples),
        rows,
        HEAVY_ITERS if not a.has_distinct_open else QUERY_ITERS,
        warmup=0 if not a.has_distinct_open else QUERY_WARMUP,
        teardown=lambda h: a.dispose(h),
    )

    # --- match: triple patterns + full scan ---
    handle = a.open(artifact, args.triples)
    probes = dataset_probes(args.n_triples)
    for pat in probes["triples"]:
        log(f"match {pat.name}…")
        counts[pat.name] = a.count(handle, pat)
        measure(f"{a.slug}::{pat.name}", lambda p=pat: a.count(handle, p), rows, QUERY_ITERS, QUERY_WARMUP)

    log("match full…")
    full = probes["full"][0]
    counts["full"] = a.count(handle, full)
    measure(f"{a.slug}::full", lambda: a.count(handle, full), rows, FULL_SCAN_ITERS)
    a.dispose(handle)
    del handle
    gc.collect()

    # --- match: quad patterns, over the separately built quads dataset ---
    quad_probes = dataset_probes(args.n_quads, DatasetOpts(graphs=args.graphs))["quads"]
    if not a.supports_quads:
        for pat in quad_probes:
            rows.append(
                unsupported_row(f"{a.slug}::{pat.name}", "no named-graph argument in the query API")
            )
    else:
        log("build (quads)…")
        qh = a.build(args.quads, a.artifact_path(workdir, args.quads))
        for pat in quad_probes:
            log(f"match {pat.name}…")
            counts[pat.name] = a.count(qh, pat)
            measure(f"{a.slug}::{pat.name}", lambda p=pat: a.count(qh, p), rows, QUERY_ITERS, QUERY_WARMUP)
        a.dispose(qh)
        del qh
        gc.collect()

    return {
        "rows": rows,
        "counts": counts,
        "artifact_bytes": artifact_bytes,
        "peak_rss_mb": peak_rss_mb(),
    }


def run_mutate(a, args) -> dict:
    rows: list = []
    log = lambda msg: print(f"  [{a.label}] {msg}", file=sys.stderr, flush=True)

    if not a.supports_mutation:
        reason = {
            "vortex": "the Python bindings expose no add/delete API",
            "pycottas": "COTTASStore raises: 'The COTTAS store is read only!'",
            "lightrdf": "a streaming parser, with no store to mutate",
        }.get(a.slug.split("_")[0], "not supported by this library")
        rows.append(unsupported_row(f"add::{a.slug}", reason))
        rows.append(unsupported_row(f"delete::{a.slug}", reason))
        # No memory reading: this process did nothing but start an interpreter,
        # so its peak RSS would be a bare-interpreter figure masquerading as a
        # mutation-workload measurement in the memory panel.
        return {"rows": rows, "counts": {}, "artifact_bytes": None, "peak_rss_mb": None}

    fresh = fresh_quads(args.mut_batch)
    del_slice = dataset_prefix(args.n_triples, args.mut_batch)

    # add: into an empty store, so the measurement is the insert path and not a
    # lookup against however many rows the dataset happens to hold.
    log(f"add ({args.mut_batch})…")
    empty_src = os.path.join(args.workdir, "empty.nt")
    if not os.path.exists(empty_src):
        open(empty_src, "w").close()
    holder: list = []

    def setup_empty():
        holder.clear()
        holder.append(a.build(empty_src, a.artifact_path(args.workdir, empty_src)))

    measure(
        f"add::{a.slug}",
        lambda: a.add(holder[0], fresh),
        rows,
        HEAVY_ITERS,
        setup=setup_empty,
        teardown=lambda _: (a.dispose(holder[0]), holder.clear()),
    )

    # delete: from a store that actually contains the batch.
    log(f"delete ({args.mut_batch})…")
    dholder: list = []

    def setup_loaded():
        dholder.clear()
        dholder.append(a.build(args.triples, a.artifact_path(args.workdir, args.triples)))

    measure(
        f"delete::{a.slug}",
        lambda: a.delete(dholder[0], del_slice),
        rows,
        HEAVY_ITERS,
        setup=setup_loaded,
        teardown=lambda _: (a.dispose(dholder[0]), dholder.clear()),
    )

    return {"rows": rows, "counts": {}, "artifact_bytes": None, "peak_rss_mb": peak_rss_mb()}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--slug", required=True)
    ap.add_argument("--role", choices=["query", "mutate"], default="query")
    ap.add_argument("--triples", required=True)
    ap.add_argument("--quads", required=True)
    ap.add_argument("--workdir", required=True)
    ap.add_argument("--n-triples", type=int, required=True)
    ap.add_argument("--n-quads", type=int, required=True)
    ap.add_argument("--graphs", type=int, default=8)
    ap.add_argument("--mut-batch", type=int, default=10_000)
    args = ap.parse_args()

    a = build_adapter(args.slug)
    result = run_query(a, args) if args.role == "query" else run_mutate(a, args)
    result.update({"slug": a.slug, "label": a.label, "role": args.role})
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    sys.exit(main())
