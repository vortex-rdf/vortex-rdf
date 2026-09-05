"""Measure ONE adapter, in its own process and its own virtualenv.

Run by `run.py`, never directly in a full run. One adapter per process buys
two things: peak RSS is attributable to a single library, not to whichever
ran first, and no library's garbage or import-time allocation taints
another's timings. It also lets each adapter keep an incompatible dependency
set -- see the pyoxigraph pin note in `run.py`.

Results go to stdout as one JSON object; progress goes to stderr, so the
orchestrator can stream progress to the terminal while parsing the result.

One role per process: `query` (build, open, every pattern in both cache
regimes, the full scan), `arrow` (the same probes read back as Arrow columns,
for the libraries that export any) and `mutate`.
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
from typing import Any, Callable, Optional

from adapters import Adapter, build_adapter
from datasets import (
    FULL_SCAN_PATTERN,
    dataset_opts,
    dataset_prefix,
    dataset_probes,
    fresh_quads,
)

# ─── Timing ─────────────────────────────────────────────────────────────────
#
# Iteration counts mirror js/bench/shared.ts: a selective query is cheap enough
# to repeat, while a build or a full-table materialization is not. Warmup runs
# are discarded, not averaged in.

QUERY_ITERS = int(os.environ.get("PY_BENCH_QUERY_ITERS", 10))
QUERY_WARMUP = int(os.environ.get("PY_BENCH_QUERY_WARMUP", 5))
HEAVY_ITERS = int(os.environ.get("PY_BENCH_HEAVY_ITERS", 3))
FULL_SCAN_ITERS = int(os.environ.get("PY_BENCH_FULL_SCAN_ITERS", 3))

#: A phase whose single execution already exceeds this stops after one sample
#: (and skips any remaining warmup); ``0`` disables the rule. Repetition exists
#: to average out noise that is small relative to the reading, which a
#: multi-ten-second phase does not need. The reduced count is reported through
#: the row's ``samples`` field, which the dashboard displays. Same knob and
#: default as the JS worker and ``compare.rs``.
SLOW_PHASE_NS = float(os.environ.get("BENCH_SLOW_PHASE_MS", 30_000)) * 1e6


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
    fn: Callable[[Any], object],
    rows: list,
    iters: int,
    warmup: int = 0,
    setup: Optional[Callable[[], Any]] = None,
    teardown: Optional[Callable[[Any, object], None]] = None,
    regime: Optional[str] = None,
) -> None:
    """Time `fn` and append one dashboard-shaped row.

    `setup`/`teardown` run outside the timed region. `setup` returns the
    iteration's state (`None` when absent), `fn` receives it, and `teardown`
    receives the state and `fn`'s result -- a build phase must dispose the
    store it just made before the next iteration, or the process accumulates
    one full store per iteration and the peak RSS attributed to this adapter
    becomes a multiple of what it actually needs.

    Everything already alive is frozen (`gc.freeze`) for the duration, so the
    per-iteration `gc.collect()` walks only what an iteration itself created
    instead of the interpreter's whole object graph. `gc.unfreeze` runs
    afterwards: leaving objects frozen would exempt later cyclic garbage for
    the rest of the process.
    """
    samples: list[float] = []
    gc.collect()
    gc.freeze()
    try:
        remaining_warmup = warmup
        while len(samples) < iters:
            state = setup() if setup else None
            # Keeps an automatic collection from firing inside a timed call.
            # Cheap now: the frozen objects are out of the collector's reach.
            gc.collect()
            t0 = time.perf_counter_ns()
            out = fn(state)
            elapsed = time.perf_counter_ns() - t0
            if teardown:
                teardown(state, out)
            del state, out
            if remaining_warmup > 0:
                remaining_warmup -= 1
                # A warmup run this long says the phase needs no more warming:
                # whatever caches move is noise beside a 30 s+ reading.
                if SLOW_PHASE_NS and elapsed > SLOW_PHASE_NS:
                    remaining_warmup = 0
                continue
            samples.append(float(elapsed))
            # See SLOW_PHASE_NS: one sample of a multi-ten-second phase.
            if SLOW_PHASE_NS and len(samples) == 1 and elapsed > SLOW_PHASE_NS:
                break
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
            **({"regime": regime} if regime else {}),
        }
    )


def unsupported_row(ident: str, reason: str) -> dict:
    """A row the dashboard renders as 'unsupported' rather than a missing cell.

    `median_ns` is None so the panel excludes it from the column's best/ratio
    arithmetic: an operation a library cannot perform is not a slow operation.
    The reason rides along as the cell's tooltip, so one word covers every kind
    of absence on the page and the specifics stay one hover away.
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


def log(label: str, msg: str) -> None:
    print(f"  [{label}] {msg}", file=sys.stderr, flush=True)


def run_query(adapter: Adapter, args: argparse.Namespace) -> dict:
    a = adapter
    rows: list = []
    counts: dict[str, int] = {}

    workdir = args.workdir
    # One dataset for the whole run. A library whose model has graphs builds
    # from the quads file and answers every pattern on it; one whose model does
    # not reads the triples projection of the same rows -- same subjects,
    # predicates and objects, graph dropped -- so the two still count the same
    # rows for every pattern that binds no graph.
    src = args.quads if a.supports_quads else args.triples
    artifact = a.artifact_path(workdir, src)

    # --- build (ingest) ---
    log(a.label, "build…")
    measure(
        f"ingest::{a.slug}",
        lambda _: a.build(src, artifact),
        rows,
        HEAVY_ITERS,
        teardown=lambda _, h: a.dispose(h),
    )

    artifact_bytes = a.artifact_bytes(artifact)

    # --- open ---
    # Only for a library that has an artifact to open. A store that lives in
    # memory has none: "opening" it is re-parsing the source, which is what the
    # Build column already measures -- and measuring it here again would rank a
    # whole re-parse against a footer read as though the two were the same
    # operation. The cost is real and it is Build's to report; this column stays
    # about opening an artifact.
    if a.has_distinct_open:
        log(a.label, "open…")
        measure(
            f"open::{a.slug}",
            lambda _: a.open(artifact, src),
            rows,
            QUERY_ITERS,
            warmup=QUERY_WARMUP,
            teardown=lambda _, h: a.dispose(h),
        )
    else:
        rows.append(
            unsupported_row(
                f"open::{a.slug}",
                "no artifact to reopen: a fresh process re-parses the source, "
                "which is the Build column",
            )
        )

    # --- match: triple patterns + full scan ---
    handle = a.open(artifact, src)
    probes = dataset_probes(args.n, dataset_opts(graphs=args.graphs))
    # The graph probes are part of the one pattern set now, not a second
    # dataset's; a library without graphs in its model reports them unsupported.
    pattern_set = list(probes["triples"])
    if a.supports_quads:
        pattern_set += probes["quads"]
    else:
        # Both regimes, matching the cold pass this adapter does run below, so
        # neither cell reads as a benchmark nobody bothered with.
        regimes = ["warm", "cold"] if a.has_distinct_open else ["warm"]
        for pat in probes["quads"]:
            for regime in regimes:
                for ident in (f"{a.slug}::{pat.name}", f"{a.slug}::{pat.name}::count"):
                    rows.append(
                        unsupported_row(
                            ident,
                            "this library's model has no graphs -- it reads the triples projection",
                        )
                        | {"regime": regime}
                    )
    # Term parsing happens once per pattern, outside every timed region.
    prepared = {pat.name: a.prepare(pat) for pat in pattern_set}
    for pat in pattern_set:
        log(a.label, f"match {pat.name}…")
        query = prepared[pat.name]
        counts[pat.name] = a.count(handle, query)
        # A count path that resolves differently must fail loudly here, not
        # time the wrong work below.
        n_count = a.count_only(handle, query)
        if n_count != counts[pat.name]:
            raise RuntimeError(
                f"count_only disagrees for {pat.name}: {n_count} != {counts[pat.name]}"
            )
        measure(
            f"{a.slug}::{pat.name}",
            lambda _, q=query: a.count(handle, q),
            rows,
            QUERY_ITERS,
            QUERY_WARMUP,
            regime="warm",
        )
        measure(
            f"{a.slug}::{pat.name}::count",
            lambda _, q=query: a.count_only(handle, q),
            rows,
            QUERY_ITERS,
            QUERY_WARMUP,
            regime="warm",
        )

    # --- match: the same patterns, COLD ---
    # Each iteration answers the FIRST query on a freshly opened handle. The
    # open runs in `setup`, which is outside the timed region, so this isolates
    # what a query costs against empty caches, excluding the open -- which is
    # measured on its own as `open::<slug>` above.
    #
    # Gated on `has_distinct_open`: for a store that lives only in memory,
    # opening IS re-parsing the source, so its cold cell would be tens of
    # seconds of ingest sitting in a microsecond column. Those adapters simply
    # have no cold rows, and the dashboard leaves the cells blank.
    def open_cold():
        return a.open(artifact, src)

    def drop_cold(h, _result):
        a.dispose(h)

    if a.has_distinct_open:
        for pat in pattern_set:
            log(a.label, f"match {pat.name} (cold)…")
            query = prepared[pat.name]
            measure(
                f"{a.slug}::{pat.name}",
                lambda h, q=query: a.count(h, q),
                rows,
                QUERY_ITERS,
                QUERY_WARMUP,
                setup=open_cold,
                teardown=drop_cold,
                regime="cold",
            )
            measure(
                f"{a.slug}::{pat.name}::count",
                lambda h, q=query: a.count_only(h, q),
                rows,
                QUERY_ITERS,
                QUERY_WARMUP,
                setup=open_cold,
                teardown=drop_cold,
                regime="cold",
            )

    log(a.label, "match full…")
    full = a.prepare(FULL_SCAN_PATTERN)
    counts["full"] = a.count(handle, full)
    n_count = a.count_only(handle, full)
    if n_count != counts["full"]:
        raise RuntimeError(f"count_only disagrees for full: {n_count} != {counts['full']}")
    measure(f"{a.slug}::full", lambda _: a.count(handle, full), rows, FULL_SCAN_ITERS, regime="warm")
    measure(
        f"{a.slug}::full::count",
        lambda _: a.count_only(handle, full),
        rows,
        FULL_SCAN_ITERS,
        regime="warm",
    )

    if a.has_distinct_open:
        log(a.label, "match full (cold)…")
        measure(
            f"{a.slug}::full",
            lambda h: a.count(h, full),
            rows,
            FULL_SCAN_ITERS,
            setup=open_cold,
            teardown=drop_cold,
            regime="cold",
        )
        measure(
            f"{a.slug}::full::count",
            lambda h: a.count_only(h, full),
            rows,
            FULL_SCAN_ITERS,
            setup=open_cold,
            teardown=drop_cold,
            regime="cold",
        )
    a.dispose(handle)
    del handle
    gc.collect()

    return {
        "rows": rows,
        "counts": counts,
        # Reported here because these are env-tunable: the only figure that is
        # certainly right is the one the process that did the timing used. The
        # dashboard shows them next to the numbers.
        "iters": {
            "query": QUERY_ITERS,
            "queryWarmup": QUERY_WARMUP,
            "heavy": HEAVY_ITERS,
            "fullScan": FULL_SCAN_ITERS,
        },
        "artifact_bytes": artifact_bytes,
        "peak_rss_mb": peak_rss_mb(),
    }


def run_arrow(adapter: Adapter, args: argparse.Namespace) -> dict:
    """The Arrow read of the same probes, in a process of its own.

    Two encodings against one baseline: ``::arrow`` reads all four term values
    of every matched row out of ``string_view`` columns -- the same values
    ``::<pattern>`` reads out of quads, so the pair prices the same delivered
    data two ways -- and ``::arrow_codes`` reads the ``uint32`` code columns'
    buffers, decoding no term at all.

    Its own process because importing pyarrow costs tens of megabytes of RSS,
    and the query role's reading is this library's peak-memory figure on the
    dashboard. Only an adapter with ``supports_arrow`` is ever run here.
    """
    a = adapter
    rows: list = []
    if not a.supports_arrow:
        return {"rows": rows, "counts": {}, "artifact_bytes": None, "peak_rss_mb": None}

    src = args.quads if a.supports_quads else args.triples
    artifact = a.artifact_path(args.workdir, src)
    if not os.path.exists(artifact):
        a.dispose(a.build(src, artifact))
    handle = a.open(artifact, src)

    probes = dataset_probes(args.n, dataset_opts(graphs=args.graphs))
    pattern_set = list(probes["triples"])
    if a.supports_quads:
        pattern_set += probes["quads"]
    pattern_set.append(FULL_SCAN_PATTERN)

    for pat in pattern_set:
        log(a.label, f"arrow {pat.name}…")
        query = a.prepare(pat)
        # The same agreement check the count paths get: an export that resolved
        # something else would time the wrong work rather than fail.
        want = a.count_only(handle, query)
        for encoding, suffix in (("strings", "arrow"), ("codes", "arrow_codes")):
            got = a.arrow_count(handle, query, encoding)
            if got != want:
                raise RuntimeError(
                    f"arrow {encoding} disagrees for {pat.name}: {got} != {want}"
                )
            measure(
                f"{a.slug}::{pat.name}::{suffix}",
                lambda _, q=query, e=encoding: a.arrow_count(handle, q, e),
                rows,
                QUERY_ITERS,
                QUERY_WARMUP,
                regime="warm",
            )

    a.dispose(handle)
    del handle
    gc.collect()
    return {"rows": rows, "counts": {}, "artifact_bytes": None, "peak_rss_mb": None}


def run_mutate(adapter: Adapter, args: argparse.Namespace) -> dict:
    a = adapter
    rows: list = []

    if a.mutation_unsupported:
        reason = a.mutation_unsupported
        rows.append(unsupported_row(f"add::{a.slug}", reason))
        rows.append(unsupported_row(f"delete::{a.slug}", reason))
        # No memory reading: this process did nothing but start an interpreter,
        # so its peak RSS would be a bare-interpreter figure masquerading as a
        # mutation-workload measurement in the memory panel.
        return {"rows": rows, "counts": {}, "artifact_bytes": None, "peak_rss_mb": None}

    src = args.quads if a.supports_quads else args.triples
    fresh = fresh_quads(args.mut_batch)
    del_slice = dataset_prefix(args.n, args.mut_batch, dataset_opts(graphs=args.graphs))

    # add: into an empty store, so the measurement is the insert path and not a
    # lookup against however many rows the dataset happens to hold.
    log(a.label, f"add ({args.mut_batch})…")
    empty_src = os.path.join(args.workdir, "empty.nt")
    if not os.path.exists(empty_src):
        open(empty_src, "w").close()
    measure(
        f"add::{a.slug}",
        lambda h: a.add(h, fresh),
        rows,
        HEAVY_ITERS,
        setup=lambda: a.build(empty_src, a.artifact_path(args.workdir, empty_src)),
        teardown=lambda h, _: a.dispose(h),
    )

    # delete: from a store that actually contains the batch.
    log(a.label, f"delete ({args.mut_batch})…")
    measure(
        f"delete::{a.slug}",
        lambda h: a.delete(h, del_slice),
        rows,
        HEAVY_ITERS,
        setup=lambda: a.build(src, a.artifact_path(args.workdir, src)),
        teardown=lambda h, _: a.dispose(h),
    )

    return {"rows": rows, "counts": {}, "artifact_bytes": None, "peak_rss_mb": peak_rss_mb()}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--slug", required=True)
    ap.add_argument("--role", choices=["query", "arrow", "mutate"], default="query")
    ap.add_argument("--triples", required=True)
    ap.add_argument("--quads", required=True)
    ap.add_argument("--workdir", required=True)
    ap.add_argument("--n", type=int, required=True)
    ap.add_argument("--graphs", type=int, default=8)
    ap.add_argument("--mut-batch", type=int, default=10_000)
    args = ap.parse_args()

    adapter = build_adapter(args.slug)
    roles = {"query": run_query, "arrow": run_arrow, "mutate": run_mutate}
    result = roles[args.role](adapter, args)
    result.update({"slug": adapter.slug, "label": adapter.label, "role": args.role})
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    sys.exit(main())
