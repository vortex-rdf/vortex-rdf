"""Measure ONE adapter, in its own process and its own virtualenv.

Run by `run.py`, never directly in a full run. One adapter per process buys
two things: peak RSS is attributable to a single library, not to whichever
ran first, and no library's garbage or import-time allocation taints
another's timings. It also lets each adapter keep an incompatible dependency
set -- see the pyoxigraph pin note in `run.py`.

Results go to stdout as one JSON object; progress goes to stderr, so the
orchestrator can stream progress to the terminal while parsing the result.

The memory role (`--role memory`) measures one store's memory, and nothing
else, in a process of its own: it opens the store, reads every pattern once
under a 2 ms `/proc/self/status` sampler (the peaks, RssAnon and RssFile above
a pre-open baseline), then disposes the store and reports the live bytes that
disposing released (glibc's `mallinfo2`, with `PYTHONMALLOC=malloc` so Python's
own objects are counted too). It first checks that the counter sees an 8 MiB
block held; if it does not, the run exits non-zero, naming the counter, on
stderr.
"""

from __future__ import annotations

import argparse
import ctypes
import gc
import json
import os
import re
import statistics
import sys
import threading
import time
from typing import Any, Callable, NamedTuple, Optional

from adapters import Adapter, build_adapter
from datasets import (
    FULL_SCAN_PATTERN,
    Pat,
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

#: The time limit for query cells, the same for every library in every suite:
#: a match or count cell, warm or cold, the full scan included, whose first
#: run, warmup or sample, takes longer than this is not measured, and reads
#: "timeout" with that run's time as its tooltip; ``0`` disables it. Builds,
#: opens and mutations are always measured. A query that takes tens of seconds
#: per run is orders of magnitude slower than the rest of its column, and its
#: repetitions would cost minutes for one number. Same knob and default as the
#: JS worker, the Java harness and ``compare.rs``.
TIMEOUT_NS = float(os.environ.get("BENCH_TIMEOUT_MS", 30_000)) * 1e6


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
    limit_ns: float = 0.0,
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
        first = True
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
            # `limit_ns`, TIMEOUT_NS for a query cell, holds its first run.
            if first:
                first = False
                if limit_ns and elapsed > limit_ns:
                    rows.append(timeout_row(ident, elapsed, limit_ns, regime))
                    return
            if remaining_warmup > 0:
                remaining_warmup -= 1
                continue
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
            **({"regime": regime} if regime else {}),
        }
    )


def unmeasured_row(ident: str, word: str, reason: str) -> dict:
    """A row with no measurement, whose cell reads `word` rather than rendering
    as a missing cell, which reads as a benchmark nobody ran.

    `median_ns` is None so the panel excludes it from the column's best/ratio
    arithmetic. The reason rides along as the cell's tooltip, so one word
    covers each kind of absence on the page and the specifics stay one hover
    away.
    """
    group, _, variant = ident.partition("::")
    return {
        "group": group,
        "variant": variant or None,
        "id": ident,
        "reason": reason,
        "fastest": word,
        "slowest": word,
        "median": word,
        "mean": word,
        "fastest_ns": None,
        "slowest_ns": None,
        "median_ns": None,
        "mean_ns": None,
        "samples": "0",
    }


def unsupported_row(ident: str, reason: str) -> dict:
    """A row the dashboard renders as 'unsupported': an operation a library
    cannot perform is not a slow operation."""
    return unmeasured_row(ident, "unsupported", reason) | {"unsupported": True}


def timeout_row(ident: str, elapsed_ns: float, limit_ns: float, regime: Optional[str] = None) -> dict:
    """A row the dashboard renders as 'timeout': a query cell whose first run
    took `elapsed_ns`, past the time limit `limit_ns` (see TIMEOUT_NS)."""
    reason = f"not measured: one run took {fmt_ns(elapsed_ns)}, past the {limit_ns / 1e9:g} s limit"
    return (
        unmeasured_row(ident, "timeout", reason)
        | {"timeout": True}
        | ({"regime": regime} if regime else {})
    )


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


class Rss(NamedTuple):
    """Resident set size by kind, in bytes."""

    anon: int
    file: int


def read_rss() -> Rss:
    """This process's `RssAnon` and `RssFile`, from `/proc/self/status`."""
    fields: dict[str, int] = {}
    with open("/proc/self/status", encoding="utf-8") as f:
        for line in f:
            m = re.match(r"(RssAnon|RssFile):\s+(\d+)\s+kB", line)
            if m:
                fields[m.group(1)] = int(m.group(2)) * 1024
    return Rss(fields["RssAnon"], fields["RssFile"])


class RssSampler:
    """Keeps the highest `RssAnon + RssFile` seen while the block runs.

    Samples on entry, every `INTERVAL_S` on a daemon thread, and once more on
    exit. `peak` is the whole sample with the highest sum.
    """

    INTERVAL_S = 0.002

    def __init__(self) -> None:
        self.peak = Rss(0, 0)
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._error: Optional[BaseException] = None
        self._switch = sys.getswitchinterval()

    def _sample(self) -> None:
        rss = read_rss()
        if rss.anon + rss.file > self.peak.anon + self.peak.file:
            self.peak = rss

    def _run(self) -> None:
        try:
            while not self._stop.wait(self.INTERVAL_S):
                self._sample()
        except BaseException as e:  # re-raised from __exit__
            self._error = e

    def __enter__(self) -> "RssSampler":
        self._sample()
        # A short switch interval bounds how long a thread running bytecode
        # keeps the GIL once the sampler asks for it. A native call that holds
        # the GIL still blocks the sampler until it returns.
        self._switch = sys.getswitchinterval()
        sys.setswitchinterval(0.001)
        try:
            self._thread = threading.Thread(target=self._run, daemon=True)
            self._thread.start()
        except BaseException:
            sys.setswitchinterval(self._switch)
            raise
        return self

    def __exit__(self, *exc: object) -> None:
        self._stop.set()
        assert self._thread is not None
        try:
            self._thread.join()
            if self._error is None:
                self._sample()
        finally:
            sys.setswitchinterval(self._switch)
        if self._error is not None:
            raise self._error


class _Mallinfo2(ctypes.Structure):
    _fields_ = [
        (name, ctypes.c_size_t)
        for name in (
            "arena", "ordblks", "smblks", "hblks", "hblkhd",
            "usmblks", "fsmblks", "uordblks", "fordblks", "keepcost",
        )
    ]


_libc: Optional[ctypes.CDLL] = None


def _libc_handle() -> ctypes.CDLL:
    global _libc
    if _libc is None:
        libc = ctypes.CDLL(None)
        if hasattr(libc, "mallinfo2"):
            libc.mallinfo2.restype = _Mallinfo2
        _libc = libc
    return _libc


def live_bytes() -> int:
    """Bytes glibc's malloc has handed out and not yet had freed: `uordblks`
    (the arenas) plus `hblkhd` (the mmapped blocks)."""
    libc = _libc_handle()
    try:
        mallinfo2 = libc.mallinfo2
    except AttributeError:
        raise RuntimeError("this libc has no mallinfo2 (glibc 2.33+ only)") from None
    info = mallinfo2()
    return info.uordblks + info.hblkhd


#: The block the self-check holds, and the least `live_bytes` must rise by.
SELF_CHECK_BYTES = 8 << 20


def self_check(live: Callable[[], int] = live_bytes) -> None:
    """Exit non-zero, naming the counter, unless `live` sees 8 MiB held."""
    before = live()
    # 64 KiB blocks sit below glibc's mmap threshold, so freeing them leaves
    # its dynamic threshold alone.
    blocks = [bytearray(64 << 10) for _ in range(SELF_CHECK_BYTES // (64 << 10))]
    rise = live() - before
    del blocks
    # Otherwise the bindings' 64 MiB trim threshold would keep these pages in
    # the baseline.
    try:
        _libc_handle().malloc_trim(0)
    except AttributeError:
        pass
    if rise < SELF_CHECK_BYTES:
        raise SystemExit(
            f"self-check failed: mallinfo2 (uordblks + hblkhd) rose by {rise:,} bytes "
            "while the process held 8 MiB"
        )


# ─── Phases ─────────────────────────────────────────────────────────────────


def log(label: str, msg: str) -> None:
    print(f"  [{label}] {msg}", file=sys.stderr, flush=True)


def patterns(a: Adapter, args: argparse.Namespace) -> list[Pat]:
    """The triple patterns, and the graph ones when the library's model has
    graphs, that the query and memory roles both read."""
    probes = dataset_probes(args.n, dataset_opts(graphs=args.graphs))
    return list(probes["triples"]) + (list(probes["quads"]) if a.supports_quads else [])


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
    pattern_set = patterns(a, args)
    if not a.supports_quads:
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
    # The COLD regime: each iteration answers the FIRST query on a freshly
    # opened handle. The open runs in `setup`, which is outside the timed
    # region, so this isolates what a query costs against empty caches,
    # excluding the open -- which is measured on its own as `open::<slug>`
    # above.
    def open_cold():
        return a.open(artifact, src)

    def drop_cold(h, _result):
        a.dispose(h)

    def counted(name: str, query: Any) -> int:
        """`count`'s answer for `query`, checked against `count_only`'s: a
        count path that resolves differently must fail loudly here, not time
        the wrong work."""
        count = a.count(handle, query)
        n_count = a.count_only(handle, query)
        if n_count != count:
            raise RuntimeError(f"count_only disagrees for {name}: {n_count} != {count}")
        return count

    def measure_both(name: str, query: Any, iters: int, warmup: int, regime: str) -> None:
        """Time `count` and `count_only` of `query` as `<slug>::<name>` and
        `<slug>::<name>::count`: warm on the open handle, or cold. Query cells,
        so each one's first run is held to TIMEOUT_NS."""
        for suffix, fn in (("", a.count), ("::count", a.count_only)):
            ident = f"{a.slug}::{name}{suffix}"
            if regime == "warm":
                measure(
                    ident,
                    lambda _, fn=fn: fn(handle, query),
                    rows,
                    iters,
                    warmup,
                    regime="warm",
                    limit_ns=TIMEOUT_NS,
                )
            else:
                measure(
                    ident,
                    lambda h, fn=fn: fn(h, query),
                    rows,
                    iters,
                    warmup,
                    setup=open_cold,
                    teardown=drop_cold,
                    regime="cold",
                    limit_ns=TIMEOUT_NS,
                )

    # Term parsing happens once per pattern, outside every timed region.
    prepared = {pat.name: a.prepare(pat) for pat in pattern_set}
    for pat in pattern_set:
        log(a.label, f"match {pat.name}…")
        counts[pat.name] = counted(pat.name, prepared[pat.name])
        measure_both(pat.name, prepared[pat.name], QUERY_ITERS, QUERY_WARMUP, "warm")

    # --- match: the same patterns, COLD ---
    # Gated on `has_distinct_open`: for a store that lives only in memory,
    # opening IS re-parsing the source, so its cold cell would be tens of
    # seconds of ingest sitting in a microsecond column. Those adapters simply
    # have no cold rows, and the dashboard leaves the cells blank.
    if a.has_distinct_open:
        for pat in pattern_set:
            log(a.label, f"match {pat.name} (cold)…")
            measure_both(pat.name, prepared[pat.name], QUERY_ITERS, QUERY_WARMUP, "cold")

    log(a.label, "match full…")
    full = a.prepare(FULL_SCAN_PATTERN)
    counts["full"] = counted("full", full)
    measure_both("full", full, FULL_SCAN_ITERS, 0, "warm")
    if a.has_distinct_open:
        log(a.label, "match full (cold)…")
        measure_both("full", full, FULL_SCAN_ITERS, 0, "cold")
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


def run_memory(adapter: Adapter, args: argparse.Namespace) -> dict:
    """One store's memory: the peaks during its reads, and what disposing it
    releases. Each figure is a difference in bytes."""
    a = adapter
    src = args.quads if a.supports_quads else args.triples
    artifact = a.artifact_path(args.workdir, src)

    a.import_library()
    pats = patterns(a, args) + [FULL_SCAN_PATTERN]
    prepared = [(pat.name, a.prepare(pat)) for pat in pats]
    self_check()

    gc.collect()
    base = read_rss()
    log(a.label, "open…")
    handle = a.open(artifact, src)
    # A store built here leaves its build garbage out of the read peak.
    gc.collect()

    counts: dict[str, int] = {}
    with RssSampler() as sampler:
        for name, query in prepared:
            log(a.label, f"read {name}…")
            counts[name] = a.count(handle, query)

    gc.collect()
    before = live_bytes()
    a.dispose(handle)
    del handle
    gc.collect()
    after = live_bytes()

    return {
        "obtained": a.obtained,
        "counts": counts,
        "storeBytes": before - after,
        "peakAnonBytes": sampler.peak.anon - base.anon,
        "peakFileBytes": sampler.peak.file - base.file,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--slug", required=True)
    ap.add_argument("--role", choices=["query", "mutate", "memory"], default="query")
    ap.add_argument("--triples", required=True)
    ap.add_argument("--quads", required=True)
    ap.add_argument("--workdir", required=True)
    ap.add_argument("--n", type=int, required=True)
    ap.add_argument("--graphs", type=int, default=8)
    ap.add_argument("--mut-batch", type=int, default=10_000)
    args = ap.parse_args()

    if args.role == "memory" and os.environ.get("PYTHONMALLOC") != "malloc":
        sys.exit(
            "the memory role needs PYTHONMALLOC=malloc (run.py sets it), "
            "so that mallinfo2 counts Python objects too"
        )
    adapter = build_adapter(args.slug)
    run = {"query": run_query, "mutate": run_mutate, "memory": run_memory}[args.role]
    result = run(adapter, args)
    result.update({"slug": adapter.slug, "label": adapter.label, "role": args.role})
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    sys.exit(main())
