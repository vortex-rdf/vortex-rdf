"""Orchestrate the Python comparative benchmark and emit `results.json`.

Provisions one virtualenv per library, generates the shared dataset files once,
runs each adapter in its own subprocess, and aggregates the rows into the shape
`scripts/render_bench_dashboard.py` feeds to the dashboard's Python tab.

    python3 python/bench/run.py                    # full run
    BENCH_DIM=32 python3 python/bench/run.py       # quick pilot

Isolation is per library, not just per process: pycottas hard-pins
`pyoxigraph==0.3.18`, so installing everything together would quietly bench a
pyoxigraph two minor versions behind the oxigraph the JavaScript tab compares
against. Each venv is provisioned once and reused.

The Vortex bindings are not installed into their venv -- the compiled extension
is abi3 and `python/vortex_rdf/` is a plain package directory, so putting it on
PYTHONPATH is enough and avoids a maturin build per run. That venv does carry
pyarrow, for the `arrow` role alone: the libraries that export Arrow answer the
same probes a second time through it, in a process of their own so pyarrow's
import never enters another role's peak-RSS reading.

Every adapter counts every query pattern before timing it; a disagreement
between libraries is recorded under `config.countWarnings` in `results.json`.
A phase a library lacks is emitted as an explained `unsupported` cell instead
of a missing row, so the dashboard can say why the cell is empty.
"""

from __future__ import annotations

import json
import os
import platform
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from adapters import ALL_SLUGS, ARROW_SLUGS, VENV_FOR  # noqa: E402
from datasets import dataset_opts, moduli, write_dataset  # noqa: E402

BENCH_DIR = Path(__file__).resolve().parent
REPO_ROOT = BENCH_DIR.parent.parent
VENV_ROOT = BENCH_DIR / ".venvs"
DATA_DIR = BENCH_DIR / ".data"

# Scale, as a row count: BENCH_SIZE -- the same env name every suite reads (the
# Rust benches included), so one knob sets the whole dashboard's scale. BENCH_DIM
# remains as cube shorthand for quick pilots (BENCH_DIM=16 -> 4,096 rows) and
# loses to an explicit row count. Default is the indicative-overview scale, 2**20.
_dim = int(os.environ.get("BENCH_DIM", 0))
#: One dataset for the whole run: every library that has graphs in its model
#: builds from it, and the ones that do not read its triples projection.
N_TRIPLES = int(os.environ.get("BENCH_SIZE", 0)) or (_dim**3 if _dim else 1_048_576)
GRAPHS = int(os.environ.get("BENCH_GRAPHS_QUADS", 8))
MUT_BATCH = int(os.environ.get("MUT_BATCH", 10_000))
PYTHON_VERSION = os.environ.get("BENCH_PYTHON", "3.13")

#: Package set per virtualenv. The bindings themselves ride on PYTHONPATH, so
#: the vortex venv carries only what a *consumer* of the Arrow export needs:
#: pyarrow, imported by the Arrow role alone (`worker.py`'s `run_arrow`, in its
#: own process, so its footprint never enters the memory panel).
VENV_PACKAGES = {
    "vortex": ["pyarrow"],
    "pyoxigraph": ["pyoxigraph"],
    "pycottas": ["pycottas"],
    "rdflib": ["rdflib"],
    "lightrdf": ["lightrdf"],
}


def log(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)


def provision(name: str) -> Path:
    """Create the virtualenv for `name` if absent, and make sure it holds the
    packages it is declared to hold; return its interpreter."""
    venv = VENV_ROOT / name
    python = venv / "bin" / "python"
    if not python.exists():
        log(f"provisioning venv: {name}")
        # `bin/python` is an absolute symlink to a uv-managed interpreter that
        # lives outside this tree, so a restored cache can bring the venv back
        # with that link dangling — and the check above follows symlinks, so it
        # reads as absent. Let uv replace the directory rather than refuse it.
        cmd = ["uv", "venv", str(venv), "--python", PYTHON_VERSION, "-q"]
        if venv.exists():
            cmd.append("--clear")
        subprocess.run(cmd, check=True, cwd=REPO_ROOT)
    # Unconditionally, not only on creation: a venv provisioned by an earlier
    # run predates any package added to VENV_PACKAGES since, and the check
    # above would hand it back missing one. uv resolves an already-satisfied
    # set in well under a second.
    pkgs = VENV_PACKAGES[name]
    if pkgs:
        subprocess.run(
            ["uv", "pip", "install", "--python", str(python), "-q", *pkgs], check=True, cwd=REPO_ROOT
        )
    return python


def installed_versions(python: Path, pkgs: list[str]) -> dict[str, str]:
    """Record what actually got installed -- the provenance line must state the
    versions measured, not the ones requested."""
    out = {}
    for pkg in pkgs:
        try:
            r = subprocess.run(
                [str(python), "-c", f"import importlib.metadata as m; print(m.version('{pkg}'))"],
                capture_output=True,
                text=True,
                check=True,
            )
            out[pkg] = r.stdout.strip()
        except subprocess.CalledProcessError:
            out[pkg] = "?"
    return out


def run_worker(slug: str, role: str, triples: Path, quads: Path) -> dict | None:
    venv_name = VENV_FOR[slug]
    python = provision(venv_name)
    workdir = DATA_DIR / "work" / slug
    workdir.mkdir(parents=True, exist_ok=True)

    env = dict(os.environ)
    # Every worker needs this directory (datasets/adapters/worker); the Vortex
    # adapter additionally needs the bindings package.
    path_parts = [str(BENCH_DIR)]
    if venv_name == "vortex":
        path_parts.append(str(REPO_ROOT / "python"))
    env["PYTHONPATH"] = os.pathsep.join(path_parts)

    cmd = [
        str(python), str(BENCH_DIR / "worker.py"),
        "--slug", slug, "--role", role,
        "--triples", str(triples), "--quads", str(quads),
        "--workdir", str(workdir),
        "--n", str(N_TRIPLES),
        "--graphs", str(GRAPHS), "--mut-batch", str(MUT_BATCH),
    ]
    log(f"\n[{slug}] {role}")
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, cwd=REPO_ROOT)
    sys.stderr.write(proc.stderr)
    if proc.returncode != 0:
        # One library failing must not discard the rest of the run: report it
        # and carry on, so a partial dashboard still beats no dashboard.
        log(f"!! [{slug}] {role} FAILED (exit {proc.returncode})")
        return None
    try:
        return json.loads(proc.stdout)
    except json.JSONDecodeError:
        log(f"!! [{slug}] {role} produced unparseable output: {proc.stdout[:400]}")
        return None


def check_count_agreement(counts_by_slug: dict[str, dict[str, int]]) -> list[str]:
    """Cross-check every adapter's matched-row counts against the others.

    This is the harness's correctness gate. Five libraries with five different
    term spellings are being asked the same question; if two disagree on how
    many rows a pattern matches, at least one is being queried wrongly and its
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


def main() -> int:
    if any(arg in ("-h", "--help") for arg in sys.argv[1:]):
        print(__doc__)
        return 0
    if not shutil.which("uv"):
        log("uv is required to provision the per-adapter virtualenvs")
        return 1

    VENV_ROOT.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Sampled before any adapter runs, so it describes the headroom the whole
    # run had, not whatever the last library left behind.
    mem = meminfo_mb()

    opts = dataset_opts(graphs=GRAPHS)
    qm = moduli(N_TRIPLES, opts)

    # Keyed on the resolved cardinality as well as the row count: the env knobs
    # `dataset_opts` reads change the file's contents at the same row count.
    key = f"{N_TRIPLES}_{qm.n_subj}x{qm.n_pred}x{qm.n_obj}x{qm.n_graph}_{opts.literal_frac:g}"
    quads = DATA_DIR / f"quads_{key}.nq"
    triples = DATA_DIR / f"triples_{key}.nt"
    for path, drop in ((quads, False), (triples, True)):
        if path.exists():
            log(f"reusing dataset {path.name} ({path.stat().st_size / 1e6:.1f} MB)")
        else:
            log(f"generating {path.name} ({N_TRIPLES:,} rows)…")
            t0 = time.perf_counter()
            write_dataset(str(path), N_TRIPLES, opts, drop_graph=drop)
            log(f"  wrote {path.stat().st_size / 1e6:.1f} MB in {time.perf_counter() - t0:.1f}s")

    rows: list = []
    memory: list = []
    sizes: list = []
    counts_by_slug: dict[str, dict[str, int]] = {}
    labels: dict[str, str] = {}
    iters: dict[str, int] = {}

    for slug in ALL_SLUGS:
        res = run_worker(slug, "query", triples, quads)
        if not res:
            continue
        rows.extend(res["rows"])
        labels[slug] = res["label"]
        if res.get("counts"):
            counts_by_slug[slug] = res["counts"]
        # Every worker runs the same counts; keep the first that reports them so
        # the dashboard can state the repetition count the way the JS tab does.
        iters = iters or res.get("iters") or {}
        if res.get("peak_rss_mb") is not None:
            memory.append(
                {"slug": slug, "label": res["label"], "role": "query", "peakRssMb": res["peak_rss_mb"]}
            )
        sizes.append(
            {"slug": slug, "label": res["label"], "bytes": res.get("artifact_bytes")}
        )

    # The Arrow read of the same probes, for the libraries that export any.
    # Its own process per adapter: pyarrow's import alone is tens of megabytes
    # of RSS, and the query role above is where this library's peak-memory
    # figure comes from.
    for slug in ARROW_SLUGS:
        res = run_worker(slug, "arrow", triples, quads)
        if not res:
            continue
        rows.extend(res["rows"])

    # Every adapter, including the ones that cannot mutate: a worker that finds
    # the operation unsupported exits before building anything, so the sweep is
    # cheap, and the rows it emits are what the dashboard renders as an
    # explained `unsupported` cell rather than a blank one.
    for slug in ALL_SLUGS:
        res = run_worker(slug, "mutate", triples, quads)
        if not res:
            continue
        rows.extend(res["rows"])
        if res.get("peak_rss_mb") is not None:
            memory.append(
                {"slug": slug, "label": res["label"], "role": "mutate", "peakRssMb": res["peak_rss_mb"]}
            )

    warnings = check_count_agreement(counts_by_slug)
    for w in warnings:
        log(f"!! count disagreement: {w}")

    versions: dict[str, str] = {}
    for name, pkgs in VENV_PACKAGES.items():
        if pkgs:
            versions.update(installed_versions(provision(name), pkgs))
    try:
        vortex_version = subprocess.run(
            [str(provision("vortex")), "-c", "import vortex_rdf; print(vortex_rdf.__version__)"],
            capture_output=True, text=True, check=True,
            env={**os.environ, "PYTHONPATH": str(REPO_ROOT / "python")},
        ).stdout.strip()
    except subprocess.CalledProcessError:
        vortex_version = "?"

    py_ver = subprocess.run(
        [str(provision("rdflib")), "-c", "import sys; print('.'.join(map(str, sys.version_info[:3])))"],
        capture_output=True, text=True, check=True,
    ).stdout.strip()

    lib_str = ", ".join(
        [f"vortex-rdf {vortex_version}"] + [f"{k} {v}" for k, v in sorted(versions.items())]
    )
    # UTC, to the minute — the format every dashboard tab dates itself in (see
    # `scripts/render_bench_dashboard.py`, `js/bench/compare.bench.ts`), so one
    # page never shows three formats or three timezones for one run.
    measured = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    provenance = (
        f"Measured {measured} · Python {py_ver} · {cpu_model()}, {os.cpu_count()} threads · "
        f"{N_TRIPLES:,} quads over {qm.n_graph} named graphs ({qm.terms:,} terms; "
        f"lightrdf reads their triples projection), MUT_BATCH={MUT_BATCH:,} · "
        f"wall-clock (perf_counter_ns) · {lib_str} · one adapter per process and per virtualenv, isolated"
    )

    matched = {}
    for c in counts_by_slug.values():
        matched.update(c)

    out = {
        "provenance": provenance,
        "results": rows,
        "memory": memory,
        "sizes": sizes,
        "config": {
            "triplesCount": N_TRIPLES,
            "cardinality": qm.__dict__,
            "matchedRows": matched,
            "mutBatch": MUT_BATCH,
            # Same key names the JS harness emits, so both tabs state their
            # repetition counts identically.
            "queryIterations": iters.get("query"),
            "heavyIterations": iters.get("heavy"),
            "fullScanIterations": iters.get("fullScan"),
            "countWarnings": warnings,
            "systemMemoryMb": mem.get("MemTotal"),
            "availableMemoryMb": mem.get("MemAvailable"),
        },
    }
    dest = BENCH_DIR / "results.json"
    dest.write_text(json.dumps(out, indent=2), encoding="utf-8")
    log(
        f"\nWrote {len(rows)} benchmark rows, {len(memory)} memory readings, "
        f"{len(sizes)} artifact sizes → {dest}"
    )
    if warnings:
        log(f"NOTE: {len(warnings)} pattern count disagreement(s) recorded in config.countWarnings")
    return 0


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


if __name__ == "__main__":
    sys.exit(main())
