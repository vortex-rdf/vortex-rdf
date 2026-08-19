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
PYTHONPATH is enough and avoids a maturin build per run.
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
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from adapters import ALL_SLUGS, VENV_FOR  # noqa: E402
from datasets import DatasetOpts, moduli, write_dataset  # noqa: E402

BENCH_DIR = Path(__file__).resolve().parent
REPO_ROOT = BENCH_DIR.parent.parent
VENV_ROOT = BENCH_DIR / ".venvs"
DATA_DIR = BENCH_DIR / ".data"

# Scale knobs mirror the JavaScript comparative bench so the two tabs describe
# the same dataset: D**3 triples and DQ**4 quads.
D = int(os.environ.get("BENCH_DIM", 128))
DQ = int(os.environ.get("BENCH_DIM_QUADS", 32))
N_TRIPLES = D**3
N_QUADS = DQ**4
GRAPHS = int(os.environ.get("BENCH_GRAPHS_QUADS", 8))
MUT_BATCH = int(os.environ.get("MUT_BATCH", 10_000))
PYTHON_VERSION = os.environ.get("BENCH_PYTHON", "3.13")

#: Package set per virtualenv. Vortex needs none -- it rides on PYTHONPATH.
VENV_PACKAGES = {
    "vortex": [],
    "pyoxigraph": ["pyoxigraph"],
    "pycottas": ["pycottas"],
    "rdflib": ["rdflib"],
    "lightrdf": ["lightrdf"],
}


def log(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)


def provision(name: str) -> Path:
    """Create the virtualenv for `name` if absent; return its interpreter."""
    venv = VENV_ROOT / name
    python = venv / "bin" / "python"
    if python.exists():
        return python
    log(f"provisioning venv: {name}")
    subprocess.run(
        ["uv", "venv", str(venv), "--python", PYTHON_VERSION, "-q"], check=True, cwd=REPO_ROOT
    )
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
        "--n-triples", str(N_TRIPLES), "--n-quads", str(N_QUADS),
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
    dashboard's provenance rather than silently averaged over.
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
    if not shutil.which("uv"):
        log("uv is required to provision the per-adapter virtualenvs")
        return 1

    VENV_ROOT.mkdir(parents=True, exist_ok=True)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Sampled before any adapter runs, so it describes the headroom the whole
    # run had rather than whatever the last library left behind.
    mem = meminfo_mb()

    tm = moduli(N_TRIPLES)
    qm = moduli(N_QUADS, DatasetOpts(graphs=GRAPHS))

    triples = DATA_DIR / f"triples_{N_TRIPLES}.nt"
    quads = DATA_DIR / f"quads_{N_QUADS}.nq"
    for path, n, opts in ((triples, N_TRIPLES, None), (quads, N_QUADS, DatasetOpts(graphs=GRAPHS))):
        if path.exists():
            log(f"reusing dataset {path.name} ({path.stat().st_size / 1e6:.1f} MB)")
        else:
            log(f"generating {path.name} ({n:,} quads)…")
            t0 = time.perf_counter()
            write_dataset(str(path), n, opts)
            log(f"  wrote {path.stat().st_size / 1e6:.1f} MB in {time.perf_counter() - t0:.1f}s")

    rows: list = []
    memory: list = []
    sizes: list = []
    counts_by_slug: dict[str, dict[str, int]] = {}
    labels: dict[str, str] = {}

    for slug in ALL_SLUGS:
        res = run_worker(slug, "query", triples, quads)
        if not res:
            continue
        rows.extend(res["rows"])
        labels[slug] = res["label"]
        if res.get("counts"):
            counts_by_slug[slug] = res["counts"]
        if res.get("peak_rss_mb") is not None:
            memory.append(
                {"slug": slug, "label": res["label"], "role": "query", "peakRssMb": res["peak_rss_mb"]}
            )
        sizes.append(
            {"slug": slug, "label": res["label"], "bytes": res.get("artifact_bytes")}
        )

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
            [str(provision("vortex")), "-c", "from vortex_rdf import _native as n; print(n.__version__)"],
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
    provenance = (
        f"Measured {date.today().isoformat()} · Python {py_ver} · {cpu_model()}, {os.cpu_count()} threads · "
        f"triples D={D} ({N_TRIPLES:,} quads, {tm.terms:,} terms), "
        f"quads Dq={DQ} ({N_QUADS:,} quads, {qm.terms:,} terms), MUT_BATCH={MUT_BATCH:,} · "
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
            "quadsCount": N_QUADS,
            "cardinality": {"triples": tm.__dict__, "quads": qm.__dict__},
            "matchedRows": matched,
            "mutBatch": MUT_BATCH,
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

    Recorded because a library whose peak RSS approaches what the machine had
    free was measured under swap pressure, and its *timings* are then a
    property of this machine rather than of the library. The dashboard compares
    each peak against these figures and says so rather than leaving the reader
    to trust a number that quietly includes page-fault time.
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
