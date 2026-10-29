"""Orchestrate the Python comparative benchmark and emit `target/bench/python.json`.

Provisions one virtualenv per library, generates the shared dataset files once,
runs each adapter in its own subprocess, and aggregates the rows into the shape
`scripts/render_bench_dashboard.py` feeds to the dashboard's Python tab. The
orchestration it shares with the Java suite (`java/bench/run.py`) lives in
`suite.py`.

    python3 python/bench/run.py                    # full run
    BENCH_DIM=32 python3 python/bench/run.py       # quick pilot

Isolation is per library, not just per process: pycottas hard-pins
`pyoxigraph==0.3.18`, so installing everything together would quietly bench a
pyoxigraph two minor versions behind the oxigraph the JavaScript tab compares
against. Each venv is provisioned once and reused.

The Vortex bindings are not installed into their venv -- the compiled extension
is abi3 and `python/vortex_rdf/` is a plain package directory, so putting it on
PYTHONPATH is enough and avoids a maturin build per run.

Every adapter counts every query pattern before timing it; a disagreement
between libraries is recorded under `config.countWarnings` in that file.
A phase a library lacks is emitted as an explained `unsupported` cell instead
of a missing row, so the dashboard can say why the cell is empty.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from adapters import ALL_SLUGS, VENV_FOR  # noqa: E402
from suite import (  # noqa: E402
    bench_graphs,
    bench_rows,
    check_count_agreement,
    cpu_model,
    log,
    measured_now,
    meminfo_mb,
    mut_batch,
    prepare_dataset,
    run_roles,
    suite_output,
    write_output,
)

BENCH_DIR = Path(__file__).resolve().parent
REPO_ROOT = BENCH_DIR.parent.parent
VENV_ROOT = BENCH_DIR / ".venvs"
DATA_DIR = BENCH_DIR / ".data"

#: One dataset for the whole run: every library that has graphs in its model
#: builds from it, and the ones that do not read its triples projection.
N_TRIPLES = bench_rows()
GRAPHS = bench_graphs()
MUT_BATCH = mut_batch()
PYTHON_VERSION = os.environ.get("BENCH_PYTHON", "3.13")

#: Package set per virtualenv. Vortex needs none -- it rides on PYTHONPATH.
VENV_PACKAGES = {
    "vortex": [],
    "pyoxigraph": ["pyoxigraph"],
    "pycottas": ["pycottas"],
    "rdflib": ["rdflib"],
    "lightrdf": ["lightrdf"],
}


def provision(name: str) -> Path:
    """Create the virtualenv for `name` if absent; return its interpreter."""
    venv = VENV_ROOT / name
    python = venv / "bin" / "python"
    if python.exists():
        return python
    log(f"provisioning venv: {name}")
    # `bin/python` is an absolute symlink to a uv-managed interpreter that
    # lives outside this tree, so a restored cache can bring the venv back
    # with that link dangling — and the check above follows symlinks, so it
    # reads as absent. Let uv replace the directory rather than refuse it.
    cmd = ["uv", "venv", str(venv), "--python", PYTHON_VERSION, "-q"]
    if venv.exists():
        cmd.append("--clear")
    subprocess.run(cmd, check=True, cwd=REPO_ROOT)
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

    dataset = prepare_dataset(DATA_DIR, N_TRIPLES, GRAPHS)
    run = run_roles(ALL_SLUGS, lambda slug, role: run_worker(slug, role, dataset.triples, dataset.quads))

    warnings = check_count_agreement(run.counts_by_slug)
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
    qm = dataset.moduli
    provenance = (
        f"Measured {measured_now()} · Python {py_ver} · {cpu_model()}, {os.cpu_count()} threads · "
        f"{N_TRIPLES:,} quads over {qm.n_graph} named graphs ({qm.terms:,} terms; "
        f"lightrdf reads their triples projection), MUT_BATCH={MUT_BATCH:,} · "
        f"wall-clock (perf_counter_ns) · {lib_str} · one adapter per process and per virtualenv, isolated"
    )

    out = suite_output(run, provenance, dataset, MUT_BATCH, mem, warnings)
    write_output(REPO_ROOT / "target" / "bench" / "python.json", out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
