#!/usr/bin/env python3
"""Orchestrate the Java comparative benchmark and emit `target/bench/java.json`.

    python3 java/bench/run.py                  # full run
    BENCH_DIM=16 python3 java/bench/run.py     # quick pilot
    python3 java/bench/run.py --fixture DIR    # write the harness's test fixture

Needs the JNI library and the CLI (`cargo build --release -p vortex-rdf-java`,
then `-p vortex-rdf-cli`, separately) and the bench itself (`mvn -f
java/bench/pom.xml package`) built first; `scripts/refresh.sh --only java`
does all three.

The dataset generator and the orchestration are python/bench's (`datasets.py`
and `suite.py`), so the Java tab measures the Python tab's data by the same
rules. This file writes the files a Java worker reads besides the dataset (the
query probes and the two mutation batches), runs one JVM per adapter and role
on that library's own classpath, and reads each worker's peak memory as the
kernel reports it when the worker exits.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

BENCH_DIR = Path(__file__).resolve().parent
REPO_ROOT = BENCH_DIR.parent.parent
sys.path.insert(0, str(REPO_ROOT / "python" / "bench"))

from datasets import DatasetOpts, dataset_prefix, dataset_probes, fresh_quads, write_dataset  # noqa: E402
from suite import (  # noqa: E402
    Dataset,
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

DATA_DIR = BENCH_DIR / ".data"
OUT_JSON = REPO_ROOT / "target" / "bench" / "java.json"
NATIVE_LIB = REPO_ROOT / "target" / "release" / "libvortex_rdf_jni.so"
CLI = REPO_ROOT / "target" / "release" / "vortex-rdf-cli"
HARNESS_JAR = BENCH_DIR / "harness" / "target" / "harness.jar"
WORKER = "org.vortexrdf.bench.Worker"

#: Every adapter a full run measures, in dashboard row order, with the module
#: whose classpath it runs on: the Python tab's six Vortex builds, then the
#: Java libraries Vortex is measured against.
ADAPTERS: list[tuple[str, str]] = [
    ("vortex_dict", "vortex"),
    ("vortex_dict_mem", "vortex"),
    ("vortex_dict_bycopy", "vortex"),
    ("vortex_dict_bycopy_mem", "vortex"),
    ("vortex_dict_byref", "vortex"),
    ("vortex_dict_byref_mem", "vortex"),
    ("hdt_java", "hdt-java"),
    ("qendpoint_hdt", "qendpoint"),
    ("jena_tim", "jena"),
    ("jena_tdb2", "jena"),
    ("rdf4j_memory", "rdf4j"),
    ("rdf4j_native", "rdf4j"),
    ("rdf4j_lmdb", "rdf4j"),
]
MODULE_FOR = dict(ADAPTERS)

#: Workers that died or wrote no result, one entry per adapter and role, in
#: the failures shape the dashboard lists beside the tab's count check.
CRASHES: list[dict] = []

#: The harness's test fixture: small enough to commit, with named graphs so
#: the graph probes match rows, and its own fixed options rather than the
#: environment's, so regenerating it always gives the same bytes.
FIXTURE_ROWS = 512
FIXTURE_GRAPHS = 8
FIXTURE_BATCH = 16


# ─── Worker inputs ──────────────────────────────────────────────────────────


@dataclass(frozen=True)
class WorkerInputs:
    """The files a worker reads besides the dataset itself."""

    probes: Path
    fresh: Path
    prefix: Path
    empty: Path


@dataclass(frozen=True)
class RunFiles:
    """Everything a run's workers read: the dataset, its triples projection and the worker inputs."""

    quads: Path
    triples: Path
    inputs: WorkerInputs


def write_probes(path: Path, n: int, opts: DatasetOpts) -> Path:
    """The query probes, one per line: name, s, p, o, g and kind (`triples`,
    `quads` or `full`), tab-separated, with an empty field for a wildcard."""
    probes = dataset_probes(n, opts)
    with open(path, "w", encoding="utf-8") as f:
        for kind in ("triples", "quads", "full"):
            for pat in probes[kind]:
                f.write("\t".join([pat.name, pat.s or "", pat.p or "", pat.o or "", pat.g or "", kind]) + "\n")
    return path


def write_rows(path: Path, rows: Iterable[tuple]) -> Path:
    """Rows as N-Triples or N-Quads lines. A row without a graph term, or with
    None in its place, is a triple in the default graph."""
    with open(path, "w", encoding="utf-8") as f:
        for row in rows:
            s, p, o, *rest = row
            g = rest[0] if rest else None
            f.write(f"{s} {p} {o} .\n" if g is None else f"{s} {p} {o} {g} .\n")
    return path


def write_worker_inputs(data_dir: Path, dataset: Dataset, mut_batch: int) -> WorkerInputs:
    """Write the probes and mutation batches for `dataset` into `data_dir`:
    the add batch is rows the store cannot hold yet, the delete batch the
    dataset's first `mut_batch` rows."""
    probes = write_probes(data_dir / f"probes_{dataset.key}.tsv", dataset.n, dataset.opts)
    fresh = write_rows(data_dir / f"fresh_{mut_batch}.nt", fresh_quads(mut_batch))
    prefix = write_rows(
        data_dir / f"prefix_{dataset.key}_{mut_batch}.nq", dataset_prefix(dataset.n, mut_batch, dataset.opts)
    )
    empty = data_dir / "empty.nt"
    empty.write_text("", encoding="utf-8")
    return WorkerInputs(probes, fresh, prefix, empty)


def write_fixture(dest: Path) -> None:
    """The harness's test fixture, under fixed file names in `dest`."""
    dest.mkdir(parents=True, exist_ok=True)
    opts = DatasetOpts(graphs=FIXTURE_GRAPHS)
    write_dataset(str(dest / "quads.nq"), FIXTURE_ROWS, opts)
    write_dataset(str(dest / "triples.nt"), FIXTURE_ROWS, opts, drop_graph=True)
    write_probes(dest / "probes.tsv", FIXTURE_ROWS, opts)
    write_rows(dest / "fresh.nt", fresh_quads(FIXTURE_BATCH))
    write_rows(dest / "prefix.nq", dataset_prefix(FIXTURE_ROWS, FIXTURE_BATCH, opts))


# ─── Workers ────────────────────────────────────────────────────────────────


def java_executable() -> str:
    home = os.environ.get("JAVA_HOME")
    return str(Path(home) / "bin" / "java") if home else "java"


def jvm_flags() -> list[str]:
    """The flags every worker JVM gets, whichever library it runs: one heap
    limit, the default collector, native access for the libraries that load
    native code, and a temporary directory on disk rather than tmpfs."""
    return [
        f"-Xmx{os.environ.get('JAVA_BENCH_XMX') or '6g'}",
        "-XX:+UseG1GC",
        "--enable-native-access=ALL-UNNAMED",
        f"-Djava.io.tmpdir={DATA_DIR / 'tmp'}",
        f"-Dvortexrdf.library={NATIVE_LIB}",
        f"-Dvortexrdf.cli={CLI}",
    ]


def classpath(module: str) -> str:
    """A library module's own classpath: its jar, and the dependencies Maven copied beside it."""
    target = BENCH_DIR / module / "target"
    return f"{target / (module + '.jar')}{os.pathsep}{target / 'lib'}/*"


def worker_command(slug: str, role: str, out: Path, files: Optional[RunFiles]) -> list[str]:
    """The JVM command that measures `slug` in `role` and writes its result to
    `out`. The baseline is an empty worker: the harness alone, no library."""
    if role == "baseline":
        return [java_executable(), *jvm_flags(), "-cp", str(HARNESS_JAR), WORKER, "--role", "baseline", "--out", str(out)]
    return [
        java_executable(), *jvm_flags(), "-cp", classpath(MODULE_FOR[slug]), WORKER,
        "--slug", slug, "--role", role,
        "--quads", str(files.quads), "--triples", str(files.triples),
        "--probes", str(files.inputs.probes), "--fresh", str(files.inputs.fresh),
        "--prefix", str(files.inputs.prefix), "--empty", str(files.inputs.empty),
        "--workdir", str(DATA_DIR / "work" / slug),
        "--out", str(out),
    ]


def run_measured(cmd: list[str]) -> tuple[int, Optional[int]]:
    """Run `cmd` to completion; return its exit code and its peak resident
    memory in MB. `wait4` reports the larger of the process's own peak and the
    peaks of the children it reaped, so a build the worker hands to a child
    process (Vortex's CLI) counts as the worker's own memory does."""
    proc = subprocess.Popen(cmd)
    _, status, usage = os.wait4(proc.pid, 0)
    proc.returncode = os.waitstatus_to_exitcode(status)
    return proc.returncode, round(usage.ru_maxrss / 1024)


def allocated_bytes(path: Path) -> int:
    """The bytes a file or directory occupies on disk: allocated blocks, not
    apparent size, so a store's sparse preallocated files count what they hold."""
    if path.is_dir():
        return sum(p.lstat().st_blocks * 512 for p in path.rglob("*") if p.is_file() and not p.is_symlink())
    return path.lstat().st_blocks * 512


def run_java_worker(slug: str, role: str, files: Optional[RunFiles] = None) -> Optional[dict]:
    """Measure one adapter in one role, in its own JVM: the `run_worker` that
    `suite.run_roles` takes. A worker that dies is recorded in CRASHES and
    yields None, so one library failing never discards the rest of the run."""
    out = DATA_DIR / "out" / f"{slug}.{role}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.unlink(missing_ok=True)
    log(f"\n[{slug}] {role}")
    code, peak_mb = run_measured(worker_command(slug, role, out, files))
    if code != 0:
        return crashed(slug, role, f"worker exited with {code}")
    try:
        res = json.loads(out.read_text(encoding="utf-8"))
    except (OSError, ValueError) as e:
        return crashed(slug, role, f"worker wrote no readable result: {e}")
    # A role the library cannot perform did nothing but start a JVM; its peak
    # would be a bare-JVM figure posing as a measurement.
    res["peak_rss_mb"] = None if res.get("skippedRole") else peak_mb
    res["artifact_bytes"] = allocated_bytes(Path(res["artifact"])) if res.get("artifact") else None
    res["failures"] = [{**f, "slug": slug, "label": res["label"], "role": role} for f in res.get("failures", [])]
    return res


def crashed(slug: str, role: str, error: str) -> None:
    log(f"!! [{slug}] {role} FAILED: {error}")
    CRASHES.append({"slug": slug, "label": slug, "role": role, "phase": "worker", "keys": [], "error": error})
    return None


# ─── The run ────────────────────────────────────────────────────────────────


def library_versions() -> list[str]:
    """The versions measured: the pins in java/bench/pom.xml, which Maven
    resolves exactly, and the bindings' own version."""
    ns = {"m": "http://maven.apache.org/POM/4.0.0"}
    props = ET.parse(BENCH_DIR / "pom.xml").getroot().find("m:properties", ns)
    pin = {child.tag.split("}")[1]: child.text for child in props}
    vortex = ET.parse(REPO_ROOT / "java" / "pom.xml").getroot().find("m:version", ns).text
    return [
        f"vortex-rdf {vortex}",
        f"hdt-java {pin['hdt.version']}",
        f"qendpoint-core {pin['qendpoint.version']}",
        f"Apache Jena {pin['jena.version']}",
        f"Eclipse RDF4J {pin['rdf4j.version']}",
    ]


def repo_relative(flags: list[str]) -> list[str]:
    return [f.replace(f"{REPO_ROOT}{os.sep}", "") for f in flags]


def baseline() -> tuple[str, Optional[int]]:
    """The JDK the workers run on and an empty worker JVM's peak memory, the
    floor every Java row's memory includes."""
    out = DATA_DIR / "out" / "baseline.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    code, peak_mb = run_measured(worker_command("", "baseline", out, None))
    if code != 0:
        raise SystemExit(f"the baseline worker exited with {code}: is the bench built (mvn -f java/bench/pom.xml package)?")
    return json.loads(out.read_text(encoding="utf-8"))["jdk"], peak_mb


def main(argv: list[str]) -> int:
    if len(argv) == 2 and argv[0] == "--fixture":
        write_fixture(Path(argv[1]))
        return 0
    if argv:
        print(__doc__, file=sys.stderr)
        return 2
    for path, how in (
        (NATIVE_LIB, "cargo build --release -p vortex-rdf-java"),
        (CLI, "cargo build --release -p vortex-rdf-cli"),
        (HARNESS_JAR, "mvn -f java/bench/pom.xml package -DskipTests"),
    ):
        if not path.exists():
            log(f"{path.relative_to(REPO_ROOT)} is missing; build it first ({how}), or run scripts/refresh.sh --only java")
            return 1
    (DATA_DIR / "tmp").mkdir(parents=True, exist_ok=True)

    # Sampled before any worker runs, so it describes the headroom the whole
    # run had, not whatever the last library left behind.
    mem = meminfo_mb()
    dataset = prepare_dataset(DATA_DIR, bench_rows(), bench_graphs())
    batch = mut_batch()
    files = RunFiles(dataset.quads, dataset.triples, write_worker_inputs(DATA_DIR, dataset, batch))

    jdk, baseline_mb = baseline()
    major = int(re.match(r"\d+", jdk).group())
    if major < 25:
        log(f"the workers need JDK 25 or newer (RDF4J 6 does), not {jdk}; point JAVA_HOME at one")
        return 1

    supports_quads: dict[str, bool] = {}

    def worker(slug: str, role: str) -> Optional[dict]:
        res = run_java_worker(slug, role, files)
        if res and role == "query":
            supports_quads[slug] = res["supportsQuads"]
        return res

    run = run_roles([slug for slug, _ in ADAPTERS], worker)
    warnings = check_count_agreement(run.counts_by_slug)
    for w in warnings:
        log(f"!! count disagreement: {w}")

    flags = repo_relative(jvm_flags())
    heap_flags = " ".join(f for f in flags if f.startswith("-X"))
    triples_only = [run.labels[slug] for slug, ok in supports_quads.items() if not ok]
    projection = (
        f"; {' and '.join(triples_only)} read{'s' if len(triples_only) == 1 else ''} their triples projection"
        if triples_only else ""
    )
    qm = dataset.moduli
    provenance = (
        f"Measured {measured_now()} · Java {jdk} · {cpu_model()}, {os.cpu_count()} threads · "
        f"{dataset.n:,} quads over {qm.n_graph} named graphs ({qm.terms:,} terms{projection}), "
        f"MUT_BATCH={batch:,} · wall-clock (System.nanoTime) · {', '.join(library_versions())} · "
        f"JVM {heap_flags} · one adapter per JVM and per classpath, isolated"
    )
    out = suite_output(
        run, provenance, dataset, batch, mem, warnings,
        extra_config={"jdk": jdk, "jvmFlags": flags, "jvmBaselineRssMb": baseline_mb},
        failures=run.failures + CRASHES,
    )
    write_output(OUT_JSON, out)
    if CRASHES:
        log(f"NOTE: {len(CRASHES)} worker(s) did not finish; see the failures in {OUT_JSON.name}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
