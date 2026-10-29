"""Tests for the Java suite's driver, `java/bench/run.py`.

Run from python/ (whose environment has pytest):
    cd python && uv run --no-sync pytest ../java/bench/test_run.py -q
"""

from __future__ import annotations

import importlib.util
import json
import os
import sys
from pathlib import Path

import pytest

BENCH_DIR = Path(__file__).resolve().parent
FIXTURE = BENCH_DIR / "harness" / "src" / "test" / "resources" / "fixture"

# Loaded by path: python/bench/run.py, which the driver's imports put on
# sys.path, has the same module name. Registered before it runs, because a
# dataclass resolves its annotations through sys.modules.
_spec = importlib.util.spec_from_file_location("java_bench_run", BENCH_DIR / "run.py")
run = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = run
_spec.loader.exec_module(run)


def test_rows_write_triples_and_quads(tmp_path):
    rows = [("<s>", "<p>", '"v w"', None), ("<s>", "<p>", "<o>", "<g>"), ("<a>", "<b>", "<c>")]

    path = run.write_rows(tmp_path / "rows.nq", rows)

    assert path.read_text(encoding="utf-8") == '<s> <p> "v w" .\n<s> <p> <o> <g> .\n<a> <b> <c> .\n'


def test_worker_inputs_are_keyed_by_dataset_and_batch(tmp_path):
    from suite import prepare_dataset

    ds = prepare_dataset(tmp_path, 64, 8)

    inputs = run.write_worker_inputs(tmp_path, ds, 16)

    assert inputs.probes == tmp_path / f"probes_{ds.key}.tsv"
    assert inputs.fresh == tmp_path / "fresh_16.nt"
    assert inputs.prefix == tmp_path / f"prefix_{ds.key}_16.nq"
    assert inputs.empty.read_text(encoding="utf-8") == ""
    assert len(inputs.fresh.read_text(encoding="utf-8").splitlines()) == 16
    # The delete batch must be rows the store holds: the dataset file's first 16.
    first_rows = ds.quads.read_text(encoding="utf-8").splitlines()[:16]
    assert inputs.prefix.read_text(encoding="utf-8").splitlines() == first_rows


def test_fixture_reproduces_the_committed_files(tmp_path):
    run.write_fixture(tmp_path)

    for name in ("quads.nq", "triples.nt", "probes.tsv", "fresh.nt", "prefix.nq"):
        assert (tmp_path / name).read_bytes() == (FIXTURE / name).read_bytes(), name


# ─── Running the workers ────────────────────────────────────────────────────


def stub(out, payload):
    """A command standing in for a JVM worker: it writes `payload` to `out`."""
    script = "import json, sys; json.dump(json.loads(sys.argv[2]), open(sys.argv[1], 'w'))"
    return [sys.executable, "-c", script, str(out), json.dumps(payload)]


def result(**overrides):
    """A worker result with every key the Java worker writes."""
    base = {
        "slug": "x", "label": "X", "role": "query", "rows": [{"id": "ingest::x"}], "counts": {"S": 1},
        "iters": {"query": 10, "queryWarmup": 5, "heavy": 3, "fullScan": 3}, "artifact": None, "failures": [],
        "skippedRole": False, "supportsQuads": True, "jdk": "25.0.4.1 (Ubuntu)",
    }
    return {**base, **overrides}


@pytest.fixture
def driver(tmp_path, monkeypatch):
    """The driver with its data directory and crash list private to one test."""
    monkeypatch.setattr(run, "DATA_DIR", tmp_path / "data")
    monkeypatch.setattr(run, "CRASHES", [])
    return run


def test_peak_rss_includes_reaped_grandchildren():
    # The grandchild touches every page of 300 MB; its parent only waits for it.
    grandchild = "b = bytearray(300 << 20); b[::4096] = b'x' * len(b[::4096])"
    child = f"import subprocess, sys; subprocess.run([sys.executable, '-c', {grandchild!r}], check=True)"

    code, mb = run.run_measured([sys.executable, "-c", child])

    assert code == 0
    assert mb >= 300


def test_allocated_bytes_counts_blocks_not_apparent_size(tmp_path):
    sparse = tmp_path / "sparse"
    with open(sparse, "wb") as f:
        f.truncate(100 << 20)
    tree = tmp_path / "tree"
    tree.mkdir()
    (tree / "dense").write_bytes(os.urandom(1 << 20))

    assert run.allocated_bytes(sparse) < 1 << 20
    assert run.allocated_bytes(tree) >= 1 << 20


def test_a_crashed_worker_is_reported_and_the_run_continues(driver, monkeypatch):
    def command(slug, role, out, files):
        if slug == "a":
            return [sys.executable, "-c", "import sys; sys.exit(3)"]
        return stub(out, result(slug="b", label="B", role=role, rows=[{"id": f"{role}::b"}]))

    monkeypatch.setattr(driver, "worker_command", command)

    from suite import run_roles

    outcome = run_roles(["a", "b"], driver.run_java_worker)

    assert [(c["slug"], c["role"], c["error"]) for c in driver.CRASHES] == [
        ("a", "query", "worker exited with 3"),
        ("a", "mutate", "worker exited with 3"),
    ]
    assert all(c["label"] == "a" and c["phase"] == "worker" and c["keys"] == [] for c in driver.CRASHES)
    assert [r["id"] for r in outcome.rows] == ["query::b", "mutate::b"]


def test_a_skipped_role_reports_no_memory(driver, monkeypatch):
    monkeypatch.setattr(driver, "worker_command", lambda slug, role, out, files: stub(out, result(skippedRole=True)))

    assert driver.run_java_worker("x", "mutate")["peak_rss_mb"] is None


def test_artifact_size_comes_from_the_store_location(driver, tmp_path, monkeypatch):
    store = tmp_path / "store"
    store.mkdir()
    (store / "data").write_bytes(os.urandom(1 << 20))
    monkeypatch.setattr(driver, "worker_command", lambda slug, role, out, files: stub(out, result(artifact=str(store))))

    res = driver.run_java_worker("x", "query")

    assert res["artifact_bytes"] >= 1 << 20
    assert isinstance(res["peak_rss_mb"], int) and res["peak_rss_mb"] > 0


def test_the_retained_figure_joins_the_memory_row_in_megabytes(driver, monkeypatch):
    def worker(retained):
        monkeypatch.setattr(driver, "worker_command", lambda slug, role, out, files: stub(out, result(retained=retained)))
        return driver.run_java_worker("x", "query").get("memory_extra")

    assert worker({"heapBytes": 3 << 20, "nativeBytes": 3 << 19}) == {"retainedHeapMb": 3.0, "retainedNativeMb": 1.5}
    # A role that ended before its store was open has nothing to report.
    assert worker(None) == {}


def test_worker_failures_carry_their_slug_label_and_role(driver, monkeypatch):
    failure = {"phase": "O", "keys": ["x::O", "x::O::count"], "error": "boom"}
    monkeypatch.setattr(driver, "worker_command", lambda slug, role, out, files: stub(out, result(failures=[failure])))

    res = driver.run_java_worker("x", "query")

    assert res["failures"] == [{**failure, "slug": "x", "label": "X", "role": "query"}]
    assert res["artifact_bytes"] is None


def test_a_worker_runs_on_its_own_library_classpath(driver, tmp_path):
    files = driver.RunFiles(tmp_path / "q.nq", tmp_path / "t.nt", driver.WorkerInputs(
        tmp_path / "p.tsv", tmp_path / "f.nt", tmp_path / "x.nq", tmp_path / "e.nt"))

    cmd = driver.worker_command("jena_tdb2", "query", tmp_path / "out.json", files)

    assert cmd[cmd.index("-cp") + 1] == driver.classpath("jena")
    # The retained figure reads the JVM's own native allocations from its tracking.
    assert "-XX:NativeMemoryTracking=summary" in cmd
    assert cmd[cmd.index("--slug") + 1] == "jena_tdb2"
    assert cmd[cmd.index("--workdir") + 1] == str(tmp_path / "data" / "work" / "jena_tdb2")
    assert cmd[-2:] == ["--out", str(tmp_path / "out.json")]
    baseline = driver.worker_command("", "baseline", tmp_path / "b.json", None)
    assert baseline[baseline.index("-cp") + 1].endswith("harness/target/harness.jar")
