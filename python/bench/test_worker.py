"""Tests for `worker.py`: `measure`, the timing rule every Python cell goes
through, and the memory role (its `mallinfo2` counter, `/proc` sampler,
self-check and the worker process itself)."""

from __future__ import annotations

import argparse
import json
import os
import platform
import subprocess
import sys
import time
import types
from pathlib import Path

import pytest

BENCH = Path(__file__).resolve().parent
sys.path.insert(0, str(BENCH))

import worker  # noqa: E402
from adapters import Adapter, PycottasAdapter, build_adapter  # noqa: E402
from suite import prepare_dataset  # noqa: E402

PROBES = ("S", "P", "O", "SP", "PO", "SPO", "G", "SPOG", "full")

def glibc_has_mallinfo2() -> bool:
    name, version = platform.libc_ver()
    try:
        numbers = tuple(int(part) for part in version.split(".")[:2])
    except ValueError:
        return False
    return name == "glibc" and numbers >= (2, 33)


needs_glibc = pytest.mark.skipif(
    not glibc_has_mallinfo2(), reason="reads /proc and glibc's mallinfo2 (glibc 2.33+)"
)


class Using(Adapter):
    """A store of one block, whose opens and reads leave known amounts behind."""

    slug, label, obtained = "using", "Using", "built"

    def __init__(self, store, cache=0, first_open_leak=0, read_leak=0):
        self.store, self.cache = store, cache
        self.first_open_leak, self.read_leak = first_open_leak, read_leak
        self.opens = 0
        self.leaked = []

    def artifact_path(self, workdir, src):
        return ""

    def open(self, artifact, src):
        if self.opens == 0:
            self.leaked.append(bytearray(self.first_open_leak))
        self.opens += 1
        return types.SimpleNamespace(block=bytearray(self.store), caches=[])

    def count(self, handle, query):
        handle.caches.append(bytearray(self.cache))
        self.leaked.append(bytearray(self.read_leak))
        return 7


class PycottasOverABlock(PycottasAdapter):
    """pycottas's adapter with its store swapped for one block of memory, so a
    run needs no pycottas or DuckDB: only the adapter's declarations remain."""

    def __init__(self, size):
        self.size = size
        self.duck = 0  # the fake DuckDB count
        self.kept = []  # live bytes the count's own query keeps, as a library's accounting call can

    def import_library(self):
        pass

    def open(self, artifact, src):
        self.duck += 3 << 20
        return bytearray(self.size)

    def prepare(self, pat):
        return pat

    def count(self, handle, query):
        self.duck += 5 << 20
        return 7

    def unseen_live_bytes(self):
        self.kept.append(bytearray(4 << 20))
        return self.duck


def memory_role(slug, ds, workdir, env):
    return subprocess.run(
        [
            sys.executable, BENCH / "worker.py",
            "--slug", slug, "--role", "memory",
            "--triples", ds.triples, "--quads", ds.quads,
            "--workdir", str(workdir), "--n", str(ds.n), "--graphs", "8",
        ],
        capture_output=True, text=True, env=env,
    )


def test_warmup_runs_are_discarded():
    calls: list[int] = []
    rows: list[dict] = []

    worker.measure("list::S", lambda _: calls.append(1), rows, 10, warmup=5, regime="warm")

    assert len(calls) == 15
    assert rows[0]["samples"] == "10"
    assert rows[0]["regime"] == "warm"
    assert "timeout" not in rows[0]


def slow_body(calls: list[int]):
    def body(_):
        calls.append(1)
        time.sleep(0.005)

    return body


def test_a_query_whose_first_run_is_past_the_limit_times_out():
    calls: list[int] = []
    rows: list[dict] = []

    worker.measure("list::S", slow_body(calls), rows, 10, warmup=5, regime="cold", limit_ns=1e6)

    assert len(calls) == 1
    row = rows[0]
    assert row["timeout"] is True
    assert (row["median"], row["median_ns"], row["samples"]) == ("timeout", None, "0")
    assert row["reason"].startswith("not measured: one run took ")
    assert row["regime"] == "cold"


def test_a_cell_without_a_limit_is_measured_however_slow():
    calls: list[int] = []
    rows: list[dict] = []

    worker.measure("ingest::list", slow_body(calls), rows, 3)

    assert len(calls) == 3
    assert rows[0]["samples"] == "3"
    assert "timeout" not in rows[0]


def test_only_the_first_run_is_held_to_the_limit():
    calls: list[int] = []
    rows: list[dict] = []

    def slower_after_the_first(_):
        if calls:
            time.sleep(0.002)
        calls.append(1)

    worker.measure("list::S", slower_after_the_first, rows, 3, warmup=1, limit_ns=1e6)

    assert len(calls) == 4
    assert rows[0]["samples"] == "3"


# ─── The memory role ────────────────────────────────────────────────────────


@needs_glibc
def test_live_bytes_count_a_block_until_it_is_freed():
    before = worker.live_bytes()
    block = bytearray(32 << 20)
    held = worker.live_bytes()
    assert held - before >= 32 << 20
    del block
    assert held - worker.live_bytes() >= 32 << 20


@needs_glibc
def test_the_sampler_keeps_the_highest_sample():
    worker.self_check()  # trims, so no free chunk left by earlier tests serves the block
    switch = sys.getswitchinterval()
    base = worker.read_rss()
    assert base.anon > 0 and base.file > 0
    with worker.RssSampler() as s:
        block = bytearray(64 << 20)  # zero-filled, so its pages are resident
        time.sleep(0.05)
        del block
    assert s.peak.anon - base.anon >= 60 << 20
    assert worker.read_rss().anon - base.anon < 32 << 20
    assert sys.getswitchinterval() == switch


def test_a_libc_without_mallinfo2_is_named(monkeypatch):
    monkeypatch.setattr(worker, "_libc", object())
    with pytest.raises(RuntimeError, match="mallinfo2"):
        worker.live_bytes()
    with pytest.raises(RuntimeError, match="mallinfo2"):
        worker.self_check()


@needs_glibc
def test_the_self_check_names_a_counter_that_misses_the_block():
    worker.self_check()
    with pytest.raises(SystemExit, match="mallinfo2"):
        worker.self_check(live=lambda: 0)


@needs_glibc
def test_the_store_figure_is_what_a_store_no_read_has_touched_holds(tmp_path):
    a = Using(32 << 20, cache=2 << 20, first_open_leak=8 << 20, read_leak=1 << 20)
    out = worker.run_memory(
        a, argparse.Namespace(quads="q.nq", triples="t.nt", workdir=str(tmp_path), n=64, graphs=8)
    )
    assert (out["obtained"], out["counts"]) == ("built", dict.fromkeys(PROBES, 7))
    assert abs(out["storeBytes"] - (32 << 20)) < 1 << 20
    assert a.opens == 2
    assert out["peakAnonBytes"] >= 30 << 20


@needs_glibc
def test_a_pycottas_memory_run_adds_what_duckdb_tracked_over_the_open(tmp_path):
    out = worker.run_memory(
        PycottasOverABlock(32 << 20),
        argparse.Namespace(quads="q.nq", triples="t.nt", workdir=str(tmp_path), n=64, graphs=8),
    )
    assert out["obtained"] == "opened"
    assert abs(out["storeBytes"] - (35 << 20)) < 1 << 20


def test_pycottas_counts_duckdbs_own_tracked_memory(monkeypatch):
    seen = []

    def execute(sql):
        seen.append(sql)
        return types.SimpleNamespace(fetchone=lambda: (123_456,))

    monkeypatch.setitem(sys.modules, "duckdb", types.SimpleNamespace(execute=execute))
    assert PycottasAdapter().unseen_live_bytes() == 123_456
    assert "duckdb_memory()" in seen[0]


@needs_glibc
@pytest.mark.parametrize("slug,obtained", [("vortex_dict", "mapped"), ("vortex_dict_mem", "loaded")])
def test_the_memory_role_prints_one_run(slug, obtained, tmp_path):
    ds = prepare_dataset(tmp_path, 4096, 8)
    a = build_adapter(slug)
    a.dispose(a.build(str(ds.quads), a.artifact_path(str(tmp_path), str(ds.quads))))

    proc = memory_role(slug, ds, tmp_path, {**os.environ, "PYTHONMALLOC": "malloc"})
    assert proc.returncode == 0, proc.stderr

    out = json.loads(proc.stdout)
    assert (out["slug"], out["role"], out["obtained"]) == (slug, "memory", obtained)
    assert set(out["counts"]) == set(PROBES)
    assert out["counts"]["full"] == 4096
    assert all(type(out[k]) is int for k in ("storeBytes", "peakAnonBytes", "peakFileBytes"))
    # The store's native memory goes through glibc and is released.
    assert out["storeBytes"] > 0


def test_the_memory_role_needs_the_malloc_allocator(tmp_path):
    env = {k: v for k, v in os.environ.items() if k != "PYTHONMALLOC"}
    proc = memory_role("vortex_dict", prepare_dataset(tmp_path, 64, 8), tmp_path, env)
    assert proc.returncode == 1
    assert "PYTHONMALLOC=malloc" in proc.stderr


def test_only_memory_workers_run_with_the_malloc_allocator(monkeypatch):
    import run

    monkeypatch.delenv("PYTHONMALLOC", raising=False)
    assert run.worker_env("vortex", "memory")["PYTHONMALLOC"] == "malloc"
    assert "PYTHONMALLOC" not in run.worker_env("vortex", "query")
    monkeypatch.setenv("PYTHONMALLOC", "pymalloc")
    monkeypatch.setenv("PYTHONTRACEMALLOC", "1")
    for role in ("query", "mutate"):
        assert "PYTHONMALLOC" not in run.worker_env("vortex", role)
    memory = run.worker_env("vortex", "memory")
    assert memory["PYTHONMALLOC"] == "malloc"
    assert "PYTHONTRACEMALLOC" not in memory
    monkeypatch.delenv("PYTHONMALLOC")
    assert run.worker_env("rdflib", "memory")["PYTHONPATH"] == str(run.BENCH_DIR)
