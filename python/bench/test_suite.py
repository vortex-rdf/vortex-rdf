"""Tests for `suite.py`, the orchestration the Python and Java suites share."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from suite import (  # noqa: E402
    SuiteRun,
    bench_rows,
    check_count_agreement,
    prepare_dataset,
    run_roles,
    suite_output,
)

#: The generator's knobs, cleared so the dataset is the documented default.
DATASET_KNOBS = ("BENCH_SUBJ_RATIO", "BENCH_PREDICATES", "BENCH_OBJ_RATIO", "BENCH_LITERAL_FRAC", "BENCH_GRAPHS")


@pytest.fixture
def default_knobs(monkeypatch):
    for name in DATASET_KNOBS:
        monkeypatch.delenv(name, raising=False)


def test_check_count_agreement_names_each_side():
    warnings = check_count_agreement({"a": {"S": 10, "P": 5}, "b": {"S": 10, "P": 6}})
    assert warnings == ["pattern P -- 5 rows: a; 6 rows: b"]


def test_bench_rows_prefers_bench_size_then_the_cube(monkeypatch):
    monkeypatch.delenv("BENCH_SIZE", raising=False)
    monkeypatch.delenv("BENCH_DIM", raising=False)
    assert bench_rows() == 1_048_576
    monkeypatch.setenv("BENCH_DIM", "16")
    assert bench_rows() == 4096
    monkeypatch.setenv("BENCH_SIZE", "500")
    assert bench_rows() == 500


def test_prepare_dataset_reuses_files_by_key(tmp_path, default_knobs):
    ds = prepare_dataset(tmp_path, 64, 8)
    # 64 rows at the default ratios: 6 subjects, 32 predicates, 32 objects and
    # 8 graphs, nudged to the pairwise-coprime 6, 35, 37 and 11.
    assert ds.key == "64_6x35x37x11_0.4"
    assert ds.quads == tmp_path / "quads_64_6x35x37x11_0.4.nq"
    assert ds.triples == tmp_path / "triples_64_6x35x37x11_0.4.nt"
    quads = ds.quads.read_text(encoding="utf-8").splitlines()
    triples = ds.triples.read_text(encoding="utf-8").splitlines()
    assert len(quads) == 64 and len(triples) == 64
    assert all(line.endswith(" .") and "/graph/2026/named/" in line for line in quads)
    assert all(line.endswith(" .") and "/graph/2026/named/" not in line for line in triples)

    stamps = (ds.quads.stat().st_mtime_ns, ds.triples.stat().st_mtime_ns)
    again = prepare_dataset(tmp_path, 64, 8)
    assert again.key == ds.key
    assert (again.quads.stat().st_mtime_ns, again.triples.stat().st_mtime_ns) == stamps


def test_run_roles_runs_queries_then_mutations():
    results = {
        ("a", "query"): {
            "rows": [{"id": "a1"}], "label": "A", "counts": {"S": 1}, "iters": {"query": 10},
            "peak_rss_mb": 50, "artifact_bytes": 123, "failures": [{"slug": "a", "error": "x"}],
            "memory_extra": {"retainedHeapMb": 2.5, "retainedNativeMb": 0.5},
        },
        ("b", "query"): {
            "rows": [{"id": "b1"}], "label": "B", "counts": {"S": 1}, "iters": {"query": 99},
            "peak_rss_mb": None, "artifact_bytes": None,
        },
        ("a", "mutate"): None,
        ("b", "mutate"): {"rows": [{"id": "b2"}], "label": "B", "peak_rss_mb": 70},
    }
    calls = []

    def worker(slug, role):
        calls.append((slug, role))
        return results[(slug, role)]

    run = run_roles(["a", "b"], worker)

    assert calls == [("a", "query"), ("b", "query"), ("a", "mutate"), ("b", "mutate")]
    assert [r["id"] for r in run.rows] == ["a1", "b1", "b2"]
    assert run.memory == [
        {"slug": "a", "label": "A", "role": "query", "peakRssMb": 50, "retainedHeapMb": 2.5, "retainedNativeMb": 0.5},
        {"slug": "b", "label": "B", "role": "mutate", "peakRssMb": 70},
    ]
    assert run.sizes == [{"slug": "a", "label": "A", "bytes": 123}, {"slug": "b", "label": "B", "bytes": None}]
    assert run.iters == {"query": 10}
    assert run.counts_by_slug == {"a": {"S": 1}, "b": {"S": 1}}
    assert run.labels == {"a": "A", "b": "B"}
    assert run.failures == [{"slug": "a", "error": "x"}]


def test_suite_output_keeps_the_python_json_keys(tmp_path, default_knobs):
    ds = prepare_dataset(tmp_path, 64, 8)
    run = SuiteRun(
        rows=[], memory=[], sizes=[], counts_by_slug={"a": {"S": 3, "full": 64}}, labels={},
        iters={"query": 10, "heavy": 3, "fullScan": 3}, failures=[],
    )

    out = suite_output(run, "prov", ds, 16, {"MemTotal": 100, "MemAvailable": 50}, ["w"])

    assert set(out) == {"provenance", "results", "memory", "sizes", "config"}
    assert out["provenance"] == "prov"
    assert out["config"] == {
        "triplesCount": 64,
        "cardinality": {"n_subj": 6, "n_pred": 35, "n_obj": 37, "n_graph": 11, "terms": 89},
        "matchedRows": {"S": 3, "full": 64},
        "mutBatch": 16,
        "queryIterations": 10,
        "heavyIterations": 3,
        "fullScanIterations": 3,
        "countWarnings": ["w"],
        "systemMemoryMb": 100,
        "availableMemoryMb": 50,
    }

    java = suite_output(run, "prov", ds, 16, {}, [], extra_config={"jdk": "x"}, failures=[])
    assert java["failures"] == []
    assert java["config"]["jdk"] == "x"
