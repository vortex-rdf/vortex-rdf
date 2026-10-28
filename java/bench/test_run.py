"""Tests for the Java suite's driver, `java/bench/run.py`.

Run from python/ (whose environment has pytest):
    cd python && uv run --no-sync pytest ../java/bench/test_run.py -q
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

BENCH_DIR = Path(__file__).resolve().parent
FIXTURE = BENCH_DIR / "harness" / "src" / "test" / "resources" / "fixture"

# Loaded by path: python/bench/run.py, which the driver's imports put on
# sys.path, has the same module name. Registered before it runs, because a
# dataclass resolves its annotations through sys.modules.
_spec = importlib.util.spec_from_file_location("java_bench_run", BENCH_DIR / "run.py")
run = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = run
_spec.loader.exec_module(run)

SUBJECT_0 = "<http://data.example.org/resource/2026/subject/000000000>"
GRAPH_0 = "<http://data.example.org/graph/2026/named/000000>"


def test_probes_list_every_pattern_with_its_kind(tmp_path):
    path = run.write_probes(tmp_path / "probes.tsv", 512, run.DatasetOpts(graphs=8))

    lines = path.read_text(encoding="utf-8").splitlines()
    assert [line.split("\t")[0] for line in lines] == ["S", "P", "O", "SP", "PO", "SPO", "G", "SPOG", "full"]
    assert lines[0] == f"S\t{SUBJECT_0}\t\t\t\ttriples"
    assert lines[6] == f"G\t\t\t\t{GRAPH_0}\tquads"
    assert lines[-1] == "full\t\t\t\t\tfull"
    assert all(len(line.split("\t")) == 6 for line in lines)


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
