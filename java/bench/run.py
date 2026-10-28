#!/usr/bin/env python3
"""Orchestrate the Java comparative benchmark and emit `target/bench/java.json`.

    python3 java/bench/run.py --fixture DIR   # write the harness's test fixture

The dataset generator and the orchestration are python/bench's (`datasets.py`
and `suite.py`), so the Java tab measures the Python tab's data by the same
rules. This file writes the files a Java worker reads besides the dataset: the
query probes and the two mutation batches.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

BENCH_DIR = Path(__file__).resolve().parent
REPO_ROOT = BENCH_DIR.parent.parent
sys.path.insert(0, str(REPO_ROOT / "python" / "bench"))

from datasets import DatasetOpts, dataset_prefix, dataset_probes, fresh_quads, write_dataset  # noqa: E402
from suite import Dataset  # noqa: E402

#: The harness's test fixture: small enough to commit, with named graphs so
#: the graph probes match rows, and its own fixed options rather than the
#: environment's, so regenerating it always gives the same bytes.
FIXTURE_ROWS = 512
FIXTURE_GRAPHS = 8
FIXTURE_BATCH = 16


@dataclass(frozen=True)
class WorkerInputs:
    """The files a worker reads besides the dataset itself."""

    probes: Path
    fresh: Path
    prefix: Path
    empty: Path


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


def main(argv: list[str]) -> int:
    if len(argv) == 2 and argv[0] == "--fixture":
        write_fixture(Path(argv[1]))
        return 0
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
