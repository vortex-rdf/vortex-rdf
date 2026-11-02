"""Tests for `measure` in `worker.py`, the timing rule every Python cell goes through."""

from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import worker  # noqa: E402


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
