"""Batch probes: answered in input order through a bounded window of
probes in flight, and a failing probe stops the rest."""

import time

import pytest

from vortex_rdf import CancelToken, Cancelled, VortexRdfError, VortexRdfStore, serialize_rdf

from conftest import NAME, u64_rows

ROWS = 300_000
PROBES = 8_000

#: Probes over the conftest fixture, as tuples and dicts: a bound
#: predicate, a subject, a subject with no rows, a window and every quad.
FIXTURE_PROBES = [
    (None, NAME, None, None),
    {"s": "<http://ex.org/bob>"},
    {"s": "<http://ex.org/nobody>"},
    {"p": NAME, "limit": 2, "offset": 1},
    {},
]


@pytest.fixture(scope="module", params=["file", "in-memory"])
def code_store(request, vortex_files):
    """The conftest fixture's Dictionary-layout store, opened mapped or
    loaded whole."""
    return VortexRdfStore(vortex_files["dictionary"], in_memory=request.param == "in-memory")


def _one_shot(store, probe):
    """`match_codes` of one batch probe."""
    return store.match_codes(**probe) if isinstance(probe, dict) else store.match_codes(*probe)


@pytest.fixture(scope="module")
def string_store(tmp_path_factory):
    """A Default-layout store: every probe scans its strings, and a probe
    with a `keep` fails (a keep needs the Dictionary layout)."""
    directory = tmp_path_factory.mktemp("batch")
    source = directory / "rows.nt"
    with source.open("w") as out:
        for i in range(ROWS):
            out.write(
                f"<http://ex.org/s/{i}> <http://ex.org/p/{i % 17}> "
                f"<http://ex.org/o/{i % 100_003}> .\n"
            )
    target = directory / "rows.vortex"
    serialize_rdf(source, target, format="ntriples", layout="default", indexes=[])
    return VortexRdfStore(target)


def test_a_failing_probe_aborts_the_rest_of_the_batch(string_store):
    slow = {"o": "<http://ex.org/o/7>"}
    failing = {"o": "<http://ex.org/o/7>", "keep": {"s": (0, 5)}}

    # What the work costs when it all runs.
    started = time.process_time()
    assert len(string_store.count_quads_many([slow] * PROBES)) == PROBES
    whole_batch = time.process_time() - started
    assert whole_batch > 0.5, "the batch has to be real work for this test to mean anything"

    with pytest.raises(VortexRdfError, match="keep needs the Dictionary layout"):
        string_store.count_quads_many([failing] + [slow] * PROBES)
    returned = time.process_time()
    time.sleep(1.5)

    burnt_after_the_error = time.process_time() - returned
    assert burnt_after_the_error < whole_batch / 4, (
        f"probes kept running after the call raised: {burnt_after_the_error:.2f} s of CPU "
        f"against {whole_batch:.2f} s for the whole batch"
    )


def test_answers_are_pulled_one_probe_at_a_time_in_input_order(code_store):
    answers = code_store.match_codes_many(FIXTURE_PROBES)
    assert iter(answers) is answers
    assert len(answers) == len(FIXTURE_PROBES)
    for probe in FIXTURE_PROBES:
        assert u64_rows(next(answers)) == u64_rows(_one_shot(code_store, probe)), probe
    for _ in range(2):
        with pytest.raises(StopIteration):
            next(answers)
    assert len(answers) == len(FIXTURE_PROBES)


def test_a_window_of_one_answers_like_the_default_window(code_store):
    one = [u64_rows(cols) for cols in code_store.match_codes_many(FIXTURE_PROBES, lookahead=1)]
    default = [u64_rows(cols) for cols in code_store.match_codes_many(FIXTURE_PROBES)]
    assert one == default
    counts = code_store.count_quads_many(FIXTURE_PROBES, lookahead=1)
    assert counts == code_store.count_quads_many(FIXTURE_PROBES)
    for many in (code_store.match_codes_many, code_store.count_quads_many):
        with pytest.raises(ValueError, match="lookahead"):
            many(FIXTURE_PROBES, lookahead=0)


def test_a_cancelled_token_raises_at_the_next_pull_then_stops(code_store):
    token = CancelToken()
    answers = code_store.match_codes_many(FIXTURE_PROBES, cancel=token)
    assert u64_rows(next(answers)) == u64_rows(_one_shot(code_store, FIXTURE_PROBES[0]))
    token.cancel()
    with pytest.raises(Cancelled):
        next(answers)
    for _ in range(2):
        with pytest.raises(StopIteration):
            next(answers)
    assert len(answers) == len(FIXTURE_PROBES)


def test_a_store_without_codes_answers_none_and_runs_no_probe(string_store):
    # Run, this probe fails: a keep needs the Dictionary layout.
    failing = {"o": "<http://ex.org/o/7>", "keep": {"s": (0, 5)}}
    answers = string_store.match_codes_many([failing] * 3)
    assert len(answers) == 3
    assert list(answers) == [None] * 3
