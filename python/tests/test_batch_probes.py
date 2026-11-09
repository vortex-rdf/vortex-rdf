"""Batch probes: answered in input order through a bounded window of
probes in flight, and a failing probe raises its error (`tests/batch.rs`
pins that it stops the rest)."""

import sys

import pytest

from vortex_rdf import CancelToken, Cancelled, VortexRdfError, VortexRdfStore

from conftest import NAME, one_shot, u64_rows

#: Probes over the conftest fixture, as tuples and dicts: a bound
#: predicate, a subject, a subject with no rows, a window and every quad.
FIXTURE_PROBES = [
    (None, NAME, None, None),
    {"s": "<http://ex.org/bob>"},
    {"s": "<http://ex.org/nobody>"},
    {"p": NAME, "limit": 2, "offset": 1},
    {},
]


@pytest.fixture(scope="module")
def string_store(vortex_files):
    """The conftest fixture's Default-layout store: its rows have no codes,
    and a probe with a `keep` fails (a keep needs the Dictionary layout)."""
    return VortexRdfStore(vortex_files["default"])


def test_a_failing_probe_raises_its_error_from_the_batch(string_store):
    """A batch larger than its window answers every probe, and a failing
    probe raises its error as the binding maps it."""
    probe = {"p": NAME}
    failing = {"p": NAME, "keep": {"s": (0, 5)}}
    assert string_store.count_quads_many([probe] * 3, lookahead=1) == [3, 3, 3]
    with pytest.raises(VortexRdfError, match="keep needs the Dictionary layout"):
        string_store.count_quads_many([failing] + [probe] * 3)


def test_answers_are_pulled_one_probe_at_a_time_in_input_order(dictionary_store):
    answers = dictionary_store.match_codes_many(FIXTURE_PROBES)
    assert iter(answers) is answers
    assert len(answers) == len(FIXTURE_PROBES)
    for probe in FIXTURE_PROBES:
        assert u64_rows(next(answers)) == u64_rows(one_shot(dictionary_store.match_codes, probe)), probe
    for _ in range(2):
        with pytest.raises(StopIteration):
            next(answers)
    assert len(answers) == len(FIXTURE_PROBES)


def test_a_window_of_one_answers_like_the_default_window(dictionary_store):
    one = [u64_rows(cols) for cols in dictionary_store.match_codes_many(FIXTURE_PROBES, lookahead=1)]
    default = [u64_rows(cols) for cols in dictionary_store.match_codes_many(FIXTURE_PROBES)]
    assert one == default
    counts = dictionary_store.count_quads_many(FIXTURE_PROBES, lookahead=1)
    assert counts == dictionary_store.count_quads_many(FIXTURE_PROBES)
    for many in (dictionary_store.match_codes_many, dictionary_store.count_quads_many):
        with pytest.raises(ValueError, match="lookahead"):
            many(FIXTURE_PROBES, lookahead=0)


def test_a_window_past_the_batch_answers_like_the_default_window(dictionary_store):
    widest = [
        u64_rows(cols)
        for cols in dictionary_store.match_codes_many(FIXTURE_PROBES, lookahead=sys.maxsize)
    ]
    assert widest == [u64_rows(cols) for cols in dictionary_store.match_codes_many(FIXTURE_PROBES)]
    counts = dictionary_store.count_quads_many(FIXTURE_PROBES, lookahead=sys.maxsize)
    assert counts == dictionary_store.count_quads_many(FIXTURE_PROBES)


def test_a_cancelled_token_refuses_the_calls_before_anything_runs(dictionary_store, string_store):
    cancelled = CancelToken()
    cancelled.cancel()
    # Run, this probe fails (a keep needs the Dictionary layout), so a call
    # that raises `Cancelled` for it checked the token before the probe's
    # error could surface.
    failing = {"o": "<http://ex.org/o/7>", "keep": {"s": (0, 5)}}
    for store in (dictionary_store, string_store):
        with pytest.raises(Cancelled):
            store.match_codes_many(FIXTURE_PROBES, cancel=cancelled)
        with pytest.raises(Cancelled):
            store.count_quads_many([failing], cancel=cancelled)
        with pytest.raises(Cancelled):
            store.count_quads(**failing, cancel=cancelled)
        # The arguments are checked before the token: a bad one raises
        # `ValueError`, not `Cancelled`.
        with pytest.raises(ValueError, match="batch_rows"):
            store.match_code_chunks(batch_rows=0, cancel=cancelled)
        with pytest.raises(ValueError, match="lookahead"):
            store.count_quads_many(FIXTURE_PROBES, lookahead=0, cancel=cancelled)
        with pytest.raises(ValueError, match="Invalid input"):
            store.count_quads(p="<bad", cancel=cancelled)
    # Where no code path applies, the token still refuses before the answer.
    for call in (string_store.match_codes, string_store.match_code_chunks):
        with pytest.raises(Cancelled):
            call(cancel=cancelled)
    # A batch of declined probes stops at its next pull once the token is
    # cancelled.
    token = CancelToken()
    answers = string_store.match_codes_many([failing] * 3, cancel=token)
    assert next(answers) is None
    token.cancel()
    with pytest.raises(Cancelled):
        next(answers)
    with pytest.raises(StopIteration):
        next(answers)


def test_a_cancelled_token_raises_at_the_next_pull_then_stops(dictionary_store):
    token = CancelToken()
    answers = dictionary_store.match_codes_many(FIXTURE_PROBES, cancel=token)
    assert u64_rows(next(answers)) == u64_rows(one_shot(dictionary_store.match_codes, FIXTURE_PROBES[0]))
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
