"""The code chunk iterator and the per-query cancel token: a match's chunks
concatenate to its one-shot `match_codes` columns on every backend, and a
cancelled token stops chunk pulls, batch pulls and the one-shot calls."""

import threading

import pytest

from vortex_rdf import CancelToken, Cancelled, VortexRdfError, VortexRdfStore

from conftest import serialized, u64_rows, u64s

ROWS = 1_000
#: A predicate on every seventh row: 143 rows spread over the store.
P3 = "<http://ex.org/p/3>"


@pytest.fixture(scope="module")
def files(tmp_path_factory):
    """One source as a Dictionary file, the same with a by-copy index, and
    as a Default-layout file, keyed by those names."""
    directory = tmp_path_factory.mktemp("streaming")
    rows = [
        f"<http://ex.org/s/{i % 97}> <http://ex.org/p/{i % 7}> <http://ex.org/o/{i}> ."
        for i in range(ROWS)
    ]
    return {
        name: serialized(directory, name, rows, layout=layout, indexes=indexes)
        for name, layout, indexes in (
            ("dictionary", "dictionary", []),
            ("served", "dictionary", ["secondary-by-copy"]),
            ("default", "default", []),
        )
    }


@pytest.fixture(scope="module", params=["file", "in-memory", "bytes", "served"])
def backend(request, files):
    """`(name, store)`: the Dictionary file mapped, loaded whole, or adopted
    from its bytes, or the by-copy-indexed file, whose index serves `P3`."""
    if request.param == "bytes":
        return request.param, VortexRdfStore.from_bytes(files["dictionary"].read_bytes())
    path = files["served" if request.param == "served" else "dictionary"]
    return request.param, VortexRdfStore(path, in_memory=request.param == "in-memory")


@pytest.fixture(scope="module")
def store(files):
    return VortexRdfStore(files["dictionary"])


@pytest.mark.parametrize("batch_rows", [7, 65536])
def test_chunks_concatenate_to_match_codes(backend, batch_rows):
    name, store = backend
    keep = {"o": store.term_dict().prefix_range("<http://ex.org/o/1")}
    for narrowing in ({}, {"keep": keep}, {"limit": 50, "offset": 13}):
        whole = [u64s(col) for col in store.match_codes(p=P3, **narrowing)]
        assert whole[0], narrowing
        for columns in (("o",), ("s", "p", "o", "g")):
            chunks = store.match_code_chunks(
                p=P3, **narrowing, columns=columns, batch_rows=batch_rows
            )
            assert iter(chunks) is chunks
            assert (chunks.columns, chunks.batch_rows) == (columns, batch_rows)
            got = [[] for _ in columns]
            sizes = []
            for chunk in chunks:
                assert len(chunk) == len(columns)
                sizes.append(len(chunk[0]))
                for values, column in zip(got, chunk):
                    values.extend(u64s(column))
            assert got == [whole["spog".index(c)] for c in columns], (narrowing, columns)
            assert all(0 < size <= batch_rows for size in sizes), sizes
            if name in ("in-memory", "bytes"):
                assert sizes[:-1] == [batch_rows] * (len(sizes) - 1), sizes
            for _ in range(2):
                with pytest.raises(StopIteration):
                    next(chunks)


def test_chunks_are_none_without_codes_and_refuse_bad_arguments(files, store):
    default = VortexRdfStore(files["default"])
    assert default.match_code_chunks() is None
    for bad in ({"batch_rows": 0}, {"columns": ()}, {"columns": ("x",)}):
        for each in (store, default):
            with pytest.raises(ValueError):
                each.match_code_chunks(**bad)


def test_cancel_before_the_first_pull(store):
    token = CancelToken()
    chunks = store.match_code_chunks(p=P3, batch_rows=7, cancel=token)
    assert not token.cancelled
    token.cancel()
    assert token.cancelled
    with pytest.raises(Cancelled):
        next(chunks)


def test_cancel_after_exhaustion_still_stops(store):
    token = CancelToken()
    chunks = store.match_code_chunks(p=P3, batch_rows=7, cancel=token)
    assert sum(len(chunk[0]) for chunk in chunks) == store.count_quads(p=P3)
    token.cancel()
    with pytest.raises(StopIteration):
        next(chunks)


def test_next_after_an_error_stops(store):
    """`Cancelled` is the error a chunk pull raises on a store the bindings
    cannot change; once raised, the iterator is spent."""
    token = CancelToken()
    chunks = store.match_code_chunks(p=P3, batch_rows=7, cancel=token)
    assert 0 < len(next(chunks)[0]) <= 7
    token.cancel()
    with pytest.raises(Cancelled):
        next(chunks)
    for _ in range(2):
        with pytest.raises(StopIteration):
            next(chunks)


def test_a_shared_token_ends_chunks_and_batch(store):
    token = CancelToken()
    chunks = store.match_code_chunks(p=P3, batch_rows=7, cancel=token)
    answers = store.match_codes_many([(None, P3, None, None)] * 4, cancel=token)
    next(chunks)
    next(answers)
    canceller = threading.Thread(target=token.cancel)
    canceller.start()
    canceller.join()
    assert token.cancelled
    for pulled in (chunks, answers):
        with pytest.raises(Cancelled):
            next(pulled)
        with pytest.raises(StopIteration):
            next(pulled)


def test_one_shot_calls_race_a_token(store):
    assert issubclass(Cancelled, VortexRdfError)
    live = CancelToken()
    probes = [(None, P3, None, None), {"s": "<http://ex.org/s/5>"}]
    assert u64_rows(store.match_codes(p=P3, cancel=live)) == u64_rows(store.match_codes(p=P3))
    assert store.count_quads(p=P3, cancel=live) == store.count_quads(p=P3)
    assert store.count_quads_many(probes, cancel=live) == store.count_quads_many(probes)

    cancelled = CancelToken()
    cancelled.cancel()
    for call in (
        lambda: store.match_codes(p=P3, cancel=cancelled),
        lambda: store.count_quads(p=P3, cancel=cancelled),
        lambda: store.count_quads_many(probes, cancel=cancelled),
        lambda: store.match_code_chunks(p=P3, cancel=cancelled),
    ):
        with pytest.raises(Cancelled):
            call()
    # A call without the token ignores its state.
    assert store.count_quads(p=P3) == len(store.match_codes(p=P3)[0])
