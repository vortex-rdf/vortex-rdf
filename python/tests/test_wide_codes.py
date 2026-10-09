"""Term codes are u64: a code past ``u32::MAX`` crosses the Python boundary
unchanged in both directions, and what is no u64 is refused.

The fixture's dictionary holds a handful of terms, so a code past
``u32::MAX`` names none of them — ``code + 2**32`` is the code a 32-bit
binding would narrow back onto term ``code``, which makes every check below a
truncation detector: a narrowed code would decode, filter or keep a real
term. The Rust core runs the full round trip, with dictionaries numbered past
``u32::MAX`` by a test hook (``core/src/tests/wide_codes.rs``).
"""

from array import array

import pytest

import vortex_rdf
from vortex_rdf import U64Column, VortexRdfStore

#: Past ``u32::MAX``, and 5 modulo 2**32.
WIDE = (1 << 32) + 5
#: The largest u64.
TOP = (1 << 64) - 1
#: Out of the u64 range on either side.
OUT_OF_RANGE = [-1, 1 << 64]


def _values(col):
    return memoryview(col).cast("Q").tolist()


@pytest.fixture(params=["resident", "file-backed"])
def store(request, vortex_files):
    """The dictionary-layout fixture loaded whole or opened mapped."""
    return VortexRdfStore(vortex_files["dictionary"], in_memory=request.param == "resident")


def test_the_column_type_is_u64_and_has_no_u32_alias():
    assert not hasattr(vortex_rdf, "U32Column")
    assert "U64Column" in vortex_rdf.__all__


def test_u64_column_carries_codes_past_u32():
    col = U64Column([WIDE, 0, TOP, WIDE])
    assert len(col) == 4
    assert memoryview(col).nbytes == 32
    assert _values(col) == [WIDE, 0, TOP, WIDE]
    # Every accepted input form keeps the values whole.
    for form in (col, memoryview(col).cast("Q"), array("Q", [WIDE, 0, TOP, WIDE]), bytes(memoryview(col))):
        assert _values(U64Column(form)) == [WIDE, 0, TOP, WIDE]
    # A u32 buffer widens.
    assert _values(U64Column(array("I", [5, 7]))) == [5, 7]
    # The kernels carry them through.
    assert _values(col.distinct()) == [WIDE, 0, TOP]
    values, counts = col.value_counts()
    assert (_values(values), _values(counts)) == ([WIDE, 0, TOP], [2, 1, 1])
    assert _values(col.take([2, 0, 3])) == [TOP, WIDE, WIDE]
    left, right = col.join_indices(U64Column([TOP, WIDE, 5]))
    assert list(zip(_values(left), _values(right))) == [(0, 1), (2, 0), (3, 1)]
    # A code that narrows to 5 does not join 5.
    left, _ = U64Column([5]).join_indices([WIDE])
    assert len(left) == 0


def test_u64_column_take_refuses_indices_that_would_wrap():
    col = U64Column([10, 20])
    for index in ((1 << 32) + 1, TOP):
        with pytest.raises(IndexError):
            col.take([0, index])


@pytest.mark.parametrize("bad", OUT_OF_RANGE)
def test_u64_column_refuses_values_outside_u64(bad):
    with pytest.raises((ValueError, OverflowError)):
        U64Column([1, bad])
    with pytest.raises((ValueError, OverflowError)):
        U64Column([1]).take([bad])
    with pytest.raises((ValueError, OverflowError)):
        U64Column([1]).join_indices([bad])


def test_u64_column_refuses_a_partial_code():
    # The raw byte view is read as whole u64s.
    with pytest.raises(ValueError, match="u64"):
        U64Column(b"\x01\x02\x03\x04")


def test_match_codes_hands_out_u64_columns(store):
    cols = store.match_codes()
    assert all(isinstance(col, U64Column) for col in cols)
    assert all(memoryview(col).nbytes == 8 * len(col) for col in cols)
    term_dict = store.term_dict()
    rows = sorted(zip(*(term_dict.decode_many(memoryview(col).cast("Q")) for col in cols)))
    assert rows == sorted(store.get_quads())


def test_term_dict_takes_codes_past_u32(store):
    term_dict = store.term_dict()
    n = len(term_dict)
    every = list(range(n))
    wide = [code + (1 << 32) for code in every]
    for code in wide:
        assert term_dict.decode(code) is None
    assert term_dict.decode(TOP) is None
    assert term_dict.decode_many(wide) == [None] * n
    assert term_dict.decode_many(U64Column(wide)) == [None] * n
    assert term_dict.decode_many(array("Q", wide)) == [None] * n
    # The narrow codes still decode, beside the wide ones.
    assert term_dict.decode_many(every + wide) == term_dict.decode_many(every) + [None] * n
    for code in (WIDE, TOP):
        with pytest.raises(ValueError, match=str(code)):
            term_dict.filter_codes("is_iri", "", [code])
        with pytest.raises(ValueError, match=str(code)):
            term_dict.filter_codes("is_iri", "", U64Column([0, code]))


@pytest.mark.parametrize("bad", OUT_OF_RANGE)
def test_term_dict_refuses_codes_outside_u64(store, bad):
    term_dict = store.term_dict()
    with pytest.raises((ValueError, OverflowError)):
        term_dict.decode(bad)
    with pytest.raises((ValueError, OverflowError)):
        term_dict.decode_many([0, bad])
    with pytest.raises(ValueError):
        term_dict.filter_codes("is_iri", "", [0, bad])


def test_keeps_take_codes_past_u32(store):
    term_dict = store.term_dict()
    alice = term_dict.encode("<http://ex.org/alice>")
    rows = store.count_quads(keep={"s": [alice]})
    assert rows == 2
    wide = alice + (1 << 32)
    for keep in (
        {"s": [wide]},
        {"s": U64Column([wide])},
        {"s": array("Q", [wide])},
        {"s": range(wide, wide + 1)},
        {"s": (wide, wide + 1)},
        {0: (wide, TOP)},
    ):
        assert all(len(col) == 0 for col in store.match_codes(keep=keep)), keep
        assert store.count_quads(keep=keep) == 0, keep
    # A wide code beside a narrow one admits the narrow one's rows only.
    assert store.count_quads(keep={"s": [alice, wide]}) == rows
    assert store.count_quads_many([{"keep": {"s": [wide]}}, {"keep": {"s": [alice]}}]) == [0, rows]
    many = store.match_codes_many([{"keep": {"s": U64Column([wide])}}])
    assert all(len(col) == 0 for col in many[0])


@pytest.mark.parametrize("bad", OUT_OF_RANGE)
def test_keeps_refuse_codes_outside_u64(store, bad):
    for keep in ({"s": [0, bad]}, {"s": (0, bad)}, {"s": (bad, 1)}):
        with pytest.raises(ValueError):
            store.match_codes(keep=keep)
        with pytest.raises(ValueError):
            store.count_quads(keep=keep)
