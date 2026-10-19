"""The native pushdown primitives: the dictionary handle under both
residencies, spelling-tolerant encoding, byte-order ranges and term
predicates."""

import re
import time
import warnings
from array import array

import pytest

from vortex_rdf import U64Column, VortexRdfStore, serialize_rdf

from conftest import INDEXES, NAME, PATTERNS, u64_rows, u64s

XSD = "http://www.w3.org/2001/XMLSchema#"


@pytest.fixture(params=["resident", "file-backed"])
def dictionary(request, vortex_files):
    """The fixture's dictionary-layout store loaded whole (resident
    dictionary) or opened from its file (mapped, file-backed dictionary), as
    `(store, term_dict)`."""
    path = vortex_files["dictionary"]
    store = VortexRdfStore(path, in_memory=request.param == "resident")
    term_dict = store.term_dict()
    assert term_dict is not None
    assert term_dict.file_backed == (request.param == "file-backed")
    return store, term_dict


def _terms(term_dict):
    return term_dict.decode_many(list(range(len(term_dict))))


def test_encode_is_spelling_tolerant(dictionary):
    _, term_dict = dictionary
    alice = term_dict.encode("<http://ex.org/alice>")
    assert alice is not None
    assert term_dict.encode("http://ex.org/alice") == alice
    plain = term_dict.encode('"Alice"')
    assert term_dict.encode(f'"Alice"^^<{XSD}string>') == plain
    assert term_dict.encode('"Bob"@EN') == term_dict.encode('"Bob"@en') is not None
    assert term_dict.encode("") == term_dict.encode("default") == term_dict.encode("[]") == 0
    assert term_dict.encode("_:b0") is not None
    assert term_dict.encode("<http://ex.org/nobody>") is None
    assert term_dict.encode('"Alice"@fr') is None
    for malformed in ['"unterminated', "<no closing", '"x"^^', '"x"@']:
        with pytest.raises(ValueError):
            term_dict.encode(malformed)


def test_encode_many_matches_encode(dictionary):
    _, term_dict = dictionary
    terms = _terms(term_dict)
    probes = terms + ["http://ex.org/alice", '"Bob"@EN', "default", "<http://ex.org/nobody>"]
    assert term_dict.encode_many(probes) == [term_dict.encode(t) for t in probes]
    assert term_dict.encode_many([]) == []
    with pytest.raises(ValueError):
        term_dict.encode_many(["<http://ex.org/alice>", '"unterminated'])


def test_decode_many_any_order_with_repeats(dictionary):
    _, term_dict = dictionary
    terms = _terms(term_dict)
    n = len(terms)
    codes = [n - 1, 0, 3, 3, n, 1, n + 5, 0]
    want = [terms[c] if c < n else None for c in codes]
    assert term_dict.decode_many(codes) == want
    assert [term_dict.decode(c) for c in codes] == want
    assert term_dict.decode_many([]) == []


@pytest.fixture(params=["resident", "file-backed"])
def wide_dictionary(request, tmp_path_factory):
    """A dictionary of 602 terms (300 subjects, one predicate, 300 literals
    and the default graph), resident or file-backed, as `(term_dict,
    spellings)`: the sorted N-Triples spellings, so a term's code is its
    index."""
    root = tmp_path_factory.mktemp("wide")
    source = root / "wide.nt"
    source.write_text(
        "".join(
            f'<http://ex.org/s{i:03}> <http://ex.org/p> "v{i:03}" .\n' for i in range(300)
        ),
        encoding="utf-8",
    )
    out = root / "wide.vortex"
    serialize_rdf(source, out, layout="dictionary")
    spellings = {"", "<http://ex.org/p>"}
    spellings |= {f"<http://ex.org/s{i:03}>" for i in range(300)}
    spellings |= {f'"v{i:03}"' for i in range(300)}
    store = VortexRdfStore(out, in_memory=request.param == "resident")
    term_dict = store.term_dict()
    assert term_dict.file_backed == (request.param == "file-backed")
    return term_dict, sorted(spellings, key=lambda t: t.encode())


def test_decode_many_colliding_repeats_unsorted_and_out_of_range(wide_dictionary):
    """Codes 256 apart share a bucket of the batch's sharing cache, so
    alternating them repeats codes in the batch the dictionary is asked for;
    unsorted and out-of-range codes (the first past the end, the last past
    any dictionary) ride along."""
    term_dict, spellings = wide_dictionary
    n = len(term_dict)
    assert n == len(spellings) == 602
    codes = [3, 259, 3, 515, 259, n + 1, 3, 515, n, 259, 0, n - 1, 515, 3, 2**63]
    want = [spellings[c] if c < n else None for c in codes]
    assert want.count(None) == 3
    assert term_dict.decode_many(codes) == want
    assert term_dict.decode_many(array("Q", codes)) == want
    assert [term_dict.decode(c) for c in codes] == want
    # Every code, descending, then ascending: nothing lost to the sharing.
    everything = list(range(n))
    assert term_dict.decode_many(everything) == spellings
    assert term_dict.decode_many(everything[::-1]) == spellings[::-1]


# The bucket count of the batch's sharing cache (`RECENT_BUCKETS` in
# `src/codes.rs`): codes this far apart evict one another.
RECENT_BUCKETS = 256


def test_decode_many_shares_strings_when_the_decoded_batch_repeats_a_code(wide_dictionary):
    """`[3, 3, 3 + RECENT_BUCKETS, 3]` decodes fewer distinct terms than it
    has occurrences (the second 3 shares the first's slot) while the last 3
    was evicted by its bucket-mate and is decoded again, so the batch handed
    to the dictionary repeats code 3: the branch that clones the strings
    shared between occurrences runs, over repeats inside the decoded batch."""
    term_dict, spellings = wide_dictionary
    n = len(term_dict)
    far = 3 + RECENT_BUCKETS
    codes = [3, 3, far, 3]
    got = term_dict.decode_many(codes)
    assert got == [spellings[3], spellings[3], spellings[far], spellings[3]]
    # Equal codes give equal strings, and the adjacent repeat is one object.
    assert got[0] == got[1] == got[3]
    assert got[0] is got[1]
    # The same shape with out-of-range codes riding along, as a buffer too.
    codes = [3, 3, n + 1, far, 3, n, 3, 3, far, far]
    want = [spellings[c] if c < n else None for c in codes]
    assert term_dict.decode_many(codes) == want
    assert term_dict.decode_many(array("Q", codes)) == want


def test_decode_many_of_a_batch_longer_than_the_text_reservation(wide_dictionary):
    """A batch of 240,800 codes asks for far more text than the batch decode
    reserves up front (1 MiB at most): the buffer grows as the terms are
    written and every string comes out right, in order, with the
    out-of-range codes among them as `None`."""
    term_dict, spellings = wide_dictionary
    n = len(term_dict)
    # Cycling through the codes defeats the sharing cache, so almost every
    # occurrence is decoded again.
    codes = list(range(n)) * 400
    got = term_dict.decode_many(codes)
    assert len(got) == len(codes)
    assert got == spellings * 400
    beyond = list(range(n, n + 100_000))
    assert term_dict.decode_many(beyond) == [None] * len(beyond)


def test_prefix_range_and_lower_bound(dictionary):
    """One call of each probe per residency: a present term, a probe past
    every term and the empty prefix. The byte-order sweep against brute
    force runs in core, on the same handle (`core/src/tests/dictionary_handle.rs`)."""
    _, term_dict = dictionary
    n = len(term_dict)
    alice = term_dict.encode("<http://ex.org/alice>")
    assert term_dict.lower_bound("<http://ex.org/alice>") == alice
    assert term_dict.lower_bound("~") == n
    assert term_dict.prefix_range("<http://ex.org/alice>") == (alice, alice + 1)
    assert term_dict.prefix_range("") == (0, n)


def test_filter_codes_validates_its_arguments(dictionary):
    """An unknown kind, a malformed argument or an option the kind does not
    take raises `ValueError`. Candidates must be sorted, unique and inside the
    dictionary, however they are passed; none at all is two empty columns."""
    _, term_dict = dictionary
    n = len(term_dict)
    every = U64Column(range(n))
    for kind, arg, options in [
        ("no_such_kind", "", {}),
        ("datatype", "", {}),
        ("num_lt", "abc", {}),
        ("lang_matches", "", {}),
        ("contains", "not a spelling", {}),
        ("contains", '"a"', {"case": "title"}),
        ("num_lt", "5", {"as_str": True}),
    ]:
        with pytest.raises(ValueError):
            term_dict.filter_codes(kind, arg, every, **options)

    def forms(codes):
        """The same candidates as every input the call accepts."""
        column = U64Column(codes)
        return [
            column,
            list(codes),
            tuple(codes),
            array("Q", codes),
            memoryview(array("Q", codes)),
            bytes(memoryview(column)),
        ]

    for codes in ([3, 1], [1, 1], [0, n], [n + 7], [n - 1, 0]):
        for candidates in forms(codes):
            with pytest.raises(ValueError):
                term_dict.filter_codes("is_iri", "", candidates)
    for candidates in forms([]):
        passed, undecided = term_dict.filter_codes("is_iri", "", candidates)
        assert len(passed) == len(undecided) == 0
    # What is no list of u64 codes at all is as bad a value.
    for candidates in ([-1], [1 << 64], [0.5], b"\x01\x02\x03", "abc", None, 5):
        with pytest.raises(ValueError):
            term_dict.filter_codes("is_iri", "", candidates)
    # A valid list answers alike in every form, as a pair of columns.
    iri = [c for c, t in enumerate(_terms(term_dict)) if t.startswith("<")]
    for candidates in forms(iri[:3] + [n - 1]):
        passed, undecided = term_dict.filter_codes("is_iri", "", candidates)
        assert isinstance(passed, U64Column) and isinstance(undecided, U64Column)
        assert (u64s(passed), u64s(undecided)) == (iri[:3], [])


@pytest.mark.parametrize("api", ["decode", "decode_many"])
def test_match_codes_decode_to_get_quads(dictionary, api):
    """Decoding the code columns position by position reproduces `get_quads`,
    under either residency and through either decode call.

    `get_quads` assembles its rows from those same columns, so this pins the
    decode-and-zip assembly: columns swapped or misaligned would still yield
    plausible-looking rows of real terms.
    """
    store, term_dict = dictionary
    assert repr(term_dict) == f"TermDict(len={len(term_dict)}, file_backed={term_dict.file_backed})"
    for pattern in PATTERNS:
        cols = store.match_codes(**pattern)
        assert cols is not None
        if api == "decode":
            decoded = [[term_dict.decode(code) for code in u64s(col)] for col in cols]
        else:
            decoded = [term_dict.decode_many(col) for col in cols]
        assert sorted(zip(*decoded)) == sorted(store.get_quads(**pattern)), pattern


# ─── keeps, windows, batches ──────────────────────────────────────────────


@pytest.fixture(params=["file", "in-memory"])
def code_store(request, vortex_files):
    """The fixture's dictionary-layout store under each open mode."""
    store = VortexRdfStore(vortex_files["dictionary"], in_memory=request.param == "in-memory")
    assert store.term_dict().file_backed == (request.param == "file")
    return store


def test_keep_narrows_like_filtering(code_store):
    store = code_store
    term_dict = store.term_dict()
    all_rows = u64_rows(store.match_codes())
    name = term_dict.encode(NAME)
    knows = term_dict.encode("<http://xmlns.com/foaf/0.1/knows>")
    alice = term_dict.encode("<http://ex.org/alice>")
    lit_lo, lit_hi = term_dict.prefix_range('"')
    iri_lo, iri_hi = term_dict.prefix_range("<")

    def expect(position, admit):
        return sorted(r for r in all_rows if admit(r[position]))

    cases = [
        ({"p": [name, knows]}, 1, lambda c: c in (name, knows)),
        ({"p": [knows, knows, name]}, 1, lambda c: c in (name, knows)),
        ({"p": range(name, name + 1)}, 1, lambda c: c == name),
        ({1: (knows, name + 1)}, 1, lambda c: knows <= c <= name),
        ({"o": range(lit_lo, lit_hi)}, 2, lambda c: lit_lo <= c < lit_hi),
        ({"o": term_dict.filter_codes("is_iri", "", U64Column(range(len(term_dict))))[0]}, 2, lambda c: iri_lo <= c < iri_hi),
        ({"s": memoryview(term_dict.filter_codes("is_blank", "", U64Column(range(len(term_dict))))[0])}, 0, lambda c: term_dict.decode(c).startswith("_:")),
        ({"s": [alice]}, 0, lambda c: c == alice),
        ({"g": [0]}, 3, lambda c: c == 0),
        ({"p": []}, 1, lambda c: False),
        ({"p": range(5, 5)}, 1, lambda c: False),
    ]
    for keep, position, admit in cases:
        want = expect(position, admit)
        assert sorted(u64_rows(store.match_codes(keep=keep))) == want, keep
        assert store.count_quads(keep=keep) == len(want), keep
        assert store.count_quads(keep=keep, limit=1) == min(1, len(want)), keep
    # Keeps compose with a pattern and with each other.
    want = sorted(r for r in all_rows if r[1] == name and lit_lo <= r[2] < lit_hi)
    got = store.match_codes(p=NAME, keep={"o": range(lit_lo, lit_hi)})
    assert sorted(u64_rows(got)) == want
    got = store.match_codes(keep={"o": (lit_lo, lit_hi), "p": [name]})
    assert sorted(u64_rows(got)) == want
    assert store.count_quads(p=NAME, keep={"o": [lit_lo]}) == len(
        [r for r in all_rows if r[1] == name and r[2] == lit_lo]
    )


def test_keep_rejects_bad_specs(code_store):
    store = code_store
    for keep in [
        "p",
        {"x": [1]},
        {4: [1]},
        {"p": range(0, 10, 2)},
        {"p": range(-1, 3)},
        {"p": [-1]},
        {"p": [1 << 64]},
        {"p": (0, 1 << 64)},
        {"p": [1], 1: [2]},
    ]:
        with pytest.raises(ValueError):
            store.match_codes(keep=keep)
        with pytest.raises(ValueError):
            store.count_quads(keep=keep)
    # A code past the dictionary, but inside the u64 code range, is a valid
    # keep that matches nothing.
    assert store.count_quads(keep={"p": [1 << 40]}) == 0
    assert store.count_quads(keep={"p": (1 << 40, (1 << 64) - 1)}) == 0
    # A pair of ints is a range; any other int sequence is a set.
    assert store.count_quads(keep={"p": (1, 2, 3)}) == store.count_quads(keep={"p": [1, 2, 3]})
    assert store.count_quads(keep={"p": (0, (1 << 64) - 1)}) == store.count_quads()


def test_keep_needs_codes(vortex_files):
    strings = VortexRdfStore(vortex_files["default"])
    assert strings.match_codes(keep={"p": [1]}) is None
    with pytest.raises(Exception):
        strings.count_quads(keep={"p": [1]})
    # Windows and capped counts work on every layout.
    assert strings.count_quads(limit=2) == 2
    assert strings.count_quads(limit=100) == 5


def test_window_slices_base_order(code_store):
    store = code_store
    rows = u64_rows(store.match_codes())
    n = len(rows)
    for offset, limit in [(0, 2), (1, 3), (3, 10), (n, 1), (0, 0), (2, None)]:
        got = store.match_codes(limit=limit, offset=offset)
        want = rows[offset:] if limit is None else rows[offset : offset + limit]
        assert u64_rows(got) == want, (offset, limit)
        assert store.count_quads(limit=limit, offset=offset) == len(want), (offset, limit)
    named = u64_rows(store.match_codes(p=NAME))
    assert u64_rows(store.match_codes(p=NAME, limit=2, offset=1)) == named[1:3]
    assert store.count_quads(p=NAME, limit=2) == 2
    assert store.count_quads(p=NAME, limit=10) == len(named)
    assert store.count_quads(s="<http://ex.org/nobody>", limit=1) == 0


def test_many_matches_singles(code_store, vortex_files):
    store = code_store
    term_dict = store.term_dict()
    lit_lo, lit_hi = term_dict.prefix_range('"')
    probes = [
        (None, None, None, None),
        (None, NAME, None, None),
        ["<http://ex.org/bob>", None, None, None],
        {"s": "<http://ex.org/nobody>"},
        {"p": NAME, "keep": {"o": range(lit_lo, lit_hi)}, "limit": 2, "offset": 1},
        {"keep": {"g": [0]}, "limit": 3},
        {},
    ]
    many = store.match_codes_many(probes)
    counts = store.count_quads_many(probes)
    assert len(many) == len(counts) == len(probes)
    for probe, cols, count in zip(probes, many, counts):
        if isinstance(probe, dict):
            single = store.match_codes(**probe)
            single_count = store.count_quads(**probe)
        else:
            single = store.match_codes(*probe)
            single_count = store.count_quads(*probe)
        assert u64_rows(cols) == u64_rows(single), probe
        assert count == single_count == len(u64_rows(cols)), probe
    assert list(store.match_codes_many([])) == []
    assert store.count_quads_many([]) == []
    # Parse-all-first: a malformed probe raises before any evaluation.
    for bad in [[(None, None, None)], [{"bogus": 1}], ["<x>"], [(None, "not a term", None, None)]]:
        with pytest.raises(ValueError):
            store.match_codes_many(bad)
        with pytest.raises(ValueError):
            store.count_quads_many(bad)
    # A malformed term names its slot in a tuple probe as in a dict probe.
    for bad in ([(None, "not a term", None, None)], [{"p": "not a term"}]):
        with pytest.raises(ValueError, match=r"^Invalid input: pattern predicate: "):
            store.match_codes_many(bad)
        with pytest.raises(ValueError, match=r"^Invalid input: pattern predicate: "):
            store.count_quads_many(bad)
    strings = VortexRdfStore(vortex_files["default"])
    assert list(strings.match_codes_many(probes[:2])) == [None, None]
    assert strings.count_quads_many(probes[:2]) == [5, 3]


def test_column_kernels():
    from vortex_rdf import U64Column

    col = U64Column([3, 1, 3, 2, 1, 3])
    assert len(col) == 6 and u64s(col) == [3, 1, 3, 2, 1, 3]
    assert u64s(U64Column(col)) == u64s(col)
    assert u64s(U64Column(memoryview(col))) == u64s(col)
    assert u64s(U64Column(array("Q", [7, 8]))) == [7, 8]
    assert u64s(col.distinct()) == [3, 1, 2]
    values, counts = col.value_counts()
    assert u64s(values) == [3, 1, 2]
    assert u64s(counts) == [3, 2, 1]
    for indices in ([5, 0, 1], U64Column([5, 0, 1]), array("Q", [5, 0, 1])):
        assert u64s(col.take(indices)) == [3, 3, 1]
    assert u64s(col.take(col.distinct())) == [2, 1, 3]
    assert u64s(col.take([])) == []
    with pytest.raises(IndexError):
        col.take([6])
    right = [1, 9, 3, 3]
    for other in (U64Column(right), right, array("Q", right)):
        left_idx, right_idx = col.join_indices(other)
        pairs = list(zip(u64s(left_idx), u64s(right_idx)))
        want = [(i, j) for i, l in enumerate([3, 1, 3, 2, 1, 3]) for j, r in enumerate(right) if l == r]
        assert pairs == want
    empty = U64Column([])
    assert len(empty.distinct()) == 0
    assert len(empty.join_indices(right)[0]) == 0
    assert len(col.join_indices(empty)[1]) == 0
    with pytest.raises((ValueError, OverflowError, TypeError)):
        U64Column([-1])


def test_slice_is_a_window_of_the_column():
    col = U64Column([3, 1, 3, 2, 1, 3])
    assert u64s(col.slice(2, 5)) == u64s(col)[2:5] == [3, 2, 1]
    assert u64s(col.slice(0, 6)) == u64s(col)
    assert len(col.slice(3, 3)) == len(col.slice(6, 6)) == 0
    with pytest.raises(IndexError):
        col.slice(2, 7)
    with pytest.raises(IndexError):
        col.slice(4, 3)


def test_slice_shares_the_column_instead_of_copying_it():
    """1,000 slices of nearly all of a 10-million-row column: as copies they
    would move 80 GB."""
    rows = 10_000_000
    col = U64Column(bytes(8 * rows))
    started = time.process_time()
    for start in range(1_000):
        window = col.slice(start, rows - start)
    spent = time.process_time() - started
    assert len(window) == rows - 2 * 999
    assert spent < 1.0, f"1,000 slices took {spent:.2f} s of CPU: a slice copies"


def test_join_count_counts_the_pairs_join_indices_makes():
    col = U64Column([3, 1, 3, 2, 1, 3])
    right = [1, 9, 3, 3]
    empty = U64Column([])
    cases = [(col, U64Column(right)), (col, right), (col, array("Q", right)), (empty, right), (col, empty)]
    for left, other in cases:
        assert left.join_count(other) == len(left.join_indices(other)[0])
    assert col.join_count(right) == 8


def test_join_indices_refuses_more_pairs_than_max_pairs():
    from vortex_rdf import JoinTooLarge, VortexRdfError

    col = U64Column([3, 1, 3, 2, 1, 3])
    right = [1, 9, 3, 3]
    pairs = col.join_count(right)
    with pytest.raises(JoinTooLarge) as refused:
        col.join_indices(right, max_pairs=pairs - 1)
    message, count = refused.value.args
    assert count == pairs and str(pairs) in message
    assert f"max_pairs of {pairs - 1}" in message, "the message names the cap"
    assert isinstance(refused.value, VortexRdfError)
    capped = col.join_indices(right, max_pairs=pairs)
    assert [u64s(c) for c in capped] == [u64s(c) for c in col.join_indices(right)]


def test_groups_join_slice_by_slice_like_join_indices():
    from vortex_rdf import JoinTooLarge

    left = U64Column([3, 1, 3, 2, 1, 3])
    right = U64Column([1, 9, 3, 3])
    groups = right.groups()
    assert len(groups) == len(right)
    assert groups.count(left) == left.join_count(right) == 8
    want = [u64s(c) for c in left.join_indices(right)]
    # Two-row slices make at most three pairs each.
    for max_pairs in (None, 3):
        got_left, got_right = [], []
        for start in range(0, len(left), 2):
            left_idx, right_idx = groups.join(left.slice(start, start + 2), max_pairs=max_pairs)
            got_left += [start + i for i in u64s(left_idx)]
            got_right += u64s(right_idx)
        assert [got_left, got_right] == want, max_pairs
    with pytest.raises(JoinTooLarge) as refused:
        groups.join(left, max_pairs=7)
    assert refused.value.args[1] == 8


def test_kernels_round_trip_store_columns(code_store):
    """Distinct subjects and a self-join over the fixture's codes decode to
    the expected terms."""
    from vortex_rdf import U64Column

    store = code_store
    term_dict = store.term_dict()
    s, p, o, g = store.match_codes()
    subjects = term_dict.decode_many(s.distinct())
    assert subjects == list(dict.fromkeys(term_dict.decode_many(s)))
    # Join objects with subjects: every (object row, subject row) pair naming
    # the same term — alice knows bob, and bob is the subject of two rows.
    left_idx, right_idx = o.join_indices(s)
    assert o.join_count(s) == len(left_idx)
    joined = sorted(
        (term_dict.decode(c), term_dict.decode(d))
        for c, d in zip(u64s(o.take(left_idx)), u64s(s.take(right_idx)))
    )
    objects, subjects = term_dict.decode_many(o), term_dict.decode_many(s)
    want = sorted((t, u) for t in objects for u in subjects if t == u)
    assert joined == want
    assert want == [("<http://ex.org/bob>", "<http://ex.org/bob>")] * 2
    assert isinstance(left_idx, U64Column)


def test_filter_codes_regex(dictionary):
    _, term_dict = dictionary
    terms = _terms(term_dict)
    every = U64Column(range(len(terms)))
    code = {t: c for c, t in enumerate(terms)}

    def run(arg, **options):
        passed, undecided = term_dict.filter_codes("regex", arg, every, **options)
        return u64s(passed), u64s(undecided)

    alice, anon, bob = code['"Alice"'], code['"Anon"'], code['"Bob"@en']
    assert run("^A") == ([alice, anon], [0])
    assert run("B", flags="i") == ([bob], [0])
    assert run("b$", case="lower") == ([bob], [0])
    assert run("^http://ex", as_str=True)[0] == sorted(
        code[t] for t in ("<http://ex.org/age>", "<http://ex.org/alice>", "<http://ex.org/bob>")
    )
    # Outside the allow-listed subset: every text undecided, non-texts still fail.
    assert run("(?=A)") == ([], [0, alice, anon, bob])


def test_filter_codes_regex_options(dictionary):
    """`case` and `as_str` apply to `regex` like to every string kind, in the
    order `REGEX(LCASE(STR(?x)), pattern)`; `flags` are rdflib's: `i`, `s` and
    `m` apply and any other letter is ignored."""
    _, term_dict = dictionary
    terms = _terms(term_dict)
    every = U64Column(range(len(terms)))
    code = {t: c for c, t in enumerate(terms)}

    def run(arg, **options):
        passed, undecided = term_dict.filter_codes("regex", arg, every, **options)
        return u64s(passed), u64s(undecided)

    alice, anon, bob = code['"Alice"'], code['"Anon"'], code['"Bob"@en']
    forty_two, blank = code[f'"42"^^<{XSD}integer>'], code["_:b0"]
    iris = sorted(code[t] for t in terms if t.startswith("<"))
    ex = sorted(code[t] for t in ("<http://ex.org/age>", "<http://ex.org/alice>", "<http://ex.org/bob>"))
    # STR() first, then the case wrapper: an IRI's string is lower-cased as a
    # whole, and under STR() a blank node or a normalized literal is undecided.
    assert run("^http://ex\\.org/a", as_str=True, case="lower") == (
        [code["<http://ex.org/age>"], code["<http://ex.org/alice>"]], [0, forty_two, blank])
    assert run("^HTTP://EX", as_str=True, case="upper")[0] == ex
    assert run("^HTTP://EX", as_str=True, case="lower")[0] == []
    assert run("^http", as_str=True)[0] == iris
    assert run("^http")[0] == []
    assert run("^b", as_str=True, case="lower") == ([bob], [0, forty_two, blank])
    # Flags: `i` honoured, any other letter ignored.
    assert run("^ALICE$", flags="i")[0] == [alice]
    assert run("^ALICE$", flags="xq")[0] == []
    assert run("^ALICE$", flags="I")[0] == []
    assert run("^b", flags="im", case="upper")[0] == [bob]
    assert run("^a..n$", flags="i")[0] == [anon]
    # A pattern outside the subset is undecided on the texts whatever the
    # options, and the rules for non-texts do not change.
    assert run("(?=a)", as_str=True) == ([], list(range(len(terms))))
    assert run("(?=a)", case="lower") == ([], [0, alice, anon, bob])
    # An option that applies to no kind but the string kinds is refused as before.
    with pytest.raises(ValueError):
        term_dict.filter_codes("lang", "en", every, case="lower")
    with pytest.raises(ValueError):
        term_dict.filter_codes("contains", '"a"', every, flags="i")


def test_filter_codes_rejects_malformed_constants(dictionary):
    """A string constant is a strict N-Triples spelling: a malformed one raises
    instead of failing every candidate silently."""
    _, term_dict = dictionary
    every = U64Column(range(len(term_dict)))
    malformed = [
        '"a"^^xsd:string',  # a prefixed name
        f'"a"^^<{XSD}string',  # an unclosed `<`
        '"a"^^',
        "<http://ex/a",  # an unclosed IRI
        '"a"@e n',
        '"a"@en^^<x>',
        '"a"@',
        '"a"@en-',
        '"a"@1en',
        '"a"^^<x>>',
        "_:",
        '"a',
        "a",
        "",
    ]
    for kind in ("contains", "strstarts", "strends"):
        for constant in malformed:
            with pytest.raises(ValueError):
                term_dict.filter_codes(kind, constant, every)
        for options in ({"as_str": True}, {"case": "lower"}):
            with pytest.raises(ValueError):
                term_dict.filter_codes(kind, malformed[0], every, **options)
        # Well formed, so valid: a constant that is no string literal matches nothing.
        for constant in ("<http://ex.org/a>", "_:b0", f'"4"^^<{XSD}integer>', '"a"@en-GB', '"a"@EN'):
            passed, _ = term_dict.filter_codes(kind, constant, every)
            assert len(passed) == 0, (kind, constant)
    # A prefix and a pattern are raw text, not spellings.
    term_dict.filter_codes("str_prefix", '"a"^^xsd:string', every)
    term_dict.filter_codes("regex", '"a"^^xsd:string', every)


def test_filter_codes_regex_compiled_size_is_bounded(dictionary):
    """A pattern whose compiled program is large (counted repetitions
    multiply) is undecided, like any pattern outside the subset, instead of
    costing seconds per candidate; the shapes the subset is for stay decided."""
    _, term_dict = dictionary
    terms = _terms(term_dict)
    every = U64Column(range(len(terms)))
    code = {t: c for c, t in enumerate(terms)}
    alice, anon, bob = code['"Alice"'], code['"Anon"'], code['"Bob"@en']

    def run(arg):
        passed, undecided = term_dict.filter_codes("regex", arg, every)
        return u64s(passed), u64s(undecided)

    for pattern in ["(?:a{1000}){5}b", "(?:a{100}){100}b", r"(?:\w{1000}){5}b", "(?:a{1000}){1000}"]:
        assert run(pattern) == ([], [0, alice, anon, bob]), pattern
    for pattern in ["(?:a{50}){50}b", "[0-9A-Za-z_]{1000}x", r"\w{1000}x", "a" * 2000]:
        assert run(pattern) == ([], [0]), pattern
    assert run(r"^\w{3,10}$") == ([alice, anon, bob], [0])
    assert run(r"^\W+$") == ([], [0])


# Texts for the REGEX tests against Python's own `re`: the edges of the subset
# (a final newline, the empty text, ASCII and non-ASCII whitespace and word
# characters, letters Unicode case-folds across ASCII, an astral character).
REGEX_TEXTS = [
    "", "a", "b", "A", "ab", "AB", "aB", "abc", "xaby", "ab\n", "\n", "\n\n", "a\nb", "a\n\n", "ab\r\n",
    "a\rb", "foo", "afoob", "a foo b", "foo bar", "x42", "42", "x\u0663", "caf\u00e9", "CAF\u00c9", "\u00e9", "\u00c9",
    "na\u00efve", "\U0001f600", "a\U0001f600b", "a\x1cb", "\x1c", "\x1d", "\x1e", "\x1f", "a\x0bb", "a\x0cb",
    " ", "\t", "a b\tc", "\u00a0", "\u2028", "\u0085", "\u2003", "a.b", "a|b", "a$", "$", "^", "a^b", "a-b", "-",
    "[x]", "a\\b", "a/b", "a&&b", "\u00df", "\u017f", "\u212a", "k", "K", "\u0130", "\u0131", "i", "I", "\u03a3",
    "\u03c3", "\u03c2", "x y.z@a.b", "aaaa", "ababcc", "ababc", "Alice", "bob", "BOB", "Zed", "a1_", "_", "1", "}",
    "]", "a]", "{", "\u00e0", "\u00ff", "|",
]
# Every ASCII character on its own, so each class shorthand meets all 128.
REGEX_TEXTS += [chr(c) for c in range(128) if chr(c) not in REGEX_TEXTS]

REGEX_PATTERNS = [
    # Literals, anchors, alternation, groups and quantifiers.
    "", "a", "ab", "^a", "a$", "^a$", "^$", "$", "^", "a^b", "a$b", "a|b", "ab|", "|a", "a|b|c", "(a)", "(a)(b)?",
    "(?:a|b)+", "^(?:a|b)+$", "(?:)", "()", "a*", "a+", "a?", "a{0}", "a{2}", "a{2,}", "a{1,3}", "a{02}",
    "a*?", "a+?", "a??", "a{1,3}?", "a{2,3}?b", "^(a|b)*?c{1,}$", "(?:^)*b", "x*", "(?:a|)$",
    # The dot, and the escaped metacharacters.
    ".", "a.b", "^.$", "^..$", ".*", r"\.", r"\*", r"\+", r"\?", r"\(", r"\)", r"\[", r"\]", r"\{", r"\}", r"\|",
    r"\^", r"\$", r"\\", r"\/", r"\-", r"a\|b",
    # Shorthands, boundaries and anchors.
    r"\d", r"\D", r"\w", r"\W", r"\s", r"\S", r"\d+", r"\S+$", r"\bfoo\b", r"\Bo", r"\b", r"\B", r"a\b", r"a\B",
    r"\w+\b$", r"\Aa", r"\Aa|b", r"^\B$", r"x*\B", r"\B$", r"\s$", r"\W$",
    # Counted and nested shorthands, and programs around the compiled-size limit.
    r"\w{3}", r"^\w{3,10}$", r"\d{2,}", r"\D{2}", r"\W+", r"\S{1,3}$", r"\s{2}", r"[\w-]{2}", r"[^\w\s]", r"[\D\s]",
    r"(?:\w\d){2}", r"\w{1000}x", r"\s{1000}", "(?:a{50}){50}b", "(?:a{1000}){5}b", "(?:a{1000}){1000}", "a" * 2000,
    # Classes.
    "[a-c]", "[^a-c]", "[abc]", "[^a]", "[a-]", "[-a]", "[a-c-e]", r"[\d-]", r"[-\d]", r"[\w.]+@[\w.]+", r"[\s,]+",
    r"[\S]", r"[^\s]", r"[\W]", r"[\D]", r"[^\S]", r"[\^\-\]\\]", "[$]", "[.]", "[a|b]", "[a-]]", "[^^]", "[a^]",
    # Non-ASCII characters in the pattern.
    "\u00e9", "\u00c9", "\u00e9+", "[\u00e9]", "[^\u00e9]", "[\u00e0-\u00ff]", "\u00df", "\u017f", "\u212a", "k", "K",
    "\u0131", "i", "\u0130", "\u03c3", "\u03a3", "caf\u00e9$",
    # Outside the subset: Python rejects them, warns, or reads them differently.
    "(?=a)", "(?i)a", r"\Z", "{", "a{,2}", "a**", "[a&&b]", r"\x41", r"\n", r"\t", "[[]", "(a", "a)", "[]a]",
    "[a||b]", "[+--]", r"\p{L}", r"(a)\1", "a{2}{3}", "a*+", "(?P<n>a)", "a{1001}", r"[\d-z]", "a{2,1}", "^*", r"\b+",
]

REGEX_FLAGS = ["", "i", "s", "m", "x", "is", "im", "ims", "I"]

_PY_FLAGS = {"i": re.IGNORECASE, "s": re.DOTALL, "m": re.MULTILINE}


def _nt_literal(text):
    """`text` as an N-Triples string literal, everything but printable ASCII escaped."""
    out = []
    for ch in text:
        cp = ord(ch)
        if ch in '"\\':
            out.append("\\" + ch)
        elif ch == "\n":
            out.append("\\n")
        elif ch == "\r":
            out.append("\\r")
        elif 0x20 <= cp < 0x7F:
            out.append(ch)
        elif cp <= 0xFFFF:
            out.append(f"\\u{cp:04X}")
        else:
            out.append(f"\\U{cp:08X}")
    return '"' + "".join(out) + '"'


@pytest.fixture(scope="module")
def regex_corpus(tmp_path_factory):
    assert len(set(REGEX_TEXTS)) == len(REGEX_TEXTS)
    directory = tmp_path_factory.mktemp("regex")
    nt = directory / "corpus.nt"
    nt.write_text(
        "".join(f"<http://ex.org/s{i}> <http://ex.org/p> {_nt_literal(t)} .\n" for i, t in enumerate(REGEX_TEXTS)),
        encoding="utf-8",
    )
    out = directory / "corpus.vortex"
    serialize_rdf(nt, out, layout="dictionary")
    return out


@pytest.fixture(params=["resident", "file-backed"])
def regex_dictionary(request, regex_corpus):
    store = VortexRdfStore(regex_corpus, in_memory=request.param == "resident")
    term_dict = store.term_dict()
    assert term_dict is not None
    return store, term_dict


def test_filter_codes_regex_agrees_with_python_re(regex_dictionary):
    """rdflib's REGEX is `re.search` with flags `i`/`s`/`m` (any other flag
    ignored). A text `regex` passes must match, a text it fails must not, and
    a pattern Python rejects or warns about decides nothing; every other text
    is undecided and claims nothing."""
    _, term_dict = regex_dictionary
    code_of = {t: term_dict.encode(_nt_literal(t)) for t in REGEX_TEXTS}
    assert all(c is not None for c in code_of.values())
    every = U64Column(range(len(term_dict)))
    decided = 0
    for pattern in REGEX_PATTERNS:
        for flags in REGEX_FLAGS:
            bits = 0
            for flag in flags:
                bits |= _PY_FLAGS.get(flag, 0)
            with warnings.catch_warnings():
                warnings.simplefilter("error", FutureWarning)
                try:
                    compiled = re.compile(pattern, bits)
                except (re.error, FutureWarning):
                    compiled = None
            passed, undecided = term_dict.filter_codes("regex", pattern, every, flags=flags)
            passed, undecided = set(u64s(passed)), set(u64s(undecided))
            assert not passed & undecided
            for text, code in code_of.items():
                if code in undecided:
                    continue
                assert compiled is not None, (pattern, flags, text)
                assert (code in passed) == bool(compiled.search(text)), (pattern, flags, text)
                decided += 1
    assert decided > 100_000, decided  # most of the pairs are decided

    # The rules that leave texts undecided do so, whatever the Python version.
    def undecided_of(pattern, flags=""):
        _, undecided = term_dict.filter_codes("regex", pattern, every, flags=flags)
        return {t for t, c in code_of.items() if c in set(u64s(undecided))}

    assert {t for t in REGEX_TEXTS if t.endswith("\n")} <= undecided_of("a$")
    assert "" in undecided_of(r"\B") and "ab" not in undecided_of(r"\B")
    assert {t for t in REGEX_TEXTS if not t.isascii()} <= undecided_of(r"\w")
    assert {t for t in REGEX_TEXTS if not t.isascii()} <= undecided_of("a", "i")
    assert set(REGEX_TEXTS) == undecided_of("(?=a)")


@pytest.mark.parametrize("in_memory", [False, True])
def test_located_reference_runs_agree_with_the_row_path(indexed_files, in_memory):
    """A guard, not a discriminator. A located run's count (its width) and its
    windows (its own rows) are exactly what the row path gives, so every
    assertion here also holds for an implementation that reads the run's row
    ids first. What tells the two apart, the row ids a count or a window asks
    the file for, is not visible from Python; it is asserted where the read
    counter lives, in core/src/tests/indexes_file.rs."""
    store = VortexRdfStore(indexed_files[("dictionary", "secondary-by-reference")], in_memory=in_memory)
    rows = u64_rows(store.match_codes(p=NAME))
    assert store.count_quads(p=NAME) == len(rows) == 3
    assert store.count_quads(p=NAME, limit=2) == 2
    assert store.count_quads_many([{"p": NAME, "limit": 1}, (None, NAME, None, None)]) == [1, 3]
    # Every window of the run, counted and read, against the same slice of its rows.
    for offset in range(5):
        for limit in (None, 0, 1, 2, 5):
            window = rows[offset:] if limit is None else rows[offset : offset + limit]
            assert u64_rows(store.match_codes(p=NAME, offset=offset, limit=limit)) == window, (offset, limit)
            probe = {"p": NAME, "offset": offset, "limit": limit}
            assert store.count_quads_many([probe]) == [len(window)], (offset, limit)
    # An object-only run: the fixture's one IRI object.
    bob = "<http://ex.org/bob>"
    assert store.count_quads(o=bob) == len(u64_rows(store.match_codes(o=bob))) == 1
    assert store.count_quads_many([{"o": bob, "offset": 1}]) == [0]


@pytest.mark.parametrize("index", [None, *INDEXES])
@pytest.mark.parametrize("in_memory", [False, True])
def test_an_empty_code_set_keep_matches_nothing(vortex_files, indexed_files, index, in_memory):
    # pinned contract: a keep with an empty code set is valid and matches nothing
    path = vortex_files["dictionary"] if index is None else indexed_files[("dictionary", index)]
    store = VortexRdfStore(path, in_memory=in_memory)
    empty = {"o": U64Column([])}
    assert store.count_quads(p=NAME, keep=empty) == 0
    assert u64_rows(store.match_codes(p=NAME, keep=empty)) == []
    assert store.count_quads_many([{"p": NAME, "keep": empty}]) == [0]
