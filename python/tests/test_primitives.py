"""The native pushdown primitives: the dictionary handle under both
residencies, spelling-tolerant encoding, byte-order ranges and term
predicates."""

import re
import warnings
from array import array

import pytest

from vortex_rdf import U64Column, VortexRdfStore, serialize_rdf

XSD = "http://www.w3.org/2001/XMLSchema#"
RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"


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


def _codes(col):
    return memoryview(col).cast("Q").tolist()


def _terms(term_dict):
    return term_dict.decode_many(list(range(len(term_dict))))


def test_terms_sort_by_kind(dictionary):
    _, term_dict = dictionary
    terms = _terms(term_dict)
    # "" (the default graph), then literals, IRIs, blank nodes, in byte order.
    assert terms == sorted(terms, key=lambda t: t.encode())
    assert terms[0] == ""
    assert [t[0] for t in terms[1:]] == sorted(t[0] for t in terms[1:])
    assert {t[0] for t in terms[1:]} == {'"', "<", "_"}


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


def test_prefix_range_and_lower_bound(dictionary):
    _, term_dict = dictionary
    terms = _terms(term_dict)
    n = len(terms)

    def brute_lower(probe):
        return sum(1 for t in terms if t.encode() < probe.encode())

    def brute_prefix(probe):
        lo = brute_lower(probe)
        hi = lo + sum(1 for t in terms if t.startswith(probe))
        return lo, hi

    probes = ["", '"', "<", "_:", "<http://ex.org/", "<http://xmlns.com/", '"Bob', "~"] + terms
    for probe in probes:
        assert term_dict.lower_bound(probe) == brute_lower(probe), probe
        assert term_dict.prefix_range(probe) == brute_prefix(probe), probe
    assert term_dict.prefix_range("") == (0, n)
    for term in terms:
        lo, hi = term_dict.prefix_range(term)
        assert lo <= term_dict.encode(term) < hi
    # The kind ranges tile the codes after the default graph.
    lit = term_dict.prefix_range('"')
    iri = term_dict.prefix_range("<")
    blank = term_dict.prefix_range("_:")
    assert lit[0] == 1 and lit[1] == iri[0] and iri[1] == blank[0] and blank[1] == n


def test_filter_codes_over_candidates(dictionary):
    """`filter_codes(kind, arg, codes)` answers `(passed, undecided)`, both
    subsets of the candidates; a candidate in neither fails."""
    _, term_dict = dictionary
    terms = _terms(term_dict)
    every = U64Column(range(len(terms)))
    code = {t: c for c, t in enumerate(terms)}
    literal = [c for c, t in enumerate(terms) if t.startswith('"')]
    iri = [c for c, t in enumerate(terms) if t.startswith("<")]
    blank = [c for c, t in enumerate(terms) if t.startswith("_:")]

    def run(kind, arg="", codes=every):
        passed, undecided = term_dict.filter_codes(kind, arg, codes)
        return _codes(passed), _codes(undecided)

    # Kind tests: decided by the kind ranges; only "" (code 0) is no kind.
    assert run("is_literal") == (literal, [0])
    assert run("is_iri") == (iri, [0])
    assert run("is_blank") == (blank, [0])
    plain = [c for c in literal if terms[c].endswith('"')]
    assert run("datatype", f"{XSD}string") == (plain, [0])
    assert run("datatype", f"<{RDF}langString>")[0] == [code['"Bob"@en']]
    assert run("datatype", f"{XSD}integer")[0] == [code[f'"42"^^<{XSD}integer>']]
    assert run("lang", "en")[0] == [code['"Bob"@en']]
    assert run("lang_matches", "EN")[0] == [code['"Bob"@en']]
    # 42 by value; string literals order as xsd:string, above xsd:integer.
    assert run("num_lt", "100") == ([code[f'"42"^^<{XSD}integer>']], [0])
    assert run("num_gt", "100")[0] == sorted(code[t] for t in ('"Alice"', '"Anon"', '"Bob"@en'))
    # `!=` holds for every IRI and blank node; a string literal is the engine's call.
    passed, undecided = run("num_ne", "42")
    assert passed == iri + blank
    assert undecided == [0] + sorted(code[t] for t in ('"Alice"', '"Anon"', '"Bob"@en'))
    # Any candidate subset, any int sequence or buffer.
    sub = [code['"Alice"'], code['"Bob"@en'], iri[0]]
    assert run("lang", "en", sub) == ([code['"Bob"@en']], [])
    assert run("lang", "en", array("Q", sub)) == ([code['"Bob"@en']], [])
    for kind, arg in [("no_such_kind", ""), ("datatype", ""), ("num_lt", "abc"), ("lang_matches", "")]:
        with pytest.raises(ValueError):
            term_dict.filter_codes(kind, arg, every)


def test_filter_codes_validates_candidates(dictionary):
    """Candidates must be sorted, unique and inside the dictionary, however
    they are passed; none at all is two empty columns."""
    _, term_dict = dictionary
    n = len(term_dict)

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
    # A valid list answers alike in every form.
    iri = [c for c, t in enumerate(_terms(term_dict)) if t.startswith("<")]
    for candidates in forms(iri[:3] + [n - 1]):
        passed, undecided = term_dict.filter_codes("is_iri", "", candidates)
        assert (_codes(passed), _codes(undecided)) == (iri[:3], [])


def test_file_backed_codes_decode_to_quads(vortex_files):
    """A file-backed handle decodes what `match_codes` gathers to the quads
    the string matchers return."""
    store = VortexRdfStore(vortex_files["dictionary"])
    term_dict = store.term_dict()
    assert term_dict.file_backed
    assert "file_backed=True" in repr(term_dict)
    for pattern in ({}, {"p": "<http://xmlns.com/foaf/0.1/name>"}, {"o": '"Bob"@en'}):
        cols = store.match_codes(**pattern)
        assert cols is not None
        decoded = [term_dict.decode_many(col) for col in cols]
        rows = sorted(zip(*decoded))
        assert rows == sorted(store.get_quads(**pattern))


# ─── keeps, windows, batches ──────────────────────────────────────────────


@pytest.fixture(params=["file", "in-memory"])
def code_store(request, vortex_files):
    """The fixture's dictionary-layout store under each open mode."""
    store = VortexRdfStore(vortex_files["dictionary"], in_memory=request.param == "in-memory")
    assert store.term_dict().file_backed == (request.param == "file")
    return store


def _rows(cols):
    return list(zip(*(memoryview(c).cast("Q").tolist() for c in cols)))


def _sorted_rows(cols):
    return sorted(_rows(cols))


def test_keep_narrows_like_filtering(code_store):
    store = code_store
    term_dict = store.term_dict()
    all_rows = _rows(store.match_codes())
    name = term_dict.encode("<http://xmlns.com/foaf/0.1/name>")
    knows = term_dict.encode("<http://xmlns.com/foaf/0.1/knows>")
    alice = term_dict.encode("<http://ex.org/alice>")
    lit_lo, lit_hi = term_dict.prefix_range('"')
    iri_lo, iri_hi = term_dict.prefix_range("<")

    def expect(position, admit):
        return sorted(r for r in all_rows if admit(r[position]))

    cases = [
        ({"p": [name, knows]}, 1, lambda c: c in (name, knows)),
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
        assert _sorted_rows(store.match_codes(keep=keep)) == want, keep
        assert store.count_quads(keep=keep) == len(want), keep
        assert store.count_quads(keep=keep, limit=1) == min(1, len(want)), keep
    # Keeps compose with a pattern and with each other.
    want = sorted(r for r in all_rows if r[1] == name and lit_lo <= r[2] < lit_hi)
    got = store.match_codes(p="<http://xmlns.com/foaf/0.1/name>", keep={"o": range(lit_lo, lit_hi)})
    assert _sorted_rows(got) == want
    got = store.match_codes(keep={"o": (lit_lo, lit_hi), "p": [name]})
    assert _sorted_rows(got) == want
    assert store.count_quads(p="<http://xmlns.com/foaf/0.1/name>", keep={"o": [lit_lo]}) == len(
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
    rows = _rows(store.match_codes())
    n = len(rows)
    for offset, limit in [(0, 2), (1, 3), (3, 10), (n, 1), (0, 0), (2, None)]:
        got = store.match_codes(limit=limit, offset=offset)
        want = rows[offset:] if limit is None else rows[offset : offset + limit]
        assert _rows(got) == want, (offset, limit)
    name = "<http://xmlns.com/foaf/0.1/name>"
    named = _rows(store.match_codes(p=name))
    assert _rows(store.match_codes(p=name, limit=2, offset=1)) == named[1:3]
    assert store.count_quads(p=name, limit=2) == 2
    assert store.count_quads(p=name, limit=10) == len(named)
    assert store.count_quads(s="<http://ex.org/nobody>", limit=1) == 0


def test_many_matches_singles(code_store, vortex_files):
    store = code_store
    name = "<http://xmlns.com/foaf/0.1/name>"
    term_dict = store.term_dict()
    lit_lo, lit_hi = term_dict.prefix_range('"')
    probes = [
        (None, None, None, None),
        (None, name, None, None),
        ["<http://ex.org/bob>", None, None, None],
        {"s": "<http://ex.org/nobody>"},
        {"p": name, "keep": {"o": range(lit_lo, lit_hi)}, "limit": 2, "offset": 1},
        {"keep": {"g": [0]}, "limit": 3},
        {},
    ]
    many = store.match_codes_many(probes)
    counts = store.count_quads_many(probes)
    assert len(many) == len(counts) == len(probes)
    for probe, cols, count in zip(probes, many, counts):
        if isinstance(probe, dict):
            single = store.match_codes(**probe)
            # The offset is consumed before the limit caps the count.
            plain = {k: v for k, v in probe.items() if k not in ("offset", "limit")}
            single_count = max(0, store.count_quads(**plain) - probe.get("offset", 0))
            if "limit" in probe:
                single_count = min(single_count, probe["limit"])
        else:
            single = store.match_codes(*probe)
            single_count = store.count_quads(*probe)
        assert _rows(cols) == _rows(single), probe
        assert count == single_count == len(_rows(cols)), probe
    assert store.match_codes_many([]) == []
    assert store.count_quads_many([]) == []
    # Parse-all-first: a malformed probe raises before any evaluation.
    for bad in [[(None, None, None)], [{"bogus": 1}], ["<x>"], [(None, "not a term", None, None)]]:
        with pytest.raises(ValueError):
            store.match_codes_many(bad)
        with pytest.raises(ValueError):
            store.count_quads_many(bad)
    strings = VortexRdfStore(vortex_files["default"])
    assert strings.match_codes_many(probes[:2]) == [None, None]
    assert strings.count_quads_many(probes[:2]) == [5, 3]


def _values(col):
    return memoryview(col).cast("Q").tolist()


def test_column_kernels():
    from vortex_rdf import U64Column

    col = U64Column([3, 1, 3, 2, 1, 3])
    assert len(col) == 6 and _values(col) == [3, 1, 3, 2, 1, 3]
    assert _values(U64Column(col)) == _values(col)
    assert _values(U64Column(memoryview(col))) == _values(col)
    assert _values(U64Column(array("Q", [7, 8]))) == [7, 8]
    assert _values(col.distinct()) == [3, 1, 2]
    values, counts = col.value_counts()
    assert _values(values) == [3, 1, 2]
    assert _values(counts) == [3, 2, 1]
    assert _values(col.take([5, 0, 1])) == [3, 3, 1]
    assert _values(col.take(col.distinct())) == [2, 1, 3]
    assert _values(col.take([])) == []
    with pytest.raises(IndexError):
        col.take([6])
    right = [1, 9, 3, 3]
    for other in (U64Column(right), right, array("Q", right)):
        left_idx, right_idx = col.join_indices(other)
        pairs = list(zip(_values(left_idx), _values(right_idx)))
        want = [(i, j) for i, l in enumerate([3, 1, 3, 2, 1, 3]) for j, r in enumerate(right) if l == r]
        assert pairs == want
    empty = U64Column([])
    assert len(empty.distinct()) == 0
    assert len(empty.join_indices(right)[0]) == 0
    assert len(col.join_indices(empty)[1]) == 0
    with pytest.raises((ValueError, OverflowError, TypeError)):
        U64Column([-1])


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
    joined = sorted(
        (term_dict.decode(c), term_dict.decode(d))
        for c, d in zip(_values(o.take(left_idx)), _values(s.take(right_idx)))
    )
    objects, subjects = term_dict.decode_many(o), term_dict.decode_many(s)
    want = sorted((t, u) for t in objects for u in subjects if t == u)
    assert joined == want
    assert want == [("<http://ex.org/bob>", "<http://ex.org/bob>")] * 2
    assert isinstance(left_idx, U64Column)


def test_filter_codes_string_kinds(dictionary):
    _, term_dict = dictionary
    terms = _terms(term_dict)
    every = U64Column(range(len(terms)))
    code = {t: c for c, t in enumerate(terms)}

    def run(kind, arg, **options):
        passed, undecided = term_dict.filter_codes(kind, arg, every, **options)
        return _codes(passed), _codes(undecided)

    alice, anon, bob = code['"Alice"'], code['"Anon"'], code['"Bob"@en']
    assert run("str_prefix", "A") == ([alice, anon], [0])
    assert run("contains", '"o"') == ([anon, bob], [0])
    assert run("contains", '"o"@en') == ([bob], [0])
    assert run("contains", '"o"@EN') == ([], [0])
    assert run("strstarts", f'"A"^^<{XSD}string>') == ([alice, anon], [0])
    assert run("strends", '"e"') == ([alice], [0])
    assert run("contains", '"O"', case="upper") == ([anon, bob], [0])
    assert run("contains", '"al"', case="lower") == ([alice], [0])
    age, alice_iri = code["<http://ex.org/age>"], code["<http://ex.org/alice>"]
    forty_two = code[f'"42"^^<{XSD}integer>']
    blank = code["_:b0"]
    assert run("str_prefix", "http://ex.org/a", as_str=True) == ([age, alice_iri], [0, forty_two, blank])
    assert run("str_prefix", "http") == ([], [0])
    assert run("contains", f'"4"^^<{XSD}integer>') == ([], [0])
    assert run("contains", "<http://ex.org/a>") == ([], [0])
    for kind, arg, options in [
        ("contains", '"a"', {"case": "title"}),
        ("num_lt", "5", {"as_str": True}),
        ("lang", "en", {"case": "lower"}),
        ("contains", '"a"', {"flags": "i"}),
        ("contains", "not a spelling", {}),
    ]:
        with pytest.raises(ValueError):
            term_dict.filter_codes(kind, arg, every, **options)


def test_filter_codes_regex(dictionary):
    _, term_dict = dictionary
    terms = _terms(term_dict)
    every = U64Column(range(len(terms)))
    code = {t: c for c, t in enumerate(terms)}

    def run(arg, **options):
        passed, undecided = term_dict.filter_codes("regex", arg, every, **options)
        return _codes(passed), _codes(undecided)

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
        return _codes(passed), _codes(undecided)

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
        return _codes(passed), _codes(undecided)

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
            passed, undecided = set(_codes(passed)), set(_codes(undecided))
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
        return {t for t, c in code_of.items() if c in set(_codes(undecided))}

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
    name = "<http://xmlns.com/foaf/0.1/name>"
    rows = _rows(store.match_codes(p=name))
    assert store.count_quads(p=name) == len(rows) == 3
    assert store.count_quads(p=name, limit=2) == 2
    assert store.count_quads_many([{"p": name, "limit": 1}, (None, name, None, None)]) == [1, 3]
    # Every window of the run, counted and read, against the same slice of its rows.
    for offset in range(5):
        for limit in (None, 0, 1, 2, 5):
            window = rows[offset:] if limit is None else rows[offset : offset + limit]
            assert _rows(store.match_codes(p=name, offset=offset, limit=limit)) == window, (offset, limit)
            probe = {"p": name, "offset": offset, "limit": limit}
            assert store.count_quads_many([probe]) == [len(window)], (offset, limit)
    # An object-only run: the fixture's one IRI object.
    bob = "<http://ex.org/bob>"
    assert store.count_quads(o=bob) == len(_rows(store.match_codes(o=bob))) == 1
    assert store.count_quads_many([{"o": bob, "offset": 1}]) == [0]


@pytest.mark.parametrize("index", [None, "secondary-by-copy", "secondary-by-reference"])
@pytest.mark.parametrize("in_memory", [False, True])
def test_an_empty_code_set_keep_matches_nothing(vortex_files, indexed_files, index, in_memory):
    # pinned contract: a keep with an empty code set is valid and matches nothing
    path = vortex_files["dictionary"] if index is None else indexed_files[("dictionary", index)]
    store = VortexRdfStore(path, in_memory=in_memory)
    name = "<http://xmlns.com/foaf/0.1/name>"
    empty = {"o": U64Column([])}
    assert store.count_quads(p=name, keep=empty) == 0
    assert _rows(store.match_codes(p=name, keep=empty)) == []
    assert store.count_quads_many([{"p": name, "keep": empty}]) == [0]
