"""The native pushdown primitives: the dictionary handle under both
residencies, spelling-tolerant encoding, byte-order ranges and term
predicates."""

from array import array

import pytest

from vortex_rdf import VortexRdfStore

XSD = "http://www.w3.org/2001/XMLSchema#"
RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"


@pytest.fixture(params=["resident", "file-backed"])
def dictionary(request, vortex_files):
    """The fixture's dictionary-layout store opened with its dictionary
    resident or left in the file, as `(store, term_dict)`."""
    budget = None if request.param == "resident" else 0
    store = VortexRdfStore(vortex_files["dictionary"], max_resident_bytes=budget)
    term_dict = store.term_dict()
    assert term_dict is not None
    assert term_dict.file_backed == (request.param == "file-backed")
    return store, term_dict


def _codes(col):
    return memoryview(col).cast("I").tolist()


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


def test_filter_codes_kinds_and_predicates(dictionary):
    _, term_dict = dictionary
    terms = _terms(term_dict)
    by_kind = {
        "is_literal": [c for c, t in enumerate(terms) if t.startswith('"')],
        "is_iri": [c for c, t in enumerate(terms) if t.startswith("<")],
        "is_blank": [c for c, t in enumerate(terms) if t.startswith("_:")],
    }
    for kind, want in by_kind.items():
        truth, unknown = term_dict.filter_codes(kind)
        assert _codes(truth) == want, kind
        # Only the default graph's "" is undecided by its spelling.
        assert _codes(unknown) == [0], kind

    def literal_codes(pred):
        return [c for c, t in enumerate(terms) if t.startswith('"') and pred(t)]

    truth, unknown = term_dict.filter_codes("datatype", f"{XSD}string")
    assert _codes(truth) == literal_codes(lambda t: t.endswith('"'))
    assert _codes(unknown) == []
    truth, _ = term_dict.filter_codes("datatype", f"<{RDF}langString>")
    assert _codes(truth) == literal_codes(lambda t: "@" in t)
    truth, _ = term_dict.filter_codes("datatype", f"{XSD}integer")
    assert _codes(truth) == [term_dict.encode(f'"42"^^<{XSD}integer>')]
    truth, _ = term_dict.filter_codes("lang", "en")
    assert _codes(truth) == [term_dict.encode('"Bob"@en')]
    truth, _ = term_dict.filter_codes("lang_matches", "EN")
    assert _codes(truth) == [term_dict.encode('"Bob"@en')]
    truth, unknown = term_dict.filter_codes("str_prefix", "A")
    assert _codes(truth) == [term_dict.encode('"Alice"'), term_dict.encode('"Anon"')]
    truth, _ = term_dict.filter_codes("str_prefix", "http://ex.org/a")
    assert _codes(truth) == sorted(
        term_dict.encode(t) for t in ("<http://ex.org/age>", "<http://ex.org/alice>")
    )
    truth, _ = term_dict.filter_codes("str_prefix", "http://ex.org/al")
    assert _codes(truth) == [term_dict.encode("<http://ex.org/alice>")]
    truth, unknown = term_dict.filter_codes("num_lt", "100")
    # 42 by value; the string literals (plain and tagged alike order as
    # xsd:string, above xsd:integer) are false.
    assert _codes(truth) == [term_dict.encode(f'"42"^^<{XSD}integer>')]
    assert _codes(unknown) == []
    truth, _ = term_dict.filter_codes("num_gt", "100")
    assert _codes(truth) == sorted(
        term_dict.encode(t) for t in ('"Alice"', '"Anon"', '"Bob"@en')
    )
    truth, unknown = term_dict.filter_codes("num_ne", "42")
    assert term_dict.encode(f'"42"^^<{XSD}integer>') not in _codes(truth)
    # Non-literals are outside a numeric predicate's domain: in neither list
    # (the kind ranges decide them — `!=` holds for every IRI and blank node).
    iri_lo, iri_hi = term_dict.prefix_range("<")
    assert all(not (iri_lo <= c < iri_hi) for c in _codes(truth) + _codes(unknown))
    # Equality against a non-numeric literal is the engine's call.
    assert _codes(unknown) == sorted(term_dict.encode(t) for t in ('"Alice"', '"Anon"', '"Bob"@en'))
    # Memoized: a repeated ask answers the same.
    assert _codes(term_dict.filter_codes("is_iri")[0]) == by_kind["is_iri"]
    for kind, arg in [("no_such_kind", ""), ("datatype", ""), ("num_lt", "abc"), ("lang_matches", "")]:
        with pytest.raises(ValueError):
            term_dict.filter_codes(kind, arg)


def test_file_backed_codes_decode_to_quads(vortex_files):
    """A file-backed handle decodes what `match_codes` gathers to the quads
    the string matchers return."""
    store = VortexRdfStore(vortex_files["dictionary"], max_resident_bytes=0)
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


@pytest.fixture(params=["file", "in-memory", "file-backed-dict"])
def code_store(request, vortex_files):
    """The fixture's dictionary-layout store under each open mode."""
    path = vortex_files["dictionary"]
    if request.param == "in-memory":
        return VortexRdfStore(path, in_memory=True)
    if request.param == "file-backed-dict":
        return VortexRdfStore(path, max_resident_bytes=0)
    return VortexRdfStore(path)


def _rows(cols):
    return list(zip(*(memoryview(c).cast("I").tolist() for c in cols)))


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
        ({"o": term_dict.filter_codes("is_iri")[0]}, 2, lambda c: iri_lo <= c < iri_hi),
        ({"s": memoryview(term_dict.filter_codes("is_blank")[0])}, 0, lambda c: term_dict.decode(c).startswith("_:")),
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
        {"p": [1 << 40]},
        {"p": (0, 1 << 40)},
        {"p": [1], 1: [2]},
    ]:
        with pytest.raises(ValueError):
            store.match_codes(keep=keep)
        with pytest.raises(ValueError):
            store.count_quads(keep=keep)
    # A pair of ints is a range; any other int sequence is a set.
    assert store.count_quads(keep={"p": (1, 2, 3)}) == store.count_quads(keep={"p": [1, 2, 3]})
    assert store.count_quads(keep={"p": (0, (1 << 32) - 1)}) == store.count_quads()


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
    return memoryview(col).cast("I").tolist()


def test_column_kernels():
    from vortex_rdf import U32Column

    col = U32Column([3, 1, 3, 2, 1, 3])
    assert len(col) == 6 and _values(col) == [3, 1, 3, 2, 1, 3]
    assert _values(U32Column(col)) == _values(col)
    assert _values(U32Column(memoryview(col))) == _values(col)
    assert _values(U32Column(array("I", [7, 8]))) == [7, 8]
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
    for other in (U32Column(right), right, array("I", right)):
        left_idx, right_idx = col.join_indices(other)
        pairs = list(zip(_values(left_idx), _values(right_idx)))
        want = [(i, j) for i, l in enumerate([3, 1, 3, 2, 1, 3]) for j, r in enumerate(right) if l == r]
        assert pairs == want
    empty = U32Column([])
    assert len(empty.distinct()) == 0
    assert len(empty.join_indices(right)[0]) == 0
    assert len(col.join_indices(empty)[1]) == 0
    with pytest.raises((ValueError, OverflowError, TypeError)):
        U32Column([-1])


def test_kernels_round_trip_store_columns(code_store):
    """Distinct subjects and a self-join over the fixture's codes decode to
    the expected terms."""
    from vortex_rdf import U32Column

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
    assert isinstance(left_idx, U32Column)
