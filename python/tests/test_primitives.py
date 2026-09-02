"""The pushdown primitives: batch matches and counts, `keep` code
constraints, and `limit`/`offset` windows — each against the plain calls it
must agree with."""

import pytest

from vortex_rdf import VortexRdfError, VortexRdfStore

NAME = "<http://xmlns.com/foaf/0.1/name>"
KNOWS = "<http://xmlns.com/foaf/0.1/knows>"
BOB = "<http://ex.org/bob>"
PATTERNS = [
    (None, None, None, None),
    (None, NAME, None, None),
    (BOB, None, None, None),
    (None, None, '"Bob"@en', None),
    (BOB, NAME, None, None),
    ("<http://ex.org/nobody>", None, None, None),
]


def _rows(columns):
    return sorted(zip(*(memoryview(c).cast("I").tolist() for c in columns)))


def _decoded(store, columns):
    dictionary = store.term_dict()
    return sorted(tuple(dictionary.decode(c) for c in row) for row in zip(*(memoryview(c).cast("I").tolist() for c in columns)))


@pytest.mark.parametrize("in_memory", [False, True])
def test_batch_calls_agree_with_singles(vortex_files, in_memory):
    store = VortexRdfStore(vortex_files["dictionary"], in_memory=in_memory)
    many = store.match_codes_many(PATTERNS)
    counts = store.count_quads_many(PATTERNS)
    assert len(many) == len(counts) == len(PATTERNS)
    for pattern, columns, count in zip(PATTERNS, many, counts):
        single = store.match_codes(*pattern)
        assert _rows(columns) == _rows(single), pattern
        assert count == store.count_quads(*pattern) == len(_rows(columns)), pattern
    assert store.match_codes_many([]) == [] and store.count_quads_many([]) == []


def test_batch_calls_parse_every_pattern_first(vortex_files):
    store = VortexRdfStore(vortex_files["dictionary"])
    with pytest.raises(ValueError):
        store.match_codes_many([(None, None, None, None), (None, "not a term", None, None)])
    with pytest.raises(ValueError):
        store.count_quads_many([("<http://ex.org/x>", None, None, None), (None, None, "bad", None)])


@pytest.mark.parametrize("layout", ["default", "typed-object"])
def test_batch_codes_decline_without_the_code_path(vortex_files, layout):
    store = VortexRdfStore(vortex_files[layout])
    assert store.match_codes_many(PATTERNS) == [None] * len(PATTERNS)
    assert store.count_quads_many(PATTERNS) == [store.count_quads(*p) for p in PATTERNS]


def test_count_limit_caps(vortex_files, layout):
    store = VortexRdfStore(vortex_files[layout])
    assert store.count_quads(limit=2) == 2
    assert store.count_quads(limit=0) == 0
    assert store.count_quads(limit=100) == store.count_quads() == 5
    assert store.count_quads(p=NAME, limit=1) == 1
    assert store.count_quads(s="<http://ex.org/nobody>", limit=1) == 0


def test_limit_and_offset_window_the_rows(vortex_files):
    store = VortexRdfStore(vortex_files["dictionary"])
    for pattern in PATTERNS:
        full = _rows(store.match_codes(*pattern)) if store.match_codes(*pattern) else []
        ordered = list(zip(*(memoryview(c).cast("I").tolist() for c in store.match_codes(*pattern))))
        n = len(ordered)
        for offset, limit in [(0, 1), (0, 2), (1, 2), (2, 10), (n, 1), (0, 0)]:
            windowed = store.match_codes(*pattern, limit=limit, offset=offset)
            got = list(zip(*(memoryview(c).cast("I").tolist() for c in windowed)))
            assert got == ordered[offset : offset + limit], (pattern, offset, limit)
        assert sorted(zip(*(memoryview(c).cast("I").tolist() for c in store.match_codes(*pattern, offset=1)))) == sorted(ordered[1:]), pattern
        assert full == sorted(ordered)


def test_keep_restricts_by_set_and_range(vortex_files):
    store = VortexRdfStore(vortex_files["dictionary"])
    dictionary = store.term_dict()
    everything = _decoded(store, store.match_codes())

    bob_code = dictionary.encode(BOB)
    kept = store.match_codes(keep={"s": [bob_code]})
    assert _decoded(store, kept) == [row for row in everything if row[0] == BOB]
    as_column = store.match_codes(keep={"s": store.match_codes(s=BOB)[0]})
    assert _rows(as_column) == _rows(kept)

    lo, hi = dictionary.prefix_range("<http://ex.org/")
    kept = store.match_codes(keep={"o": range(lo, hi)})
    assert _decoded(store, kept) == [row for row in everything if row[2].startswith("<http://ex.org/")]

    kept = store.match_codes(p=NAME, keep={"s": range(lo, hi), "o": [dictionary.encode('"Bob"@en')]})
    assert _decoded(store, kept) == [(BOB, NAME, '"Bob"@en', "")]

    assert _rows(store.match_codes(keep={"o": []})) == []
    assert _rows(store.match_codes(keep={"o": range(0, 0)})) == []
    windowed = store.match_codes(keep={"o": range(lo, hi)}, limit=1)
    assert len(_rows(windowed)) == 1


def test_keep_rejects_bad_arguments(vortex_files):
    store = VortexRdfStore(vortex_files["dictionary"])
    with pytest.raises(ValueError):
        store.match_codes(keep={"subject": [1]})
    with pytest.raises(ValueError):
        store.match_codes(keep={"s": range(0, 10, 2)})
    with pytest.raises(ValueError):
        store.match_codes(keep={"s": range(-1, 10)})
    with pytest.raises((ValueError, TypeError)):
        store.match_codes(keep={"s": ["x"]})


def test_keep_declines_off_the_code_path(vortex_files):
    """`keep` rides the code path, so it declines like `match_codes` does;
    a window needs no codes and works on every layout."""
    plain = VortexRdfStore(vortex_files["default"])
    assert plain.match_codes(keep={"s": [1]}) is None
    assert plain.count_quads(limit=1) == 1
    with pytest.raises(VortexRdfError):
        # A tailless dictionary store with codes, but a column that holds
        # none: the error surfaces rather than a silent empty result.
        VortexRdfStore(vortex_files["dictionary"]).match_arrow(encoding="codes", projection=[])


# --- dictionary predicates -------------------------------------------------

import bisect  # noqa: E402

XSD = "http://www.w3.org/2001/XMLSchema#"
AGE = f'"42"^^<{XSD}integer>'


def _terms(dictionary):
    return [dictionary.decode(code) for code in range(len(dictionary))]


def test_bounds_bracket_the_sorted_terms(vortex_files):
    dictionary = VortexRdfStore(vortex_files["dictionary"]).term_dict()
    terms = _terms(dictionary)
    assert terms == sorted(terms)
    for probe in ["", "<http://ex.org/", "<http://ex.org/bob>", '"', "_:", "~"]:
        assert dictionary.lower_bound(probe) == bisect.bisect_left(terms, probe), probe
    for prefix in ["<http://ex.org/", '"', "_:", "<http://xmlns", "", "<http://nowhere/"]:
        lo, hi = dictionary.prefix_range(prefix)
        assert list(range(lo, hi)) == [c for c, t in enumerate(terms) if t.startswith(prefix)], prefix


def test_filter_codes_partition_the_dictionary(vortex_files):
    dictionary = VortexRdfStore(vortex_files["dictionary"]).term_dict()
    terms = _terms(dictionary)
    literals = {t for t in terms if t.startswith('"')}
    iris = {t for t in terms if t.startswith("<")}
    blanks = {t for t in terms if t.startswith("_:")}
    non_literals = iris | blanks | {""}

    def sets(kind, arg=""):
        holds, unknown = dictionary.filter_codes(kind, arg)
        return (
            {terms[c] for c in memoryview(holds).cast("I")},
            {terms[c] for c in memoryview(unknown).cast("I")},
        )

    # The default graph's empty spelling is no term: undecided by everything.
    assert sets("is_literal") == (literals, {""})
    assert sets("is_iri") == (iris, {""})
    assert sets("is_blank") == (blanks, {""})
    assert sets("datatype", f"{XSD}integer") == ({AGE}, non_literals)
    assert sets("datatype", f"<{XSD}string>") == ({'"Alice"', '"Anon"'}, non_literals)
    assert sets("lang", "en") == ({'"Bob"@en'}, non_literals)
    assert sets("lang", "") == ({'"Alice"', '"Anon"', AGE}, non_literals)
    assert sets("lang_matches", "EN") == ({'"Bob"@en'}, non_literals)
    assert sets("lang_matches", "*") == ({'"Bob"@en'}, non_literals)
    assert sets("str_prefix", "http://ex.org/") == (
        {t for t in iris if t.startswith("<http://ex.org/")},
        {AGE, ""},
    )
    assert sets("str_prefix", "A") == ({'"Alice"', '"Anon"'}, {AGE, ""})
    # rdflib orders literals of different datatypes by datatype IRI, so every
    # xsd:string literal sorts above an xsd:integer constant.
    assert sets("num_gt", "40") == ({AGE, '"Alice"', '"Anon"', '"Bob"@en'}, non_literals)
    assert sets("num_lt", "40") == (set(), non_literals)
    assert sets("num_eq", AGE) == ({AGE}, {""})
    assert sets("num_ne", "42") == (set(terms) - {AGE, ""}, {""})
    with pytest.raises(ValueError):
        dictionary.filter_codes("is_prime", "")
    with pytest.raises(ValueError):
        dictionary.filter_codes("num_gt", "forty")
    holds, _ = dictionary.filter_codes("num_gt", "40")
    assert memoryview(dictionary.filter_codes("num_gt", "40")[0]).tolist() == memoryview(holds).tolist()


def test_filter_codes_feed_keep(vortex_files):
    store = VortexRdfStore(vortex_files["dictionary"])
    dictionary = store.term_dict()
    holds, unknown = dictionary.filter_codes("num_gt", "40")
    kept = store.match_codes(keep={"o": holds})
    assert _decoded(store, kept) == [
        row for row in _decoded(store, store.match_codes())
        if row[2] in {AGE, '"Alice"', '"Anon"', '"Bob"@en'}
    ]
    assert len(memoryview(unknown)) > 0


def test_encode_is_spelling_tolerant(vortex_files):
    dictionary = VortexRdfStore(vortex_files["dictionary"]).term_dict()
    alice = dictionary.encode('"Alice"')
    assert alice is not None
    assert dictionary.encode(f'"Alice"^^<{XSD}string>') == alice
    assert dictionary.encode("nope") is None
    assert dictionary.encode("") == dictionary.encode("")  # the default graph
    assert dictionary.encode_many(['"Alice"', BOB, "nope", f'"Alice"^^<{XSD}string>']) == [
        alice,
        dictionary.encode(BOB),
        None,
        alice,
    ]
