import pytest

from conftest import LAYOUTS

from vortex_rdf import VortexRdfStore, serialize_rdf

NAME = "<http://xmlns.com/foaf/0.1/name>"


@pytest.mark.parametrize("layout", LAYOUTS)
def test_open_len_and_layout(vortex_files, layout):
    store = VortexRdfStore(str(vortex_files[layout]))
    assert store.layout() == layout
    assert len(store) == 5


@pytest.mark.parametrize("layout", LAYOUTS)
def test_get_quads_patterns(vortex_files, layout):
    store = VortexRdfStore(str(vortex_files[layout]))

    assert len(store.get_quads()) == 5

    names = store.get_quads(p=NAME)
    assert len(names) == 3
    assert all(p == NAME for _, p, _, _ in names)
    # Fixture data is in the default graph, spelled as the empty string.
    assert all(g == "" for *_, g in names)

    bob = store.get_quads(s="<http://ex.org/bob>")
    assert sorted(o for _, _, o, _ in bob) == [
        '"42"^^<http://www.w3.org/2001/XMLSchema#integer>',
        '"Bob"@en',
    ]

    by_object = store.get_quads(o='"Alice"')
    assert by_object == [("<http://ex.org/alice>", NAME, '"Alice"', "")]

    # Language-tagged and typed literal object patterns must match exactly.
    assert store.get_quads(o='"Bob"@en') == [("<http://ex.org/bob>", NAME, '"Bob"@en', "")]
    assert store.get_quads(o='"Bob"') == []
    typed = store.get_quads(o='"42"^^<http://www.w3.org/2001/XMLSchema#integer>')
    assert len(typed) == 1 and typed[0][1] == "<http://ex.org/age>"

    assert store.get_quads(s="<http://ex.org/nobody>") == []


def test_code_path_matches_decoded_rows(vortex_files):
    """Decoding the code columns position by position reproduces `get_quads`.

    `get_quads` assembles its rows from those same columns, so this pins the
    decode-and-zip assembly: columns swapped or misaligned would still yield
    plausible-looking rows of real terms.
    """
    store = VortexRdfStore(str(vortex_files["dictionary"]))
    dictionary = store.term_dict()
    assert dictionary is not None and len(dictionary) > 0

    for pattern in ({}, {"p": NAME}, {"s": "<http://ex.org/bob>"}, {"o": '"Bob"@en'}):
        cols = store.match_codes(**pattern)
        assert cols is not None
        views = [memoryview(c).cast("I").tolist() for c in cols]
        from_codes = sorted(
            tuple(dictionary.decode(code) for code in row) for row in zip(*views)
        )
        assert from_codes == sorted(store.get_quads(**pattern))


def test_code_path_unavailable_on_other_layouts(vortex_files):
    for layout in ("default", "typed-object"):
        store = VortexRdfStore(str(vortex_files[layout]))
        assert store.term_dict() is None
        assert store.match_codes() is None


def test_u32_column_buffer_is_zero_copy_view(vortex_files):
    store = VortexRdfStore(str(vortex_files["dictionary"]))
    cols = store.match_codes()
    view = memoryview(cols[0])
    assert view.readonly
    typed = view.cast("I")
    assert len(typed) == len(cols[0]) == 5
    # Two views over the same column expose identical memory.
    assert typed.tolist() == memoryview(cols[0]).cast("I").tolist()


@pytest.mark.parametrize("layout", LAYOUTS)
def test_in_memory_open_matches_file_backed(vortex_files, layout):
    file_backed = VortexRdfStore(str(vortex_files[layout]))
    in_memory = VortexRdfStore(str(vortex_files[layout]), in_memory=True)
    assert in_memory.layout() == layout
    assert len(in_memory) == len(file_backed) == 5
    assert sorted(in_memory.get_quads()) == sorted(file_backed.get_quads())
    assert sorted(in_memory.get_quads(p=NAME)) == sorted(file_backed.get_quads(p=NAME))


def test_in_memory_dictionary_keeps_code_path(vortex_files):
    store = VortexRdfStore(str(vortex_files["dictionary"]), in_memory=True)
    dictionary = store.term_dict()
    assert dictionary is not None
    cols = store.match_codes(p=NAME)
    assert cols is not None and len(cols[0]) == 3


def test_blank_node_round_trip(vortex_files):
    store = VortexRdfStore(str(vortex_files["dictionary"]))
    rows = store.get_quads(o='"Anon"')
    assert len(rows) == 1
    assert rows[0][0].startswith("_:")


def test_bad_term_raises_value_error(vortex_files):
    store = VortexRdfStore(str(vortex_files["default"]))
    # All four pattern slots validate: a malformed term raises rather than
    # silently matching nothing.
    with pytest.raises(ValueError):
        store.get_quads(s="not a term")
    with pytest.raises(ValueError):
        store.get_quads(p='"literals-cannot-be-predicates"')
    with pytest.raises(ValueError):
        store.get_quads(o="<http://ex.org/not a valid iri>")
    with pytest.raises(ValueError):
        store.get_quads(o='"Bob"@not a language tag')
    with pytest.raises(ValueError):
        store.get_quads(g="<http://ex.org/not a valid iri>")


def test_missing_file_raises_oserror(tmp_path):
    with pytest.raises(OSError):
        VortexRdfStore(str(tmp_path / "missing.vortex"))


def test_serialize_rejects_unknown_options(fixture_nt_path, tmp_path):
    out = str(tmp_path / "out.vortex")
    with pytest.raises(ValueError):
        serialize_rdf(str(fixture_nt_path), out, layout="nope")
    # The retired builder= option is no longer part of the signature.
    with pytest.raises(TypeError):
        serialize_rdf(str(fixture_nt_path), out, builder="sorted-in-memory")
    with pytest.raises(ValueError):
        serialize_rdf(str(fixture_nt_path), out, format="nope")


def test_dictionary_file_is_self_contained(fixture_nt_path, tmp_path):
    out = tmp_path / "dict.vortex"
    serialize_rdf(str(fixture_nt_path), str(out), layout="dictionary")
    # The embedded form is one file: no companion appears beside it.
    assert list(tmp_path.iterdir()) == [out]
    store = VortexRdfStore(str(out))
    assert store.layout() == "dictionary"
    assert len(store) == 5
    assert len(store.get_quads(p=NAME)) == 3


def test_decode_many_shares_one_object_per_repeated_code(vortex_files):
    """Repeats of a code decode once and share the resulting string object.

    Asserts identity, not equality: values stay correct whether or not the
    sharing happens, so only identity distinguishes them.

    Both orderings are checked. Adjacent repeats come from a bound position;
    scattered ones from a small vocabulary spread across rows, such as an
    `rdf:type` column under subject ordering.
    """
    from array import array

    dictionary = VortexRdfStore(str(vortex_files["dictionary"])).term_dict()
    assert dictionary is not None and len(dictionary) >= 2

    for label, codes in (
        ("adjacent", [0, 0, 0, 0, 1, 1, 1, 1]),
        ("scattered", [0, 1, 0, 1, 0, 1, 0, 1]),
    ):
        terms = dictionary.decode_many(array("I", codes))
        assert [id(t) for t in terms] == [
            id(terms[codes.index(c)]) for c in codes
        ], f"{label}: repeated codes did not share one object"
        assert len({id(t) for t in terms}) == 2, f"{label}: expected 2 distinct objects"
        # ...and the shared objects are still the right terms.
        assert terms == [dictionary.decode(c) for c in codes]


def test_get_quads_agrees_across_layouts(vortex_files):
    """The code path and the quad path return identical rows.

    `get_quads` is served from term codes on a Dictionary-layout store and
    by re-serializing matched quads otherwise. A Dictionary store never reaches
    the second path, so only a cross-layout comparison exercises both.
    """
    fallback = VortexRdfStore(str(vortex_files["default"]))
    codes = VortexRdfStore(str(vortex_files["dictionary"]))
    assert codes.match_codes() is not None and fallback.match_codes() is None

    for pattern in ({}, {"p": NAME}, {"s": "<http://ex.org/bob>"}, {"o": '"Bob"@en'}):
        assert sorted(fallback.get_quads(**pattern)) == sorted(
            codes.get_quads(**pattern)
        ), f"paths disagree for {pattern}"


@pytest.mark.parametrize("layout", LAYOUTS)
def test_match_columns_transposes_get_quads(vortex_files, layout):
    """`match_columns` returns the same result as `get_quads`, by position.

    Available on every layout, unlike `match_codes`, so both the code path and
    the re-serializing fallback are covered by the parametrization.
    """
    store = VortexRdfStore(str(vortex_files[layout]))
    for pattern in ({}, {"p": NAME}, {"s": "<http://ex.org/bob>"}):
        columns = store.match_columns(**pattern)
        rows = store.get_quads(**pattern)
        assert len(columns) == 4
        assert all(len(c) == len(rows) for c in columns)
        assert list(zip(*columns)) == rows


def test_get_quads_carries_named_graphs(tmp_path):
    """The graph position is part of the result, not dropped.

    The fixture dataset is entirely in the default graph, so a named-graph
    dataset is built here; without it nothing would distinguish "graph
    reported" from "graph always empty".
    """
    source = tmp_path / "quads.nq"
    source.write_text(
        '<http://ex.org/s1> <http://ex.org/p> <http://ex.org/o1> <http://ex.org/g1> .\n'
        '<http://ex.org/s2> <http://ex.org/p> <http://ex.org/o2> <http://ex.org/g2> .\n'
        '<http://ex.org/s3> <http://ex.org/p> <http://ex.org/o3> .\n',
        encoding="utf-8",
    )
    for layout in LAYOUTS:
        out = tmp_path / f"quads-{layout}.vortex"
        serialize_rdf(str(source), str(out), layout=layout)
        store = VortexRdfStore(str(out))

        graphs = sorted(g for *_, g in store.get_quads())
        assert graphs == ["", "<http://ex.org/g1>", "<http://ex.org/g2>"], layout

        # A returned graph feeds straight back in as a pattern, including the
        # empty string for the default graph.
        for graph in graphs:
            selected = store.get_quads(g=graph)
            assert len(selected) == 1 and selected[0][3] == graph, (layout, graph)
