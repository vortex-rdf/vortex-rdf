import pathlib
import shutil
from array import array

import pyarrow as pa
import pytest

import vortex_rdf
from vortex_rdf import VortexRdfError, VortexRdfStore, serialize_rdf

NAME = "<http://xmlns.com/foaf/0.1/name>"
PATTERNS = ({}, {"p": NAME}, {"s": "<http://ex.org/bob>"}, {"o": '"Bob"@en'})


def _codes(store, **pattern):
    """The matched rows' code columns, read back out of the Arrow stream."""
    table = pa.RecordBatchReader.from_stream(store.match_arrow(**pattern)).read_all()
    return [column.to_pylist() for column in table.columns]


def test_open_len_and_layout(vortex_files, layout):
    store = VortexRdfStore(vortex_files[layout])
    assert store.layout() == layout
    assert len(store) == 5


def test_open_accepts_str_and_pathlib_path(vortex_files, tmp_path, fixture_nt_path):
    path = vortex_files["default"]
    assert isinstance(path, pathlib.Path)
    assert len(VortexRdfStore(path)) == len(VortexRdfStore(str(path))) == 5
    out = tmp_path / "from-path.vortex"
    serialize_rdf(pathlib.Path(fixture_nt_path), out)
    assert len(VortexRdfStore(out)) == 5


def test_get_quads_patterns(vortex_files, layout):
    store = VortexRdfStore(vortex_files[layout])

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


@pytest.mark.parametrize("in_memory", [False, True])
def test_count_quads_agrees_with_get_quads(vortex_files, layout, in_memory):
    """`count_quads` answers from the match's row selection alone -- on a
    file-backed store an object pattern is a pushed-down filter it must still
    evaluate -- and has to agree with materializing the same match."""
    store = VortexRdfStore(vortex_files[layout], in_memory=in_memory)
    for pattern in (
        {},
        {"p": NAME},
        {"s": "<http://ex.org/bob>"},
        {"o": '"Bob"@en'},
        {"o": '"Bob"'},
        {"s": "<http://ex.org/bob>", "p": NAME},
        {"s": "<http://ex.org/nobody>"},
    ):
        assert store.count_quads(**pattern) == len(store.get_quads(**pattern)), pattern
    assert store.count_quads() == len(store) == 5


def test_code_path_matches_decoded_rows(vortex_files):
    """Decoding the code columns position by position reproduces `get_quads`.

    `get_quads` assembles its rows from those same columns, so this pins the
    decode-and-zip assembly: columns swapped or misaligned would still yield
    plausible-looking rows of real terms.
    """
    store = VortexRdfStore(vortex_files["dictionary"])
    dictionary = store.term_dict()
    assert dictionary is not None and len(dictionary) > 0

    for pattern in PATTERNS:
        cols = _codes(store, **pattern)
        from_codes = sorted(
            tuple(dictionary.decode(code) for code in row) for row in zip(*cols)
        )
        assert from_codes == sorted(store.get_quads(**pattern))


def test_encode_inverts_decode(vortex_files):
    store = VortexRdfStore(vortex_files["dictionary"])
    dictionary = store.term_dict()
    assert dictionary is not None
    for col in _codes(store):
        for code in col:
            term = dictionary.decode(code)
            assert term is not None
            assert dictionary.encode(term) == code
    assert dictionary.encode("<http://ex.org/nobody>") is None
    # The default graph is the empty string, a term of the dictionary too.
    assert dictionary.decode(dictionary.encode("")) == ""


@pytest.mark.parametrize("layout", ["default", "typed-object"])
def test_code_path_unavailable_on_other_layouts(vortex_files, layout):
    store = VortexRdfStore(vortex_files[layout])
    assert store.term_dict() is None
    with pytest.raises(VortexRdfError):
        store.match_arrow()


def test_u32_column_buffer_is_zero_copy_view(vortex_files):
    store = VortexRdfStore(vortex_files["dictionary"])
    column, _ = store.term_dict().filter_codes("is_iri", "")
    view = memoryview(column)
    assert view.readonly
    typed = view.cast("I")
    assert len(typed) == len(column) > 0
    # Two views over the same column expose identical memory.
    assert typed.tolist() == memoryview(column).cast("I").tolist()


def test_in_memory_open_matches_file_backed(vortex_files, layout):
    file_backed = VortexRdfStore(vortex_files[layout])
    in_memory = VortexRdfStore(vortex_files[layout], in_memory=True)
    assert in_memory.layout() == layout
    assert len(in_memory) == len(file_backed) == 5
    assert sorted(in_memory.get_quads()) == sorted(file_backed.get_quads())
    assert sorted(in_memory.get_quads(p=NAME)) == sorted(file_backed.get_quads(p=NAME))


def test_in_memory_dictionary_keeps_code_path(vortex_files):
    store = VortexRdfStore(vortex_files["dictionary"], in_memory=True)
    dictionary = store.term_dict()
    assert dictionary is not None
    assert len(_codes(store, p=NAME)[0]) == 3


def _terms(dictionary):
    return [dictionary.decode(code) for code in range(len(dictionary))]


@pytest.mark.parametrize("dictionary", [None, "as-written", "plaintext"])
def test_dictionary_forms_answer_alike(vortex_files, dictionary):
    """Whichever resident form the dictionary takes, an in-memory open and
    ``from_bytes`` answer exactly like the file-backed store."""
    file_backed = VortexRdfStore(vortex_files["dictionary"])
    opened = VortexRdfStore(vortex_files["dictionary"], in_memory=True, dictionary=dictionary)
    adopted = VortexRdfStore.from_bytes(file_backed.to_bytes(), dictionary=dictionary)
    for store in (opened, adopted):
        assert store.term_dict() is not None
        assert _terms(store.term_dict()) == _terms(file_backed.term_dict())
        for pattern in PATTERNS:
            assert sorted(store.get_quads(**pattern)) == sorted(file_backed.get_quads(**pattern))
            assert _codes(store, **pattern) == _codes(file_backed, **pattern)


def test_dictionary_form_is_validated(vortex_files):
    with pytest.raises(ValueError):
        VortexRdfStore(vortex_files["dictionary"], dictionary="plaintext")
    with pytest.raises(ValueError):
        VortexRdfStore(vortex_files["dictionary"], in_memory=True, dictionary="compressed")
    data = VortexRdfStore(vortex_files["dictionary"]).to_bytes()
    with pytest.raises(ValueError):
        VortexRdfStore.from_bytes(data, dictionary="fsst")


def _assert_file_backed_dictionary(fallback, resident):
    assert fallback.layout() == "dictionary"
    assert fallback.term_dict() is None
    assert resident.term_dict() is not None
    for pattern in PATTERNS:
        assert sorted(fallback.get_quads(**pattern)) == sorted(resident.get_quads(**pattern))
        # Codes are the file's codes either way; only decoding needs the
        # resident dictionary.
        assert _codes(fallback, **pattern) == _codes(resident, **pattern)
        assert fallback.count_quads(**pattern) == resident.count_quads(**pattern)


def test_residency_budget_zero_forces_file_backed_dictionary(vortex_files):
    """With no residency budget the dictionary stays in the file, the code
    path declines and every matcher is served from the shared-term rows."""
    resident = VortexRdfStore(vortex_files["dictionary"])
    fallback = VortexRdfStore(vortex_files["dictionary"], max_resident_bytes=0)
    _assert_file_backed_dictionary(fallback, resident)


def test_residency_env_var(vortex_files, monkeypatch):
    resident = VortexRdfStore(vortex_files["dictionary"])
    monkeypatch.setenv("VORTEX_RDF_DICT_MAX_RESIDENT_BYTES", "0")
    fallback = VortexRdfStore(vortex_files["dictionary"])
    _assert_file_backed_dictionary(fallback, resident)
    # An explicit budget overrides the environment.
    assert VortexRdfStore(vortex_files["dictionary"], max_resident_bytes=1 << 30).term_dict() is not None


def test_indexes_round_trip(vortex_files, indexed_files, layout):
    assert VortexRdfStore(vortex_files[layout]).indexes() == []
    for index in ("secondary-by-copy", "secondary-by-reference"):
        store = VortexRdfStore(indexed_files[(layout, index)])
        assert store.indexes() == [index]
        assert VortexRdfStore.from_bytes(store.to_bytes()).indexes() == [index]


@pytest.mark.parametrize("index", ["secondary-by-copy", "secondary-by-reference"])
def test_indexed_store_agrees_with_unindexed(vortex_files, indexed_files, layout, index):
    plain = VortexRdfStore(vortex_files[layout])
    indexed = VortexRdfStore(indexed_files[(layout, index)])
    assert indexed.layout() == layout
    assert len(indexed) == len(plain) == 5
    for pattern in PATTERNS + ({"o": '"Alice"'}, {"s": "<http://ex.org/bob>", "p": NAME}):
        assert sorted(indexed.get_quads(**pattern)) == sorted(plain.get_quads(**pattern)), pattern
        assert indexed.count_quads(**pattern) == plain.count_quads(**pattern), pattern


def test_blank_node_round_trip(vortex_files):
    store = VortexRdfStore(vortex_files["dictionary"])
    rows = store.get_quads(o='"Anon"')
    assert len(rows) == 1
    assert rows[0][0].startswith("_:")


@pytest.mark.parametrize("layout", ["default", "dictionary"])
@pytest.mark.parametrize("matcher", ["get_quads", "count_quads", "match_arrow"])
def test_bad_term_raises_value_error(vortex_files, layout, matcher):
    """All four pattern slots validate on every matcher: a malformed term
    raises ValueError."""
    match = getattr(VortexRdfStore(vortex_files[layout]), matcher)
    for pattern in (
        {"s": "not a term"},
        {"p": '"literals-cannot-be-predicates"'},
        {"o": "<http://ex.org/not a valid iri>"},
        {"o": '"Bob"@not a language tag'},
        {"g": "<http://ex.org/not a valid iri>"},
    ):
        with pytest.raises(ValueError):
            match(**pattern)


def test_missing_file_raises_oserror(tmp_path):
    with pytest.raises(OSError):
        VortexRdfStore(tmp_path / "missing.vortex")


def test_serialize_rejects_unknown_options(fixture_nt_path, tmp_path):
    out = tmp_path / "out.vortex"
    with pytest.raises(ValueError):
        serialize_rdf(fixture_nt_path, out, layout="nope")
    with pytest.raises(ValueError):
        serialize_rdf(fixture_nt_path, out, indexes=["nope"])
    with pytest.raises(ValueError, match=r"unknown RDF format"):
        serialize_rdf(fixture_nt_path, out, format="nope")


def test_serialize_options_are_keyword_only(fixture_nt_path, tmp_path):
    out = tmp_path / "out.vortex"
    with pytest.raises(TypeError):
        serialize_rdf(fixture_nt_path, out, "nquads")
    with pytest.raises(TypeError):
        serialize_rdf(fixture_nt_path, out, None, "dictionary")


def test_serialize_defaults_to_dictionary_layout(fixture_nt_path, tmp_path):
    out = tmp_path / "out.vortex"
    serialize_rdf(fixture_nt_path, out)
    store = VortexRdfStore(out)
    assert store.layout() == "dictionary"
    assert len(store) == 5


def test_serialize_format_and_detect_failure(fixture_nt_path, tmp_path):
    source = tmp_path / "data"
    shutil.copyfile(fixture_nt_path, source)
    out = tmp_path / "out.vortex"
    with pytest.raises(ValueError):
        serialize_rdf(source, out)
    serialize_rdf(source, out, format="ntriples")
    assert len(VortexRdfStore(out)) == 5


def test_dictionary_file_is_self_contained(fixture_nt_path, tmp_path):
    out = tmp_path / "dict.vortex"
    serialize_rdf(fixture_nt_path, out, layout="dictionary")
    # The embedded form is one file: no companion appears beside it.
    assert list(tmp_path.iterdir()) == [out]
    store = VortexRdfStore(out)
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
    dictionary = VortexRdfStore(vortex_files["dictionary"]).term_dict()
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


def test_term_dict_decode_edges(vortex_files):
    store = VortexRdfStore(vortex_files["dictionary"])
    dictionary = store.term_dict()
    assert dictionary is not None
    end = len(dictionary)

    assert dictionary.decode(end) is None
    assert dictionary.decode_many([end]) == [None]
    assert dictionary.decode_many(array("I", [2**31])) == [None]

    # Every accepted input shape decodes the same column identically.
    codes = _codes(store)[0]
    expected = [dictionary.decode(c) for c in codes]
    buffer = array("I", codes)
    assert dictionary.decode_many(buffer) == expected
    assert dictionary.decode_many(memoryview(buffer)) == expected
    assert dictionary.decode_many(memoryview(buffer).cast("B")) == expected
    assert dictionary.decode_many(codes) == expected
    assert dictionary.decode_many(pa.array(codes, pa.uint32())) == expected
    holds, _ = dictionary.filter_codes("is_iri", "")
    assert dictionary.decode_many(holds) == dictionary.decode_many(pa.array(holds))

    with pytest.raises(ValueError, match="whole number of u32"):
        dictionary.decode_many(bytes(5))

    # A plain int list shares one object per repeated code too.
    shared = dictionary.decode_many([0, 0])
    assert shared[0] is shared[1]


def test_get_quads_agrees_across_layouts(vortex_files):
    """The code path and the quad path return identical rows.

    `get_quads` is served from term codes on a Dictionary-layout store and
    by re-serializing matched quads otherwise. A Dictionary store never reaches
    the second path, so only a cross-layout comparison exercises both.
    """
    fallback = VortexRdfStore(vortex_files["default"])
    codes = VortexRdfStore(vortex_files["dictionary"])
    assert codes.term_dict() is not None and fallback.term_dict() is None

    for pattern in PATTERNS:
        assert sorted(fallback.get_quads(**pattern)) == sorted(
            codes.get_quads(**pattern)
        ), f"paths disagree for {pattern}"


def test_get_quads_carries_named_graphs(quad_files, layout):
    """The graph position is part of the result, not dropped.

    The `.nt` fixture is entirely in the default graph, so the named-graph
    fixture is what distinguishes "graph reported" from "graph always empty".
    """
    store = VortexRdfStore(quad_files[layout])

    graphs = sorted(g for *_, g in store.get_quads())
    assert graphs == ["", "<http://ex.org/g1>", "<http://ex.org/g2>"]

    # A returned graph feeds straight back in as a pattern, including the
    # empty string for the default graph.
    for graph in graphs:
        selected = store.get_quads(g=graph)
        assert len(selected) == 1 and selected[0][3] == graph, graph


def test_package_ships_typing_files():
    pkg = pathlib.Path(vortex_rdf.__file__).parent
    assert (pkg / "py.typed").exists()
    assert (pkg / "_native.pyi").exists()
    assert any(
        p.name.startswith("_native.") and p.suffix in (".so", ".pyd") for p in pkg.iterdir()
    )
