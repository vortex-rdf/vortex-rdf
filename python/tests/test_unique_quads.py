"""A built store holds each quad once and each term once, in canonical form.

The probe is eight lines that encode three distinct RDF triples. The parser
rewrites every spelling of a term to one canonical form, so the dictionary
holds six terms; the rows are deduplicated after sorting, so the store holds
three quads.
"""

import pytest

from vortex_rdf import VortexRdfStore, serialize_rdf

from conftest import INDEXES, LAYOUTS

# Three RDF triples under eight spellings: "x" four times (twice plain, once
# typed xsd:string, once through a \u escape), "y"@en twice (@EN and @en),
# and "z" under a subject written two ways.
PROBE_NT = r"""<http://ex.org/s> <http://ex.org/p> "x" .
<http://ex.org/s> <http://ex.org/p> "x" .
<http://ex.org/s> <http://ex.org/p> "x"^^<http://www.w3.org/2001/XMLSchema#string> .
<http://ex.org/s> <http://ex.org/p> "\u0078" .
<http://ex.org/s> <http://ex.org/p> "y"@EN .
<http://ex.org/s> <http://ex.org/p> "y"@en .
<http://ex.org/s> <http://ex.org/p> "z" .
<http://ex.org/\u0073> <http://ex.org/p> "z" .
"""

# The three distinct quads, in the form the store returns them (the default
# graph is the empty string).
PROBE_QUADS = [
    ("<http://ex.org/s>", "<http://ex.org/p>", '"x"', ""),
    ("<http://ex.org/s>", "<http://ex.org/p>", '"y"@en', ""),
    ("<http://ex.org/s>", "<http://ex.org/p>", '"z"', ""),
]

# Spellings of the same term, which a lookup resolves to one code.
SAME_TERM = [
    ['"x"', '"x"^^<http://www.w3.org/2001/XMLSchema#string>', '"\\u0078"'],
    ['"y"@en', '"y"@EN'],
    ["<http://ex.org/s>", "http://ex.org/s"],
    ["", "default", "[]"],
]

INDEX_SETS = [[], *[[index] for index in INDEXES], list(INDEXES)]


@pytest.fixture
def probe_nt(tmp_path):
    path = tmp_path / "probe.nt"
    path.write_text(PROBE_NT)
    return path


@pytest.mark.parametrize("layout", LAYOUTS)
@pytest.mark.parametrize("indexes", INDEX_SETS, ids=lambda s: "+".join(s) or "no-index")
@pytest.mark.parametrize("in_memory", [False, True])
def test_serialize_holds_each_quad_once(tmp_path, probe_nt, layout, indexes, in_memory):
    out = tmp_path / "probe.vortex"
    serialize_rdf(probe_nt, out, layout=layout, indexes=indexes)
    store = VortexRdfStore(out, in_memory=in_memory)

    quads = store.get_quads()
    assert len(quads) == len(set(quads)) == 3
    assert sorted(quads) == PROBE_QUADS
    assert len(store) == store.count_quads() == 3

    # Matches, served by an index where there is one, count each quad once.
    assert store.get_quads(o='"x"') == [PROBE_QUADS[0]]
    assert store.get_quads(o='"y"@en') == [PROBE_QUADS[1]]
    assert sorted(store.get_quads(p="<http://ex.org/p>")) == PROBE_QUADS
    assert store.count_quads(p="<http://ex.org/p>", o='"z"') == 1
    assert store.count_quads(s="<http://ex.org/s>") == 3


@pytest.mark.parametrize("indexes", INDEX_SETS, ids=lambda s: "+".join(s) or "no-index")
def test_dictionary_holds_one_code_per_rdf_term(tmp_path, probe_nt, indexes):
    out = tmp_path / "probe.vortex"
    serialize_rdf(probe_nt, out, layout="dictionary", indexes=indexes)
    store = VortexRdfStore(out)
    terms = store.term_dict()

    # The subject, the predicate, "x", "y"@en, "z" and the default graph.
    assert len(terms) == 6
    stored = {terms.decode(code) for code in range(len(terms))}
    assert stored == {
        "<http://ex.org/s>",
        "<http://ex.org/p>",
        '"x"',
        '"y"@en',
        '"z"',
        "",
    }

    # Every spelling of a term encodes to that term's one code.
    codes = []
    for spellings in SAME_TERM:
        resolved = {terms.encode(spelling) for spelling in spellings}
        assert len(resolved) == 1 and None not in resolved, spellings
        codes.extend(resolved)
    assert len(set(codes)) == len(SAME_TERM)

    # The columns carry those same codes: three rows, each term code in use.
    columns = store.match_codes()
    assert columns is not None
    rows = list(zip(*(memoryview(column).cast("Q").tolist() for column in columns)))
    assert len(rows) == len(set(rows)) == 3
