import pytest

from vortex_rdf import serialize_rdf

FIXTURE_NT = """\
<http://ex.org/alice> <http://xmlns.com/foaf/0.1/name> "Alice" .
<http://ex.org/alice> <http://xmlns.com/foaf/0.1/knows> <http://ex.org/bob> .
<http://ex.org/bob> <http://xmlns.com/foaf/0.1/name> "Bob"@en .
<http://ex.org/bob> <http://ex.org/age> "42"^^<http://www.w3.org/2001/XMLSchema#integer> .
_:b0 <http://xmlns.com/foaf/0.1/name> "Anon" .
"""

FIXTURE_NQ = """\
<http://ex.org/s1> <http://ex.org/p> <http://ex.org/o1> <http://ex.org/g1> .
<http://ex.org/s2> <http://ex.org/p> <http://ex.org/o2> <http://ex.org/g2> .
<http://ex.org/s3> <http://ex.org/p> <http://ex.org/o3> .
"""

LAYOUTS = ["default", "typed-object", "dictionary"]
INDEXES = ["secondary-by-copy", "secondary-by-reference"]

#: `foaf:name`, the predicate of three fixture triples: two on IRI subjects,
#: one on a blank node.
NAME = "<http://xmlns.com/foaf/0.1/name>"
#: Match patterns over the fixture: every quad, a bound predicate, a bound
#: subject and a language-tagged object.
PATTERNS = ({}, {"p": NAME}, {"s": "<http://ex.org/bob>"}, {"o": '"Bob"@en'})


def u64s(col):
    """The values of a `U64Column`, read through its raw byte view, as a
    list of ints."""
    return memoryview(col).cast("Q").tolist()


def u64_rows(cols):
    """The rows of `match_codes` columns, as tuples of codes."""
    return list(zip(*(u64s(col) for col in cols)))


@pytest.fixture(params=LAYOUTS)
def layout(request):
    return request.param


@pytest.fixture(scope="session")
def fixture_nt_path(tmp_path_factory):
    path = tmp_path_factory.mktemp("rdf") / "fixture.nt"
    path.write_text(FIXTURE_NT)
    return path


@pytest.fixture(scope="session")
def fixture_nq_path(tmp_path_factory):
    path = tmp_path_factory.mktemp("rdf") / "fixture.nq"
    path.write_text(FIXTURE_NQ, encoding="utf-8")
    return path


def _serialize_variants(source, out_dir, variants):
    """`source` serialized once per `(key, layout, indexes)` of `variants`
    into `out_dir`, as `{key: path}`. A key is a layout name or a tuple of
    names, which also name the file."""
    files = {}
    for key, layout, indexes in variants:
        stem = "-".join(key) if isinstance(key, tuple) else key
        out = out_dir / f"{stem}.vortex"
        serialize_rdf(source, out, layout=layout, indexes=indexes)
        files[key] = out
    return files


@pytest.fixture(scope="session")
def vortex_files(fixture_nt_path, tmp_path_factory):
    """One serialized `.vortex` file per layout, keyed by layout name."""
    return _serialize_variants(
        fixture_nt_path,
        tmp_path_factory.mktemp("vortex"),
        [(layout, layout, []) for layout in LAYOUTS],
    )


@pytest.fixture(scope="session")
def indexed_files(fixture_nt_path, tmp_path_factory):
    """`vortex_files` built with one secondary index, keyed by
    `(layout, index)`."""
    return _serialize_variants(
        fixture_nt_path,
        tmp_path_factory.mktemp("vortex-indexed"),
        [((layout, index), layout, [index]) for layout in LAYOUTS for index in INDEXES],
    )


@pytest.fixture(scope="session")
def quad_files(fixture_nq_path, tmp_path_factory):
    """The named-graph fixture serialized per layout, keyed like
    `vortex_files`."""
    return _serialize_variants(
        fixture_nq_path,
        tmp_path_factory.mktemp("vortex-quads"),
        [(layout, layout, []) for layout in LAYOUTS],
    )
