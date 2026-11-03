import pytest
import pyarrow as pa

from vortex_rdf import serialize_rdf

FIXTURE_NT = """\
<http://ex.org/alice> <http://xmlns.com/foaf/0.1/name>  "Alice" .
<http://ex.org/alice> <http://xmlns.com/foaf/0.1/knows> <http://ex.org/bob> .
<http://ex.org/bob>   <http://xmlns.com/foaf/0.1/name>  "Bob"@en .
<http://ex.org/bob>   <http://ex.org/age>               "42"^^<http://www.w3.org/2001/XMLSchema#integer> .
_:b0 <http://xmlns.com/foaf/0.1/name> "Anon" .
"""

FIXTURE_NQ = """\
<http://ex.org/s1> <http://ex.org/p> <http://ex.org/o1> <http://ex.org/g1> .
<http://ex.org/s2> <http://ex.org/p> <http://ex.org/o2> <http://ex.org/g2> .
<http://ex.org/s3> <http://ex.org/p> <http://ex.org/o3> .
"""

LAYOUTS = ["default", "typed-object", "dictionary"]
INDEXES = ["secondary-by-copy", "secondary-by-reference"]


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


@pytest.fixture(scope="session")
def vortex_files(fixture_nt_path, tmp_path_factory):
    """One serialized `.vortex` file per layout, keyed by layout name."""
    out_dir = tmp_path_factory.mktemp("vortex")
    files = {}
    for layout in LAYOUTS:
        out = out_dir / f"fixture-{layout}.vortex"
        serialize_rdf(fixture_nt_path, out, layout=layout)
        files[layout] = out
    return files


@pytest.fixture(scope="session")
def indexed_files(fixture_nt_path, tmp_path_factory):
    """`vortex_files` built with one secondary index, keyed by
    `(layout, index)`."""
    out_dir = tmp_path_factory.mktemp("vortex-indexed")
    files = {}
    for layout in LAYOUTS:
        for index in INDEXES:
            out = out_dir / f"fixture-{layout}-{index}.vortex"
            serialize_rdf(fixture_nt_path, out, layout=layout, indexes=[index])
            files[(layout, index)] = out
    return files


@pytest.fixture(scope="session")
def quad_files(fixture_nq_path, tmp_path_factory):
    """The named-graph fixture serialized per layout, keyed like
    `vortex_files`."""
    out_dir = tmp_path_factory.mktemp("vortex-quads")
    files = {}
    for layout in LAYOUTS:
        out = out_dir / f"quads-{layout}.vortex"
        serialize_rdf(fixture_nq_path, out, layout=layout)
        files[layout] = out
    return files


@pytest.fixture(scope="session")
def arrow_table():
    subject = pa.array(
        [
            "<http://ex.org/alice>",
            "<http://ex.org/alice>",
            "<http://ex.org/bob>",
            "<http://ex.org/bob>",
        ],
        type=pa.string(),
    )
    predicate = pa.array(
        [
            "<http://xmlns.com/foaf/0.1/name>",
            "<http://xmlns.com/foaf/0.1/knows>",
            "<http://xmlns.com/foaf/0.1/name>",
            "<http://ex.org/age>",
        ],
        type=pa.string(),
    )
    obj = pa.array(
        [
            "Alice",
            "<http://ex.org/bob>",
            'Bob"@en',
            '42"^^<http://www.w3.org/2001/XMLSchema#integer>',
        ],
        type=pa.string(),
    )

    graph = pa.array(["", "", "", ""], type=pa.utf8())

    table = pa.table([subject, predicate, obj, graph], names=["s", "p", "o", "g"])

    return table
