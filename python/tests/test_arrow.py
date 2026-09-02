"""The Arrow PyCapsule interface: code columns and the dictionary as arrays,
matched rows as record-batch streams, consumed by pyarrow and polars (and
DuckDB when installed) with no pyarrow dependency in the package itself."""

import polars as pl
import pyarrow as pa
import pytest

import vortex_rdf
from vortex_rdf import ArrowQuadStream, VortexRdfError, VortexRdfStore

NAME = "<http://xmlns.com/foaf/0.1/name>"
PATTERNS = ({}, {"p": NAME}, {"s": "<http://ex.org/bob>"}, {"o": '"Bob"@en'})
ENCODINGS = ("codes", "terms", "strings")
COLUMNS = ["s", "p", "o", "g"]


def _address(column):
    return pa.py_buffer(memoryview(column)).address


def _table(stream):
    return pa.RecordBatchReader.from_stream(stream).read_all()


def _cells(table, dictionary=None):
    """Every column of `table` as a list of N-Triples strings."""
    cells = []
    for column in table.columns:
        if pa.types.is_uint32(column.type):
            cells.append([dictionary.decode(code) for code in column.to_pylist()])
        else:
            cells.append(column.to_pylist())
    return cells


def test_u32_column_exports_the_same_memory_as_its_buffer(vortex_files):
    store = VortexRdfStore(vortex_files["dictionary"])
    for column in store.match_codes():
        array = pa.array(column)
        assert array.type == pa.uint32()
        assert array.null_count == 0
        assert array.to_pylist() == memoryview(column).cast("I").tolist()
        assert array.buffers()[1].address == _address(column)
        series = pl.Series(column)
        assert series.dtype == pl.UInt32
        assert series.to_list() == array.to_pylist()


def test_term_dict_exports_the_decode_table(vortex_files):
    store = VortexRdfStore(vortex_files["dictionary"])
    dictionary = store.term_dict()
    array = pa.array(dictionary)
    assert array.type == pa.string_view()
    assert len(array) == len(dictionary)
    assert array.to_pylist() == [dictionary.decode(code) for code in range(len(dictionary))]
    assert pl.Series(dictionary).to_list() == array.to_pylist()


@pytest.mark.parametrize("in_memory", [False, True])
@pytest.mark.parametrize("encoding", ENCODINGS)
def test_match_arrow_rows_are_match_columns(vortex_files, encoding, in_memory):
    """File-backed stores export through the scan, in-memory ones off the
    adopted base; both agree with `match_columns` on every encoding."""
    store = VortexRdfStore(vortex_files["dictionary"], in_memory=in_memory)
    dictionary = store.term_dict()
    for pattern in PATTERNS:
        stream = store.match_arrow(**pattern, encoding=encoding)
        assert isinstance(stream, ArrowQuadStream)
        assert stream.encoding == encoding
        schema = pa.schema(stream)
        assert schema.names == COLUMNS
        assert schema.metadata[b"vortex_rdf.term_encoding"] == encoding.encode()
        assert schema.metadata[b"vortex_rdf.layout"] == b"dictionary"
        assert schema.metadata[b"vortex_rdf.default_graph"] == b""
        assert schema.metadata[b"vortex_rdf.version"] == vortex_rdf.__version__.encode()

        table = _table(stream)
        assert table.schema.names == COLUMNS
        expected_type = {
            "codes": pa.uint32(),
            "terms": pa.dictionary(pa.uint32(), pa.string_view()),
            "strings": pa.string_view(),
        }[encoding]
        assert all(field.type == expected_type for field in table.schema), pattern
        assert all(not field.nullable for field in table.schema)
        expected = [list(column) for column in store.match_columns(**pattern)]
        assert _cells(table, dictionary) == expected, pattern


def test_terms_share_one_dictionary_across_columns_and_batches(vortex_files):
    store = VortexRdfStore(vortex_files["dictionary"])
    table = _table(store.match_arrow(encoding="terms"))
    dictionary = pa.array(store.term_dict())
    for column in table.columns:
        for chunk in column.chunks:
            assert chunk.dictionary.equals(dictionary)
            assert chunk.dictionary.buffers()[1].address == dictionary.buffers()[1].address


def test_stream_is_consumed_once_but_schema_is_not(vortex_files):
    store = VortexRdfStore(vortex_files["dictionary"])
    stream = store.match_arrow()
    assert pa.schema(stream).names == COLUMNS
    assert _table(stream).num_rows == 5
    assert pa.schema(stream).names == COLUMNS
    with pytest.raises(ValueError, match="already consumed"):
        pa.RecordBatchReader.from_stream(stream)


def test_projection_picks_columns_in_order(vortex_files):
    store = VortexRdfStore(vortex_files["dictionary"])
    for encoding in ENCODINGS:
        table = _table(store.match_arrow(p=NAME, encoding=encoding, projection=["o", "s"]))
        assert table.schema.names == ["o", "s"]
        subjects, _, objects, _ = store.match_columns(p=NAME)
        assert _cells(table, store.term_dict()) == [list(objects), list(subjects)]
    with pytest.raises(ValueError):
        store.match_arrow(projection=["subject"])
    with pytest.raises(VortexRdfError):
        store.match_arrow(projection=[])
    with pytest.raises(VortexRdfError):
        store.match_arrow(projection=["s", "s"])


def test_encodings_per_layout(vortex_files, layout):
    store = VortexRdfStore(vortex_files[layout])
    with pytest.raises(ValueError):
        store.match_arrow(encoding="Codes")
    if layout == "typed-object":
        for encoding in ENCODINGS:
            with pytest.raises(VortexRdfError):
                store.match_arrow(encoding=encoding)
        return
    strings = _table(store.match_arrow(encoding="strings"))
    assert _cells(strings) == [list(column) for column in store.match_columns()]
    if layout == "default":
        for encoding in ("codes", "terms"):
            with pytest.raises(VortexRdfError):
                store.match_arrow(encoding=encoding)


def test_named_graphs_cross_the_stream(quad_files, layout):
    if layout == "typed-object":
        pytest.skip("no Arrow export for the typed-object layout")
    store = VortexRdfStore(quad_files[layout])
    table = _table(store.match_arrow(encoding="strings", projection=["g"]))
    assert sorted(table.column("g").to_pylist()) == ["", "<http://ex.org/g1>", "<http://ex.org/g2>"]


def test_polars_consumes_the_stream(vortex_files):
    store = VortexRdfStore(vortex_files["dictionary"])
    frame = pl.DataFrame(store.match_arrow(encoding="terms"))
    assert frame.columns == COLUMNS
    assert frame.height == 5
    assert frame.filter(pl.col("p") == NAME).height == 3
    codes = pl.DataFrame(store.match_arrow(p=NAME))
    assert codes.dtypes == [pl.UInt32] * 4
    dictionary = store.term_dict()
    assert sorted(dictionary.decode(code) for code in codes["s"]) == sorted(
        store.match_columns(p=NAME)[0]
    )


def test_duckdb_consumes_the_stream(vortex_files):
    duckdb = pytest.importorskip("duckdb")
    store = VortexRdfStore(vortex_files["dictionary"])
    quads = store.match_arrow(encoding="strings")  # noqa: F841 -- read by name below
    assert duckdb.sql("select count(*) from quads").fetchone()[0] == 5
