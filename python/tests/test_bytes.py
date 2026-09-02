"""`to_bytes` / `from_bytes`: the native-container round trip and its
failure modes."""

import pytest

from vortex_rdf import VortexRdfError, VortexRdfStore

NAME = "<http://xmlns.com/foaf/0.1/name>"


def test_bytes_round_trip(vortex_files, layout):
    store = VortexRdfStore(vortex_files[layout])
    clone = VortexRdfStore.from_bytes(store.to_bytes())
    assert clone.layout() == layout
    assert len(clone) == 5
    assert sorted(clone.get_quads()) == sorted(store.get_quads())
    assert sorted(clone.get_quads(p=NAME)) == sorted(store.get_quads(p=NAME))
    assert (clone.term_dict() is not None) == (layout == "dictionary")
    assert repr(clone) == f'VortexRdfStore(layout="{layout}")'


def test_from_bytes_accepts_file_bytes(vortex_files):
    path = vortex_files["dictionary"]
    from_file = VortexRdfStore(path)
    from_bytes = VortexRdfStore.from_bytes(path.read_bytes())
    assert from_bytes.layout() == "dictionary"
    assert sorted(from_bytes.get_quads()) == sorted(from_file.get_quads())
    assert from_bytes.term_dict() is not None


def test_from_bytes_rejects_corrupt_buffer(vortex_files):
    assert issubclass(VortexRdfError, Exception)
    assert not issubclass(VortexRdfError, OSError)
    assert not issubclass(VortexRdfError, ValueError)

    data = VortexRdfStore(vortex_files["dictionary"]).to_bytes()
    for corrupt in (b"not a vortex file", b"", data[: len(data) // 2]):
        with pytest.raises(VortexRdfError):
            VortexRdfStore.from_bytes(corrupt)


def test_from_bytes_rejects_str(vortex_files):
    with pytest.raises(TypeError):
        VortexRdfStore.from_bytes("not bytes")
