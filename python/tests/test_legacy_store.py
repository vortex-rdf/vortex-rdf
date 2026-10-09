"""Stores written before vortex-rdf 0.12 are refused, with an error that
names the cause and the way out.

0.12 readers rely on two guarantees that only a 0.12 writer gives: each quad
is stored once, and a reference index's children are in ``(val, rid)`` order.
A file of 0.11 or earlier can break either and still opens as a Vortex file,
so the store root layout was renamed from ``vortex-rdf.store.v1`` to
``vortex-rdf.store.v2`` and a ``v1`` root is refused rather than checked.

The two ids are the same length, so renaming one into the other in a written
store moves no offset, and a Vortex file carries no checksum over its footer:
that is how these tests make a file of 0.11 and earlier without a binary
fixture.
"""

import re

import pytest

from vortex_rdf import VortexRdfError, VortexRdfStore

from conftest import INDEXES, LAYOUTS

CURRENT = b"vortex-rdf.store.v2"
LEGACY = b"vortex-rdf.store.v1"


def as_written_before_0_12(data: bytes) -> bytes:
    assert data.count(CURRENT) == 1, "the footer names the root layout once"
    return data.replace(CURRENT, LEGACY)


def assert_actionable(error: pytest.ExceptionInfo) -> None:
    """The cause (version and root layout), that it is refused, the way out;
    and neither the generic message nor Vortex's own unknown-layout error."""
    message = str(error.value)
    for needle in (
        "written by vortex-rdf 0.11 or earlier",
        "vortex-rdf.store.v1",
        "cannot read",
        "rebuild it from its RDF source with vortex-rdf 0.12",
        "serialize_rdf",
    ):
        assert needle in message, (needle, message)
    assert not re.search(r"not a vortex-rdf store file|Invalid encoding ID", message), message


@pytest.mark.parametrize("layout", LAYOUTS)
def test_a_store_written_now_carries_the_v2_root(vortex_files, layout):
    data = vortex_files[layout].read_bytes()
    assert data.count(CURRENT) == 1
    assert data.count(LEGACY) == 0


@pytest.mark.parametrize("in_memory", [False, True])
@pytest.mark.parametrize("layout", LAYOUTS)
def test_a_pre_0_12_file_is_refused(tmp_path, vortex_files, layout, in_memory):
    legacy = tmp_path / "legacy.vortex"
    legacy.write_bytes(as_written_before_0_12(vortex_files[layout].read_bytes()))

    with pytest.raises(VortexRdfError) as error:
        VortexRdfStore(legacy, in_memory=in_memory)
    assert_actionable(error)

    # The current file it was made from still opens.
    assert len(VortexRdfStore(vortex_files[layout], in_memory=in_memory)) == 5


@pytest.mark.parametrize("index", INDEXES)
@pytest.mark.parametrize("layout", LAYOUTS)
def test_a_pre_0_12_indexed_file_is_refused(tmp_path, indexed_files, layout, index):
    legacy = tmp_path / "legacy.vortex"
    legacy.write_bytes(as_written_before_0_12(indexed_files[(layout, index)].read_bytes()))

    with pytest.raises(VortexRdfError) as error:
        VortexRdfStore(legacy)
    assert_actionable(error)


@pytest.mark.parametrize("layout", LAYOUTS)
def test_pre_0_12_bytes_are_refused(vortex_files, layout):
    legacy = as_written_before_0_12(vortex_files[layout].read_bytes())

    with pytest.raises(VortexRdfError) as error:
        VortexRdfStore.from_bytes(legacy)
    assert_actionable(error)
