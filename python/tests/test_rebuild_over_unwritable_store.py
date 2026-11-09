"""Rebuilding a store file over one the process cannot write.

Before it builds anything, `serialize_rdf` asks the store at the path to
open for writing. A store the process may not write is never replaced: the
call raises `PermissionError` and leaves the store as it was.
"""

import sys

import pytest

from vortex_rdf import VortexRdfStore, serialize_rdf

from conftest import write_ntriples

# The store at the path, and the rebuild refused over it.
OLD_TRIPLES = 3_000
NEW_TRIPLES = 5

pytestmark = pytest.mark.skipif(
    sys.platform != "linux",
    reason="the store is made unwritable through POSIX permission bits, checked on Linux here",
)


def statements(tag, count):
    """`count` triples whose subjects and literals carry `tag`."""
    return (f'<http://ex.org/{tag}/s{i}> <http://ex.org/p{i % 3}> "{tag} {i}" .' for i in range(count))


def test_serialize_refuses_to_replace_a_store_it_cannot_write(tmp_path):
    old_nt = write_ntriples(tmp_path / "old.nt", statements("old", OLD_TRIPLES))
    new_nt = write_ntriples(tmp_path / "new.nt", statements("new", NEW_TRIPLES))
    store = tmp_path / "store.vortex"
    serialize_rdf(old_nt, store)
    before = store.read_bytes()
    store.chmod(0o444)
    try:
        # Root (or CAP_DAC_OVERRIDE) writes a 0444 file anyway, and the
        # refusal cannot be observed there.
        with open(store, "r+b"):
            pytest.skip("this process can write a 0444 file (root?)")
    except PermissionError:
        pass

    with pytest.raises(PermissionError, match="never replaced"):
        serialize_rdf(new_nt, store)

    assert store.read_bytes() == before
    assert store.stat().st_mode & 0o7777 == 0o444
    assert sorted(p.name for p in tmp_path.iterdir()) == ["new.nt", "old.nt", "store.vortex"]
    assert len(VortexRdfStore(store)) == OLD_TRIPLES
