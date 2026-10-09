"""Rebuilding a store file while a `VortexRdfStore` has it open.

A file store is memory-mapped, so a writer that truncates and rewrites the
path pulls the pages out from under the open store (SIGBUS on its next
read). `serialize_rdf` builds beside the path and renames the finished file
over it, which leaves the open store reading the file it mapped.
"""

import sys

import pytest

from vortex_rdf import VortexRdfStore, serialize_rdf

# The old file must dwarf its replacement, so an in-place rewrite leaves most
# of the old mapping past the new end of file.
OLD_TRIPLES = 3_000
NEW_TRIPLES = 5

pytestmark = pytest.mark.skipif(
    sys.platform != "linux",
    reason="replacing a mapped file by rename is only guaranteed on Linux here",
)


def write_ntriples(path, tag, count):
    path.write_text(
        "".join(
            f'<http://ex.org/{tag}/s{i}> <http://ex.org/p{i % 3}> "{tag} {i}" .\n'
            for i in range(count)
        )
    )


@pytest.mark.parametrize("layout", ["default", "dictionary"])
def test_serialize_over_a_live_store_leaves_it_readable(tmp_path, layout):
    old_nt, new_nt = tmp_path / "old.nt", tmp_path / "new.nt"
    write_ntriples(old_nt, "old", OLD_TRIPLES)
    write_ntriples(new_nt, "new", NEW_TRIPLES)
    path = tmp_path / "store.vortex"

    serialize_rdf(old_nt, path, layout=layout)
    live = VortexRdfStore(path)
    assert len(live) == OLD_TRIPLES

    serialize_rdf(new_nt, path, layout=layout)

    # The store opened before the rebuild still answers from the file it
    # mapped.
    quads = live.get_quads()
    assert len(quads) == OLD_TRIPLES
    assert all("/old/" in s for s, _, _, _ in quads)
    assert live.count_quads(p="<http://ex.org/p1>") == OLD_TRIPLES // 3

    # A store opened after it sees the new data, and the rebuild left no
    # sibling behind.
    fresh = VortexRdfStore(path)
    assert len(fresh) == NEW_TRIPLES
    assert all("/new/" in s for s, _, _, _ in fresh.get_quads())
    assert sorted(entry.name for entry in tmp_path.iterdir()) == [
        "new.nt",
        "old.nt",
        "store.vortex",
    ]
