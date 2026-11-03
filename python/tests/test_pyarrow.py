import pytest

from vortex_rdf import VortexRdfError, VortexRdfStore

NAME = "<http://xmlns.com/foaf/0.1/name>"


def test_arrow_round_trip(arrow_table):
    clone = VortexRdfStore.from_pyarrow(arrow_table)
    layout = "dictionary"
    assert clone.layout() == layout
    assert len(clone) == 4
    assert clone.term_dict() is not None
    assert clone.match_codes() is not None
    assert repr(clone) == f'VortexRdfStore(layout="{layout}")'
