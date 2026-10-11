"""A batch call stops the rest of its probes when one fails."""

import time

import pytest

from vortex_rdf import VortexRdfError, VortexRdfStore, serialize_rdf

ROWS = 300_000
PROBES = 8_000


@pytest.fixture(scope="module")
def string_store(tmp_path_factory):
    """A Default-layout store: every probe scans its strings, and a probe
    with a `keep` fails (a keep needs the Dictionary layout)."""
    directory = tmp_path_factory.mktemp("batch")
    source = directory / "rows.nt"
    with source.open("w") as out:
        for i in range(ROWS):
            out.write(
                f"<http://ex.org/s/{i}> <http://ex.org/p/{i % 17}> "
                f"<http://ex.org/o/{i % 100_003}> .\n"
            )
    target = directory / "rows.vortex"
    serialize_rdf(source, target, format="ntriples", layout="default", indexes=[])
    return VortexRdfStore(target)


def test_a_failing_probe_aborts_the_rest_of_the_batch(string_store):
    slow = {"o": "<http://ex.org/o/7>"}
    failing = {"o": "<http://ex.org/o/7>", "keep": {"s": (0, 5)}}

    # What the work costs when it all runs.
    started = time.process_time()
    assert len(string_store.count_quads_many([slow] * PROBES)) == PROBES
    whole_batch = time.process_time() - started
    assert whole_batch > 0.5, "the batch has to be real work for this test to mean anything"

    with pytest.raises(VortexRdfError, match="keep needs the Dictionary layout"):
        string_store.count_quads_many([failing] + [slow] * PROBES)
    returned = time.process_time()
    time.sleep(1.5)

    burnt_after_the_error = time.process_time() - returned
    assert burnt_after_the_error < whole_batch / 4, (
        f"probes kept running after the call raised: {burnt_after_the_error:.2f} s of CPU "
        f"against {whole_batch:.2f} s for the whole batch"
    )
