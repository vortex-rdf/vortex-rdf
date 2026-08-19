"""The dashboard harness's Vortex adapters, run over the fixture dataset.

`bench/run.py` runs each adapter in its own virtualenv and keeps going when one
fails, so an adapter that raises drops out of the dashboard silently -- a full
run just writes fewer rows. That is how a `serialize_rdf(builder=...)` call
outlived the argument. Nothing else in the suite imports `bench/`, and the
harness itself is far too slow to run in CI, so this exercises the one thing
that goes stale -- the binding call in each adapter's build/open/count path --
over five triples instead of the dashboard's millions.

Only the Vortex adapters: the others' libraries are deliberately absent from
this environment (see `bench/adapters.py` on why each gets its own virtualenv).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

BENCH_DIR = Path(__file__).resolve().parents[1] / "bench"
sys.path.insert(0, str(BENCH_DIR))

from adapters import VORTEX_VARIANTS, build_adapter  # noqa: E402
from datasets import FULL_SCAN_PATTERN, Pat  # noqa: E402

VORTEX_SLUGS = [v[0] for v in VORTEX_VARIANTS]

#: The fixture holds two `foaf:name` triples on IRI subjects plus one on a
#: blank node; a bound predicate is the pattern that routes through a secondary
#: index when the variant has one, so it covers the indexed cells too.
NAME_PATTERN = Pat("P", None, "<http://xmlns.com/foaf/0.1/name>", None, None)


@pytest.mark.parametrize("slug", VORTEX_SLUGS)
def test_vortex_adapter_round_trip(slug, fixture_nt_path, tmp_path):
    adapter = build_adapter(slug)
    assert adapter.slug == slug

    src = str(fixture_nt_path)
    artifact = adapter.artifact_path(str(tmp_path), src)
    handle = adapter.build(src, artifact)
    assert adapter.count(handle, FULL_SCAN_PATTERN) == 5
    assert adapter.count(handle, NAME_PATTERN) == 3
    adapter.dispose(handle)

    # The Open column of the dashboard: a second process reads the artifact the
    # build wrote, so the two paths must agree on the same file.
    reopened = adapter.open(artifact, src)
    assert adapter.count(reopened, FULL_SCAN_PATTERN) == 5
    adapter.dispose(reopened)

    assert adapter.artifact_bytes(artifact) > 0
