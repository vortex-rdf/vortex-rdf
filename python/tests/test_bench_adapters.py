"""The dashboard harness's Vortex adapters, run over the fixture dataset.

`bench/run.py` runs each adapter in its own virtualenv and keeps going when one
fails, so an adapter whose binding call no longer matches the bindings'
signature drops out of the dashboard silently. Nothing else imports `bench/`
and the harness is too slow for CI, so this exercises each Vortex adapter's
build/open/count path over five triples instead of the dashboard's millions.

Only the Vortex adapters: the others' libraries are deliberately absent from
this environment (see `bench/adapters.py` on why each gets its own virtualenv).
"""

from __future__ import annotations

import pytest

from adapters import VORTEX_SLUGS, build_adapter, split_term
from conftest import NAME
from datasets import FULL_SCAN_PATTERN, Pat

#: The fixture holds two `foaf:name` triples on IRI subjects plus one on a
#: blank node; a bound predicate is the pattern that routes through a secondary
#: index when the variant has one, so it covers the indexed cells too.
NAME_PATTERN = Pat("P", None, NAME, None, None)


@pytest.mark.parametrize("slug", VORTEX_SLUGS)
def test_vortex_adapter_round_trip(slug, fixture_nt_path, tmp_path):
    adapter = build_adapter(slug)
    assert adapter.slug == slug

    src = str(fixture_nt_path)
    artifact = adapter.artifact_path(str(tmp_path), src)
    handle = adapter.build(src, artifact)
    full, name = adapter.prepare(FULL_SCAN_PATTERN), adapter.prepare(NAME_PATTERN)
    assert adapter.count(handle, full) == 5
    assert adapter.count_only(handle, full) == 5
    assert adapter.count(handle, name) == 3
    assert adapter.count_only(handle, name) == 3
    adapter.dispose(handle)

    # The Open column of the dashboard: a second process reads the artifact the
    # build wrote, so the two paths must agree on the same file.
    reopened = adapter.open(artifact, src)
    assert adapter.count(reopened, full) == 5
    assert adapter.count_only(reopened, full) == 5
    adapter.dispose(reopened)

    assert adapter.artifact_bytes(artifact) > 0


@pytest.mark.parametrize(
    "term, parts",
    [
        ("<http://example.org/s>", ("iri", "http://example.org/s", None, None)),
        ('"plain"', ("literal", "plain", None, None)),
        ('"hello"@en', ("literal", "hello", "en", None)),
        (
            '"42"^^<http://www.w3.org/2001/XMLSchema#integer>',
            ("literal", "42", None, "http://www.w3.org/2001/XMLSchema#integer"),
        ),
        ("_:b0", ("blank", "b0", None, None)),
    ],
)
def test_split_term(term, parts):
    assert split_term(term) == parts


@pytest.mark.parametrize("garbage", ["", "http://no.brackets", '"unterminated', '"x"y', "nonsense"])
def test_split_term_rejects_garbage(garbage):
    with pytest.raises(ValueError):
        split_term(garbage)
