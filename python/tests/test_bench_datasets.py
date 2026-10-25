"""Golden values of the benchmark dataset generator, computed once and pinned
with identical literals in js/test/bench-datasets.test.ts and
core/tests/bench_dataset.rs, so the three dashboard tabs keep measuring the
same data."""

from __future__ import annotations

from datasets import (
    DatasetOpts,
    Moduli,
    dataset_prefix,
    dataset_probes,
    fresh_quads,
    moduli,
    object_term,
    quad_at,
)

BASE = "http://data.example.org"
LITERAL_FRAC = 0.4


def test_moduli_at_the_dashboard_scales_with_8_graphs():
    assert moduli(32768, DatasetOpts(graphs=8)) == Moduli(3277, 32, 16387, 9, 19705)
    assert moduli(1048576, DatasetOpts(graphs=8)) == Moduli(104858, 33, 524291, 17, 629199)


def test_nquads_spellings_of_quads_0_1_7_and_12345_at_32768_rows():
    m = moduli(32768, DatasetOpts(graphs=8))

    def spell(i: int) -> str:
        s, p, o, g = quad_at(i, m, LITERAL_FRAC)
        return f"{s} {p} {o} {g} ."

    assert spell(0) == (
        f"<{BASE}/resource/2026/subject/000000000> <{BASE}/ontology/2026/property/0000> "
        f'"descriptive object value number 000000000" <{BASE}/graph/2026/named/000000> .'
    )
    assert spell(1) == (
        f"<{BASE}/resource/2026/subject/000000001> <{BASE}/ontology/2026/property/0001> "
        f'"descriptive object value number 000000001" <{BASE}/graph/2026/named/000001> .'
    )
    assert spell(7) == (
        f"<{BASE}/resource/2026/subject/000000007> <{BASE}/ontology/2026/property/0007> "
        f"<{BASE}/resource/2026/object/000000007> <{BASE}/graph/2026/named/000007> ."
    )
    assert spell(12345) == (
        f"<{BASE}/resource/2026/subject/000002514> <{BASE}/ontology/2026/property/0025> "
        f"<{BASE}/resource/2026/object/000012345> <{BASE}/graph/2026/named/000006> ."
    )


def test_object_0_is_a_literal_so_the_o_probe_binds_one():
    assert object_term(0, LITERAL_FRAC) == '"descriptive object value number 000000000"'


def test_single_graph_dataset_yields_no_quad_probes():
    assert dataset_probes(32768, DatasetOpts(graphs=1))["quads"] == []
    quads = dataset_probes(32768, DatasetOpts(graphs=8))["quads"]
    assert [p.name for p in quads] == ["G", "SPOG"]
    assert quads[0].g == f"<{BASE}/graph/2026/named/000000>"


def test_drop_graph_projection_keeps_row_identity():
    # The triples projection is the quad dataset minus its fourth term: the
    # graph modulus is nudged last, so the first three moduli -- and the first
    # three terms of every row -- are identical under the quad options.
    m = moduli(32768, DatasetOpts(graphs=8))
    single = moduli(32768, DatasetOpts(graphs=1))
    assert (m.n_subj, m.n_pred, m.n_obj) == (single.n_subj, single.n_pred, single.n_obj)
    assert quad_at(12345, m, LITERAL_FRAC)[:3] == quad_at(12345, single, LITERAL_FRAC)[:3]
    assert quad_at(12345, m, LITERAL_FRAC)[3] == f"<{BASE}/graph/2026/named/000006>"
    assert quad_at(12345, single, LITERAL_FRAC)[3] is None


def test_fresh_quads_are_disjoint_from_the_dataset():
    fresh = set(fresh_quads(64))
    prefix = {q[:3] for q in dataset_prefix(32768, 64, DatasetOpts(graphs=8))}
    assert fresh.isdisjoint(prefix)
    assert all(s.startswith(f"<{BASE}/fresh/") for s, _, _ in fresh)
