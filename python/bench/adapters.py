"""One adapter per Python RDF library under comparison.

Every import of a third-party library is deferred into the adapter that needs
it. Each adapter runs in its own virtualenv (see `run.py`) precisely because
their dependency sets conflict -- pycottas hard-pins `pyoxigraph==0.3.18`, so a
shared environment would silently bench a pyoxigraph two minor versions behind
the one the JavaScript tab compares against. A module-level import here would
break that isolation on the first `import adapters`.

MEASURED UNIT. `count` consumes a pattern's results and returns how many there
were, which is the only unit all five libraries share. What that materializes
differs by library and is not normalized away: Vortex builds N-Triples strings
in Rust, pyoxigraph yields Quad objects, rdflib yields term objects, and
pycottas/lightrdf yield tuples of strings. Each pays for its own natural
result form, the same contract the JavaScript tab's `countMatch` uses.

That contract cuts against Vortex on triple patterns: its result is a quad, so
it materializes a graph term per row that rdflib, lightrdf and pycottas do not.

TERM SPELLING. The canonical form throughout the harness is N-Triples
(`<iri>`, `"literal"`) -- what `datasets.py` writes and what the generated file
contains. Adapters convert inward. Vortex and lightrdf accept that form
directly; pyoxigraph, rdflib, and pycottas need parsed term objects.
"""

from __future__ import annotations

import os
from typing import Any, Iterable, Optional

from datasets import Pat

# ─── Canonical term parsing ─────────────────────────────────────────────────


def split_term(t: str) -> tuple[str, str, Optional[str], Optional[str]]:
    """Split an N-Triples term into (kind, value, language, datatype).

    Deliberately narrow: it handles exactly the shapes `datasets.py` emits
    (plain IRIs and quote-delimited literals with no escapes) plus the language
    and datatype suffixes, and raises on anything else rather than guessing. A
    lenient parser here would turn a generator change into silently mismatched
    probes across five libraries.
    """
    if t.startswith("<") and t.endswith(">"):
        return ("iri", t[1:-1], None, None)
    if t.startswith('"'):
        end = t.rfind('"')
        if end <= 0:
            raise ValueError(f"unterminated literal: {t!r}")
        value, suffix = t[1:end], t[end + 1 :]
        if suffix.startswith("@"):
            return ("literal", value, suffix[1:], None)
        if suffix.startswith("^^"):
            dt = suffix[2:]
            return ("literal", value, None, dt[1:-1] if dt.startswith("<") else dt)
        if suffix:
            raise ValueError(f"unexpected literal suffix {suffix!r} in {t!r}")
        return ("literal", value, None, None)
    if t.startswith("_:"):
        return ("blank", t[2:], None, None)
    raise ValueError(f"unrecognized N-Triples term: {t!r}")


# ─── Adapter base ───────────────────────────────────────────────────────────


class Adapter:
    """A library under test.

    `supports_quads` / `supports_mutation` drive the dashboard's "unsupported"
    cells. They record a fact about the library's API, not a gap in the
    harness, and each one is justified on the subclass.
    """

    slug: str = ""
    label: str = ""
    supports_quads: bool = True
    supports_mutation: bool = True
    #: Whether opening the built artifact is a distinct operation from building
    #: it. False for stores that live only in memory and must re-parse the
    #: source file every process start -- which is the comparison, not a flaw.
    has_distinct_open: bool = True

    def artifact_path(self, workdir: str, src: str) -> str:
        raise NotImplementedError

    def build(self, src: str, artifact: str) -> Any:
        """Ingest `src` (an .nt/.nq file) producing a queryable handle."""
        raise NotImplementedError

    def open(self, artifact: str, src: str) -> Any:
        """Get a queryable handle from an already-built artifact."""
        raise NotImplementedError

    def count(self, handle: Any, pat: Pat) -> int:
        raise NotImplementedError

    def add(self, handle: Any, quads: Iterable[tuple]) -> None:
        raise NotImplementedError

    def delete(self, handle: Any, quads: Iterable[tuple]) -> None:
        raise NotImplementedError

    def dispose(self, handle: Any) -> None:
        pass

    def artifact_bytes(self, artifact: str) -> Optional[int]:
        """On-disk size of the built artifact, or None when the library keeps
        no file of its own (its store lives in memory)."""
        if not artifact or not os.path.exists(artifact):
            return None
        if os.path.isdir(artifact):
            return sum(
                os.path.getsize(os.path.join(root, f))
                for root, _, files in os.walk(artifact)
                for f in files
            )
        return os.path.getsize(artifact)


# ─── Vortex-RDF (this project) ──────────────────────────────────────────────


class VortexAdapter(Adapter):
    """`serialize_rdf` to build a .vortex file, then open and match it.

    supports_mutation is False: the Python bindings expose no add/delete at all
    (`VortexRdfStore` is open / match / to_bytes). That is a real gap in the
    bindings, not an omission here -- the JS bindings do have addQuad/addQuads.
    """

    supports_mutation = False

    def __init__(
        self,
        slug: str,
        label: str,
        layout: str,
        indexes: list[str],
        in_memory: bool = False,
    ):
        self.slug, self.label = slug, label
        self.layout, self.indexes = layout, indexes
        #: File-backed (the default) reads columns from the .vortex file per
        #: query; in-memory loads the whole file up front. Both are shipped
        #: modes, and the Rust tab measures the same axis -- pyoxigraph and
        #: rdflib are in-memory stores, so the comparison is only interpretable
        #: with both Vortex modes on the page.
        self.in_memory = in_memory

    def artifact_path(self, workdir: str, src: str) -> str:
        # Keyed by source stem so the triples and quads artifacts never collide.
        stem = os.path.splitext(os.path.basename(src))[0]
        return os.path.join(workdir, f"{self.slug}.{stem}.vortex")

    def build(self, src: str, artifact: str) -> Any:
        from vortex_rdf import _native as native

        if os.path.exists(artifact):
            os.remove(artifact)
        native.serialize_rdf(src, artifact, layout=self.layout, indexes=self.indexes)
        return native.VortexRdfStore(artifact, in_memory=self.in_memory)

    def open(self, artifact: str, src: str) -> Any:
        from vortex_rdf import _native as native

        return native.VortexRdfStore(artifact, in_memory=self.in_memory)

    def count(self, handle: Any, pat: Pat) -> int:
        # `get_quads` is the idiomatic read, and it materializes four terms per
        # row. On a triple pattern the libraries whose result is a triple build
        # three, so Vortex is doing strictly more work per row here, not less.
        return len(handle.get_quads(pat.s, pat.p, pat.o, pat.g))


# ─── pyoxigraph ─────────────────────────────────────────────────────────────


class PyoxigraphAdapter(Adapter):
    """In-memory `Store` + `bulk_load`, mirroring the JavaScript tab's oxigraph.

    has_distinct_open is False: an in-memory Store has no artifact to reopen,
    so a fresh process must bulk_load the source file again. The Open column
    measures exactly that -- it is the cost the file-backed formats avoid.
    """

    slug, label = "pyoxigraph", "pyoxigraph"
    has_distinct_open = False

    def _term(self, t: Optional[str]):
        if t is None:
            return None
        import pyoxigraph as ox

        kind, value, lang, dt = split_term(t)
        if kind == "iri":
            return ox.NamedNode(value)
        if kind == "blank":
            return ox.BlankNode(value)
        if dt:
            return ox.Literal(value, datatype=ox.NamedNode(dt))
        if lang:
            return ox.Literal(value, language=lang)
        return ox.Literal(value)

    def artifact_path(self, workdir: str, src: str) -> str:
        return ""  # in-memory: no artifact of its own

    def _mime(self, src: str) -> str:
        return "application/n-quads" if src.endswith(".nq") else "application/n-triples"

    def build(self, src: str, artifact: str) -> Any:
        import pyoxigraph as ox

        store = ox.Store()
        with open(src, "rb") as f:
            store.bulk_load(f, self._mime(src))
        return store

    def open(self, artifact: str, src: str) -> Any:
        return self.build(src, artifact)

    def count(self, handle: Any, pat: Pat) -> int:
        g = self._term(pat.g)
        return sum(
            1
            for _ in handle.quads_for_pattern(
                self._term(pat.s), self._term(pat.p), self._term(pat.o), g
            )
        )

    def _quad(self, q: tuple):
        import pyoxigraph as ox

        s, p, o = self._term(q[0]), self._term(q[1]), self._term(q[2])
        g = self._term(q[3]) if len(q) > 3 and q[3] else ox.DefaultGraph()
        return ox.Quad(s, p, o, g)

    def add(self, handle: Any, quads: Iterable[tuple]) -> None:
        for q in quads:
            handle.add(self._quad(q))

    def delete(self, handle: Any, quads: Iterable[tuple]) -> None:
        for q in quads:
            handle.remove(self._quad(q))


# ─── pycottas ───────────────────────────────────────────────────────────────


class PycottasAdapter(Adapter):
    """`rdf2cottas` builds a Parquet-backed .cottas file; queries are DuckDB SQL.

    supports_mutation is False on the library's own authority: every write
    method on `pycottas.COTTASStore` raises
    `TypeError('The COTTAS store is read only!')`.
    """

    slug, label = "pycottas", "pycottas"
    supports_mutation = False

    def _term(self, t: Optional[str]):
        if t is None:
            return None
        from rdflib.util import from_n3

        return from_n3(t)

    def artifact_path(self, workdir: str, src: str) -> str:
        stem = os.path.splitext(os.path.basename(src))[0]
        return os.path.join(workdir, f"pycottas.{stem}.cottas")

    def build(self, src: str, artifact: str) -> Any:
        import pycottas

        if os.path.exists(artifact):
            os.remove(artifact)
        pycottas.rdf2cottas(src, artifact, index="spo")
        return pycottas.COTTASDocument(artifact)

    def open(self, artifact: str, src: str) -> Any:
        import pycottas

        return pycottas.COTTASDocument(artifact)

    def count(self, handle: Any, pat: Pat) -> int:
        terms = [self._term(pat.s), self._term(pat.p), self._term(pat.o)]
        if pat.g is not None:
            terms.append(self._term(pat.g))
        return len(handle.search(tuple(terms)))


# ─── lightrdf ───────────────────────────────────────────────────────────────


class LightrdfAdapter(Adapter):
    """A streaming pattern-filtered parser, not a store.

    `RDFDocument.search_triples` re-parses the entire source file on every
    call -- there is no index and no build step, so this adapter is the
    no-index baseline: what every pattern costs when nothing is precomputed.

    supports_quads is False: `search_triples(s, p, o)` takes no graph argument.
    supports_mutation is False: there is nothing to mutate.
    """

    slug, label = "lightrdf", "lightrdf"
    supports_quads = False
    supports_mutation = False
    has_distinct_open = True  # opening is just wrapping the path -- near zero

    def artifact_path(self, workdir: str, src: str) -> str:
        return src  # its "artifact" is the source file itself

    def build(self, src: str, artifact: str) -> Any:
        # No store to construct. The honest ingest-equivalent is one full parse
        # of the source, which is what every other adapter's build must also do
        # before it can index anything.
        import lightrdf

        doc = lightrdf.RDFDocument(src)
        for _ in doc.search_triples(None, None, None):
            pass
        return doc

    def open(self, artifact: str, src: str) -> Any:
        import lightrdf

        return lightrdf.RDFDocument(src)

    def count(self, handle: Any, pat: Pat) -> int:
        return sum(1 for _ in handle.search_triples(pat.s, pat.p, pat.o))


# ─── rdflib ─────────────────────────────────────────────────────────────────


class RdflibAdapter(Adapter):
    """The reference pure-Python implementation: `Graph`/`Dataset` + `parse`.

    has_distinct_open is False for the same reason as pyoxigraph: the graph is
    in memory, so a fresh process re-parses the file.
    """

    slug, label = "rdflib", "rdflib"
    has_distinct_open = False

    def _term(self, t: Optional[str]):
        if t is None:
            return None
        from rdflib.util import from_n3

        return from_n3(t)

    def artifact_path(self, workdir: str, src: str) -> str:
        return ""

    def build(self, src: str, artifact: str) -> Any:
        from rdflib import Dataset, Graph

        if src.endswith(".nq"):
            g = Dataset()
            g.parse(src, format="nquads")
        else:
            g = Graph()
            g.parse(src, format="nt")
        return g

    def open(self, artifact: str, src: str) -> Any:
        return self.build(src, artifact)

    def count(self, handle: Any, pat: Pat) -> int:
        s, p, o = self._term(pat.s), self._term(pat.p), self._term(pat.o)
        if pat.g is not None:
            return sum(1 for _ in handle.quads((s, p, o, self._term(pat.g))))
        return sum(1 for _ in handle.triples((s, p, o)))

    def _rdflib_triple(self, q: tuple):
        return (self._term(q[0]), self._term(q[1]), self._term(q[2]))

    def add(self, handle: Any, quads: Iterable[tuple]) -> None:
        for q in quads:
            handle.add(self._rdflib_triple(q))

    def delete(self, handle: Any, quads: Iterable[tuple]) -> None:
        for q in quads:
            handle.remove(self._rdflib_triple(q))


# ─── Registry ───────────────────────────────────────────────────────────────

#: Vortex build variants, the same star design the Rust and JS tabs use:
#: layout x secondary index x residency. Every one of them is a row
#: in the cross-library panels.
#:
#: Residency and secondary indexes are crossed rather than varied one at a
#: time: every Dictionary build appears file-backed and in-memory, so the
#: index's effect can be read within a residency mode and residency's within
#: an index configuration. The in-memory indexed cells are the ones that match
#: how pyoxigraph and rdflib are configured.
VORTEX_VARIANTS = [
    ("vortex_dict", "Vortex Dict", "dictionary", [], False),
    ("vortex_dict_mem", "Vortex Dict (in-memory)", "dictionary", [], True),
    ("vortex_default", "Vortex Default", "default", [], False),
    ("vortex_dict_bycopy", "Vortex Dict+ByCopy", "dictionary", ["secondary-by-copy"], False),
    ("vortex_dict_bycopy_mem", "Vortex Dict+ByCopy (in-memory)", "dictionary", ["secondary-by-copy"], True),
    ("vortex_dict_byref", "Vortex Dict+ByRef", "dictionary", ["secondary-by-reference"], False),
    ("vortex_dict_byref_mem", "Vortex Dict+ByRef (in-memory)", "dictionary", ["secondary-by-reference"], True),
]


def build_adapter(slug: str) -> Adapter:
    """Construct one adapter by slug, importing only that library."""
    for vslug, label, layout, indexes, in_memory in VORTEX_VARIANTS:
        if slug == vslug:
            return VortexAdapter(vslug, label, layout, indexes, in_memory)
    if slug == "pyoxigraph":
        return PyoxigraphAdapter()
    if slug == "pycottas":
        return PycottasAdapter()
    if slug == "lightrdf":
        return LightrdfAdapter()
    if slug == "rdflib":
        return RdflibAdapter()
    raise KeyError(f"unknown adapter slug: {slug}")


#: Every adapter a full run measures, in dashboard row order: each Vortex
#: variant is a row in the cross-library panels alongside the other libraries,
#: not a footnote to them.
ALL_SLUGS = [v[0] for v in VORTEX_VARIANTS] + ["pyoxigraph", "pycottas", "rdflib", "lightrdf"]

#: Which virtualenv each adapter needs. Vortex variants share one.
VENV_FOR = {
    **{v[0]: "vortex" for v in VORTEX_VARIANTS},
    "pyoxigraph": "pyoxigraph",
    "pycottas": "pycottas",
    "rdflib": "rdflib",
    "lightrdf": "lightrdf",
}
