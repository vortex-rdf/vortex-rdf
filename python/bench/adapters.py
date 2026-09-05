"""One adapter per Python RDF library under comparison.

Every import of a third-party library is deferred into the adapter that needs
it: each adapter runs in its own virtualenv (see `run.py` for why), and a
module-level import here would break that isolation on the first
`import adapters`.

MEASURED UNIT. `count` consumes a pattern's results and returns how many there
were: every matched term's lexical string is read in Python and its length
folded into a sink -- the same rule the Rust and JavaScript harnesses apply --
so what a cell times is producing results, not walking index entries. Each
library still delivers its own natural result form (Vortex N-Triples strings
built in Rust, pyoxigraph Quad objects whose `.value` reads build the Python
strings, rdflib str-subclass terms, pycottas/lightrdf tuples of strings);
producing that form for every matched row is exactly the measured work.

On a triple pattern the result widths differ, and that is not normalized away:
Vortex, pyoxigraph and rdflib all deliver quads (four terms per row), while
pycottas returns 3-tuples and lightrdf, reading the triples projection, has no
graph to return.

`count_only` is the COUNT/ASK twin: resolve the same pattern and return only
how many rows matched, reading nothing else -- each library's cheapest correct
count path. Vortex counts from the match's row selection without materializing
a term; the others walk their iterators or result lists unread.

TERM SPELLING. The canonical form throughout the harness is N-Triples
(`<iri>`, `"literal"`) -- what `datasets.py` writes and what the generated file
contains. Adapters convert inward through `prepare`, once per pattern and
outside the timed region: Vortex and lightrdf accept that form directly;
pyoxigraph, rdflib, and pycottas need parsed term objects.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Iterable, Optional

from datasets import Pat

# ─── The uniform consume rule ───────────────────────────────────────────────

#: Where every consumed term length lands. Python never dead-code-eliminates
#: the reads, but the sink keeps the contract identical to the Rust and
#: JavaScript harnesses' black-boxed accumulators.
_consumed = 0


def consume(acc: int) -> None:
    global _consumed
    _consumed += acc


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


def rdflib_term(t: Optional[str]):
    """An rdflib term for an N-Triples spelling; `None` stays a wildcard."""
    if t is None:
        return None
    from rdflib.util import from_n3

    return from_n3(t)


# ─── Adapter base ───────────────────────────────────────────────────────────


class Adapter:
    """A library under test.

    `supports_quads` and `mutation_unsupported` drive the dashboard's
    "unsupported" cells. They record a fact about the library's API, not a gap
    in the harness, and each one is justified on the subclass.
    """

    slug: str = ""
    label: str = ""
    supports_quads: bool = True
    #: The reason the library cannot add/delete, shown as the cell's tooltip;
    #: `None` when it can.
    mutation_unsupported: Optional[str] = None
    #: Whether opening the built artifact is a distinct operation from building
    #: it. False for stores that live only in memory and must re-parse the
    #: source file every process start; those get no Open measurement, because
    #: the re-parse is what the Build column already reports.
    has_distinct_open: bool = True
    #: Whether the library hands a match out as Arrow, i.e. whether
    #: ``arrow_count`` is implemented. Drives the Arrow role, which is skipped
    #: entirely for a library without one.
    supports_arrow: bool = False

    def artifact_path(self, workdir: str, src: str) -> str:
        raise NotImplementedError

    def build(self, src: str, artifact: str) -> Any:
        """Ingest `src` (an .nt/.nq file) producing a queryable handle."""
        raise NotImplementedError

    def open(self, artifact: str, src: str) -> Any:
        """Get a queryable handle from an already-built artifact."""
        raise NotImplementedError

    def prepare(self, pat: Pat) -> Any:
        """Convert a pattern into the form `count`/`count_only` take, so term
        parsing happens once per pattern and outside the timed region."""
        return pat

    def count(self, handle: Any, query: Any) -> int:
        """Consume every match of a prepared pattern (see the module doc's
        MEASURED UNIT) and return how many there were."""
        raise NotImplementedError

    def count_only(self, handle: Any, query: Any) -> int:
        """Resolve the same prepared pattern and return only the match count
        -- no term is read. Each library's cheapest correct count path: the
        COUNT/ASK shape of the workload."""
        raise NotImplementedError

    def arrow_count(self, handle: Any, query: Any, encoding: str) -> int:
        """Resolve the same prepared pattern and read the matched rows as
        Arrow columns, returning how many there were.

        ``strings`` reads all four term values of every row into Python, which
        is what ``count`` reads out of quads -- so the pair prices the same
        delivered data two ways. ``codes`` reads the ``uint32`` code columns'
        buffers and decodes no term at all: not a like-for-like with the other
        two, but the currency a query engine joins on.

        Only the libraries with an Arrow export implement it (see
        ``supports_arrow``); the others simply have no Arrow cells.
        """
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

    mutation_unsupported: the Python bindings expose no add/delete API
    (`VortexRdfStore` is open / match / to_bytes).
    """

    mutation_unsupported = "the Python bindings expose no add/delete API"
    supports_arrow = True

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
        from vortex_rdf import VortexRdfStore, serialize_rdf

        if os.path.exists(artifact):
            os.remove(artifact)
        serialize_rdf(src, artifact, layout=self.layout, indexes=self.indexes)
        return VortexRdfStore(artifact, in_memory=self.in_memory)

    def open(self, artifact: str, src: str) -> Any:
        from vortex_rdf import VortexRdfStore

        return VortexRdfStore(artifact, in_memory=self.in_memory)

    def count(self, handle: Any, pat: Pat) -> int:
        # `get_quads` is the idiomatic read, and it materializes four terms per
        # row. On a triple pattern the libraries whose result is a triple build
        # three, so Vortex is doing strictly more work per row here, not less.
        acc = rows = 0
        for s, p, o, g in handle.get_quads(pat.s, pat.p, pat.o, pat.g):
            acc += len(s) + len(p) + len(o) + len(g)
            rows += 1
        consume(acc)
        return rows

    def count_only(self, handle: Any, pat: Pat) -> int:
        return handle.count_quads(pat.s, pat.p, pat.o, pat.g)

    def arrow_count(self, handle: Any, pat: Pat, encoding: str) -> int:
        import pyarrow as pa

        # A fresh stream per call: `__arrow_c_stream__` is taken out of the
        # object once, so the export is part of every measured iteration
        # exactly as `get_quads` is.
        stream = handle.match_arrow(pat.s, pat.p, pat.o, pat.g, encoding=encoding)
        table = pa.RecordBatchReader.from_stream(stream).read_all()
        acc = 0
        if encoding == "codes":
            # The read an engine does: each column's `uint32` values buffer,
            # nothing decoded. Buffer 0 is the (absent) validity bitmap.
            for name in ("s", "p", "o", "g"):
                for chunk in table.column(name).chunks:
                    acc += len(chunk.buffers()[1])
        else:
            # Every term value, as Python strings -- the same four per row
            # `count` builds out of `get_quads`.
            for name in ("s", "p", "o", "g"):
                for value in table.column(name).to_pylist():
                    acc += len(value)
        consume(acc)
        return table.num_rows


# ─── pyoxigraph ─────────────────────────────────────────────────────────────


class PyoxigraphAdapter(Adapter):
    """In-memory `Store` + `bulk_load`, mirroring the JavaScript tab's oxigraph.

    has_distinct_open is False: an in-memory Store has no artifact to reopen,
    so a fresh process must bulk_load the source file again -- which is what
    the Build column already measures, so Open is left blank (a re-parse is
    not comparable with a file-backed format's footer read).
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

    def prepare(self, pat: Pat) -> tuple:
        return (self._term(pat.s), self._term(pat.p), self._term(pat.o), self._term(pat.g))

    def count(self, handle: Any, query: tuple) -> int:
        acc = rows = 0
        for q in handle.quads_for_pattern(*query):
            acc += len(q.subject.value) + len(q.predicate.value) + len(q.object.value)
            gn = q.graph_name
            acc += len(gn.value) if hasattr(gn, "value") else 0
            rows += 1
        consume(acc)
        return rows

    def count_only(self, handle: Any, query: tuple) -> int:
        # No count API: the iterator still yields quad wrappers, which is this
        # store's floor for a count; their term strings stay unbuilt.
        return sum(1 for _ in handle.quads_for_pattern(*query))

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

    mutation_unsupported on the library's own authority: every write method on
    `pycottas.COTTASStore` raises `TypeError('The COTTAS store is read only!')`.
    """

    slug, label = "pycottas", "pycottas"
    mutation_unsupported = "COTTASStore raises: 'The COTTAS store is read only!'"

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

    def prepare(self, pat: Pat) -> tuple:
        terms = [rdflib_term(pat.s), rdflib_term(pat.p), rdflib_term(pat.o)]
        if pat.g is not None:
            terms.append(rdflib_term(pat.g))
        return tuple(terms)

    def count(self, handle: Any, query: tuple) -> int:
        acc = rows = 0
        for row in handle.search(query):
            for t in row:
                acc += len(t)
            rows += 1
        consume(acc)
        return rows

    def count_only(self, handle: Any, query: tuple) -> int:
        # `search` builds its full result list either way -- pycottas's floor
        # for a count; the strings just go unread.
        return len(handle.search(query))


# ─── lightrdf ───────────────────────────────────────────────────────────────


class LightrdfAdapter(Adapter):
    """A streaming pattern-filtered parser, not a store.

    `RDFDocument.search_triples` re-parses the entire source file on every
    call -- there is no index and no build step, so this adapter is the
    no-index baseline: what every pattern costs when nothing is precomputed.

    supports_quads is False: `search_triples(s, p, o)` takes no graph argument.
    mutation_unsupported: there is nothing to mutate. Opening is distinct from
    building: it wraps the path and parses nothing.
    """

    slug, label = "lightrdf", "lightrdf"
    supports_quads = False
    mutation_unsupported = "a streaming parser, with no store to mutate"

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
        acc = rows = 0
        for s, p, o in handle.search_triples(pat.s, pat.p, pat.o):
            acc += len(s) + len(p) + len(o)
            rows += 1
        consume(acc)
        return rows

    def count_only(self, handle: Any, pat: Pat) -> int:
        return sum(1 for _ in handle.search_triples(pat.s, pat.p, pat.o))


# ─── rdflib ─────────────────────────────────────────────────────────────────


class RdflibAdapter(Adapter):
    """The reference pure-Python implementation: `Graph`/`Dataset` + `parse`.

    has_distinct_open is False for the same reason as pyoxigraph: the graph is
    in memory, so a fresh process re-parses the file.
    """

    slug, label = "rdflib", "rdflib"
    has_distinct_open = False

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

    def prepare(self, pat: Pat) -> tuple:
        return (rdflib_term(pat.s), rdflib_term(pat.p), rdflib_term(pat.o), rdflib_term(pat.g))

    def count(self, handle: Any, query: tuple) -> int:
        s, p, o, g = query
        # `Dataset.triples` searches the default graph only, and every row of
        # the shared dataset sits in a named one -- so an unbound graph has to
        # go through `quads` with a `None` graph, which spans all of them.
        from rdflib import Dataset

        acc = rows = 0
        if isinstance(handle, Dataset):
            for s2, p2, o2, g2 in handle.quads((s, p, o, g)):
                # rdflib terms subclass str; the graph position is a Graph
                # whose identifier is the name.
                gid = getattr(g2, "identifier", g2)
                acc += len(s2) + len(p2) + len(o2) + len(gid)
                rows += 1
        else:
            for s2, p2, o2 in handle.triples((s, p, o)):
                acc += len(s2) + len(p2) + len(o2)
                rows += 1
        consume(acc)
        return rows

    def count_only(self, handle: Any, query: tuple) -> int:
        s, p, o, g = query
        from rdflib import Dataset

        if isinstance(handle, Dataset):
            return sum(1 for _ in handle.quads((s, p, o, g)))
        return sum(1 for _ in handle.triples((s, p, o)))

    def _rdflib_triple(self, q: tuple):
        return (rdflib_term(q[0]), rdflib_term(q[1]), rdflib_term(q[2]))

    def add(self, handle: Any, quads: Iterable[tuple]) -> None:
        for q in quads:
            handle.add(self._rdflib_triple(q))

    def delete(self, handle: Any, quads: Iterable[tuple]) -> None:
        for q in quads:
            handle.remove(self._rdflib_triple(q))


# ─── Registry ───────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class VortexVariant:
    slug: str
    label: str
    layout: str
    indexes: tuple[str, ...]
    in_memory: bool


#: Vortex build variants: the same secondary index x residency matrix as the
#: Rust compare tab (core/benches/compare.rs), so the two tabs' Vortex rows line
#: up one for one. The JS tab has no file residency. Every variant is a row in
#: the cross-library panels -- Dictionary is the only layout here, because the
#: layout axis belongs to the instrumented suite and a variant the dashboard
#: does not list costs a whole process to measure and then discards the rows.
VORTEX_VARIANTS = [
    VortexVariant("vortex_dict", "Vortex Dict", "dictionary", (), False),
    VortexVariant("vortex_dict_mem", "Vortex Dict (in-memory)", "dictionary", (), True),
    VortexVariant("vortex_dict_bycopy", "Vortex Dict+ByCopy", "dictionary", ("secondary-by-copy",), False),
    VortexVariant(
        "vortex_dict_bycopy_mem", "Vortex Dict+ByCopy (in-memory)", "dictionary", ("secondary-by-copy",), True
    ),
    VortexVariant("vortex_dict_byref", "Vortex Dict+ByRef", "dictionary", ("secondary-by-reference",), False),
    VortexVariant(
        "vortex_dict_byref_mem", "Vortex Dict+ByRef (in-memory)", "dictionary", ("secondary-by-reference",), True
    ),
]

VORTEX_SLUGS = [v.slug for v in VORTEX_VARIANTS]


def build_adapter(slug: str) -> Adapter:
    """Construct one adapter by slug, importing only that library."""
    for v in VORTEX_VARIANTS:
        if slug == v.slug:
            return VortexAdapter(v.slug, v.label, v.layout, list(v.indexes), v.in_memory)
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
ALL_SLUGS = VORTEX_SLUGS + ["pyoxigraph", "pycottas", "rdflib", "lightrdf"]

#: The adapters the Arrow role runs for: the ones whose library hands a match
#: out as Arrow columns. Kept beside `ALL_SLUGS` so the orchestrator never has
#: to construct an adapter just to ask.
ARROW_SLUGS = VORTEX_SLUGS

#: Which virtualenv each adapter needs. Vortex variants share one.
VENV_FOR = {
    **{slug: "vortex" for slug in VORTEX_SLUGS},
    "pyoxigraph": "pyoxigraph",
    "pycottas": "pycottas",
    "rdflib": "rdflib",
    "lightrdf": "lightrdf",
}
