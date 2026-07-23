from pathlib import Path
from typing import Optional
import os

from rdflib.term import BNode, Node, URIRef
from rdflib.store import Store, NO_STORE, VALID_STORE
from rdflib.util import from_n3

from .vortex_rdf_native import (
    match_triples, match_triples_compact, count_triples, diagnose_match, diagnose_direct_compact
)


def _term_debug(t):
    if t is None:
        return "None"
    try:
        return t.n3()
    except Exception:
        return repr(t)


class VortexStore(Store):
    context_aware = False
    formula_aware = False
    transaction_aware = False
    graph_aware = False

    def __init__(
        self,
        configuration=None,
        identifier=None,
        path: Optional[str] = None,
        layout: str = "cottas-native-strings",
        backend: str = "native",
        **kwargs,
    ):
        # IMPORTANT:
        # RDFLib Store.__init__ may call self.open(configuration).
        # Therefore all attributes used by open() must exist BEFORE super().__init__.
        if path is None:
            path = configuration

        self.path = str(Path(path)) if path is not None else None
        self.layout = layout
        self.backend = backend
        self._backend = None

        # Do not pass configuration here, otherwise RDFLib calls open()
        # before our initialization logic is fully under control.
        super().__init__(configuration=None, identifier=identifier)

        # Explicitly open after initialization.
        if self.path is not None:
            self.open(self.path)

    def open(self, configuration, create=False):
        """
        RDFLib calls open() for some stores.
        For this read-only Vortex store, configuration is the Vortex file path.
        """
        if configuration is not None:
            self.path = str(Path(configuration))

        if self.path is None:
            return NO_STORE

        if self.backend == "duckdb":
            if self.layout in {"cottas-native-ids", "cottas-native"}:
                raise ValueError(
                    "DuckDB backend does not currently support cottas-native-ids. "
                    "Use backend='native' for native-ID files."
                )

            from .duckdb_backend import DuckDBVortexBackend

            self._backend = DuckDBVortexBackend(self.path)

        return VALID_STORE

    def close(self, commit_pending_transaction=False):
        if self._backend is not None:
            self._backend.close()
            self._backend = None

    def triples(self, triple_pattern, context=None):
        """
        RDFLib Store API.

        Input:
            triple_pattern = (subject, predicate, object)

        Output:
            yields ((s, p, o), context)
        """
        if self.path is None:
            return

        s, p, o = triple_pattern

        # RDFLib can propagate an object binding into subject or predicate
        # position during joins. That pattern is unsatisfiable, not an error.
        if s is not None and not isinstance(s, (URIRef, BNode)):
            return
        if p is not None and not isinstance(p, URIRef):
            return

        trace = os.environ.get("VORTEX_RDF_TRACE_TRIPLES") == "1"
        n = 0

        if self.backend == "duckdb":
            if self._backend is None:
                from .duckdb_backend import DuckDBVortexBackend

                self._backend = DuckDBVortexBackend(self.path)

            for triple in self._backend.triples(s, p, o):
                yield triple, None
            return

        if self.backend != "native":
            raise ValueError(f"Unsupported Vortex backend: {self.backend}")

        s_n3 = self._node_to_n3(s)
        p_n3 = self._node_to_n3(p)
        o_n3 = self._node_to_n3(o)

        if trace:
            print(
                "[VortexStore.triples:start] "
                f"layout={getattr(self, 'layout', None)} "
                f"backend={type(self.backend).__name__} "
                f"s={_term_debug(s)} "
                f"p={_term_debug(p)} "
                f"o={_term_debug(o)}",
                f"s_n3={s_n3} ",
                f"p_n3={p_n3} ",
                f"o_n3={o_n3} ",
                flush=True,
            )

        if self.layout in {"cottas-native-ids", "cottas-native"}:
            lexical_terms, indexed_rows = match_triples_compact(
                self.path, s_n3, p_n3, o_n3, self.layout,
            )
            # Parse every distinct RDF lexical term once per query, including bounds.
            parsed_terms = [self._from_n3_safe(value) for value in lexical_terms]
            term_count = len(parsed_terms)
            for s_idx, p_idx, o_idx in indexed_rows:
                if s_idx >= term_count or p_idx >= term_count or o_idx >= term_count:
                    raise ValueError(
                        "Compact native result has an invalid term index: "
                        f"{(s_idx, p_idx, o_idx)!r}, terms={term_count}"
                    )
                yield (
                    parsed_terms[s_idx], parsed_terms[p_idx], parsed_terms[o_idx]
                ), None
            return

        triples_out = match_triples(
            self.path, s_n3, p_n3, o_n3, self.layout,
        )
        for ss, pp, oo in triples_out:
            yield (
                self._from_n3_safe(ss),
                self._from_n3_safe(pp),
                self._from_n3_safe(oo),
            ), None

    def diagnose_triples(self, triple_pattern):
        """Return timings plus raw/unique rows for one native-ID triple pattern."""
        if self.path is None:
            raise ValueError("Store has no path")
        s, p, o = triple_pattern
        started_ns = __import__("time").perf_counter_ns()
        result = dict(diagnose_match(
            self.path,
            self._node_to_n3(s),
            self._node_to_n3(p),
            self._node_to_n3(o),
            self.layout,
        ))
        returned_ns = __import__("time").perf_counter_ns()

        raw_rows = [tuple(row) for row in result.pop("legacy_rows_data")]
        unique_rows = set(raw_rows)
        result["python_call_ms"] = (returned_ns - started_ns) / 1_000_000
        result["python_wrapper_ms"] = max(
            0.0, result["python_call_ms"] - result["legacy_native_ms"]
                 - result["optimized_binding_ms"]
        )
        result["legacy_unique_rows"] = len(unique_rows)
        result["legacy_duplicate_rows"] = len(raw_rows) - len(unique_rows)
        result["legacy_sample"] = raw_rows[:10]
        return result

    def __len__(self, context=None):
        if self.path is None:
            return 0

        if self.backend == "duckdb":
            if self._backend is None:
                from .duckdb_backend import DuckDBVortexBackend

                self._backend = DuckDBVortexBackend(self.path)

            return len(self._backend)

        if self.backend != "native":
            raise ValueError(f"Unsupported Vortex backend: {self.backend}")

        return count_triples(self.path, self.layout)

    def add(self, triple, context=None, quoted=False):
        raise NotImplementedError("VortexStore is read-only")

    def addN(self, quads):
        raise NotImplementedError("VortexStore is read-only")

    def remove(self, triple_pattern, context=None):
        raise NotImplementedError("VortexStore is read-only")

    def bind(self, prefix, namespace, override=True):
        return None

    def namespace(self, prefix):
        return None

    def namespaces(self):
        return iter(())

    def prefix(self, namespace):
        return None

    @staticmethod
    def _node_to_n3(node: Optional[Node]) -> Optional[str]:
        if node is None:
            return None
        return node.n3()

    @staticmethod
    def _from_n3_safe(value: str) -> Node:
        try:
            return from_n3(value)
        except Exception as e:
            raise ValueError(
                f"Could not parse returned RDF term as N3: {value!r}"
            ) from e
