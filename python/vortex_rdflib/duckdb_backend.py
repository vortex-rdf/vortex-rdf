from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Optional, Tuple

import duckdb
from rdflib import URIRef, BNode, Literal
from rdflib.term import Node


Triple = Tuple[Node, Node, Node]


@dataclass
class DuckDBVortexBackend:
    vortex_path: str
    table_name: str = "triples"

    def __post_init__(self):
        self.vortex_path = str(Path(self.vortex_path))
        self.con = duckdb.connect(database=":memory:")

        # Preferred route: core extension
        try:
            self.con.execute("INSTALL vortex")
            self.con.execute("LOAD vortex")
        except Exception:
            # Optional fallback if you install duckdb-extension-vortex
            try:
                from duckdb_extensions import import_extension
                import_extension("vortex")
                self.con.execute("LOAD vortex")
            except Exception as e:
                raise RuntimeError(
                    "Could not load DuckDB vortex extension. Try: "
                    "`pip install duckdb-extension-vortex duckdb-extensions` "
                    "or ensure DuckDB can INSTALL/LOAD vortex."
                ) from e

        # Register as view so every triples() call is simple.
        self.con.execute(
            f"""
            CREATE VIEW {self.table_name} AS
            SELECT *
            FROM read_vortex(?)
            """,
            [self.vortex_path],
        )

    def triples(
        self,
        s: Optional[Node],
        p: Optional[Node],
        o: Optional[Node],
    ) -> Iterator[Triple]:
        where = []
        params = []

        if s is not None:
            where.append("s = ?")
            params.append(self._to_storage_value(s))

        if p is not None:
            where.append("p = ?")
            params.append(self._to_storage_value(p))

        if o is not None:
            where.append("o = ?")
            params.append(self._to_storage_value(o))

        sql = f"SELECT s, p, o FROM {self.table_name}"
        if where:
            sql += " WHERE " + " AND ".join(where)

        for ss, pp, oo in self.con.execute(sql, params).fetchall():
            yield (
                self._from_storage_value(ss),
                self._from_storage_value(pp),
                self._from_storage_value(oo),
            )

    def __len__(self) -> int:
        return self.con.execute(
            f"SELECT COUNT(*) FROM {self.table_name}"
        ).fetchone()[0]

    def close(self):
        self.con.close()

    def _to_storage_value(self, node: Node):
        # Adjust this once I see your exact Vortex RDF schema.
        return str(node)

    def _from_storage_value(self, value):
        # Temporary conservative behavior.
        # If your stored strings are N-Triples terms, we should parse them properly.
        v = str(value)

        if v.startswith("_:"):
            return BNode(v[2:])

        if v.startswith("http://") or v.startswith("https://"):
            return URIRef(v)

        # If predicates are always URI strings, p will become URIRef through above.
        # Literals need exact schema-aware handling.
        return Literal(v)
