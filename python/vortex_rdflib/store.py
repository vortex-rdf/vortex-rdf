from pathlib import Path

from rdflib.store import Store, NO_STORE, VALID_STORE
from rdflib.util import from_n3

from .vortex_rdf_native import match_triples, count_triples


def _sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


class VortexStore(Store):
    context_aware = False
    formula_aware = False
    transaction_aware = False
    graph_aware = False

    def __init__(
        self,
        vortex_file=None,
        *,
        layout="cottas-native-strings",
        backend="native",
        configuration=None,
        identifier=None,
    ):
        super().__init__(configuration=configuration, identifier=identifier)
        self.vortex_file = str(vortex_file) if vortex_file is not None else None
        self.layout = layout
        self.backend = backend
        self._num_triples = None
        self._is_quad_table = None
        self._duckdb_ready = False

    def open(self, configuration, create=False):
        if configuration is not None:
            self.vortex_file = str(configuration)

        if self.vortex_file is None or not Path(self.vortex_file).exists():
            return NO_STORE

        if self.backend == "native":
            self._num_triples = count_triples(self.vortex_file, self.layout)
            return VALID_STORE

        if self.backend == "duckdb":
            self._init_duckdb_vortex_repro()
            return VALID_STORE

        raise ValueError(f"Unknown VortexStore backend: {self.backend}")

    def _init_duckdb_vortex_repro(self):
        """
        COTTAS-reproducible DuckDB/Vortex setup.

        Mirrors COTTASStore.__init__ as closely as possible:
        - uses module-level duckdb.query/execute
        - sets progress bar off
        - touches the file with SELECT * FROM ...
        - computes COUNT(*)
        - checks whether column g exists

        Does NOT CREATE TEMP TABLE.
        Does NOT CREATE INDEX.
        Does NOT materialize the file into DuckDB memory.
        """
        if self._duckdb_ready:
            return

        import duckdb

        path_sql = _sql_quote(self.vortex_file)

        try:
            duckdb.execute("LOAD vortex")
        except Exception:
            try:
                duckdb.execute("INSTALL vortex")
                duckdb.execute("LOAD vortex")
            except Exception:
                try:
                    from duckdb_extensions import import_extension

                    import_extension("vortex")
                    duckdb.execute("LOAD vortex")
                except Exception as e:
                    raise RuntimeError(
                        "Could not load DuckDB vortex extension. Try:\n"
                        "  pip install 'duckdb>=1.4.2'\n"
                        "or:\n"
                        "  pip install duckdb-extensions duckdb-extension-vortex"
                    ) from e

        # COTTAS does:
        # duckdb.query("SET parquet_metadata_cache=true; SET enable_progress_bar=false; SELECT * FROM PARQUET_SCAN(...)")
        #
        # For Vortex, parquet_metadata_cache is irrelevant, but harmless if accepted.
        try:
            duckdb.query(
                f"""
                SET parquet_metadata_cache=true;
                SET enable_progress_bar=false;
                SELECT * FROM read_vortex({path_sql})
                """
            )
        except Exception:
            # Some DuckDB builds may reject parquet-specific setting with Vortex.
            duckdb.query(
                f"""
                SET enable_progress_bar=false;
                SELECT * FROM read_vortex({path_sql})
                """
            )

        self._num_triples = duckdb.execute(
            f"SELECT COUNT(*) FROM read_vortex({path_sql})"
        ).fetchone()[0]

        columns = [
            row[0]
            for row in duckdb.execute(
                f"DESCRIBE SELECT * FROM read_vortex({path_sql}) LIMIT 1"
            ).fetchall()
        ]
        self._is_quad_table = "g" in columns

        self._duckdb_ready = True

    def __len__(self, context=None):
        if self.backend == "native":
            if self._num_triples is None:
                self._num_triples = count_triples(self.vortex_file, self.layout)
            return self._num_triples

        if self.backend == "duckdb":
            self._init_duckdb_vortex_repro()
            return self._num_triples

        raise ValueError(f"Unknown VortexStore backend: {self.backend}")

    def triples(self, pattern, context=None):
        if self.backend == "native":
            yield from self._triples_native(pattern, context)
            return

        if self.backend == "duckdb":
            yield from self._triples_duckdb_repro(pattern, context)
            return

        raise ValueError(f"Unknown VortexStore backend: {self.backend}")

    def _triples_native(self, pattern, context=None):
        s, p, o = pattern

        rows = match_triples(
            self.vortex_file,
            s.n3() if s is not None else None,
            p.n3() if p is not None else None,
            o.n3() if o is not None else None,
            self.layout,
        )

        for ss, pp, oo in rows:
            yield (from_n3(ss), from_n3(pp), from_n3(oo)), None

    def _triples_duckdb_repro(self, pattern, context=None):
        """
        Mirrors COTTASStore.triples:
        - build SQL per triple pattern
        - execute directly over file scan
        - fetchall()
        - from_n3 conversion
        """
        self._init_duckdb_vortex_repro()

        import duckdb

        sql = self._translate_vortex_triple_pattern_tuple(pattern)

        for ss, pp, oo in duckdb.execute(sql).fetchall():
            yield (from_n3(ss), from_n3(pp), from_n3(oo)), None

    def _translate_vortex_triple_pattern_tuple(self, pattern):
        if len(pattern) != 3:
            raise TypeError("The pattern must be a tuple of length 3.")

        path_sql = _sql_quote(self.vortex_file)
        query = f"SELECT s, p, o FROM read_vortex({path_sql}) WHERE "

        names = ["s", "p", "o"]

        for i, term in enumerate(pattern):
            if term is not None:
                query += f"{names[i]}={_sql_quote(term.n3())} AND "

        if query.endswith("AND "):
            query = query[:-4]

        if query.endswith("WHERE "):
            query = query[:-6]

        return query

    def close(self, commit_pending_transaction=False):
        # COTTASStore does not close the module-level DuckDB connection.
        # Keep this minimal for behavioral similarity.
        self._duckdb_ready = False

    def add(self, triple, context=None, quoted=False):
        raise TypeError("VortexStore is read-only")

    def addN(self, quads):
        raise TypeError("VortexStore is read-only")

    def remove(self, triple, context=None):
        raise TypeError("VortexStore is read-only")

    def create(self, configuration):
        return self.open(configuration, create=True)

    def destroy(self, configuration):
        pass

    def commit(self):
        pass

    def rollback(self):
        pass