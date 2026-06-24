# inspect_vortex_duckdb.py
import sys
import duckdb

path = sys.argv[1]

con = duckdb.connect(":memory:")

try:
    con.execute("INSTALL vortex")
    con.execute("LOAD vortex")
except Exception:
    from duckdb_extensions import import_extension
    import_extension("vortex")
    con.execute("LOAD vortex")

print("DESCRIBE:")
print(con.execute("DESCRIBE SELECT * FROM read_vortex(?)", [path]).fetchdf())

print("\nSAMPLE:")
print(con.execute("SELECT * FROM read_vortex(?) LIMIT 5", [path]).fetchdf())