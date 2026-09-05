# Vortex-RDF for Python
[![PyPI](https://img.shields.io/pypi/v/vortex-rdf.svg)](https://pypi.org/project/vortex-rdf/)

Python bindings for [Vortex-RDF](https://github.com/vortex-rdf/vortex-rdf), a columnar RDF store format built on Vortex. Stores are opened lazily from `.vortex` files and queried in place, without loading the dataset into memory. The bindings are read-only (mutations are in the roadmap): build `.vortex` files with `serialize_rdf` (file → file), then open and query them; in-memory builds are not yet supported.

A separate [`vortex-rdflib`](https://pypi.org/project/vortex-rdflib/) package builds an rdflib integration on these bindings; see its own documentation.

## Install

```bash
pip install vortex-rdf
```

## Quick start

```python
from vortex_rdf import VortexRdfStore, serialize_rdf

serialize_rdf("data.nt", "data.vortex", layout="dictionary")   # RDF file -> .vortex file
store = VortexRdfStore("data.vortex")                            # lazy open; layout auto-detected
store.count_quads(p="<http://xmlns.com/foaf/0.1/name>")          # match count, no terms materialized
store.get_quads(p="<http://xmlns.com/foaf/0.1/name>")            # [(s, p, o, g), ...]
```

## Reading quads

Every read takes a pattern as the keyword arguments `s`, `p`, `o`, `g`; an omitted position is a wildcard. Terms cross the boundary as N-Triples strings (`<iri>`, `_:b0`, `"lit"@en`, `"3"^^<http://www.w3.org/2001/XMLSchema#integer>`); the graph of a quad in the default graph is the empty string, which is also how a pattern selects it. A malformed term raises `ValueError`; a failing store operation raises `VortexRdfError`.

```python
len(store)                                                   # number of quads
store.layout()                                               # "dictionary" | "default" | "typed-object"
store.indexes()                                              # e.g. ["secondary-by-reference"]
store.count_quads(p="<http://xmlns.com/foaf/0.1/name>")      # int
store.get_quads(p="<http://xmlns.com/foaf/0.1/name>")        # [(s, p, o, g), ...]
```

`get_quads` is the library-shaped read: whole quads as Python strings, served from the term-code columns whenever the store can (Dictionary layout, resident dictionary) and from the matched quads otherwise. On the code path a term that repeats down a column is one shared Python string, so a caller converting terms into its own representation can rely on the cached string it is handed. Everything a query planner or executor needs goes through the Arrow interface below.

## Arrow interface

Every matched view, code column and dictionary speaks the [Arrow PyCapsule interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html), so pyarrow, polars, DuckDB, DataFusion or any other Arrow-native engine consumes them directly — the package itself depends on none of them:

```python
import pyarrow as pa, polars as pl

stream = store.match_arrow(p="<http://xmlns.com/foaf/0.1/name>")   # ArrowQuadStream
pa.schema(stream)                                    # s, p, o, g — uint32 term codes
table = pa.RecordBatchReader.from_stream(stream).read_all()
frame = pl.DataFrame(store.match_arrow(encoding="terms"))    # strings backed by codes

pa.array(store.term_dict())                          # the dictionary: element i = term of code i
```

`match_arrow` streams one record batch per decode chunk (the whole store in memory, each scan split of a file), pulled as the consumer reads. `encoding="codes"` (the default) hands out `uint32` term codes sharing the store's buffers — join, filter and aggregate in code space, then decode the survivors through `term_dict()` or the dictionary array; `encoding="terms"` wraps the same codes as an Arrow dictionary over the whole term dictionary, so engines see strings while carrying codes; `encoding="strings"` materializes `string_view` N-Triples strings and is the one encoding every layout serves. `projection=["o", "s"]` restricts and orders the columns (a file scan then reads only those). `batch_rows=n` caps the rows per batch: a longer chunk arrives as consecutive slices of itself, sharing its buffers — the shape an engine pipelines on. A stream is consumed once; its schema can be read any number of times.

### Term codes

For Dictionary-layout stores, the `codes` encoding is the store's own vocabulary: `uint32` ranks into one sorted term dictionary, decodable through a `term_dict()` handle:

```python
codes = pa.RecordBatchReader.from_stream(store.match_arrow(p=p)).read_all()
dictionary = store.term_dict()                       # TermDict or None
dictionary.decode(codes["s"][0].as_py())             # N-Triples string for that code
dictionary.decode_many(codes["s"].combine_chunks())  # bulk-decode a whole column
dictionary.encode("<http://xmlns.com/foaf/0.1/name>")  # code for a term, or None
```

`decode_many` decodes a batch in one GIL-released call — point decodes, so decoding a few codes never decompresses the whole dictionary the way `pa.array(store.term_dict())` does. It takes a `uint32` Arrow array (anything with `__arrow_c_array__`), a buffer (`array("I", ...)`, a `uint32` NumPy array, a `U32Column`), or any sequence of ints, and a repeated code yields one shared Python string. `encode` is the inverse of `decode`. `term_dict()` returns `None` when the code path does not apply (a non-Dictionary layout, or a dictionary left file-backed by the residency budget); `match_arrow(encoding="codes")` raises on a layout without codes.

### Pushdown primitives

A query layer narrows a match inside the store rather than over gathered rows:

```python
store.match_arrow_many([(None, p, None, None), (s, None, None, None)])   # one call, one GIL release
store.count_quads_many([...])                                            # the counts, in order
store.count_quads(p=p, limit=1)                                          # an ASK: reads one row
store.match_arrow(p=p, limit=10, offset=20)                              # a window, in match order

lo, hi = dictionary.prefix_range("<http://ex.org/")                     # codes of an IRI namespace
store.match_arrow(p=p, keep={"s": range(lo, hi)})                        # rows whose subject is in it
holds, unknown = dictionary.filter_codes("num_gt", "40")                 # a FILTER, decided per term
store.match_arrow(p=p, keep={"o": holds})                                # rows it holds for
store.match_arrow(p=p, keep={"o": pa.array(holds)})                      # the same set, as an Arrow array
```

`keep` restricts a position by term code before any row is gathered — a `range` of codes (what `prefix_range` yields, or `lower_bound` bounds) or a set of codes in any form `decode_many` accepts — and composes with `encoding`, `projection`, `limit` and `offset`. `filter_codes(kind, arg)` evaluates one predicate over the whole dictionary once (memoized) and returns the codes it definitely holds for and the codes it cannot decide, as two zero-copy `U32Column`s (Arrow-exportable, `pa.array(holds)`), which the caller resolves itself; kinds are `is_literal`, `is_iri`, `is_blank`, `datatype`, `lang`, `lang_matches`, `str_prefix` and `num_lt`/`num_le`/`num_gt`/`num_ge`/`num_eq`/`num_ne`, under the rules a SPARQL engine over rdflib applies. `encode` resolves a term in its canonical N-Triples form when the given spelling misses (`"x"^^xsd:string` is `"x"`), and `encode_many` batches it.

## Build options

```python
serialize_rdf(input_path, output_path, *, format=None, layout="dictionary", indexes=[])
```

Every option after the two paths is keyword-only. `format` is an RDF format name (`"ntriples"`, `"nquads"`, `"turtle"`, `"trig"`, `"n3"`, `"rdfxml"`, `"jsonld"`, or the short aliases `nt`, `nq`, `ttl`, `rdf`, `xml`), detected from the input file extension when omitted. Opening auto-detects the layout and indexes — `VortexRdfStore` takes no layout argument; `store.layout()` and `store.indexes()` report the same names.

**`layout`** — how terms are encoded into columns. `"dictionary"` is the default in every vortex-rdf frontend (Python, JS and the CLI):

| Value | Notes |
| --- | --- |
| `"dictionary"` (default) | Terms replaced by codes into a sorted term dictionary. Most compact and fastest to query; backs the `codes` and `terms` encodings and `term_dict` |
| `"default"` | All four terms as N-Triples strings |
| `"typed-object"` | Object split into kind/value/datatype/language columns |

**`indexes`** — secondary access paths, each costing extra space:

| Value | Notes |
| --- | --- |
| `"secondary-by-reference"` | Sorted predicate/object columns plus row-id back-references, so predicate-only and object-only patterns use a binary search instead of a full scan |
| `"secondary-by-copy"` | Two complete extra copies of the quad columns — one sorted by `(p, o, s, g)`, one by `(o, s, p, g)` — giving predicate- and object-bound patterns (including predicate+object prefix lookups) the same sorted access path subjects have |

## Bytes & files

The default open is lazy and file-backed. `VortexRdfStore(path, in_memory=True)` loads the store into memory once, so each subsequent match skips the per-call file-scan pipeline. Such a store keeps the file's column encodings: a wide `match_arrow` read decodes a column into a canonical form that every result alive shares and that is freed with the last of them, so memory follows what you hold, not what you have read (see [docs/memory.md](../docs/memory.md)).

For Dictionary-layout files the term dictionary is lifted into memory when its compressed size in the file fits the residency budget — 512 MiB by default, overridable process-wide with `VORTEX_RDF_DICT_MAX_RESIDENT_BYTES`. `VortexRdfStore(path, max_resident_bytes=n)` sets the budget for that open (the environment variable is ignored for it). A dictionary left file-backed is point-read through its chunk leaves; `term_dict()` then returns `None` and `get_quads` falls back to the matched quads; `match_arrow` still exports the codes, only decoding them needs the resident dictionary.

Stores also round-trip through bytes: `store.to_bytes()` serializes to the native container (the same exchange format as the `.vortex` file, the CLI and the JS bindings), and `VortexRdfStore.from_bytes(data)` opens such a buffer — `bytes` or `bytearray` — as a fully in-memory store.

An in-memory store's term dictionary has two resident forms, picked by `dictionary=` on `VortexRdfStore(path, in_memory=True, dictionary=...)` and `from_bytes(data, dictionary=...)`: `"plaintext"` (the default) decodes the column once into one canonical form that every probe, decode and `terms` export then reads in place; `"as-written"` keeps the file's FSST chunks, a third of that size, and decodes one term per read — the lean load for a store opened, queried a few times and dropped. Its `u32` code columns have the same choice, `codes=`: `"canonical"` (the default) decodes them once, so every code read and every Arrow export is a slice of them with nothing to hold; `"as-written"` keeps the writer's encodings, a quarter of that size, and decodes a column into a form shared only while some result holds it. A store built from quads always holds both canonical.

## Development

Managed with [uv](https://docs.astral.sh/uv/); maturin runs under the hood as the build backend:

```bash
cd python
uv sync                      # creates .venv, builds + installs the extension
uv run pytest tests          # run the test suite
uv run maturin develop --uv  # fast rebuild while iterating on Rust code
```

Rust source changes are picked up by `uv sync` automatically (see `[tool.uv] cache-keys` in pyproject.toml). Without uv: `python -m venv .venv && pip install maturin pytest && maturin develop && pytest tests`.

Building from source (the sdist or a development build) additionally requires **libclang**: a transitive build dependency of the Vortex file engine (`custom-labels`, via `vortex-io`) generates C bindings with `bindgen` at compile time. It is preinstalled on most dev setups (Xcode, LLVM on Windows); on Linux install e.g. `clang-devel` (dnf) or `libclang-dev` (apt). Installing a published wheel needs none of this.

### Benchmarks

`bench/run.py` measures these bindings against [pyoxigraph](https://pypi.org/project/pyoxigraph/), [pycottas](https://pypi.org/project/pycottas/), [rdflib](https://pypi.org/project/rdflib/) and [lightrdf](https://pypi.org/project/lightrdf/) on a file → store → query workload and writes `bench/results.json` for [the dashboard's Python tab](https://vortex-rdf.github.io/vortex-rdf/#py); `bench/test_codspeed.py` is the instrumented suite CodSpeed runs.

```bash
python3 python/bench/run.py                 # full run
BENCH_DIM=32 python3 python/bench/run.py    # quick pilot
uv run pytest bench/test_codspeed.py --codspeed
```

Harness design (per-library virtualenvs, dataset parity with `js/bench/datasets.ts`, `unsupported` cells where a library lacks the operation, matched-row counts cross-checked and any disagreement recorded in `config.countWarnings`) is documented in `bench/run.py`, `bench/worker.py` and `bench/adapters.py`. Configuration variables:

| Var | Default | Meaning |
| --- | --- | --- |
| `BENCH_SIZE` | 1,048,576 | rows (value shared with the Rust and JS suites) |
| `BENCH_DIM` | unset | optional cube shorthand, `D³` rows; ignored if `BENCH_SIZE` is set |
| `BENCH_GRAPHS_QUADS` | 8 | named graphs the comparative bench asks for |
| `MUT_BATCH` | 10000 | quads per add/delete batch |
| `BENCH_PYTHON` | 3.13 | Python version the per-library virtualenvs are provisioned with |
| `BENCH_SUBJ_RATIO` / `BENCH_OBJ_RATIO` | 0.1 / 0.5 | distinct subjects / objects per row |
| `BENCH_PREDICATES` | 32 | distinct predicates |
| `BENCH_GRAPHS` | 1 | distinct named graphs in the generator; 1 means default graph only |
| `BENCH_LITERAL_FRAC` | 0.4 | fraction of objects that are literals |
| `BENCH_SLOW_PHASE_MS` | 30000 | a phase slower than this runs once, without warmup |
| `PY_BENCH_QUERY_ITERS` / `PY_BENCH_QUERY_WARMUP` | 10 / 5 | measured / warmup iterations per query |
| `PY_BENCH_HEAVY_ITERS` / `PY_BENCH_FULL_SCAN_ITERS` | 3 / 3 | iterations for the heavy and full-scan phases |
| `CODSPEED_BENCH_DIM` / `CODSPEED_BENCH_DIM_QUADS` | 32 / 13 | CodSpeed suite: `D³` triples / `D⁴` quads |

## License

MIT
