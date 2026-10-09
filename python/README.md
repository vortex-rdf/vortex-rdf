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
store.match_columns(p="<http://xmlns.com/foaf/0.1/name>")    # (subjects, predicates, objects, graphs)
```

`get_quads` returns whole quads; `match_columns` returns the same rows transposed into four parallel columns, for callers that work a position at a time. Both are served from the term-code columns whenever the store can (Dictionary layout, resident dictionary) and from the matched quads otherwise; results are identical. On the code path a term that repeats down a column is one shared Python string, so a caller converting terms into its own representation can rely on the cached string it is handed.

## Term codes (low-level)

For Dictionary-layout stores, `match_codes` returns the matched rows as four **zero-copy** `u32` term-code columns — `memoryview(col).cast("I")` views the Rust memory directly — decodable through a `term_dict()` handle:

```python
cols = store.match_codes(p="<http://xmlns.com/foaf/0.1/name>")  # (s, p, o, g) or None
dictionary = store.term_dict()                                    # TermDict or None
subjects = memoryview(cols[0]).cast("I")
dictionary.decode(subjects[0])                       # N-Triples string for that code
dictionary.decode_many(cols[0])                      # bulk-decode a whole column
dictionary.encode("<http://xmlns.com/foaf/0.1/name>")  # code for a term, or None
```

`decode_many` decodes a batch in one GIL-released call. Buffer-protocol inputs — a column straight from `match_codes`, an `array("I", ...)`, a `uint32` NumPy array — are read in a single bulk copy with no per-element int conversion; any sequence of ints works too. `encode` is the inverse of `decode` and tolerant of spelling: an IRI with or without angle brackets, a literal with an explicit `xsd:string` type or an upper-case language tag, and the default graph as `""`, `default` or `[]` all resolve to the stored form's code (a malformed term raises `ValueError`); `encode_many` does a batch. Both `term_dict()` and `match_codes` return `None` when the code path does not apply (a non-Dictionary layout, or an append tail). A dictionary left in the file by the residency budget is served by reading it on demand — `TermDict.file_backed` says so — with the same calls.

Consumers can join, count, and de-duplicate entirely in code space and decode each distinct term once, never materializing a term string for a row they discard. The handle and the columns carry the pieces a query layer pushes below a pattern:

```python
d = store.term_dict()
lo, hi = d.prefix_range("<http://xmlns.com/foaf/0.1/")   # codes of one namespace: one range
literals = d.prefix_range('"')                            # codes of every literal
true_codes, unknown = d.filter_codes("num_lt", "42")      # codes a term predicate holds for
true_codes, unknown = d.filter_codes("lang_matches", "en")

# Narrow inside the store, before a row is gathered: a code set or range per position.
cols = store.match_codes(p=NAME, keep={"o": range(lo, hi)})
cols = store.match_codes(keep={"s": true_codes, "g": [0]}, limit=100, offset=20)
n = store.count_quads(p=NAME, keep={"o": true_codes}, limit=1)   # an existence test

# Many probes in one GIL-released call, answered in input order.
views = store.match_codes_many([(None, NAME, None, None), {"s": "<http://ex.org/bob>", "limit": 5}])
counts = store.count_quads_many([{"p": NAME, "keep": {"o": literals}}])

# Column kernels, order-preserving: first-seen distinct values, nested-loop joins.
s, p, o, g = cols
s.distinct(); o.value_counts()
left_idx, right_idx = o.join_indices(s)     # rows where o == s, as index pairs
o.take(left_idx)                            # gather a joined column
```

`filter_codes(kind, arg)` answers `(true_codes, unknown_codes)`: the codes for which the predicate definitely holds, and the codes inside its domain the native layer leaves to the caller's own evaluator (an ill-typed number, a datatype it does not order). Codes outside the domain — non-literals, for the literal predicates — appear in neither. Kinds: `is_literal`, `is_iri`, `is_blank`, `datatype <iri>`, `lang <tag>`, `lang_matches <range>`, `str_prefix <p>` (`strstarts(str(?v), p)`: a string-like literal's lexical form, an IRI, a blank node's label), and `num_lt`/`num_le`/`num_gt`/`num_ge`/`num_eq`/`num_ne <number>` (value comparison for well-formed numeric literals; different XSD datatypes order by their IRIs; a non-literal is `False` under `=` and the orderings and `True` under `!=`). `keep` takes a dict from position (`"s"`, `"p"`, `"o"`, `"g"` or 0–3) to a code set (`U32Column`, u32 buffer or int sequence) or a code range (`range` with step 1, or `(lo, hi)`); `limit`/`offset` window the rows in base order, and a filtered file scan stops at the first block that fills the window. A probe of the `*_many` calls is an `(s, p, o, g)` tuple or a dict with keys `s`, `p`, `o`, `g`, `keep`, `limit`, `offset`; every probe is parsed before any is evaluated.

## Build options

```python
serialize_rdf(input_path, output_path, *, format=None, layout="dictionary", indexes=[])
```

Every option after the two paths is keyword-only. `format` is an RDF format name (`"ntriples"`, `"nquads"`, `"turtle"`, `"trig"`, `"n3"`, `"rdfxml"`, `"jsonld"`, or the short aliases `nt`, `nq`, `ttl`, `rdf`, `xml`), detected from the input file extension when omitted. Opening auto-detects the layout and indexes — `VortexRdfStore` takes no layout argument; `store.layout()` and `store.indexes()` report the same names.

**`layout`** — how terms are encoded into columns. `"dictionary"` is the default in every vortex-rdf frontend (Python, JS and the CLI):

| Value | Notes |
| --- | --- |
| `"dictionary"` (default) | Terms replaced by codes into a sorted term dictionary. Most compact and fastest to query; backs `match_codes`/`term_dict` |
| `"default"` | All four terms as N-Triples strings |
| `"typed-object"` | Object split into kind/value/datatype/language columns |

**`indexes`** — secondary access paths, each costing extra space:

| Value | Notes |
| --- | --- |
| `"secondary-by-reference"` | Sorted predicate/object columns plus row-id back-references, so predicate-only and object-only patterns use a binary search instead of a full scan |
| `"secondary-by-copy"` | Two complete extra copies of the quad columns — one sorted by `(p, o, s, g)`, one by `(o, s, p, g)` — giving predicate- and object-bound patterns (including predicate+object prefix lookups) the same sorted access path subjects have |

## Bytes & files

The default open is lazy and file-backed. `VortexRdfStore(path, in_memory=True)` loads the store into memory once, so each subsequent match skips the per-call file-scan pipeline.

For Dictionary-layout files the term dictionary is lifted into memory when its compressed size in the file fits the residency budget — 512 MiB by default, overridable process-wide with `VORTEX_RDF_DICT_MAX_RESIDENT_BYTES`. `VortexRdfStore(path, max_resident_bytes=n)` sets the budget for that open (the environment variable is ignored for it). A dictionary left file-backed is point-read through its chunk leaves: the string reads resolve each chunk's distinct codes with one dictionary scan, and `term_dict()` hands out a handle that reads the file on demand (`TermDict.file_backed`), so `match_codes` and the code path keep working.

Stores also round-trip through bytes: `store.to_bytes()` serializes to the native container (the same exchange format as the `.vortex` file, the CLI and the JS bindings), and `VortexRdfStore.from_bytes(data)` opens such a buffer — `bytes` or `bytearray` — as a fully in-memory store.

## Development

Managed with [uv](https://docs.astral.sh/uv/); maturin runs under the hood as the build backend:

```bash
cd python
uv sync                      # creates .venv, builds + installs the extension
uv run pytest tests          # run the test suite
uv run maturin develop --uv  # fast rebuild while iterating on Rust code
```

Rust source changes are picked up by `uv sync` automatically (see `[tool.uv] cache-keys` in pyproject.toml). Without uv: `python -m venv .venv && pip install maturin pytest "rdflib>=7.6,<8" && maturin develop && pytest tests` (rdflib is the oracle of `tests/test_filter_codes_differential.py`).

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
