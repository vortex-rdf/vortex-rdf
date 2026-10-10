# Vortex-RDF for Python
[![PyPI](https://img.shields.io/pypi/v/vortex-rdf.svg)](https://pypi.org/project/vortex-rdf/)

Python bindings for [Vortex-RDF](https://github.com/vortex-rdf/vortex-rdf), a columnar RDF store format built on Vortex. Stores are opened memory-mapped from `.vortex` files and queried in place, without loading the dataset into memory. The bindings are read-only, with no in-memory build: build `.vortex` files with `serialize_rdf` (file → file), then open and query them.

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

Every read takes a pattern as the keyword arguments `s`, `p`, `o`, `g`; an omitted position is a wildcard. Terms cross the boundary as N-Triples strings (`<iri>`, `_:b0`, `"lit"@en`, `"3"^^<http://www.w3.org/2001/XMLSchema#integer>`); the graph of a quad in the default graph is the empty string, which is also how a pattern selects it. A malformed term raises `ValueError`; a failing store operation raises `VortexRdfError`. That includes opening a store written by vortex-rdf 0.11 or earlier: its root layout is refused, and the error says to rebuild it from its RDF source with `serialize_rdf`.

```python
len(store)                                                   # number of quads
store.layout()                                               # "dictionary" | "default" | "typed-object"
store.indexes()                                              # e.g. ["secondary-by-reference"]
store.count_quads(p="<http://xmlns.com/foaf/0.1/name>")      # int
store.get_quads(p="<http://xmlns.com/foaf/0.1/name>")        # [(s, p, o, g), ...]
store.match_columns(p="<http://xmlns.com/foaf/0.1/name>")    # (subjects, predicates, objects, graphs)
```

`get_quads` returns whole quads; `match_columns` returns the same rows transposed into four parallel columns, for callers that work a position at a time. Both are served from the term-code columns whenever the store can (Dictionary layout) and from the matched quads otherwise; results are identical. On the code path a term that repeats down a column is one shared Python string, so a caller converting terms into its own representation can rely on the cached string it is handed.

## Term codes (low-level)

For Dictionary-layout stores, `match_codes` returns the matched rows as four **zero-copy** `u64` term-code columns (`U64Column`) — `memoryview(col).cast("Q")` views the Rust memory directly — decodable through a `term_dict()` handle. A dictionary holds at most 2**31 - 1 terms; a code is a Python int from 0 to 2**64 - 1, and anything else is refused (`OverflowError` or `ValueError`), never narrowed:

```python
cols = store.match_codes(p="<http://xmlns.com/foaf/0.1/name>")  # (s, p, o, g) or None
dictionary = store.term_dict()                                    # TermDict or None
subjects = memoryview(cols[0]).cast("Q")
dictionary.decode(subjects[0])                       # N-Triples string for that code
dictionary.decode_many(cols[0])                      # bulk-decode a whole column
dictionary.encode("<http://xmlns.com/foaf/0.1/name>")  # code for a term, or None
```

`decode_many` decodes a batch in one GIL-released call. Buffer-protocol inputs — a column straight from `match_codes`, `memoryview(col).cast("Q")`, an `array("Q", ...)`, a `uint64` or non-negative `int64` NumPy array — are read in a single bulk copy with no per-element int conversion; a buffer of any other items (a `cast("I")` view, a `uint32` array) raises `ValueError` rather than being read element by element. Any sequence of ints works too. `encode` is the inverse of `decode` and tolerant of spelling: an IRI with or without angle brackets, a literal with an explicit `xsd:string` type or an upper-case language tag, and the default graph as `""`, `default` or `[]` all resolve to the stored form's code (a malformed term raises `ValueError`); `encode_many` does a batch. Both `term_dict()` and `match_codes` return `None` when the code path does not apply (a non-Dictionary layout, or an append tail). A file store's dictionary is read from the mapped file on demand — `TermDict.file_backed` says so — with the same calls.

Consumers can join, count, and de-duplicate entirely in code space and decode each distinct term once, never materializing a term string for a row they discard. The handle and the columns carry the pieces a query layer pushes below a pattern:

```python
NAME = "<http://xmlns.com/foaf/0.1/name>"
d = store.term_dict()
lo, hi = d.prefix_range("<http://xmlns.com/foaf/0.1/")   # codes of one namespace: one range
literals = d.prefix_range('"')                            # codes of every literal

# A FILTER over the candidates a query produced: sorted, unique codes.
cols = store.match_codes(p=NAME)
codes = sorted(memoryview(cols[2].distinct()).cast("Q"))
passed, undecided = d.filter_codes("num_lt", "42", codes)
passed, undecided = d.filter_codes("regex", "^ali", codes, flags="i")
passed, undecided = d.filter_codes("contains", '"bob"', codes, case="lower", as_str=True)

# Narrow inside the store, before a row is gathered: a code set or range per position.
cols = store.match_codes(p=NAME, keep={"o": range(lo, hi)})
cols = store.match_codes(p=NAME, keep={"o": passed, "g": [0]}, limit=100, offset=20)
n = store.count_quads(p=NAME, keep={"o": passed}, limit=1)   # an existence test

# Many probes in one GIL-released call, answered in input order.
views = store.match_codes_many([(None, NAME, None, None), {"s": "<http://ex.org/bob>", "limit": 5}])
counts = store.count_quads_many([{"p": NAME, "keep": {"o": literals}}])

# Column kernels, order-preserving: first-seen distinct values, nested-loop joins.
s, p, o, g = cols
s.distinct(); o.value_counts()
left_idx, right_idx = o.join_indices(s)     # rows where o == s, as index pairs
o.take(left_idx)                            # gather a joined column
```

`filter_codes(kind, arg, codes, *, flags="", case=None, as_str=False)` evaluates a FILTER predicate over the candidate `codes` (sorted and unique, else `ValueError`) and answers `(passed, undecided)`, both subsets of `codes`; a candidate in neither fails, and the undecided ones are left to the caller's own evaluator. Nothing is memoized. Kinds: `is_literal`, `is_iri`, `is_blank`; `datatype <iri>`, `lang <tag>`, `lang_matches <range>`; `num_lt`/`num_le`/`num_gt`/`num_ge`/`num_eq`/`num_ne <number>` (value comparison for well-formed numeric literals; `xsd:long`/`xsd:unsignedLong` beyond 64 bits undecided; different XSD datatypes order by their IRIs; a non-literal fails the orderings and `=`, passes `!=`); and the string kinds `str_prefix <p>`, `contains`/`strstarts`/`strends <constant's N-Triples spelling>`, `regex <pattern>` with `flags`. String kinds follow rdflib 7.6: the text is a string literal's lexical form (with `as_str=True`, SPARQL `STR()`: an IRI's string or a literal's lexical form; a blank node, or a literal whose datatype rdflib normalizes, is undecided), `case="lower"|"upper"` wraps that text (Python's `str.lower()`/`str.upper()`, decided on ASCII text only), a language-tagged constant needs the same tag on the text, and `regex` decides only patterns from an allow-listed subset that agrees with Python's `re`; the stub's docstring has the exact rules.

`keep` takes a dict from position (`"s"`, `"p"`, `"o"`, `"g"` or 0–3) to a code set (`U64Column`, u64 buffer or int sequence) or a code range (`range` with step 1, or `(lo, hi)`); `limit`/`offset` window the rows in base order, and a filtered file scan stops at the first block that fills the window. A probe of the `*_many` calls is an `(s, p, o, g)` tuple or a dict with keys `s`, `p`, `o`, `g`, `keep`, `limit`, `offset`; every probe is parsed before any is evaluated.

## Build options

```python
serialize_rdf(input_path, output_path, *, format=None, layout="dictionary", indexes=[])
```

`serialize_rdf` writes beside `output_path` and renames the finished file into place, so a build that fails (a parse error halfway through the input, say) leaves no partial file and an existing store untouched. A store file the process cannot write (a read-only file) is never replaced: `serialize_rdf` raises `PermissionError`. On Linux and macOS rebuilding a path that an open `VortexRdfStore` has mapped is safe: the open store keeps reading the file it mapped. On Windows the rename is refused while a store has the file mapped, so the rebuild fails with an `OSError` until that store is dropped. Every option after the two paths is keyword-only. `format` is an RDF format name (`"ntriples"`, `"nquads"`, `"turtle"`, `"trig"`, `"n3"`, `"rdfxml"`, `"jsonld"`, or the short aliases `nt`, `nq`, `ttl`, `rdf`, `xml`), detected from the input file extension when omitted. Opening auto-detects the layout and indexes — `VortexRdfStore` takes no layout argument; `store.layout()` and `store.indexes()` report the same names.

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

A file store is **memory-mapped**: opening reads only the footer (and, under the Dictionary layout, the dictionary's window bounds), and every query reads the pages it touches straight from the file. What stays in RAM is the operating system's page cache, which shows up as file-backed RSS (`RssFile`) and is reclaimable — the process's own (anonymous) memory stays small whatever the store's size. Never truncate or rewrite a store file in place while it is open (`serialize_rdf` replaces it by rename instead); network filesystems are not supported. `VortexRdfStore(path, in_memory=True)` loads the whole store into memory instead, so each subsequent match skips the file.

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
