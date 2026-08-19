# vortex-rdf
[![PyPI](https://img.shields.io/pypi/v/vortex-rdf.svg)](https://pypi.org/project/vortex-rdf/)

Python bindings for [Vortex-RDF](https://github.com/vortex-rdf/vortex-rdf), a
modern, high-performance columnar RDF serialization format.

Stores can be **opened lazily from `.vortex` files** and queried in place, without loading the dataset into memory.

A separate [`vortex-rdflib`](https://pypi.org/project/vortex-rdflib/) package builds an rdflib integration on these bindings; see its own documentation for what it supports.

## Install

```bash
pip install vortex-rdf
```

Development build (managed with [uv](https://docs.astral.sh/uv/); maturin
runs under the hood as the build backend):

```bash
cd python
uv sync                      # creates .venv, builds + installs the extension
uv run pytest tests          # run the test suite
uv run maturin develop --uv  # fast rebuild while iterating on Rust code
```

Rust source changes are picked up by `uv sync` automatically (see
`[tool.uv] cache-keys` in pyproject.toml). Without uv, the classic flow
works too: `python -m venv .venv && pip install maturin && maturin develop`.

Building from source (the sdist or a development build) additionally
requires **libclang**: a transitive build dependency of the Vortex file
engine (`custom-labels`, via `vortex-io`) generates C bindings with
`bindgen` at compile time. It is preinstalled on most dev setups (Xcode,
LLVM on Windows); on Linux install e.g. `clang-devel` (dnf) or
`libclang-dev` (apt). Installing a published wheel needs none of this.

## Usage

```python
from vortex_rdf import VortexRdfStore, serialize_rdf

# RDF file -> .vortex store file
serialize_rdf("data.nt", "data.vortex", layout="dictionary")

store = VortexRdfStore("data.vortex")   # lazy open; file layout is auto-detected
len(store)                              # number of quads
store.get_quads(p="<http://xmlns.com/foaf/0.1/name>")       # [(s, p, o, g), ...]
store.match_columns(p="<http://xmlns.com/foaf/0.1/name>")   # (subjects, predicates, objects, graphs)
```

Terms cross the boundary as N-Triples strings (`<iri>`, `_:b0`, `"lit"@en`,
`"3"^^<http://www.w3.org/2001/XMLSchema#integer>`).

`get_quads` returns whole quads — the graph of a quad in the default graph is
the empty string, which is also how a pattern selects it. `match_columns`
returns the same rows transposed into four parallel columns, for callers that
work a position at a time rather than row by row.

Both are served from the term-code columns whenever the store can (Dictionary
layout, resident dictionary, no append tail), which is roughly 2x faster on a
65,536-row match than re-serializing each matched quad. On that path a term
that repeats down a column is one shared Python string rather than an equal
copy per occurrence, so a caller converting terms into its own representation
can memoize on the string it is handed. Results are identical either way, so
this is a speed-up rather than a choice to make; the code API below is for
callers who want to skip building strings altogether.

### Code columns (Dictionary layout)

For Dictionary-layout stores, `match_codes` returns the matched rows as four
**zero-copy** `u32` term-code columns — `memoryview(col).cast("I")` views the
Rust memory directly — decodable through a `term_dict()` handle:

```python
cols = store.match_codes(p="<http://xmlns.com/foaf/0.1/name>")  # (s, p, o, g)
dictionary = store.term_dict()
subjects = memoryview(cols[0]).cast("I")
dictionary.decode(subjects[0])           # N-Triples string for that code
dictionary.decode_many(cols[0])          # bulk-decode a whole column at once
```

`decode_many` decodes a batch in one GIL-released call. Buffer-protocol
inputs — a column straight from `match_codes`, an `array("I", ...)`, a
`uint32` NumPy array — are read in a single bulk copy with no per-element
int conversion; any sequence of ints works too. Both `term_dict()` and
`match_codes` return `None` when the code path does not apply (non-Dictionary
layout, or a dictionary left file-backed by the residency budget).

Consumers can join, count, and de-duplicate entirely in code space and decode
each distinct term once, never materializing a term string for a row they
discard.

## Layouts

`serialize_rdf(..., layout=...)` accepts `"default"`, `"typed-object"` and
`"dictionary"`; `store.layout()` reports the same names. `indexes=[...]`
lists secondary index components to build into the file
(`"secondary-by-copy"`, `"secondary-by-reference"`). `format=` is an RDF
format name (`"ntriples"`, `"nquads"`, `"turtle"`, `"trig"`, `"n3"`,
`"rdfxml"`, `"jsonld"`, or their short aliases), detected from the input file
extension when omitted. Opening auto-detects the layout — `VortexRdfStore`
takes no layout argument.

For Dictionary-layout files, the term dictionary (carried in the file as its
own dictionary component) is held in memory when its byte size fits the
residency budget; pass `VortexRdfStore(path, max_resident_bytes=...)` to
change the budget (recommended for benchmarking large stores).

## File-backed vs in-memory

The default open is lazy and file-backed. `VortexRdfStore(path, in_memory=True)`
loads the store into memory once: each subsequent match skips the per-call
file-scan pipeline (~1 ms → ~0.15 ms per call).

Stores also round-trip through bytes: `store.to_bytes()` serializes to the
native container (the same exchange format as the `.vortex` file and the JS
bindings), and `VortexRdfStore.from_bytes(data)` opens such a buffer as a
fully in-memory store.

## Tests

```bash
uv run pytest tests   # or: maturin develop && pip install pytest && pytest tests
```

## Comparative benchmark

`python/bench/run.py` measures these bindings against
[pyoxigraph](https://pypi.org/project/pyoxigraph/),
[pycottas](https://pypi.org/project/pycottas/),
[rdflib](https://pypi.org/project/rdflib/), and
[lightrdf](https://pypi.org/project/lightrdf/), and writes `bench/results.json`
for the dashboard's Python tab.

```bash
python3 python/bench/run.py                 # full run (D=128, 2,097,152 triples)
BENCH_DIM=32 python3 python/bench/run.py    # quick pilot
```

It provisions **one virtualenv per library** (via `uv`, on first run) rather
than installing them together: pycottas hard-pins `pyoxigraph==0.3.18`, so a
shared environment would quietly measure a pyoxigraph two minor versions behind
the oxigraph the JavaScript tab compares against. Each library also runs in its
own process, so peak RSS is attributable to one library alone.

Two things differ from the JavaScript comparative bench, both because the
libraries do:

- **The workload is file → store → query.** Every library ingests from a file,
  and lightrdf has no store at all, so a serialized dataset is the only input
  the five share. That makes `open` a first-class measurement — a file-backed
  format opens its artifact, while an in-memory store re-parses the source
  every process start.
- **Not every library can do everything.** Cells read `unsupported` where the
  API does not exist: the Python bindings expose no mutation (unlike the JS
  ones), pycottas's store raises *"The COTTAS store is read only!"*, and
  lightrdf is a streaming parser with nothing to mutate.

The dataset generator is a deliberate port of `js/bench/datasets.ts` — same
moduli, same term spellings — so rows on the two tabs describe the same data.
The run cross-checks every library's matched-row counts against the others and
records any disagreement in `config.countWarnings`, which the dashboard shows:
five libraries with five term-spelling conventions are being asked the same
question, and a mismatch means at least one was asked it wrongly.
