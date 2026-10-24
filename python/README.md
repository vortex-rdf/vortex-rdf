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

Every read takes a pattern as the keyword arguments `s`, `p`, `o`, `g`; an omitted position is a wildcard. Terms cross the boundary as N-Triples strings (`<iri>`, `_:b0`, `"lit"@en`, `"3"^^<http://www.w3.org/2001/XMLSchema#integer>`); the graph of a quad in the default graph is the empty string, which is also how a pattern selects it. A malformed term or an invalid argument (an unknown format, layout or index name, an unknown predicate kind, a bad option, candidate codes out of order) raises `ValueError`, and a malformed pattern term's message names the position that held it (`Invalid input: pattern subject: invalid IRI "…": …`); an I/O failure raises the `OSError` subclass its kind names; anything else raises `VortexRdfError`: a store that does not read, or an operation the store refuses, such as `keep` on a non-Dictionary store.

```python
len(store)                                                   # number of quads
store.layout()                                               # "dictionary" | "default" | "typed-object"
store.indexes()                                              # e.g. ["secondary-by-reference"]
store.count_quads(p="<http://xmlns.com/foaf/0.1/name>")      # int
store.get_quads(p="<http://xmlns.com/foaf/0.1/name>")        # [(s, p, o, g), ...]
store.match_columns(p="<http://xmlns.com/foaf/0.1/name>")    # (subjects, predicates, objects, graphs)
```

`get_quads` returns whole quads; `match_columns` returns the same rows transposed into four parallel columns, for callers that work a position at a time. Both are served from the term-code columns whenever the store can (Dictionary layout) and from the matched quads otherwise; results are identical. On the code path a term that repeats down a column is usually one shared Python string: a small cache of recently decoded codes serves the repeats, and a code it has dropped is decoded again as an equal, separate string.

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

`decode_many` decodes a batch in one GIL-released call. A column straight from `match_codes` is shared, not copied. Other buffer-protocol inputs — `memoryview(col).cast("Q")`, an `array("Q", ...)`, a `uint64` or non-negative `int64` NumPy array — are read in a single bulk copy with no per-element int conversion; a raw-byte buffer whose length is a multiple of 8 (`memoryview(col)` itself, before the `cast`) is read as native-endian `u64`s; a buffer of any other items (a `cast("I")` view, a `uint32` array) raises `ValueError` rather than being read element by element. Any sequence of ints works too. `encode` is the inverse of `decode` and tolerant of spelling: an IRI with or without angle brackets, a literal with an explicit `xsd:string` type or an upper-case language tag, and the default graph as `""`, `default` or `[]` all resolve to the stored form's code (a malformed term raises `ValueError`); `encode_many` does a batch. Both `term_dict()` and `match_codes` return `None` when the code path does not apply (a non-Dictionary layout). A file store's dictionary is read from the mapped file on demand — `TermDict.file_backed` says so — with the same calls.

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

# Many probes, answered in input order: at most `lookahead` in flight, `lookahead` answers waiting.
for answer in store.match_codes_many([(None, NAME, None, None), {"s": "<http://ex.org/bob>", "limit": 5}]):
    ...                                     # one probe's (s, p, o, g), or None
counts = store.count_quads_many([{"p": NAME, "keep": {"o": literals}}])

# A match of any size in chunks of at most batch_rows rows: in memory each is sliced or
# gathered when it is pulled, and a file scan reads a bounded window of splits ahead.
for subjects, objects in store.match_code_chunks(p=NAME, columns=("s", "o"), batch_rows=65536):
    ...

# One token per query stops its long calls from any thread.
from vortex_rdf import CancelToken
token = CancelToken()
chunks = store.match_code_chunks(p=NAME, cancel=token)
n = store.count_quads(p=NAME, cancel=token)
token.cancel()                              # the next pull with it raises Cancelled

# Column kernels, order-preserving: first-seen distinct values, nested-loop joins.
s, p, o, g = cols
s.distinct(); o.value_counts()
left_idx, right_idx = o.join_indices(s)     # rows where o == s, as index pairs
o.take(left_idx)                            # gather a joined column
o.join_count(s)                             # the number of pairs, none built
o.join_indices(s, max_pairs=1_000_000)      # JoinTooLarge (.pairs is the count) rather than more pairs
groups = s.groups()                         # the join's right side, built once
x, z = groups.gather(o, [s], [o])           # rows where o == s: the left s, the right o, no index pairs
for start in range(0, len(o), 65536):       # a large join, one zero-copy left slice at a time
    left_idx, right_idx = groups.join(o.slice(start, min(start + 65536, len(o))))
```

`filter_codes(kind, arg, codes, *, flags="", case=None, as_str=False)` evaluates a FILTER predicate over the candidate `codes` (sorted and unique, else `ValueError`) and answers `(passed, undecided)`, both subsets of `codes`; a candidate in neither fails, and the undecided ones are left to the caller's own evaluator. No verdict is memoized. Kinds: `is_literal`, `is_iri`, `is_blank`; `datatype <iri>`, `lang <tag>`, `lang_matches <range>`; `num_lt`/`num_le`/`num_gt`/`num_ge`/`num_eq`/`num_ne <number>` (value comparison for well-formed numeric literals; `xsd:long`/`xsd:unsignedLong` beyond 64 bits undecided; different XSD datatypes order by their IRIs; a non-literal fails the orderings and `=`, passes `!=`); and the string kinds `str_prefix <p>`, `contains`/`strstarts`/`strends <constant's N-Triples spelling>`, `regex <pattern>` with `flags`. String kinds follow rdflib 7.6: the text is a string literal's lexical form (with `as_str=True`, SPARQL `STR()`: an IRI's string or a literal's lexical form; a blank node, or a literal whose datatype rdflib normalizes, is undecided), `case="lower"|"upper"` wraps that text (Python's `str.lower()`/`str.upper()`, decided on ASCII text only), a language-tagged constant needs the same tag on the text, and `regex` decides only patterns from an allow-listed subset that agrees with Python's `re`; the stub's docstring has the exact rules.

`keep` takes a dict from position (`"s"`, `"p"`, `"o"`, `"g"` or 0–3) to a code set or a code range. A set is a `U64Column`, a u64 buffer or a list of ints. A range is a `range` with step 1 (a stepped `range` raises `ValueError`) or a 2-tuple `(lo, hi)`, the half-open codes `lo <= code < hi`. A 2-tuple is always a range, so a set of codes is passed as a list or a `U64Column`, never as `tuple(codes)`. `limit`/`offset` window the rows (`count_quads` counts the same window): a window that cuts rows takes them in base order, and a filtered file scan stops at the first block that fills it, while a `limit` that cannot cut a row (no `offset`, and at least the match's row count, or a bound on it, known without reading its rows: an index-served file match of unknown width reads its row ids for the count) returns the match as it is, in the order the call without a limit returns. With no `offset`, a `limit` that a filtered file match's rows turn out to fall short of returns the match as it is too, read from the window's evaluation of the filter, which a match whose reads evaluate the filter before reading a row makes even when its bound fits the limit. A probe of the `*_many` calls is an `(s, p, o, g)` tuple or a dict with keys `s`, `p`, `o`, `g`, `keep`, `limit`, `offset`; every probe is parsed before any is evaluated.

`match_codes_many` returns an iterator whose probes start with the call and keep running between pulls, at most `lookahead` of them in flight (by default twice the CPU count, and at least 64; pass a smaller one for heavy scan probes) and at most `lookahead` answers waiting to be pulled; dropping it aborts the probes still running. `match_code_chunks` takes the narrowing of `match_codes`, plus `columns` (any of `"s"`, `"p"`, `"o"`, `"g"`, in the order the chunks hold them) and `batch_rows` (65,536 by default): in memory each chunk is sliced or gathered when it is pulled, and on file a scan reads a bounded window of splits ahead of the pulls ([docs/interfaces.md §3](https://github.com/vortex-rdf/vortex-rdf/blob/main/docs/interfaces.md#3-rows-as-columns-without-arrow)). A `CancelToken` passed as `cancel=` to `match_codes`, `count_quads`, the `*_many` calls or `match_code_chunks` stops them from any thread: a call or pull waiting when it is cancelled stops at its next wait for data (on file, the next split boundary) and raises `Cancelled`, a `VortexRdfError`, as does every later call or pull given the token, while a result already complete is returned; without a token the calls run unraced on the calling thread. `join_count` counts a join's pairs without building them, and `join_indices(other, max_pairs=n)` raises `JoinTooLarge` (`.pairs` is the pair count) before building more than `n`. `groups()` builds a column's side of a join once; `CodeGroups.join` of consecutive `slice`s of the other column, each offset by its start, concatenates to the one-shot `join_indices`, and `CodeGroups.gather` returns the joined columns themselves, filled as the pairs are found, with no index pairs written in between.

## Build options

```python
serialize_rdf(input_path, output_path, *, format=None, layout="dictionary", indexes=[])
```

`serialize_rdf` writes beside `output_path` and renames the finished file into place, so a build that fails (a parse error halfway through the input, say) leaves no partial file and an existing store untouched. A store file the process cannot write (a read-only file) is never replaced: `serialize_rdf` raises `PermissionError`. On Linux and macOS rebuilding a path that an open `VortexRdfStore` has mapped is safe: the open store keeps reading the file it mapped. On Windows the rename is refused while a store has the file mapped, so the rebuild fails with an `OSError` until that store, and every `TermDict` and `U64Column` taken from it, is dropped. Every option after the two paths is keyword-only. `format` is an RDF format name (`"ntriples"`, `"nquads"`, `"turtle"`, `"trig"`, `"n3"`, `"rdfxml"`, `"jsonld"`, or the short aliases `nt`, `nq`, `ttl`, `rdf`, `xml`), detected from the input file extension when omitted. Opening auto-detects the layout and indexes — `VortexRdfStore` takes no layout argument; `store.layout()` and `store.indexes()` report the same names.

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

A file store is **memory-mapped**: opening reads only the footer (and, under the Dictionary layout, the dictionary's window bounds), and every query reads the pages it touches straight from the file. What stays in RAM is the operating system's page cache, which shows up as file-backed RSS (`RssFile`) and is reclaimable, plus at most 4 MiB of rebuilt dictionary windows per memory-mapped store, which every `VortexRdfStore(path)` is — the process's own (anonymous) memory stays small whatever the store's size. The mapping lives as long as the store, or any `TermDict` or `U64Column` taken from it, does. Never truncate or rewrite a store file in place while it is open (`serialize_rdf` replaces it by rename instead); network filesystems are not supported. `VortexRdfStore(path, in_memory=True)` loads the whole store into memory instead, so each subsequent match skips the file. On Linux with glibc, importing the module raises the allocator's trim threshold, top pad and mmap threshold (64 MiB, 16 MiB, 32 MiB), so the buffers one large read frees are kept for the next instead of being returned to the kernel and faulted in again; set `GLIBC_TUNABLES` or any `MALLOC_*_` variable and your own settings stand.

Stores also round-trip through bytes: `store.to_bytes()` serializes to the native container (the same exchange format as the `.vortex` file, the CLI and the JS bindings), and `VortexRdfStore.from_bytes(data)` opens such a buffer — `bytes` or `bytearray` — as a fully in-memory store.

The bindings start their worker threads on first use. A process that forks after calling into them (opening a store, `serialize_rdf`) leaves the child without them, and batch calls in the child hang. Call into the bindings only after forking, or use the `spawn` or `forkserver` start method.

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

Benchmarks live in [bench/](https://github.com/vortex-rdf/vortex-rdf/blob/main/python/bench/README.md).

## License

MIT
