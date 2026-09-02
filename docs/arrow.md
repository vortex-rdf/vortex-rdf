# The Arrow interface

This document describes how a store hands its data to Apache Arrow
consumers: what an exported record batch looks like, how each cell encoding
is produced from the store's own columns, what is and is not copied along
the way, and how the two bindings surface it — the Python PyCapsule
protocol and the JavaScript IPC bytes. How the rows themselves are resolved
is [matching.md](matching.md); where the term dictionary comes from is
[file-format.md](file-format.md); how appended rows (the tail) and deletes
enter a view is [mutations.md](mutations.md).

---

## 1. Purpose

A matched view is a row selection over columns that are already columnar:
`u32` term codes under the Dictionary layout, N-Triples strings under the
Default layout. The pattern-read APIs (`get_quads`, `match`, `getQuads`)
turn those columns into per-row, per-term objects, and that conversion —
not the match — is what dominates a query that keeps thousands of rows.

The Arrow interface skips it. A view is exported as Arrow record batches
whose columns share the store's buffers wherever the memory layout already
matches, so any Arrow-native engine — pyarrow, polars, DuckDB, DataFusion,
`apache-arrow` in JavaScript — consumes the match directly. For a SPARQL
engine that is the difference between joining Python tuples and joining
`uint32` columns: the codes of a Dictionary-layout store are lexicographic
ranks over one global term dictionary, so joins, filters, `DISTINCT` and
`GROUP BY` run in code space and only the surviving codes are decoded, once
each, through the dictionary. The interface is therefore also the contract
a query engine integration builds on: the schema, the projected batch
stream, the dictionary as an Arrow array, and the code bounds that turn a
term prefix into a code range.

Nothing here changes the store or its file format; the Arrow surface reads
the same views every other read path reads.

---

## 2. What is exported

### 2.1 The quad schema

Every batch carries the schema
[`quad_schema`](../core/src/arrow/mod.rs#L160) gives for the store's
layout and the requested [`TermEncoding`](../core/src/arrow/mod.rs#L47):
the four primary columns `s`, `p`, `o`, `g`, in the serialized order
([`PRIMARY_COLUMNS`](../core/src/store/schema.rs#L28)), all non-nullable,
every cell typed alike.

| Encoding | Cell type | Layouts | What a cell is |
|---|---|---|---|
| `codes` | `uint32` | Dictionary | the term's code: its rank in the sorted term dictionary |
| `terms` | `dictionary<uint32, string_view>` | Dictionary | the same code, as a key into the whole term dictionary attached as the Arrow dictionary values |
| `strings` | `string_view` | Dictionary, Default | the term's N-Triples spelling (`<iri>`, `_:blank`, `"lit"@lang`, `"lit"^^<dt>`) |

The default graph is spelled as the empty string, the store's own
convention. The TypedObject layout has no Arrow export; the dictionary
column encodings on a Default-layout store, or any encoding on a
TypedObject store, are rejected when the schema is asked for.

The schema metadata names the producer
([`META_LAYOUT`](../core/src/arrow/mod.rs#L33) and its siblings):
`vortex_rdf.layout` (the canonical kebab-case layout name),
`vortex_rdf.term_encoding` (`codes` | `terms` | `strings`),
`vortex_rdf.version` (the crate version) and `vortex_rdf.default_graph`
(`""`).

A **projection** — a list of [`QuadColumn`](../core/src/arrow/mod.rs#L96)s
— restricts and orders the columns ([`projected_schema`](../core/src/arrow/mod.rs#L199)
keeps the metadata); it must be non-empty and name each column once. A
triple pattern rarely needs all four positions, and a file scan reads only
the projected columns ([§3.2](#32-code-batches)).

### 2.2 The batch stream

[`to_record_batches`](../core/src/store/batches.rs#L56) is the one entry
point: it takes an encoding and an optional projection and returns a
[`QuadBatches`](../core/src/arrow/mod.rs#L231) — a `Stream` of
`RecordBatch`es that all carry the schema `QuadBatches::schema()` reports,
one batch per decode chunk (the whole in-memory base, or each scan split
of a file), empty chunks skipped. The stream owns everything it reads —
decoded rows, or a scan over the file handle — so it outlives the store
handle it was taken from, which is what lets a binding hand it to a
consumer that pulls batches later. Tombstoned rows are never exported.

Codes and terms need every row to be addressable in the dictionary a
consumer will decode against. A view with a non-empty append tail is not:
tail rows store their terms as strings, and the terms appended have no code
in the frozen dictionary ([mutations.md §2](mutations.md#2-additions-the-append-tail)).
Such a view rejects `codes` and `terms` with an error — export `strings`,
or compact first — exactly the gate
[`code_read_snapshot`](../core/src/store/mod.rs#L506) applies to the
code-column readers.

### 2.3 The dictionary as an Arrow array

[`DictSnapshot::to_arrow`](../core/src/store/layouts/dictionary/term_dict.rs#L941)
returns the whole term dictionary as one `string_view` array whose element
`i` is the term of code `i`: a code → term lookup table, and the values
array every `terms` batch is keyed over. It is built on first use and
held weakly on the dictionary
([`arrow_values`](../core/src/store/layouts/dictionary/term_dict.rs#L559)):
every export alive at the same time shares one `Arc`, and the array is
freed with its last holder. Canonical (plaintext) chunks convert
buffer-sharing; FSST-compressed chunks decompress once per set of
concurrent holders, a cost bounded by the dictionary's size, never by a
result's ([§3.4](#34-the-dictionary-values)).

Two bounds expose the lexicographic-rank structure of the code space to a
planner. [`lower_bound`](../core/src/store/layouts/dictionary/term_dict.rs#L949)
is the first code whose term is byte-wise `>=` a string, so
`lower_bound(a)..lower_bound(b)` is exactly the codes of the terms in
`a..b`; [`prefix_range`](../core/src/store/layouts/dictionary/term_dict.rs#L958)
is the half-open code range of the terms spelled with a prefix — an IRI
namespace is the prefix `<http://…/`, and because N-Triples kinds partition
the space by first byte (`"` literals, `<` IRIs, `_` blank nodes), kind
bounds are prefix ranges too. Both are a binary search through the
dictionary cursor
([`lower_bound_bytes`](../core/src/store/layouts/dictionary/term_dict.rs#L538)),
the same probe the exact `encode` runs.

---

## 3. How a batch is produced

```mermaid
flowchart TD
    V["matched view"] --> E{"encoding"}
    E -- "codes / terms" --> G{"tail empty?"}
    G -- "no" --> X["error: export strings or compact"]
    G -- "yes" --> C{"code_columns_shared()<br/>serves the view?"}
    C -- "yes (canonical base, served run,<br/>or live canonical form)" --> B1["one batch over the<br/>served u32 buffers"]
    C -- "no" --> P["primary_chunks:<br/>in-memory base rows, or<br/>projected file scan splits"]
    P --> B2["u32 struct chunk →<br/>UInt32 arrays, buffer-sharing"]
    B1 --> T{"terms?"}
    B2 --> T
    T -- "yes" --> D["wrap keys over the one<br/>shared dictionary values array"]
    E -- "strings" --> S["shared_quad_chunks():<br/>serve plans, tombstones, tail"]
    S --> B3["StringViewArray per column"]
```

### 3.1 Two pipelines

The two code-typed encodings and the string encoding take different
routes, because their inputs are different things.

`codes` and `terms` ([`code_batches`](../core/src/store/batches.rs#L88))
want the primary columns exactly as the Dictionary layout stores them:
`u32` code columns. Nothing is decoded; the work is finding the right rows
and converting each column's buffer.

`strings` ([`string_batches`](../core/src/store/batches.rs#L125)) wants
N-Triples spellings, which under the Dictionary layout means resolving
codes through the dictionary and under the Default layout means the stored
strings themselves. Rather than a third decode path, it rides the store's
existing shared-term decode stream,
[`shared_quad_chunks`](../core/src/store/streaming.rs#L65) — the same
[`decoded_chunks`](../core/src/store/streaming.rs#L92) pipeline behind
`quads_vec`, which already applies serve plans, drops tombstones, decodes
each distinct term of a chunk once and appends the tail — and builds a
`string_view` column per projected position from each decoded chunk
([`shared_chunk_to_batch`](../core/src/store/batches.rs#L233)). That is a
copy of every cell's bytes, the price of materializing strings at all.

### 3.2 Code batches

Two sources feed the code pipeline, chosen per view.

**Served buffers.** When
[`code_columns_shared`](../core/src/store/rows.rs#L238) serves the view —
a built base's canonical `u32` columns, a served match reading the
answering index's own columns, or an adopted base's live canonical form —
the batch is built straight from the four buffers it returns
([`code_buffers_to_batch`](../core/src/store/batches.rs#L177)). This is
the path the bindings' `match_arrow` / `matchArrowIPC` take — their one
engine-facing read — so every consumer of a view hands out the same memory. A
store *adopted* from bytes or a file keeps its base wire-encoded
([serialization.md](serialization.md)): a contiguous wide read decodes
each column once into a form every holder shares and the last holder
frees, so two exports alive at the same time are the same buffers
([memory.md](memory.md)); a point-sized or scattered
selection over an adopted base is gathered instead, one allocation per
call.

**Primary chunks.** Otherwise
[`primary_chunks`](../core/src/store/batches.rs#L140) streams the base's
primary columns as encoded chunks in base row order, the view's selection
applied and tombstones excluded: one chunk for an in-memory base (the
array itself when the view covers all of it), and for a file one chunk per
scan split of the restricted scan every unserved file read starts from —
here in its projected form,
[`restricted_file_scan_projected`](../core/src/store/rows.rs#L417), so
only the projected columns are decoded off the file. A served match's
pending selection materializes first, as it does for every base-order
read. Each chunk's columns then convert through vortex-arrow's
buffer-sharing primitive kernel
([`code_chunk_to_batch`](../core/src/store/batches.rs#L197)): the Arrow
`UInt32Array` wraps the chunk's own buffer.

For `terms`, each column's keys are wrapped over the dictionary's values
array ([§3.4](#34-the-dictionary-values)); the wrap validates that every
key is in range, a linear pass over the codes and no copy.

### 3.3 What is and is not copied

| Step | Copies | Notes |
|---|---|---|
| served `u32` buffers → `UInt32Array` | no | Arrow's buffer refcounts the vortex buffer |
| adopted base, contiguous wide read | once per set of concurrent holders | the live canonical form: shared with every export alive, freed with the last ([memory.md](memory.md)) |
| file scan chunk → `UInt32Array` | no, after the scan's own decode | the scan materializes each split once |
| `terms` key wrap | no | one shared values `Arc` per stream |
| dictionary values, canonical chunks | no | `string_view` over the dictionary's own buffers |
| dictionary values, FSST chunks | once per set of concurrent holders | decompressed on first use, held weakly, freed with the last holder |
| `strings` cells | yes, every cell | the string materialization itself |
| Python capsule export | no | the C Data Interface hands out the same buffers |
| JavaScript IPC bytes | yes, the whole result | one copy out of wasm memory, like the lazy quad payload |

### 3.4 The dictionary values

[`arrow_values`](../core/src/store/layouts/dictionary/term_dict.rs#L559)
turns the term column into one canonical `VarBinViewArray` — a single
canonical chunk is used as it is; an FSST chunk, or a chunked column, is
executed to canonical once through the Vortex session (chunks concatenated
as a `ChunkedArray`) — and converts it with vortex-arrow's canonical
byte-view kernel, which shares the views and data buffers. The result is
held by a weak reference on the dictionary: callers that arrive while some
holder is alive share it, and after the last holder drops the next call
rebuilds it.

The conversion registry those kernels belong to, the `ArrowSession`, is
registered on the crate's one Vortex session at startup
([`vortex_arrow::initialize`](../core/src/session.rs#L38)).

---

## 4. Python: the PyCapsule interface

The bindings speak the
[Arrow PyCapsule interface](https://arrow.apache.org/docs/format/CDataInterface/PyCapsuleInterface.html):
objects expose `__arrow_c_array__`, `__arrow_c_schema__` or
`__arrow_c_stream__`, each returning a capsule around a C Data Interface
struct, and any consumer — `pyarrow.array`, `pyarrow.RecordBatchReader.from_stream`,
`polars.Series`/`polars.DataFrame`, a DuckDB query — imports it. Neither
side needs pyarrow; the package keeps zero runtime dependencies.

```python
stream = store.match_arrow(p="<http://xmlns.com/foaf/0.1/name>")     # ArrowQuadStream
pa.schema(stream)                                                     # s, p, o, g: uint32
table = pa.RecordBatchReader.from_stream(stream).read_all()
frame = pl.DataFrame(store.match_arrow(encoding="terms", projection=["s", "o"]))
pa.array(store.term_dict().filter_codes("is_iri", "")[0])             # a code set, zero-copy
pa.array(store.term_dict())                                           # the dictionary
```

[`match_arrow`](../python/src/store.rs#L471) resolves the pattern and
builds the core batch stream off the GIL, then wraps it in an
[`ArrowQuadStream`](../python/src/arrow.rs#L110). Its
[`__arrow_c_schema__`](../python/src/arrow.rs#L136) can be read any number
of times; [`__arrow_c_stream__`](../python/src/arrow.rs#L146) takes the
stream out of the object once (a second call raises `ValueError`) and
exports it as an `FFI_ArrowArrayStream` over a
[`BlockingReader`](../python/src/arrow.rs#L84): each `get_next` the
consumer issues blocks on the next batch on the bindings' tokio runtime.
The reader holds no Python state, so it runs wherever the consumer calls it
from — pyarrow, for one, releases the GIL around `read_next_batch` — and
batches are produced as they are pulled, never ahead of the consumer.

A code set's [`__arrow_c_array__`](../python/src/codes.rs#L200) wraps
the column's `u32` buffer as a `UInt32Array` — the same memory the buffer
protocol exposes — and the dictionary's
[`__arrow_c_array__`](../python/src/codes.rs#L200) hands out the cached
values array of [§2.3](#23-the-dictionary-as-an-arrow-array). Both go
through [`array_capsules`](../python/src/arrow.rs#L74): the array's
`ArrayData` is exported with arrow-rs's `to_ffi`, which shares the buffers
and keeps them alive from the struct's private data until `release`.

Capsule ownership follows the protocol. Each struct is boxed and its
address becomes the capsule pointer ([`capsule`](../python/src/arrow.rs#L56));
the capsule's destructor ([`release_capsule`](../python/src/arrow.rs#L41))
drops the box, which runs the struct's `release` callback — unless a
consumer already moved the struct out and nulled `release`, in which case
the drop is a no-op and the consumer owns the buffers. Release callbacks
only drop Rust reference counts, so they are safe on whatever thread
finalizes the capsule. `requested_schema` is accepted everywhere for
protocol conformance and not applied: the batches keep the schema the
stream reports.

The `pyarrow`/`polars` packages appear only as test dependencies
([tests/test_arrow.py](../python/tests/test_arrow.py)).

---

## 5. JavaScript: Arrow IPC bytes

JavaScript has no C Data Interface counterpart of the capsule protocol,
and the Arrow ecosystem there — `apache-arrow`'s `tableFromIPC`,
DuckDB-WASM, Arquero, Perspective — consumes the
[IPC streaming format](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format).
So the wasm bindings export exactly that:
[`matchArrowIPC`](../js/src/store.rs#L340) resolves the pattern, drives
the core batch stream to completion (no wasm read path performs I/O, so
the stream is already resolved and nothing suspends) and writes the
schema and every batch through arrow-ipc's `StreamWriter` into one
`Uint8Array`.

```javascript
const table = tableFromIPC(store.matchArrowIPC(null, myPredicate, null, null));
const terms = tableFromIPC(store.matchArrowIPC(null, null, null, null, { encoding: 'terms' }));
```

The options object ([`parse_arrow_options`](../js/src/options.rs#L93))
carries the same `encoding` and `projection` vocabulary as core, parsed by
core's own `FromStr` impls so an error message reads the same from every
frontend. Under `terms` the dictionary is written as IPC dictionary
batches and the record batches carry only `u32` keys, so the shared
dictionary crosses the boundary once per column rather than once per
batch.

The bytes are one copy out of wasm memory — the same choice the lazy quad
payload makes with its `Uint32Array`s
([`set_code_columns`](../js/src/store.rs#L460)), because a view into wasm
linear memory is detached the moment the memory grows. A zero-copy path (`arrow-js-ffi` reading C Data Interface structs
out of wasm memory) would need explicit release handles and memory-growth
discipline on the consumer's side, and is not part of this surface.

---

## 6. Test map

| Surface | Tests |
|---|---|
| core | [tests/arrow.rs](../core/src/tests/arrow.rs): buffer sharing on a built store, codes/terms/strings against the code and shared-quad readers (in memory and file-backed), tail and tombstone equivalence, projection, rejected combinations; [arrow/mod.rs](../core/src/arrow/mod.rs) schema tests; [term_dict.rs](../core/src/store/layouts/dictionary/term_dict.rs) dictionary values and bounds |
| Python | [tests/test_arrow.py](../python/tests/test_arrow.py): capsule round-trips into pyarrow and polars, buffer-address equality for code sets, stream-versus-`get_quads` equality on file-backed and in-memory stores, one shared dictionary across columns and batches, consume-once semantics, projection, per-layout rejection; [tests/test_primitives.py](../python/tests/test_primitives.py): `keep`, windows and batches through the stream, Arrow arrays as code sets |
| JavaScript | [test/arrow.test.ts](../js/test/arrow.test.ts): IPC tables against `getQuads` and `termDict`, schema metadata, string-view and dictionary column types, projection, rejected options and layouts |
