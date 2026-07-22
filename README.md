# Vortex-RDF
[![Crates.io](https://img.shields.io/crates/v/vortex-rdf-core.svg)](https://crates.io/crates/vortex-rdf-core)
[![docs.rs](https://img.shields.io/docsrs/vortex-rdf-core)](https://docs.rs/vortex-rdf-core)
[![License](https://img.shields.io/crates/l/vortex-rdf-core)](https://github.com/vortex-rdf/vortex-rdf/blob/main/LICENSE)
[![CodSpeed](https://img.shields.io/endpoint?url=https://codspeed.io/badge.json)](https://app.codspeed.io/vortex-rdf/vortex-rdf?utm_source=badge)
[![CI](https://github.com/vortex-rdf/vortex-rdf/actions/workflows/ci.yml/badge.svg)](https://github.com/vortex-rdf/vortex-rdf/actions/workflows/ci.yml)

Vortex-RDF is a columnar RDF serialization built on top of the [Vortex](https://docs.vortex.dev) data format. It combines the flexible graph-based model of RDF with the efficiency of modern columnar data formats. Its main goal is to **provide a compact, zero-copy and high-performance serialization format for exchanging and read/write RDF data**.

This library provides both serialization and deserialization capabilities for converting traditional RDF formats (everything supported by [`oxrdfio`](https://docs.rs/oxrdfio/latest/oxrdfio/)) to Vortex-RDF and vice-versa. It also provides a queryable RDF quad store (`VortexRdfStore`) with basic graph pattern matching, exposed to JavaScript/WASM through an interface modeled after the [RDF-JS specification](https://rdf.js.org/dataset-spec/#datasetcore-interface).

## Key Features

- 📊 **Advanced Columnar Storage**: Leverages [Vortex format specifications](https://docs.vortex.dev/specs/file-format) for flexible arrays organized in columnar layouts, both on disk and in memory.
- ♻️ **Zero-Copy**: Vortex-RDF is built on a ["Zero-Copy" philosophy](https://en.wikipedia.org/wiki/Zero-copy). This means that after the RDF data is serialized into the Vortex format, it can be read, filtered, and queried without ever moving or copying the bytes in memory.
- 📦 **Adaptive Compression**: Smart compression strategies can be applied based on the [BtrBlocks approach](https://www.cs.cit.tum.de/fileadmin/w00cfj/dis/papers/btrblocks.pdf), which provides a sophisticated multi-level compression system that adaptively selects optimal compression schemes based on data characteristics. These include e.g., [Fast Static Symbol Table (FSST)](https://doi.org/10.14778/3407790.3407851), [Run-Length Encoding (RLE)](https://en.wikipedia.org/wiki/Run-length_encoding), [BitPacking](https://doi.org/10.1002/spe.2326), among others.
- ☄️ **Streaming & Out-of-Core Ingestion**: Quads can be streamed directly into and out of Vortex files with bounded memory. Larger than RAM knowledge graphs can be globally sorted via an external merge sort that spills sorted runs to disk, when serializing.
- 🍀 **RDF Quads Support**: Full support for named Graphs `(S, P, O, G)` and in general for [RDF 1.1](https://www.w3.org/TR/rdf11-concepts/).
- 🌍 **Cross-Platform**: Native Rust library with a CLI + WebAssembly (WASM) bindings for browsers/Node.js. Python bindings coming soon.

#### How it works:
1. **Zero-copy buffer views**: When you want to access a specific column (e.g., just the `predicates`) or a specific subset of Quads, Vortex creates a [_Layout_](https://docs.vortex.dev/concepts/layouts) either from a Vortex file stored on disk or from Vortex encoded data in memory. Both representations are structured in the same way, which avoids having to convert data before reading it. Bottom line, the Layout is just metadata and pointers to the actual data, it doesn't need to duplicate it.
2. **Lazy Decompression**: Even when compressed, Vortex is designed to decompress data "_just-in-time_" at the CPU register level, while leveraging [SIMD optimizations](https://en.wikipedia.org/wiki/Single_instruction,_multiple_data) and avoiding the decompression of unnecessary data.

### Vortex File format & IPC

Vortex-RDF leverages the [Vortex File specification](https://docs.vortex.dev/specs/file-format) and the [Vortex IPC (Inter-Process Communication) protocol](https://docs.vortex.dev/developer-guide/internals/serialization#ipc-format) to provide versatile serialization options optimized for both local storage and remote data exchange.

1. **Vortex Files**: Zero-Copy
The `.vortex` files are optimized for **local usage** with disk-based storage (Cloud-based alternatives based on blob storage solutions, e.g., Amazon S3 buckets, could be also supported). These files are designed to allow efficient compression and random access, allowing the OS to load only necessary chunks on demand without any parsing overhead. Opening a file (`VortexRdfStore::from_file`) is lazy: no data is read until queried, and `match_pattern` filters are pushed down into the file scan as Vortex filter [expressions](https://docs.vortex.dev/concepts/expressions).

2. **IPC Streams**: Remote Exchange
For exchanging data between different systems or over a network, the library can serialize RDF graphs into a **Vortex IPC Stream**. This format follows the Vortex IPC streaming protocol, making it suitable for pipes, sockets, and network transfers. These streams can be consumed by any Vortex-compatible client (Rust, Python, C++, etc.) to receive the Vortex-RDF data, while avoiding any deserialization and decompression overhead. The WASM bindings use IPC as their byte-exchange format.

Both formats share the same underlying principles:
- **Self-Describing**: Every file/stream contains a FlatBuffers schema describing the set of [`DType`](https://docs.vortex.dev/concepts/dtypes) (data types).
- **Unified Encodings**: Specialized encodings are preserved verbatim. This means compressed data **stays compressed** during transfer and is only decompressed lazily when strictly needed by the consumer.

This versatile approach ensures that Vortex-RDF can serve as both a high-performance local database engine and an efficient interchange format for distributed RDF processing.

---

## Architecture

RDF quads are stored as a Vortex [`StructArray`](https://docs.rs/vortex/latest/vortex/array/arrays/struct_/type.StructArray.html) (or a [`ChunkedArray`](https://docs.rs/vortex/latest/vortex/array/arrays/chunked/type.ChunkedArray.html) of StructArrays for chunked/streamed builds). How that array is shaped and built is controlled by three **orthogonal, build-time choices**:

1. A **column layout** (`LayoutStrategy`) — the columnar schema used for the quad terms.
2. An optional set of **secondary indexes** (`IndexType`) — extra columns embedded alongside the quads to accelerate pattern matching.
3. An **ingestion builder** (`BuilderStrategy`) — how the incoming quad stream is turned into that array (sorting and memory model).

A single store type, `VortexRdfStore`, works over any resulting array: it auto-detects the layout from the schema's field names and routes queries accordingly.

### 1. Column Layouts

#### `Default`
All four quad fields are stored as raw UTF-8 strings in canonical N-Triples form. Vortex's adaptive compression (FSST, dictionary encodings, etc.) handles the heavy repetition typical of RDF terms.

| Column | Type | Content |
|---|---|---|
| `s` | `VarBin<Utf8>` | Subject: `<IRI>` or `_:blank` |
| `p` | `VarBin<Utf8>` | Predicate: `<IRI>` |
| `o` | `VarBin<Utf8>` | Object: `<IRI>`, `_:blank`, `"lit"`, `"lit"@lang`, `"lit"^^<dt>` |
| `g` | `VarBin<Utf8>` | Graph: `<IRI>`, `_:blank`, or `""` for the default graph |

#### `TypedObject`
Same as `Default` for `s`, `p`, `g`, but the object column is decomposed into typed sub-columns so that Vortex can apply datatype-appropriate encodings (delta, RLE, dictionary) per component:

| Column | Type | Content |
|---|---|---|
| `o_kind` | `PrimitiveArray<u8>` | 0=IRI, 1=BlankNode, 2=PlainLiteral, 3=LangLiteral, 4=TypedLiteral |
| `o_value` | `VarBin<Utf8>` | IRI string, blank node ID, or literal value |
| `o_datatype` | `VarBin<Utf8>` (nullable) | Datatype IRI — non-null when `o_kind = 4` |
| `o_lang` | `VarBin<Utf8>` (nullable) | Language tag — non-null when `o_kind = 3` |

#### `Dictionary`
All four quad fields are stored as `u32` codes into a **single global term dictionary**: the lexicographically sorted set of unique term strings. The dictionary is intrinsic to the layout — it lives inside the array itself, in a `_dict_terms` column (`list<utf8>`, with the whole dictionary carried as row 0's list), so no external auxiliary structure is needed and the zero-copy principle is upheld.

| Column | Type | Content |
|---|---|---|
| `s`, `p`, `o`, `g` | `PrimitiveArray<u32>` | code = position of the term in the sorted dictionary |
| `_dict_terms` | `list<utf8>` | row 0 = the sorted unique terms as one list; all other rows empty |

Because term IDs are **lexicographic ranks**:
- ID comparisons are order-isomorphic to string comparisons, so sorted builders keep the subject binary-search fast path directly on the `u32` column.
- Term → ID lookup is a binary search over zero-copy string views (no `HashMap` needed on the query side); ID → term is a positional read.
- Query patterns are translated to integer comparisons host-side, so file scans push down cheap `u32` equality filters instead of string comparisons.

### 2. Secondary Indexes

Indexes are opt-in, embedded as extra columns at build time. Indexes are static and can only incorporate mutations by full reconstruction of the dataset (a.k.a. [compaction](https://github.com/vortex-rdf/vortex-rdf/blob/ff9add07c6ba855abb30d12f407777778de562de/core/src/store/vortex_rdf_store.rs#L313)). Two types are supported:

#### `SecondaryByReference`
Sorted permutation indexes for the predicate and object columns, enabling binary-search routing for predicate-only and object-only patterns without re-sorting the whole dataset:

| Column | Content |
|---|---|
| `_idx_o_val` | All object values in sorted order (`VarBin<Utf8>`; `u32` codes under the Dictionary layout) |
| `_idx_o_rid` | Row ID (`u32`) of the quad each sorted object value came from |
| `_idx_p_val` | All predicate values in sorted order (same dtype rules as above) |
| `_idx_p_rid` | Row ID (`u32`) of the quad each sorted predicate value came from |

The two backends engage this index differently:
- **In-memory**: binary-search routing only engages when the value columns carry the `IsSorted` statistic, which builders stamp when the columns hold a *globally* sorted order (single-chunk builds, or the sorted builders' global index emission) — an unstamped column (e.g. the concatenation of several per-chunk sorts) makes the store decline and fall back to a [mask scan](https://docs.rs/vortex/latest/vortex/scan/selection/enum.Selection.html).
- **File-backed**: the probe is always pushed down as a range-predicate scan over the index columns, regardless of the `IsSorted` stamp — sortedness only decides how tightly the scan's zone pruning shrinks the read, not whether the index is used. The index columns live in the file itself, so they're never re-derived or stripped; they stay valid however the store's view has been narrowed, and are simply never projected into the quads a caller sees.

#### `SecondaryByCopy`
Two complete extra copies of the quad columns — the classic triple-store permutation indexes (POS/OSP), adapted to quads — each paired with the primary row IDs it permutes. This gives predicate- and object-bound patterns the same sorted-column access path the primary `(s, p, o, g)` order gives subjects:

| Columns | Content |
|---|---|
| `_idx_posg_{s,p,o,g}` | The quads re-sorted by `(p, o, s, g)` (term strings, or `u32` codes under the Dictionary layout) |
| `_idx_posg_rid` | Row ID (`u32`) of the primary quad each copy row mirrors |
| `_idx_ospg_{s,p,o,g}` | The quads re-sorted by `(o, s, p, g)` |
| `_idx_ospg_rid` | Row ID (`u32`) of the primary quad each copy row mirrors |

Predicate-bound patterns binary-search `_idx_posg_p`; a bound predicate **and** object resolve in one `(p, o)` *prefix* search; object-bound patterns binary-search `_idx_ospg_o`. The two backends engage this index the same way `SecondaryByReference` does:
- **In-memory**: routing requires the lead value column's `IsSorted` stamp; an unstamped copy (a per-chunk sort that doesn't span the whole array) makes the store decline to a mask scan.
- **File-backed**: the probe is always pushed down as a range predicate, `IsSorted` or not — sortedness only sharpens zone pruning. File-backed stores additionally let `quads()` stream matched rows straight from the copy family — where they sit in a contiguous, zone-prunable run — instead of scattering row-ID reads across the primary columns, mirroring the subject fast path's locality.

Compared to `SecondaryByReference`, this costs roughly 2× the primary columns in extra storage (before compression; the sorted copies compress well) in exchange for contiguous reads on predicate/object patterns.

### 3. Ingestion Builders

| Builder | Memory model | Sorting | Disk spill | Result |
|---|---|---|---|---|
| **`UnsortedStream`** (default) | Streaming / bounded by chunk size | None (insertion order) | Only for the Dictionary layout* | Chunks of ≤100K quads |
| **`SortedInMemory`** | Full dataset in memory | Global `S → P → O → G` | No | Globally sorted array + globally sorted index columns |
| **`SortedStream`** | Out-of-core, bounded by chunk size | Global `S → P → O → G` | Yes | Globally sorted chunks + globally sorted index columns |

- **`UnsortedStreamBuilder`**: The simplest and fastest pipeline. It preserves the exact ordering of the incoming RDF stream and emits fixed-size chunks lazily; when serializing to a file, the Vortex writer compresses and flushes each chunk as it arrives, so peak memory is bounded by the chunk size instead of the dataset size. It cannot leverage subject ordering for binary-search pruning.
- **`SortedInMemoryBuilder`**: Loads all quads in memory and performs a global sort by `(s, p, o, g)`. Requested secondary index columns are built once over the whole dataset and emitted in global order (stamped `IsSorted`), so `match_pattern` can binary-search subjects, predicates, and objects.
- **`SortedStreamBuilder`**: External [merge-sort](https://en.wikipedia.org/wiki/Merge_sort) for datasets larger than memory. Quads are ingested in bounded batches, sorted locally, and spilled to disk as sorted runs; a K-way heap merge then emits globally sorted, fixed-size chunks. When secondary indexes are requested, a second external sort over `(value, row ID)` pairs produces globally sorted index columns as well. Spill files are internal, length-prefixed [`rkyv` records](https://github.com/rkyv/rkyv) under `target/` and are cleaned up automatically.

\* The Dictionary layout always needs two passes (the global dictionary is only complete after ingesting the whole stream), so even the unsorted builder spills raw quads to disk and re-reads them for encoding (except when called from the JS/WASM library).

The JS/WASM bindings only expose `UnsortedStream` (`'Unsorted'`) and `SortedInMemory` (`'Sorted'`) — `SortedStreamBuilder` spills sorted runs to disk, which WebAssembly has no access to, so it is unreachable from `VortexRdfStore`'s `BuildOptions`.

## The Store & Query Routing

`VortexRdfStore` wraps either an in-memory array or a lazily-scanned Vortex file. `match_pattern(s?, p?, o?, g?)` routes each bound component to the cheapest available access path, but the two backends get there differently.

**In-memory** resolves patterns with a routing cascade, each step clearing whichever components it answers:
1. **Subject binary search**: if a subject is bound and the base's `s` column is stamped `IsSorted`, the matching row range is found via a fast binary search and sliced — no scan at all.
2. **Index routing**: if `SecondaryByCopy` columns are present and stamped `IsSorted`, predicate-bound patterns binary-search `_idx_posg_p`, object-bound patterns `_idx_ospg_o`, and predicate+object patterns resolve both components in one `(p, o)` prefix search. Otherwise, if `SecondaryByReference` columns are present and stamped `IsSorted`, object-only and predicate-only patterns binary-search `_idx_*_val` and `take` the referenced rows from the quads columns.
3. **Vectorized mask scan**: remaining constraints are resolved with columnar equality masks (SIMD-friendly, no string materialization).

**File-backed** stores skip host-side binary search entirely: every bound component — subject included — compiles to a Vortex equality filter [expression](https://docs.vortex.dev/concepts/expressions) pushed down into the lazy scan, and the filter's zone-map envelope [narrows the read](https://docs.vortex.dev/developer-guide/internals/execution) to a contiguous row range before any data is fetched. Index columns (`SecondaryByCopy`/`SecondaryByReference`) are read the same way, as a range-predicate scan rather than a binary search, with `SecondaryByCopy` additionally streaming matched rows straight from the copy family (a contiguous, zone-prunable run) instead of gathering by row ID. `IsSorted` never gates whether a file-backed pattern uses its filter or index — sortedness only sharpens how tightly zone pruning shrinks the scanned range. Under the Dictionary layout, bound terms are first translated to their `u32` codes through the cached dictionary; a term absent from the dictionary short-circuits to an empty result on either backend.

## Mutations

`VortexRdfStore` never rewrites its data in place to answer a mutation. Instead it follows a [**merge-on-read** pattern](https://iceberglakehouse.com/iceberg/iceberg-merge-on-read/): the store you built or opened (the "base") stays exactly as it was written — sorted order, secondary indexes, file bytes and all — and mutations are layered on top of it as two lightweight side-structures, (i) an append-only [**Tail**](https://github.com/vortex-rdf/vortex-rdf/blob/ff9add07c6ba855abb30d12f407777778de562de/core/src/store/vortex_rdf_store.rs#L97) for additions and; (ii)  **Tombstone masks** (for [in-memory](https://github.com/vortex-rdf/vortex-rdf/blob/ff9add07c6ba855abb30d12f407777778de562de/core/src/store/mod.rs#L121) and [file-backed](https://github.com/vortex-rdf/vortex-rdf/blob/ff9add07c6ba855abb30d12f407777778de562de/core/src/store/mod.rs#L160) stores) for deletions. Reads transparently merge base + tail, minus tombstones, so the store still behaves as one dataset; nothing is actually rewritten, and none of the base's row ids ever move, which is what lets secondary indexes (whose `_idx_*_rid` columns address base row ids) and any in-flight views survive a mutation. 

All of this is virtual: nothing is written back to the original Vortex file unless a compaction takes place.

#### Additions: the append Tail

`add_quad`/`add_quads` never touch the base — they append into a second, in-memory array kept beside it, the **Tail** (the write-optimized delta half of the design, with the base as its read-optimized main half):

- The tail is always **one flat, contiguous `StructArray`**, never a chain of per-add chunks: each `add_quads` call gathers the tail's current live rows plus the new ones and rebuilds it in one shot, so a scan over the tail never has to walk fragments.
- Quads already present are skipped (as per RDF/JS set semantics) — an in-batch `HashSet` catches duplicates within the call, and each remaining quad is checked with `contains()` against the store (base + existing tail).
- The tail has its own `RowSelection` and its own `deleted` mask, in **tail-local ids** (`0..rows.len()`), entirely separate from the base's — a view can narrow or tombstone the tail independently of the base it sits beside.
- Works under every layout, including **Dictionary**: an appended term has no code in the base's sorted dictionary, so the tail stores plain Default-layout N-Triples strings instead of `u32` codes. Pattern matching probes the base by dictionary code and the tail by string, and a query that touches both unions the results.
- `match_pattern` runs the base's normal routing (binary search / index / mask scan) and a mask scan over the tail independently, then unions the two — base short-circuits (e.g. `AlwaysFalse` from a Dictionary miss) don't skip the tail, since a term absent from the base's dictionary may still exist in the tail's plain strings.

#### Deletions: tombstone masks

`delete_quad`/`delete_matching` never remove or rewrite rows either — they mark them dead:
- Both calls reuse `match_pattern` to find the doomed rows, then fold that set into a `deleted: Option<Mask>` — one bit per row, `None` until the first delete — carried beside the base (and, separately, beside the tail, since each has its own row-id space). A later delete unions into the existing mask, so it composes for free with rows already tombstoned.
- The base (or the file) itself, and any secondary index built over it, is left completely untouched — a tombstone costs a bit per row, not a copy of the surviving data.
- `match_pattern` deliberately does **not** consult tombstones when it computes a selection (that keeps its row positions aligned for mask-based refinement); every *read* path does — `size`, `quads`, and friends all subtract the tombstones (`live_mask`/`gather_live`) before rows reach the caller.
- File-backed stores tombstone the same way (a file can't be rewritten on delete), and the mask is applied **inside the scan** itself — as an `ExcludeByIndex` selection, or subtracted up front from an id list — so it composes with a pushed-down filter instead of post-filtering the filter's output (which would carry no row ids left to re-align against).
- Tombstoned rows are only reclaimed by compaction; until then they still occupy physical storage.

#### Compaction

`compact()` (keep the current index set) / `compact_with_indexes(indexes)` (rebuild a chosen set) are the only operations that actually rewrite data. A compaction:
1. Reads every *live* row the store's view covers — base rows first, then tail rows, tombstones already excluded (`live_raw_quads`).
2. Sorts them by `(s, p, o, g)`.
3. Rebuilds a fresh base through the normal builder pipeline (a fresh `TermDictionary` under the Dictionary layout, since the tail may hold terms the old dictionary never assigned a code to), stamping `s` as sorted.
4. Rebuilds the requested secondary indexes over the new order.

The result is a store with an empty tail, no tombstones, and — because it's freshly sorted — the subject binary-search fast path restored, even if the view being compacted had lost it (e.g. a tail, or a narrowed match result). A **file-backed** store stays file-backed: the rebuilt array is written to a temp sibling file and atomically renamed over the original path, then reopened.

**Auto-compaction policy**: `add_quads` is append-then-check — the append itself is policy-free, and whichever call pushes the tail past a threshold pays for folding it back into the base, amortizing the O(n log n) rebuild cost to roughly constant per appended row (the same argument as a dynamic array's growth factor). The tail is folded once it reaches:
- **one builder chunk** — 100,000 rows — regardless of how large the base is, since the tail is the store's only unindexed, unsorted region: every query mask-scans it and every append rebuilds it, so past this size it would dominate an otherwise index-routed lookup on a large base; or
- **a tenth of the base**, with a **4,096-row floor** so a small store isn't compacted every few appends.

This applies equally to in-memory **and file-backed** stores — a file-backed store past the threshold pays for a disk write (rewriting its source file in place, as above) as part of the `add_quads` call.

---

## Data Representation Example

Given a sample RDF dataset, serialized in the Turtle format:

```turtle
ex:alice a foaf:Person ;
         foaf:name "Alice" .
ex:bob a foaf:Person ;
       foaf:knows ex:alice .
```

### `Default` layout

With a sorted builder, the quads are stored as four string columns, globally ordered by `S → P → O → G`:

| s | p | o | g |
|---|---|---|---|
| `<http://example.org/alice>` | `<…rdf-syntax-ns#type>` | `<…foaf/0.1/Person>` | `""` |
| `<http://example.org/alice>` | `<…foaf/0.1/name>` | `"Alice"` | `""` |
| `<http://example.org/bob>` | `<…rdf-syntax-ns#type>` | `<…foaf/0.1/Person>` | `""` |
| `<http://example.org/bob>` | `<…foaf/0.1/knows>` | `<http://example.org/alice>` | `""` |

Vortex compresses the repeated strings internally (FSST, dictionary encodings), and the sorted `s` column supports binary-search lookups.

### `Dictionary` layout

**Term dictionary** (`_dict_terms`): the lexicographically sorted set of unique terms. IDs are implicit — a term's ID is simply its position:

| ID* | Term |
|---|---|
| 0 | `""` (default graph) |
| 1 | `"Alice"` |
| 2 | `<http://example.org/alice>` |
| 3 | `<http://example.org/bob>` |
| 4 | `<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>` |
| 5 | `<http://xmlns.com/foaf/0.1/Person>` |
| 6 | `<http://xmlns.com/foaf/0.1/knows>` |
| 7 | `<http://xmlns.com/foaf/0.1/name>` |

> *IDs are not actually stored; they are implicit in the sorted position. Looking up a term's ID is a binary search; looking up an ID's term is a positional read. Both operate on zero-copy views.

**Quad columns** (u32 codes, sorted by `S → P → O → G` — note that because IDs are sorted ranks, sorting by code equals sorting by term string):

| s | p | o | g | |
|---|---|---|---|---|
| 2 | 4 | 5 | 0 | *(alice, type, Person)* |
| 2 | 7 | 1 | 0 | *(alice, name, "Alice")* |
| 3 | 4 | 5 | 0 | *(bob, type, Person)* |
| 3 | 6 | 2 | 0 | *(bob, knows, alice)* |

### Adding `SecondaryByReference` indexes

Four extra columns hold the objects and predicates in sorted order, each paired with the row ID it came from (shown here with Dictionary-layout codes; under the other layouts the `_val` columns hold the term strings instead):

| s | p | o | g | _idx_o_val | _idx_o_rid | _idx_p_val | _idx_p_rid |
|---|---|---|---|---|---|---|---|
| 2 | 4 | 5 | 0 | 1 | 1 | 4 | 0 |
| 2 | 7 | 1 | 0 | 2 | 3 | 4 | 2 |
| 3 | 4 | 5 | 0 | 5 | 0 | 6 | 3 |
| 3 | 6 | 2 | 0 | 5 | 2 | 7 | 1 |

Example object-only query `(?, ?, ex:alice, ?)`:
1. Translate `<http://example.org/alice>` to its code: `2`.
2. Binary-search `_idx_o_val` for `2` → position 1.
3. `_idx_o_rid[1] = 3` → row 3: *(bob, knows, alice)*.

---

## Installation

### Rust
Declare `vortex-rdf-core` as a dependency as follows:

```toml
[dependencies]
vortex-rdf-core = "0.1.0"
```

The `file-io` feature (enabled by default) provides Vortex file reading/writing on top of Tokio; disable default features for WASM or IPC-only environments.

Install the CLI with:

```bash
cargo install vortex-rdf-cli
```

### JavaScript/WebAssembly 

```bash
npm install @vortex-rdf/vortex-rdf-store
```

See [js/README.md](js/README.md) for more details.

---

## Usage

### Rust API

- **Building a Vortex file and querying it:**

```rust
use std::fs::File;
use oxrdf::NamedNode;
use oxrdfio::RdfFormat;
use vortex_rdf_core::{
    VortexRdfStore, SortedInMemoryBuilder, LayoutStrategy, IndexType,
    io::{deserialize, quads_stream_to_vortex_writer_with_builder},
    common::utils::parse_quads_from_reader,
};

// 1. Parse an RDF file into a stream of quads
let input = File::open("data.ttl")?;
let quads = parse_quads_from_reader(input, RdfFormat::Turtle);

// 2. Stream the quads into a Vortex file, choosing a builder (type parameter),
//    a column layout, and optional secondary indexes. Streaming-capable
//    builders never materialize the full dataset in memory.
let writer = tokio::fs::File::create("data.vortex").await?;
quads_stream_to_vortex_writer_with_builder::<SortedInMemoryBuilder, _, _>(
    quads,
    writer,
    LayoutStrategy::Default,
    vec![IndexType::SecondaryByReference],
).await?;

// 3. Open the file lazily (zero-copy: nothing is read until queried)
let store = VortexRdfStore::from_file("data.vortex").await?;

// 4. Pattern matching — filters are pushed down into the file scan
let knows = NamedNode::new("http://xmlns.com/foaf/0.1/knows")?;
let filtered = store.match_pattern(None, Some(&knows), None, None).await?;
println!("Found {} matches", filtered.size().await?);

// 5. Deserialize back to a traditional RDF format
let output = File::create("filtered.nq")?;
deserialize(filtered, output, RdfFormat::NQuads).await?;
```

- **Building an in-memory store and mutating it**:

```rust
// In-memory build (default configuration: UnsortedStream builder,
// Default layout, no indexes)
let quads = parse_quads_from_reader(File::open("data.ttl")?, RdfFormat::Turtle);
let array = VortexRdfStore::build_vortex_array(quads).await?;
let store = VortexRdfStore::new(array)?;

// Or with explicit builder / layout / indexes:
let array = VortexRdfStore::build_vortex_array_with_builder::<SortedInMemoryBuilder>(
    quads, LayoutStrategy::Dictionary, vec![IndexType::SecondaryByReference],
).await?;

// Mutations return derived stores and are virtual — they are only
// persisted back into the original Vortex file via compaction.

// Add Quad: appended into an in-memory tail beside the base,
// so the base's row ids and secondary indexes stay valid.
let mutated = store.add_quad(new_quad).await?;

// Delete Quad: inverse vectorized columnar filter.
let cleaned = mutated.delete_quad(&new_quad).await?;
```

### Command Line Interface
```bash
# Convert Turtle to Vortex
# (defaults: unsorted-stream builder, default layout, no indexes)
vortex-rdf-cli serialize --input test.ttl --output test.vortex

# Out-of-core globally sorted build with dictionary layout and secondary indexes
vortex-rdf-cli serialize --input big.nq --output big.vortex \
  --builder-strategy sorted-stream \
  --layout dictionary \
  --indexes secondary-by-reference

# Available options:
#   --builder-strategy  unsorted-stream | sorted-in-memory | sorted-stream
#   --layout            default | typed-object | dictionary
#   --indexes           secondary-by-copy | secondary-by-reference (repeatable)

# Convert Vortex back to RDF (output defaults to N-Quads on stdout)
vortex-rdf-cli deserialize --input test.vortex
vortex-rdf-cli deserialize --input test.vortex --format jsonld

# Pattern matching / filtering
vortex-rdf-cli match --input test.vortex --predicate "http://example.org/p1"
vortex-rdf-cli match --input test.vortex --subject "http://example.org/s1" --output filtered.nq

# Enable debug logging (shows timing metrics)
RUST_LOG=vortex_rdf_cli=debug,vortex_rdf_core=debug vortex-rdf-cli serialize --input data.ttl --output data.vortex
```
