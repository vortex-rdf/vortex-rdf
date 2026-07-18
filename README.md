# Vortex-RDF
[![CodSpeed Badge](https://img.shields.io/endpoint?url=https://codspeed.io/badge.json)](https://codspeed.io/julianrojas87/vortex-rdf?utm_source=badge)
[![CI](https://github.com/julianrojas87/vortex-rdf/actions/workflows/ci.yml/badge.svg)](https://github.com/julianrojas87/vortex-rdf/actions/workflows/ci.yml)

Vortex-RDF is a columnar RDF serialization built on top of the [Vortex](https://docs.vortex.dev) data format. It combines the flexible graph-based model of RDF with the efficiency of modern columnar data formats. Its main goal is to **provide a compact, zero-copy and high-performance serialization format for exchanging and read/write RDF data**.

This library provides both serialization and deserialization capabilities for converting traditional RDF formats (everything supported by [`oxrdfio`](https://docs.rs/oxrdfio/latest/oxrdfio/)) to Vortex-RDF and vice-versa. It also provides a queryable RDF quad store (`VortexRdfStore`) with pattern matching, exposed to JavaScript/WASM through an interface modeled after the [RDF-JS specification](https://rdf.js.org/dataset-spec/#datasetcore-interface).

## Key Features

- 📊 **Advanced Columnar Storage**: Leverages [Vortex format specifications](https://docs.vortex.dev/specs/file-format) for flexible arrays organized in columnar layouts, both on disk and in memory.
- ♻️ **Zero-Copy**: Vortex-RDF is built on a ["Zero-Copy" philosophy](https://en.wikipedia.org/wiki/Zero-copy). This means that after the RDF data is serialized into the Vortex format, it can be read, filtered, and queried without ever moving or copying the bytes in memory.
- 📦 **Adaptive Compression**: Smart compression strategies can be applied based on the [BtrBlocks approach](https://www.cs.cit.tum.de/fileadmin/w00cfj/dis/papers/btrblocks.pdf) [1], which provides a sophisticated multi-level compression system that adaptively selects optimal compression schemes based on data characteristics. These include e.g., [Fast Static Symbol Table (FSST)](https://doi.org/10.14778/3407790.3407851) [2], [Run-Length Encoding (RLE)](https://en.wikipedia.org/wiki/Run-length_encoding), [BitPacking](https://doi.org/10.1002/spe.2326) [3] among others.
- 🌊 **Streaming & Out-of-Core Ingestion**: Quads can be streamed directly into a Vortex file in fixed-size chunks with bounded memory, and datasets larger than RAM can be globally sorted via an external merge sort that spills sorted runs to disk.
- 🍀 **RDF Quads Support**: Full support for named Graphs `(S, P, O, G)` and in general for [RDF 1.1](https://www.w3.org/TR/rdf11-concepts/).
- 🌍 **Cross-Platform**: Native Rust library with a CLI + WebAssembly (WASM) bindings for browsers/Node.js. Python bindings coming soon.

#### How it works:
1. **Zero-copy buffer views**: When you want to access a specific column (e.g., just the `predicates`) or a specific subset of Quads, Vortex creates a [_Layout_](https://docs.vortex.dev/concepts/layouts) either from a Vortex file stored on disk or from Vortex encoded data in memory. This view is just a pointer and some metadata, it doesn't duplicate the data.
2. **Lazy Decompression**: Even when compressed, Vortex is designed to decompress data "_just-in-time_" at the CPU register level, while leveraging [SIMD optimizations](https://en.wikipedia.org/wiki/Single_instruction,_multiple_data) and avoiding the creation of temporary intermediate strings.

### Vortex File format & IPC

Vortex-RDF leverages the [Vortex File specification](https://docs.vortex.dev/specs/file-format) and the [Vortex IPC (Inter-Process Communication) protocol](https://docs.rs/vortex-ipc/latest/vortex_ipc/) to provide versatile serialization options optimized for both local storage and remote data exchange.

1. **Vortex Files**: Zero-Copy
The `.vortex` files are optimized for **local usage** with disk-based storage (Cloud-based alternatives based on blob storage solutions, e.g., Amazon S3 buckets, could be also supported via technologies such as [Apache Iceberg](https://iceberg.apache.org/)). These files are designed to allow efficient compression and random access, allowing the OS to load only necessary chunks on demand without any parsing overhead. Opening a file (`VortexRdfStore::from_file`) is lazy: no data is read until queried, and `match_pattern` filters are pushed down into the file scan as Vortex filter expressions.

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

Appended quads cannot be dictionary-encoded (a new term has no code in the sorted dictionary), so on Dictionary-layout stores they live in the in-memory string tail: patterns probe the base by code and the tail by string, and compaction (`compact_with_indexes`) re-encodes everything against a fresh dictionary.

### 2. Secondary Indexes

Indexes are opt-in, embedded as extra columns at build time. Two types are supported:

#### `SecondaryByReference`
Sorted permutation indexes for the predicate and object columns, enabling binary-search routing for predicate-only and object-only patterns without re-sorting the whole dataset:

| Column | Content |
|---|---|
| `_idx_o_val` | All object values in sorted order (`VarBin<Utf8>`; `u32` codes under the Dictionary layout) |
| `_idx_o_rid` | Row ID (`u32`) of the quad each sorted object value came from |
| `_idx_p_val` | All predicate values in sorted order (same dtype rules as above) |
| `_idx_p_rid` | Row ID (`u32`) of the quad each sorted predicate value came from |

Binary-search routing only engages when the value columns carry the `IsSorted` statistic, which builders stamp when the columns hold a *globally* sorted order (single-chunk builds, or the sorted builders' global index emission). Indexes are build-time only: stores derived by slicing, filtering, or mutation strip the index columns, because their row IDs would be stale.

#### `SecondaryByCopy`
Two complete extra copies of the quad columns — the classic triple-store permutation indexes (POS/OSP), adapted to quads — each paired with the primary row IDs it permutes. This gives predicate- and object-bound patterns the same sorted-column access path the primary `(s, p, o, g)` order gives subjects:

| Columns | Content |
|---|---|
| `_idx_posg_{s,p,o,g}` | The quads re-sorted by `(p, o, s, g)` (term strings, or `u32` codes under the Dictionary layout) |
| `_idx_posg_rid` | Row ID (`u32`) of the primary quad each copy row mirrors |
| `_idx_ospg_{s,p,o,g}` | The quads re-sorted by `(o, s, p, g)` |
| `_idx_ospg_rid` | Row ID (`u32`) of the primary quad each copy row mirrors |

Predicate-bound patterns binary-search `_idx_posg_p`; a bound predicate **and** object resolve in one `(p, o)` *prefix* search; object-bound patterns binary-search `_idx_ospg_o`. On file-backed stores the copies additionally let `quads()` stream matched rows straight from the copy family — where they sit in a contiguous, zone-prunable run — instead of scattering row-ID reads across the primary columns, mirroring the subject fast path's locality. The same `IsSorted` stamping rules as `SecondaryByReference` apply for in-memory binary-search routing.

Compared to `SecondaryByReference`, this costs roughly 2× the primary columns in extra storage (before compression; the sorted copies compress well) in exchange for contiguous reads on predicate/object patterns — choose it when those reads dominate. When both index types are embedded, `SecondaryByCopy` is preferred at query time.

### 3. Ingestion Builders

| Builder | Memory model | Sorting | Disk spill | Result |
|---|---|---|---|---|
| **`UnsortedStream`** (default) | Streaming / bounded by chunk size | None (insertion order) | Only for the Dictionary layout* | Chunks of ≤500K quads |
| **`SortedInMemory`** | Full dataset in memory | Global `S → P → O → G` | No | Globally sorted array + globally sorted index columns |
| **`SortedStream`** | Out-of-core, bounded by chunk size | Global `S → P → O → G` | Yes | Globally sorted chunks + globally sorted index columns |

- **`UnsortedStreamBuilder`**: The simplest and fastest pipeline. It preserves the exact ordering of the incoming RDF stream and emits fixed-size chunks lazily; when serializing to a file, the Vortex writer compresses and flushes each chunk as it arrives, so peak memory is bounded by the chunk size instead of the dataset size. It cannot leverage subject ordering for binary-search pruning.
- **`SortedInMemoryBuilder`**: Loads all quads in memory and performs a global sort by `(s, p, o, g)`. Requested secondary index columns are built once over the whole dataset and emitted in global order (stamped `IsSorted`), so `match_pattern` can binary-search subjects, predicates, and objects. Best suited for small-to-medium graphs that fit in RAM.
- **`SortedStreamBuilder`**: External merge sort for datasets larger than memory. Quads are ingested in bounded batches, sorted locally, and spilled to disk as sorted runs; a K-way heap merge then emits globally sorted, fixed-size chunks. When secondary indexes are requested, a second external sort over `(value, row ID)` pairs produces globally sorted index columns as well. Spill files are internal, length-prefixed `rkyv` records under `target/` and are cleaned up automatically.

\* The Dictionary layout always needs two passes (the global dictionary is only complete after ingesting the whole stream), so even the unsorted builder spills raw quads to disk and re-reads them for encoding.

### 4. The Store & Query Routing

`VortexRdfStore` wraps either an in-memory array or a lazily-scanned Vortex file. `match_pattern(s?, p?, o?, g?)` resolves patterns with a routing cascade:

1. **Subject binary search**: if a subject is bound and the `s` column is stamped `IsSorted`, the matching row range is found via binary search and sliced — no scan at all.
2. **Index routing**: if `SecondaryByCopy` columns are present (and globally sorted), predicate-bound patterns binary-search `_idx_posg_p`, object-bound patterns `_idx_ospg_o`, and predicate+object patterns resolve both components in one `(p, o)` prefix search; on file-backed stores the matched rows are then *streamed from the copy family itself* (a contiguous run) rather than gathered by row ID. Otherwise, if `SecondaryByReference` columns are present, object-only and predicate-only patterns binary-search `_idx_*_val` and `take` the referenced rows.
3. **Vectorized mask scan**: remaining constraints are resolved with columnar equality masks (SIMD-friendly, no string materialization).

For **file-backed stores**, patterns compile to Vortex filter expressions pushed down into the lazy scan, which benefits from zone-map pruning and minimal column projection. Under the Dictionary layout, bound terms are first translated to their `u32` codes through the cached dictionary; a term absent from the dictionary short-circuits to an empty result.

**Mutations** are virtual (in-memory only, never persisted back to the original file):
- `add_quad`/`add_quads` append into an in-memory tail beside the base (never rewriting it), so secondary indexes survive appends; quads already present are skipped, per RDF/JS set semantics. Works on every layout, including Dictionary (the tail stores strings).
- `delete_quad`/`delete_matching` tombstone the matched rows with a bitmask; the data is untouched and indexes stay valid.
- `compact` / `compact_with_indexes` reclaim tombstones, fold the tail in, re-sort by (s, p, o, g), and rebuild the store's current (or a chosen) index set.
- In-memory stores **auto-compact** when an append leaves the tail past a tenth of the base (floor: 4,096 rows) or past one builder chunk (100K rows), whichever comes first — amortized-constant cost per added row. File-backed stores never auto-compact (folding would pull the file into memory); watch `tail_len()` and call `compact()` deliberately.

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

## How Datatypes are Handled

Vortex-RDF ensures 100% fidelity for all [XSD datatypes](https://www.w3.org/TR/xmlschema-2/) (numeric, boolean, datetime, etc.) by leveraging [N-Triples Canonicalization](https://www.w3.org/TR/n-triples/#canonical-ntriples). Every Literal is stored using its canonical N-Triples string representation. For example, the integer `42` is stored as `"42"^^<http://www.w3.org/2001/XMLSchema#integer>`.

Because RDF datatypes are highly repetitive (e.g., thousands of numbers sharing the same `XMLSchema#integer` suffix), applying e.g., **FSST compression** leads to:
1. Identifying common datatype suffixes as **Frequent Symbols**.
2. Replacing these long IRI strings with **1-byte codes**.
3. Cheap reconstruction of the full string during zero-copy reads.

This provides the storage efficiency of native types while maintaining the flexibility to store any arbitrary or custom RDF term.

#### Towards Type Lifting

The `TypedObject` layout is the first step in this direction: it already separates the literal value from its datatype and language tag into dedicated columns. Full "Type Lifting" — moving common analytical types (integers, floats, timestamps) into native Vortex columns (e.g., `PrimitiveArray`, `DateTimeArray`) to enable range queries and mathematical operations pushed down to the compressed columnar data — remains future work.

---

## Installation

### Rust
Add to your `Cargo.toml`:
```toml
[dependencies]
vortex-rdf-core = { "TODO: Publish on crates.io" }
```

The `file-io` feature (enabled by default) provides Vortex file reading/writing on top of Tokio; disable default features for WASM or IPC-only environments.

For the time being you may clone this repo and compile the CLI with:

```bash
cargo build --release -p vortex-rdf-cli
# The binary will be at ./target/release/vortex-rdf-cli
```

### WebAssembly (WASM)
Build for JS environments using `wasm-pack`:
```bash
cd js
npm run build:node # for Node.js
npm run build:web  # for the Browser
```
> TODO: Publish on npm

---

## Usage

### Rust API
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

Building an in-memory store and mutating it:

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

// Mutations return derived stores and are virtual — they are never
// persisted back into the original Vortex file.

// Add Quad: appended into an in-memory tail beside the base,
// so the base's row ids and secondary indexes stay valid.
let mutated = store.add_quad(new_quad).await?;

// Delete Quad: inverse vectorized columnar filter.
let cleaned = mutated.delete_quad(&new_quad).await?;
```

### JavaScript / WASM

```javascript
import { VortexStore, init_panic_hook, nquads_to_vortex, vortex_to_nquads } from './pkg/vortex_rdf.js';

init_panic_hook();

// Convert an N-Quads string to Vortex IPC bytes and back.
// The optional builder strategy is 'Unsorted' (default) or 'Sorted'.
const bytes = await nquads_to_vortex(nquadsString, 'Sorted');
const restored = await vortex_to_nquads(bytes);
```

The `VortexStore` class follows the [RDF-JS DatasetCore](https://rdf.js.org/dataset-spec/#datasetcore-interface) and [Data Model](https://rdf.js.org/data-model-spec/) shapes, with all methods returning Promises:

```javascript
// Create a store from Vortex IPC bytes or from an RDF string
const store = await VortexStore.fromBytes(vortexBytes);
// const store = await VortexStore.fromString(turtleString, "turtle", "Sorted");
// const store = VortexStore.empty();

console.log(`Loaded ${await store.size()} quads`);

// Query using an RDF-JS match() pattern: match(subject?, predicate?, object?, graph?)
const matches = await store.match(null, namedNode, null, null);

// Iterate over results
for (const quad of await matches.values()) {
  console.log(quad.subject.value, quad.predicate.value, quad.object.value);
}

// Membership and mutations (virtual, in-memory)
await store.has(someQuad);
await store.addQuad(someQuad);
await store.deleteQuad(otherQuad);
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

---

## Benchmarking with CodSpeed

Vortex-RDF features a benchmark suite built on top of [Divan](https://github.com/nvzqz/divan) and fully integrated with [CodSpeed](https://codspeed.io) for precise, CPU-instruction-level performance tracking.

The consolidated **`benchmark`** target evaluates the full supported matrix of:
* Builder strategies: `SortedInMemory`, `SortedStream`, `UnsortedStream`
* Layout strategies: `Default`, `TypedObject`, `Dictionary`
* Workloads: serialization and file-backed `match_pattern` queries (`s`, `p`, `o`, `g`)

### Running Benchmarks Locally

#### 1. Fast Sanity Check (Test Mode)
Running full statistical loops can take several minutes due to heavy sampling. You can execute all benchmarks **exactly once** in test-mode to instantly verify correctness:
```bash
cargo test --bench benchmark
```

#### 2. Run Full Statistical Benchmarks
To run the full Divan statistical profiling loops:
```bash
cargo bench --bench benchmark
```

#### 3. Filtering Benchmarks
You can isolate specific builders or query shapes:
```bash
# Profile only match pattern benchmarks
cargo bench --bench benchmark -- match_pattern

# Profile only subject-selective patterns
cargo bench --bench benchmark -- _s

# Profile a specific builder
cargo bench --bench benchmark -- sorted_stream
```

### Store File Caching
To prevent redundant re-builds, the suite keeps a thread-safe global cache of serialized `.vortex` files (under `target/bench_vortex_files`), keyed by builder, layout, and dataset size. Each store is built and serialized once; all `match_pattern_*` benchmarks reuse the cached file, isolating query performance from ingestion overhead and keeping CodSpeed telemetry clean and noise-free.

---

## References

[1] Maximilian Kuschewski, David Sauerwein, Adnan Alhomssi, and Viktor Leis. 2023. BtrBlocks: Efficient Columnar Compression for Data Lakes. Proc. ACM Manag. Data, June 2023. https://doi.org/10.1145/3589263

[2] Peter Boncz, Thomas Neumann, and Viktor Leis. FSST: Fast Random Access String Compression. Proc. VLDB Endow., 13(12):2649–2661, July 2020. https://doi.org/10.14778/3407790.3407851.

[3] Daniel Lemire, Leonid Boytsov and Nathan Kurz. SIMD compression and the intersection of sorted integers. 2015. https://doi.org/10.1002/spe.2326
