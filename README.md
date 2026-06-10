# Vortex-RDF
[![CodSpeed Badge](https://img.shields.io/endpoint?url=https://codspeed.io/badge.json)](https://codspeed.io/julianrojas87/vortex-rdf?utm_source=badge)

Vortex-RDF is a columnar RDF serialization built on top of the [Vortex](https://docs.vortex.dev) data format. It combines the flexible graph-based model of RDF with the efficiency of modern columnar data formats. Its main goal is to **provide a compact, zero-copy and high-performance serialization format for exchanging and read/write RDF data**.

This library provides both serialization and deserialization capabilities for converting traditional RDF formats (everything supported by [`oxrdfio`](https://docs.rs/oxrdfio/latest/oxrdfio/)) to Vortex-RDF and vice-versa. It also provides implementations of RDF stores, based on the [RDF-JS specification](https://rdf.js.org/dataset-spec/#datasetcore-interface). 

## Key Features

- 📊 **Advanced Columnar Storage**: Leverages [Vortex format specifications](https://docs.vortex.dev/specs/file-format) for flexible arrays organized in columnar layouts, both on disk and in memory.
- ♻️ **Zero-Copy**: Vortex-RDF is built on a ["Zero-Copy" philosophy](https://en.wikipedia.org/wiki/Zero-copy). This means that after the RDF data is serialized into the Vortex format, it can be read, filtered, and queried without ever moving or copying the bytes in memory.
- 📦 **Adaptive Compression**: Smart compression strategies can be applied based on the [BtrBlocks approach](https://www.cs.cit.tum.de/fileadmin/w00cfj/dis/papers/btrblocks.pdf) [1], which provides a sophisticated multi-level compression system that adaptively selects optimal compression schemes based on data characteristics. These include e.g., [Fast Static Symbol Table (FSST)](https://doi.org/10.14778/3407790.3407851) [2],  [Run-Length Encoding (RLE)](https://en.wikipedia.org/wiki/Run-length_encoding), [BitPacking](https://doi.org/10.1002/spe.2326) [3] among others.
- 🍀 **RDF Quads Support**: Full support for named Graphs `(S, P, O, G)` and in general for [RDF 1.1](https://www.w3.org/TR/rdf11-concepts/).
- 🌍 **Cross-Platform**: Native Rust library with a CLI + WebAssembly (WASM) bindings for browsers/Node.js. Python bindings coming soon.

#### How it works:
1. **Zero-copy buffer views**: When you want to access a specific column (e.g., just the `predicates`) or a specific subset of Quads, Vortex creates a [_Layout_](https://docs.vortex.dev/concepts/layouts) either from a Vortex file stored on disk or from Vortex encoded data in memory. This view is just a pointer and some metadata, it doesn't duplicate the data.
2. **Lazy Decompression**: Even when compressed, Vortex is designed to decompress data "_just-in-time_" at the CPU register level, while leveraging [SIMD optimizations](https://en.wikipedia.org/wiki/Single_instruction,_multiple_data) and avoiding the creation of temporary intermediate strings.

### Vortex File format & IPC

Vortex-RDF leverages the [Vortex File specification](https://docs.vortex.dev/specs/file-format) and the [Vortex IPC (Inter-Process Communication) protocol](https://docs.rs/vortex-ipc/latest/vortex_ipc/) to provide versatile serialization options optimized for both local storage and remote data exchange.

1. **Vortex Files**: Zero-Copy
The `.vortex` files are optimized for **local usage** with disk-based storage (Cloud-based alternatives based on blob storage solutions, e.g., Amazon S3 buckets, could be also supported via technologies such as [Apache Iceberg](https://iceberg.apache.org/). These files are designed to allow efficient compression and random access, allowing the OS to load only necessary chunks on demand without any parsing overhead.

2. **IPC Streams**: Remote Exchange
For exchanging data between different systems or over a network, the library can serialize RDF graphs into a **Vortex IPC Stream**. This format follows the Vortex IPC streaming protocol, making it suitable for pipes, sockets, and network transfers. These streams can be consumed by any Vortex-compatible client (Rust, Python, C++, etc.) to receive the Vortex-RDF data, while avoiding any deserialization and decompression overhead.

Both formats share the same underlying principles:
- **Self-Describing**: Every file/stream contains a FlatBuffers schema describing the set of [`DType`](https://docs.vortex.dev/concepts/dtypes) (data types).
- **Unified Encodings**: Specialized encodings are preserved verbatim. This means compressed data **stays compressed** during transfer and is only decompressed lazily when strictly needed by the consumer.

This versatile approach ensures that Vortex-RDF can serve as both a high-performance local database engine and an efficient interchange format for distributed RDF processing.

---

## Architecture

Vortex-RDF encodes RDF quads using three main architectural concepts:

### 1. Resource Dictionary   
A data structure that encodes all IRIs, Literals, and Blank Nodes. Multiple encoding strategies are possible. Currently we support 2 types:

#### **Simple Dictionary Index**
The entire set of RDF Terms (IRIs, Blank Nodes and Literals) are persisted as a single FSST-compressed [`VarBinViewArray`]https://docs.rs/vortex/latest/vortex/array/arrays/type.VarBinViewArray.html) storing all the unique term strings.
* **Pros**: Easier and faster to create.
* **Cons**: Requires an external auxiliary data structure (e.g., in-memory `HashMap`) to allow efficient look ups (Term -> ID), which goes against the zero-copy principle. 

#### **Chained Hash Index**
Persists the entire set of RDF Terms as [chained hash map](https://en.wikipedia.org/wiki/Hash_table#Separate_chaining) using 2 additional Vortex arrays alongside a simple dictionary:
1. **`Terms`**: Same as above (strings), stored as a compressed `VarBinViewArray`.
2. **`buckets`**: A fixed-size [`PrimitiveArray<i32>`](https://docs.rs/vortex/latest/vortex/array/arrays/struct.PrimitiveArray.html) acting as the hash table entry point.
3. **`next`**: A `PrimitiveArray<i32>` acting as the linked list for collision resolution.

* **Pros**: Able to fully encode the set of unique RDF terms into a Vortex structure, including an index that allows for efficient lookups, while upholding the zero-copy principle.
* **Cons**: Higher complexity for creation and lookups.   

### 2. Quad Collection  
The graph structure is stored as a [`StructArray`](https://docs.rs/vortex/latest/vortex/array/arrays/struct.StructArray.html) of indices. By default, quads are sorted by **`Subject -> Predicate -> Object -> Graph`** during ingestion, allowing direct and efficient binary searches on the subject column (**`s`**). 

To optimize predicate and object lookups without sorting the entire dataset multiple times, the `StructArray` optionally includes four flat secondary permutation index columns:
- **`s`** (Subject ID): `PrimitiveArray<u32>`.
- **`p`** (Predicate ID): `PrimitiveArray<u32>`.
- **`o`** (Object ID): `PrimitiveArray<u32>`.
- **`g`** (Graph ID): `PrimitiveArray<u32>`.
- **`_idx_o_val`** (Object Index Values - optional): A sorted `PrimitiveArray<u32>` listing all object IDs.
- **`_idx_o_rid`** (Object Row Mappings - optional): A `PrimitiveArray<u32>` mapping each entry in `_idx_o_val` back to its original quad row offset.
- **`_idx_p_val`** (Predicate Index Values - optional): A sorted `PrimitiveArray<u32>` listing all predicate IDs.
- **`_idx_p_rid`** (Predicate Row Mappings - optional): A `PrimitiveArray<u32>` mapping each entry in `_idx_p_val` back to its original quad row offset.

### 3. Ingestion Builders (Indexing Strategies)

To compile the resource dictionary and quad collection into a highly compressed Vortex representation, Vortex-RDF supports **four distinct ingestion builders** depending on dataset size, RAM constraints, and target query requirements:

| Ingestion Strategy | Memory Model | Sorting | Disk Spill | Layout Structure | Indexed |
|---|---|---|---|---|---|
| **`UnsortedInMemory`** | In-Memory | No | No | Flat `StructArray` | No |
| **`SortedInMemory`** | In-Memory | Global | No | Flat `StructArray` | Ordered by `subject` with secondary indexes by reference on `predicate` and `object` |
| **`UnsortedStream`** | Out-of-Core | No | Yes | `ChunkedArray` of unsorted `StructArray` blocks | No |
| **`SortedStream`** | Out-of-Core | Global | Yes | `ChunkedArray` of globally sorted `StructArray` blocks | Ordered by `subject` with secondary indexes by reference on `predicate` and `object` |

#### Builder Details:
* **`UnsortedInMemoryBuilder`**: The simplest pipeline. It preserves the exact ordering of the incoming RDF stream, loading all data to build a single, flat `StructArray`. It adds minimal overhead, but cannot leverage Vortex's native zone-map statistics for efficient pruning during query execution.
* **`SortedInMemoryBuilder`**: Loads all quads in memory, performs a global sort on the quads, and appends global flat secondary indexes (`_idx_o_val`, `_idx_o_rid`, `_idx_p_val`, `_idx_p_rid`) to optimize `match_pattern` search. Best suited for queries on small-to-medium graphs.
* **`UnsortedStreamBuilder` (Out-of-Core)**: It ingests data in memory-bounded batches and flushes them directly to disk chunks without sorting to maximize ingestion speed, producing a `ChunkedArray` layout.
* **`SortedStreamBuilder` (Out-of-Core)**: It ingests data in memory-bounded batches, sorts them locally, and serializes sorted runs to disk (`runs`) to prevent memory exhaustion. It then performs an out-of-core heap-merge of the sorted runs and flushes them into sorted, indexed thin chunks wrapped in a final `ChunkedArray` layout.

---

## Data Representation Examples

Given a sample RDF data, serialized in the Turtle format:

```turtle
ex:alice a foaf:Person ;
         foaf:name "Alice" .
ex:bob a foaf:Person ;
       foaf:knows ex:alice .
```
### 1. Using a Simple Dictionary Index

A Vortex-RDF file, using a **`Simple Dictionary Index`**, would store this data as follows:

#### Resource Dictionary 

**Terms Array**: Vortex stores all unique terms in a single (FSST-compressed) `VarBinViewArray`.

| ID* | Value |
|---|---|
| 0 | `<http://example.org/alice>` |
| 1 | `<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>` |
| 2 | `<http://xmlns.com/foaf/0.1/Person>` |
| 3 | `<http://xmlns.com/foaf/0.1/name>` |
| 4 | `"Alice"` |
| 5 | `<http://example.org/bob>` |
| 6 | `<http://xmlns.com/foaf/0.1/knows>` |
| 7 | `""` (Default Graph) |

> *`IDs` are not actually stored in the Vortex file, they are implicit and determined by the position of the terms in the array. Simply shown in this example for clarity.

### 2. Using a Chained Hash Index
A Vortex-RDF file using a **`Chained Hash Index`**, would be stored as follows:

#### Resource Dictionary 

**Terms Array**: Vortex stores all unique terms in a single (FSST-compressed) `VarBinViewArray`.

| ID*  | Value                                               |
| ---- | --------------------------------------------------- |
| 0    | `<http://example.org/alice>`                        |
| 1    | `<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>` |
| 2    | `<http://xmlns.com/foaf/0.1/Person>`                |
| 3    | `<http://xmlns.com/foaf/0.1/name>`                  |
| 4    | `"Alice"@en`                                        |
| 5    | `<http://example.org/bob>`                          |
| 6    | `<http://xmlns.com/foaf/0.1/knows>`                 |
| 7    | `""` (Default Graph)                                |
> *`IDs` are not actually stored in the Vortex file, they are implicit and determined by the position of the terms in the array. Simply shown in this example for clarity.

To enable efficient lookups to find the ID of a term (e.g., getting `0` for `<http://example.org/alice>`) without checking every string and without needing to copy the whole dictionary into an auxiliary in-memory data structure, two additional integer arrays are stored.

**Buckets Array** (Hash Table Entry Points):

(Assuming a **`Bucket Size of 5`** as a simplified example)

This array relies on a hash function  (e.g., `hash(term) % buckets.length`) whose result represents a "bucket". When a given RDF term is added, here we store its ID (i.e., its position in the **`Terms Array`**), into the bucket position corresponding to the result of the hash operation.  

| Bucket_ID* | Head_Term_ID | Latest_Value* |
|---|---|---|
| 0 | 5 | `<http://example.org/bob>` |
| 1 | 3 | `<http://xmlns.com/foaf/0.1/name>` |
| 2 | 7 | `""` |
| 3 | -1 | - |
| 4 | 4 | `"Alice"@en` |
| 5 | 1 | `<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>` |
> *Bucket IDs and Latest_Values are not actually stored in the Vortex file, they are implicit and simply shown in this example for clarity.

The **`Head_Term_ID column`** is initially filled with `-1` values, indicating that there are no terms whose hash result matches that bucket position. When a hash collision occurs, only the latest term ID is stored in **`Head_Term_ID`**. Note that not all terms are directly "present" in the above example, which indicates hash collisions have occurred. These are handled by the **`Next Array`** described below.  

**Next Array** (Collision Chain):

This array has the same length as the **`Terms Array`**. Each row contains the ID of the next term in the chain of collisions (if any) for the corresponding bucket.

| Row_ID* | Next_Row_ID | Value* |
|---|---|---|
| 0 | -1 | `<http://example.org/alice>` |
| 1 | -1 | `<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>` |
| 2 | -1 | `<http://xmlns.com/foaf/0.1/Person>` |
| 3 | -1 | `<http://xmlns.com/foaf/0.1/name>` |
| 4 | -1 | `"Alice"@en` |
| 5 | 2 | `<http://example.org/bob>` |
| 6 | 0 | `<http://xmlns.com/foaf/0.1/knows>` |
| 7 | 6 | `""` |
> *`Row IDs` and `Values` are not actually stored in the Vortex file, they are implicit and simply shown in this example for clarity.

Based on the data encoded in this example **`Next Array`**, we can observe 2 handled collisions, which are encoded as implicit linked lists:
- `<http://xmlns.com/foaf/0.1/Person> ← <http://example.org/bob>`
- `<http://example.org/alice> ← <http://xmlns.com/foaf/0.1/knows> ← ""`

**How does a lookup work?**

1. Say you want to find the ID of `<http://example.org/bob>`.
2. Compute `hash("<http://example.org/bob>") % 5`. Let's say it equals `0`.
3. Check `Buckets[0]`. It points to term ID `5`.
4. Check  `Terms[5]`. Is it `<http://example.org/bob>`? **Yes**.
   - **Result**: ID is `5`.

**Collision Example:**
1. Now you want to find the ID of `<http://example.org/alice>`.
2. Compute `hash("<http://example.org/alice>") % 5`. Let's say it equals `2`.
3. Check `Buckets[2]`. It points to term ID `7` .
4. Check ` Terms[7]`. It is `""`. **No match**.
5. Check `Next[7]`. It points to term ID `6`.
6. Check  `Terms[6]`. It is `<http://xmlns.com/foaf/0.1/knows>` **Still no match**.
7. Check `Next[6]`. It points to ID `0`. Is it `<http://example.org/alice>`? **Yes**.
   - **Result**: ID is `0`.

### Quad Collection (common to both dictionary types)

The quads are stored as a `StructArray` of indices. Since the quads are globally (or chunk-locally) sorted by `Subject -> Predicate -> Object -> Graph`, binary search is highly efficient on the `s` column.

To enable fast binary-search querying on the unsorted predicate (`p`) and object (`o`) columns, the struct array can optionally be extended with secondary sorted permutation indexes at serialization time:

| s | p | o | g | _idx_o_val | _idx_o_rid | _idx_p_val | _idx_p_rid |
|---|---|---|---|---|---|---|---|
| 0 | 1 | 2 | 7 | 0 | 3 | 1 | 0 |
| 0 | 3 | 4 | 7 | 2 | 0 | 1 | 2 |
| 5 | 1 | 2 | 7 | 2 | 2 | 3 | 1 |
| 5 | 6 | 0 | 7 | 4 | 1 | 6 | 3 |

* **`_idx_o_val` / `_idx_o_rid` (Object Index):**
  * `_idx_o_val` lists all object IDs in sorted order (`[0, 2, 2, 4]`).
  * `_idx_o_rid` maps each value back to its original row ID in the quad collection (`Row 3` has object `0`, `Row 0` has object `2`, `Row 2` has object `2`, and `Row 1` has object `4`).
* **`_idx_p_val` / `_idx_p_rid` (Predicate Index):**
  * `_idx_p_val` lists all predicate IDs in sorted order (`[1, 1, 3, 6]`).
  * `_idx_p_rid` maps each value back to its original row ID in the quad collection (`Row 0` has predicate `1`, `Row 2` has predicate `1`, `Row 1` has predicate `3`, and `Row 3` has predicate `6`).

## How Datatypes are Handled

Vortex-RDF ensures 100% fidelity for all [XSD datatypes](https://www.w3.org/TR/xmlschema-2/) (numeric, boolean, datetime, etc.) by leveraging [N-Triples Canonicalization](https://www.w3.org/TR/n-triples/#canonical-ntriples). Every Literal is stored using its canonical N-Triples string representation. For example, the integer `42` is stored as `"42"^^<http://www.w3.org/2001/XMLSchema#integer>`.

Because RDF datatypes are highly repetitive (e.g., thousands of numbers sharing the same `XMLSchema#integer` suffix), applying e.g.,  **FSST compression** leads to:
1. Identifying common datatype suffixes as **Frequent Symbols**.
2. Replacing these long IRI strings with **1-byte codes**.
3. Cheap reconstruction of the full string during zero-copy reads.

This provides the storage efficiency of native types while maintaining the flexibility to store any arbitrary or custom RDF term.

#### Future Optimization: Type Lifting

The architecture is designed to support "Type Lifting," where common analytical types (integers, floats, timestamps) are moved from the string dictionary into dedicated native Vortex columns (e.g., `PrimitiveArray`, `DateTimeArray`). This will enable faster range queries and mathematical operations by pushing down these operations directly to the compressed columnar data.

---

## Installation

### Rust
Add to your `Cargo.toml`:
```toml
[dependencies]
vortex-rdf-core = { "TODO: Publish on crates.io" }
```

For the time being you may clone this repo and compile it with:

```bash
cargo build --release -p vortex-rdf-cli
# The binary will be at ./target/release/vortex-rdf-cli
```

### WebAssembly (WASM)
Build for JS environments using `wasm-pack`:
```bash
cd js
npm run build:node #for Node.js
npm run build:browser # for the Browser
```
> TODO: Publish on npm

---

## Usage

### Rust API
```rust
use vortex_rdf_core::io::{serialize, deserialize};
use vortex_rdf_core::common::utils::parse_quads_from_reader;
use vortex_rdf_core::{SimpleDictionaryStore, ChainedHashStore};
use vortex_rdf_core::VortexRdfStore;
use oxrdfio::RdfFormat;
use oxrdf::{NamedNode, Subject};
use std::fs::File;

// 1. Parse RDF file into a Stream of Quads
let input_file = File::open("data.ttl")?;
let quads = parse_quads_from_reader(input_file, RdfFormat::Turtle);

// 2. Build the Vortex Store (Index) from the stream
// This processes the stream and builds the compressed index in memory
// Choose the index type from SimpleDictionaryStore or ChainedHashStore
let vortex_index = SimpleDictionaryStore::build_vortex_index(quads).await?;

// 3. Serialize to disk (Vortex File)
let mut output_file = File::create("output.vortex")?;
serialize(vortex_index, output_file).await?;

// 4. Deserialize (Read back)
// Load the store from the file (Zero-Copy)
let store = SimpleDictionaryStore::from_file("output.vortex").await?;

let mut output_writer = File::create("output.nq")?;
// Deserialize the store back to N-Quads format
deserialize(store, output_writer, RdfFormat::NQuads).await?;

// Pattern Matching

// Load Store (SimpleDictionary or ChainedHash)
// ChainedHashStore::from_file(...) is also available and allows zero-copy lookups
let store = SimpleDictionaryStore::from_file("data.vortex").await?;

let predicate = NamedNode::new("http://xmlns.com/foaf/0.1/knows")?;
let filtered = store.match_pattern(None, Some(&predicate), None, None).await?;
println!("Found {} matches", filtered.size());

// --- Data Modification ---

// 1. Add Quad (Zero-Copy Concatenation)
// Uses Vortex's ChunkedArray to virtually append a new row without copying 
// the existing store in memory.
let new_quad = Quad::new(s, p, o, g);
let mutated = store.add_quad(new_quad).await?;

// 2. Delete Quad (Inverse Columnar Filter)
// Uses columnar pattern matching to find the quad and applies an inverse 
// vectorized filter to exclude it.
let cleaned = mutated.delete_quad(&new_quad).await?;
```

**Important Note**: Currently mutations are only virtual and not persisted into the original Vortex files.

### JavaScript / WASM

```javascript
import init, { nquads_to_vortex, vortex_to_nquads } from './pkg/vortex_rdf_wasm.js';

await init();

// Convert N-Quads string to Vortex bytes
const bytes = nquads_to_vortex(nquadsString);

// Convert back to N-Quads
const restored = vortex_to_nquads(bytes);
```

### RDF-JS Support
Vortex-RDF provides an implementation of the [RDF-JS DatasetCore](https://rdf.js.org/store-spec/#storecore-interface) and [Data Model](https://rdf.js.org/data-model-spec/) interfaces via its WASM bindings.

```javascript
import init, { SimpleDictionaryStore } from './pkg/vortex_rdf.js';
await init();

// Create store from Vortex bytes or RDF string
const store = SimpleDictionaryStore.fromBytes(vortexBytes);
// const store = SimpleDictionaryStore.fromString(turtleString, "turtle");
// const store = SimpleDictionaryStore.empty();

console.log(`Loaded ${store.size} quads`);

// Query using RDF-JS match() pattern
// match(subject?, predicate?, object?, graph?)
const matches = store.match(null, null, null, null);

// Iterate over results
for (const quad of matches.values()) {
  console.log(quad.subject.value);
  console.log(quad.predicate.value);
  console.log(quad.object.value);
}

// Mutate (Simple in-memory overlay)
store.add(someQuad);
store.delete(otherQuad);
```

### Command Line Interface
```bash
# Convert Turtle to Vortex using a Simple Dictionary Index
vortex-rdf-cli serialize --input test.ttl --output test-sd.vortex --index-type simple-dictionary

# Convert using Chained Hash Index (Faster lookups, slightly larger file)
vortex-rdf-cli serialize --index-type chained-hash --input test/test.ttl --output test/test-ch.vortex

# Convert Vortex to N-Quads (output defaults to stdout)
vortex-rdf-cli deserialize --input test/test.vortex

# Specific format or pipe support
cat test/test.vortex | vortex-rdf-cli deserialize --format jsonld

# Pattern Matching / Filtering
vortex-rdf-cli match --input test/test.vortex --predicate "http://example.org/p1"
# Save filtered results back to Vortex
vortex-rdf-cli match --input test/test.vortex --subject "http://example.org/s1" --output filtered.vortex

# Enable debug logging (shows timing metrics)
RUST_LOG=vortex_rdf_cli=debug,vortex_rdf_core=debug vortex-rdf-cli serialize --input data.ttl --output data.vortex
```

---

## Benchmarking with CodSpeed

Vortex-RDF features a comprehensive benchmark suite built on top of [Divan](https://github.com/nvzqz/divan) and fully integrated with [CodSpeed](https://codspeed.io) for precise, CPU-instruction-level performance tracking.

The benchmarks evaluate four builder strategies across both dictionary indices (`ChainedHash` and `SimpleDictionary`) and multiple selective query patterns:
1. **Build Index (`build_vortex_index_*`)**: Profiles raw indexing and dictionary encoding throughput.
2. **Instantiate Store (`instantiate_store_*`)**: Profiles metadata parsing and zero-copy store instantiation.
3. **Match Pattern (`match_pattern_*`)**: Profiles vectorized scans and zone map pruning performance across 13 selective query patterns:
   * **Subject Only `(s, ?, ?, ?)`**: Highly selective (retrieves exactly 1 unique matching row).
   * **Predicate Only `(?, p, ?, ?)`**: Medium-high selectivity (retrieves $1\%$ of the dataset).
   * **Object Only `(?, ?, o, ?)`**: Medium selectivity (retrieves $2\%$ of the dataset).
   * **Graph Only `(?, ?, ?, g)`**: Non-selective (retrieves $100\%$ of all rows).
   * **Subject & Predicate `(s, p, ?, ?)`**: Highly selective (retrieves exactly 1 or 0 rows).
   * **Subject & Object `(s, ?, o, ?)`**: Restricts both endpoints of a triple/quad relation.
   * **Subject & Graph `(s, ?, ?, g)`**: Targets a specific subject inside a named graph.
   * **Predicate & Object `(?, p, o, ?)`**: Selective path querying typical for property-object relations.
   * **Predicate & Graph `(?, p, ?, g)`**: Scans a property inside a named graph.
   * **Subject, Predicate & Object `(s, p, o, ?)`**: Highly selective triple lookup.
   * **Predicate, Object & Graph `(?, p, o, g)`**: Scans a property-value pair inside a named graph.
   * **Subject, Object & Graph `(s, ?, o, g)`**: Restricts subject, object, and graph elements.
   * **Subject, Predicate & Graph `(s, p, ?, g)`**: Restricts subject, predicate, and graph elements.

### Running Benchmarks Locally

#### 1. Fast Sanity Check (Test Mode)
Running full statistical loops can take several minutes due to heavy sampling. You can execute all benchmarks **exactly once** in test-mode to instantly verify correctness:
```bash
# Run ChainedHash benchmarks in test mode
cargo test --bench chained_hash_bench

# Run SimpleDictionary benchmarks in test mode
cargo test --bench simple_dict_bench
```

#### 2. Run Full Statistical Benchmarks
To run the full Divan statistical profiling loops:
```bash
# Profile ChainedHash
cargo bench --bench chained_hash_bench

# Profile SimpleDictionary
cargo bench --bench simple_dict_bench
```

#### 3. Filtering Benchmarks
You can isolate specific builders, dictionary types, or query shapes:
```bash
# Profile only match pattern benchmarks
cargo bench --bench chained_hash_bench -- match_pattern

# Profile only Subject-selective patterns (_s)
cargo bench --bench chained_hash_bench -- _s

# Profile a specific builder
cargo bench --bench chained_hash_bench -- sorted_stream
```

### Dynamic Index Caching
To optimize local testing and prevent redundant index re-compilations, the benchmark suite leverages a thread-safe global index cache (`std::sync::OnceLock`). 
* When `build_vortex_index_*` runs, it builds the array fresh to measure indexing throughput and automatically **populates the cache**.
* All subsequent `instantiate_store_*` and `match_pattern_*` benchmarks reuse the cached index in $O(1)$ time, completely isolating query matching performance from indexing overhead and keeping CodSpeed telemetry clean and noise-free.

---

## References

[1] Maximilian Kuschewski, David Sauerwein, Adnan Alhomssi, and Viktor Leis. 2023. BtrBlocks: Efficient Columnar Compression for Data Lakes. Proc. ACM Manag. Data, June 2023. https://doi.org/10.1145/3589263

[2] Peter Boncz, Thomas Neumann, and Viktor Leis. FSST: Fast Random Access String Compression. Proc. VLDB Endow., 13(12):2649–2661, July 2020. https://doi.org/10.14778/3407790.3407851.

[3] Daniel Lemire, Leonid Boytsov and Nathan Kurz. SIMD compression and the intersection of sorted integers. 2015. https://doi.org/10.1002/spe.2326