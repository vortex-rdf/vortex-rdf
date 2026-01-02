# Vortex-RDF

Vortex-RDF is modern and high-performance columnar RDF serialization built on top of the [Vortex](https://vortex.dev) data format. It combines the flexible graph-based model of RDF with the efficiency of modern analytical data formats.

This library provides both serialization and deserialization capabilities for converting RDF files to Vortex-RDF and vice versa. It also provides an implementation of a RDF store according to the [RDF-JS specification](https://rdf.js.org/dataset-spec/#datasetcore-interface). 

## Key Features

- 📊 **Advanced Columnar Storage**: Leverages Vortex's zero-copy, compressed array formats.
- 📦 **FSST String Compression**: Uses [Fast Static Symbol Table (FSST)](https://doi.org/10.14778/3407790.3407851) compression [1] for the dictionary/resource table, specifically optimized for repetitive short strings like IRIs.
- 🧠 **Memory Optimization**: Automatically narrows Predicate indices to `u16` when unique predicates are few (< 65,536), saving up to 50% RAM for the most repetitive column.
- 🧊 **Quads Support**: Full support for named Graphs (S, P, O, G) and in general for [RDF 1.1](https://www.w3.org/TR/rdf11-concepts/).
- 🌍 **Cross-Platform**: Native Rust library with a CLI + WebAssembly (WASM) bindings for browsers/Node.js. Python binding comming soon.

### The Power of Zero-Copy

Vortex-RDF is built on a ["Zero-Copy" philosophy](https://en.wikipedia.org/wiki/Zero-copy). This means that after the RDF data is serialized into the Vortex format, it can be read, filtered, and queried without ever moving or copying the bytes in memory.

#### How it works:
1. **Memory Mapping**: Using `mmap` (via the [`memmap2`](https://docs.rs/memmap2/latest/memmap2/) crate), the library maps large RDF files directly into the process memory. The operating system handles loading only the parts of the file that your query actually touches.
2. **Buffer Views**: When you extract a specific column (e.g., just the Predicates) or a specific row range, Vortex creates a [_Layout_](https://docs.vortex.dev/concepts/layouts). This view is just a pointer and some metadata—it doesn't duplicate the data.
3. **Lazy Decompression**: Even with FSST compression, Vortex is designed to decompress data "just-in-time" at the CPU register level, avoiding the creation of temporary intermediate strings.

### Vortex IPC & File Format

Vortex-RDF is serialized using the [_Vortex IPC (Inter-Process Communication)_ protocol](https://docs.rs/vortex-ipc/0.56.0/vortex_ipc/). This is a streaming, self-describing binary format that shares its design philosophy with the Apache Arrow IPC protocol but is specifically optimized for Vortex's advanced encodings.

#### How IPC is Used:
1. **Self-Describing Stream**: Every `.vortex` file begins with a **Schema Message** (using FlatBuffers) that describes the `DType` (data types) of the columns. The consumer doesn't need a sidecar file to know it is looking at an (S, P, O, G) struct.
2. **Message-Based Serialization**: The data is written as a sequence of messages. In Vortex-RDF, the entire store is typically bundled into a single high-level `StructArray` message, though it could be chunked into multiple messages for massive streaming stores.
3. **Flat Memory Layout**: IPC messages are designed to be **8-byte aligned**. When mapped into memory, the columnar data (like the Subject `u32` IDs) can be read directly by the CPU as a native array without any "shuffling" or "parsing."
4. **Encodings in the Stream**: The IPC stream encodes not just the values, but the **Encoding IDs** (e.g., `vortex.fsst`, `vortex.dict`). When a reader encounters these IDs, it uses the Vortex Registry to instantiate the correct decompressors.

This IPC-based approach ensures that a Vortex-RDF file produced by the Rust CLI can be consumed by a Python process, a C++ engine, or a WebAssembly browser app with identical performance characteristics and zero-copy overhead.

## Architecture

Vortex-RDF uses a layered architecture to separate logical RDF quads from their physical representation:

### 1. Unified Resource Table
All IRIs, Literals, and Blank Nodes are stored in a single global `VarBinViewArray`. This "Resource Table" is then compressed using **FSST**, which creates a shared symbol table to compress common IRI prefixes and substrings across the entire store.

### 2. Triple/Quad Columns
The graph structure is stored as an optimized `StructArray` of indices:
- **Subject**: `u32` indices into the Resource Table.
- **Predicate**: `u16` (or `u32`) indices. Automatically narrowed to `u16` if unique predicates < 65,536.
- **Object**: `u32` indices.
- **Graph**: `u32` indices.

This layout allows for extremely fast analytical queries and filtering on specific columns without reading the entire store.

## Data Representation Example

Given a sample Turtle file (`test/test.ttl`):

```turtle
ex:alice a foaf:Person ;
         foaf:name "Alice" .
ex:bob a foaf:Person ;
       foaf:knows ex:alice .
```

### 1. Resource Table (Physical View)
Vortex stores all unique terms in a single FSST-compressed `VarBinViewArray`.

| ID | Value (N-Triples String) |
|---|---|
| 0 | `<http://example.org/alice>` |
| 1 | `<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>` |
| 2 | `<http://xmlns.com/foaf/0.1/Person>` |
| 3 | `<http://xmlns.com/foaf/0.1/name>` |
| 4 | `"Alice"` |
| 5 | `<http://example.org/bob>` |
| 6 | `<http://xmlns.com/foaf/0.1/knows>` |
| 7 | `""` (Default Graph) |

### 2. Quad Table (Physical View)
The quads are stored as an optimized table of indices.

| S (u32) | P (u16) | O (u32) | G (u32) |
|---|---|---|---|
| 0 | 1 | 2 | 7 |
| 0 | 3 | 4 | 7 |
| 5 | 1 | 2 | 7 |
| 5 | 6 | 0 | 7 |

**Behind the scenes:** 
- The **P column** is narrowed to `u16` because there are < 65k predicates.
- The **G column** is Run-Length Encoded (RLE) by Vortex because all values are the same (Default Graph), effectively reducing its size to near-zero.
- The **S, P, O columns** are bitpacked to the minimum number of bits needed to represent the max ID.

## How Datatypes are Handled

Vortex-RDF ensures 100% fidelity for all [XSD datatypes](https://www.w3.org/TR/xmlschema-2/) (numeric, boolean, datetime, etc.) by leveraging **N-Triples Canonicalization** combined with **Symbolic Compression**.

### 1. Canonical Normalization
Every literal is stored using its canonical N-Triples string representation. For example, the integer `42` is stored as `"42"^^<http://www.w3.org/2001/XMLSchema#integer>`.

### 2. Symbolic Compression (FSST)
Because RDF datatypes are highly repetitive (e.g., thousands of numbers sharing the same `XMLSchema#integer` suffix), the **FSST compression** engine automatically:
1. Identifies common datatype suffixes as **Frequent Symbols**.
2. Replaces these long IRI strings with **1-byte codes**.
3. Reconstructs the full string perfectly during zero-copy reads.

This provides the storage efficiency of native types while maintaining the flexibility to store any arbitrary or custom RDF term.

### 3. Future Optimization: Type Lifting
The architecture is designed to support "Type Lifting," where common analytical types (integers, floats, timestamps) are moved from the string dictionary into dedicated native Vortex columns (`PrimitiveArray`, `DateTimeArray`). This will enable even faster range queries and mathematical operations directly on the compressed columnar data.

---

## Installation

### Rust
Add to your `Cargo.toml`:
```toml
[dependencies]
vortex-rdf-core = { path = "path/to/vortex-rdf/core" }
```

### CLI
Build and install the CLI tool:
```bash
cargo build --release -p vortex-rdf-cli
# The binary will be at ./target/release/vortex-rdf-cli
```

### WebAssembly (WASM)
Build for JS environments using `wasm-pack`:
```bash
cd wasm
wasm-pack build --target web
```

---

## Usage

### Rust API
```rust
use vortex_rdf_core::{serialize, deserialize, RdfFormat};
use std::fs::File;

// High-level Streaming API (Recommended)
let input_file = File::open("data.ttl")?;
let vortex_bytes = serialize(input_file, RdfFormat::Turtle)?;

let output_file = File::create("output.nq")?;
deserialize(&vortex_bytes, output_file, RdfFormat::NQuads)?;

// Low-level Quad API
use vortex_rdf_core::{quads_to_vortex, vortex_to_quads};
let quads = vortex_to_quads(&vortex_bytes)?;

// Pattern Matching (Zero-Copy)
use vortex_rdf_core::{VortexRdfStore, Subject, NamedNode};
let store = VortexRdfStore::from_file("data.vortex")?;
let predicate = NamedNode::new("http://xmlns.com/foaf/0.1/knows")?;
let filtered = store.match_pattern(None, Some(&predicate), None, None)?;
println!("Found {} matches", filtered.size());

// --- Data Modification ---

// 1. Add Quad (Zero-Copy Concatenation)
// Uses Vortex's ChunkedArray to virtually append a new row without copying 
// the existing store in memory.
let new_quad = Quad::new(s, p, o, g);
let mutated = store.add_quad(new_quad)?;

// 2. Delete Quad (Inverse Columnar Filter)
// Uses columnar pattern matching to find the quad and applies an inverse 
// vectorized filter to exclude it.
let cleaned = mutated.delete_quad(&new_quad)?;
```

### JavaScript / WASM
```javascript
import init, { nquads_to_vortex, vortex_to_nquads } from './pkg/vortex_rdf_wasm.js';

await init();

// Convert N-Quads string to Vortex bytes
const bytes = nquads_to_vortex(nquadsString);

// Convert back to N-Quads
const restored = vortex_to_nquads(bytes);
```

### RDF-JS DatasetCore Support
Vortex-RDF provides a high-performance implementation of the [RDF-JS DatasetCore](https://rdf.js.org/store-spec/#storecore-interface) and [Data Model](https://rdf.js.org/data-model-spec/) interfaces.

Unlike traditional JS stores that hold thousands of objects in memory, `VortexRdfStore` is **truly backed by the Vortex columnar store**. Operations like `.size` and `.has()` are performed directly on compressed columns, minimizing memory overhead and GC pressure.

```javascript
import init, { VortexRdfStore } from './pkg/vortex_rdf_wasm.js';
await init();

// Create store from Vortex bytes or RDF string
const store = VortexRdfStore.fromBytes(vortexBytes);
// const store = VortexRdfStore.fromString(turtleString, "turtle");
// const store = VortexRdfStore.empty();

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
# Convert Turtle to Vortex
vortex-rdf-cli serialize --input test/test.ttl --output test/test.vortex

# Convert Vortex to N-Quads (defaults to stdout)
vortex-rdf-cli deserialize --input test/test.vortex

# Specific format or pipe support
cat test/test.vortex | vortex-rdf-cli deserialize --format jsonld

# Pattern Matching / Filtering
vortex-rdf-cli match --input test/test.vortex --predicate "http://example.org/p1"
# Save filtered results back to Vortex
vortex-rdf-cli match --input test/test.vortex --subject "http://example.org/s1" --output filtered.vortex

# Enable debug logging (shows timing metrics)
RUST_LOG=debug vortex-rdf-cli serialize --input data.ttl --output data.vortex
```

---

## Project Structure

- `core/`: The core Rust implementation of the serialization logic.
- `wasm/`: WebAssembly bindings for the core library.
- `cli/`: Command-line tool for file conversion.

## References

[1] Peter Boncz, Thomas Neumann, and Viktor Leis. FSST: Fast Random Access String Compression. Proc. VLDB Endow., 13(12):2649–2661, July 2020. URL: https://doi.org/10.14778/3407790.3407851.