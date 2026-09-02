# Vortex-RDF
[![Crates.io](https://img.shields.io/crates/v/vortex-rdf-core.svg)](https://crates.io/crates/vortex-rdf-core)
[![npm](https://img.shields.io/npm/v/@vortex-rdf/vortex-rdf-store.svg)](https://www.npmjs.com/package/@vortex-rdf/vortex-rdf-store)
[![PyPI](https://img.shields.io/pypi/v/vortex-rdf.svg)](https://pypi.org/project/vortex-rdf/)
[![docs.rs](https://img.shields.io/docsrs/vortex-rdf-core)](https://docs.rs/vortex-rdf-core)
[![License](https://img.shields.io/crates/l/vortex-rdf-core)](https://github.com/vortex-rdf/vortex-rdf/blob/main/LICENSE)
[![CodSpeed](https://img.shields.io/endpoint?url=https://codspeed.io/badge.json)](https://app.codspeed.io/vortex-rdf/vortex-rdf?utm_source=badge)
[![CI](https://github.com/vortex-rdf/vortex-rdf/actions/workflows/ci.yml/badge.svg)](https://github.com/vortex-rdf/vortex-rdf/actions/workflows/ci.yml)

Vortex-RDF is a columnar RDF serialization and a queryable quad store built on the [Vortex](https://docs.vortex.dev) data format. It converts any RDF syntax [`oxrdfio`](https://docs.rs/oxrdfio/latest/oxrdfio/) reads into a compact, self-describing `.vortex` file (or the same bytes in memory) and queries it in place — zero-copy, without decompressing what a pattern does not touch — from Rust, a CLI, JavaScript/WebAssembly and Python.

## Key features

- 📊 **Columnar storage**: quads are [Vortex](https://docs.vortex.dev/specs/file-format) arrays, on disk and in memory alike, with the same layout in both.
- ♻️ **Zero-copy reads**: opening a file is lazy, and pattern filters are pushed down into the scan so only the touched chunks are read.
- 📦 **Adaptive compression**: Vortex picks per-column encodings (FSST, dictionary, run-length, bit-packing, …) and decompresses just in time.
- ☄️ **Streaming, out-of-core ingestion**: datasets larger than RAM are globally sorted through an external merge sort with bounded memory.
- 🍀 **RDF 1.1 quads**: named graphs `(s, p, o, g)`, blank nodes, language-tagged and typed literals.
- 🌍 **Cross-platform**: one Rust core behind a CLI, WebAssembly bindings for Node.js and browsers, and Python bindings.

## Install

| Surface | Install | Notes |
|---|---|---|
| Rust | `cargo add vortex-rdf-core` | the default `file-io` feature adds path-based file reading/writing on Tokio; add `--no-default-features` on wasm, where stores are exchanged as bytes |
| CLI | `cargo install vortex-rdf-cli` | |
| JavaScript | `npm install @vortex-rdf/vortex-rdf-store` | [js/README.md](js/README.md) |
| Python | `pip install vortex-rdf` | [python/README.md](python/README.md); [`vortex-rdflib`](https://pypi.org/project/vortex-rdflib/) builds an rdflib integration on it |

## Quick start

**Rust** — convert an RDF file to `.vortex`, open it lazily, match a pattern, export the matches:

```rust no_run
use oxrdf::NamedNode;
use oxrdfio::RdfFormat;
use vortex_rdf_core::{IndexType, LayoutStrategy, VortexRdfStore, export_rdf};
use vortex_rdf_core::{common::terms::parse_quads_from_reader, io::quads_stream_to_vortex_file};
# fn main() -> Result<(), Box<dyn std::error::Error>> {
# tokio::runtime::Runtime::new()?.block_on(async {

// Parse an RDF file into a quad stream and write it as a .vortex file
let quads = parse_quads_from_reader(std::fs::File::open("data.ttl")?, RdfFormat::Turtle);
let indexes = vec![IndexType::SecondaryByReference];
quads_stream_to_vortex_file(quads, "data.vortex".as_ref(), LayoutStrategy::Dictionary, indexes).await?;

// Open it lazily: nothing is read until a query runs
let store = VortexRdfStore::from_file("data.vortex").await?;

// Match a pattern; the filter is pushed down into the file scan
let knows = NamedNode::new("http://xmlns.com/foaf/0.1/knows")?;
let matched = store.match_pattern(None, Some(&knows), None, None).await?;
println!("{} matches", matched.size().await?);

// Export the matches back to textual RDF
export_rdf(matched, std::fs::File::create("knows.nq")?, RdfFormat::NQuads).await?;
# Ok::<(), Box<dyn std::error::Error>>(()) })?; Ok(()) }
```

**Rust** — build in memory, then mutate:

```rust
use oxrdf::{GraphName, NamedNode, Quad};
use vortex_rdf_core::{LayoutStrategy, RawQuad, VortexRdfStore};
# fn main() -> Result<(), Box<dyn std::error::Error>> {
# futures::executor::block_on(async {
 
// Create some quads
let (s, p) = (NamedNode::new("http://ex/s")?, NamedNode::new("http://ex/p")?);
let a = Quad::new(s.clone(), p.clone(), NamedNode::new("http://ex/a")?, GraphName::DefaultGraph);
let b = Quad::new(s, p, NamedNode::new("http://ex/b")?, GraphName::DefaultGraph);

// Sort the quads and adopt the result as a store (Dictionary layout, no indexes)
let quads = futures::stream::iter([Ok(RawQuad::from_quad(&a))]);
let store = VortexRdfStore::from_quads(quads, LayoutStrategy::Dictionary, vec![]).await?;

// Mutations return derived stores: additions append to a tail, deletions tombstone rows
let store = store.add_quad(b).await?;
let store = store.delete_quad(&a).await?;

assert_eq!(store.size().await?, 1);
# Ok::<(), Box<dyn std::error::Error>>(()) })?; Ok(()) }
```

**CLI**:

```bash
# Transform RDF data into Vortex-RDF
vortex-rdf-cli serialize -i data.ttl -o data.vortex --indexes secondary-by-reference   # --layout defaults to dictionary

# Transform back to RDF
vortex-rdf-cli deserialize -i data.vortex -o data.nq                                # format from the extension, else N-Quads

# Match graph patterns over Vortex-RDF stores
vortex-rdf-cli match -i data.vortex --predicate "http://xmlns.com/foaf/0.1/knows"       # matches to stdout as N-Quads
# --layout: default | typed-object | dictionary    --indexes (repeatable): secondary-by-copy | secondary-by-reference

# Any command takes RUST_LOG for detailed debug logs timing the path used to resolve
# a pattern (an index, a binary search, a mask scan):
RUST_LOG=vortex_rdf_core=debug vortex-rdf-cli match -i data.vortex --predicate "http://xmlns.com/foaf/0.1/knows"
```

**Python**:

```python
from vortex_rdf import VortexRdfStore, serialize_rdf

serialize_rdf("data.nt", "data.vortex", layout="dictionary")   # RDF file -> .vortex file
store = VortexRdfStore("data.vortex")                            # lazy open; layout auto-detected
store.count_quads(p="<http://xmlns.com/foaf/0.1/name>")          # match count, no terms materialized
store.get_quads(p="<http://xmlns.com/foaf/0.1/name>")            # [(s, p, o, g), ...]
```

**JavaScript**:

```javascript
import { VortexRdfStore, serializeRdf } from '@vortex-rdf/vortex-rdf-store';

const bytes = await serializeRdf('<http://ex/s> <http://ex/p> "o" .', 'turtle');
const store = await VortexRdfStore.fromBytes(bytes);

for await (const quad of store.match(null, 'http://ex/p', null, null)) {
  console.log(quad.subject.value, quad.object.value);
}
```

## Main concepts overview

- **Column layouts** — `Default` stores the four terms as N-Triples strings; `TypedObject` splits the object into kind/value/datatype/language columns; `Dictionary` stores `u32` codes into one sorted term dictionary, held as the file's `dictionary` child and point-read through its chunk leaves when it stays file-backed. See [docs/file-format.md §4–5](docs/file-format.md#4-the-quad-table).
- **Secondary indexes** — `SecondaryByCopy` keeps two extra copies of the quads sorted by `(p, o, s, g)` and `(o, s, p, g)`; `SecondaryByReference` keeps sorted `{val, rid}` pairs for predicates and objects. In memory they are binary-searched; in a file, a run is located by a chunk-probe binary search, then range-scanned or point-read. See [docs/file-format.md §6](docs/file-format.md#6-the-index-children) and [docs/matching.md §8](docs/matching.md#8-the-index-resolvers).
- **Builders** — every build sorts globally by `(s, p, o, g)`: in memory on wasm, out of core (spilling sorted runs to disk) everywhere a filesystem exists. See [docs/serialization.md](docs/serialization.md).
- **The store** — a base (array or lazily scanned file) plus a view: a row selection, tombstone masks and an append tail; `match_pattern` routes each bound position to the cheapest path (subject prefix probe, index, pushed-down filter, mask scan) and matches the tail independently. See [docs/matching.md](docs/matching.md).
- **Mutations & compaction** — additions are accumulated in the `Tail`, deletions are `Tombstone` rows, nothing is rewritten until a compaction rebuilds one sorted, indexed base (automatically once the tail outgrows its thresholds). See [docs/mutations.md](docs/mutations.md).
- **The `.vortex` container** — one self-describing Vortex file: the quad table, the `dictionary` child and the index children under a `vortex-rdf.store.v1` root, also the byte-exchange format of the bindings. See [docs/file-format.md](docs/file-format.md).

## Read more

| Document | Covers |
|---|---|
| [docs/file-format.md](docs/file-format.md) | the `.vortex` store file: root, quad table, dictionary and index children, a worked example |
| [docs/matching.md](docs/matching.md) | how a quad pattern is resolved on each backend, and what each path costs |
| [docs/serialization.md](docs/serialization.md) | how a store is built and written: both sort pipelines, columns per layout, index builds |
| [docs/mutations.md](docs/mutations.md) | the merge-on-read model: tail, tombstones, compaction and auto-compaction |
| [docs/arrow.md](docs/arrow.md) | the Arrow interface: quad batches per encoding, the dictionary export, the Python PyCapsule and JavaScript IPC surfaces |
| [js/README.md](js/README.md) | the JavaScript/WebAssembly bindings |
| [python/README.md](python/README.md) | the Python bindings |
| [CONTRIBUTING.md](CONTRIBUTING.md) | git hooks, the local CI mirror, doc-anchor checks, changelog generation |
| [CHANGELOG.md](CHANGELOG.md) | release history |

## License

MIT — see [LICENSE](LICENSE).
