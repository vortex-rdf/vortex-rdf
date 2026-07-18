# Vortex-RDF for JavaScript

High-performance, columnar RDF storage and serialization for Node.js and the Web, powered by [Vortex](https://vortex.dev) and WebAssembly.

## Features

- **Blazing Fast**: Columnar filtering and pattern matching executed in Rust.
- **Memory Efficient**: Backed by a zero-copy Vortex store.
- **RDF-JS Compatible**: Designed to work with the RDF-JS Data Model and DatasetCore interfaces.
- **WASM Powered**: Near-native performance in the browser and Node.js.

## Installation

```bash
npm install vortex-rdf
```

## Usage

### Loading data

```javascript
import { VortexStore } from 'vortex-rdf';

// From a Turtle/N-Quads/... string
const store = await VortexStore.fromString(ttlData, 'turtle');

// From RDF-JS quads, skipping a serialize/parse round-trip
const store = await VortexStore.fromQuads(quads);

// From Vortex binary data
const store = await VortexStore.fromBytes(vortexBytes);

// Or create a new empty store
const store = VortexStore.empty();
```

Supported formats: `ntriples`, `nquads`, `turtle`, `trig`, `n3`, `rdfxml`, `jsonld`
(plus the short aliases `nt`, `nq`, `ttl`, `rdf`, `xml`).

### Querying

```javascript
// Perform a match (subject, predicate, object, graph).
// Pass null/undefined for a variable position.
const matches = await store.match(null, myPredicate, null, null);

console.log(`Found ${await matches.size()} results`);

// .values() is async and returns an iterator
const iterator = await matches.values();
for (const quad of iterator) {
  console.log(`${quad.subject.value} -> ${quad.object.value}`);
}
```

`match` returns another `VortexStore`, so matches compose and can themselves be
counted, iterated, or serialized.

### Manipulation

```javascript
await store.addQuad(myQuad);
await store.addQuads([quadA, quadB]);
await store.deleteQuad(existingQuad);
```

Mutations follow RDF/JS dataset semantics: adding a quad already present is a
no-op, and deleting never rewrites the columnar data (rows are tombstoned).
Added quads accumulate in a small in-memory tail beside the immutable base, so
the store's indexes keep working across edits; when the tail outgrows the base
(a tenth of its rows, or 100K rows) the store compacts itself back into one
sorted, indexed array. Prefer `addQuads` over a loop of `addQuad` calls; for
bulk loading, build once with `fromString`/`fromQuads`.

### Serializing

```javascript
// Back to RDF text, in any supported format
const turtle = await store.toRdf('turtle');

// To Vortex binary data; read back with VortexStore.fromBytes
const bytes = await store.toBytes();
```

### Build options

Ingestion accepts an optional `BuildOptions` object that trades build cost
against query speed and size. All fields are optional.

```javascript
const store = await VortexStore.fromString(data, 'nquads', {
  builder: 'Sorted',                  // 'Unsorted' (default) | 'Sorted'
  layout: 'Dictionary',               // 'Default' (default) | 'TypedObject' | 'Dictionary'
  indexes: ['SecondaryByReference'],  // default: []
});
```

**`builder`** — how quads are ordered while building:

| Value | Build cost | Effect on queries |
| --- | --- | --- |
| `'Unsorted'` (default) | Cheapest; natural insertion order | Every `match` is a full column scan |
| `'Sorted'` | Global in-memory sort by subject → predicate → object → graph | Subject lookups use a binary search |

**`layout`** — how terms are encoded into columns:

| Value | Notes |
| --- | --- |
| `'Default'` | All four terms as N-Triples strings |
| `'TypedObject'` | Object split into kind/value/datatype/language columns |
| `'Dictionary'` | Terms replaced by codes into a sorted term dictionary. Most compact and fastest to query; added quads live in an in-memory string tail until serialized |

**`indexes`** — `'SecondaryByReference'` adds sorted predicate/object columns
plus row-id back-references, so predicate-only and object-only patterns use a
binary search instead of a full scan. `'SecondaryByCopy'` embeds two complete
extra copies of the quad columns — one sorted by `(p, o, s, g)`, one by
`(o, s, p, g)` — giving predicate- and object-bound patterns (including
combined predicate+object lookups, resolved in one prefix search) the same
sorted access path subjects have, at roughly 2× the storage. Both cost extra
space and are only effective alongside `builder: 'Sorted'`.

A good query-optimized default is
`{ builder: 'Sorted', layout: 'Dictionary', indexes: ['SecondaryByReference'] }`;
the default `{ builder: 'Unsorted', layout: 'Default' }` is the fastest to build.

> The core's out-of-core `SortedStream` builder is not available here: it spills
> sorted runs to disk, which WebAssembly has no access to.

### Helper functions

For one-shot conversions without holding a store:

```javascript
import { rdf_to_vortex, vortex_to_rdf, nquads_to_vortex, vortex_to_nquads } from 'vortex-rdf';

const bytes = await rdf_to_vortex(turtleText, 'turtle', { builder: 'Sorted' });
const text  = await vortex_to_rdf(bytes, 'nquads');

// N-Quads shorthands
const bytes2 = await nquads_to_vortex(nquadsText);
const text2  = await vortex_to_nquads(bytes2);
```

### TypeScript support

The package ships typings generated from the Rust bindings, using RDF-JS types.

```typescript
import { VortexStore, type BuildOptions } from 'vortex-rdf';
import { DataFactory } from 'rdf-data-factory';

const df = new DataFactory();

const options: BuildOptions = { builder: 'Sorted', layout: 'Dictionary' };
const store = await VortexStore.fromString(data, 'nquads', options);

const matches = await store.match(null, df.namedNode('http://schema.org/name'), null, null);
console.log(store.layout()); // 'Dictionary'

for (const quad of await matches.values()) {
  console.log(quad.subject.value);
}
```

## Building

This package is built using [wasm-pack](https://rustwasm.github.io/wasm-pack/).

```bash
# Build for Node.js
npm run build:node

# Build for the Web
npm run build:web

# Run the test suite (requires a build first)
npm test
```

## License

MIT
