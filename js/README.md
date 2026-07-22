# Vortex-RDF for JavaScript

JavaScript bindings of  [`vortex-rdf-core`](https://crates.io/crates/vortex-rdf-core) compiled via WebAssembly.

## Installation

```bash
npm install @vortex-rdf/vortex-rdf-store
```

The package is ESM-only (no CommonJS `require`) and works identically in Node.js and browsers — the same `import` statement resolves to a Node-specific entry point or a browser one depending on where it runs, so there's no environment-specific code to write:

```javascript
import { VortexRdfStore } from '@vortex-rdf/vortex-rdf-store';

const store = await VortexRdfStore.fromString(ttlData, 'turtle');
```

Under the hood, a single `wasm-pack --target web` build backs both: the Node entry point reads the `.wasm` file straight off disk (`node:fs`), and the browser entry point uses the standard `fetch`-based loading that bundlers and browsers already understand via `import.meta.url`. Both call the WASM module's async initialization for you, so there's no `init()` to await yourself.

> **Bundler note:** both entry points use top-level `await`. Vite and Rollup
> support this by default; webpack 5 needs
> `experiments: { topLevelAwait: true }` enabled in its config.

## Usage

### Loading data

```typescript
import { VortexRdfStore } from '@vortex-rdf/vortex-rdf-store';
import { Readable } from 'node:stream';
import type { Quad, Stream } from '@rdfjs/types';

// From a Turtle/N-Quads/... string
// Supported formats: `ntriples`, `nquads`, `turtle`, `trig`, `n3`, `rdfxml`, `jsonld` 
// (plus the short aliases `nt`, `nq`, `ttl`, `rdf`, `xml`).
const store = await VortexRdfStore.fromString(ttlData, 'turtle');

// From RDF-JS quads — an array
const quads: Quad[] = [...];
const store = await VortexRdfStore.fromQuads(quads);

// Or from any RDF/JS Stream<Quad>
const quadStream: Stream<Quad> = Readable.from(quads, { objectMode: true });
const store = await VortexRdfStore.fromQuads(quadStream);

// From Vortex binary data (e.g, fetched from a remote server)
const store = await VortexRdfStore.fromBytes(vortexBytes);

// Or create a new empty store
const store = VortexRdfStore.empty();
```

### Querying

`match` implements the RDF/JS [`Source.match`](https://rdf.js.org/stream-spec/#source-interface) contract. It takes a `(subject, predicate, object, graph)` pattern — pass
`null`/`undefined` for a variable position — and returns **synchronously** an RDF/JS `Stream<Quad>`:

```javascript
store.match(null, myPredicate, null, null)
  .on('data', (quad) => console.log(`${quad.subject.value} -> ${quad.object.value}`))
  .on('end', () => console.log('done'));
```

The returned stream also implements `Symbol.asyncIterator` as a convenience, so it can be consumed with `for await` (in TypeScript, cast to `AsyncIterable<Quad>` since the declared type is `Stream<Quad>`):

```javascript
for await (const quad of store.match(null, myPredicate, null, null)) {
  console.log(quad.object.value);
}
```

When you just want the matches as an array, `getQuads` is the array-returning counterpart (`async`, because resolving the match crosses the WebAssembly boundary):

```javascript
const quads = await store.getQuads(null, myPredicate, null, null);
console.log(`Found ${quads.length} results`);
```

**Quads are lazy and zero-copy.** `match`/`getQuads` don't build eager term objects — they hand back quads backed by the store's columnar data. A term's string is decoded only when you read `.value`/`.termType`, and then interned, so iterating, counting, filtering, and `.equals` never materialize strings you don't use. Under the default `Dictionary` layout, `.equals` between terms of the same store is an **integer code compare** (no decoding at all). 

The quads implement the RDF/JS `Quad`/`Term` interface (`.subject.value`, `.equals`, …) and interoperate with foreign RDF/JS terms via `.equals` in both directions. (They're lazy views into the producing store, so — unlike a plain data object — don't `structuredClone`
them or rely on enumerating own properties.)

Test membership of a single quad with `has` (an exact four-component lookup):

```javascript
if (await store.has(myQuad)) {
  console.log('present');
}
```

### Mutation

```javascript
await store.addQuad(myQuad);
await store.addQuads([quadA, quadB]);
await store.deleteQuad(existingQuad);
```

Mutations follow RDF/JS dataset semantics: adding a quad already present is a no-op, and deleting never rewrites the columnar data (rows are tombstoned). 

Added quads accumulate in a small in-memory tail beside the immutable base, so the store's indexes keep working across edits; when the tail outgrows the base (a tenth of its rows, or 100K rows) the store compacts itself back into one sorted, indexed array. 

Prefer `addQuads` over a loop of `addQuad` calls; for bulk loading, build once with `fromString`/`fromQuads`.

### Serializing

```javascript
// Back to RDF text, in any supported format
const turtle = await store.toRdf('turtle');

// To Vortex binary data; read back with VortexRdfStore.fromBytes
const bytes = await store.toBytes();
```

### Build options

Ingestion accepts an optional `BuildOptions` object that trades build cost against query speed and size. All fields are optional.

```javascript
const store = await VortexRdfStore.fromString(data, 'nquads', {
  builder: 'Sorted',                  // 'Unsorted' (default) | 'Sorted'
  layout: 'Dictionary',               // 'Dictionary' (default) | 'Default' | 'TypedObject'
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
| `'Dictionary'` (default) | Terms replaced by codes into a sorted term dictionary. Most compact and fastest to query; backs the integer `.equals` fast path on lazy quads; added quads live in an in-memory string tail until serialized |
| `'Default'` | All four terms as N-Triples strings |
| `'TypedObject'` | Object split into kind/value/datatype/language columns |

**`indexes`** 

- `'SecondaryByReference'` adds sorted predicate/object columns plus row-id back-references, so predicate-only and object-only patterns use a binary search instead of a full scan. 
- `'SecondaryByCopy'` embeds two complete extra copies of the quad columns — one sorted by `(p, o, s, g)`, one by `(o, s, p, g)` — giving predicate- and object-bound patterns (including combined predicate+object lookups, resolved in one prefix search) the same sorted access path subjects have, at roughly 2× the storage. Both cost extra space and are only effective alongside `builder: 'Sorted'`.

A good query-optimized configuration is 

```javascript
{ 
    builder: 'Sorted', 
    layout: 'Dictionary', 
    indexes: ['SecondaryByReference'] 
};
```


The default `{ builder: 'Unsorted', layout: 'Dictionary' }` already gives compact, code-based lazy reads and is cheap to build.

### Helper functions

For one-shot conversions without holding a store:

```javascript
import { rdf_to_vortex, vortex_to_rdf, nquads_to_vortex, vortex_to_nquads } from '@vortex-rdf/vortex-rdf-store';

const bytes = await rdf_to_vortex(turtleText, 'turtle', { builder: 'Sorted' });
const text  = await vortex_to_rdf(bytes, 'nquads');

// N-Quads shorthands
const bytes2 = await nquads_to_vortex(nquadsText);
const text2  = await vortex_to_nquads(bytes2);
```

### TypeScript support

The package ships typings generated from the Rust bindings, using RDF-JS types.

```typescript
import { VortexRdfStore, type BuildOptions } from '@vortex-rdf/vortex-rdf-store';
import { DataFactory } from 'rdf-data-factory';

const df = new DataFactory();

const options: BuildOptions = { builder: 'Sorted', layout: 'Dictionary' };
const store = await VortexRdfStore.fromString(data, 'nquads', options);

console.log(store.layout()); // 'Dictionary'

const quads = await store.getQuads(null, df.namedNode('http://schema.org/name'), null, null);
for (const quad of quads) {
  console.log(quad.subject.value);
}
```

## Building

This package is built using [wasm-pack](https://rustwasm.github.io/wasm-pack/), targeting `web` — the same wasm build is shared by both environments:

```bash
# Build the wasm module (writes to pkg/web/)
npm run build

# Run the test suite (requires a build first)
npm test
```

`entry/node.js` and `entry/browser.js` are small, hand-written wrappers around that single build — they differ only in how they supply the `.wasm` bytes to the generated `init()` (a direct file read vs. the default `fetch`-based path), and `package.json`'s `exports` map picks the right one per environment.

There's no separate Node-targeted wasm build to maintain.

## License

MIT
