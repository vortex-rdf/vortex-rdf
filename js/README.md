# Vortex-RDF for JavaScript
[![npm](https://img.shields.io/npm/v/@vortex-rdf/vortex-rdf-store.svg)](https://www.npmjs.com/package/@vortex-rdf/vortex-rdf-store)

JavaScript bindings for [Vortex-RDF](https://github.com/vortex-rdf/vortex-rdf), a columnar RDF store format built on Vortex: [`vortex-rdf-core`](https://crates.io/crates/vortex-rdf-core) compiled to WebAssembly, exposed through an RDF/JS-shaped API that builds stores from RDF text or quads, queries them with lazy zero-copy quads, mutates them in place, and round-trips them through `.vortex` bytes.

## Install

```bash
npm install @vortex-rdf/vortex-rdf-store
```

The package is ESM-only (no CommonJS `require`) and works identically in Node.js and browsers: the same `import` resolves to a Node entry point (reads the `.wasm` file off disk with `node:fs`) or a browser one (`fetch` via `import.meta.url`), and both await the wasm module's initialization.

> **Bundler note:** both entry points use top-level `await`. Vite and Rollup
> support this by default; webpack 5 needs
> `experiments: { topLevelAwait: true }` enabled in its config.

## Quick start

```javascript
import { VortexRdfStore, serializeRdf } from '@vortex-rdf/vortex-rdf-store';

const bytes = await serializeRdf('<http://ex/s> <http://ex/p> "o" .', 'turtle');
const store = await VortexRdfStore.fromBytes(bytes);
for await (const quad of store.match(null, 'http://ex/p', null, null)) {
  console.log(quad.subject.value, quad.object.value);
}
```

## Reading quads

### Loading data

```typescript
import { VortexRdfStore } from '@vortex-rdf/vortex-rdf-store';
import { Readable } from 'node:stream';
import type { Quad, Stream } from '@rdfjs/types';

// From RDF text. Formats: `ntriples`, `nquads`, `turtle`, `trig`, `n3`, `rdfxml`, `jsonld`
// (plus the short aliases `nt`, `nq`, `ttl`, `rdf`, `xml`).
const store = await VortexRdfStore.fromString(ttlData, 'turtle');

// From RDF/JS quads: an array ...
const quads: Quad[] = [...];
const store = await VortexRdfStore.fromQuads(quads);

// ... or any RDF/JS Stream<Quad>
const quadStream: Stream<Quad> = Readable.from(quads, { objectMode: true });
const store = await VortexRdfStore.fromQuads(quadStream);

// From Vortex bytes (e.g., fetched from a server)
const store = await VortexRdfStore.fromBytes(vortexBytes);

// Or start empty
const store = VortexRdfStore.empty();
```

`layout()` and `indexes()` report what a store was built with, as the same kebab-case names `BuildOptions` takes (`store.layout()` is `'dictionary'`; `store.indexes()` is an array such as `['secondary-by-reference']`).

### Querying

`match` implements the RDF/JS [`Source.match`](https://rdf.js.org/stream-spec/#source-interface) contract. It takes a `(subject, predicate, object, graph)` pattern — `null`/`undefined` or an RDF/JS `Variable` for a wildcard position, a bare string for a NamedNode IRI — and returns **synchronously** an RDF/JS `Stream<Quad>`:

```javascript
store.match(null, myPredicate, null, null)
  .on('data', (quad) => console.log(`${quad.subject.value} -> ${quad.object.value}`))
  .on('end', () => console.log('done'));
```

The returned stream also implements `Symbol.asyncIterator`, so it can be consumed with `for await` (in TypeScript, cast to `AsyncIterable<Quad>` since the declared type is `Stream<Quad>`):

```javascript
for await (const quad of store.match(null, myPredicate, null, null)) {
  console.log(quad.object.value);
}
```

`getQuads` is the array-returning counterpart (synchronous — no read path performs I/O, so there is nothing to await), and `countQuads` answers from the match's row selection without materializing a quad:

```javascript
const quads = store.getQuads(null, myPredicate, null, null);
const n = store.countQuads(null, myPredicate, null, null);
```

**Quads are lazy and zero-copy.** `match`/`getQuads` hand back quads backed by the store's columnar data. A term's string is decoded only when you read `.value`/`.termType`, and then interned, so iterating, counting, filtering, and `.equals` never materialize strings you don't use. Under the default `dictionary` layout, `.equals` between terms of the same store is an integer code compare (no decoding at all).

The quads implement the RDF/JS `Quad`/`Term` interface (`.subject.value`, `.equals`, …) and interoperate with foreign RDF/JS terms via `.equals` in both directions. They are lazy views into the producing store, so — unlike a plain data object — don't `structuredClone` them or rely on enumerating own properties.

Test membership of a single quad with `has` (an exact four-component lookup) and read the quad count with `size`; both return promises:

```javascript
if (await store.has(myQuad)) {
  console.log('present');
}
console.log(await store.size());
```

An invalid pattern term or a malformed quad object throws (or rejects) with an `Error`.

### Mutation

```javascript
await store.addQuad(myQuad);
await store.addQuads([quadA, quadB]);
await store.deleteQuad(existingQuad);
```

Mutations follow RDF/JS dataset semantics: adding a quad already present is a no-op, and deleting never rewrites the columnar data (rows are tombstoned).

Added quads accumulate in an in-memory tail beside the immutable base, so the store's indexes keep working across edits. When the tail reaches a tenth of the base's rows (never fewer than 4,096) or 100,000 rows, the store compacts itself back into one sorted, indexed array.

Prefer `addQuads` over a loop of `addQuad` calls; for bulk loading, build once with `fromString`/`fromQuads`.

### RDF text out

```javascript
const turtle = await store.toRdf('turtle');   // any supported format name
```

## Term codes (low-level)

Under the default `dictionary` layout, terms are stored as `u32` codes into a sorted term dictionary. `termDict()` is the one door to code↔term translation: it returns an immutable `TermDict` handle, or `undefined` when the store's rows aren't code-addressable (a non-dictionary layout, or added quads pending in the in-memory tail):

```javascript
const dict = store.termDict();   // TermDict | undefined
if (dict) {
  const code = dict.encode('<http://schema.org/name>');  // number | undefined
  console.log(dict.decode(code));                        // '<http://schema.org/name>'
}
```

`decode`/`encode` speak N-Triples term strings — `<iri>`, `_:blank`, `"lit"@lang`, `"lit"^^<dt>`, and `''` for the default graph. The handle is a snapshot: it keeps decoding correctly after the store is mutated, because it retains the dictionary its codes address. It is a wasm-side handle — call `free()` when done (also wired to `Symbol.dispose`, so `using` disposes it automatically).

`match`/`getQuads` are the way to read quads; the codes themselves come out of the [Arrow interface](#arrow-interface) below (`encoding: 'codes'`), for callers that join, count and de-duplicate in code space and decode each distinct term once.

## Arrow interface

`matchArrowIPC` hands a match to any Arrow consumer as an [Arrow IPC stream](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format) — the bytes `apache-arrow`'s `tableFromIPC`, DuckDB-WASM, Arquero or Perspective read:

```javascript
import { tableFromIPC } from 'apache-arrow';

const table = tableFromIPC(store.matchArrowIPC(null, myPredicate, null, null));
table.schema.fields.map((f) => f.name);                  // ['s', 'p', 'o', 'g'] — uint32 term codes
const terms = tableFromIPC(store.matchArrowIPC(null, null, null, null, { encoding: 'terms' }));
terms.getChild('o')?.get(0);                             // an N-Triples string, carried as a dictionary key
```

One record batch per decode chunk. `encoding: 'codes'` (the default) ships `uint32` term codes — join, filter and aggregate in code space, then decode the survivors through `termDict()`; `'terms'` wraps the same codes as an Arrow dictionary over the whole term dictionary, so engines see strings while carrying codes; `'strings'` materializes `string_view` N-Triples strings and is the one encoding every layout serves. `projection: ['o', 's']` restricts and orders the columns. The bytes are a copy out of wasm memory. This is the one engine-facing read surface of the bindings: a query planner or executor over a store consumes it, on this and every other language binding.

## Build options

`fromString`, `fromQuads` and `serializeRdf` accept an optional `BuildOptions` object; every field is optional. Quads are always sorted by subject → predicate → object → graph while the columnar array is built — that global order is what gives subject lookups their binary search and what every secondary index routes against. The wasm build sorts in memory (WebAssembly has no filesystem for an out-of-core builder) and takes no builder option.

```javascript
const store = await VortexRdfStore.fromString(data, 'nquads', {
  layout: 'dictionary',                   // 'dictionary' (default) | 'default' | 'typed-object'
  indexes: ['secondary-by-reference'],    // default: []
});
```

**`layout`** — how terms are encoded into columns. `'dictionary'` is the default in every vortex-rdf frontend (JS, Python and the CLI):

| Value | Notes |
| --- | --- |
| `'dictionary'` (default) | Terms replaced by codes into a sorted term dictionary. Most compact and fastest to query; backs the integer `.equals` fast path on lazy quads; added quads live in an in-memory string tail until serialized or compacted |
| `'default'` | All four terms as N-Triples strings |
| `'typed-object'` | Object split into kind/value/datatype/language columns |

**`indexes`** — secondary access paths, each costing extra space:

| Value | Notes |
| --- | --- |
| `'secondary-by-reference'` | Sorted predicate/object columns plus row-id back-references, so predicate-only and object-only patterns use a binary search instead of a full scan |
| `'secondary-by-copy'` | Two complete extra copies of the quad columns — one sorted by `(p, o, s, g)`, one by `(o, s, p, g)` — giving predicate- and object-bound patterns (including predicate+object prefix lookups) the same sorted access path subjects have |

## Bytes & files

```javascript
import { VortexRdfStore, serializeRdf, deserializeRdf } from '@vortex-rdf/vortex-rdf-store';

// Store <-> native-container bytes (the `.vortex` file format)
const bytes = await store.toBytes();
const again = await VortexRdfStore.fromBytes(bytes);

// One-shot conversions, without holding a store
const vortex = await serializeRdf(turtleText, 'turtle', { layout: 'dictionary' });
const nquads = await deserializeRdf(vortex, 'nquads');
```

`toBytes` writes the same exchange format the CLI and the Python bindings read, so a buffer can be written to disk as a `.vortex` file or handed across bindings.

A `VortexRdfStore` is a wasm-side handle: call `free()` when you are done with it, or declare it with `using` (`free` is wired to `Symbol.dispose`); an unfreed store is reclaimed only when its JS wrapper is garbage-collected.

## TypeScript

The package ships a hand-written `src/api.d.ts` typed against `@rdfjs/types`, embedded into the wasm-pack output; it is the public surface, checked by `npm run typecheck`.

```typescript
import { VortexRdfStore, type BuildOptions } from '@vortex-rdf/vortex-rdf-store';
import { DataFactory } from 'rdf-data-factory';

const df = new DataFactory();
const options: BuildOptions = { layout: 'dictionary' };
const store = await VortexRdfStore.fromString(data, 'nquads', options);

for (const quad of store.getQuads(null, df.namedNode('http://schema.org/name'), null, null)) {
  console.log(quad.subject.value);
}
```

## Development

The package is built with [wasm-pack](https://rustwasm.github.io/wasm-pack/) targeting `web`; one build (`pkg/web/`) backs both environments. `entry/node.js` and `entry/browser.js` are small hand-written wrappers that differ only in how they supply the `.wasm` bytes to the generated `init()`, and `package.json`'s `exports` map picks one per environment.

```bash
npm run build       # wasm-pack build into pkg/web/ (build:fast skips wasm-opt)
npm test            # vitest suite (requires a build)
npm run typecheck   # tsc over the tests and benchmarks, against the published API
```

Benchmarks live in [bench/](bench/README.md).

## License

MIT
