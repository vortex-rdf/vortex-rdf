# Vortex-RDF for JavaScript
[![npm](https://img.shields.io/npm/v/@vortex-rdf/vortex-rdf-store.svg)](https://www.npmjs.com/package/@vortex-rdf/vortex-rdf-store)

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

When you just want the matches as an array, `getQuads` is the array-returning counterpart (synchronous — no read path performs I/O, so there is nothing to await):

```javascript
const quads = store.getQuads(null, myPredicate, null, null);
console.log(`Found ${quads.length} results`);
```

**Quads are lazy and zero-copy.** `match`/`getQuads` don't build eager term objects — they hand back quads backed by the store's columnar data. A term's string is decoded only when you read `.value`/`.termType`, and then interned, so iterating, counting, filtering, and `.equals` never materialize strings you don't use. Under the default `dictionary` layout, `.equals` between terms of the same store is an **integer code compare** (no decoding at all). 

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

Quads are always sorted by subject → predicate → object → graph while the columnar array is built — that global order is what gives subject lookups their binary search and what every secondary index routes against.

```javascript
const store = await VortexRdfStore.fromString(data, 'nquads', {
  layout: 'dictionary',                   // 'dictionary' (default) | 'default' | 'typed-object'
  indexes: ['secondary-by-reference'],    // default: []
});
```

Core's out-of-core builder spills sorted runs to disk — a filesystem WebAssembly does not have — so the wasm build always sorts in memory and takes no builder option.

**`layout`** — how terms are encoded into columns:

| Value | Notes |
| --- | --- |
| `'dictionary'` (default) | Terms replaced by codes into a sorted term dictionary. Most compact and fastest to query; backs the integer `.equals` fast path on lazy quads; added quads live in an in-memory string tail until serialized |
| `'default'` | All four terms as N-Triples strings |
| `'typed-object'` | Object split into kind/value/datatype/language columns |

**`indexes`** 

- `'secondary-by-reference'` adds sorted predicate/object columns plus row-id back-references, so predicate-only and object-only patterns use a binary search instead of a full scan. 
- `'secondary-by-copy'` embeds two complete extra copies of the quad columns — one sorted by `(p, o, s, g)`, one by `(o, s, p, g)` — giving predicate- and object-bound patterns (including combined predicate+object lookups, resolved in one prefix search) the same sorted access path subjects have, at roughly 2× the storage. Both cost extra space.

The default `{ layout: 'dictionary' }` already gives compact, code-based lazy reads and a binary-searchable subject column. Adding an index on top is what a predicate- or object-heavy workload wants:

```javascript
{ 
    layout: 'dictionary', 
    indexes: ['secondary-by-reference'] 
};
```

### Term codes (low-level)

Under the default `dictionary` layout, terms are stored as `u32` codes into a sorted term dictionary. `termDict()` is the one door to code↔term translation: it returns an immutable `TermDict` handle, or `undefined` when the store's rows aren't code-addressable (a non-dictionary layout, or added quads pending in the in-memory tail):

```javascript
const dict = store.termDict();   // TermDict | undefined
if (dict) {
  const code = dict.encode('<http://schema.org/name>');  // number | undefined
  console.log(dict.decode(code));                        // '<http://schema.org/name>'
}
```

`decode`/`encode` speak N-Triples term strings — `<iri>`, `_:blank`, `"lit"@lang`, `"lit"^^<dt>`, and `''` for the default graph. The handle is a snapshot: it keeps decoding correctly after the store is mutated, because it retains the dictionary its codes address. It is a wasm-side handle — call `free()` when done (also wired to `Symbol.dispose`, so `using` disposes it automatically).

`matchCodes` is its pattern-matching counterpart: it resolves a pattern to the matched rows' raw term codes — four columnar `Uint32Array`s plus a `length` — without materializing any term strings, and returns `null` under the same conditions `termDict()` returns `undefined`:

```javascript
const cols = store.matchCodes(null, myPredicate, null, null);
if (cols) {
  console.log(cols.length, dict.decode(cols.o[0]));
}
```

`matchCodes` is a prototype read path; `match`/`getQuads` are the supported way to read quads.

### Helper functions

For one-shot conversions without holding a store:

```javascript
import { rdf_to_vortex, vortex_to_rdf } from '@vortex-rdf/vortex-rdf-store';

const bytes = await rdf_to_vortex(turtleText, 'turtle', { layout: 'dictionary' });
const text  = await vortex_to_rdf(bytes, 'nquads');

// N-Quads is just another format
const bytes2 = await rdf_to_vortex(nquadsText, 'nquads');
const text2  = await vortex_to_rdf(bytes2, 'nquads');
```

### TypeScript support

The package ships typings generated from the Rust bindings, using RDF-JS types.

```typescript
import { VortexRdfStore, type BuildOptions } from '@vortex-rdf/vortex-rdf-store';
import { DataFactory } from 'rdf-data-factory';

const df = new DataFactory();

const options: BuildOptions = { layout: 'dictionary' };
const store = await VortexRdfStore.fromString(data, 'nquads', options);

console.log(store.layout()); // 'dictionary'

const quads = store.getQuads(null, df.namedNode('http://schema.org/name'), null, null);
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

Types are checked separately from the build, since `wasm-pack` doesn't run `tsc`:

```bash
npm run typecheck   # tests and benchmarks, both against the published API
```

## Benchmarks

Three surfaces, kept separate because they answer different questions and are trustworthy in different ways. All require `npm run build` first.

| Command | Question | Output |
| --- | --- | --- |
| `npm run bench` | How do we compare to other JS RDF stores? | `bench/results.json`, rendered to the dashboard |
| `npm run bench:codspeed` | Did this PR regress anything? | uploaded to CodSpeed |
| `npm run bench:dict-memory` | Where is wasm memory actually going? | printed; `bench/dict-memory.json` |

**`npm run bench`** runs each store — the Vortex build variants, [rdf-stores.js](https://github.com/rubensworks/rdf-stores.js), and [oxigraph](https://github.com/oxigraph/oxigraph) — through the same query, mutation, and serialization workload, one adapter per child process so peak RSS is attributable and no store's garbage taints another's timings. It is wall-clock, so it is only as stable as the machine it runs on; it is deliberately **not** uploaded to CodSpeed. `scripts/render_bench_dashboard.py` turns its `results.json` plus a `cargo bench --bench benchmark` run into `public/index.html`.

The dataset is generated by `genDataset` in [bench/datasets.ts](bench/datasets.ts), with term cardinality as an explicit knob — real RDF has terms scaling with rows, and a generator that draws millions of quads from a handful of IRIs makes every store's term handling invisible. Tunable by env var:

| Var | Default | Meaning |
| --- | --- | --- |
| `BENCH_DIM` | 128 | triples dataset is `D³` rows |
| `BENCH_DIM_QUADS` | 32 | quads dataset is `Dq⁴` rows |
| `BENCH_SUBJ_RATIO` | 0.1 | distinct subjects / rows — the reciprocal of triples per subject |
| `BENCH_OBJ_RATIO` | 0.5 | distinct objects / rows |
| `BENCH_PREDICATES` | 32 | distinct predicates (a closed vocabulary) |
| `BENCH_GRAPHS` | 1 | distinct named graphs; 1 means default graph only |
| `BENCH_LITERAL_FRAC` | 0.4 | fraction of objects that are literals |
| `MUT_BATCH` | 10000 | quads per add/delete batch |

The two ratios decide how much of the dataset is *terms*: the dictionary holds `subjectRatio + objectRatio` of them per quad. At the defaults that is 2,097,152 triples over ~1.26M distinct terms, ten triples describing each subject, and probes spanning five orders of magnitude of selectivity (1 row for a fully-bound pattern, ~10 for a subject, ~3% for a predicate).

`BENCH_SUBJ_RATIO=1.0` gives every row its own subject, which puts more distinct terms in the dictionary than there are rows. That is a useful dictionary-stress configuration — it is what `bench:dict-memory` asks for — but a poor default for the comparison: it makes five of the seven probes match two rows or fewer, so the timings become almost entirely query setup with the decode path barely exercised.

Each adapter process peaks at 3–6 GB — check available memory before running, because a swapping run produces numbers that look like regressions.

**`npm run bench:dict-memory`** is the only instrument that can attribute wasm memory; `peakRssMb` in the comparative run is the right cross-library number but cannot say *what* holds the memory. Wasm linear memory never shrinks, so it works by differentials — several stores held live at once, memory read after each, slope against store count — and every config runs in its own process. It defaults to a single config as a regression check against the figures recorded when FSST landed; passing several `DICT_MEM_RATIOS` sweeps term cardinality instead and fits the per-term cost. The header of [bench/dict-memory.bench.ts](bench/dict-memory.bench.ts) documents the method and the invocations.

## License

MIT
