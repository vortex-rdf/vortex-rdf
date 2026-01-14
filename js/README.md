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

### Loading Data

```javascript
import { SimpleDictionaryStore, ChainedHashStore } from 'vortex-rdf';

// From a Vortex binary file (SimpleDictionaryStore)
const store = await SimpleDictionaryStore.fromBytes(vortexBytes);

// Or from a Turtle/N-Quads string
const store = await SimpleDictionaryStore.fromString(ttlData, "turtle");

// Or create a new empty store
const store = SimpleDictionaryStore.empty();
```

### Querying

```javascript
// Perform a match (subject, predicate, object, graph)
// Patterns can be Iris, Literals, or null/undefined for variables
const matches = await store.match(null, "http://schema.org/name", null, null);

console.log(`Found ${matches.size()} results`);

// .values() is async and returns an iterator
const iterator = await matches.values();
for (const quad of iterator) {
  console.log(`${quad.subject.value} -> ${quad.object.value}`);
}
```

### Manipulation

```javascript
await store.addQuad(myQuad);
await store.deleteQuad(existingQuad);
```

### TypeScript Support

Both `SimpleDictionaryStore` and `ChainedHashStore` implement the `VortexStore` interface.

```typescript
import { SimpleDictionaryStore, VortexStore } from 'vortex-rdf';
import { Quad, NamedNode, Term } from '@rdfjs/types';

async function queryExample(store: VortexStore) {
  // Methods are strictly typed to RDF-JS types
  const predicate: NamedNode = {
    termType: 'NamedNode',
    value: 'http://schema.org/name',
    equals: (other: Term) => other.termType === 'NamedNode' && other.value === 'http://schema.org/name'
  };

  // Note: match returns the concrete store type (SimpleDictionaryStore or ChainedHashStore)
  const matches = await store.match(null, predicate, null, null);
  
  // We can iterate values
  const iterator = await matches.values();
  for (const quad of iterator) {
    const s: Quad['subject'] = quad.subject;
    console.log(`Subject: ${s.value}`);
  }
}
```

## Building

This package is built using [wasm-pack](https://rustwasm.github.io/wasm-pack/).

```bash
# Build for Node.js
npm run build

# Build for the Web
npm run build:web
```

## License

MIT
