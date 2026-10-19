// Node ESM entry point. Loads the wasm bytes directly via `node:fs` rather
// than through the generated `init()`'s default `fetch(...)` path — Node's
// `fetch` does not support `file:` URLs, so the bundled default would fail
// here even though it works for a real browser or bundler.
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import init, { VortexRdfStore } from '../pkg/web/vortex_rdf.js';

const wasmPath = fileURLToPath(new URL('../pkg/web/vortex_rdf_bg.wasm', import.meta.url));
await init({ module_or_path: await readFile(wasmPath) });

// The chunk and batch iterables read their store: each holds it, so the store
// is not collected (and freed) while an iterable over it can still pull.
for (const name of ['matchCodeChunks', 'matchCodesMany']) {
    const read = VortexRdfStore.prototype[name];
    VortexRdfStore.prototype[name] = function (...args) {
        const iterable = read.apply(this, args);
        if (iterable !== null) iterable._store = this;
        return iterable;
    };
}

export {
    TermDict,
    VortexRdfStore,
    serializeRdf,
    deserializeRdf,
} from '../pkg/web/vortex_rdf.js';
