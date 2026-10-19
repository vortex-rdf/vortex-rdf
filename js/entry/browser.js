// Browser/bundler ESM entry point. `init()` with no argument resolves the
// wasm file relative to its own module URL and `fetch`es it — the standard
// wasm-bindgen `web`-target path, understood natively by modern bundlers
// (Vite, webpack 5+, Rollup) via `new URL(..., import.meta.url)` and by a
// bare `<script type="module">` served from a CDN.
import init, { VortexRdfStore } from '../pkg/web/vortex_rdf.js';

await init();

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
