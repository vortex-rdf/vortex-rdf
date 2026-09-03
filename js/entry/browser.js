// Browser/bundler ESM entry point. `init()` with no argument resolves the
// wasm file relative to its own module URL and `fetch`es it — the standard
// wasm-bindgen `web`-target path, understood natively by modern bundlers
// (Vite, webpack 5+, Rollup) via `new URL(..., import.meta.url)` and by a
// bare `<script type="module">` served from a CDN.
import init from '../pkg/web/vortex_rdf.js';

await init();

export {
    ArrowFFI,
    TermDict,
    VortexRdfStore,
    serializeRdf,
    deserializeRdf,
} from '../pkg/web/vortex_rdf.js';
