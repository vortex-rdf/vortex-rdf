// Browser/bundler ESM entry point. `init()` with no argument resolves the
// wasm file relative to its own module URL and `fetch`es it — the standard
// wasm-bindgen `web`-target path, understood natively by modern bundlers
// (Vite, webpack 5+, Rollup) via `new URL(..., import.meta.url)` and by a
// bare `<script type="module">` served from a CDN.
import init from '../pkg/web/vortex_rdf.js';

await init();

export {
    VortexRdfStore,
    init_panic_hook,
    rdf_to_vortex,
    vortex_to_rdf,
    nquads_to_vortex,
    vortex_to_nquads,
} from '../pkg/web/vortex_rdf.js';
