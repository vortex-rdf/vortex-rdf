// Node ESM entry point. Loads the wasm bytes directly via `node:fs` rather
// than through the generated `init()`'s default `fetch(...)` path — Node's
// `fetch` does not support `file:` URLs, so the bundled default would fail
// here even though it works for a real browser or bundler.
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import init, { init_panic_hook } from '../pkg/web/vortex_rdf.js';

const wasmPath = fileURLToPath(new URL('../pkg/web/vortex_rdf_bg.wasm', import.meta.url));
await init({ module_or_path: await readFile(wasmPath) });
init_panic_hook();

export {
    TermDict,
    VortexRdfStore,
    rdf_to_vortex,
    vortex_to_rdf,
} from '../pkg/web/vortex_rdf.js';
