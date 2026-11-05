// The Vortex bindings the comparative and dict-memory workers load: the
// bench's own build in pkg/bench (`npm run build:bench-wasm`), compiled with
// the js crate's `memory-stats` feature, so the module carries the live-byte
// counter the memory runs read. The published build (pkg/web, `npm run
// build`) never carries it, and the instrumented CodSpeed suite keeps
// loading that one through the package entry.
//
// Loaded the way entry/node.js loads pkg/web: init with the wasm bytes,
// because Node's fetch does not speak file: URLs, then pin each store to the
// iterables over it. Both modules are imported through computed specifiers,
// so TypeScript resolves neither: pkg/bench need not exist for `npm run
// typecheck`, which types it as the published build, the same API.

import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

import type { VortexRdfStore as PublishedStore } from '@vortex-rdf/vortex-rdf-store';
import { wasmCounter, type WasmCounter } from './util.js';

export type { BuildOptions } from '@vortex-rdf/vortex-rdf-store';

type Bindings = typeof import('../pkg/web/vortex_rdf.js');

const bindings = (await import(new URL('../pkg/bench/vortex_rdf.js', import.meta.url).href).catch((e: unknown) => {
    throw new Error(
        'no bench build of the Vortex bindings at js/pkg/bench — run `npm run build:bench-wasm` '
        + '(scripts/refresh.sh --only js builds it)',
        { cause: e });
})) as Bindings;

/** The exports of the bench build's wasm instance: its memory and allocator. */
export const vortexWasm = await bindings.default({
    module_or_path: readFileSync(fileURLToPath(new URL('../pkg/bench/vortex_rdf_bg.wasm', import.meta.url))),
});

const { pinStoresToIterables } = (await import(new URL('../entry/pin.js', import.meta.url).href)) as {
    pinStoresToIterables(store: Bindings['VortexRdfStore']): void;
};
pinStoresToIterables(bindings.VortexRdfStore);

export const VortexRdfStore = bindings.VortexRdfStore;
export type VortexRdfStore = PublishedStore;

/** The bench build's live-byte counter. */
export const vortexCounter = (): WasmCounter => wasmCounter('vortex-rdf', bindings, vortexWasm);
