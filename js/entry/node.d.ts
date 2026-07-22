// Types for the Node entry point: the same public surface as the generated
// wasm bindings, minus the low-level `init`/`initSync` (the entry calls init
// for the caller). Kept in sync with entry/node.js's re-exports.
export {
    VortexRdfStore,
    init_panic_hook,
    rdf_to_vortex,
    vortex_to_rdf,
    nquads_to_vortex,
    vortex_to_nquads,
} from '../pkg/web/vortex_rdf.js';
export type {
    BuildOptions,
    BuildOptionsInput,
    BuilderStrategy,
    LayoutStrategy,
    IndexType,
    RdfFormatName,
} from '../pkg/web/vortex_rdf.js';
