// Types shared by the Node and browser entry points: both expose the same
// public surface as the generated wasm bindings, minus the low-level
// `init`/`initSync` (each entry calls init for the caller). Kept in sync with
// the re-exports in entry/node.js and entry/browser.js.
export {
    ArrowFFI,
    TermDict,
    VortexRdfStore,
    serializeRdf,
    deserializeRdf,
} from '../pkg/web/vortex_rdf.js';
export type {
    ArrowOptions,
    BuildOptions,
    CodeRange,
    DictForm,
    LayoutStrategy,
    IndexType,
    OpenOptions,
    QuadColumn,
    RdfFormatName,
    TermEncoding,
} from '../pkg/web/vortex_rdf.js';
