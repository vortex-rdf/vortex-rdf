// The Arrow entry for browsers and bundlers: everything the main entry
// exports, plus `VortexRdfStore.prototype.matchArrow` and the `MatchView` it
// returns. Importing this subpath is what installs `matchArrow`; the main
// entry never references `@vortex-rdf/arrow-js-ffi` or `apache-arrow`, so a
// consumer that does not import it needs neither installed.
import init from '../pkg/web/vortex_rdf.js';
import { VortexRdfStore } from './browser.js';
import { MatchView, attachMatchArrow, enableZeroCopy } from './match-view.js';

// `./browser.js` has initialized the module, so `init()` hands back its
// exports (the memory among them) without loading anything.
const { memory } = await init();
attachMatchArrow(VortexRdfStore, memory, enableZeroCopy(memory));

export * from './browser.js';
export { MatchView };
