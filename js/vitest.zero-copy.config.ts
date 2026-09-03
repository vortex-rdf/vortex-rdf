// The suite again, in workers started with V8's resizable-buffer integration
// for wasm memory (`WebAssembly.Memory.prototype.toResizableBuffer`: Node
// 24.5+ behind this flag; on by default in Chrome 144, Firefox 145 and Safari
// 26.2). The `/arrow` entry then hands out zero-copy views instead of
// parse-time copies, and test/arrow.test.ts asserts that path. `npm run
// test:zero-copy`; the plain `npm test` covers the copy fallback.
import { defineConfig } from 'vitest/config';

export default defineConfig({
    test: {
        execArgv: ['--experimental-wasm-rab-integration'],
        // Seen by test/arrow.test.ts, which then fails rather than passing
        // vacuously if the flag did not reach the worker.
        env: { VORTEX_RDF_EXPECT_ZERO_COPY: '1' },
    },
});
