// Types of the `/arrow` entry: the main entry's surface, `matchArrow` on the
// store — a module augmentation, so it appears on every `VortexRdfStore` once
// this entry is imported, which is when the method exists at runtime — and
// the `MatchView` it returns. Only this file references apache-arrow, so a
// consumer of the main entry alone never resolves it.
import type { Term } from '@rdfjs/types';
import type { Table } from 'apache-arrow';
import type { ArrowOptions } from '../pkg/web/vortex_rdf.js';

export * from './types.js';

/**
 * A match as an Arrow JS `Table`, with the lifetime of what backs it.
 *
 * Zero-copy (`zeroCopy === true`, a runtime with resizable wasm buffers):
 * `table` reads the store's memory in place and the wasm-side buffers live
 * until `free()` — read, or `toTable()` a copy to keep, then free; a vector
 * or typed array read after `free()` reads recycled memory. Fallback
 * (`zeroCopy === false`): `table` is a copy the parse took, the wasm side was
 * freed at once, and `free()` only drops it. Also wired to `Symbol.dispose`.
 */
export class MatchView {
    /** Whether `table` is a view over wasm memory rather than a copy. */
    readonly zeroCopy: boolean;
    /** The match as an Arrow JS `Table`. Throws after `free()`. */
    readonly table: Table;
    /**
     * A `Table` owned by JS: a deep copy out of wasm memory when this view is
     * zero-copy, the (already copied) `table` itself otherwise. What to hold
     * past `free()`, `structuredClone` or transfer to a Worker.
     */
    toTable(): Table;
    /**
     * The match as Arrow IPC stream bytes — what `tableFromIPC` reads back,
     * and the transferable currency for a store hosted in a Worker
     * (`postMessage(bytes, [bytes.buffer])`). Under `terms` each column
     * carries its own dictionary batch.
     */
    toIPC(): Uint8Array;
    /** Release the wasm-side buffers (zero-copy) and drop the table. Idempotent. */
    free(): void;
    [Symbol.dispose](): void;
}

declare module '../pkg/web/vortex_rdf.js' {
    interface VortexRdfStore {
        /**
         * The quads matching a pattern as an Arrow JS `Table`, inside a
         * `MatchView` that owns what backs it: `matchArrowFFI` parsed by
         * `@vortex-rdf/arrow-js-ffi` — zero-copy views over wasm memory on a
         * runtime with resizable wasm buffers, a copy taken at parse time
         * elsewhere (`view.zeroCopy` says which). Columns, encodings, `keep`,
         * `offset` and `limit` as `matchArrowFFI`. Returns synchronously;
         * throws on what `matchArrowFFI` throws on. Call `free()` (or
         * declare the view with `using`) once done reading.
         */
        matchArrow(subject?: Term | string | null, predicate?: Term | string | null, object?: Term | string | null, graph?: Term | string | null, options?: ArrowOptions): MatchView;
    }
}
