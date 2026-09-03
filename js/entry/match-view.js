// The Arrow view of a match, and what installs `matchArrow` on the store
// class. `matchArrowFFI` leaves a match as C Data Interface structs in wasm
// memory; `@vortex-rdf/arrow-js-ffi` parses them into an Arrow JS `Table` —
// as views over the module's memory where the runtime keeps those valid
// across memory growth, as a copy taken at parse time everywhere else — and
// the `MatchView` owns the wasm-side handle until `free()`.
import { parseTable } from '@vortex-rdf/arrow-js-ffi';
import { tableToIPC } from 'apache-arrow';

/**
 * Make the module's memory grow in place, so that fixed-length views into
 * `memory.buffer` stay valid across growth: `WebAssembly.Memory.prototype
 * .toResizableBuffer()` (Chrome 144, Firefox 145, Safari 26.2; Node 24.5+
 * behind `--experimental-wasm-rab-integration`) turns the buffer into a
 * resizable `ArrayBuffer` that keeps its identity and grows instead of
 * detaching. It needs a declared memory maximum, which the build sets.
 * Returns whether views are safe; `false` on a runtime without the call, or
 * where it throws, and every parse then copies.
 */
export function enableZeroCopy(memory) {
    if (typeof memory.toResizableBuffer !== 'function') return false;
    try {
        memory.toResizableBuffer();
    } catch {
        return false;
    }
    return memory.buffer.resizable === true;
}

/**
 * A match as an Arrow JS `Table`, with the lifetime of what backs it.
 *
 * Zero-copy (`zeroCopy === true`): `table` reads the store's memory in place
 * and the wasm-side buffers live until `free()` — read, or `toTable()` a copy
 * to keep, then free; a vector or typed array read after `free()` reads
 * recycled memory. Fallback (`zeroCopy === false`): `table` is a copy the
 * parse took, the wasm side was freed at once, and `free()` only drops it.
 */
export class MatchView {
    #table;
    #handle;
    #memory;
    #zeroCopy;

    constructor(table, handle, memory, zeroCopy) {
        this.#table = table;
        this.#handle = handle;
        this.#memory = memory;
        this.#zeroCopy = zeroCopy;
    }

    /** Whether `table` is a view over wasm memory rather than a copy. */
    get zeroCopy() {
        return this.#zeroCopy;
    }

    /** The match as an Arrow JS `Table`. Throws after `free()`. */
    get table() {
        if (this.#table === null) throw new Error('MatchView used after free()');
        return this.#table;
    }

    /**
     * A `Table` owned by JS: a deep copy out of wasm memory when this view is
     * zero-copy, the (already copied) `table` itself otherwise. What to hold
     * past `free()`, `structuredClone` or transfer to a Worker.
     */
    toTable() {
        const table = this.table;
        if (!this.#zeroCopy) return table;
        return parseTable(this.#memory.buffer, this.#handle.arrayPtrs(), this.#handle.schemaPtr(), true);
    }

    /**
     * The match as Arrow IPC stream bytes — what `tableFromIPC` reads back,
     * and the transferable currency for a store hosted in a Worker
     * (`postMessage(bytes, [bytes.buffer])`). Under `terms` each column
     * carries its own dictionary batch.
     */
    toIPC() {
        return tableToIPC(this.table, 'stream');
    }

    /** Release the wasm-side buffers (zero-copy) and drop the table. Idempotent. */
    free() {
        this.#table = null;
        if (this.#handle !== null) {
            this.#handle.free();
            this.#handle = null;
        }
    }
}
MatchView.prototype[Symbol.dispose] = MatchView.prototype.free;

/**
 * Install `matchArrow` on the store class: `matchArrowFFI` plus the parse,
 * against `memory` (the module's), as views when `zeroCopy` and as a copy
 * otherwise — in which case the wasm side is freed before returning.
 */
export function attachMatchArrow(StoreClass, memory, zeroCopy) {
    StoreClass.prototype.matchArrow = function matchArrow(subject, predicate, object, graph, options) {
        const handle = this.matchArrowFFI(subject, predicate, object, graph, options);
        let table;
        try {
            table = parseTable(memory.buffer, handle.arrayPtrs(), handle.schemaPtr(), !zeroCopy);
        } catch (error) {
            handle.free();
            throw error;
        }
        if (zeroCopy) return new MatchView(table, handle, memory, true);
        handle.free();
        return new MatchView(table, null, null, false);
    };
}
