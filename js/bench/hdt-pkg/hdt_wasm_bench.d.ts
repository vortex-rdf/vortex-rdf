/* tslint:disable */
/* eslint-disable */

export class Hdt {
    free(): void;
    [Symbol.dispose](): void;
    /**
     * ids: flat Int32Array of IDs [s1, p1, o1, s2, p2, o2, ...].
     * Returns string triples as a flat array of strings [s1, p1, o1, s2, p2, o2, ...].
     * WASM memory is limited, several million triple IDs may lead to OOM crashes reported as "RuntimeError: unreachable executed"
     */
    ids_to_strings(ids: Uint32Array | number[]): string[];
    constructor(data: Uint8Array | number[]);
    /**
     * Returns a flat Int32Array of IDs [s1, p1, o1, s2, p2, o2, ...].
     * There is some duplication with constants in triple patterns but as we only return 32 bit integers this should only be a few MB even for millions of results.
     * On the other hand this hopefully allows performant transitions between WASM and JavaScript.
     * Also this is expected to often be used with pagination and should use CPU cache better when using a specific "window".
     */
    triple_ids_with_pattern(sp?: string | null, pp?: string | null, op?: string | null): Uint32Array;
}

export function memoryStats(): { liveBytes: number, peakLiveBytes: number, heapBytes: number };

export function resetMemoryPeak(): void;

export function setMemoryCounting(on: boolean): void;

export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module;

export interface InitOutput {
    readonly memory: WebAssembly.Memory;
    readonly __wbg_hdt_free: (a: number, b: number) => void;
    readonly hdt_ids_to_strings: (a: number, b: number, c: number) => [number, number, number, number];
    readonly hdt_new: (a: number, b: number) => [number, number, number];
    readonly hdt_triple_ids_with_pattern: (a: number, b: number, c: number, d: number, e: number, f: number, g: number) => [number, number];
    readonly memoryStats: () => [number, number, number];
    readonly resetMemoryPeak: () => void;
    readonly setMemoryCounting: (a: number) => void;
    readonly __wbindgen_free: (a: number, b: number, c: number) => void;
    readonly __wbindgen_exn_store: (a: number) => void;
    readonly __externref_table_alloc: () => number;
    readonly __wbindgen_externrefs: WebAssembly.Table;
    readonly __wbindgen_malloc: (a: number, b: number) => number;
    readonly __wbindgen_realloc: (a: number, b: number, c: number, d: number) => number;
    readonly __externref_table_dealloc: (a: number) => void;
    readonly __externref_drop_slice: (a: number, b: number) => void;
    readonly __wbindgen_start: () => void;
}

export type SyncInitInput = BufferSource | WebAssembly.Module;

/**
 * Instantiates the given `module`, which can either be bytes or
 * a precompiled `WebAssembly.Module`.
 *
 * @param {{ module: SyncInitInput }} module - Passing `SyncInitInput` directly is deprecated.
 *
 * @returns {InitOutput}
 */
export function initSync(module: { module: SyncInitInput } | SyncInitInput): InitOutput;

/**
 * If `module_or_path` is {RequestInfo} or {URL}, makes a request and
 * for everything else, calls `WebAssembly.instantiate` directly.
 *
 * @param {{ module_or_path: InitInput | Promise<InitInput> }} module_or_path - Passing `InitInput` directly is deprecated.
 *
 * @returns {Promise<InitOutput>}
 */
export default function __wbg_init (module_or_path?: { module_or_path: InitInput | Promise<InitInput> } | InitInput | Promise<InitInput>): Promise<InitOutput>;
