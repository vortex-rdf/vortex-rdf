import { describe, test, expect, vi } from 'vitest';

import { SELF_CHECK_BYTES, checkCounter, memoryRunFigures, wasmCounter } from '../bench/util.js';

const MiB = 1048576;

describe('memoryRunFigures', () => {
    test('memoryRunFigures takes each store as held after the reads minus disposed, and the peak above the baseline', () => {
        expect(memoryRunFigures({
            baseline: 100 * MiB,
            built: { held: 123.25 * MiB, disposed: 100 * MiB },
            loaded: { held: 117 * MiB, disposed: 100.5 * MiB },
            probeTotals: [110 * MiB, 131 * MiB, 125 * MiB],
        })).toEqual({ storeBuiltMb: 23.3, storeLoadedMb: 16.5, peakLiveMb: 31 });
    });

    test('memoryRunFigures leaves what a run did not measure null', () => {
        expect(memoryRunFigures({
            baseline: 100 * MiB,
            built: null,
            loaded: { held: 117 * MiB, disposed: 100.5 * MiB },
            probeTotals: [],
        })).toEqual({ storeBuiltMb: null, storeLoadedMb: 16.5, peakLiveMb: null });
    });

    test('memoryRunFigures clamps a store held below its disposed reading, and a peak below the baseline, at 0', () => {
        expect(memoryRunFigures({
            baseline: 100 * MiB,
            built: { held: 99 * MiB, disposed: 100 * MiB },
            loaded: { held: 100 * MiB, disposed: 100.25 * MiB },
            probeTotals: [90 * MiB, 99.5 * MiB],
        })).toEqual({ storeBuiltMb: 0, storeLoadedMb: 0, peakLiveMb: 0 });
    });
});

describe('checkCounter', () => {
    test('checkCounter passes a counter that sees the block, and frees it', () => {
        let live = 0;
        const asked: number[] = [];
        const hold = (b: number) => {
            asked.push(b);
            live += b;
            return () => { live -= b; };
        };
        expect(() => checkCounter('fake', () => live, hold)).not.toThrow();
        expect(live).toBe(0);
        expect(asked).toEqual([SELF_CHECK_BYTES]);
    });

    test('checkCounter names a counter that misses the block, and frees it anyway', () => {
        let released = false;
        const hold = () => () => { released = true; };
        expect(() => checkCounter('fake', () => 0, hold))
            .toThrow(/^memory self-check: the fake counter rose by 0\.0 MiB while an 8 MiB block was held/);
        expect(released).toBe(true);
    });
});

describe('wasmCounter', () => {
    const allocator = () => ({
        __wbindgen_malloc: vi.fn((_size: number, _align: number) => 4096),
        __wbindgen_free: vi.fn((_ptr: number, _size: number, _align: number) => {}),
    });

    test('wasmCounter forwards to the module and holds blocks through its allocator', () => {
        const stats = { liveBytes: 1, peakLiveBytes: 2, heapBytes: 3 };
        const mod = {
            setMemoryCounting: vi.fn((_on: boolean) => {}),
            memoryStats: vi.fn(() => stats),
            resetMemoryPeak: vi.fn(() => {}),
        };
        const alloc = allocator();
        // Unbound: the counter's methods are closures, passed around bare.
        const { name, hold, setCounting, stats: read, resetPeak } = wasmCounter('vortex-rdf', mod, alloc);
        expect(name).toBe('vortex-rdf');

        const release = hold(SELF_CHECK_BYTES);
        expect(alloc.__wbindgen_malloc).toHaveBeenCalledWith(8388608, 8);
        expect(alloc.__wbindgen_free).not.toHaveBeenCalled();
        release();
        expect(alloc.__wbindgen_free).toHaveBeenCalledWith(4096, 8388608, 8);

        setCounting(true);
        expect(mod.setMemoryCounting).toHaveBeenCalledWith(true);
        expect(read()).toBe(stats);
        expect(mod.memoryStats).toHaveBeenCalledTimes(1);
        resetPeak();
        expect(mod.resetMemoryPeak).toHaveBeenCalledTimes(1);
    });

    test('wasmCounter refuses a module built without the counter', () => {
        expect(() => wasmCounter('hdt-wasm', {}, allocator())).toThrow(/hdt-wasm has no memory-stats exports/);
    });
});
