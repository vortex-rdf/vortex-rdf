import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { beforeAll, describe, expect, test } from 'vitest';
import * as published from '../pkg/web/vortex_rdf.js';

// A computed specifier: a literal one makes TypeScript resolve the module, so
// typecheck would fail wherever `pkg/bench` was never built.
const BENCH_URL = new URL('../pkg/bench/vortex_rdf.js', import.meta.url).href;
const bench = (await import(BENCH_URL).catch(() => null)) as
    | typeof import('../pkg/web/vortex_rdf.js')
    | null;

interface MemoryCounter {
    setMemoryCounting(on: boolean): void;
    memoryStats(): { liveBytes: number; peakLiveBytes: number; heapBytes: number };
    resetMemoryPeak(): void;
}
const counter = bench as unknown as Partial<MemoryCounter> | null;

const ROWS_NQ = Array.from(
    { length: 20_000 },
    (_, i) => `<http://example.org/s${i}> <http://example.org/p${i % 2}> "o${i % 5}" .`,
).join('\n');

test('the published build exports no memory counter', () => {
    const exported = published as unknown as Record<string, unknown>;
    for (const name of ['setMemoryCounting', 'memoryStats', 'resetMemoryPeak']) {
        expect(exported[name]).toBeUndefined();
    }
});

describe.skipIf(!counter?.memoryStats)('the bench build', () => {
    // The suite body runs even when skipped, so everything that needs the bench
    // module waits for `beforeAll`.
    const setMemoryCounting = (on: boolean) => counter!.setMemoryCounting!(on);
    const memoryStats = () => counter!.memoryStats!();
    const resetMemoryPeak = () => counter!.resetMemoryPeak!();
    let output: Awaited<ReturnType<NonNullable<typeof bench>['default']>>;
    beforeAll(async () => {
        output = await bench!.default({
            module_or_path: await readFile(
                fileURLToPath(new URL('../pkg/bench/vortex_rdf_bg.wasm', import.meta.url)),
            ),
        });
    });

    const build = async () => {
        const store = await bench!.VortexRdfStore.fromString(ROWS_NQ, 'nquads');
        store.countQuads();
        return store;
    };

    test('counting is off until setMemoryCounting(true)', async () => {
        (await build()).free();
        expect(memoryStats()).toMatchObject({ liveBytes: 0, peakLiveBytes: 0 });
    });

    test("heapBytes is the wasm memory's byteLength", () => {
        expect(memoryStats().heapBytes).toBe(output.memory.buffer.byteLength);
    });

    test('liveBytes rises on a build and falls back to its level before the store on free()', async () => {
        setMemoryCounting(true);
        try {
            (await build()).free(); // warm up
            const before = memoryStats().liveBytes;
            const store = await build();
            const built = memoryStats().liveBytes;
            expect(built - before).toBeGreaterThan(256 * 1024);
            store.free();
            expect(memoryStats().liveBytes).toBe(before);
        } finally {
            setMemoryCounting(false);
        }
    });

    test('peakLiveBytes keeps the highest liveBytes since resetMemoryPeak()', async () => {
        setMemoryCounting(true);
        try {
            resetMemoryPeak();
            const start = memoryStats();
            expect(start.peakLiveBytes).toBe(start.liveBytes);
            const store = await build();
            const built = memoryStats();
            expect(built.peakLiveBytes).toBeGreaterThanOrEqual(built.liveBytes);
            expect(built.liveBytes).toBeGreaterThan(start.liveBytes);
            store.free();
            const freed = memoryStats();
            expect(freed.peakLiveBytes).toBeGreaterThan(freed.liveBytes);
            resetMemoryPeak();
            const reset = memoryStats();
            expect(reset.peakLiveBytes).toBe(reset.liveBytes);
        } finally {
            setMemoryCounting(false);
        }
    });
});
