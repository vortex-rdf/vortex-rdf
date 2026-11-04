// match() read a chunk at a time: its stream yields the quads getQuads
// returns, in getQuads' order, pulling them from a QuadChunks handle 8,192
// rows at a time and only once the last chunk's quads are taken. The handle is
// freed at the end, on an early exit, when a data listener throws and when
// freeing the store fails the stream, and the stream reads the store as it was
// when match was called.
import { afterEach, describe, expect, test, vi, type MockInstance } from 'vitest';
import type { Quad, Stream } from '@rdfjs/types';
import { VortexRdfStore, type BuildOptions } from '@vortex-rdf/vortex-rdf-store';
// The wasm handle under match's stream, which the package entries do not re-export.
import { QuadChunks } from '../pkg/web/vortex_rdf.js';
import { CHUNKED_PATTERNS, CHUNKED_ROWS, NEW_QUAD, VARIANTS, chunkedNquads, collect, df, quadKey } from './support.js';

afterEach(() => vi.restoreAllMocks());

const keys = (q: Quad[]) => q.map(quadKey);

/** The rows of each chunk a spied `next` returned, `undefined` for the end. */
const sizes = (next: MockInstance<QuadChunks['next']>) =>
    next.mock.results.map((r) => (r.value as { length: number } | undefined)?.length);

/** A store built from `chunkedNquads()`. */
const chunkedStore = (options?: BuildOptions) => VortexRdfStore.fromString(chunkedNquads(), 'nquads', options);

/** The quads `stream` emits as `data` events, once it emits `end`. */
function readEvents(stream: Stream<Quad>): Promise<Quad[]> {
    return new Promise((resolve, reject) => {
        const quads: Quad[] = [];
        stream.on('data', (quad: Quad) => quads.push(quad));
        stream.on('end', () => resolve(quads));
        stream.on('error', reject);
    });
}

/** The events `stream` emits until it ends or fails, counted a macrotask
 *  later so that an event queued behind the first is counted too, each
 *  error as its string; `onData` runs on each `data` event. */
function events(stream: Stream<Quad>, onData: () => void = () => {}) {
    return new Promise<{ data: number; ends: number; errors: string[] }>((resolve) => {
        const seen = { data: 0, ends: 0, errors: [] as string[] };
        const settle = () => setTimeout(() => resolve(seen), 0);
        stream.on('data', () => {
            seen.data++;
            onData();
        });
        stream.on('end', () => {
            seen.ends++;
            settle();
        });
        stream.on('error', (error: unknown) => {
            seen.errors.push(String(error));
            settle();
        });
    });
}

/** `match`'s stream with what the stream rules reach beyond `Stream<Quad>`:
 *  `destroy()`, the chunk of quads it buffers, and the store it reads. */
type MatchStream = Stream<Quad> & { destroy(): MatchStream; _buffer: Quad[]; _store?: VortexRdfStore };

/** Runs the microtasks queued so far and those they queue, a few rounds deep:
 *  the stream's start, and the `end` or `error` it queues, are a round or
 *  two away. No timer. */
async function microtasks() {
    for (let round = 0; round < 5; round++) await Promise.resolve();
}

describe('match streams the quads getQuads returns, in its order', () => {
    for (const { name, options } of VARIANTS) {
        test(name, async () => {
            const built = await chunkedStore(options);
            const tailed = await chunkedStore(options);
            await tailed.addQuad(NEW_QUAD);
            const stores: [string, VortexRdfStore][] = [
                ['fromString', built],
                ['fromQuads', await VortexRdfStore.fromQuads(built.getQuads(), options)],
                ['fromBytes', await VortexRdfStore.fromBytes(await built.toBytes())],
                ['an append tail', tailed],
            ];
            for (const [source, store] of stores) {
                for (const p of CHUNKED_PATTERNS) {
                    const label = `${source}: match(${p.map((t) => t?.value ?? null).join(', ')})`;
                    expect(keys(await collect(store.match(...p))), label).toEqual(keys(store.getQuads(...p)));
                }
            }
        }, 60_000);
    }
});

test('a match bigger than one chunk arrives over several pulls of at most 8,192 rows', async () => {
    const next = vi.spyOn(QuadChunks.prototype, 'next');
    const tailed = await chunkedStore({ layout: 'dictionary' });
    await tailed.addQuad(NEW_QUAD);
    const stores: [string, VortexRdfStore, number][] = [
        ['dictionary', await chunkedStore({ layout: 'dictionary' }), CHUNKED_ROWS],
        ['default', await chunkedStore({ layout: 'default' }), CHUNKED_ROWS],
        ['typed-object', await chunkedStore({ layout: 'typed-object' }), CHUNKED_ROWS],
        ['dictionary with an append tail', tailed, CHUNKED_ROWS + 1],
    ];
    for (const [label, store, rows] of stores) {
        next.mockClear();
        expect((await readEvents(store.match())).length, label).toBe(rows);
        expect(sizes(next), label).toEqual([8192, 8192, rows - 16384, undefined]);
    }
});

test('read() pulls the next chunk only once the buffer is empty', async () => {
    const next = vi.spyOn(QuadChunks.prototype, 'next');
    const store = await chunkedStore();
    // An invalid pattern still throws from the call.
    expect(() => store.match(null, null, null, df.literal('g'))).toThrow(/Invalid graph term/);
    const stream = store.match();
    expect(next).not.toHaveBeenCalled();
    expect(stream.read()).not.toBeNull();
    expect(next).toHaveBeenCalledTimes(1);
    let taken = 1;
    while (taken < 8192 && stream.read() !== null) taken++;
    expect(taken).toBe(8192);
    expect(next).toHaveBeenCalledTimes(1);
    expect(stream.read()).not.toBeNull();
    expect(next).toHaveBeenCalledTimes(2);
    let rest = 0;
    while (stream.read() !== null) rest++;
    expect(rest).toBe(CHUNKED_ROWS - 8193);
    expect(next).toHaveBeenCalledTimes(4);
    await new Promise((r) => stream.on('end', r));
});

test('for await pulls a chunk at a time and frees the handle at the end and on an early exit', async () => {
    const next = vi.spyOn(QuadChunks.prototype, 'next');
    const free = vi.spyOn(QuadChunks.prototype, 'free');
    const store = await chunkedStore();
    // The pulls so far at quads 0, 8,191 and 8,192.
    const pulls: number[] = [];
    let quads = 0;
    for await (const _ of store.match() as unknown as AsyncIterable<Quad>) {
        if (quads === 0 || quads === 8191 || quads === 8192) pulls.push(next.mock.calls.length);
        quads++;
    }
    expect(quads).toBe(CHUNKED_ROWS);
    expect(pulls).toEqual([1, 1, 2]);
    expect(next).toHaveBeenCalledTimes(4);
    expect(free).toHaveBeenCalledTimes(1);

    next.mockClear();
    free.mockClear();
    const stream = store.match();
    expect((stream as any)._store).toBe(store);
    for await (const _ of stream as unknown as AsyncIterable<Quad>) break;
    expect(next).toHaveBeenCalledTimes(1);
    expect(free).toHaveBeenCalledTimes(1);
    expect((stream as any)._store).toBeUndefined();
});

test('freeing the store mid-stream is an error event', async () => {
    const free = vi.spyOn(QuadChunks.prototype, 'free');
    const freed = { data: 0, ends: 0, errors: [expect.stringMatching(/VortexRdfStore .*freed/)] };

    // Freed on the first quad: the rest of the chunk already pulled arrives.
    const store = await chunkedStore();
    let first = true;
    const midway = await events(store.match(), () => {
        if (first) store.free();
        first = false;
    });
    expect(midway).toEqual({ ...freed, data: 8192 });
    expect(free).toHaveBeenCalledTimes(1);

    // Freed before the first pull.
    free.mockClear();
    const early = await chunkedStore();
    const stream = early.match();
    early.free();
    expect(await events(stream)).toEqual(freed);
    expect(free).toHaveBeenCalledTimes(1);
});

test('a mutation during the iteration leaves the matched snapshot', async () => {
    for (const layout of ['dictionary', 'default'] as const) {
        const store = await chunkedStore({ layout });
        const want = keys(store.getQuads());
        const last = store.getQuads().at(-1)!;
        const got: Quad[] = [];
        for await (const quad of store.match() as unknown as AsyncIterable<Quad>) {
            got.push(quad);
            if (got.length === 1) {
                await store.deleteQuad(last);
                await store.addQuad(NEW_QUAD);
            }
        }
        expect(keys(got), layout).toEqual(want);
        expect(store.has(last), layout).toBe(false);
        expect(store.has(NEW_QUAD), layout).toBe(true);
        expect(store.size(), layout).toBe(CHUNKED_ROWS);
    }
});

describe('the stream rules', () => {
    test('destroy() stops the stream without end or error, and frees the handle once', async () => {
        const next = vi.spyOn(QuadChunks.prototype, 'next');
        const free = vi.spyOn(QuadChunks.prototype, 'free');
        const store = await chunkedStore();
        const ends: string[] = [];

        // Before the start: nothing is pulled or emitted.
        const early = store.match() as MatchStream;
        let earlyData = 0;
        early.on('data', () => earlyData++);
        early.on('end', () => ends.push('end')).on('error', () => ends.push('error'));
        expect(early.destroy()).toBe(early);
        early.destroy();
        await microtasks();
        expect(earlyData).toBe(0);
        expect(next).not.toHaveBeenCalled();
        expect(free).toHaveBeenCalledTimes(1);
        expect(early._store).toBeUndefined();
        expect(early.read()).toBeNull();

        // From a data listener: no data after it.
        next.mockClear();
        free.mockClear();
        const midway = store.match() as MatchStream;
        let data = 0;
        midway.on('data', () => {
            if (++data === 10) midway.destroy();
        });
        midway.on('end', () => ends.push('end')).on('error', () => ends.push('error'));
        await microtasks();
        expect(data).toBe(10);
        expect(next).toHaveBeenCalledTimes(1);
        expect(free).toHaveBeenCalledTimes(1);
        expect(midway._store).toBeUndefined();
        expect(ends).toEqual([]);
    });

    test('a data listener that throws destroys the stream, and its error propagates', async () => {
        const next = vi.spyOn(QuadChunks.prototype, 'next');
        const free = vi.spyOn(QuadChunks.prototype, 'free');
        const store = await chunkedStore();
        const stream = store.match() as MatchStream;
        const thrown = new Error('a data listener threw');
        const ends: string[] = [];
        let data = 0;
        stream.on('data', () => {
            if (++data === 1) throw thrown;
        });
        stream.on('end', () => ends.push('end')).on('error', () => ends.push('error'));
        // The error leaves the stream's start, a microtask, as an uncaught
        // exception. Vitest reports one as an unhandled error, failing the run,
        // unless a listener besides its own is registered, as this one is.
        const uncaught: unknown[] = [];
        const onUncaught = (error: unknown) => uncaught.push(error);
        process.on('uncaughtException', onUncaught);
        try {
            await microtasks();
        } finally {
            process.off('uncaughtException', onUncaught);
        }
        expect(uncaught).toEqual([thrown]);
        expect(free).toHaveBeenCalledTimes(1);
        expect(stream._store).toBeUndefined();
        // Nothing follows: no other quad, chunk, end or error.
        expect(stream.read()).toBeNull();
        await microtasks();
        expect(data).toBe(1);
        expect(next).toHaveBeenCalledTimes(1);
        expect(ends).toEqual([]);
    });

    test('a data listener that reads or attaches another listener gets no quad twice and loses none', async () => {
        const store = await chunkedStore();
        const stream = store.match();
        // Every quad the first listener takes, by data events and read(), in order.
        const taken: Quad[] = [];
        const second: Quad[] = [];
        let takenAtAttach = 0;
        stream.on('data', (quad: Quad) => {
            taken.push(quad);
            if (taken.length === 1) {
                // A drain is running: the one this would start returns at once.
                stream.on('data', (q: Quad) => second.push(q));
                takenAtAttach = taken.length;
            }
            // Mid-chunk, and at the first chunk's last quad, where read() pulls the next.
            if (taken.length === 5 || taken.length === 8192) taken.push(stream.read()!);
        });
        await new Promise((resolve) => stream.on('end', resolve));
        expect(takenAtAttach).toBe(1);
        expect(keys(taken)).toEqual(keys(store.getQuads()));
        // The second listener has every data event after it was attached.
        expect(keys(second)).toEqual(keys(taken.filter((_, i) => i !== 0 && i !== 5 && i !== 8192)));
    });

    test('a data listener attached after the start drains at once, and a readable one is called at once', async () => {
        const store = await chunkedStore();
        const stream = store.match();
        await microtasks();
        let readable = 0;
        stream.on('readable', () => readable++);
        expect(readable).toBe(1);
        const quads: Quad[] = [];
        stream.on('data', (quad: Quad) => quads.push(quad));
        expect(quads).toHaveLength(CHUNKED_ROWS);
        // Done: a readable listener is no longer called.
        stream.on('readable', () => readable++);
        expect(readable).toBe(1);
        await new Promise((resolve) => stream.on('end', resolve));
    });

    test('for await rethrows a failure mid-loop, and frees the handle', async () => {
        const free = vi.spyOn(QuadChunks.prototype, 'free');
        const store = await chunkedStore();
        const stream = store.match() as MatchStream;
        let quads = 0;
        const loop = async () => {
            for await (const _ of stream as unknown as AsyncIterable<Quad>) {
                // The first chunk is pulled: the next pull fails.
                if (quads++ === 0) store.free();
            }
        };
        await expect(loop()).rejects.toThrow(/VortexRdfStore .*freed/);
        expect(quads).toBe(8192);
        expect(free).toHaveBeenCalledTimes(1);
        expect(stream._store).toBeUndefined();
    });

    test('the stream drops its store and its last chunk at the end, on an error and on destroy()', async () => {
        const store = await chunkedStore();

        const ended = store.match() as MatchStream;
        expect(ended._store).toBe(store);
        expect(await readEvents(ended)).toHaveLength(CHUNKED_ROWS);
        expect(ended._store).toBeUndefined();
        expect(ended._buffer).toHaveLength(0);
        expect(ended.read()).toBeNull();

        const destroyed = store.match() as MatchStream;
        expect(destroyed.read()).not.toBeNull();
        destroyed.destroy();
        expect(destroyed._store).toBeUndefined();
        expect(destroyed._buffer).toHaveLength(0);
        expect(destroyed.read()).toBeNull();

        // The store freed after the first chunk: the next pull fails.
        const failed = store.match() as MatchStream;
        expect(failed.read()).not.toBeNull();
        store.free();
        let rest = 0;
        while (failed.read() !== null) rest++;
        expect(rest).toBe(8191);
        await new Promise((resolve) => failed.on('error', resolve));
        expect(failed._store).toBeUndefined();
        expect(failed._buffer).toHaveLength(0);
        expect(failed.read()).toBeNull();
    });

    test('a mutation between match() and its first pull leaves the snapshot', async () => {
        const next = vi.spyOn(QuadChunks.prototype, 'next');
        for (const layout of ['dictionary', 'default'] as const) {
            const store = await chunkedStore({ layout });
            const want = keys(store.getQuads());
            const last = store.getQuads().at(-1)!;
            next.mockClear();
            const stream = store.match();
            await store.deleteQuad(last);
            await store.addQuad(NEW_QUAD);
            expect(next, layout).not.toHaveBeenCalled();
            expect(keys(await collect(stream)), layout).toEqual(want);
        }
    });
});
