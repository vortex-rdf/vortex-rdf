import { EventEmitter } from 'node:events';
import { describe, test, expect, vi } from 'vitest';
import { Bench } from 'tinybench';
import { VortexRdfStore } from '@vortex-rdf/vortex-rdf-store';

import { genDataset, predicateTerm } from '../bench/datasets.js';
import { consumeStream, errorText, makingLast } from '../bench/util.js';

function spyQuad(reads: string[]) {
    const term = (tag: string) => ({ get value() { reads.push(tag); return tag; } });
    return { subject: term('s'), predicate: term('p'), object: term('o'), graph: term('g') };
}

describe('consumeStream', () => {
    test('consumeStream reads every term value and resolves to the quad count', async () => {
        const s = new EventEmitter();
        const reads: string[] = [];
        const done = consumeStream(s as never);
        s.emit('data', spyQuad(reads));
        s.emit('data', spyQuad(reads));
        s.emit('end');
        await expect(done).resolves.toBe(2);
        expect(reads).toEqual(['s', 'p', 'o', 'g', 's', 'p', 'o', 'g']);
    });

    test("consumeStream rejects with the stream's error", async () => {
        const s = new EventEmitter();
        const done = consumeStream(s as never);
        s.emit('error', new Error('boom'));
        await expect(done).rejects.toThrow('boom');
    });

    test('consumeStream past its budget rejects after one window and destroys the stream', async () => {
        const s = Object.assign(new EventEmitter(), { destroy: vi.fn() });
        const done = consumeStream(s as never, Number.MIN_VALUE);
        done.catch(() => {});
        const reads: string[] = [];
        for (let i = 0; i < 65_537; i++) s.emit('data', spyQuad(reads));
        s.emit('end');
        await expect(done).rejects.toThrow(/consume budget exceeded after/);
        expect(s.destroy).toHaveBeenCalledTimes(1);
    });

    test('consumeStream counts a multi-chunk VortexRdfStore match()', async () => {
        const store = await VortexRdfStore.fromQuads(genDataset(20_000, { graphs: 8 }), { layout: 'dictionary' });
        try {
            expect(await consumeStream(store.match())).toBe(20_000);
            expect(await consumeStream(store.match(null, predicateTerm(0))))
                .toBe(await store.countQuads(null, predicateTerm(0)));
        } finally {
            store.free();
        }
    });
});

describe('makingLast', () => {
    test('makingLast times every make, disposes each handle before the next, and keeps the last', async () => {
        const log: string[] = [];
        let n = 0;
        const kept = makingLast(
            () => { const id = ++n; log.push(`make ${id}`); return id; },
            (h) => { log.push(`dispose ${h}`); },
        );
        const bench = new Bench({ time: 0, iterations: 3, warmup: false, warmupIterations: 0, throws: true });
        bench.add('build', ...kept.task);
        await bench.run();
        expect(log).toEqual(['make 1', 'dispose 1', 'make 2', 'dispose 2', 'make 3']);
        expect(kept.last()).toBe(3);
    });
});

describe('errorText', () => {
    test('errorText names a subclass, keeps a plain Error\'s message, and strings anything else', () => {
        expect(errorText(new Error('boom'))).toBe('boom');
        expect(errorText(new WebAssembly.RuntimeError('unreachable'))).toBe('RuntimeError: unreachable');
        expect(errorText(new TypeError('x is not a function'))).toBe('TypeError: x is not a function');
        expect(errorText('plain string')).toBe('plain string');
        expect(errorText(42)).toBe('42');
    });
});
