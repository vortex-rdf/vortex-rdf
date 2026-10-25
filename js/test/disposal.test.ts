// Freeing the wasm handles: the store, its TermDict, and the iterables over
// the CodeChunks and ProbeAnswers handles.
import { describe, test, expect } from 'vitest';
import { TermDict, VortexRdfStore } from '@vortex-rdf/vortex-rdf-store';
// The wasm iterator handles, which the package entries do not re-export.
import { CodeChunks, ProbeAnswers } from '../pkg/web/vortex_rdf.js';
import { nquadsStore } from './support.js';

describe('disposal', () => {
    test('a freed store throws on use', async () => {
        const store = await nquadsStore();
        const dict = store.termDict()!;
        store.free();
        expect(() => store.countQuads(null, null, null, null)).toThrow();
        expect(() => store.match(null, null, null, null)).toThrow();
        // The dictionary handle is independent of the store's lifetime.
        expect(dict.decode(0)).toBeDefined();
        dict.free();
        expect(() => dict.decode(0)).toThrow();
    });

    test('Symbol.dispose is free on every wasm handle', () => {
        expect(typeof VortexRdfStore.prototype[Symbol.dispose]).toBe('function');
        expect(VortexRdfStore.prototype[Symbol.dispose]).toBe(VortexRdfStore.prototype.free);
        expect(typeof TermDict.prototype[Symbol.dispose]).toBe('function');
        expect(TermDict.prototype[Symbol.dispose]).toBe(TermDict.prototype.free);
        // The iterator handles under matchCodeChunks and matchCodesMany, which
        // the package does not export: their iterables free them.
        expect(typeof CodeChunks.prototype[Symbol.dispose]).toBe('function');
        expect(CodeChunks.prototype[Symbol.dispose]).toBe(CodeChunks.prototype.free);
        expect(typeof ProbeAnswers.prototype[Symbol.dispose]).toBe('function');
        expect(ProbeAnswers.prototype[Symbol.dispose]).toBe(ProbeAnswers.prototype.free);
    });

    test('an iterable over a freed store throws on its next pull, then ends', async () => {
        const store = await nquadsStore();
        const chunks = store.matchCodeChunks(null, null, null, null, { batchRows: 2 })![Symbol.iterator]();
        const answers = store.matchCodesMany([{}, {}])[Symbol.iterator]();
        expect(chunks.next().done).toBe(false);
        expect(answers.next().done).toBe(false);
        store.free();
        expect(() => chunks.next()).toThrow(/VortexRdfStore .*freed/);
        expect(() => answers.next()).toThrow(/VortexRdfStore .*freed/);
        expect(chunks.next()).toEqual({ done: true, value: undefined });
        expect(answers.next()).toEqual({ done: true, value: undefined });
    });

    test('return() and Symbol.dispose end the iterables', async () => {
        const store = await nquadsStore();
        const makers = [
            () => store.matchCodeChunks(null, null, null, null, { batchRows: 1 })!,
            () => store.matchCodesMany([{}, {}, {}]),
        ];
        for (const make of makers) {
            // A for…of left early calls return().
            const left = make();
            let pulled = 0;
            for (const _ of left) {
                pulled++;
                break;
            }
            expect(pulled).toBe(1);
            expect(left[Symbol.iterator]().next()).toEqual({ done: true, value: undefined });
            const disposed = make();
            expect(disposed[Symbol.iterator]().next().done).toBe(false);
            disposed[Symbol.dispose]();
            disposed[Symbol.dispose]();
            expect(disposed[Symbol.iterator]().next()).toEqual({ done: true, value: undefined });
        }
    });
});
