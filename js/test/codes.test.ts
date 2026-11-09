// The term-code surface: matchCodes and termDict, the keep and window
// options, the chunk and batch iterables, and countQuadsMany.
import { describe, test, expect } from 'vitest';
import type { Quad, Term } from '@rdfjs/types';
import type { ChunkOptions, CodeColumns, CodeKeep, Probe, ReadOptions } from '@vortex-rdf/vortex-rdf-store';
import { NEW_QUAD, columnArrays, df, drainChunks, ex, nquadsStore } from './support.js';

/** The rows of code columns as `[s, p, o, g]` tuples, in column order. */
function codeRows(cols: CodeColumns): number[][] {
    return Array.from({ length: cols.length }, (_, i) => [cols.s[i], cols.p[i], cols.o[i], cols.g[i]]);
}

describe('matchCodes / termDict gates', () => {
    const p1 = ex('p1');

    test('Dictionary: codes agree with getQuads and decode through termDict', async () => {
        const store = await nquadsStore({ layout: 'dictionary' });
        const dict = store.termDict()!;
        expect(dict).toBeDefined();

        const cols = store.matchCodes(null, p1, null, null)!;
        expect(cols).not.toBeNull();
        expect(cols.length).toBe(3);
        for (const col of [cols.s, cols.p, cols.o, cols.g]) {
            expect(col).toBeInstanceOf(Float64Array);
            expect(col.length).toBe(3);
        }
        const quads = store.getQuads(null, p1, null, null);
        const iri = (t: Term) => `<${t.value}>`;
        expect([...cols.s].map((c) => dict.decode(c)).sort())
            .toEqual(quads.map((q: Quad) => iri(q.subject)).sort());
        expect([...cols.o].map((c) => dict.decode(c)).sort())
            .toEqual(quads.map((q: Quad) => iri(q.object)).sort());

        const full = store.matchCodes(ex('s2'), p1, ex('o2'), ex('g1'))!;
        expect(full.length).toBe(1);
        expect(dict.decode(full.g[0])).toBe('<http://example.org/g1>');
    });

    for (const layout of ['default', 'typed-object'] as const) {
        test(`${layout}: matchCodes is null and termDict undefined`, async () => {
            const store = await nquadsStore({ layout });
            expect(store.matchCodes(null, p1, null, null)).toBeNull();
            expect(store.termDict()).toBeUndefined();
        });

        test(`${layout}: matchCodes throws on an invalid pattern instead of returning null`, async () => {
            const store = await nquadsStore({ layout });
            expect(() => store.matchCodes(null, null, null, df.literal('g'))).toThrow(/Invalid graph term/);
        });
    }

    test('a pending append closes the code path', async () => {
        const store = await nquadsStore({ layout: 'dictionary' });
        expect(store.matchCodes(null, p1, null, null)).not.toBeNull();
        await store.addQuad(NEW_QUAD);
        expect(store.matchCodes(null, p1, null, null)).toBeNull();
        expect(store.termDict()).toBeUndefined();
        // The term path still answers.
        expect(store.getQuads(null, p1, null, null).length).toBe(3);
    });

    test('TermDict edges: out-of-range decode and unknown encode are undefined', async () => {
        const store = await nquadsStore({ layout: 'dictionary' });
        const dict = store.termDict()!;
        expect(dict.decode(2 ** 31)).toBeUndefined();
        expect(dict.encode('<http://example.org/nope>')).toBeUndefined();
        // The default graph is the empty string in this vocabulary.
        expect(typeof dict.encode('')).toBe('number');
        const code = dict.encode('<http://example.org/s1>')!;
        expect(dict.decode(code)).toBe('<http://example.org/s1>');
    });

    test('TermDict.encode takes variant spellings and throws on a malformed one', async () => {
        const store = await nquadsStore({ layout: 'dictionary' });
        const dict = store.termDict()!;
        // Each variant finds the code of the stored form it spells.
        const spellings: [string, string][] = [
            ['http://example.org/s1', '<http://example.org/s1>'],
            ['"hola"@ES', '"hola"@es'],
            ['"lit"^^<http://www.w3.org/2001/XMLSchema#string>', '"lit"'],
            ['default', ''],
        ];
        for (const [variant, stored] of spellings) {
            const code = dict.encode(stored);
            expect(code, stored).toBeDefined();
            expect(dict.encode(variant), variant).toBe(code);
        }
        // An absent variant is still `undefined`; a malformed term throws.
        expect(dict.encode('http://example.org/nope')).toBeUndefined();
        expect(() => dict.encode('"unterminated')).toThrow(/Invalid input/);
    });

    // Codes are u64 and cross as JS numbers, exact up to 2^53 - 1. A code
    // past u32::MAX is a code like any other — out of this dictionary's range
    // here, never wrapped onto the term 2^32 below it — and what is no code
    // (negative, fractional, not finite, past 2^53 - 1) is refused.
    test('TermDict takes codes past u32::MAX without wrapping them', async () => {
        const store = await nquadsStore({ layout: 'dictionary' });
        const dict = store.termDict()!;
        const code = dict.encode('<http://example.org/s1>')!;
        expect(dict.decode(code)).toBe('<http://example.org/s1>');
        expect(dict.decode(2 ** 32 + code)).toBeUndefined();
        expect(dict.decode(2 ** 40 + code)).toBeUndefined();
        expect(dict.decode(Number.MAX_SAFE_INTEGER)).toBeUndefined();
    });

    test('TermDict refuses what is no code', async () => {
        const store = await nquadsStore({ layout: 'dictionary' });
        const dict = store.termDict()!;
        for (const bad of [-1, -0.5, 1.5, NaN, Infinity, -Infinity, 2 ** 53, 2 ** 64]) {
            expect(() => dict.decode(bad), `${bad}`).toThrow(/code/);
        }
    });

    test('TermDict.decode refuses what is no number instead of coercing it', async () => {
        const store = await nquadsStore({ layout: 'dictionary' });
        const dict = store.termDict()!;
        // Coerced, `null`, `[]`, `false` and `''` would be code 0, and `true`,
        // `'1'` and `[1]` code 1: real terms of this dictionary.
        expect(dict.decode(0)).toBeDefined();
        expect(dict.decode(1)).toBeDefined();
        const notNumbers: unknown[] = [null, undefined, [], [1], {}, true, false, '1', '', 1n];
        for (const bad of notNumbers) {
            expect(() => dict.decode(bad as number), String(bad)).toThrow(/code/);
        }
    });

    test('matchCodes columns are Float64Arrays of exact codes', async () => {
        const store = await nquadsStore({
            layout: 'dictionary',
            indexes: ['secondary-by-reference'],
        });
        const dict = store.termDict()!;
        const cols = store.matchCodes()!;
        expect(cols.length).toBe(6);
        for (const col of [cols.s, cols.p, cols.o, cols.g]) {
            expect(col).toBeInstanceOf(Float64Array);
            for (const code of col) {
                expect(Number.isSafeInteger(code)).toBe(true);
                expect(dict.decode(code)).toBeDefined();
                expect(dict.encode(dict.decode(code)!)).toBe(code);
            }
        }
    });

    test('keep, limit and offset narrow matchCodes and countQuads as filtering matchCodes() does', async () => {
        const store = await nquadsStore({ layout: 'dictionary' });
        const dict = store.termDict()!;
        const code = (term: string) => dict.encode(term)!;
        const [p1c, p2c, p4c] = ['p1', 'p2', 'p4'].map((p) => code(`<http://example.org/${p}>`));
        const o1c = code('<http://example.org/o1>');
        const o2c = code('<http://example.org/o2>');
        const litc = code('"lit"');
        const all = codeRows(store.matchCodes()!);
        // A two-element array is a half-open range [lo, hi); a Float64Array,
        // or an array of any other length, is a set.
        const cases: [CodeKeep, (row: number[]) => boolean][] = [
            [{ p: Float64Array.of(p1c, p2c) }, (r) => r[1] === p1c || r[1] === p2c],
            [{ o: Float64Array.of(litc, o2c) }, (r) => r[2] === litc || r[2] === o2c],
            [{ o: [litc, o2c] }, (r) => litc <= r[2] && r[2] < o2c],
            [{ p: [p4c, p1c, p1c] }, (r) => r[1] === p1c || r[1] === p4c],
            [{ p: [p1c] }, (r) => r[1] === p1c],
            [{ o: [o1c, o1c + 1] }, (r) => r[2] === o1c],
            [{ o: [o1c + 1, o1c] }, () => false],
            [{ s: [0, Number.MAX_SAFE_INTEGER] }, () => true],
            [{ g: [code('')] }, (r) => r[3] === code('')],
            [{ s: [] }, () => false],
            [{ p: [2 ** 40] }, () => false],
            [{ p: [p1c], o: [o1c, o1c + 1] }, (r) => r[1] === p1c && r[2] === o1c],
        ];
        for (const [keep, admits] of cases) {
            const want = all.filter(admits);
            const name = JSON.stringify(keep);
            expect(codeRows(store.matchCodes(null, null, null, null, { keep })!), name).toEqual(want);
            expect(store.countQuads(null, null, null, null, { keep }), name).toBe(want.length);
            expect(store.countQuads(null, null, null, null, { keep, limit: 1 }), name).toBe(Math.min(1, want.length));
        }
        const windows: [number, number | undefined][] = [[0, 2], [1, 3], [3, 10], [6, 1], [0, 0], [2, undefined]];
        for (const [offset, limit] of windows) {
            const want = all.slice(offset, limit === undefined ? undefined : offset + limit);
            expect(codeRows(store.matchCodes(null, null, null, null, { offset, limit })!), `${offset} ${limit}`)
                .toEqual(want);
            expect(store.countQuads(null, null, null, null, { offset, limit }), `${offset} ${limit}`)
                .toBe(want.length);
        }
        // The keep applies to the pattern's match, and the window to the kept rows.
        const kept = all.filter((r) => r[1] === p1c && r[2] === o1c);
        const narrowed: ReadOptions = { keep: { o: [o1c, o1c + 1] }, offset: 1, limit: 1 };
        expect(codeRows(store.matchCodes(null, p1, null, null, narrowed)!)).toEqual(kept.slice(1, 2));
        expect(store.countQuads(null, p1, null, null, narrowed)).toBe(1);
    });

    test('malformed narrowing options throw before the store is read', async () => {
        const store = await nquadsStore({ layout: 'dictionary' });
        const strings = await nquadsStore({ layout: 'default' });
        const bad: [unknown, ErrorConstructor][] = [
            ['keep', TypeError],
            [{ keep: 'p' }, TypeError],
            [{ keep: { x: [1] } }, TypeError],
            [{ keep: { p: new Uint32Array([1]) } }, TypeError],
            [{ keep: { p: ['1'] } }, TypeError],
            [{ keep: { p: [-1] } }, RangeError],
            [{ keep: { p: [1.5] } }, RangeError],
            [{ keep: { p: Float64Array.of(NaN) } }, RangeError],
            [{ keep: { p: [0, 2 ** 53] } }, RangeError],
            [{ limit: -1 }, RangeError],
            [{ offset: 1.5 }, RangeError],
            [{ limit: '2' }, TypeError],
            [{ bogus: 1 }, TypeError],
        ];
        for (const each of [store, strings]) {
            for (const [options, error] of bad) {
                const name = JSON.stringify(options);
                expect(() => each.matchCodes(null, null, null, null, options as ReadOptions), name).toThrow(error);
                expect(() => each.countQuads(null, null, null, null, options as ReadOptions), name).toThrow(error);
            }
        }
    });

    test('matchCodeChunks yields the columns asked for, narrowed as matchCodes narrows', async () => {
        const store = await nquadsStore({ layout: 'dictionary' });
        const o1c = store.termDict()!.encode('<http://example.org/o1>')!;
        const narrowings: ReadOptions[] = [{}, { keep: { o: [o1c, o1c + 1] } }, { limit: 2, offset: 1 }];
        for (const narrowing of narrowings) {
            const whole = store.matchCodes(null, null, null, null, narrowing)!;
            for (const batchRows of [1, 2, 8192]) {
                const chunks = store.matchCodeChunks(null, null, null, null, { ...narrowing, batchRows })!;
                const { cols, sizes } = drainChunks(chunks);
                const name = `${JSON.stringify(narrowing)} ${batchRows}`;
                expect(cols, name).toEqual(columnArrays(whole));
                expect(sizes.every((size) => size > 0 && size <= batchRows), name).toBe(true);
            }
        }
        // Only the columns asked for are set.
        const whole = store.matchCodes(null, p1, null, null)!;
        const iterator = store.matchCodeChunks(null, p1, null, null, { columns: ['o'] })![Symbol.iterator]();
        const first = iterator.next();
        expect(first.done).toBe(false);
        expect(Object.keys(first.value).sort()).toEqual(['length', 'o']);
        expect([...first.value.o]).toEqual([...whole.o]);
        // An exhausted iterator stays exhausted.
        for (let i = 0; i < 2; i++) expect(iterator.next()).toEqual({ done: true, value: undefined });
        const picked = drainChunks(store.matchCodeChunks(null, p1, null, null, { columns: ['g', 's', 'g'] })!);
        expect(picked.cols).toEqual({ s: [...whole.s], g: [...whole.g] });
    });

    test('matchCodeChunks is null without codes and refuses bad chunk options', async () => {
        const store = await nquadsStore({ layout: 'dictionary' });
        const strings = await nquadsStore({ layout: 'default' });
        expect(strings.matchCodeChunks(null, p1, null, null)).toBeNull();
        const bad: [unknown, ErrorConstructor][] = [
            [{ batchRows: 0 }, RangeError],
            [{ batchRows: 1.5 }, RangeError],
            [{ batchRows: '8' }, TypeError],
            [{ columns: [] }, RangeError],
            [{ columns: ['x'] }, TypeError],
            [{ columns: 'o' }, TypeError],
            [{ signal: {} }, TypeError],
            [{ keep: { p: [-1] } }, RangeError],
        ];
        for (const each of [store, strings]) {
            for (const [options, error] of bad) {
                expect(() => each.matchCodeChunks(null, null, null, null, options as ChunkOptions), JSON.stringify(options))
                    .toThrow(error);
            }
        }
    });

    test('matchCodesMany answers each probe in input order, as matchCodes does', async () => {
        const store = await nquadsStore({ layout: 'dictionary' });
        const o1c = store.termDict()!.encode('<http://example.org/o1>')!;
        const probes: Probe[] = [
            [null, p1, null, null],
            ['http://example.org/s1', null, null, null],
            { s: ex('nobody') },
            { p: p1, keep: { o: [o1c, o1c + 1] }, limit: 1, offset: 1 },
            { s: ex('s2'), limit: 1 },
            {},
        ];
        const single = (probe: Probe) => Array.isArray(probe)
            ? store.matchCodes(...probe)
            : store.matchCodes(probe.s, probe.p, probe.o, probe.g, {
                keep: probe.keep, limit: probe.limit, offset: probe.offset,
            });
        const answers = [...store.matchCodesMany(probes)];
        expect(answers.length).toBe(probes.length);
        answers.forEach((cols, i) => expect(codeRows(cols!), `probe ${i}`).toEqual(codeRows(single(probes[i])!)));
        expect([...store.matchCodesMany([])]).toEqual([]);
        // Every probe is parsed before any runs: a malformed one throws from the call.
        for (const many of [store.matchCodesMany.bind(store), store.countQuadsMany.bind(store)]) {
            expect(() => many([{}, [null, null, null]] as unknown as Probe[])).toThrow(TypeError);
            expect(() => many([{}, { bogus: 1 }] as unknown as Probe[])).toThrow(TypeError);
            expect(() => many([{}, 'probe'] as unknown as Probe[])).toThrow(TypeError);
            expect(() => many({} as unknown as Probe[])).toThrow(TypeError);
            expect(() => many([{}, [null, 'not an iri', null, null]])).toThrow(/Invalid predicate term/);
            expect(() => many([{}, { p: 'not an iri' }])).toThrow(/Invalid predicate term/);
            expect(() => many([{}, { keep: { p: [-1] } }])).toThrow(RangeError);
        }
    });

    test('matchCodesMany answers null per probe without codes, evaluating none', async () => {
        const strings = await nquadsStore({ layout: 'default' });
        // Run, this probe throws: a keep needs the Dictionary layout.
        const failing: Probe = { p: p1, keep: { o: [0, 5] } };
        expect(() => strings.countQuadsMany([failing])).toThrow(/keep needs the Dictionary layout/);
        expect([...strings.matchCodesMany([failing, failing, failing])]).toEqual([null, null, null]);
    });

    test('countQuadsMany counts each probe as countQuads does, on every layout', async () => {
        for (const layout of ['dictionary', 'default', 'typed-object'] as const) {
            const store = await nquadsStore({ layout });
            const probes: Probe[] = [
                [null, p1, null, null],
                { s: 'http://example.org/s1' },
                { s: ex('nobody'), limit: 1 },
                { offset: 2, limit: 3 },
                { p: p1, limit: 2 },
                {},
            ];
            const single = (probe: Probe) => Array.isArray(probe)
                ? store.countQuads(...probe)
                : store.countQuads(probe.s, probe.p, probe.o, probe.g, { limit: probe.limit, offset: probe.offset });
            expect(store.countQuadsMany(probes), layout).toEqual(probes.map(single));
            expect(store.countQuadsMany([]), layout).toEqual([]);
        }
        const store = await nquadsStore({ layout: 'dictionary' });
        const o1c = store.termDict()!.encode('<http://example.org/o1>')!;
        const kept: Probe = { keep: { o: [o1c, o1c + 1] } };
        expect(store.countQuadsMany([kept, { ...kept, limit: 1 }])).toEqual([2, 1]);
        // On a store without codes, the first probe with a keep makes the call throw.
        const strings = await nquadsStore({ layout: 'default' });
        expect(() => strings.countQuadsMany([{}, { p: p1, keep: { o: [0, 5] } }]))
            .toThrow(/keep needs the Dictionary layout/);
    });

    test('an aborted signal ends a chunk iteration and a batch at the next pull', async () => {
        const store = await nquadsStore({ layout: 'dictionary' });
        const chunking = new AbortController();
        const chunks = store.matchCodeChunks(null, null, null, null, { batchRows: 2, signal: chunking.signal })!;
        const iterator = chunks[Symbol.iterator]();
        expect(iterator.next().value.length).toBe(2);
        chunking.abort();
        for (let i = 0; i < 2; i++) expect(iterator.next()).toEqual({ done: true, value: undefined });

        const batching = new AbortController();
        const answers = store.matchCodesMany([{}, {}, {}], { signal: batching.signal })[Symbol.iterator]();
        expect(answers.next().done).toBe(false);
        batching.abort();
        expect(answers.next()).toEqual({ done: true, value: undefined });
        expect(() => store.matchCodesMany([{}], { signal: true } as unknown as { signal: AbortSignal }))
            .toThrow(TypeError);
    });

    test('a signal aborted before the call returns a finished iterable, once the arguments are checked', async () => {
        const store = await nquadsStore({ layout: 'dictionary' });
        const signal = AbortSignal.abort();
        const chunks = store.matchCodeChunks(null, null, null, null, { signal })![Symbol.iterator]();
        expect(chunks.next()).toEqual({ done: true, value: undefined });
        const answers = store.matchCodesMany([{}, [null, p1, null, null]], { signal })[Symbol.iterator]();
        expect(answers.next()).toEqual({ done: true, value: undefined });
        // The arguments are checked first, and `null` still means no codes.
        expect(() => store.matchCodeChunks(null, null, null, null, { batchRows: 0, signal })).toThrow(RangeError);
        expect(() => store.matchCodesMany([{ bogus: 1 } as unknown as Probe], { signal })).toThrow(TypeError);
        const strings = await nquadsStore({ layout: 'default' });
        expect(strings.matchCodeChunks(null, null, null, null, { signal })).toBeNull();
    });

    test('an empty match yields no chunk', async () => {
        const store = await nquadsStore({ layout: 'dictionary' });
        const nobody = ex('nobody');
        const empty: [Term | null, ChunkOptions][] = [[nobody, {}], [null, { keep: { s: [] } }], [null, { limit: 0 }]];
        for (const [subject, options] of empty) {
            const iterator = store.matchCodeChunks(subject, null, null, null, options)![Symbol.iterator]();
            expect(iterator.next(), JSON.stringify(options)).toEqual({ done: true, value: undefined });
        }
    });
});
