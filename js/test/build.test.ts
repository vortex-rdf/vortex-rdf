// Building a store: every build variant end to end, the canonical form a
// build holds, the RDF/JS stream and RDF syntax inputs, the one-shot
// conversions, the build options, multi-chunk payloads and indexes().
import { beforeAll, describe, test, expect } from 'vitest';
import { Readable } from 'node:stream';
import type { Quad, Term, Literal } from '@rdfjs/types';
import { VortexRdfStore, serializeRdf, deserializeRdf } from '@vortex-rdf/vortex-rdf-store';
import { NQUADS, VARIANTS, columnArrays, df, drainChunks, ex, nquadsStore } from './support.js';

describe('build variants', () => {
    for (const { name, options } of VARIANTS) {
        describe(name, () => {
            test('builds and reports its layout', async () => {
                const store = await nquadsStore(options);
                expect(await store.size()).toBe(6);
                expect(store.layout()).toBe(options.layout);
            });

            test('size and has return values, not promises', async () => {
                const store = await nquadsStore(options);

                expect(store.size()).toBe(6);
                expect(store.has(df.quad(ex('s1'), ex('p1'), ex('o1')))).toBe(true);
                expect(store.has(df.quad(ex('s2'), ex('p1'), ex('o2'), ex('g1')))).toBe(true);
                // An exact four-component lookup: the same triple in the
                // default graph is a different quad.
                expect(store.has(df.quad(ex('s2'), ex('p1'), ex('o2')))).toBe(false);
            });

            test('matches every quad-position pattern', async () => {
                const store = await nquadsStore(options);
                const count = (s: Term | null, p: Term | null, o: Term | null, g: Term | null) => {
                    const n = store.getQuads(s, p, o, g).length;
                    // countQuads answers from the match's row selection alone
                    // and must agree with materializing the same match.
                    expect(store.countQuads(s, p, o, g)).toBe(n);
                    return n;
                };

                // Subject-only: exercises the sorted binary-search path.
                expect(count(ex('s1'), null, null, null)).toBe(2);

                // Predicate-only: exercises SecondaryByReference index routing.
                expect(count(null, ex('p1'), null, null)).toBe(3);
                // Its terms decode to their strings, not just to the right
                // count: under the Dictionary layout a lost term dictionary
                // would yield the row count but codes instead of IRIs.
                const p1 = store.getQuads(null, ex('p1'), null, null);
                expect(p1.map((q) => q.subject.value).sort())
                    .toEqual(['http://example.org/s1', 'http://example.org/s2', 'http://example.org/s2']);
                expect(p1.map((q) => q.object.value).sort())
                    .toEqual(['http://example.org/o1', 'http://example.org/o1', 'http://example.org/o2']);

                // Object-only: exercises the object index / TypedObject columns.
                expect(count(null, null, ex('o1'), null)).toBe(2);

                // Graph-only.
                expect(count(null, null, null, ex('g1'))).toBe(1);

                // Fully bound.
                expect(count(ex('s2'), ex('p1'), ex('o2'), ex('g1'))).toBe(1);

                // Non-existent term.
                expect(count(ex('nope'), null, null, null)).toBe(0);
            });

            test('round-trips typed, language and escaped literals', async () => {
                const store = await nquadsStore(options);

                const typed = store.getQuads(
                    null, null,
                    df.literal('42', df.namedNode('http://www.w3.org/2001/XMLSchema#integer')),
                    null,
                );
                expect(typed.length).toBe(1);

                const lang = store.getQuads(null, null, df.literal('hola', 'es'), null);
                expect(lang.length).toBe(1);

                // Values whose stored N-Triples spelling escapes a character (a
                // quote, a backslash, a newline, and a control character
                // written as a \u escape) read back exactly as they went in.
                const subject = ex('escaped');
                const text = ex('text');
                const values = ['say "hi"', 'C:\\temp', 'two\nlines', 'bell\u0007'];
                const escaped = await VortexRdfStore.fromQuads(
                    values.map((value) => df.quad(subject, text, df.literal(value))),
                    options,
                );
                const objects = escaped.getQuads(subject, text, null, null).map((quad) => quad.object);
                expect(objects.map((object) => object.value).sort()).toEqual([...values].sort());
                for (const value of values) {
                    expect(objects.some((object) => object.equals(df.literal(value))), value).toBe(true);
                }
            });

            test('toBytes/fromBytes preserves the store', async () => {
                const store = await nquadsStore(options);
                const bytes = await store.toBytes();
                expect(bytes).toBeInstanceOf(Uint8Array);

                const restored = await VortexRdfStore.fromBytes(bytes);
                expect(await restored.size()).toBe(6);
                expect(restored.layout()).toBe(options.layout);

                const p1 = restored.getQuads(null, ex('p1'), null, null);
                expect(p1.length).toBe(3);
                expect(restored.countQuads(null, ex('p1'), null, null)).toBe(3);
            });

            test('toRdf emits all quads back', async () => {
                const store = await nquadsStore(options);
                const nq = await store.toRdf('nquads');
                const lines = nq.trim().split('\n').filter(Boolean);
                expect(lines.length).toBe(6);

                // Re-parsing the output yields an equivalent store.
                const reparsed = await VortexRdfStore.fromString(nq, 'nquads', options);
                expect(await reparsed.size()).toBe(6);
            });

            test('fromQuads matches fromString', async () => {
                const viaString = await nquadsStore(options);
                const quads = viaString.getQuads(null, null, null, null);
                expect(quads.length).toBe(6);

                const viaQuads = await VortexRdfStore.fromQuads(quads, options);
                expect(await viaQuads.size()).toBe(6);
                expect(viaQuads.layout()).toBe(options.layout);

                const p1 = viaQuads.getQuads(null, ex('p1'), null, null);
                expect(p1.length).toBe(3);

                // A matched subset rebuilds into a standalone store that
                // round-trips through bytes and stays queryable.
                const derived = await VortexRdfStore.fromQuads(viaString.getQuads(null, ex('p1'), null, null), options);
                const restored = await VortexRdfStore.fromBytes(await derived.toBytes());
                expect(await restored.size()).toBe(3);
                const lines = (await restored.toRdf('nquads')).trim().split('\n').filter(Boolean).sort();
                expect(lines).toEqual([
                    '<http://example.org/s1> <http://example.org/p1> <http://example.org/o1> .',
                    '<http://example.org/s2> <http://example.org/p1> <http://example.org/o1> .',
                    '<http://example.org/s2> <http://example.org/p1> <http://example.org/o2> <http://example.org/g1> .',
                ].sort());
                expect(restored.getQuads(null, null, ex('o1'), null).length).toBe(2);
            });
        });
    }
});

// The probe: three RDF triples written eight
// ways -- "x" four times (twice plain, once typed xsd:string, once through a
// \u escape), "y"@en twice (@EN and @en), "z" under a subject written two ways.
const PROBE_NT = [
    '<http://ex.org/s> <http://ex.org/p> "x" .',
    '<http://ex.org/s> <http://ex.org/p> "x" .',
    '<http://ex.org/s> <http://ex.org/p> "x"^^<http://www.w3.org/2001/XMLSchema#string> .',
    '<http://ex.org/s> <http://ex.org/p> "\\u0078" .',
    '<http://ex.org/s> <http://ex.org/p> "y"@EN .',
    '<http://ex.org/s> <http://ex.org/p> "y"@en .',
    '<http://ex.org/s> <http://ex.org/p> "z" .',
    '<http://ex.org/\\u0073> <http://ex.org/p> "z" .',
].join('\n') + '\n';

describe('a built store holds each quad once', () => {
    const s = df.namedNode('http://ex.org/s');
    const p = df.namedNode('http://ex.org/p');
    const xsdString = df.namedNode('http://www.w3.org/2001/XMLSchema#string');
    const langString = df.namedNode('http://www.w3.org/1999/02/22-rdf-syntax-ns#langString');
    // A literal the factory would not build: an upper-case language tag.
    const upperCaseLang = {
        termType: 'Literal' as const,
        value: 'y',
        language: 'EN',
        datatype: langString,
    } as unknown as Literal;
    const spellings = (): Quad[] => [
        df.quad(s, p, df.literal('x')),
        df.quad(s, p, df.literal('x')),
        df.quad(s, p, df.literal('x', xsdString)),
        df.quad(s, p, upperCaseLang),
        df.quad(s, p, df.literal('y', 'en')),
        df.quad(s, p, df.literal('z')),
        df.quad(s, p, df.literal('z')),
    ];

    for (const { name, options } of VARIANTS) {
        describe(name, () => {
            test('fromString keeps one row per distinct quad', async () => {
                const store = await VortexRdfStore.fromString(PROBE_NT, 'ntriples', options);
                expect(await store.size()).toBe(3);
                expect(store.getQuads().length).toBe(3);
                expect(store.countQuads(null, null, df.literal('x'), null)).toBe(1);
                expect(store.countQuads(null, null, df.literal('y', 'en'), null)).toBe(1);
                expect(store.countQuads(null, p, null, null)).toBe(3);

                const restored = await VortexRdfStore.fromBytes(await store.toBytes());
                expect(await restored.size()).toBe(3);
                expect(restored.getQuads().length).toBe(3);
            });

            test('fromQuads canonicalizes RDF/JS terms before it counts them', async () => {
                const store = await VortexRdfStore.fromQuads(spellings(), options);
                expect(await store.size()).toBe(3);
                expect(store.getQuads().length).toBe(3);
                expect(store.countQuads(null, null, df.literal('x'), null)).toBe(1);

                const viaStream = await VortexRdfStore.fromQuads(
                    Readable.from(spellings(), { objectMode: true }),
                    options,
                );
                expect(await viaStream.size()).toBe(3);
            });
        });
    }

    test('the dictionary holds one code per RDF term', async () => {
        const store = await VortexRdfStore.fromString(PROBE_NT, 'ntriples', { layout: 'dictionary' });
        const dict = store.termDict()!;
        const codes = store.matchCodes()!;
        expect(codes.length).toBe(3);
        // The subject, the predicate, "x", "y"@en, "z" and the default graph:
        // six codes in use across the four columns, each a distinct term.
        const used = new Set([...codes.s, ...codes.p, ...codes.o, ...codes.g]);
        expect(used.size).toBe(6);
        expect(new Set([...used].map(code => dict.decode(code))).size).toBe(6);
        expect(new Set([...used].map(code => dict.decode(code)))).toEqual(new Set([
            '<http://ex.org/s>', '<http://ex.org/p>', '"x"', '"y"@en', '"z"', '',
        ]));
    });
});

describe('fromQuads with an RDF/JS Stream', () => {
    test('accepts a Node Readable in object mode', async () => {
        const viaString = await nquadsStore();
        const quads = viaString.getQuads(null, null, null, null);
        expect(quads.length).toBe(6);

        const stream = Readable.from(quads, { objectMode: true });
        const viaStream = await VortexRdfStore.fromQuads(stream);
        expect(await viaStream.size()).toBe(6);

        const p1 = viaStream.getQuads(null, ex('p1'), null, null);
        expect(p1.length).toBe(3);
    });

    test('propagates a stream error', async () => {
        const stream = new Readable({
            objectMode: true,
            read() {
                this.emit('error', new Error('boom'));
            },
        });

        await expect(VortexRdfStore.fromQuads(stream)).rejects.toThrow(/boom/);
    });
});

describe('RDF format support', () => {
    const TURTLE = '<http://example.org/s> <http://example.org/p> "o" .\n';

    for (const format of ['ntriples', 'nquads', 'turtle', 'trig', 'rdfxml', 'jsonld'] as const) {
        test(`serializes to and parses back from ${format}`, async () => {
            const store = await VortexRdfStore.fromString(TURTLE, 'turtle');
            const text = await store.toRdf(format);
            expect(text.length).toBeGreaterThan(0);

            const reparsed = await VortexRdfStore.fromString(text, format);
            expect(await reparsed.size()).toBe(1);
        });
    }

    test('parses n3', async () => {
        const store = await VortexRdfStore.fromString(TURTLE, 'n3');
        expect(await store.size()).toBe(1);
    });

    // Core's invalid-input error, its kind first, listing the accepted names.
    test('rejects an unsupported format', async () => {
        const refusal = /^Invalid input: unknown RDF format "nope"; expected one of: ntriples, nt,/;
        await expect(VortexRdfStore.fromString(TURTLE, 'nope' as any)).rejects.toThrow(refusal);
        const store = await VortexRdfStore.fromString(TURTLE, 'turtle');
        await expect(store.toRdf('nope' as any)).rejects.toThrow(refusal);
    });
});

describe('free functions', () => {
    test('serializeRdf / deserializeRdf round-trip with options', async () => {
        const bytes = await serializeRdf(NQUADS, 'nquads', {
            layout: 'dictionary',
            indexes: ['secondary-by-reference'],
        });
        expect(bytes).toBeInstanceOf(Uint8Array);

        const nq = await deserializeRdf(bytes, 'nquads');
        expect(nq.trim().split('\n').filter(Boolean).length).toBe(6);
    });

    test('serializeRdf accepts a BuildOptions object', async () => {
        const bytes = await serializeRdf(NQUADS, 'nquads', { layout: 'dictionary' });
        const store = await VortexRdfStore.fromBytes(bytes);
        expect(store.layout()).toBe('dictionary');
        expect(await store.size()).toBe(6);
    });
});

describe('option validation', () => {
    test('rejects an unknown layout', async () => {
        await expect(nquadsStore({ layout: 'Nope' as any })).rejects
            .toThrow(/^Invalid input: unknown layout strategy/);
    });

    test('rejects an unknown index', async () => {
        await expect(nquadsStore({ indexes: ['Nope'] as any })).rejects
            .toThrow(/^Invalid input: unknown index type/);
    });

    test('rejects retired spellings, naming the canonical vocabulary', async () => {
        // Parsing is strict kebab-case: PascalCase inputs error, and the
        // message lists the canonical names.
        await expect(nquadsStore({ layout: 'Dictionary' as any })).rejects
            .toThrow(/unknown layout strategy "Dictionary".*"dictionary"/);
        await expect(nquadsStore({ indexes: ['SecondaryByCopy'] as any })).rejects
            .toThrow(/unknown index type "SecondaryByCopy".*"secondary-by-copy"/);
    });

    test('rejects a non-array indexes option', async () => {
        await expect(nquadsStore({ indexes: 'Nope' as any })).rejects
            .toThrow(/must be an array/);
    });

    test('rejects a non-string layout', async () => {
        await expect(nquadsStore({ layout: 5 as any })).rejects
            .toThrow(/must be a string/);
    });

    test('defaults to the Dictionary layout when options are omitted', async () => {
        const store = await nquadsStore();
        expect(store.layout()).toBe('dictionary');
        expect(await store.size()).toBe(6);
    });
});

describe('multi-chunk payloads', () => {
    // Builders emit chunks of DEFAULT_CHUNK_SIZE (100,000) quads; a payload one
    // row past the boundary exercises every multi-chunk read path.
    const CHUNK_SIZE = 100_000;
    const N = CHUNK_SIZE + 1;

    const manyNquads = (n: number): string => {
        let out = '';
        for (let i = 0; i < n; i++) {
            out += `<http://example.org/s${i}> <http://example.org/p${i % 10}> <http://example.org/o${i % 5}> .\n`;
        }
        return out;
    };

    // Built once for the cases below: the payload's `serializeRdf` bytes, a
    // store built from its text, and that store's `toBytes`.
    let serialized: Uint8Array;
    let built: VortexRdfStore;
    let builtBytes: Uint8Array;
    beforeAll(async () => {
        const text = manyNquads(N);
        serialized = await serializeRdf(text, 'nquads');
        built = await VortexRdfStore.fromString(text, 'nquads');
        builtBytes = await built.toBytes();
    }, 120_000);

    test('serializeRdf/deserializeRdf round-trips nquads across a chunk boundary', async () => {
        const out = await deserializeRdf(serialized, 'nquads');
        expect(out.trim().split('\n').filter(Boolean).length).toBe(N);
    });

    test('fromBytes recovers every chunk', async () => {
        const store = await VortexRdfStore.fromBytes(serialized);
        expect(await store.size()).toBe(N);
    });

    test('toBytes/fromBytes round-trips a multi-chunk store', async () => {
        const restored = await VortexRdfStore.fromBytes(builtBytes);
        expect(await restored.size()).toBe(N);
    });

    test('matchCodeChunks at batchRows 701 and the default concatenate to matchCodes', async () => {
        // Adopted from bytes, the store reads its rows from the encoded columns.
        const adopted = await VortexRdfStore.fromBytes(builtBytes);
        // Every tenth row: a predicate match the store gathers by row id. A
        // batch of 701 rows divides neither its 10,000 rows nor the default
        // batch of 8,192.
        const p3 = ex('p3');
        const cases: [Term | null, number | undefined][] = [[null, undefined], [p3, undefined], [p3, 701]];
        for (const store of [built, adopted]) {
            for (const [predicate, batchRows] of cases) {
                const whole = store.matchCodes(null, predicate, null, null)!;
                const options = batchRows === undefined ? undefined : { batchRows };
                const { cols, sizes } = drainChunks(store.matchCodeChunks(null, predicate, null, null, options)!);
                expect(cols).toEqual(columnArrays(whole));
                const most = batchRows ?? 8192;
                expect(sizes.slice(0, -1).every((size) => size === most), `${sizes}`).toBe(true);
                expect(sizes.at(-1)).toBe(whole.length - most * (sizes.length - 1));
                expect(sizes.length).toBe(Math.ceil(whole.length / most));
            }
        }
    });
}, 120_000);

describe('indexes()', () => {
    test('reports the canonical index names and survives toBytes/fromBytes', async () => {
        const plain = await nquadsStore();
        expect(plain.indexes()).toEqual([]);

        const store = await nquadsStore({
            layout: 'dictionary',
            indexes: ['secondary-by-copy'],
        });
        expect(store.indexes()).toEqual(['secondary-by-copy']);

        const restored = await VortexRdfStore.fromBytes(await store.toBytes());
        expect(restored.indexes()).toEqual(['secondary-by-copy']);
    });
});
