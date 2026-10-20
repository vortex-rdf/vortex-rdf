import { beforeAll, describe, test, expect } from 'vitest';
import { DataFactory } from 'rdf-data-factory';
import { Readable } from 'node:stream';
import type { Quad, Term, Literal, Stream } from '@rdfjs/types';
import {
    TermDict,
    VortexRdfStore,
    serializeRdf,
    deserializeRdf,
    type BuildOptions,
    type CodeChunk,
    type CodeColumns,
    type ChunkOptions,
    type CodeKeep,
    type Probe,
    type ReadOptions,
} from '@vortex-rdf/vortex-rdf-store';
// The wasm iterator handles, which the package entries do not re-export.
import { CodeChunks, ProbeAnswers } from '../pkg/web/vortex_rdf.js';

const df = new DataFactory();

/** Drain the quads of a match() result (via its Symbol.asyncIterator) into an array. */
async function collect(stream: Stream<Quad>): Promise<Quad[]> {
    const out: Quad[] = [];
    for await (const quad of stream as unknown as AsyncIterable<Quad>) out.push(quad);
    return out;
}

/** The rows of code columns as `[s, p, o, g]` tuples, in column order. */
function codeRows(cols: CodeColumns): number[][] {
    return Array.from({ length: cols.length }, (_, i) => [cols.s[i], cols.p[i], cols.o[i], cols.g[i]]);
}

type Position = 's' | 'p' | 'o' | 'g';

/** Every chunk of `chunks` pulled: the codes of each column a chunk holds,
 *  concatenated across the chunks, and each chunk's row count. */
function drainChunks(chunks: Iterable<CodeChunk>): { cols: Partial<Record<Position, number[]>>; sizes: number[] } {
    const cols: Partial<Record<Position, number[]>> = {};
    const sizes: number[] = [];
    for (const chunk of chunks) {
        sizes.push(chunk.length);
        for (const position of ['s', 'p', 'o', 'g'] as const) {
            const column = chunk[position];
            if (column === undefined) continue;
            expect(column).toBeInstanceOf(Float64Array);
            expect(column.length).toBe(chunk.length);
            (cols[position] ??= []).push(...column);
        }
    }
    return { cols, sizes };
}

/** All four columns of a `matchCodes` result as arrays, as `drainChunks` collects them. */
function columnArrays(cols: CodeColumns): Record<Position, number[]> {
    return { s: [...cols.s], p: [...cols.p], o: [...cols.o], g: [...cols.g] };
}

const NQUADS = [
    '<http://example.org/s1> <http://example.org/p1> <http://example.org/o1> .',
    '<http://example.org/s1> <http://example.org/p2> "lit" .',
    '<http://example.org/s2> <http://example.org/p1> <http://example.org/o1> .',
    '<http://example.org/s2> <http://example.org/p1> <http://example.org/o2> <http://example.org/g1> .',
    '<http://example.org/s3> <http://example.org/p3> "42"^^<http://www.w3.org/2001/XMLSchema#integer> .',
    '<http://example.org/s4> <http://example.org/p4> "hola"@es .',
].join('\n') + '\n';

/** A quad no term of `NQUADS` appears in: absent from any store built from it. */
const NEW_QUAD = df.quad(
    df.namedNode('http://example.org/s9'),
    df.namedNode('http://example.org/p9'),
    df.literal('new'),
);

/** The first quad of `NQUADS`: present in any store built from it. */
const EXISTING_QUAD = df.quad(
    df.namedNode('http://example.org/s1'),
    df.namedNode('http://example.org/p1'),
    df.namedNode('http://example.org/o1'),
);

/** Every build variant reachable from JS, in the canonical kebab-case
 *  vocabulary — the only spellings accepted, and what `layout()` reports. */
const VARIANTS: { name: string; options: BuildOptions }[] = [
    { name: 'Default', options: { layout: 'default' } },
    { name: 'TypedObject', options: { layout: 'typed-object' } },
    { name: 'Dictionary', options: { layout: 'dictionary' } },
    {
        name: 'Default+index',
        options: { layout: 'default', indexes: ['secondary-by-reference'] },
    },
    {
        name: 'Dictionary+index',
        options: { layout: 'dictionary', indexes: ['secondary-by-reference'] },
    },
    {
        name: 'Default+copy-index',
        options: { layout: 'default', indexes: ['secondary-by-copy'] },
    },
    {
        name: 'Dictionary+copy-index',
        options: { layout: 'dictionary', indexes: ['secondary-by-copy'] },
    },
];

describe('build variants', () => {
    for (const { name, options } of VARIANTS) {
        describe(name, () => {
            test('builds and reports its layout', async () => {
                const store = await VortexRdfStore.fromString(NQUADS, 'nquads', options);
                expect(await store.size()).toBe(6);
                expect(store.layout()).toBe(options.layout);
            });

            test('size and has return values, not promises', async () => {
                const store = await VortexRdfStore.fromString(NQUADS, 'nquads', options);
                const ex = (name: string) => df.namedNode(`http://example.org/${name}`);

                expect(store.size()).toBe(6);
                expect(store.has(df.quad(ex('s1'), ex('p1'), ex('o1')))).toBe(true);
                expect(store.has(df.quad(ex('s2'), ex('p1'), ex('o2'), ex('g1')))).toBe(true);
                // An exact four-component lookup: the same triple in the
                // default graph is a different quad.
                expect(store.has(df.quad(ex('s2'), ex('p1'), ex('o2')))).toBe(false);
            });

            test('matches every quad-position pattern', async () => {
                const store = await VortexRdfStore.fromString(NQUADS, 'nquads', options);
                const count = (s: Term | null, p: Term | null, o: Term | null, g: Term | null) => {
                    const n = store.getQuads(s, p, o, g).length;
                    // countQuads answers from the match's row selection alone
                    // and must agree with materializing the same match.
                    expect(store.countQuads(s, p, o, g)).toBe(n);
                    return n;
                };

                // Subject-only: exercises the sorted binary-search path.
                expect(count(df.namedNode('http://example.org/s1'), null, null, null)).toBe(2);

                // Predicate-only: exercises SecondaryByReference index routing.
                expect(count(null, df.namedNode('http://example.org/p1'), null, null)).toBe(3);

                // Object-only: exercises the object index / TypedObject columns.
                expect(count(null, null, df.namedNode('http://example.org/o1'), null)).toBe(2);

                // Graph-only.
                expect(count(null, null, null, df.namedNode('http://example.org/g1'))).toBe(1);

                // Fully bound.
                expect(count(
                    df.namedNode('http://example.org/s2'),
                    df.namedNode('http://example.org/p1'),
                    df.namedNode('http://example.org/o2'),
                    df.namedNode('http://example.org/g1'),
                )).toBe(1);

                // Non-existent term.
                expect(count(df.namedNode('http://example.org/nope'), null, null, null)).toBe(0);
            });

            test('round-trips typed and language literals', async () => {
                const store = await VortexRdfStore.fromString(NQUADS, 'nquads', options);

                const typed = store.getQuads(
                    null, null,
                    df.literal('42', df.namedNode('http://www.w3.org/2001/XMLSchema#integer')),
                    null,
                );
                expect(typed.length).toBe(1);

                const lang = store.getQuads(null, null, df.literal('hola', 'es'), null);
                expect(lang.length).toBe(1);
            });

            test('toBytes/fromBytes preserves the store', async () => {
                const store = await VortexRdfStore.fromString(NQUADS, 'nquads', options);
                const bytes = await store.toBytes();
                expect(bytes).toBeInstanceOf(Uint8Array);

                const restored = await VortexRdfStore.fromBytes(bytes);
                expect(await restored.size()).toBe(6);
                expect(restored.layout()).toBe(options.layout);

                const p1 = restored.getQuads(null, df.namedNode('http://example.org/p1'), null, null);
                expect(p1.length).toBe(3);
                expect(restored.countQuads(null, df.namedNode('http://example.org/p1'), null, null)).toBe(3);
            });

            test('toRdf emits all quads back', async () => {
                const store = await VortexRdfStore.fromString(NQUADS, 'nquads', options);
                const nq = await store.toRdf('nquads');
                const lines = nq.trim().split('\n').filter(Boolean);
                expect(lines.length).toBe(6);

                // Re-parsing the output yields an equivalent store.
                const reparsed = await VortexRdfStore.fromString(nq, 'nquads', options);
                expect(await reparsed.size()).toBe(6);
            });

            test('fromQuads matches fromString', async () => {
                const viaString = await VortexRdfStore.fromString(NQUADS, 'nquads', options);
                const quads = viaString.getQuads(null, null, null, null);
                expect(quads.length).toBe(6);

                const viaQuads = await VortexRdfStore.fromQuads(quads, options);
                expect(await viaQuads.size()).toBe(6);
                expect(viaQuads.layout()).toBe(options.layout);

                const p1 = viaQuads.getQuads(null, df.namedNode('http://example.org/p1'), null, null);
                expect(p1.length).toBe(3);
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

describe('match returns an RDF/JS Stream<Quad>', () => {
    test('for-await and data/end events both yield the matches', async () => {
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads');
        const pattern = [null, df.namedNode('http://example.org/p1'), null, null] as const;

        // match() returns synchronously; the result is an RDF/JS Stream and also
        // implements Symbol.asyncIterator (consumed here with for-await).
        const stream = store.match(...pattern);
        expect(typeof stream.read).toBe('function'); // RDF/JS Stream.read()
        expect(typeof stream.on).toBe('function');    // EventEmitter
        const viaAwait = await collect(stream);
        expect(viaAwait.length).toBe(3);

        // Stream contract: the same pattern re-run and consumed via events.
        const viaEvents = await new Promise<Quad[]>((resolve, reject) => {
            const acc: Quad[] = [];
            const s = store.match(...pattern);
            s.on('data', (q: Quad) => acc.push(q));
            s.on('end', () => resolve(acc));
            s.on('error', reject);
        });
        expect(viaEvents.length).toBe(3);
    });

    test('read() drains the buffered quads after readable', async () => {
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads');
        const stream = store.match(null, df.namedNode('http://example.org/p1'), null, null);
        const acc = await new Promise<Quad[]>((resolve) => {
            const out: Quad[] = [];
            stream.on('readable', () => {
                let q: Quad | null;
                while ((q = stream.read()) !== null) out.push(q);
            });
            stream.on('end', () => resolve(out));
        });
        expect(acc.length).toBe(3);
    });

    test('an empty match ends cleanly with no quads', async () => {
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads');
        const none = await collect(store.match(df.namedNode('http://example.org/nope'), null, null, null));
        expect(none.length).toBe(0);
    });
});

describe('fromQuads with an RDF/JS Stream', () => {
    test('accepts a Node Readable in object mode', async () => {
        const viaString = await VortexRdfStore.fromString(NQUADS, 'nquads');
        const quads = viaString.getQuads(null, null, null, null);
        expect(quads.length).toBe(6);

        const stream = Readable.from(quads, { objectMode: true });
        const viaStream = await VortexRdfStore.fromQuads(stream);
        expect(await viaStream.size()).toBe(6);

        const p1 = viaStream.getQuads(null, df.namedNode('http://example.org/p1'), null, null);
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

describe('lazy terms outliving a dictionary rebuild', () => {
    // A Dictionary-layout read hands back term codes plus a handle on the
    // dictionary they index into. Auto-compaction re-encodes the store against a
    // *fresh* dictionary, renumbering every term, so lazy quads that decoded
    // against the live store would silently resolve old codes to other terms.
    // They must decode against the snapshot taken when they were produced.
    const dictOpts: BuildOptions = { layout: 'dictionary' };
    const PROBE = '<http://example.org/s3>';

    /** Enough quads to cross the auto-compaction floor, all sorting *before*
     *  the probe term so its code is guaranteed to move. */
    function compactionTrigger(): Quad[] {
        const out: Quad[] = [];
        for (let i = 0; i < 4200; i++) {
            out.push(df.quad(
                df.namedNode('http://example.org/aaa' + i),
                df.namedNode('http://example.org/aap'),
                df.namedNode('http://example.org/aao' + i),
            ));
        }
        return out;
    }

    test('getQuads results survive the rebuild', async () => {
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads', dictOpts);
        // Deliberately do not read `.value` yet — the terms stay undecoded, so a
        // stale-dictionary bug cannot be masked by the intern cache.
        const held = store.getQuads(null, df.namedNode('http://example.org/p3'), null, null);
        expect(held.length).toBe(1);

        const codeBefore = store.termDict()!.encode(PROBE);
        await store.addQuads(compactionTrigger());
        // Preconditions, so this cannot pass vacuously if compaction stops
        // firing: the term was renumbered, and its old code now names a
        // *different* term — which is precisely what a lazy quad decoding
        // against the live store would hand back.
        expect(store.termDict()!.encode(PROBE)).not.toBe(codeBefore);
        expect(store.termDict()!.decode(codeBefore!)).not.toBe(PROBE);

        expect(held[0].subject.value).toBe('http://example.org/s3');
        expect(held[0].predicate.value).toBe('http://example.org/p3');
        expect(held[0].object.value).toBe('42');
        expect((held[0].object as Literal).datatype.value)
            .toBe('http://www.w3.org/2001/XMLSchema#integer');
    });

    test('match() results survive the rebuild', async () => {
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads', dictOpts);
        const held = await collect(store.match(null, df.namedNode('http://example.org/p4'), null, null));
        expect(held.length).toBe(1);

        const codeBefore = store.termDict()!.encode(PROBE);
        await store.addQuads(compactionTrigger());
        expect(store.termDict()!.encode(PROBE)).not.toBe(codeBefore);

        expect(held[0].subject.value).toBe('http://example.org/s4');
        expect(held[0].object.value).toBe('hola');
        expect((held[0].object as Literal).language).toBe('es');
    });

    test('reads taken after the rebuild see the new dictionary', async () => {
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads', dictOpts);
        await store.addQuads(compactionTrigger());

        const after = store.getQuads(null, df.namedNode('http://example.org/p3'), null, null);
        expect(after.length).toBe(1);
        expect(after[0].subject.value).toBe('http://example.org/s3');
        expect(after[0].object.value).toBe('42');
    });
});

describe('option validation', () => {
    test('rejects an unknown layout', async () => {
        await expect(VortexRdfStore.fromString(NQUADS, 'nquads', { layout: 'Nope' as any })).rejects
            .toThrow(/^Invalid input: unknown layout strategy/);
    });

    test('rejects an unknown index', async () => {
        await expect(VortexRdfStore.fromString(NQUADS, 'nquads', { indexes: ['Nope'] as any })).rejects
            .toThrow(/^Invalid input: unknown index type/);
    });

    test('rejects retired spellings, naming the canonical vocabulary', async () => {
        // Parsing is strict kebab-case: PascalCase inputs error, and the
        // message lists the canonical names.
        await expect(VortexRdfStore.fromString(NQUADS, 'nquads', { layout: 'Dictionary' as any })).rejects
            .toThrow(/unknown layout strategy "Dictionary".*"dictionary"/);
        await expect(VortexRdfStore.fromString(NQUADS, 'nquads', { indexes: ['SecondaryByCopy'] as any })).rejects
            .toThrow(/unknown index type "SecondaryByCopy".*"secondary-by-copy"/);
    });

    test('rejects a non-array indexes option', async () => {
        await expect(VortexRdfStore.fromString(NQUADS, 'nquads', { indexes: 'Nope' as any })).rejects
            .toThrow(/must be an array/);
    });

    test('rejects a non-string layout', async () => {
        await expect(VortexRdfStore.fromString(NQUADS, 'nquads', { layout: 5 as any })).rejects
            .toThrow(/must be a string/);
    });

    test('defaults to the Dictionary layout when options are omitted', async () => {
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads');
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
        const p3 = df.namedNode('http://example.org/p3');
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

describe('match / getQuads across layouts', () => {
    for (const layout of ['default', 'typed-object', 'dictionary'] as const) {
        test(`${layout}: getQuads returns correctly-decoded terms`, async () => {
            const store = await VortexRdfStore.fromString(NQUADS, 'nquads', { layout });
            const quads = store.getQuads(null, df.namedNode('http://example.org/p1'), null, null);
            expect(quads.length).toBe(3);

            // Assert on the decoded term strings, not just the count: under the
            // Dictionary layout a lost term dictionary would yield the right row
            // count but codes instead of IRIs.
            expect(quads.map((q: Quad) => q.subject.value).sort())
                .toEqual(['http://example.org/s1', 'http://example.org/s2', 'http://example.org/s2']);
            expect(quads.map((q: Quad) => q.object.value).sort())
                .toEqual(['http://example.org/o1', 'http://example.org/o1', 'http://example.org/o2']);
        });

        test(`${layout}: a matched subset round-trips through fromQuads → bytes`, async () => {
            const store = await VortexRdfStore.fromString(NQUADS, 'nquads', { layout });
            const quads = store.getQuads(null, df.namedNode('http://example.org/p1'), null, null);

            // Rebuild a standalone store from the matched quads and round-trip it.
            const derived = await VortexRdfStore.fromQuads(quads, { layout });
            const restored = await VortexRdfStore.fromBytes(await derived.toBytes());
            expect(await restored.size()).toBe(3);

            const lines = (await restored.toRdf('nquads')).trim().split('\n').filter(Boolean).sort();
            expect(lines).toEqual([
                '<http://example.org/s1> <http://example.org/p1> <http://example.org/o1> .',
                '<http://example.org/s2> <http://example.org/p1> <http://example.org/o1> .',
                '<http://example.org/s2> <http://example.org/p1> <http://example.org/o2> <http://example.org/g1> .',
            ].sort());

            // And the rebuilt store is still queryable.
            const o1 = restored.getQuads(null, null, df.namedNode('http://example.org/o1'), null);
            expect(o1.length).toBe(2);
        });
    }
});

describe('lazy terms', () => {
    test('Dictionary: .equals compares terms across match results (integer fast path)', async () => {
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads'); // Dictionary default
        const a = store.getQuads(df.namedNode('http://example.org/s1'), null, null, null);
        const b = store.getQuads(df.namedNode('http://example.org/s1'), null, null, null);

        // Same subject term, two independent match results of the same store.
        expect(a[0].subject.equals(b[0].subject)).toBe(true);
        // Distinct terms compare unequal (p1 vs p2; subject vs object).
        const aP1 = a.find((q: Quad) => q.predicate.value === 'http://example.org/p1')!;
        const aP2 = a.find((q: Quad) => q.predicate.value === 'http://example.org/p2')!;
        expect(aP1.predicate.equals(aP2.predicate)).toBe(false);
        expect(aP1.subject.equals(aP1.object)).toBe(false);

        // The object <o1> appears under s1/p1 and s2/p1 — equal across results.
        const s2 = store.getQuads(df.namedNode('http://example.org/s2'), df.namedNode('http://example.org/p1'), null, null);
        const s2o1 = s2.find((q: Quad) => q.object.value === 'http://example.org/o1')!;
        expect(aP1.object.equals(s2o1.object)).toBe(true);
    });

    test('literal value/datatype/language decode lazily and correctly', async () => {
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads');

        const typed = (store.getQuads(null, df.namedNode('http://example.org/p3'), null, null))[0];
        expect(typed.object.termType).toBe('Literal');
        expect(typed.object.value).toBe('42');
        expect((typed.object as Literal).datatype.value).toBe('http://www.w3.org/2001/XMLSchema#integer');

        const lang = (store.getQuads(null, df.namedNode('http://example.org/p4'), null, null))[0];
        expect(lang.object.value).toBe('hola');
        expect((lang.object as Literal).language).toBe('es');
        expect((lang.object as Literal).datatype.value).toBe('http://www.w3.org/1999/02/22-rdf-syntax-ns#langString');
    });

    test('interoperates with foreign RDF/JS terms via .equals (both directions)', async () => {
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads');
        const q = (store.getQuads(
            df.namedNode('http://example.org/s1'), df.namedNode('http://example.org/p1'), null, null))[0];

        expect(q.predicate.equals(df.namedNode('http://example.org/p1'))).toBe(true);
        expect(q.predicate.equals(df.namedNode('http://example.org/nope'))).toBe(false);
        // Foreign term comparing against our lazy term reads our getters.
        expect(df.namedNode('http://example.org/p1').equals(q.predicate)).toBe(true);
        expect(df.literal('hola', 'es').equals(
            (store.getQuads(null, df.namedNode('http://example.org/p4'), null, null))[0].object,
        )).toBe(true);
    });

    test('Default layout: .equals falls back to value/termType compare', async () => {
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads', { layout: 'default' });
        const a = store.getQuads(null, df.namedNode('http://example.org/p1'), null, null);
        // <o1> appears twice under p1 — equal by value even without codes.
        const o1s = a.filter((q: Quad) => q.object.value === 'http://example.org/o1');
        expect(o1s.length).toBe(2);
        expect(o1s[0].object.equals(o1s[1].object)).toBe(true);
        expect(a[0].predicate.equals(df.namedNode('http://example.org/p1'))).toBe(true);
    });
});

describe('adding quads', () => {
    test('Dictionary layout supports addQuad via the string tail', async () => {
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads', { layout: 'dictionary' });
        // Every term here is absent from the dictionary built at load time;
        // the quad lands in the string tail and must still be found by match.
        await store.addQuad(NEW_QUAD);
        expect(await store.size()).toBe(7);
        expect(await store.has(NEW_QUAD)).toBe(true);
        const matched = store.getQuads(null, df.namedNode('http://example.org/p9'), null, null);
        expect(matched.length).toBe(1);
        // Decoded values must be correct: appended terms live in the string tail,
        // re-encoded against a fresh dictionary, so the code path (which decodes
        // against the store's cached base dictionary) must not be used here.
        expect(matched[0].subject.value).toBe('http://example.org/s9');
        expect(matched[0].object.value).toBe('new');
        // A pre-existing quad still decodes correctly on the mutated store too.
        const p1 = store.getQuads(null, df.namedNode('http://example.org/p1'), null, null);
        expect(p1.map((q: Quad) => q.object.value).sort())
            .toEqual(['http://example.org/o1', 'http://example.org/o1', 'http://example.org/o2']);
        // The appended store still serializes to standalone bytes (the terms
        // are re-encoded against a fresh dictionary).
        const restored = await VortexRdfStore.fromBytes(await store.toBytes());
        expect(await restored.size()).toBe(7);
        expect(await restored.has(NEW_QUAD)).toBe(true);
    });

    test('Default layout supports addQuad', async () => {
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads', { layout: 'default' });
        await store.addQuad(NEW_QUAD);
        expect(await store.size()).toBe(7);
        expect(await store.has(NEW_QUAD)).toBe(true);
    });

    test('addQuad ignores a quad already present (RDF/JS set semantics)', async () => {
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads');
        expect(await store.has(EXISTING_QUAD)).toBe(true);
        await store.addQuad(EXISTING_QUAD);
        expect(await store.size()).toBe(6);
    });

    test('addQuads appends a batch, skipping duplicates', async () => {
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads');
        await store.addQuads([NEW_QUAD, NEW_QUAD, EXISTING_QUAD]);
        expect(await store.size()).toBe(7);
        expect(await store.has(NEW_QUAD)).toBe(true);
    });
});

describe('empty store + delete', () => {
    test('empty() builds a store of size 0', async () => {
        const store = VortexRdfStore.empty();
        expect(await store.size()).toBe(0);
    });

    test('deleteQuad drops a quad added to an empty store', async () => {
        const store = VortexRdfStore.empty();
        const quad = df.quad(
            df.namedNode('http://example.org/s'),
            df.namedNode('http://example.org/p'),
            df.literal('hello'),
        );

        await store.addQuad(quad);
        expect(await store.size()).toBe(1);
        expect(await store.has(quad)).toBe(true);

        await store.deleteQuad(quad);
        expect(await store.size()).toBe(0);
    });
});

describe('duck-typed quads', () => {
    // Deliberately not a real Quad instance (no `.equals`): the store accepts any
    // structurally-shaped value, reading only the four term fields.
    const structural = {
        subject: { termType: 'NamedNode' as const, value: 'http://example.org/s' },
        predicate: { termType: 'NamedNode' as const, value: 'http://example.org/p' },
        object: { termType: 'Literal' as const, value: 'hello' },
        graph: { termType: 'DefaultGraph' as const, value: '' },
    } as unknown as Quad;

    test('addQuad / has / deleteQuad accept a non-Quad with the right fields', async () => {
        const store = VortexRdfStore.empty();

        await store.addQuad(structural);
        expect(await store.size()).toBe(1);
        expect(await store.has(structural)).toBe(true);

        await store.deleteQuad(structural);
        expect(await store.size()).toBe(0);
    });
});

describe('getQuads without a pattern', () => {
    test('zero arguments returns every quad', async () => {
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads');
        const r = store.getQuads();
        expect(Array.isArray(r)).toBe(true);
        expect(r).not.toBeInstanceOf(Promise);
        expect(r.length).toBe(6);
    });
});


describe('indexes()', () => {
    test('reports the canonical index names and survives toBytes/fromBytes', async () => {
        const plain = await VortexRdfStore.fromString(NQUADS, 'nquads');
        expect(plain.indexes()).toEqual([]);

        const store = await VortexRdfStore.fromString(NQUADS, 'nquads', {
            layout: 'dictionary',
            indexes: ['secondary-by-copy'],
        });
        expect(store.indexes()).toEqual(['secondary-by-copy']);

        const restored = await VortexRdfStore.fromBytes(await store.toBytes());
        expect(restored.indexes()).toEqual(['secondary-by-copy']);
    });
});

describe('matchCodes / termDict gates', () => {
    const p1 = df.namedNode('http://example.org/p1');

    test('Dictionary: codes agree with getQuads and decode through termDict', async () => {
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads', { layout: 'dictionary' });
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

        const full = store.matchCodes(
            df.namedNode('http://example.org/s2'),
            p1,
            df.namedNode('http://example.org/o2'),
            df.namedNode('http://example.org/g1'),
        )!;
        expect(full.length).toBe(1);
        expect(dict.decode(full.g[0])).toBe('<http://example.org/g1>');
    });

    for (const layout of ['default', 'typed-object'] as const) {
        test(`${layout}: matchCodes is null and termDict undefined`, async () => {
            const store = await VortexRdfStore.fromString(NQUADS, 'nquads', { layout });
            expect(store.matchCodes(null, p1, null, null)).toBeNull();
            expect(store.termDict()).toBeUndefined();
        });

        test(`${layout}: matchCodes throws on an invalid pattern instead of returning null`, async () => {
            const store = await VortexRdfStore.fromString(NQUADS, 'nquads', { layout });
            expect(() => store.matchCodes(null, null, null, df.literal('g'))).toThrow(/Invalid graph term/);
        });
    }

    test('a pending append closes the code path', async () => {
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads', { layout: 'dictionary' });
        expect(store.matchCodes(null, p1, null, null)).not.toBeNull();
        await store.addQuad(NEW_QUAD);
        expect(store.matchCodes(null, p1, null, null)).toBeNull();
        expect(store.termDict()).toBeUndefined();
        // The term path still answers.
        expect(store.getQuads(null, p1, null, null).length).toBe(3);
    });

    test('TermDict edges: out-of-range decode and unknown encode are undefined', async () => {
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads', { layout: 'dictionary' });
        const dict = store.termDict()!;
        expect(dict.decode(2 ** 31)).toBeUndefined();
        expect(dict.encode('<http://example.org/nope>')).toBeUndefined();
        // The default graph is the empty string in this vocabulary.
        expect(typeof dict.encode('')).toBe('number');
        const code = dict.encode('<http://example.org/s1>')!;
        expect(dict.decode(code)).toBe('<http://example.org/s1>');
    });

    test('TermDict.encode takes variant spellings and throws on a malformed one', async () => {
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads', { layout: 'dictionary' });
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
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads', { layout: 'dictionary' });
        const dict = store.termDict()!;
        const code = dict.encode('<http://example.org/s1>')!;
        expect(dict.decode(code)).toBe('<http://example.org/s1>');
        expect(dict.decode(2 ** 32 + code)).toBeUndefined();
        expect(dict.decode(2 ** 40 + code)).toBeUndefined();
        expect(dict.decode(Number.MAX_SAFE_INTEGER)).toBeUndefined();
    });

    test('TermDict refuses what is no code', async () => {
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads', { layout: 'dictionary' });
        const dict = store.termDict()!;
        for (const bad of [-1, -0.5, 1.5, NaN, Infinity, -Infinity, 2 ** 53, 2 ** 64]) {
            expect(() => dict.decode(bad), `${bad}`).toThrow(/code/);
        }
    });

    test('TermDict.decode refuses what is no number instead of coercing it', async () => {
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads', { layout: 'dictionary' });
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
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads', {
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
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads', { layout: 'dictionary' });
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
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads', { layout: 'dictionary' });
        const strings = await VortexRdfStore.fromString(NQUADS, 'nquads', { layout: 'default' });
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
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads', { layout: 'dictionary' });
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
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads', { layout: 'dictionary' });
        const strings = await VortexRdfStore.fromString(NQUADS, 'nquads', { layout: 'default' });
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
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads', { layout: 'dictionary' });
        const o1c = store.termDict()!.encode('<http://example.org/o1>')!;
        const probes: Probe[] = [
            [null, p1, null, null],
            ['http://example.org/s1', null, null, null],
            { s: df.namedNode('http://example.org/nobody') },
            { p: p1, keep: { o: [o1c, o1c + 1] }, limit: 1, offset: 1 },
            { s: df.namedNode('http://example.org/s2'), limit: 1 },
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
        const strings = await VortexRdfStore.fromString(NQUADS, 'nquads', { layout: 'default' });
        // Run, this probe throws: a keep needs the Dictionary layout.
        const failing: Probe = { p: p1, keep: { o: [0, 5] } };
        expect(() => strings.countQuadsMany([failing])).toThrow(/keep needs the Dictionary layout/);
        expect([...strings.matchCodesMany([failing, failing, failing])]).toEqual([null, null, null]);
    });

    test('countQuadsMany counts each probe as countQuads does, on every layout', async () => {
        for (const layout of ['dictionary', 'default', 'typed-object'] as const) {
            const store = await VortexRdfStore.fromString(NQUADS, 'nquads', { layout });
            const probes: Probe[] = [
                [null, p1, null, null],
                { s: 'http://example.org/s1' },
                { s: df.namedNode('http://example.org/nobody'), limit: 1 },
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
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads', { layout: 'dictionary' });
        const o1c = store.termDict()!.encode('<http://example.org/o1>')!;
        const kept: Probe = { keep: { o: [o1c, o1c + 1] } };
        expect(store.countQuadsMany([kept, { ...kept, limit: 1 }])).toEqual([2, 1]);
        // On a store without codes, the first probe with a keep makes the call throw.
        const strings = await VortexRdfStore.fromString(NQUADS, 'nquads', { layout: 'default' });
        expect(() => strings.countQuadsMany([{}, { p: p1, keep: { o: [0, 5] } }]))
            .toThrow(/keep needs the Dictionary layout/);
    });

    test('an aborted signal ends a chunk iteration and a batch at the next pull', async () => {
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads', { layout: 'dictionary' });
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
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads', { layout: 'dictionary' });
        const signal = AbortSignal.abort();
        const chunks = store.matchCodeChunks(null, null, null, null, { signal })![Symbol.iterator]();
        expect(chunks.next()).toEqual({ done: true, value: undefined });
        const answers = store.matchCodesMany([{}, [null, p1, null, null]], { signal })[Symbol.iterator]();
        expect(answers.next()).toEqual({ done: true, value: undefined });
        // The arguments are checked first, and `null` still means no codes.
        expect(() => store.matchCodeChunks(null, null, null, null, { batchRows: 0, signal })).toThrow(RangeError);
        expect(() => store.matchCodesMany([{ bogus: 1 } as unknown as Probe], { signal })).toThrow(TypeError);
        const strings = await VortexRdfStore.fromString(NQUADS, 'nquads', { layout: 'default' });
        expect(strings.matchCodeChunks(null, null, null, null, { signal })).toBeNull();
    });

    test('an empty match yields no chunk', async () => {
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads', { layout: 'dictionary' });
        const nobody = df.namedNode('http://example.org/nobody');
        const empty: [Term | null, ChunkOptions][] = [[nobody, {}], [null, { keep: { s: [] } }], [null, { limit: 0 }]];
        for (const [subject, options] of empty) {
            const iterator = store.matchCodeChunks(subject, null, null, null, options)![Symbol.iterator]();
            expect(iterator.next(), JSON.stringify(options)).toEqual({ done: true, value: undefined });
        }
    });
});

describe('deleteQuad tombstones a base row', () => {
    const p1 = df.namedNode('http://example.org/p1');
    const s1 = df.namedNode('http://example.org/s1');

    const LAYOUTS: { name: string; options: BuildOptions }[] = [
        { name: 'default', options: { layout: 'default' } },
        { name: 'dictionary', options: { layout: 'dictionary' } },
        {
            name: 'dictionary+secondary-by-reference',
            options: { layout: 'dictionary', indexes: ['secondary-by-reference'] },
        },
    ];

    for (const { name, options } of LAYOUTS) {
        test(name, async () => {
            const store = await VortexRdfStore.fromString(NQUADS, 'nquads', options);
            await store.deleteQuad(EXISTING_QUAD);
            expect(await store.size()).toBe(5);
            expect(await store.has(EXISTING_QUAD)).toBe(false);
            expect(store.countQuads(null, p1, null, null)).toBe(2);
            expect(store.getQuads(s1, null, null, null).length).toBe(1);

            // Deleting an absent quad is a no-op.
            await store.deleteQuad(EXISTING_QUAD);
            expect(await store.size()).toBe(5);

            const restored = await VortexRdfStore.fromBytes(await store.toBytes());
            expect(await restored.size()).toBe(5);
            expect(await restored.has(EXISTING_QUAD)).toBe(false);
            expect(restored.countQuads(null, p1, null, null)).toBe(2);
        });
    }
});

describe('graph and blank-node terms', () => {
    const GRAPHS = [
        '_:b1 <http://example.org/p5> _:b2 .',
        '<http://example.org/s> <http://example.org/p> "x" <http://example.org/g> .',
        '<http://example.org/s> <http://example.org/p> "y" .',
    ].join('\n') + '\n';

    for (const layout of ['default', 'dictionary'] as const) {
        test(`${layout}: graph terms decode`, async () => {
            const store = await VortexRdfStore.fromString(GRAPHS, 'nquads', { layout });
            const s = df.namedNode('http://example.org/s');
            const inDefault = store.getQuads(s, null, df.literal('y'), null);
            expect(inDefault.length).toBe(1);
            expect(inDefault[0].graph.termType).toBe('DefaultGraph');
            expect(inDefault[0].graph.value).toBe('');
            expect(inDefault[0].graph.equals(df.defaultGraph())).toBe(true);

            const named = store.getQuads(s, null, df.literal('x'), null);
            expect(named.length).toBe(1);
            expect(named[0].graph.termType).toBe('NamedNode');
            expect(named[0].graph.value).toBe('http://example.org/g');
        });

        test(`${layout}: the default-graph pattern matches only default-graph quads`, async () => {
            const store = await VortexRdfStore.fromString(GRAPHS, 'nquads', { layout });
            expect(store.countQuads(null, null, null, df.defaultGraph())).toBe(2);
            expect(store.countQuads(null, null, null, df.namedNode('http://example.org/g'))).toBe(1);
        });

        test(`${layout}: blank nodes decode and match`, async () => {
            const store = await VortexRdfStore.fromString(GRAPHS, 'nquads', { layout });
            const quads = store.getQuads(null, df.namedNode('http://example.org/p5'), null, null);
            expect(quads.length).toBe(1);
            expect(quads[0].subject.termType).toBe('BlankNode');
            expect(quads[0].object.termType).toBe('BlankNode');
            expect(quads[0].subject.value).toBe('b1');
            expect(quads[0].object.value).toBe('b2');
            expect(store.countQuads(df.blankNode('b1'), null, null, null)).toBe(1);
            expect(store.countQuads(null, null, df.blankNode('b2'), null)).toBe(1);

            // A blank-node quad added through the RDF/JS boundary round-trips.
            const added = df.quad(df.blankNode('b3'), df.namedNode('http://example.org/p5'), df.blankNode('b4'));
            await store.addQuad(added);
            expect(await store.has(added)).toBe(true);
            expect(store.countQuads(null, df.namedNode('http://example.org/p5'), null, null)).toBe(2);
        });
    }
});

describe('term validation and malformed input', () => {
    const good = NEW_QUAD;
    const s = { termType: 'NamedNode', value: 'http://example.org/s' };
    const p = { termType: 'NamedNode', value: 'http://example.org/p' };
    const o = { termType: 'Literal', value: 'o' };
    const badObject = { subject: { termType: 'Literal', value: 'x' }, predicate: p, object: o } as unknown as Quad;

    test('the packed path reports the index of a malformed quad', async () => {
        await expect(VortexRdfStore.fromQuads([badObject])).rejects.toThrow(/Invalid quad object at index 0/);
        await expect(VortexRdfStore.fromQuads([good, badObject])).rejects.toThrow(/Invalid quad object at index 1/);
        // A non-string term value is malformed, not coerced.
        const numeric = { subject: s, predicate: p, object: { termType: 'Literal', value: 42 } } as unknown as Quad;
        await expect(VortexRdfStore.fromQuads([numeric])).rejects.toThrow(/Invalid quad object at index 0/);
    });

    test('the stream path reports the position of a malformed quad', async () => {
        const stream = Readable.from([good, badObject], { objectMode: true });
        await expect(VortexRdfStore.fromQuads(stream)).rejects
            .toThrow(/Invalid quad object in RDF\/JS stream at position 1/);
    });

    test('addQuads rejects a batch with a malformed quad and leaves the store unchanged', async () => {
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads');
        await expect(store.addQuads([good, badObject])).rejects.toThrow(/Invalid quad object/);
        expect(await store.size()).toBe(6);
    });

    test('addQuad / deleteQuad reject and has throws a malformed quad object as an Error', async () => {
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads');
        await expect(store.addQuad({} as any)).rejects.toThrow(/Invalid quad/);
        await expect(store.deleteQuad({} as any)).rejects.toThrow(/Invalid quad/);
        expect(() => store.has({} as any)).toThrow(/Invalid quad/);
        expect(() => store.has({} as any)).toThrow(Error);
        expect(await store.size()).toBe(6);
    });

    test('fromBytes / fromQuads reject garbage input', async () => {
        await expect(VortexRdfStore.fromBytes(new Uint8Array([1, 2, 3]))).rejects.toThrow();
        await expect(VortexRdfStore.fromQuads(42 as any)).rejects.toThrow();
    });

    test('every quad entry point rejects a malformed term', async () => {
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads');
        const cases = [
            { subject: s, predicate: p, object: { termType: 'Variable', value: 'v' } },
            { subject: s, predicate: { termType: 'Literal', value: 'p' }, object: o },
            { subject: s, predicate: p, object: o, graph: { termType: 'Literal', value: 'g' } },
            { subject: s, predicate: p, object: o, graph: { termType: 'Variable', value: 'g' } },
            { subject: s, predicate: p, object: { termType: 'Literal' } },
            { subject: { termType: 'NamedNode', value: 'not an iri' }, predicate: p, object: o },
            // A bare string is a NamedNode only in a pattern, never in a quad.
            { subject: s, predicate: p, object: 'http://example.org/o' },
            // An invalid datatype IRI is an error, never a plain literal.
            {
                subject: s,
                predicate: p,
                object: { termType: 'Literal', value: 'o', datatype: { termType: 'NamedNode', value: 'not an iri' } },
            },
            // So is a language that is not a string.
            { subject: s, predicate: p, object: { termType: 'Literal', value: 'o', language: 42 } },
            // A datatype is a NamedNode, never a bare IRI string.
            {
                subject: s,
                predicate: p,
                object: { termType: 'Literal', value: '5', datatype: 'http://www.w3.org/2001/XMLSchema#integer' },
            },
        ] as unknown as Quad[];
        for (const quad of cases) {
            // Soft, so that one run reports every entry point that accepts a case.
            expect.soft(() => store.has(quad)).toThrow(/Invalid quad object/);
            await expect.soft(store.addQuad(quad)).rejects.toThrow(/Invalid quad object/);
            await expect.soft(store.deleteQuad(quad)).rejects.toThrow(/Invalid quad object/);
            await expect.soft(store.addQuads([quad])).rejects.toThrow(/Invalid quad object at index 0/);
            await expect.soft(VortexRdfStore.fromQuads([quad])).rejects.toThrow(/Invalid quad object at index 0/);
            await expect.soft(VortexRdfStore.fromQuads(Readable.from([quad], { objectMode: true }))).rejects
                .toThrow(/Invalid quad object in RDF\/JS stream at position 0/);
        }
        expect(await store.size()).toBe(6);
    });

    test('an absent graph field means the default graph', async () => {
        const store = VortexRdfStore.empty();
        await store.addQuad({ subject: s, predicate: p, object: o } as unknown as Quad);
        expect(await store.has(df.quad(
            df.namedNode('http://example.org/s'), df.namedNode('http://example.org/p'), df.literal('o'),
        ))).toBe(true);
        expect(store.countQuads(null, null, null, df.defaultGraph())).toBe(1);
    });

    test('blank-node labels are validated on both ingest paths', async () => {
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads');
        const bad = df.quad(df.blankNode('not a label'), df.namedNode('http://example.org/p'), df.literal('o'));
        await expect(store.addQuad(bad)).rejects.toThrow(/Invalid quad object/);
        await expect(VortexRdfStore.fromQuads([bad])).rejects.toThrow(/Invalid quad object at index 0/);
        expect(() => store.has(bad)).toThrow(/Invalid quad object/);
    });

    test('an invalid pattern term throws instead of matching everything', async () => {
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads');
        expect(() => store.getQuads(df.literal('x'), null, null, null)).toThrow(/Invalid subject term/);
        expect(() => store.countQuads(null, df.literal('x'), null, null)).toThrow(/Invalid predicate term/);
        expect(() => store.countQuads(null, null, { termType: 'Nope', value: '' } as unknown as Term, null))
            .toThrow(/Invalid object term/);
        expect(() => store.match(null, null, null, df.literal('g'))).toThrow(/Invalid graph term/);
        expect(() => store.matchCodes(null, null, null, df.literal('g'))).toThrow(/Invalid graph term/);
        expect(() => store.countQuads(df.namedNode('not an iri'), null, null, null)).toThrow(Error);
        // An invalid datatype IRI throws instead of matching the plain literal "lit".
        expect(() => store.countQuads(null, null, df.literal('lit', df.namedNode('not an iri')), null))
            .toThrow(/Invalid object term/);
        // So does a datatype that is a bare IRI string, not a NamedNode.
        const bareDatatype = { termType: 'Literal', value: '42', datatype: 'http://www.w3.org/2001/XMLSchema#integer' };
        expect(() => store.countQuads(null, null, bareDatatype as unknown as Term, null)).toThrow(/Invalid object term/);
    });

    test('a Variable term is a wildcard', async () => {
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads');
        expect(store.countQuads(df.variable('s'), df.variable('p'), df.variable('o'), df.variable('g'))).toBe(6);
        expect(store.countQuads(df.variable('s'), df.namedNode('http://example.org/p1'), null, null)).toBe(3);
    });

    test('a bare IRI string is accepted as a NamedNode pattern term', async () => {
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads');
        expect(store.countQuads('http://example.org/s1', null, null, null)).toBe(2);
        expect(store.getQuads(null, 'http://example.org/p1', null, null).length).toBe(3);
        expect(store.countQuads(null, null, null, 'http://example.org/g1')).toBe(1);
    });
});

describe('disposal', () => {
    test('a freed store throws on use', async () => {
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads');
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
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads');
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
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads');
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
