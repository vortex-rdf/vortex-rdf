import { describe, test, expect } from 'vitest';
import { DataFactory } from 'rdf-data-factory';
import { Readable } from 'node:stream';
import type { Quad, Term, Literal, Stream } from '@rdfjs/types';
import {
    VortexRdfStore,
    rdf_to_vortex,
    vortex_to_rdf,
    type BuildOptions,
} from '../entry/node.js';

const df = new DataFactory();

/** Drain the quads of a match() result (via its Symbol.asyncIterator) into an array. */
async function collect(stream: Stream<Quad>): Promise<Quad[]> {
    const out: Quad[] = [];
    for await (const quad of stream as unknown as AsyncIterable<Quad>) out.push(quad);
    return out;
}

const NQUADS = [
    '<http://example.org/s1> <http://example.org/p1> <http://example.org/o1> .',
    '<http://example.org/s1> <http://example.org/p2> "lit" .',
    '<http://example.org/s2> <http://example.org/p1> <http://example.org/o1> .',
    '<http://example.org/s2> <http://example.org/p1> <http://example.org/o2> <http://example.org/g1> .',
    '<http://example.org/s3> <http://example.org/p3> "42"^^<http://www.w3.org/2001/XMLSchema#integer> .',
    '<http://example.org/s4> <http://example.org/p4> "hola"@es .',
].join('\n') + '\n';

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

            test('matches every quad-position pattern', async () => {
                const store = await VortexRdfStore.fromString(NQUADS, 'nquads', options);
                const count = (s: Term | null, p: Term | null, o: Term | null, g: Term | null) =>
                    store.getQuads(s, p, o, g).length;

                // Subject-only: exercises the sorted binary-search path.
                expect(await count(df.namedNode('http://example.org/s1'), null, null, null)).toBe(2);

                // Predicate-only: exercises SecondaryByReference index routing.
                expect(await count(null, df.namedNode('http://example.org/p1'), null, null)).toBe(3);

                // Object-only: exercises the object index / TypedObject columns.
                expect(await count(null, null, df.namedNode('http://example.org/o1'), null)).toBe(2);

                // Graph-only.
                expect(await count(null, null, null, df.namedNode('http://example.org/g1'))).toBe(1);

                // Fully bound.
                expect(await count(
                    df.namedNode('http://example.org/s2'),
                    df.namedNode('http://example.org/p1'),
                    df.namedNode('http://example.org/o2'),
                    df.namedNode('http://example.org/g1'),
                )).toBe(1);

                // Non-existent term.
                expect(await count(df.namedNode('http://example.org/nope'), null, null, null)).toBe(0);
            });

            test('round-trips typed and language literals', async () => {
                const store = await VortexRdfStore.fromString(NQUADS, 'nquads', options);

                const typed = await store.getQuads(
                    null, null,
                    df.literal('42', df.namedNode('http://www.w3.org/2001/XMLSchema#integer')),
                    null,
                );
                expect(typed.length).toBe(1);

                const lang = await store.getQuads(null, null, df.literal('hola', 'es'), null);
                expect(lang.length).toBe(1);
            });

            test('toBytes/fromBytes preserves the store', async () => {
                const store = await VortexRdfStore.fromString(NQUADS, 'nquads', options);
                const bytes = await store.toBytes();
                expect(bytes).toBeInstanceOf(Uint8Array);

                const restored = await VortexRdfStore.fromBytes(bytes);
                expect(await restored.size()).toBe(6);
                expect(restored.layout()).toBe(options.layout);

                const p1 = await restored.getQuads(null, df.namedNode('http://example.org/p1'), null, null);
                expect(p1.length).toBe(3);
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
                const quads = await viaString.getQuads(null, null, null, null);
                expect(quads.length).toBe(6);

                const viaQuads = await VortexRdfStore.fromQuads(quads, options);
                expect(await viaQuads.size()).toBe(6);
                expect(viaQuads.layout()).toBe(options.layout);

                const p1 = await viaQuads.getQuads(null, df.namedNode('http://example.org/p1'), null, null);
                expect(p1.length).toBe(3);
            });
        });
    }
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
        const quads = await viaString.getQuads(null, null, null, null);
        expect(quads.length).toBe(6);

        const stream = Readable.from(quads, { objectMode: true });
        const viaStream = await VortexRdfStore.fromQuads(stream);
        expect(await viaStream.size()).toBe(6);

        const p1 = await viaStream.getQuads(null, df.namedNode('http://example.org/p1'), null, null);
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

    test('rejects an unsupported format', async () => {
        await expect(VortexRdfStore.fromString(TURTLE, 'nope' as any)).rejects.toThrow(
            /Unsupported format/,
        );
    });
});

describe('free functions', () => {
    test('rdf_to_vortex / vortex_to_rdf round-trip with options', async () => {
        const bytes = await rdf_to_vortex(NQUADS, 'nquads', {
            layout: 'dictionary',
            indexes: ['secondary-by-reference'],
        });
        expect(bytes).toBeInstanceOf(Uint8Array);

        const nq = await vortex_to_rdf(bytes, 'nquads');
        expect(nq.trim().split('\n').filter(Boolean).length).toBe(6);
    });

    test('rdf_to_vortex / vortex_to_rdf round-trip nquads', async () => {
        const bytes = await rdf_to_vortex(NQUADS, 'nquads');
        const nq = await vortex_to_rdf(bytes, 'nquads');
        expect(nq.trim().split('\n').filter(Boolean).length).toBe(6);
    });

    test('rdf_to_vortex accepts a BuildOptions object', async () => {
        const bytes = await rdf_to_vortex(NQUADS, 'nquads', { layout: 'dictionary' });
        const store = await VortexRdfStore.fromBytes(bytes);
        expect(store.layout()).toBe('dictionary');
        expect(await store.size()).toBe(6);
    });
});

describe('lazy terms outliving a dictionary rebuild', () => {
    // A Dictionary-layout read hands back `u32` term codes plus a handle on the
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
        const held = await store.getQuads(null, df.namedNode('http://example.org/p3'), null, null);
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

        const after = await store.getQuads(null, df.namedNode('http://example.org/p3'), null, null);
        expect(after.length).toBe(1);
        expect(after[0].subject.value).toBe('http://example.org/s3');
        expect(after[0].object.value).toBe('42');
    });
});

describe('option validation', () => {
    test('rejects an unknown layout', async () => {
        await expect(VortexRdfStore.fromString(NQUADS, 'nquads', { layout: 'Nope' as any })).rejects
            .toThrow(/unknown layout strategy/);
    });

    test('rejects an unknown index', async () => {
        await expect(VortexRdfStore.fromString(NQUADS, 'nquads', { indexes: ['Nope'] as any })).rejects
            .toThrow(/unknown index type/);
    });

    test('rejects retired spellings, naming the canonical vocabulary', async () => {
        // Parsing is strict kebab-case: the old PascalCase inputs error, and
        // the message lists the canonical names.
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
    // Builders emit fixed-size chunks (100_000 quads), and the IPC writer emits
    // one message per chunk. Reading back only the first message silently dropped
    // every quad past the first chunk.
    const CHUNK_SIZE = 100_000;

    const manyNquads = (n: number): string => {
        let out = '';
        for (let i = 0; i < n; i++) {
            out += `<http://example.org/s${i}> <http://example.org/p${i % 10}> <http://example.org/o${i % 5}> .\n`;
        }
        return out;
    };

    test('rdf_to_vortex/vortex_to_rdf round-trips nquads across a chunk boundary', async () => {
        const n = CHUNK_SIZE + 1;
        const bytes = await rdf_to_vortex(manyNquads(n), 'nquads');
        const out = await vortex_to_rdf(bytes, 'nquads');
        expect(out.trim().split('\n').filter(Boolean).length).toBe(n);
    });

    test('fromBytes recovers every chunk', async () => {
        const n = CHUNK_SIZE + 1;
        const bytes = await rdf_to_vortex(manyNquads(n), 'nquads');
        const store = await VortexRdfStore.fromBytes(bytes);
        expect(await store.size()).toBe(n);
    });

    test('toBytes/fromBytes round-trips a multi-chunk store', async () => {
        const n = CHUNK_SIZE + 1;
        const store = await VortexRdfStore.fromString(manyNquads(n), 'nquads');
        const restored = await VortexRdfStore.fromBytes(await store.toBytes());
        expect(await restored.size()).toBe(n);
    });
}, 120_000);

describe('match / getQuads across layouts', () => {
    for (const layout of ['default', 'typed-object', 'dictionary'] as const) {
        test(`${layout}: getQuads returns correctly-decoded terms`, async () => {
            const store = await VortexRdfStore.fromString(NQUADS, 'nquads', { layout });
            const quads = await store.getQuads(null, df.namedNode('http://example.org/p1'), null, null);
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
            const quads = await store.getQuads(null, df.namedNode('http://example.org/p1'), null, null);

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
            const o1 = await restored.getQuads(null, null, df.namedNode('http://example.org/o1'), null);
            expect(o1.length).toBe(2);
        });
    }
});

describe('lazy terms', () => {
    test('Dictionary: .equals compares terms across match results (integer fast path)', async () => {
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads'); // Dictionary default
        const a = await store.getQuads(df.namedNode('http://example.org/s1'), null, null, null);
        const b = await store.getQuads(df.namedNode('http://example.org/s1'), null, null, null);

        // Same subject term, two independent match results of the same store.
        expect(a[0].subject.equals(b[0].subject)).toBe(true);
        // Distinct terms compare unequal (p1 vs p2; subject vs object).
        const aP1 = a.find((q: Quad) => q.predicate.value === 'http://example.org/p1')!;
        const aP2 = a.find((q: Quad) => q.predicate.value === 'http://example.org/p2')!;
        expect(aP1.predicate.equals(aP2.predicate)).toBe(false);
        expect(aP1.subject.equals(aP1.object)).toBe(false);

        // The object <o1> appears under s1/p1 and s2/p1 — equal across results.
        const s2 = await store.getQuads(df.namedNode('http://example.org/s2'), df.namedNode('http://example.org/p1'), null, null);
        const s2o1 = s2.find((q: Quad) => q.object.value === 'http://example.org/o1')!;
        expect(aP1.object.equals(s2o1.object)).toBe(true);
    });

    test('literal value/datatype/language decode lazily and correctly', async () => {
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads');

        const typed = (await store.getQuads(null, df.namedNode('http://example.org/p3'), null, null))[0];
        expect(typed.object.termType).toBe('Literal');
        expect(typed.object.value).toBe('42');
        expect((typed.object as Literal).datatype.value).toBe('http://www.w3.org/2001/XMLSchema#integer');

        const lang = (await store.getQuads(null, df.namedNode('http://example.org/p4'), null, null))[0];
        expect(lang.object.value).toBe('hola');
        expect((lang.object as Literal).language).toBe('es');
        expect((lang.object as Literal).datatype.value).toBe('http://www.w3.org/1999/02/22-rdf-syntax-ns#langString');
    });

    test('interoperates with foreign RDF/JS terms via .equals (both directions)', async () => {
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads');
        const q = (await store.getQuads(
            df.namedNode('http://example.org/s1'), df.namedNode('http://example.org/p1'), null, null))[0];

        expect(q.predicate.equals(df.namedNode('http://example.org/p1'))).toBe(true);
        expect(q.predicate.equals(df.namedNode('http://example.org/nope'))).toBe(false);
        // Foreign term comparing against our lazy term reads our getters.
        expect(df.namedNode('http://example.org/p1').equals(q.predicate)).toBe(true);
        expect(df.literal('hola', 'es').equals(
            (await store.getQuads(null, df.namedNode('http://example.org/p4'), null, null))[0].object,
        )).toBe(true);
    });

    test('Default layout: .equals falls back to value/termType compare', async () => {
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads', { layout: 'default' });
        const a = await store.getQuads(null, df.namedNode('http://example.org/p1'), null, null);
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
        const quad = df.quad(
            df.namedNode('http://example.org/s9'),
            df.namedNode('http://example.org/p9'),
            df.literal('new'),
        );
        await store.addQuad(quad);
        expect(await store.size()).toBe(7);
        expect(await store.has(quad)).toBe(true);
        const matched = await store.getQuads(null, df.namedNode('http://example.org/p9'), null, null);
        expect(matched.length).toBe(1);
        // Decoded values must be correct: appended terms live in the string tail,
        // re-encoded against a fresh dictionary, so the code path (which decodes
        // against the store's cached base dictionary) must not be used here.
        expect(matched[0].subject.value).toBe('http://example.org/s9');
        expect(matched[0].object.value).toBe('new');
        // A pre-existing quad still decodes correctly on the mutated store too.
        const p1 = await store.getQuads(null, df.namedNode('http://example.org/p1'), null, null);
        expect(p1.map((q: Quad) => q.object.value).sort())
            .toEqual(['http://example.org/o1', 'http://example.org/o1', 'http://example.org/o2']);
        // The appended store still serializes to standalone bytes (the terms
        // are re-encoded against a fresh dictionary).
        const restored = await VortexRdfStore.fromBytes(await store.toBytes());
        expect(await restored.size()).toBe(7);
        expect(await restored.has(quad)).toBe(true);
    });

    test('Default layout supports addQuad', async () => {
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads', { layout: 'default' });
        const quad = df.quad(
            df.namedNode('http://example.org/s9'),
            df.namedNode('http://example.org/p9'),
            df.literal('new'),
        );
        await store.addQuad(quad);
        expect(await store.size()).toBe(7);
        expect(await store.has(quad)).toBe(true);
    });

    test('addQuad ignores a quad already present (RDF/JS set semantics)', async () => {
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads');
        const quad = df.quad(
            df.namedNode('http://example.org/s1'),
            df.namedNode('http://example.org/p1'),
            df.namedNode('http://example.org/o1'),
        );
        expect(await store.has(quad)).toBe(true);
        await store.addQuad(quad);
        expect(await store.size()).toBe(6);
    });

    test('addQuads appends a batch, skipping duplicates', async () => {
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads');
        const fresh = df.quad(
            df.namedNode('http://example.org/s9'),
            df.namedNode('http://example.org/p9'),
            df.literal('new'),
        );
        const existing = df.quad(
            df.namedNode('http://example.org/s1'),
            df.namedNode('http://example.org/p1'),
            df.namedNode('http://example.org/o1'),
        );
        await store.addQuads([fresh, fresh, existing]);
        expect(await store.size()).toBe(7);
        expect(await store.has(fresh)).toBe(true);
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
        expect((await store.getQuads()).length).toBe(6);
    });
});

