import { describe, test, expect } from 'vitest';
import { DataFactory } from 'rdf-data-factory';
import { Readable } from 'node:stream';
import {
    VortexRdfStore,
    init_panic_hook,
    rdf_to_vortex,
    vortex_to_rdf,
    nquads_to_vortex,
    vortex_to_nquads,
    type BuildOptions,
} from '../entry/node.js';

const df = new DataFactory();

init_panic_hook();

/** Drain the quads of a match() result (via its Symbol.asyncIterator) into an array. */
async function collect(stream: any): Promise<any[]> {
    const out: any[] = [];
    for await (const quad of stream) out.push(quad);
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

/** Every build variant reachable from JS. */
const VARIANTS: { name: string; options: BuildOptions }[] = [
    { name: 'Unsorted/Default', options: { builder: 'Unsorted', layout: 'Default' } },
    { name: 'Unsorted/TypedObject', options: { builder: 'Unsorted', layout: 'TypedObject' } },
    { name: 'Unsorted/Dictionary', options: { builder: 'Unsorted', layout: 'Dictionary' } },
    { name: 'Sorted/Default', options: { builder: 'Sorted', layout: 'Default' } },
    { name: 'Sorted/TypedObject', options: { builder: 'Sorted', layout: 'TypedObject' } },
    { name: 'Sorted/Dictionary', options: { builder: 'Sorted', layout: 'Dictionary' } },
    {
        name: 'Sorted/Default+index',
        options: { builder: 'Sorted', layout: 'Default', indexes: ['SecondaryByReference'] },
    },
    {
        name: 'Sorted/Dictionary+index',
        options: { builder: 'Sorted', layout: 'Dictionary', indexes: ['SecondaryByReference'] },
    },
    {
        name: 'Sorted/Default+copy-index',
        options: { builder: 'Sorted', layout: 'Default', indexes: ['SecondaryByCopy'] },
    },
    {
        name: 'Sorted/Dictionary+copy-index',
        options: { builder: 'Sorted', layout: 'Dictionary', indexes: ['SecondaryByCopy'] },
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
                const count = (s: any, p: any, o: any, g: any) =>
                    store.getQuads(s, p, o, g).then(qs => qs.length);

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

                const viaQuads = await VortexRdfStore.fromQuads(quads as any, options);
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
        expect(typeof (stream as any).read).toBe('function'); // RDF/JS Stream.read()
        expect(typeof (stream as any).on).toBe('function');   // EventEmitter
        const viaAwait = await collect(stream);
        expect(viaAwait.length).toBe(3);

        // Stream contract: the same pattern re-run and consumed via events.
        const viaEvents = await new Promise<any[]>((resolve, reject) => {
            const acc: any[] = [];
            const s = store.match(...pattern);
            s.on('data', (q: any) => acc.push(q));
            s.on('end', () => resolve(acc));
            s.on('error', reject);
        });
        expect(viaEvents.length).toBe(3);
    });

    test('read() drains the buffered quads after readable', async () => {
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads');
        const stream: any = store.match(null, df.namedNode('http://example.org/p1'), null, null);
        const acc = await new Promise<any[]>((resolve) => {
            const out: any[] = [];
            stream.on('readable', () => {
                let q;
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
        const viaStream = await VortexRdfStore.fromQuads(stream as any);
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

        await expect(VortexRdfStore.fromQuads(stream as any)).rejects.toThrow(/boom/);
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
            builder: 'Sorted',
            layout: 'Dictionary',
            indexes: ['SecondaryByReference'],
        });
        expect(bytes).toBeInstanceOf(Uint8Array);

        const nq = await vortex_to_rdf(bytes, 'nquads');
        expect(nq.trim().split('\n').filter(Boolean).length).toBe(6);
    });

    test('nquads_to_vortex / vortex_to_nquads still work', async () => {
        const bytes = await nquads_to_vortex(NQUADS);
        const nq = await vortex_to_nquads(bytes);
        expect(nq.trim().split('\n').filter(Boolean).length).toBe(6);
    });

    test('nquads_to_vortex accepts a BuildOptions object', async () => {
        const bytes = await nquads_to_vortex(NQUADS, { builder: 'Sorted', layout: 'Dictionary' });
        const store = await VortexRdfStore.fromBytes(bytes);
        expect(store.layout()).toBe('Dictionary');
        expect(await store.size()).toBe(6);
    });
});

describe('option validation', () => {
    test('rejects an unknown builder', async () => {
        await expect(VortexRdfStore.fromString(NQUADS, 'nquads', { builder: 'Nope' as any })).rejects
            .toThrow(/Unknown builder strategy/);
    });

    test('rejects an unknown layout', async () => {
        await expect(VortexRdfStore.fromString(NQUADS, 'nquads', { layout: 'Nope' as any })).rejects
            .toThrow(/Unknown layout strategy/);
    });

    test('rejects an unknown index', async () => {
        await expect(VortexRdfStore.fromString(NQUADS, 'nquads', { indexes: ['Nope'] as any })).rejects
            .toThrow(/Unknown index type/);
    });

    test('rejects a non-array indexes option', async () => {
        await expect(VortexRdfStore.fromString(NQUADS, 'nquads', { indexes: 'Nope' as any })).rejects
            .toThrow(/must be an array/);
    });

    test('rejects a non-string layout', async () => {
        await expect(VortexRdfStore.fromString(NQUADS, 'nquads', { layout: 5 as any })).rejects
            .toThrow(/must be a string/);
    });

    test('SortedStream stays unavailable in WASM', async () => {
        await expect(VortexRdfStore.fromString(NQUADS, 'nquads', { builder: 'SortedStream' as any }))
            .rejects.toThrow(/Unknown builder strategy/);
    });

    test('defaults to Unsorted/Dictionary when options are omitted', async () => {
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads');
        expect(store.layout()).toBe('Dictionary');
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

    test('nquads_to_vortex/vortex_to_nquads round-trips across a chunk boundary', async () => {
        const n = CHUNK_SIZE + 1;
        const bytes = await nquads_to_vortex(manyNquads(n));
        const out = await vortex_to_nquads(bytes);
        expect(out.trim().split('\n').filter(Boolean).length).toBe(n);
    });

    test('fromBytes recovers every chunk', async () => {
        const n = CHUNK_SIZE + 1;
        const bytes = await nquads_to_vortex(manyNquads(n));
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
    for (const layout of ['Default', 'TypedObject', 'Dictionary'] as const) {
        test(`${layout}: getQuads returns correctly-decoded terms`, async () => {
            const store = await VortexRdfStore.fromString(NQUADS, 'nquads', { builder: 'Sorted', layout });
            const quads = await store.getQuads(null, df.namedNode('http://example.org/p1'), null, null);
            expect(quads.length).toBe(3);

            // Assert on the decoded term strings, not just the count: under the
            // Dictionary layout a lost term dictionary would yield the right row
            // count but codes instead of IRIs.
            expect(quads.map(q => q.subject.value).sort())
                .toEqual(['http://example.org/s1', 'http://example.org/s2', 'http://example.org/s2']);
            expect(quads.map(q => q.object.value).sort())
                .toEqual(['http://example.org/o1', 'http://example.org/o1', 'http://example.org/o2']);
        });

        test(`${layout}: a matched subset round-trips through fromQuads → bytes`, async () => {
            const store = await VortexRdfStore.fromString(NQUADS, 'nquads', { builder: 'Sorted', layout });
            const quads = await store.getQuads(null, df.namedNode('http://example.org/p1'), null, null);

            // Rebuild a standalone store from the matched quads and round-trip it.
            const derived = await VortexRdfStore.fromQuads(quads as any, { builder: 'Sorted', layout });
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
        const aP1 = a.find(q => q.predicate.value === 'http://example.org/p1')!;
        const aP2 = a.find(q => q.predicate.value === 'http://example.org/p2')!;
        expect(aP1.predicate.equals(aP2.predicate)).toBe(false);
        expect(aP1.subject.equals(aP1.object)).toBe(false);

        // The object <o1> appears under s1/p1 and s2/p1 — equal across results.
        const s2 = await store.getQuads(df.namedNode('http://example.org/s2'), df.namedNode('http://example.org/p1'), null, null);
        const s2o1 = s2.find(q => q.object.value === 'http://example.org/o1')!;
        expect(aP1.object.equals(s2o1.object)).toBe(true);
    });

    test('literal value/datatype/language decode lazily and correctly', async () => {
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads');

        const typed = (await store.getQuads(null, df.namedNode('http://example.org/p3'), null, null))[0];
        expect(typed.object.termType).toBe('Literal');
        expect(typed.object.value).toBe('42');
        expect((typed.object as any).datatype.value).toBe('http://www.w3.org/2001/XMLSchema#integer');

        const lang = (await store.getQuads(null, df.namedNode('http://example.org/p4'), null, null))[0];
        expect(lang.object.value).toBe('hola');
        expect((lang.object as any).language).toBe('es');
        expect((lang.object as any).datatype.value).toBe('http://www.w3.org/1999/02/22-rdf-syntax-ns#langString');
    });

    test('interoperates with foreign RDF/JS terms via .equals (both directions)', async () => {
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads');
        const q = (await store.getQuads(
            df.namedNode('http://example.org/s1'), df.namedNode('http://example.org/p1'), null, null))[0];

        expect(q.predicate.equals(df.namedNode('http://example.org/p1'))).toBe(true);
        expect(q.predicate.equals(df.namedNode('http://example.org/nope'))).toBe(false);
        // Foreign term comparing against our lazy term reads our getters.
        expect(df.namedNode('http://example.org/p1').equals(q.predicate as any)).toBe(true);
        expect(df.literal('hola', 'es').equals(
            (await store.getQuads(null, df.namedNode('http://example.org/p4'), null, null))[0].object as any,
        )).toBe(true);
    });

    test('Default layout: .equals falls back to value/termType compare', async () => {
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads', { layout: 'Default' });
        const a = await store.getQuads(null, df.namedNode('http://example.org/p1'), null, null);
        // <o1> appears twice under p1 — equal by value even without codes.
        const o1s = a.filter(q => q.object.value === 'http://example.org/o1');
        expect(o1s.length).toBe(2);
        expect(o1s[0].object.equals(o1s[1].object)).toBe(true);
        expect(a[0].predicate.equals(df.namedNode('http://example.org/p1'))).toBe(true);
    });
});

describe('adding quads', () => {
    test('Dictionary layout supports addQuad via the string tail', async () => {
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads', { layout: 'Dictionary' });
        // Every term here is absent from the dictionary built at load time;
        // the quad lands in the string tail and must still be found by match.
        const quad = df.quad(
            df.namedNode('http://example.org/s9'),
            df.namedNode('http://example.org/p9'),
            df.literal('new'),
        );
        await store.addQuad(quad as any);
        expect(await store.size()).toBe(7);
        expect(await store.has(quad as any)).toBe(true);
        const matched = await store.getQuads(null, df.namedNode('http://example.org/p9'), null, null);
        expect(matched.length).toBe(1);
        // Decoded values must be correct: appended terms live in the string tail,
        // re-encoded against a fresh dictionary, so the code path (which decodes
        // against the store's cached base dictionary) must not be used here.
        expect(matched[0].subject.value).toBe('http://example.org/s9');
        expect(matched[0].object.value).toBe('new');
        // A pre-existing quad still decodes correctly on the mutated store too.
        const p1 = await store.getQuads(null, df.namedNode('http://example.org/p1'), null, null);
        expect(p1.map(q => q.object.value).sort())
            .toEqual(['http://example.org/o1', 'http://example.org/o1', 'http://example.org/o2']);
        // The appended store still serializes to standalone bytes (the terms
        // are re-encoded against a fresh dictionary).
        const restored = await VortexRdfStore.fromBytes(await store.toBytes());
        expect(await restored.size()).toBe(7);
        expect(await restored.has(quad as any)).toBe(true);
    });

    test('Default layout supports addQuad', async () => {
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads', { layout: 'Default' });
        const quad = df.quad(
            df.namedNode('http://example.org/s9'),
            df.namedNode('http://example.org/p9'),
            df.literal('new'),
        );
        await store.addQuad(quad as any);
        expect(await store.size()).toBe(7);
        expect(await store.has(quad as any)).toBe(true);
    });

    test('addQuad ignores a quad already present (RDF/JS set semantics)', async () => {
        const store = await VortexRdfStore.fromString(NQUADS, 'nquads');
        const quad = df.quad(
            df.namedNode('http://example.org/s1'),
            df.namedNode('http://example.org/p1'),
            df.namedNode('http://example.org/o1'),
        );
        expect(await store.has(quad as any)).toBe(true);
        await store.addQuad(quad as any);
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
        await store.addQuads([fresh, fresh, existing] as any);
        expect(await store.size()).toBe(7);
        expect(await store.has(fresh as any)).toBe(true);
    });
});
