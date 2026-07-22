import { describe, test, expect } from 'vitest';
import { DataFactory } from 'rdf-data-factory';
import { Readable } from 'node:stream';
import {
    VortexStore,
    init_panic_hook,
    rdf_to_vortex,
    vortex_to_rdf,
    nquads_to_vortex,
    vortex_to_nquads,
    type BuildOptions,
} from '../entry/node.js';

const df = new DataFactory();

init_panic_hook();

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
                const store = await VortexStore.fromString(NQUADS, 'nquads', options);
                expect(await store.size()).toBe(6);
                expect(store.layout()).toBe(options.layout);
            });

            test('matches every quad-position pattern', async () => {
                const store = await VortexStore.fromString(NQUADS, 'nquads', options);

                // Subject-only: exercises the sorted binary-search path.
                const s1 = await store.match(df.namedNode('http://example.org/s1'), null, null, null);
                expect(await s1.size()).toBe(2);

                // Predicate-only: exercises SecondaryByReference index routing.
                const p1 = await store.match(null, df.namedNode('http://example.org/p1'), null, null);
                expect(await p1.size()).toBe(3);

                // Object-only: exercises the object index / TypedObject columns.
                const o1 = await store.match(null, null, df.namedNode('http://example.org/o1'), null);
                expect(await o1.size()).toBe(2);

                // Graph-only.
                const g1 = await store.match(null, null, null, df.namedNode('http://example.org/g1'));
                expect(await g1.size()).toBe(1);

                // Fully bound.
                const spog = await store.match(
                    df.namedNode('http://example.org/s2'),
                    df.namedNode('http://example.org/p1'),
                    df.namedNode('http://example.org/o2'),
                    df.namedNode('http://example.org/g1'),
                );
                expect(await spog.size()).toBe(1);

                // Non-existent term.
                const none = await store.match(df.namedNode('http://example.org/nope'), null, null, null);
                expect(await none.size()).toBe(0);
            });

            test('round-trips typed and language literals', async () => {
                const store = await VortexStore.fromString(NQUADS, 'nquads', options);

                const typed = await store.match(
                    null, null,
                    df.literal('42', df.namedNode('http://www.w3.org/2001/XMLSchema#integer')),
                    null,
                );
                expect(await typed.size()).toBe(1);

                const lang = await store.match(null, null, df.literal('hola', 'es'), null);
                expect(await lang.size()).toBe(1);
            });

            test('toBytes/fromBytes preserves the store', async () => {
                const store = await VortexStore.fromString(NQUADS, 'nquads', options);
                const bytes = await store.toBytes();
                expect(bytes).toBeInstanceOf(Uint8Array);

                const restored = await VortexStore.fromBytes(bytes);
                expect(await restored.size()).toBe(6);
                expect(restored.layout()).toBe(options.layout);

                const p1 = await restored.match(null, df.namedNode('http://example.org/p1'), null, null);
                expect(await p1.size()).toBe(3);
            });

            test('toRdf emits all quads back', async () => {
                const store = await VortexStore.fromString(NQUADS, 'nquads', options);
                const nq = await store.toRdf('nquads');
                const lines = nq.trim().split('\n').filter(Boolean);
                expect(lines.length).toBe(6);

                // Re-parsing the output yields an equivalent store.
                const reparsed = await VortexStore.fromString(nq, 'nquads', options);
                expect(await reparsed.size()).toBe(6);
            });

            test('fromQuads matches fromString', async () => {
                const viaString = await VortexStore.fromString(NQUADS, 'nquads', options);
                const quads = [...(await viaString.values() as any)];
                expect(quads.length).toBe(6);

                const viaQuads = await VortexStore.fromQuads(quads as any, options);
                expect(await viaQuads.size()).toBe(6);
                expect(viaQuads.layout()).toBe(options.layout);

                const p1 = await viaQuads.match(null, df.namedNode('http://example.org/p1'), null, null);
                expect(await p1.size()).toBe(3);
            });
        });
    }
});

describe('fromQuads with an RDF/JS Stream', () => {
    test('accepts a Node Readable in object mode', async () => {
        const viaString = await VortexStore.fromString(NQUADS, 'nquads');
        const quads = [...(await viaString.values() as any)];
        expect(quads.length).toBe(6);

        const stream = Readable.from(quads, { objectMode: true });
        const viaStream = await VortexStore.fromQuads(stream as any);
        expect(await viaStream.size()).toBe(6);

        const p1 = await viaStream.match(null, df.namedNode('http://example.org/p1'), null, null);
        expect(await p1.size()).toBe(3);
    });

    test('propagates a stream error', async () => {
        const stream = new Readable({
            objectMode: true,
            read() {
                this.emit('error', new Error('boom'));
            },
        });

        await expect(VortexStore.fromQuads(stream as any)).rejects.toThrow(/boom/);
    });
});

describe('RDF format support', () => {
    const TURTLE = '<http://example.org/s> <http://example.org/p> "o" .\n';

    for (const format of ['ntriples', 'nquads', 'turtle', 'trig', 'rdfxml', 'jsonld'] as const) {
        test(`serializes to and parses back from ${format}`, async () => {
            const store = await VortexStore.fromString(TURTLE, 'turtle');
            const text = await store.toRdf(format);
            expect(text.length).toBeGreaterThan(0);

            const reparsed = await VortexStore.fromString(text, format);
            expect(await reparsed.size()).toBe(1);
        });
    }

    test('parses n3', async () => {
        const store = await VortexStore.fromString(TURTLE, 'n3');
        expect(await store.size()).toBe(1);
    });

    test('rejects an unsupported format', async () => {
        await expect(VortexStore.fromString(TURTLE, 'nope' as any)).rejects.toThrow(
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
        const store = await VortexStore.fromBytes(bytes);
        expect(store.layout()).toBe('Dictionary');
        expect(await store.size()).toBe(6);
    });
});

describe('option validation', () => {
    test('rejects an unknown builder', async () => {
        await expect(VortexStore.fromString(NQUADS, 'nquads', { builder: 'Nope' as any })).rejects
            .toThrow(/Unknown builder strategy/);
    });

    test('rejects an unknown layout', async () => {
        await expect(VortexStore.fromString(NQUADS, 'nquads', { layout: 'Nope' as any })).rejects
            .toThrow(/Unknown layout strategy/);
    });

    test('rejects an unknown index', async () => {
        await expect(VortexStore.fromString(NQUADS, 'nquads', { indexes: ['Nope'] as any })).rejects
            .toThrow(/Unknown index type/);
    });

    test('rejects a non-array indexes option', async () => {
        await expect(VortexStore.fromString(NQUADS, 'nquads', { indexes: 'Nope' as any })).rejects
            .toThrow(/must be an array/);
    });

    test('rejects a non-string layout', async () => {
        await expect(VortexStore.fromString(NQUADS, 'nquads', { layout: 5 as any })).rejects
            .toThrow(/must be a string/);
    });

    test('SortedStream stays unavailable in WASM', async () => {
        await expect(VortexStore.fromString(NQUADS, 'nquads', { builder: 'SortedStream' as any }))
            .rejects.toThrow(/Unknown builder strategy/);
    });

    test('defaults to Unsorted/Default when options are omitted', async () => {
        const store = await VortexStore.fromString(NQUADS, 'nquads');
        expect(store.layout()).toBe('Default');
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
        const store = await VortexStore.fromBytes(bytes);
        expect(await store.size()).toBe(n);
    });

    test('toBytes/fromBytes round-trips a multi-chunk store', async () => {
        const n = CHUNK_SIZE + 1;
        const store = await VortexStore.fromString(manyNquads(n), 'nquads');
        const restored = await VortexStore.fromBytes(await store.toBytes());
        expect(await restored.size()).toBe(n);
    });
}, 120_000);

describe('stores derived from match', () => {
    for (const layout of ['Default', 'TypedObject', 'Dictionary'] as const) {
        test(`${layout}: a derived store can be serialized back to bytes`, async () => {
            const store = await VortexStore.fromString(NQUADS, 'nquads', { builder: 'Sorted', layout });
            const derived = await store.match(null, df.namedNode('http://example.org/p1'), null, null);
            expect(await derived.size()).toBe(3);

            // Two hazards here: `match` leaves an unevaluated filter node (no IPC
            // encoding), and under the Dictionary layout it can slice away row 0,
            // which carries the term dictionary the codes decode against.
            const restored = await VortexStore.fromBytes(await derived.toBytes());
            expect(await restored.size()).toBe(3);

            // Decode the content, not just the row count: a lost dictionary still
            // yields the right number of rows, but undecodable terms.
            const lines = (await restored.toRdf('nquads')).trim().split('\n').filter(Boolean).sort();
            expect(lines).toEqual([
                '<http://example.org/s1> <http://example.org/p1> <http://example.org/o1> .',
                '<http://example.org/s2> <http://example.org/p1> <http://example.org/o1> .',
                '<http://example.org/s2> <http://example.org/p1> <http://example.org/o2> <http://example.org/g1> .',
            ].sort());

            // And the restored store is still queryable.
            const o1 = await restored.match(null, null, df.namedNode('http://example.org/o1'), null);
            expect(await o1.size()).toBe(2);
        });

        test(`${layout}: a derived store serializes to RDF`, async () => {
            const store = await VortexStore.fromString(NQUADS, 'nquads', { builder: 'Sorted', layout });
            const derived = await store.match(null, df.namedNode('http://example.org/p1'), null, null);
            const lines = (await derived.toRdf('nquads')).trim().split('\n').filter(Boolean);
            expect(lines.length).toBe(3);
        });
    }
});

describe('adding quads', () => {
    test('Dictionary layout supports addQuad via the string tail', async () => {
        const store = await VortexStore.fromString(NQUADS, 'nquads', { layout: 'Dictionary' });
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
        const matched = await store.match(null, df.namedNode('http://example.org/p9'), null, null);
        expect(await matched.size()).toBe(1);
        // The appended store still serializes to standalone bytes (the terms
        // are re-encoded against a fresh dictionary).
        const restored = await VortexStore.fromBytes(await store.toBytes());
        expect(await restored.size()).toBe(7);
        expect(await restored.has(quad as any)).toBe(true);
    });

    test('Default layout supports addQuad', async () => {
        const store = await VortexStore.fromString(NQUADS, 'nquads', { layout: 'Default' });
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
        const store = await VortexStore.fromString(NQUADS, 'nquads');
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
        const store = await VortexStore.fromString(NQUADS, 'nquads');
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
