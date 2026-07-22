import { describe, test, expect } from 'vitest';
import { DataFactory } from "rdf-data-factory";
import {
    VortexRdfStore,
    init_panic_hook,
    nquads_to_vortex,
    vortex_to_nquads
} from '../entry/node.js';

const df = new DataFactory();

// Initialize panic hook for better error messages
init_panic_hook();

/** Drain the quads of a match() result (via its Symbol.asyncIterator) into an array. */
async function collect(stream: any): Promise<any[]> {
    const out: any[] = [];
    for await (const quad of stream) out.push(quad);
    return out;
}

describe('VortexRdfStore basic operations', () => {
    test('create empty store', async () => {
        const store = VortexRdfStore.empty();
        expect(await store.size()).toBe(0);
    });

    test('fromString and size', async () => {
        const ttl = `
            <http://example.org/s1> <http://example.org/p1> "o1" .
            <http://example.org/s1> <http://example.org/p2> "o2" .
            <http://example.org/s2> <http://example.org/p1> "o3" .
        `;
        const store = await VortexRdfStore.fromString(ttl, "turtle");
        expect(await store.size()).toBe(3);
    });

    test('match streams the matching quads (Source.match)', async () => {
        const ttl = `
            <http://example.org/s1> <http://example.org/p1> "o1" .
            <http://example.org/s1> <http://example.org/p2> "o2" .
            <http://example.org/s2> <http://example.org/p1> "o3" .
        `;
        const store = await VortexRdfStore.fromString(ttl, "turtle");

        // Match ?s <p1> ?o — `match` returns synchronously; the Stream<Quad>
        // also supports `for await` (Symbol.asyncIterator).
        const results = await collect(store.match(null, df.namedNode("http://example.org/p1"), null, null));
        expect(results.length).toBe(2);

        const subjects = results.map(q => q.subject.value);
        expect(subjects).toContain('http://example.org/s1');
        expect(subjects).toContain('http://example.org/s2');
    });

    test('match result is also an RDF/JS Stream (data/end events)', async () => {
        const ttl = `
            <http://example.org/s1> <http://example.org/p1> "o1" .
            <http://example.org/s2> <http://example.org/p1> "o3" .
        `;
        const store = await VortexRdfStore.fromString(ttl, "turtle");

        const stream = store.match(null, df.namedNode("http://example.org/p1"), null, null);
        const got = await new Promise<any[]>((resolve, reject) => {
            const acc: any[] = [];
            stream.on('data', (q: any) => acc.push(q));
            stream.on('end', () => resolve(acc));
            stream.on('error', reject);
        });
        expect(got.length).toBe(2);
    });

    test('getQuads materializes the matching quads into an array', async () => {
        const ttl = `
            <http://example.org/s1> <http://example.org/p1> "o1" .
            <http://example.org/s1> <http://example.org/p2> "o2" .
            <http://example.org/s2> <http://example.org/p1> "o3" .
        `;
        const store = await VortexRdfStore.fromString(ttl, "turtle");

        const quads = await store.getQuads(null, df.namedNode("http://example.org/p1"), null, null);
        expect(quads.length).toBe(2);
        expect(quads.map(q => q.subject.value).sort())
            .toEqual(['http://example.org/s1', 'http://example.org/s2']);

        // No pattern → every quad.
        expect((await store.getQuads()).length).toBe(3);
    });

    test('add and delete quads', async () => {
        const store = VortexRdfStore.empty();
        expect(await store.size()).toBe(0);

        const quad = {
            subject: { termType: 'NamedNode' as const, value: 'http://example.org/s' },
            predicate: { termType: 'NamedNode' as const, value: 'http://example.org/p' },
            object: { termType: 'Literal' as const, value: 'hello' },
            graph: { termType: 'DefaultGraph' as const, value: '' }
        };

        // Add
        await store.addQuad(quad as any);
        expect(await store.size()).toBe(1);

        // Check has
        const hasQuad = await store.has(quad as any);
        expect(hasQuad).toBe(true);

        // Delete
        await store.deleteQuad(quad as any);
        expect(await store.size()).toBe(0);
    });
});

describe('Helper serialization methods', () => {
    test('nquads_to_vortex and vortex_to_nquads roundtrip', async () => {
        const nquads = `<http://example.org/s> <http://example.org/p> "hello" .\n`;
        const vortexBytes = await nquads_to_vortex(nquads);
        expect(vortexBytes).toBeInstanceOf(Uint8Array);
        expect(vortexBytes.length).toBeGreaterThan(0);

        const restored = await vortex_to_nquads(vortexBytes);
        expect(restored.trim()).toBe(nquads.trim());
    });
});

describe('Builder strategies', () => {
    const supportedStrategies = [
        'Unsorted',
        'Sorted',
    ] as const;

    for (const strategy of supportedStrategies) {
        test(`VortexRdfStore.fromString with ${strategy}`, async () => {
            const ttl = `
                <http://example.org/s1> <http://example.org/p1> "o1" .
                <http://example.org/s1> <http://example.org/p2> "o2" .
                <http://example.org/s2> <http://example.org/p1> "o3" .
            `;
            const store = await VortexRdfStore.fromString(ttl, "turtle", strategy);
            expect(await store.size()).toBe(3);

            const matches = await store.getQuads(null, df.namedNode("http://example.org/p1"), null, null);
            expect(matches.length).toBe(2);
        });

        test(`nquads_to_vortex with ${strategy}`, async () => {
            const nquads = `<http://example.org/s> <http://example.org/p> "hello" .\n`;
            const vortexBytes = await nquads_to_vortex(nquads, strategy);
            expect(vortexBytes).toBeInstanceOf(Uint8Array);
            expect(vortexBytes.length).toBeGreaterThan(0);

            const restored = await vortex_to_nquads(vortexBytes);
            expect(restored.trim()).toBe(nquads.trim());
        });
    }

    test('nquads_to_vortex with an unknown strategy throws', async () => {
        const nquads = `<http://example.org/s> <http://example.org/p> "hello" .\n`;
        await expect(nquads_to_vortex(nquads, 'SortedStream' as any)).rejects.toThrow(
            /Unknown builder strategy/
        );
    });
});
