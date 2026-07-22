import { describe, test, expect } from 'vitest';
import { DataFactory } from "rdf-data-factory";
import {
    VortexStore,
    init_panic_hook,
    nquads_to_vortex,
    vortex_to_nquads
} from '../entry/node.js';

const df = new DataFactory();

// Initialize panic hook for better error messages
init_panic_hook();

describe('VortexStore basic operations', () => {
    test('create empty store', async () => {
        const store = VortexStore.empty();
        expect(await store.size()).toBe(0);
    });

    test('fromString and size', async () => {
        const ttl = `
            <http://example.org/s1> <http://example.org/p1> "o1" .
            <http://example.org/s1> <http://example.org/p2> "o2" .
            <http://example.org/s2> <http://example.org/p1> "o3" .
        `;
        const store = await VortexStore.fromString(ttl, "turtle");
        expect(await store.size()).toBe(3);
    });

    test('match and iteration', async () => {
        const ttl = `
            <http://example.org/s1> <http://example.org/p1> "o1" .
            <http://example.org/s1> <http://example.org/p2> "o2" .
            <http://example.org/s2> <http://example.org/p1> "o3" .
        `;
        const store = await VortexStore.fromString(ttl, "turtle");

        // Match ?s <p1> ?o
        const matches = await store.match(null, df.namedNode("http://example.org/p1"), null, null);
        expect(await matches.size()).toBe(2);

        const iterator = await matches.values();
        const results: any[] = [];
        for (const quad of iterator as any) {
            results.push(quad);
        }

        expect(results.length).toBe(2);

        // Assert contents
        const subjects = results.map(q => q.subject.value);
        expect(subjects).toContain('http://example.org/s1');
        expect(subjects).toContain('http://example.org/s2');
    });

    test('add and delete quads', async () => {
        const store = VortexStore.empty();
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
        test(`VortexStore.fromString with ${strategy}`, async () => {
            const ttl = `
                <http://example.org/s1> <http://example.org/p1> "o1" .
                <http://example.org/s1> <http://example.org/p2> "o2" .
                <http://example.org/s2> <http://example.org/p1> "o3" .
            `;
            const store = await VortexStore.fromString(ttl, "turtle", strategy);
            expect(await store.size()).toBe(3);

            const matches = await store.match(null, df.namedNode("http://example.org/p1"), null, null);
            expect(await matches.size()).toBe(2);
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
