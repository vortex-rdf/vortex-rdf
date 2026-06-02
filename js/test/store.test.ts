import { describe, test, expect } from 'vitest';
import { DataFactory } from "rdf-data-factory";
import {
    SimpleDictionaryStore,
    ChainedHashStore,
    init_panic_hook,
    nquads_to_vortex,
    vortex_to_nquads
} from '../pkg/vortex_rdf.js';

const df = new DataFactory();

// Initialize panic hook for better error messages
init_panic_hook();

describe('SimpleDictionaryStore basic operations', () => {
    test('create empty store', () => {
        const store = SimpleDictionaryStore.empty();
        expect(store.size()).toBe(0);
    });

    test('fromString and size', async () => {
        const ttl = `
            <http://example.org/s1> <http://example.org/p1> "o1" .
            <http://example.org/s1> <http://example.org/p2> "o2" .
            <http://example.org/s2> <http://example.org/p1> "o3" .
        `;
        const store = await SimpleDictionaryStore.fromString(ttl, "turtle");
        expect(store.size()).toBe(3);
    });

    test('match and iteration', async () => {
        const ttl = `
            <http://example.org/s1> <http://example.org/p1> "o1" .
            <http://example.org/s1> <http://example.org/p2> "o2" .
            <http://example.org/s2> <http://example.org/p1> "o3" .
        `;
        const store = await SimpleDictionaryStore.fromString(ttl, "turtle");

        // Match ?s <p1> ?o
        const matches = await store.match(null, df.namedNode("http://example.org/p1"), null, null);
        expect(matches.size()).toBe(2);

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
        const store = SimpleDictionaryStore.empty();
        expect(store.size()).toBe(0);

        const quad = {
            subject: { termType: 'NamedNode' as const, value: 'http://example.org/s' },
            predicate: { termType: 'NamedNode' as const, value: 'http://example.org/p' },
            object: { termType: 'Literal' as const, value: 'hello' },
            graph: { termType: 'DefaultGraph' as const, value: '' }
        };

        // Add
        await store.addQuad(quad as any);
        expect(store.size()).toBe(1);

        // Check has
        const hasQuad = await store.has(quad as any);
        expect(hasQuad).toBe(true);

        // Delete
        await store.deleteQuad(quad as any);
        expect(store.size()).toBe(0);
    });
});

describe('ChainedHashStore basic operations', () => {
    test('create empty store', () => {
        const store = ChainedHashStore.empty();
        expect(store.size()).toBe(0);
    });

    test('fromString and size', async () => {
        const ttl = `
            <http://example.org/s1> <http://example.org/p1> "o1" .
            <http://example.org/s1> <http://example.org/p2> "o2" .
        `;
        const store = await ChainedHashStore.fromString(ttl, "turtle");
        expect(store.size()).toBe(2);
    });

    test('match and iteration', async () => {
        const ttl = `
            <http://example.org/s1> <http://example.org/p1> "o1" .
            <http://example.org/s1> <http://example.org/p2> "o2" .
            <http://example.org/s2> <http://example.org/p1> "o3" .
        `;
        const store = await ChainedHashStore.fromString(ttl, "turtle");
        
        // Match ?s <p1> ?o
        const matches = await store.match(null, df.namedNode("http://example.org/p1"), null, null);
        expect(matches.size()).toBe(2);

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
        const store = ChainedHashStore.empty();
        expect(store.size()).toBe(0);

        const quad = {
            subject: { termType: 'NamedNode' as const, value: 'http://example.org/s' },
            predicate: { termType: 'NamedNode' as const, value: 'http://example.org/p' },
            object: { termType: 'Literal' as const, value: 'hello' },
            graph: { termType: 'DefaultGraph' as const, value: '' }
        };

        // Add
        await store.addQuad(quad as any);
        expect(store.size()).toBe(1);
        
        // Check has
        const hasQuad = await store.has(quad as any);
        expect(hasQuad).toBe(true);

        // Delete
        await store.deleteQuad(quad as any);
        expect(store.size()).toBe(0);
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
        'UnsortedInMemory',
        'SortedInMemory',
        'ChunkSort'
    ] as const;

    for (const strategy of supportedStrategies) {
        test(`SimpleDictionaryStore.fromString with ${strategy}`, async () => {
            const ttl = `
                <http://example.org/s1> <http://example.org/p1> "o1" .
                <http://example.org/s1> <http://example.org/p2> "o2" .
                <http://example.org/s2> <http://example.org/p1> "o3" .
            `;
            const store = await SimpleDictionaryStore.fromString(ttl, "turtle", strategy);
            expect(store.size()).toBe(3);

            const matches = await store.match(null, df.namedNode("http://example.org/p1"), null, null);
            expect(matches.size()).toBe(2);
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

    test('SimpleDictionaryStore.fromString with GlobalSort throws unsupported error', async () => {
        const ttl = `<http://example.org/s1> <http://example.org/p1> "o1" .`;
        await expect(SimpleDictionaryStore.fromString(ttl, "turtle", 'GlobalSort')).rejects.toThrow(
            /Global-sort strategy is not supported/
        );
    });

    test('nquads_to_vortex with GlobalSort throws unsupported error', async () => {
        const nquads = `<http://example.org/s> <http://example.org/p> "hello" .\n`;
        await expect(nquads_to_vortex(nquads, 'GlobalSort')).rejects.toThrow(
            /Global-sort strategy is not supported/
        );
    });
});
