// Mutating a store: reads that outlive a dictionary rebuild, adding quads,
// deleting them, duck-typed quads, and deletes that tombstone a base row.
import { describe, test, expect } from 'vitest';
import type { Quad, Literal } from '@rdfjs/types';
import { VortexRdfStore, type BuildOptions } from '@vortex-rdf/vortex-rdf-store';
import { EXISTING_QUAD, NEW_QUAD, VARIANTS, collect, df, ex, nquadsStore } from './support.js';

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
            out.push(df.quad(ex('aaa' + i), ex('aap'), ex('aao' + i)));
        }
        return out;
    }

    test('getQuads results survive the rebuild', async () => {
        const store = await nquadsStore(dictOpts);
        // Deliberately do not read `.value` yet — the terms stay undecoded, so a
        // stale-dictionary bug cannot be masked by the intern cache.
        const held = store.getQuads(null, ex('p3'), null, null);
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
        const store = await nquadsStore(dictOpts);
        const held = await collect(store.match(null, ex('p4'), null, null));
        expect(held.length).toBe(1);

        const codeBefore = store.termDict()!.encode(PROBE);
        await store.addQuads(compactionTrigger());
        expect(store.termDict()!.encode(PROBE)).not.toBe(codeBefore);

        expect(held[0].subject.value).toBe('http://example.org/s4');
        expect(held[0].object.value).toBe('hola');
        expect((held[0].object as Literal).language).toBe('es');
    });

    test('reads taken after the rebuild see the new dictionary', async () => {
        const store = await nquadsStore(dictOpts);
        await store.addQuads(compactionTrigger());

        const after = store.getQuads(null, ex('p3'), null, null);
        expect(after.length).toBe(1);
        expect(after[0].subject.value).toBe('http://example.org/s3');
        expect(after[0].object.value).toBe('42');
    });
});

describe('adding quads', () => {
    test('Dictionary layout supports addQuad via the string tail', async () => {
        const store = await nquadsStore({ layout: 'dictionary' });
        // Every term here is absent from the dictionary built at load time;
        // the quad lands in the string tail and must still be found by match.
        await store.addQuad(NEW_QUAD);
        expect(await store.size()).toBe(7);
        expect(await store.has(NEW_QUAD)).toBe(true);
        const matched = store.getQuads(null, ex('p9'), null, null);
        expect(matched.length).toBe(1);
        // Decoded values must be correct: appended terms live in the string tail,
        // re-encoded against a fresh dictionary, so the code path (which decodes
        // against the store's cached base dictionary) must not be used here.
        expect(matched[0].subject.value).toBe('http://example.org/s9');
        expect(matched[0].object.value).toBe('new');
        // A pre-existing quad still decodes correctly on the mutated store too.
        const p1 = store.getQuads(null, ex('p1'), null, null);
        expect(p1.map((q: Quad) => q.object.value).sort())
            .toEqual(['http://example.org/o1', 'http://example.org/o1', 'http://example.org/o2']);
        // The appended store still serializes to standalone bytes (the terms
        // are re-encoded against a fresh dictionary).
        const restored = await VortexRdfStore.fromBytes(await store.toBytes());
        expect(await restored.size()).toBe(7);
        expect(await restored.has(NEW_QUAD)).toBe(true);
    });

    test('Default layout supports addQuad', async () => {
        const store = await nquadsStore({ layout: 'default' });
        await store.addQuad(NEW_QUAD);
        expect(await store.size()).toBe(7);
        expect(await store.has(NEW_QUAD)).toBe(true);
    });

    test('addQuad ignores a quad already present (RDF/JS set semantics)', async () => {
        const store = await nquadsStore();
        expect(await store.has(EXISTING_QUAD)).toBe(true);
        await store.addQuad(EXISTING_QUAD);
        expect(await store.size()).toBe(6);
    });

    test('addQuads appends a batch, skipping duplicates', async () => {
        const store = await nquadsStore();
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
        const quad = df.quad(ex('s'), ex('p'), df.literal('hello'));

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

describe('deleteQuad tombstones a base row', () => {
    const p1 = ex('p1');
    const s1 = ex('s1');

    for (const { name, options } of VARIANTS) {
        test(name, async () => {
            const store = await nquadsStore(options);
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
