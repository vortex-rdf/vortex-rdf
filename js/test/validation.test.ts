// Term validation and malformed input at every quad entry point, and the
// rules for pattern terms.
import { describe, test, expect } from 'vitest';
import { Readable } from 'node:stream';
import type { Quad, Term } from '@rdfjs/types';
import { VortexRdfStore } from '@vortex-rdf/vortex-rdf-store';
import { NEW_QUAD, df, ex, nquadsStore } from './support.js';

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

    test('the packed path reports a malformed quad by its index in the whole array', async () => {
        // The array is packed and decoded 65,536 quads at a time; the last
        // quad sits in the second chunk. One malformed quad is caught by the
        // packer, the other's IRI by the Rust-side term constructor.
        const n = 70_000;
        const quads = Array.from({ length: n }, (_, i) => df.quad(ex(`s${i}`), ex('p'), df.literal('o')));
        const badIri = { subject: { termType: 'NamedNode', value: 'not an iri' }, predicate: p, object: o } as unknown as Quad;
        for (const malformed of [badObject, badIri]) {
            const input = [...quads.slice(0, n - 1), malformed];
            const atLast = new RegExp(`Invalid quad object at index ${n - 1}$`);
            await expect.soft(VortexRdfStore.fromQuads(input)).rejects.toThrow(atLast);
            await expect.soft(VortexRdfStore.empty().addQuads(input)).rejects.toThrow(atLast);
        }
    });

    test('the stream path reports the position of a malformed quad', async () => {
        const stream = Readable.from([good, badObject], { objectMode: true });
        await expect(VortexRdfStore.fromQuads(stream)).rejects
            .toThrow(/Invalid quad object in RDF\/JS stream at position 1/);
    });

    test('addQuads rejects a batch with a malformed quad and leaves the store unchanged', async () => {
        const store = await nquadsStore();
        await expect(store.addQuads([good, badObject])).rejects.toThrow(/Invalid quad object/);
        expect(await store.size()).toBe(6);
    });

    test('addQuad / deleteQuad reject and has throws a malformed quad object as an Error', async () => {
        const store = await nquadsStore();
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
        const store = await nquadsStore();
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
        expect(await store.has(df.quad(ex('s'), ex('p'), df.literal('o')))).toBe(true);
        expect(store.countQuads(null, null, null, df.defaultGraph())).toBe(1);
    });

    test('blank-node labels are validated on both ingest paths', async () => {
        const store = await nquadsStore();
        const bad = df.quad(df.blankNode('not a label'), ex('p'), df.literal('o'));
        await expect(store.addQuad(bad)).rejects.toThrow(/Invalid quad object/);
        await expect(VortexRdfStore.fromQuads([bad])).rejects.toThrow(/Invalid quad object at index 0/);
        expect(() => store.has(bad)).toThrow(/Invalid quad object/);
    });

    test('an invalid pattern term throws instead of matching everything', async () => {
        const store = await nquadsStore();
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
        const store = await nquadsStore();
        expect(store.countQuads(df.variable('s'), df.variable('p'), df.variable('o'), df.variable('g'))).toBe(6);
        expect(store.countQuads(df.variable('s'), ex('p1'), null, null)).toBe(3);
    });

    test('a bare IRI string is accepted as a NamedNode pattern term', async () => {
        const store = await nquadsStore();
        expect(store.countQuads('http://example.org/s1', null, null, null)).toBe(2);
        expect(store.getQuads(null, 'http://example.org/p1', null, null).length).toBe(3);
        expect(store.countQuads(null, null, null, 'http://example.org/g1')).toBe(1);
    });
});
