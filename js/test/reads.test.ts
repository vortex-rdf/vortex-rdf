// Reading quads: the match stream, lazy terms and their equality, getQuads
// without a pattern, and graph and blank-node terms.
import { describe, test, expect } from 'vitest';
import type { Quad, Literal } from '@rdfjs/types';
import { VortexRdfStore } from '@vortex-rdf/vortex-rdf-store';
import { collect, df, ex, nquadsStore } from './support.js';

describe('match returns an RDF/JS Stream<Quad>', () => {
    test('for-await and data/end events both yield the matches', async () => {
        const store = await nquadsStore();
        const pattern = [null, ex('p1'), null, null] as const;

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
        const store = await nquadsStore();
        const stream = store.match(null, ex('p1'), null, null);
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
        const store = await nquadsStore();
        const none = await collect(store.match(ex('nope'), null, null, null));
        expect(none.length).toBe(0);
    });
});

describe('lazy terms', () => {
    test('Dictionary: .equals compares terms across match results (integer fast path)', async () => {
        const store = await nquadsStore(); // Dictionary default
        const a = store.getQuads(ex('s1'), null, null, null);
        const b = store.getQuads(ex('s1'), null, null, null);

        // Same subject term, two independent match results of the same store.
        expect(a[0].subject.equals(b[0].subject)).toBe(true);
        // Distinct terms compare unequal (p1 vs p2; subject vs object).
        const aP1 = a.find((q: Quad) => q.predicate.value === 'http://example.org/p1')!;
        const aP2 = a.find((q: Quad) => q.predicate.value === 'http://example.org/p2')!;
        expect(aP1.predicate.equals(aP2.predicate)).toBe(false);
        expect(aP1.subject.equals(aP1.object)).toBe(false);

        // The object <o1> appears under s1/p1 and s2/p1 — equal across results.
        const s2 = store.getQuads(ex('s2'), ex('p1'), null, null);
        const s2o1 = s2.find((q: Quad) => q.object.value === 'http://example.org/o1')!;
        expect(aP1.object.equals(s2o1.object)).toBe(true);
    });

    test('literal value/datatype/language decode lazily and correctly', async () => {
        const store = await nquadsStore();

        const typed = (store.getQuads(null, ex('p3'), null, null))[0];
        expect(typed.object.termType).toBe('Literal');
        expect(typed.object.value).toBe('42');
        expect((typed.object as Literal).datatype.value).toBe('http://www.w3.org/2001/XMLSchema#integer');

        const lang = (store.getQuads(null, ex('p4'), null, null))[0];
        expect(lang.object.value).toBe('hola');
        expect((lang.object as Literal).language).toBe('es');
        expect((lang.object as Literal).datatype.value).toBe('http://www.w3.org/1999/02/22-rdf-syntax-ns#langString');
    });

    test('interoperates with foreign RDF/JS terms via .equals (both directions)', async () => {
        const store = await nquadsStore();
        const q = store.getQuads(ex('s1'), ex('p1'), null, null)[0];

        expect(q.predicate.equals(ex('p1'))).toBe(true);
        expect(q.predicate.equals(ex('nope'))).toBe(false);
        // Foreign term comparing against our lazy term reads our getters.
        expect(ex('p1').equals(q.predicate)).toBe(true);
        expect(df.literal('hola', 'es').equals(
            (store.getQuads(null, ex('p4'), null, null))[0].object,
        )).toBe(true);
    });

    test('Default layout: .equals falls back to value/termType compare', async () => {
        const store = await nquadsStore({ layout: 'default' });
        const a = store.getQuads(null, ex('p1'), null, null);
        // <o1> appears twice under p1 — equal by value even without codes.
        const o1s = a.filter((q: Quad) => q.object.value === 'http://example.org/o1');
        expect(o1s.length).toBe(2);
        expect(o1s[0].object.equals(o1s[1].object)).toBe(true);
        expect(a[0].predicate.equals(ex('p1'))).toBe(true);
    });
});

describe('getQuads without a pattern', () => {
    test('zero arguments returns every quad', async () => {
        const store = await nquadsStore();
        const r = store.getQuads();
        expect(Array.isArray(r)).toBe(true);
        expect(r).not.toBeInstanceOf(Promise);
        expect(r.length).toBe(6);
    });
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
            const s = ex('s');
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
            expect(store.countQuads(null, null, null, ex('g'))).toBe(1);
        });

        test(`${layout}: blank nodes decode and match`, async () => {
            const store = await VortexRdfStore.fromString(GRAPHS, 'nquads', { layout });
            const quads = store.getQuads(null, ex('p5'), null, null);
            expect(quads.length).toBe(1);
            expect(quads[0].subject.termType).toBe('BlankNode');
            expect(quads[0].object.termType).toBe('BlankNode');
            expect(quads[0].subject.value).toBe('b1');
            expect(quads[0].object.value).toBe('b2');
            expect(store.countQuads(df.blankNode('b1'), null, null, null)).toBe(1);
            expect(store.countQuads(null, null, df.blankNode('b2'), null)).toBe(1);

            // A blank-node quad added through the RDF/JS boundary round-trips.
            const added = df.quad(df.blankNode('b3'), ex('p5'), df.blankNode('b4'));
            await store.addQuad(added);
            expect(await store.has(added)).toBe(true);
            expect(store.countQuads(null, ex('p5'), null, null)).toBe(2);
        });
    }
});
