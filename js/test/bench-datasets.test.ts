import { describe, test, expect } from 'vitest';
import { datasetProbes, flattenTerms, moduli, objectTerm, quadAt } from '../bench/datasets.js';

// Golden values of the benchmark dataset generator, computed once and pinned
// with identical literals in python/tests/test_bench_datasets.py and
// core/tests/bench_dataset.rs, so the three dashboard tabs keep measuring the
// same data.

const BASE = 'http://data.example.org';

describe('bench dataset parity', () => {
    test('moduli at the dashboard scales, 8 graphs', () => {
        expect(moduli(32768, { graphs: 8 })).toEqual({
            nSubj: 3277, nPred: 32, nObj: 16387, nGraph: 9, terms: 19705,
        });
        expect(moduli(1048576, { graphs: 8 })).toEqual({
            nSubj: 104858, nPred: 33, nObj: 524291, nGraph: 17, terms: 629199,
        });
    });

    test('N-Quads spellings of quads 0, 1, 7 and 12345 at 32,768 rows', () => {
        const m = moduli(32768, { graphs: 8 });
        const spell = (i: number) => {
            const q = quadAt(i, m, 0.4);
            const o = q.object.termType === 'Literal' ? `"${q.object.value}"` : `<${q.object.value}>`;
            return `<${q.subject.value}> <${q.predicate.value}> ${o} <${q.graph.value}> .`;
        };
        expect(spell(0)).toBe(
            `<${BASE}/resource/2026/subject/000000000> <${BASE}/ontology/2026/property/0000> "descriptive object value number 000000000" <${BASE}/graph/2026/named/000000> .`,
        );
        expect(spell(1)).toBe(
            `<${BASE}/resource/2026/subject/000000001> <${BASE}/ontology/2026/property/0001> "descriptive object value number 000000001" <${BASE}/graph/2026/named/000001> .`,
        );
        expect(spell(7)).toBe(
            `<${BASE}/resource/2026/subject/000000007> <${BASE}/ontology/2026/property/0007> <${BASE}/resource/2026/object/000000007> <${BASE}/graph/2026/named/000007> .`,
        );
        expect(spell(12345)).toBe(
            `<${BASE}/resource/2026/subject/000002514> <${BASE}/ontology/2026/property/0025> <${BASE}/resource/2026/object/000012345> <${BASE}/graph/2026/named/000006> .`,
        );
    });

    test('object 0 is a literal, so the O probe binds one', () => {
        const o0 = objectTerm(0, 0.4);
        expect(o0.termType).toBe('Literal');
        expect(o0.value).toBe('descriptive object value number 000000000');
    });

    test('quad probes bind graph 0; a single-graph dataset has no quad probes', () => {
        // Same as the Python generator: no graph term to bind at graphs=1.
        const named = datasetProbes(32768, { graphs: 8 }).quads;
        expect(named.map((p) => p.name)).toEqual(['G', 'SPOG']);
        expect(named[0].g?.termType).toBe('NamedNode');
        expect(named[0].g?.value).toBe(`${BASE}/graph/2026/named/000000`);
        expect(datasetProbes(32768, { graphs: 1 }).quads).toEqual([]);
    });
});

describe('flattenTerms', () => {
    test("flattenTerms reads each term value and each literal's datatype value", () => {
        const reads: string[] = [];
        const term = (tag: string, extra: object = {}) => ({ termType: 'NamedNode', get value() { reads.push(tag); return tag; }, ...extra });
        const literal = {
            termType: 'Literal',
            get value() { reads.push('o'); return 'o'; },
            datatype: { get value() { reads.push('dt'); return 'dt'; } },
        };
        const q = { subject: term('s'), predicate: term('p'), object: literal, graph: term('g') };
        flattenTerms([q as never]);
        expect(reads).toEqual(['s', 'p', 'o', 'dt', 'g']);
    });
});
