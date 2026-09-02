import { describe, test, expect } from 'vitest';
import { DataFactory } from 'rdf-data-factory';
import { DataType, Type, tableFromIPC, type Table } from 'apache-arrow';
import { VortexRdfStore, type BuildOptions } from '@vortex-rdf/vortex-rdf-store';

const df = new DataFactory();

const NQUADS = [
    '<http://example.org/s1> <http://example.org/p1> <http://example.org/o1> .',
    '<http://example.org/s1> <http://example.org/p2> "lit" .',
    '<http://example.org/s2> <http://example.org/p1> <http://example.org/o1> .',
    '<http://example.org/s2> <http://example.org/p1> <http://example.org/o2> <http://example.org/g1> .',
    '<http://example.org/s3> <http://example.org/p3> "42"^^<http://www.w3.org/2001/XMLSchema#integer> .',
    '<http://example.org/s4> <http://example.org/p4> "hola"@es .',
].join('\n') + '\n';

const COLUMNS = ['s', 'p', 'o', 'g'] as const;
const p1 = df.namedNode('http://example.org/p1');

async function build(options: BuildOptions): Promise<VortexRdfStore> {
    return VortexRdfStore.fromString(NQUADS, 'nquads', options);
}

/** The cells of one column, as the vector yields them (numbers for codes,
 *  strings for dictionary and string-view columns). */
function cells(table: Table, name: string): unknown[] {
    const vector = table.getChild(name);
    expect(vector, name).not.toBeNull();
    return [...vector!];
}

describe('matchArrowIPC', () => {
    test('codes are the matchCodes columns, under the quad schema', async () => {
        const store = await build({ layout: 'dictionary' });
        const codes = store.matchCodes(null, p1, null, null)!;
        const table = tableFromIPC(store.matchArrowIPC(null, p1, null, null));

        expect(table.schema.fields.map((f) => f.name)).toEqual([...COLUMNS]);
        expect(table.numRows).toBe(3);
        expect(table.schema.metadata.get('vortex_rdf.term_encoding')).toBe('codes');
        expect(table.schema.metadata.get('vortex_rdf.layout')).toBe('dictionary');
        expect(table.schema.metadata.get('vortex_rdf.default_graph')).toBe('');
        for (const name of COLUMNS) {
            const type = table.getChild(name)!.type;
            expect(DataType.isInt(type) && type.bitWidth === 32 && !type.isSigned, name).toBe(true);
            expect(cells(table, name)).toEqual(Array.from(codes[name]));
        }
    });

    test('terms are strings carried as dictionary keys over the term dictionary', async () => {
        const store = await build({ layout: 'dictionary' });
        const dict = store.termDict()!;
        const codes = store.matchCodes(null, null, null, null)!;
        const table = tableFromIPC(store.matchArrowIPC(null, null, null, null, { encoding: 'terms' }));

        expect(table.schema.metadata.get('vortex_rdf.term_encoding')).toBe('terms');
        for (const name of COLUMNS) {
            const type = table.getChild(name)!.type;
            expect(DataType.isDictionary(type), name).toBe(true);
            expect(cells(table, name)).toEqual(Array.from(codes[name], (c) => dict.decode(c)));
        }
    });

    test('strings come out on every string-capable layout', async () => {
        const reference = await build({ layout: 'dictionary' });
        const dict = reference.termDict()!;
        const expected = Object.fromEntries(
            COLUMNS.map((name) => [name, Array.from(reference.matchCodes(null, null, null, null)![name], (c) => dict.decode(c))]),
        );
        for (const layout of ['default', 'dictionary'] as const) {
            const store = await build({ layout });
            const table = tableFromIPC(store.matchArrowIPC(null, null, null, null, { encoding: 'strings' }));
            expect(table.schema.metadata.get('vortex_rdf.layout')).toBe(layout);
            for (const name of COLUMNS) {
                expect(table.getChild(name)!.type.typeId, `${layout}/${name}`).toBe(Type.Utf8View);
                expect(cells(table, name), `${layout}/${name}`).toEqual(expected[name]);
            }
        }
    });

    test('the default graph is the empty string, named graphs their IRI', async () => {
        const store = await build({ layout: 'dictionary' });
        const table = tableFromIPC(store.matchArrowIPC(null, null, null, null, { encoding: 'strings', projection: ['g'] }));
        expect(table.schema.fields.map((f) => f.name)).toEqual(['g']);
        expect([...cells(table, 'g')].sort()).toEqual(['', '', '', '', '', '<http://example.org/g1>']);
    });

    test('a projection picks columns in order', async () => {
        const store = await build({ layout: 'dictionary' });
        const codes = store.matchCodes(null, p1, null, null)!;
        const table = tableFromIPC(store.matchArrowIPC(null, p1, null, null, { projection: ['o', 's'] }));
        expect(table.schema.fields.map((f) => f.name)).toEqual(['o', 's']);
        expect(cells(table, 'o')).toEqual(Array.from(codes.o));
        expect(cells(table, 's')).toEqual(Array.from(codes.s));
    });

    test('rejects bad options, malformed patterns and unsupported layouts', async () => {
        const store = await build({ layout: 'dictionary' });
        expect(() => store.matchArrowIPC(null, null, null, null, { encoding: 'Codes' as never })).toThrow(/term encoding/);
        expect(() => store.matchArrowIPC(null, null, null, null, { projection: ['subject' as never] })).toThrow(/quad column/);
        expect(() => store.matchArrowIPC(null, null, null, null, { projection: [] })).toThrow(/projection/);
        expect(() => store.matchArrowIPC(null, null, null, null, { projection: ['s', 's'] })).toThrow(/repeated/);
        expect(() => store.matchArrowIPC(df.literal('x'), null, null, null)).toThrow(/subject/);

        const plain = await build({ layout: 'default' });
        expect(() => plain.matchArrowIPC(null, null, null, null)).toThrow(/dictionary layout/);
        expect(tableFromIPC(plain.matchArrowIPC(null, null, null, null, { encoding: 'strings' })).numRows).toBe(6);

        const typed = await build({ layout: 'typed-object' });
        for (const encoding of ['codes', 'terms', 'strings'] as const) {
            expect(() => typed.matchArrowIPC(null, null, null, null, { encoding })).toThrow(/typed-object/);
        }
    });

    test('an empty match is a schema with no rows', async () => {
        const store = await build({ layout: 'dictionary' });
        const table = tableFromIPC(store.matchArrowIPC(df.namedNode('http://example.org/nope'), null, null, null));
        expect(table.schema.fields.map((f) => f.name)).toEqual([...COLUMNS]);
        expect(table.numRows).toBe(0);
    });
});
