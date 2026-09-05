import { describe, test, expect } from 'vitest';
import { Worker } from 'node:worker_threads';
import { DataFactory } from 'rdf-data-factory';
import { DataType, Type, tableFromIPC, type Table } from 'apache-arrow';
import init from '../pkg/web/vortex_rdf.js';
import type { Quad, Term } from '@rdfjs/types';
import { MatchView, VortexRdfStore, type BuildOptions } from '@vortex-rdf/vortex-rdf-store/arrow';
import { genDataset } from '../bench/datasets.js';

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
const s2 = df.namedNode('http://example.org/s2');
const nobody = df.namedNode('http://example.org/nobody');

/** Whether this runtime can keep views over wasm memory valid across growth
 *  (`WebAssembly.Memory.prototype.toResizableBuffer`: Node behind
 *  `--experimental-wasm-rab-integration`, which `npm run test:zero-copy`
 *  passes; the plain run exercises the parse-time copy). */
const ZERO_COPY = typeof (WebAssembly.Memory.prototype as { toResizableBuffer?: unknown }).toResizableBuffer === 'function';

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

/** The table's rows, each a tuple of its cells in column order. */
function rows(table: Table): unknown[][] {
    const columns = table.schema.fields.map((f) => cells(table, f.name));
    return Array.from({ length: table.numRows }, (_, i) => columns.map((c) => c[i]));
}

type AnyTerm = { termType: string; value: string; language?: string; datatype?: { value: string } };

/** A term's N-Triples spelling — the dictionary's vocabulary. */
function nt(term: AnyTerm): string {
    switch (term.termType) {
        case 'NamedNode': return `<${term.value}>`;
        case 'BlankNode': return `_:${term.value}`;
        case 'DefaultGraph': return '';
        case 'Literal':
            if (term.language) return `"${term.value}"@${term.language}`;
            if (term.datatype && term.datatype.value !== 'http://www.w3.org/2001/XMLSchema#string') {
                return `"${term.value}"^^<${term.datatype.value}>`;
            }
            return `"${term.value}"`;
        default: throw new Error(`unexpected term type ${term.termType}`);
    }
}

/** The quads' terms per column, spelled as N-Triples. */
function spellings(quads: { subject: AnyTerm; predicate: AnyTerm; object: AnyTerm; graph: AnyTerm }[]) {
    return {
        s: quads.map((q) => nt(q.subject)),
        p: quads.map((q) => nt(q.predicate)),
        o: quads.map((q) => nt(q.object)),
        g: quads.map((q) => nt(q.graph)),
    };
}

/** The quads' term codes per column, through the dictionary. */
function codesOf(store: VortexRdfStore, quads: Parameters<typeof spellings>[0]) {
    const dict = store.termDict()!;
    const terms = spellings(quads);
    const encode = (column: string[]) => column.map((term) => dict.encode(term)!);
    return { s: encode(terms.s), p: encode(terms.p), o: encode(terms.o), g: encode(terms.g) };
}

/** The code rows of a match, in match order. */
function codeRows(store: VortexRdfStore, ...args: Parameters<VortexRdfStore['matchArrow']>): number[][] {
    using view = store.matchArrow(...args);
    return rows(view.table) as number[][];
}

describe('matchArrow', () => {
    test('codes are the quads\' dictionary codes, under the quad schema', async () => {
        const store = await build({ layout: 'dictionary' });
        const codes = codesOf(store, store.getQuads(null, p1, null, null));
        using view = store.matchArrow(null, p1, null, null);
        expect(view).toBeInstanceOf(MatchView);
        const table = view.table;

        expect(table.schema.fields.map((f) => f.name)).toEqual([...COLUMNS]);
        expect(table.numRows).toBe(3);
        expect(table.schema.metadata.get('vortex_rdf.term_encoding')).toBe('codes');
        expect(table.schema.metadata.get('vortex_rdf.layout')).toBe('dictionary');
        expect(table.schema.metadata.get('vortex_rdf.default_graph')).toBe('');
        expect(table.schema.metadata.get('vortex_rdf.version')).toMatch(/^\d+\.\d+\.\d+/);
        expect(table.schema.metadata.get('vortex_rdf.sort_order')).toBe('s,p,o,g');
        for (const name of COLUMNS) {
            const field = table.schema.fields.find((f) => f.name === name)!;
            expect(field.nullable, name).toBe(false);
            const type = field.type;
            expect(DataType.isInt(type) && type.bitWidth === 32 && !type.isSigned, name).toBe(true);
            expect(cells(table, name)).toEqual(Array.from(codes[name]));
        }
    });

    test('terms are strings carried as dictionary keys over the term dictionary', async () => {
        const store = await build({ layout: 'dictionary' });
        const dict = store.termDict()!;
        const codes = codesOf(store, store.getQuads(null, null, null, null));
        using view = store.matchArrow(null, null, null, null, { encoding: 'terms' });
        const table = view.table;

        expect(table.schema.metadata.get('vortex_rdf.term_encoding')).toBe('terms');
        for (const name of COLUMNS) {
            const type = table.getChild(name)!.type;
            expect(DataType.isDictionary(type), name).toBe(true);
            expect(type.dictionary.typeId, name).toBe(Type.Utf8View);
            expect(cells(table, name)).toEqual(Array.from(codes[name], (c) => dict.decode(c)));
        }
    });

    test('strings come out on every string-capable layout', async () => {
        const reference = await build({ layout: 'dictionary' });
        const dict = reference.termDict()!;
        const expected = Object.fromEntries(
            COLUMNS.map((name) => [name, Array.from(codesOf(reference, reference.getQuads(null, null, null, null))[name], (c) => dict.decode(c))]),
        );
        for (const layout of ['default', 'dictionary'] as const) {
            const store = await build({ layout });
            using view = store.matchArrow(null, null, null, null, { encoding: 'strings' });
            expect(view.table.schema.metadata.get('vortex_rdf.layout')).toBe(layout);
            for (const name of COLUMNS) {
                expect(view.table.getChild(name)!.type.typeId, `${layout}/${name}`).toBe(Type.Utf8View);
                expect(cells(view.table, name), `${layout}/${name}`).toEqual(expected[name]);
            }
        }
    });

    test('the default graph is the empty string, named graphs their IRI', async () => {
        const store = await build({ layout: 'dictionary' });
        using view = store.matchArrow(null, null, null, null, { encoding: 'strings', projection: ['g'] });
        expect(view.table.schema.fields.map((f) => f.name)).toEqual(['g']);
        expect([...cells(view.table, 'g')].sort()).toEqual(['', '', '', '', '', '<http://example.org/g1>']);
    });

    test('a projection picks columns in order', async () => {
        const store = await build({ layout: 'dictionary' });
        const codes = codesOf(store, store.getQuads(null, p1, null, null));
        using view = store.matchArrow(null, p1, null, null, { projection: ['o', 's'] });
        expect(view.table.schema.fields.map((f) => f.name)).toEqual(['o', 's']);
        expect(cells(view.table, 'o')).toEqual(Array.from(codes.o));
        expect(cells(view.table, 's')).toEqual(Array.from(codes.s));
    });

    test('rejects bad options, malformed patterns and unsupported layouts', async () => {
        const store = await build({ layout: 'dictionary' });
        expect(() => store.matchArrow(null, null, null, null, { encoding: 'Codes' as never })).toThrow(/term encoding/);
        expect(() => store.matchArrow(null, null, null, null, { projection: ['subject' as never] })).toThrow(/quad column/);
        expect(() => store.matchArrow(null, null, null, null, { projection: [] })).toThrow(/projection/);
        expect(() => store.matchArrow(null, null, null, null, { projection: ['s', 's'] })).toThrow(/repeated/);
        expect(() => store.matchArrow(df.literal('x'), null, null, null)).toThrow(/subject/);
        expect(() => store.matchArrowFFI(null, null, null, df.literal('g'))).toThrow(/Invalid graph term/);

        const plain = await build({ layout: 'default' });
        expect(() => plain.matchArrow(null, null, null, null)).toThrow(/dictionary layout/);
        using strings = plain.matchArrow(null, null, null, null, { encoding: 'strings' });
        expect(strings.table.numRows).toBe(6);

        const typed = await build({ layout: 'typed-object' });
        for (const encoding of ['codes', 'terms', 'strings'] as const) {
            expect(() => typed.matchArrow(null, null, null, null, { encoding })).toThrow(/typed-object/);
        }
    });

    test('an empty match is a schema with no rows', async () => {
        const store = await build({ layout: 'dictionary' });
        using view = store.matchArrow(nobody, null, null, null);
        expect(view.table.schema.fields.map((f) => f.name)).toEqual([...COLUMNS]);
        expect(view.table.numRows).toBe(0);
        using terms = store.matchArrow(nobody, null, null, null, { encoding: 'terms' });
        expect(terms.table.numRows).toBe(0);
        expect(DataType.isDictionary(terms.table.getChild('s')!.type)).toBe(true);
    });
});

describe('code gates', () => {
    test('Dictionary: codes agree with getQuads and decode through termDict', async () => {
        const store = await build({ layout: 'dictionary' });
        const dict = store.termDict()!;
        using view = store.matchArrow(null, p1, null, null);
        expect(view.table.numRows).toBe(3);
        const quads = store.getQuads(null, p1, null, null);
        const iri = (t: { value: string }) => `<${t.value}>`;
        expect((cells(view.table, 's') as number[]).map((c) => dict.decode(c)).sort())
            .toEqual(quads.map((q) => iri(q.subject)).sort());
        expect((cells(view.table, 'o') as number[]).map((c) => dict.decode(c)).sort())
            .toEqual(quads.map((q) => iri(q.object)).sort());

        using full = store.matchArrow(s2, p1, df.namedNode('http://example.org/o2'), df.namedNode('http://example.org/g1'));
        expect(full.table.numRows).toBe(1);
        expect(dict.decode(cells(full.table, 'g')[0] as number)).toBe('<http://example.org/g1>');
    });

    for (const layout of ['default', 'typed-object'] as const) {
        test(`${layout}: codes throw and termDict is undefined`, async () => {
            const store = await build({ layout });
            expect(() => store.matchArrow(null, p1, null, null)).toThrow();
            expect(store.termDict()).toBeUndefined();
        });
    }

    test('a pending append closes the code path', async () => {
        const store = await build({ layout: 'dictionary' });
        expect(codeRows(store, null, p1, null, null).length).toBe(3);
        await store.addQuad(df.quad(
            df.namedNode('http://example.org/s9'),
            df.namedNode('http://example.org/p9'),
            df.literal('new'),
        ));
        expect(() => store.matchArrow(null, p1, null, null)).toThrow(/compact/);
        expect(store.termDict()).toBeUndefined();
        // The term paths still answer.
        expect(store.getQuads(null, p1, null, null).length).toBe(3);
        using strings = store.matchArrow(null, p1, null, null, { encoding: 'strings' });
        expect(strings.table.numRows).toBe(3);
    });
});

describe('MatchView lifetime', () => {
    test('zeroCopy reflects the runtime, and a zero-copy table reads wasm memory in place', async () => {
        // vitest.zero-copy.config.ts sets the variable beside the flag.
        if (process.env.VORTEX_RDF_EXPECT_ZERO_COPY) expect(ZERO_COPY).toBe(true);
        const { memory } = await init();
        const store = await build({ layout: 'dictionary' });
        using view = store.matchArrow(null, null, null, null);
        expect(view.zeroCopy).toBe(ZERO_COPY);
        const buffer = view.table.getChild('s')!.data[0].values.buffer;
        if (ZERO_COPY) {
            expect(memory.buffer.resizable).toBe(true);
            expect(buffer).toBe(memory.buffer);
        } else {
            expect(buffer).not.toBe(memory.buffer);
        }
    });

    test('views survive wasm memory growth', async () => {
        const { memory } = await init();
        const store = await VortexRdfStore.fromQuads(genDataset(20_000), { layout: 'dictionary' });
        using view = store.matchArrow(null, null, null, null);
        const column = view.table.getChild('s')!;
        const snapshot = Array.from(column.toArray() as Uint32Array);
        expect(snapshot.length).toBe(20_000);
        const bufferBefore = memory.buffer;
        const pagesBefore = memory.buffer.byteLength / 65536;

        // Growth from JS and from inside wasm: an explicit grow, then a build
        // large enough to allocate past the current size.
        memory.grow(16);
        const other = await VortexRdfStore.fromQuads(genDataset(60_000), { layout: 'dictionary' });
        expect(memory.buffer.byteLength / 65536).toBeGreaterThan(pagesBefore);

        if (ZERO_COPY) {
            expect(memory.buffer).toBe(bufferBefore);
            expect(bufferBefore.detached).toBe(false);
        } else {
            expect(bufferBefore.detached).toBe(true);
        }
        // The table and the typed array taken before the growth still read.
        expect(Array.from(column.toArray() as Uint32Array)).toEqual(snapshot);
        expect(Array.from(view.table.getChild('s')!.toArray() as Uint32Array)).toEqual(snapshot);
        other.free();
    });

    test('toTable() is a JS-owned copy equal to the view', async () => {
        const { memory } = await init();
        const store = await build({ layout: 'dictionary' });
        const dict = store.termDict()!;
        const view = store.matchArrow(null, null, null, null, { encoding: 'terms' });
        const copy = view.toTable();
        expect(rows(copy)).toEqual(rows(view.table));
        for (const name of COLUMNS) {
            expect(copy.getChild(name)!.data[0].values.buffer).not.toBe(memory.buffer);
        }
        if (!ZERO_COPY) expect(copy).toBe(view.table);
        view.free();
        // The copy outlives the view.
        expect(cells(copy, 'p')).toEqual(store.getQuads(null, null, null, null).map((q) => nt(q.predicate)));
        expect(dict.encode(cells(copy, 's')[0] as string)).toBeDefined();
    });

    test('toIPC() round-trips through tableFromIPC', async () => {
        const store = await build({ layout: 'dictionary' });
        for (const encoding of ['codes', 'terms', 'strings'] as const) {
            using view = store.matchArrow(null, p1, null, null, { encoding });
            const bytes = view.toIPC();
            expect(bytes).toBeInstanceOf(Uint8Array);
            const table = tableFromIPC(bytes);
            expect(table.schema.metadata.get('vortex_rdf.term_encoding')).toBe(encoding);
            expect(rows(table)).toEqual(rows(view.table));
        }
    });

    test('free() is Symbol.dispose, idempotent, and a freed view throws', async () => {
        const store = await build({ layout: 'dictionary' });
        expect(MatchView.prototype[Symbol.dispose]).toBe(MatchView.prototype.free);
        const view = store.matchArrow(null, null, null, null);
        expect(view.table.numRows).toBe(6);
        view.free();
        view.free();
        expect(() => view.table).toThrow(/after free/);
        expect(() => view.toTable()).toThrow(/after free/);
        expect(() => view.toIPC()).toThrow(/after free/);
        expect(view.zeroCopy).toBe(ZERO_COPY);
    });

    test('a copied column transfers to a worker; a wasm-memory view does not', async () => {
        const store = await build({ layout: 'dictionary' });
        using view = store.matchArrow(null, null, null, null);
        const worker = new Worker(
            "const { parentPort } = require('node:worker_threads');"
            + 'parentPort.once("message", (codes) => parentPort.postMessage(codes.reduce((a, b) => a + b, 0)));',
            { eval: true },
        );
        try {
            const codes = view.toTable().getChild('s')!.toArray() as Uint32Array;
            const expected = codes.reduce((a, b) => a + b, 0);
            const sum = new Promise<number>((resolve, reject) => {
                worker.once('message', resolve);
                worker.once('error', reject);
            });
            worker.postMessage(codes, [codes.buffer as ArrayBuffer]);
            expect(await sum).toBe(expected);
            expect(codes.byteLength).toBe(0);
            if (ZERO_COPY) {
                const raw = view.table.getChild('s')!.toArray() as Uint32Array;
                expect(() => worker.postMessage(raw, [raw.buffer as ArrayBuffer])).toThrow();
            }
        } finally {
            await worker.terminate();
        }
    });
});

describe('pushdown options', () => {
    type Pattern = [Term | null, Term | null, Term | null, Term | null];
    const PATTERNS: Pattern[] = [
        [null, null, null, null],
        [null, p1, null, null],
        [s2, null, null, null],
        [null, null, df.literal('hola', 'es'), null],
        [nobody, null, null, null],
    ];

    test('limit and offset window the rows in match order', async () => {
        const store = await build({ layout: 'dictionary' });
        for (const pattern of PATTERNS) {
            const ordered = codeRows(store, ...pattern);
            const n = ordered.length;
            for (const [offset, limit] of [[0, 1], [0, 2], [1, 2], [2, 10], [n, 1], [0, 0]]) {
                expect(codeRows(store, ...pattern, { offset, limit }), `${offset}/${limit}`)
                    .toEqual(ordered.slice(offset, offset + limit));
            }
            expect(codeRows(store, ...pattern, { offset: 1 })).toEqual(ordered.slice(1));
            expect(codeRows(store, ...pattern, { limit: Number.MAX_SAFE_INTEGER })).toEqual(ordered);
        }
        // A window needs no codes: every string-capable layout serves one.
        const plain = await build({ layout: 'default' });
        using window = plain.matchArrow(null, null, null, null, { encoding: 'strings', limit: 2, offset: 1 });
        expect(window.table.numRows).toBe(2);
    });

    test('keep restricts by code set and code range', async () => {
        const store = await build({ layout: 'dictionary' });
        const dict = store.termDict()!;
        const everything = store.getQuads(null, null, null, null);
        const decoded = (rows: number[][]) => rows.map((row) => row.map((c) => dict.decode(c)));
        const expected = (keep: (q: Quad) => boolean) => spellingsRows(everything.filter(keep));

        const bob = dict.encode('<http://example.org/s2>')!;
        expect(decoded(codeRows(store, null, null, null, null, { keep: { s: [bob] } })))
            .toEqual(expected((q) => q.subject.value === 'http://example.org/s2'));
        expect(decoded(codeRows(store, null, null, null, null, { keep: { s: Uint32Array.of(bob) } })))
            .toEqual(expected((q) => q.subject.value === 'http://example.org/s2'));

        // The dictionary is sorted, so the terms sharing a prefix are one
        // code range: `<http://example.org/o…>` is o1..o2.
        const lo = dict.encode('<http://example.org/o1>')!;
        const hi = dict.encode('<http://example.org/o2>')! + 1;
        expect(decoded(codeRows(store, null, null, null, null, { keep: { o: { lo, hi } } })))
            .toEqual(expected((q) => q.object.value.startsWith('http://example.org/o')));

        expect(decoded(codeRows(store, null, p1, null, null, { keep: { s: { lo: dict.encode('<http://example.org/s1>')!, hi: bob }, o: [lo] } })))
            .toEqual([['<http://example.org/s1>', '<http://example.org/p1>', '<http://example.org/o1>', '']]);

        expect(codeRows(store, null, null, null, null, { keep: { o: [] } })).toEqual([]);
        expect(codeRows(store, null, null, null, null, { keep: { o: { lo, hi: lo } } })).toEqual([]);
        expect(codeRows(store, null, null, null, null, { keep: { o: { lo, hi } }, limit: 1 }).length).toBe(1);
        expect(codeRows(store, null, null, null, null, { keep: { o: { lo, hi }, g: undefined } }).length).toBe(3);

        // keep composes with every encoding and projection.
        using terms = store.matchArrow(null, null, null, null, { encoding: 'terms', projection: ['o'], keep: { s: [bob] } });
        expect(rows(terms.table)).toEqual(expected((q) => q.subject.value === 'http://example.org/s2').map((row) => [row[2]]));
    });

    test('keep, limit and offset reject bad arguments', async () => {
        const store = await build({ layout: 'dictionary' });
        const at = (options: object) => () => store.matchArrow(null, null, null, null, options as never);
        expect(at({ keep: { subject: [1] } })).toThrow(/quad column/);
        expect(at({ keep: [1] })).toThrow(/keyed by quad column/);
        expect(at({ keep: { s: 'x' } })).toThrow(/keep for column "s"/);
        expect(at({ keep: { s: ['x'] } })).toThrow(/u32 code space/);
        expect(at({ keep: { s: [-1] } })).toThrow(/u32 code space/);
        expect(at({ keep: { s: [1.5] } })).toThrow(/u32 code space/);
        expect(at({ keep: { s: { lo: 1 } } })).toThrow(/keep for column "s"/);
        expect(at({ keep: { s: new Int32Array([1]) } })).toThrow(/keep for column "s"/);
        expect(at({ limit: -1 })).toThrow(/'limit' must be a non-negative integer/);
        expect(at({ limit: 1.5 })).toThrow(/'limit' must be a non-negative integer/);
        expect(at({ offset: 'x' })).toThrow(/'offset' must be a non-negative integer/);

        // keep constrains term codes, so a layout without them throws.
        const plain = await build({ layout: 'default' });
        expect(() => plain.matchArrow(null, null, null, null, { encoding: 'strings', keep: { s: [1] } })).toThrow(/has none/);
    });
});

describe('TermDict.decodeMany', () => {
    test('decodes a batch as decode would, undefined out of range', async () => {
        const store = await build({ layout: 'dictionary' });
        const dict = store.termDict()!;
        using view = store.matchArrow(null, null, null, null);
        const codes = view.table.getChild('o')!.toArray() as Uint32Array;
        const expected = Array.from(codes, (c) => dict.decode(c));
        expect(dict.decodeMany(codes)).toEqual(expected);
        expect(dict.decodeMany(Array.from(codes))).toEqual(expected);
        expect(dict.decodeMany([codes[0], 2 ** 31, codes[0]])).toEqual([expected[0], undefined, expected[0]]);
        expect(dict.decodeMany([])).toEqual([]);
    });
});

/** The quads' N-Triples spellings as rows, in the quads' order. */
function spellingsRows(quads: Quad[]): string[][] {
    const terms = spellings(quads);
    return quads.map((_, i) => [terms.s[i], terms.p[i], terms.o[i], terms.g[i]]);
}
