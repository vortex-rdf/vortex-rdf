// Fixtures and helpers shared by the binding tests: the RDF/JS factory, the
// N-Quads document most stores are built from, every build variant, and the
// readers of match streams and code chunks. Not a test file itself: vitest
// collects only *.test.ts.
import { expect } from 'vitest';
import { DataFactory } from 'rdf-data-factory';
import type { Quad, Stream } from '@rdfjs/types';
import { VortexRdfStore, type BuildOptions, type CodeChunk, type CodeColumns } from '@vortex-rdf/vortex-rdf-store';

export const df = new DataFactory();

/** The NamedNode `http://example.org/<local>`, the namespace of `NQUADS`. */
export const ex = (local: string) => df.namedNode(`http://example.org/${local}`);

export const NQUADS = [
    '<http://example.org/s1> <http://example.org/p1> <http://example.org/o1> .',
    '<http://example.org/s1> <http://example.org/p2> "lit" .',
    '<http://example.org/s2> <http://example.org/p1> <http://example.org/o1> .',
    '<http://example.org/s2> <http://example.org/p1> <http://example.org/o2> <http://example.org/g1> .',
    '<http://example.org/s3> <http://example.org/p3> "42"^^<http://www.w3.org/2001/XMLSchema#integer> .',
    '<http://example.org/s4> <http://example.org/p4> "hola"@es .',
].join('\n') + '\n';

/** A store built from `NQUADS`. */
export function nquadsStore(options?: BuildOptions): Promise<VortexRdfStore> {
    return VortexRdfStore.fromString(NQUADS, 'nquads', options);
}

/** A quad no term of `NQUADS` appears in: absent from any store built from it. */
export const NEW_QUAD = df.quad(ex('s9'), ex('p9'), df.literal('new'));

/** The first quad of `NQUADS`: present in any store built from it. */
export const EXISTING_QUAD = df.quad(ex('s1'), ex('p1'), ex('o1'));

/** Every build variant reachable from JS, in the canonical kebab-case
 *  vocabulary — the only spellings accepted, and what `layout()` reports. */
export const VARIANTS: { name: string; options: BuildOptions }[] = [
    { name: 'Default', options: { layout: 'default' } },
    { name: 'TypedObject', options: { layout: 'typed-object' } },
    { name: 'Dictionary', options: { layout: 'dictionary' } },
    {
        name: 'Default+index',
        options: { layout: 'default', indexes: ['secondary-by-reference'] },
    },
    {
        name: 'Dictionary+index',
        options: { layout: 'dictionary', indexes: ['secondary-by-reference'] },
    },
    {
        name: 'Default+copy-index',
        options: { layout: 'default', indexes: ['secondary-by-copy'] },
    },
    {
        name: 'Dictionary+copy-index',
        options: { layout: 'dictionary', indexes: ['secondary-by-copy'] },
    },
];

/** Drain the quads of a match() result (via its Symbol.asyncIterator) into an array. */
export async function collect(stream: Stream<Quad>): Promise<Quad[]> {
    const out: Quad[] = [];
    for await (const quad of stream as unknown as AsyncIterable<Quad>) out.push(quad);
    return out;
}

export type Position = 's' | 'p' | 'o' | 'g';

/** Every chunk of `chunks` pulled: the codes of each column a chunk holds,
 *  concatenated across the chunks, and each chunk's row count. */
export function drainChunks(chunks: Iterable<CodeChunk>): { cols: Partial<Record<Position, number[]>>; sizes: number[] } {
    const cols: Partial<Record<Position, number[]>> = {};
    const sizes: number[] = [];
    for (const chunk of chunks) {
        sizes.push(chunk.length);
        for (const position of ['s', 'p', 'o', 'g'] as const) {
            const column = chunk[position];
            if (column === undefined) continue;
            expect(column).toBeInstanceOf(Float64Array);
            expect(column.length).toBe(chunk.length);
            (cols[position] ??= []).push(...column);
        }
    }
    return { cols, sizes };
}

/** All four columns of a `matchCodes` result as arrays, as `drainChunks` collects them. */
export function columnArrays(cols: CodeColumns): Record<Position, number[]> {
    return { s: [...cols.s], p: [...cols.p], o: [...cols.o], g: [...cols.g] };
}
