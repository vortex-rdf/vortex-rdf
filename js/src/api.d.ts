import { Quad, Term, Stream } from '@rdfjs/types';

/**
 * How quad terms are encoded into columns.
 * - 'default': all four terms as N-Triples strings natively optimised by Vortex.
 * - 'typed-object': the object is split into kind/value/datatype/language columns.
 * - 'dictionary': every term is replaced by a u32 code into a global sorted term
 *   dictionary. More compact than 'default'. Added quads live in an
 *   in-memory string tail until the store is serialized or compacted.
 *
 * These kebab-case names are the canonical vocabulary shared by every
 * vortex-rdf frontend; `layout()` reports them.
 */
export type LayoutStrategy = 'default' | 'typed-object' | 'dictionary';

/**
 * Secondary indexes embedded alongside the primary quad columns.
 * 'secondary-by-reference' adds sorted predicate/object columns plus row-id
 * back-references, letting predicate-only and object-only patterns use a
 * binary search instead of a full scan.
 * 'secondary-by-copy' embeds two complete extra copies of the quad columns —
 * one sorted by (p, o, s, g), one by (o, s, p, g) — so predicate- and
 * object-bound patterns (including predicate+object prefix lookups) get the
 * same sorted access path subjects have, at ~2x the storage.
 */
export type IndexType = 'secondary-by-reference' | 'secondary-by-copy';

/** RDF syntaxes accepted for parsing and emitted for serialization. */
export type RdfFormatName =
    | 'nt' | 'ntriples'
    | 'nq' | 'nquads'
    | 'ttl' | 'turtle'
    | 'trig'
    | 'n3'
    | 'rdf' | 'rdfxml' | 'xml'
    | 'jsonld';

/**
 * Build-time configuration. Any omitted field keeps its default.
 *
 * Quads are always sorted by subject -> predicate -> object -> graph as the
 * columnar array is built, which is what gives `match` its binary-search
 * lookups.
 */
export interface BuildOptions {
    /** @default 'dictionary' */
    layout?: LayoutStrategy;
    /** @default [] */
    indexes?: IndexType[];
}

export class VortexRdfStore {
    static empty(): VortexRdfStore;
    static fromBytes(bytes: Uint8Array): Promise<VortexRdfStore>;
    static fromString(input: string, format: RdfFormatName, options?: BuildOptions): Promise<VortexRdfStore>;
    /** `quads` may be an array, or an RDF/JS `Stream<Quad>` (a Node-style event emitter). */
    static fromQuads(quads: Quad[] | Stream<Quad>, options?: BuildOptions): Promise<VortexRdfStore>;

    /** The layout this store's columns are encoded with (canonical kebab-case name). */
    layout(): LayoutStrategy;
    size(): Promise<number>;
    has(quad: Quad): Promise<boolean>;
    /** Add one quad in place (a quad already present is ignored, per RDF/JS). */
    addQuad(quad: Quad): Promise<void>;
    /**
     * Add many quads in one call — one tail rebuild for the whole batch,
     * where a loop over addQuad pays one per quad.
     */
    addQuads(quads: Quad[]): Promise<void>;
    deleteQuad(quad: Quad): Promise<void>;
    /**
     * Stream the quads matching a pattern (the RDF/JS `Source.match` contract).
     * Pass `null`/`undefined` for a variable position. Returns **synchronously**
     * an RDF/JS `Stream<Quad>` (`.on('data'|'end'|'error', …)`, `.read()`) of
     * lazy `Quad`s: a term's string is decoded from the columnar data only when
     * its `.value`/`.termType` is read, and never eagerly. The stream also
     * implements `Symbol.asyncIterator`, so it can be consumed with `for await`
     * (cast to `AsyncIterable<Quad>` in typed code).
     */
    match(subject?: Term | null, predicate?: Term | null, object?: Term | null, graph?: Term | null): Stream<Quad>;
    /**
     * Materialize the quads matching a pattern into an array of lazy `Quad`s —
     * the array-returning counterpart of `match`. Returns synchronously: no
     * read path performs I/O, so there is nothing to await. The returned
     * `Quad`s still decode their term strings lazily on access.
     */
    getQuads(subject?: Term | null, predicate?: Term | null, object?: Term | null, graph?: Term | null): Quad[];
    /**
     * Low-level prototype: an alternative read path to `match`/`getQuads`,
     * which build their own columnar payload rather than going through this.
     * Resolves a pattern to the matched rows' raw u32 term codes — four
     * columnar `Uint32Array`s — without materializing any term strings;
     * resolve codes to terms with `termDict()`. Returns `null` unless the
     * store is Dictionary layout with no pending appends (appended quads are
     * encoded against a fresh dictionary, so their codes would not decode).
     */
    matchCodes(subject?: Term | null, predicate?: Term | null, object?: Term | null, graph?: Term | null): { s: Uint32Array; p: Uint32Array; o: Uint32Array; g: Uint32Array; length: number } | null;
    /**
     * Low-level. An immutable handle on this store's term dictionary — the one
     * door to code↔term translation. `undefined` unless the store's rows are
     * code-addressable (Dictionary layout, no pending appends, resident
     * dictionary). The handle keeps decoding correctly after the store is
     * mutated: it retains the dictionary its codes address.
     */
    termDict(): TermDict | undefined;
    /** Serialize to Vortex file bytes; read back with `VortexRdfStore.fromBytes` or write to disk as a `.vortex` file. */
    toBytes(): Promise<Uint8Array>;
    /** Serialize the quads to an RDF syntax. */
    toRdf(format: RdfFormatName): Promise<string>;
}

/**
 * An immutable snapshot of a Dictionary-layout store's term dictionary,
 * translating u32 term codes to N-Triples term strings (`<iri>`, `_:blank`,
 * `"lit"@lang`, `"lit"^^<dt>`, or `''` for the default graph) and back.
 * Obtained with `VortexRdfStore.termDict()`.
 */
export class TermDict {
    /** Decode a term code, or `undefined` when it is out of range. */
    decode(code: number): string | undefined;
    /** Encode an N-Triples term string to its code (inverse of `decode`), or `undefined` when the term is absent. */
    encode(term: string): number | undefined;
    /** Release the wasm-side handle (also invoked by `Symbol.dispose`). */
    free(): void;
}

export function rdf_to_vortex(input: string, format: RdfFormatName, options?: BuildOptions): Promise<Uint8Array>;
export function vortex_to_rdf(vortex_bytes: Uint8Array, format: RdfFormatName): Promise<string>;
