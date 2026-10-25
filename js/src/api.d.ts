import { Quad, Term, Stream } from '@rdfjs/types';

/**
 * How quad terms are encoded into columns.
 * - 'default': all four terms stored as N-Triples strings.
 * - 'typed-object': the object is split into kind/value/datatype/language columns.
 * - 'dictionary': every term is replaced by a code (a u64, handed to JS as a
 *   number) into a global sorted term dictionary. Added quads live in an in-memory string tail until the store
 *   is serialized or compacted.
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
 * same sorted access path subjects have.
 */
export type IndexType = 'secondary-by-reference' | 'secondary-by-copy';

/**
 * RDF syntaxes accepted for parsing and emitted for serialization. Any other
 * name rejects with `Invalid input: unknown RDF format …`, which lists these.
 */
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
    /**
     * Column layout. Defaults to `'dictionary'`, the single default shared by
     * every vortex-rdf frontend (JS, Python and the CLI).
     * @default 'dictionary'
     */
    layout?: LayoutStrategy;
    /** @default [] */
    indexes?: IndexType[];
}

/**
 * The term codes a code read keeps, per position, applied inside the store
 * before any row is gathered: a code set (a `Float64Array`, or an array of
 * codes of any length but two; what a `matchCodes` column holds) or a code
 * range (a two-element array `[lo, hi]`, the half-open codes
 * `lo <= code < hi`). A two-element array is always a range; a two-code set
 * is a `Float64Array`. A code is an integer from 0 to
 * `Number.MAX_SAFE_INTEGER`. A keep needs code-addressable rows: on a store
 * without them, a count with a keep throws.
 */
export type CodeKeep = {
    s?: Float64Array | number[] | [number, number];
    p?: Float64Array | number[] | [number, number];
    o?: Float64Array | number[] | [number, number];
    g?: Float64Array | number[] | [number, number];
};

/**
 * How `matchCodes`, `countQuads` and `matchCodeChunks` narrow a pattern's
 * match: `keep` first, then a window of the kept rows. A window that cuts
 * rows takes them in base row order. A `limit` that cannot cut a row (no
 * `offset`, and at least the match's row count, or a bound on it, known
 * without reading) leaves the match as it is, which `matchCodes` reads in
 * the order it reads the match without a limit and `matchCodeChunks` in base
 * row order.
 * A value of the wrong type or an unknown key throws a `TypeError`, a number
 * outside its range a `RangeError`.
 */
export interface ReadOptions {
    /** The codes to keep per position. */
    keep?: CodeKeep;
    /** The most rows to read; a count stops once this many are known to exist. */
    limit?: number;
    /** The rows to skip first. @default 0 */
    offset?: number;
}

/** `ReadOptions` plus the shape of `matchCodeChunks`'s chunks. */
export interface ChunkOptions extends ReadOptions {
    /** The columns each chunk holds, by name; at least one. @default ['s', 'p', 'o', 'g'] */
    columns?: ('s' | 'p' | 'o' | 'g')[];
    /** The most rows a chunk holds; at least 1. @default 8192 */
    batchRows?: number;
    /** Ends the iteration at the next `next()` once aborted; aborted before the call, the call returns a finished iterable and resolves nothing. */
    signal?: AbortSignal;
}

/** A match's term codes: one `Float64Array` of exact integer codes per position, each `length` long. */
export interface CodeColumns {
    s: Float64Array;
    p: Float64Array;
    o: Float64Array;
    g: Float64Array;
    length: number;
}

/** One chunk of `matchCodeChunks`: a `Float64Array` copy of each requested column, each `length` long; the others are not set. */
export interface CodeChunk {
    s?: Float64Array;
    p?: Float64Array;
    o?: Float64Array;
    g?: Float64Array;
    length: number;
}

/**
 * One probe of `matchCodesMany` or `countQuadsMany`: a pattern as an array
 * `[s, p, o, g]`, or an object whose `s`, `p`, `o`, `g` are its pattern
 * terms (absent ones wildcards) and whose `keep`, `limit` and `offset`
 * narrow it as `ReadOptions` do. Any other key throws.
 */
export type Probe =
    | [Term | string | null, Term | string | null, Term | string | null, Term | string | null]
    | {
        s?: Term | string | null;
        p?: Term | string | null;
        o?: Term | string | null;
        g?: Term | string | null;
        keep?: CodeKeep;
        limit?: number;
        offset?: number;
    };

/**
 * A wasm-side store handle. Every method that fails rejects (or, for the
 * synchronous read family, throws) with an `Error`. An argument the Rust
 * core refuses (a format, layout or index name) rejects with
 * `Invalid input: …`.
 *
 * The read family — `match`, `getQuads`, `countQuads`, `countQuadsMany`,
 * `matchCodes`, `matchCodeChunks`, `matchCodesMany`, `size` and `has` — is
 * synchronous: no read path performs I/O, so there is nothing to await. The
 * builders, mutations and serializers return promises.
 *
 * A pattern position is a wildcard when it is `null`/`undefined` or an RDF/JS
 * `Variable` term; a bare string is read as a NamedNode IRI; any other value
 * that is not a valid term of that position's kind throws `Invalid <position>
 * term`.
 *
 * A quad given to `has`, `addQuad`, `addQuads`, `deleteQuad` or `fromQuads`
 * holds RDF/JS term objects only: a bare string is a NamedNode in a pattern,
 * never in a quad. All five validate a quad's terms by one rule, and a quad is
 * malformed (`Invalid quad object`) when a term is not of its position's kind
 * or its IRI, blank-node label, language tag or datatype IRI is invalid. A
 * literal is language-tagged when its `language` is non-empty, else typed when
 * its `datatype.value` is, else simple. A literal's `datatype`, when present,
 * is a NamedNode object: a non-object such as a bare IRI string is malformed,
 * in a quad and in a pattern alike. An absent graph is the default graph.
 * `addQuads` and `fromQuads` name a malformed quad by its index in the array
 * (`Invalid quad object at index 70000`), or by its position in a stream.
 */
export class VortexRdfStore {
    static empty(): VortexRdfStore;
    static fromBytes(bytes: Uint8Array): Promise<VortexRdfStore>;
    static fromString(input: string, format: RdfFormatName, options?: BuildOptions): Promise<VortexRdfStore>;
    /** `quads` may be an array, or an RDF/JS `Stream<Quad>` (a Node-style event emitter). */
    static fromQuads(quads: Quad[] | Stream<Quad>, options?: BuildOptions): Promise<VortexRdfStore>;

    /**
     * Release the wasm-side store. The handle must not be used afterwards;
     * also wired to `Symbol.dispose`, so `using store = await
     * VortexRdfStore.fromBytes(b)` disposes it automatically. Unfreed stores
     * are reclaimed by a FinalizationRegistry only when the JS wrapper is
     * collected.
     */
    free(): void;
    [Symbol.dispose](): void;
    /** The layout this store's columns are encoded with (canonical kebab-case name). */
    layout(): LayoutStrategy;
    /** The secondary indexes this store was built with (canonical kebab-case names). */
    indexes(): IndexType[];
    /** Number of quads in the store. */
    size(): number;
    /** Whether the quad is in the store. Throws on a malformed quad object (addQuad/deleteQuad reject it). */
    has(quad: Quad): boolean;
    /** Add one quad in place (a quad already present is ignored, per RDF/JS). */
    addQuad(quad: Quad): Promise<void>;
    /**
     * Add many quads in one call: the batch is built into one column set and
     * stored as one value. Use it for bulk inserts instead of repeated addQuad.
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
     * (cast to `AsyncIterable<Quad>` in typed code). An invalid pattern term
     * throws synchronously.
     */
    match(subject?: Term | string | null, predicate?: Term | string | null, object?: Term | string | null, graph?: Term | string | null): Stream<Quad>;
    /**
     * Materialize the quads matching a pattern into an array of lazy `Quad`s —
     * the array-returning counterpart of `match`. Returns synchronously: no
     * read path performs I/O, so there is nothing to await. The returned
     * `Quad`s still decode their term strings lazily on access.
     */
    getQuads(subject?: Term | string | null, predicate?: Term | string | null, object?: Term | string | null, graph?: Term | string | null): Quad[];
    /**
     * Number of quads matching a pattern — counted from the match's row
     * selection alone, no term string materialized. `options` narrow the
     * match (`ReadOptions`); with a `limit` the count stops once that many
     * rows are known to exist (`limit: 1` is an existence test).
     */
    countQuads(subject?: Term | string | null, predicate?: Term | string | null, object?: Term | string | null, graph?: Term | string | null, options?: ReadOptions): number;
    /**
     * `countQuads` for each probe, in input order, in one call. Every probe
     * is parsed before any is counted, so a malformed one throws first; the
     * first probe that fails throws. Counts on every layout.
     */
    countQuadsMany(probes: Probe[]): number[];
    /**
     * Low-level. Resolve a pattern to the matched rows' raw term codes —
     * `{ s, p, o, g }` as `Float64Array`s plus `length` — with no term strings
     * materialized. Codes are u64 and every one is an exact integer number;
     * a code past `Number.MAX_SAFE_INTEGER` (2^53 - 1) throws rather than
     * rounding. `null` unless the store's rows are code-addressable
     * (Dictionary layout, no pending appends, resident dictionary); decode
     * codes through `termDict()`. `options` narrow the match (`ReadOptions`).
     * An invalid pattern term or option throws whether or not the rows are
     * code-addressable.
     */
    matchCodes(subject?: Term | string | null, predicate?: Term | string | null, object?: Term | string | null, graph?: Term | string | null, options?: ReadOptions): CodeColumns | null;
    /**
     * Low-level. `matchCodes` in chunks: an iterable whose items hold at most
     * `options.batchRows` rows (8,192 by default) in base row order, each a
     * `Float64Array` copy of the columns `options.columns` names (all four
     * by default). `null` under the same conditions as `matchCodes`. The
     * call resolves the match, narrowed by `options` (`ReadOptions`); each
     * `next()` reads or gathers one chunk's rows, so stopping the pulls stops
     * the work. The iterable's `return()` (a `for…of` left early) and
     * `[Symbol.dispose]` (`using`) free its wasm handle, as does its end; an
     * aborted `options.signal` ends the iteration at the next `next()`, and
     * a signal already aborted makes the call return a finished iterable
     * without resolving the match. The iterable holds its store, which stays
     * alive until the iterable is done; once the store is freed with
     * `free()`, `next()` throws. `batchRows` below 1, an empty `columns` or
     * an unknown column name throws.
     */
    matchCodeChunks(subject?: Term | string | null, predicate?: Term | string | null, object?: Term | string | null, graph?: Term | string | null, options?: ChunkOptions): (Iterable<CodeChunk> & Disposable) | null;
    /**
     * Low-level. `matchCodes` for each probe: an iterable answering one probe
     * per `next()`, in input order — its `CodeColumns`, or `null` for every
     * probe when the store's rows are not code-addressable, in which case no
     * probe runs. Every probe is parsed before any runs, so a malformed one
     * throws from this call; the first probe that fails throws from its
     * `next()` and ends the iteration. The probes run one by one, on the
     * calling thread; the batch saves the boundary crossings and the
     * parsing. Disposal, `options.signal` (an already aborted one runs no
     * probe) and the store's lifetime work as for `matchCodeChunks`.
     */
    matchCodesMany(probes: Probe[], options?: { signal?: AbortSignal }): Iterable<CodeColumns | null> & Disposable;
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
 * translating term codes to N-Triples term strings (`<iri>`, `_:blank`,
 * `"lit"@lang`, `"lit"^^<dt>`, or `''` for the default graph) and back.
 * Codes are u64 and cross as numbers, exact up to `Number.MAX_SAFE_INTEGER`
 * (2^53 - 1): never rounded, never wrapped. Obtained with
 * `VortexRdfStore.termDict()`.
 */
export class TermDict {
    /**
     * Decode a term code, or `undefined` when it is out of range. Throws when
     * `code` is no code: not a number (nothing is coerced, so `null` or `'1'`
     * throws), or not an integer from 0 to `Number.MAX_SAFE_INTEGER`.
     */
    decode(code: number): string | undefined;
    /**
     * Encode a term to its code (inverse of `decode`), or `undefined` when the
     * term is absent. Tolerant of spelling: an IRI with or without angle
     * brackets, a literal with escape variants, an `xsd:string` typing, an
     * upper-case language tag, or `'default'` / `'[]'` for the default graph
     * all find the stored form's code. Throws for a malformed term, and for a
     * code past `Number.MAX_SAFE_INTEGER`, which no number holds exactly.
     */
    encode(term: string): number | undefined;
    /** Release the wasm-side handle (also invoked by `Symbol.dispose`). */
    free(): void;
    [Symbol.dispose](): void;
}

/**
 * The wasm handle under `matchCodeChunks`'s iterable, which pulls it; not
 * used directly. `next()` returns the next chunk, or `undefined` once the
 * match is exhausted, and throws once its store is freed.
 */
export class CodeChunks {
    next(): CodeChunk | undefined;
    free(): void;
    [Symbol.dispose](): void;
}

/**
 * The wasm handle under `matchCodesMany`'s iterable, which pulls it; not
 * used directly. `next()` returns the next probe's answer, or `undefined`
 * once every probe has answered, and throws once its store is freed.
 */
export class ProbeAnswers {
    next(): CodeColumns | null | undefined;
    free(): void;
    [Symbol.dispose](): void;
}

/**
 * One-shot serialization: RDF text in, native-container bytes out
 * (`VortexRdfStore.fromString(...).toBytes()` without holding a store).
 */
export function serializeRdf(input: string, format: RdfFormatName, options?: BuildOptions): Promise<Uint8Array>;
/**
 * One-shot deserialization: native-container bytes in, RDF text out
 * (`VortexRdfStore.fromBytes(...).toRdf(format)` without holding a store).
 */
export function deserializeRdf(bytes: Uint8Array, format: RdfFormatName): Promise<string>;
