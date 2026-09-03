import { Quad, Term, Stream } from '@rdfjs/types';

/**
 * How quad terms are encoded into columns.
 * - 'default': all four terms stored as N-Triples strings.
 * - 'typed-object': the object is split into kind/value/datatype/language columns.
 * - 'dictionary': every term is replaced by a u32 code into a global sorted term
 *   dictionary. Added quads live in an in-memory string tail until the store
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
 * How term cells are typed in an Arrow export (`matchArrowIPC`).
 * - 'codes': `uint32` term codes, decodable through `termDict()` — the
 *   cheapest to ship and to join on. Dictionary layout only.
 * - 'terms': the same codes as dictionary keys over the whole term dictionary
 *   (`dictionary<uint32, string_view>`), so readers see strings and carry
 *   codes. Dictionary layout only.
 * - 'strings': `string_view` N-Triples strings — the one encoding every
 *   layout serves.
 */
export type TermEncoding = 'codes' | 'terms' | 'strings';

/** One of the four quad columns, by its serialized name. */
export type QuadColumn = 's' | 'p' | 'o' | 'g';

/** Options of `matchArrowIPC`. Any omitted field keeps its default. */
export interface ArrowOptions {
    /** @default 'codes' */
    encoding?: TermEncoding;
    /**
     * The columns to ship, in this order; all four in `s, p, o, g` order
     * when omitted. Must be non-empty and name each column at most once.
     */
    projection?: QuadColumn[];
}

/**
 * The resident form of the term dictionary of a store opened from bytes
 * (a store built from quads always holds its dictionary plaintext).
 * - 'plaintext' (the default): the column decoded once into one canonical
 *   form that every probe, decode and `terms` export then reads in place.
 * - 'as-written': the file's own FSST chunks, a third of that size, decoded
 *   one term per read — the lean load for a store opened, queried a few
 *   times and dropped.
 */
export type DictForm = 'plaintext' | 'as-written';

/** Options of `fromBytes`. Any omitted field keeps its default. */
export interface OpenOptions {
    /** @default 'plaintext' */
    dictionary?: DictForm;
}

/**
 * A wasm-side store handle. Every method that fails rejects (or, for the
 * synchronous read family, throws) with an `Error`.
 *
 * Only the pattern-read family — `match`, `getQuads`, `countQuads`,
 * `matchArrowIPC` — is synchronous; `size` and `has` return promises like the
 * builders and mutations.
 *
 * A pattern position is a wildcard when it is `null`/`undefined` or an RDF/JS
 * `Variable` term; a bare string is read as a NamedNode IRI; any other value
 * that is not a valid term of that position's kind throws `Invalid <position>
 * term`.
 */
export class VortexRdfStore {
    static empty(): VortexRdfStore;
    static fromBytes(bytes: Uint8Array, options?: OpenOptions): Promise<VortexRdfStore>;
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
    size(): Promise<number>;
    /** Rejects on a malformed quad object, like addQuad/deleteQuad. */
    has(quad: Quad): Promise<boolean>;
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
     * selection alone, no term string materialized.
     */
    countQuads(subject?: Term | string | null, predicate?: Term | string | null, object?: Term | string | null, graph?: Term | string | null): number;
    /**
     * Low-level. The quads matching a pattern as an Arrow IPC stream — the
     * bytes `tableFromIPC` (apache-arrow), DuckDB-WASM, Arquero or Perspective
     * read. One record batch per decode chunk; columns `s`, `p`, `o`, `g`
     * (or `options.projection`, in that order), typed per
     * `options.encoding` (default `'codes'`; see `TermEncoding`). The schema
     * carries `vortex_rdf.layout`, `vortex_rdf.term_encoding`,
     * `vortex_rdf.version` and `vortex_rdf.default_graph` (`''`) metadata.
     * Returns synchronously; the bytes are a copy out of wasm memory. Throws
     * on an invalid pattern term or option, and on an encoding the store's
     * layout cannot serve (`'codes'`/`'terms'` need the Dictionary layout; the
     * TypedObject layout has no Arrow export).
     */
    matchArrowIPC(subject?: Term | string | null, predicate?: Term | string | null, object?: Term | string | null, graph?: Term | string | null, options?: ArrowOptions): Uint8Array;
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
