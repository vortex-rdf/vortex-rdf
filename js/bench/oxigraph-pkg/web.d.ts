/* tslint:disable */
/* eslint-disable */

/**
 * RDF/JS DataFactory-compatible methods
 */
export function literal(value: string | undefined, languageOrDataType?: string | NamedNode | {language: string, direction?: "ltr" | "rtl"}): Literal;
export function quad(subject: Quad_Subject, predicate: Quad_Predicate, object: Quad_Object, graph?: Quad_Graph): Quad;

/**
 * RDF/JS-compatible BlankNode term
 */
export class BlankNode {
    readonly termType: "BlankNode";
    readonly value: string;

    equals(other: Term | null | undefined): boolean;
    toString(): string;
}

/**
 * RDF/JS-compatible DefaultGraph term
 */
export class DefaultGraph {
    readonly termType: "DefaultGraph";
    readonly value: "";

    equals(other: Term | null | undefined): boolean;
    toString(): string;
}

/**
 * RDF/JS-compatible Literal term
 */
export class Literal {
    readonly datatype: NamedNode;
    readonly language: string;
    readonly direction: "ltr" | "rtl" | "";
    readonly termType: "Literal";
    readonly value: string;

    equals(other: Term | null | undefined): boolean;
    toString(): string;
}

/**
 * RDF/JS-compatible BlankNode term
 */
export class NamedNode {
    readonly termType: "NamedNode";
    readonly value: string;

    equals(other: Term | null | undefined): boolean;
    toString(): string;
}

/**
 * BaseQuad is part of the RDF/JS spec and has no direct equivalent in Oxigraph.
 *
 * It's copied here from the RDF/JS reference types (https://github.com/rdfjs/types , MIT license) because:
 *  - RDF/JS DatasetCore's match method takes Term parameters.
 *  - RDF/JS's Term is defined in terms of BaseQuad and not Quad.
 */
export interface BaseQuad {
    readonly graph: Term;
    readonly object: Term;
    readonly predicate: Term;
    readonly subject: Term;
    readonly termType: "Quad";
    readonly value: "";

    equals(other: Term | null | undefined): boolean;
    toString(): string;
}

/**
 * RDF/JS-compatible Quad term
 */
export class Quad implements BaseQuad {
    readonly graph: Quad_Graph;
    readonly object: Quad_Object;
    readonly predicate: Quad_Predicate;
    readonly subject: Quad_Subject;
    readonly termType: "Quad";
    readonly value: "";

    equals(other: Term | null | undefined): boolean;
    toString(): string;
}

/**
 * Typedefs copied from the RDF/JS reference types (https://github.com/rdfjs/types)
 */
export type Quad_Graph = DefaultGraph | NamedNode | BlankNode | Variable;
export type Quad_Object = NamedNode | Literal | BlankNode | Quad | Variable;
export type Quad_Predicate = NamedNode | Variable;
export type Quad_Subject = NamedNode | BlankNode | Quad | Variable;

/**
 * RDF/JS-compatible Term typedef. See note above re: BaseQuad.
 */
export type Term = NamedNode | BlankNode | Literal | Variable | DefaultGraph | BaseQuad;

/**
 * RDF/JS-compatible Variable term
 */
export class Variable {
    readonly termType: "Variable";
    readonly value: string;

    equals(other: Term | null | undefined): boolean;
    toString(): string;
}



export class Store {
    readonly size: number;

    constructor(quads?: Iterable<Quad>);

    add(quad: Quad): void;

    delete(quad: Quad): void;

    dump(
    options: {
        format: string;
        from_graph_name?: BlankNode | DefaultGraph | NamedNode;
    }
    ): string;

    has(quad: Quad): boolean;

    load(
    input: string | UInt8Array | Iterable<string | UInt8Array>,
    options: {
        base_iri?: NamedNode | string;
        format: string;
        no_transaction?: boolean;
        to_graph_name?: BlankNode | DefaultGraph | NamedNode;
        unchecked?: boolean;
        lenient?: boolean;
    }
    ): void;

    match(subject?: Term | null, predicate?: Term | null, object?: Term | null, graph?: Term | null): Quad[];

    query(
    query: string,
    options?: {
        base_iri?: NamedNode | string;
        results_format?: string;
        default_graph?: BlankNode | DefaultGraph | NamedNode | Iterable<BlankNode | DefaultGraph | NamedNode>;
        named_graphs?: Iterable<BlankNode | NamedNode>;
        use_default_graph_as_union?: boolean;
    }
    ): boolean | Map<string, Term>[] | Quad[] | string;

    update(
    update: string,
    options?: {
        base_iri?: NamedNode | string;
    }
    ): void;
}



function parse(
input: string | UInt8Array,
options: {
    base_iri?: NamedNode | string;
    format: string;
    to_graph_name?: BlankNode | DefaultGraph | NamedNode;
    lenient?: boolean;
}
): Quad[];
function parse(
input: Iterable<string | UInt8Array>,
options: {
    base_iri?: NamedNode | string;
    format: string;
    to_graph_name?: BlankNode | DefaultGraph | NamedNode;
    lenient?: boolean;
}
): IterableIterator<Quad>;
function parse(
input: AsyncIterable<string | UInt8Array>,
options: {
    base_iri?: NamedNode | string;
    format: string;
    to_graph_name?: BlankNode | DefaultGraph | NamedNode;
    lenient?: boolean;
}
): AsyncIterableIterator<Quad>;



export function blankNode(value?: string | null): BlankNode;

export function defaultGraph(): DefaultGraph;

export function fromQuad(original: any): any;

export function fromTerm(original: any): any;

export function main(): void;

export function memoryStats(): { liveBytes: number, peakLiveBytes: number, heapBytes: number };

export function namedNode(value: string): NamedNode;

export function resetMemoryPeak(): void;

export function setMemoryCounting(on: boolean): void;

export function triple(subject: any, predicate: any, object: any): Quad;

export function variable(value: string): Variable;

export type InitInput = RequestInfo | URL | Response | BufferSource | WebAssembly.Module;

export interface InitOutput {
    readonly memory: WebAssembly.Memory;
    readonly __wbg_asyncparseriterator_free: (a: number, b: number) => void;
    readonly __wbg_blanknode_free: (a: number, b: number) => void;
    readonly __wbg_defaultgraph_free: (a: number, b: number) => void;
    readonly __wbg_literal_free: (a: number, b: number) => void;
    readonly __wbg_namednode_free: (a: number, b: number) => void;
    readonly __wbg_parseriterator_free: (a: number, b: number) => void;
    readonly __wbg_parseriteratorresult_free: (a: number, b: number) => void;
    readonly __wbg_quad_free: (a: number, b: number) => void;
    readonly __wbg_store_free: (a: number, b: number) => void;
    readonly __wbg_variable_free: (a: number, b: number) => void;
    readonly asyncparseriterator_next: (a: number) => any;
    readonly blankNode: (a: number, b: number) => [number, number, number];
    readonly blanknode_equals: (a: number, b: any) => number;
    readonly blanknode_term_type: (a: number) => [number, number];
    readonly blanknode_toString: (a: number) => [number, number];
    readonly blanknode_value: (a: number) => [number, number];
    readonly defaultGraph: () => number;
    readonly defaultgraph_equals: (a: number, b: any) => number;
    readonly defaultgraph_term_type: (a: number) => [number, number];
    readonly defaultgraph_toString: (a: number) => [number, number];
    readonly defaultgraph_value: (a: number) => [number, number];
    readonly fromQuad: (a: any) => [number, number, number];
    readonly fromTerm: (a: any) => [number, number, number];
    readonly literal: (a: number, b: number, c: any) => [number, number, number];
    readonly literal_datatype: (a: number) => number;
    readonly literal_direction: (a: number) => [number, number];
    readonly literal_equals: (a: number, b: any) => number;
    readonly literal_language: (a: number) => [number, number];
    readonly literal_term_type: (a: number) => [number, number];
    readonly literal_toString: (a: number) => [number, number];
    readonly literal_value: (a: number) => [number, number];
    readonly main: () => void;
    readonly memoryStats: () => [number, number, number];
    readonly namedNode: (a: number, b: number) => [number, number, number];
    readonly namednode_equals: (a: number, b: any) => number;
    readonly namednode_term_type: (a: number) => [number, number];
    readonly namednode_toString: (a: number) => [number, number];
    readonly namednode_value: (a: number) => [number, number];
    readonly parse: (a: any, b: any) => [number, number, number];
    readonly parseriterator_next: (a: number) => [number, number, number];
    readonly parseriteratorresult_done: (a: number) => number;
    readonly parseriteratorresult_value: (a: number) => number;
    readonly quad: (a: any, b: any, c: any, d: any) => [number, number, number];
    readonly quad_equals: (a: number, b: any) => number;
    readonly quad_graph: (a: number) => any;
    readonly quad_object: (a: number) => any;
    readonly quad_predicate: (a: number) => any;
    readonly quad_subject: (a: number) => any;
    readonly quad_term_type: (a: number) => [number, number];
    readonly quad_toString: (a: number) => [number, number];
    readonly quad_value: (a: number) => [number, number];
    readonly resetMemoryPeak: () => void;
    readonly setMemoryCounting: (a: number) => void;
    readonly store_add: (a: number, b: any) => [number, number];
    readonly store_delete: (a: number, b: any) => [number, number];
    readonly store_dump: (a: number, b: any, c: any) => [number, number, number, number];
    readonly store_has: (a: number, b: any) => [number, number, number];
    readonly store_load: (a: number, b: any, c: any, d: any, e: any) => [number, number];
    readonly store_match: (a: number, b: any, c: any, d: any, e: any) => [number, number, number, number];
    readonly store_new: (a: any) => [number, number, number];
    readonly store_query: (a: number, b: number, c: number, d: any) => [number, number, number];
    readonly store_size: (a: number) => [number, number, number];
    readonly store_update: (a: number, b: number, c: number, d: any) => [number, number];
    readonly triple: (a: any, b: any, c: any) => [number, number, number];
    readonly variable: (a: number, b: number) => [number, number, number];
    readonly variable_equals: (a: number, b: any) => number;
    readonly variable_term_type: (a: number) => [number, number];
    readonly variable_toString: (a: number) => [number, number];
    readonly variable_value: (a: number) => [number, number];
    readonly wasm_bindgen_196987114d51cf05___convert__closures_____invoke___js_sys_c501a4263dda7b25___Function_fn_wasm_bindgen_196987114d51cf05___JsValue_____wasm_bindgen_196987114d51cf05___sys__Undefined___js_sys_c501a4263dda7b25___Function_fn_wasm_bindgen_196987114d51cf05___JsValue_____wasm_bindgen_196987114d51cf05___sys__Undefined_______true_: (a: number, b: number, c: any, d: any) => void;
    readonly wasm_bindgen_196987114d51cf05___convert__closures_____invoke___wasm_bindgen_196987114d51cf05___JsValue__core_7d5f0a2ba6a62c33___result__Result_____wasm_bindgen_196987114d51cf05___JsError___true_: (a: number, b: number, c: any) => [number, number];
    readonly wasm_bindgen_196987114d51cf05___convert__closures_____invoke___wasm_bindgen_196987114d51cf05___JsValue__true_: (a: number, b: number) => any;
    readonly __wbindgen_malloc: (a: number, b: number) => number;
    readonly __wbindgen_realloc: (a: number, b: number, c: number, d: number) => number;
    readonly __wbindgen_exn_store: (a: number) => void;
    readonly __externref_table_alloc: () => number;
    readonly __wbindgen_externrefs: WebAssembly.Table;
    readonly __wbindgen_free: (a: number, b: number, c: number) => void;
    readonly __wbindgen_destroy_closure: (a: number, b: number) => void;
    readonly __externref_drop_slice: (a: number, b: number) => void;
    readonly __externref_table_dealloc: (a: number) => void;
    readonly __wbindgen_start: () => void;
}

export type SyncInitInput = BufferSource | WebAssembly.Module;

/**
 * Instantiates the given `module`, which can either be bytes or
 * a precompiled `WebAssembly.Module`.
 *
 * @param {{ module: SyncInitInput }} module - Passing `SyncInitInput` directly is deprecated.
 *
 * @returns {InitOutput}
 */
export function initSync(module: { module: SyncInitInput } | SyncInitInput): InitOutput;

/**
 * If `module_or_path` is {RequestInfo} or {URL}, makes a request and
 * for everything else, calls `WebAssembly.instantiate` directly.
 *
 * @param {{ module_or_path: InitInput | Promise<InitInput> }} module_or_path - Passing `InitInput` directly is deprecated.
 *
 * @returns {Promise<InitOutput>}
 */
export default function __wbg_init (module_or_path?: { module_or_path: InitInput | Promise<InitInput> } | InitInput | Promise<InitInput>): Promise<InitOutput>;
