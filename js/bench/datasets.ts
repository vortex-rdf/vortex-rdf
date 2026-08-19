// The pure dataset-generation layer, shared by every bench instrument in this
// directory: the cardinality-controlled generator the comparative suite and
// the dict-memory instrument drive, the query-pattern probes derived from it,
// and the dense-cube datasets the instrumented CodSpeed suite uses for its
// routing-pattern tasks.
//
// PURITY CONTRACT: this module may import only rdf-data-factory and types.
// No store library (oxigraph, rdf-stores, or the Vortex wasm module) may be
// reachable from here — codspeed.bench.ts imports this file and runs under
// CodSpeed's Valgrind instrumentation, where loading a foreign multi-MB wasm
// module would pollute every measurement (see util.ts, the other pure
// module). The impure adapter/measurement half lives in shared.ts, which
// re-exports this module so the multi-process workers keep a single import.

import { DataFactory } from 'rdf-data-factory';
import type { Quad, Term } from '@rdfjs/types';

export const df = new DataFactory();

// ─── Cardinality-controlled dataset ──────────────────────────────────────────
//
// Term cardinality is an explicit knob, independent of row count. The previous
// generator drew D³ rows from a namespace of only D IRIs (128 distinct terms for
// 2,097,152 triples), which made every store's term handling — dictionaries,
// interning, string storage — invisible to the benchmark, and is nothing like
// real RDF, where distinct terms scale with the data.
//
// Uniqueness: term indices are `i % k` per role, so quad i maps to the residue
// tuple (i mod nSubj, i mod nPred, i mod nObj, i mod nGraph). By the Chinese
// Remainder Theorem that map is injective over `i < lcm(...)`, so making the
// four moduli pairwise coprime (their lcm is then their product) and checking
// the product covers `n` guarantees every generated quad is distinct — with no
// dedupe set, which at these row counts would itself dominate memory.
//
// Deliberate consequence: index 0 exists for every role, so row 0 satisfies all
// four single-term probes and every pattern in `datasetProbes` matches at least
// one row.

const BASE = 'http://data.example.org';

export interface DatasetOpts {
    /** distinct subjects / n (default 0.1 — see `SUBJECT_RATIO_DEFAULT`). */
    subjectRatio?: number;
    /** distinct predicates: a small closed vocabulary, as in real data (default 32). */
    predicates?: number;
    /** distinct objects / n (default 0.5). */
    objectRatio?: number;
    /** fraction of distinct objects that are literals rather than IRIs (default 0.4). */
    literalFrac?: number;
    /** distinct named graphs; 1 means the default graph only (default 1). */
    graphs?: number;
}

/** Distinct subjects per row, i.e. the reciprocal of how many triples describe
 *  each resource. 0.1 is ten triples per subject.
 *
 *  This is the knob that decides how much of the dataset is *terms*: the
 *  dictionary holds `subjectRatio + objectRatio` terms per quad, so the two
 *  ratios alone say whether there are more distinct terms than rows.
 *
 *  It was 1.0 — a distinct subject for every row. That is not how RDF looks: a
 *  resource is normally described by several properties, so its subject recurs
 *  across that many triples. 1.0 also made `S` match exactly one row, and with
 *  `PO`/`SPO`/`SPOG` matching one apiece, five of the seven probes returned two
 *  rows or fewer — so the comparison measured query *setup* almost to the
 *  exclusion of decoding, on a dictionary half again the size of the dataset.
 *
 *  Set `BENCH_SUBJ_RATIO=1.0` to get that back deliberately: it is a reasonable
 *  dictionary-stress configuration, and `bench:dict-memory` asks for it
 *  explicitly for exactly that reason. It is a poor default. */
export const SUBJECT_RATIO_DEFAULT = 0.1;

/** Env-overridable defaults, so a sweep can vary cardinality without new code. */
export function datasetOpts(o: DatasetOpts = {}): Required<DatasetOpts> {
    return {
        subjectRatio:
            o.subjectRatio ?? Number(process.env.BENCH_SUBJ_RATIO ?? SUBJECT_RATIO_DEFAULT),
        predicates: o.predicates ?? Number(process.env.BENCH_PREDICATES ?? 32),
        objectRatio: o.objectRatio ?? Number(process.env.BENCH_OBJ_RATIO ?? 0.5),
        literalFrac: o.literalFrac ?? Number(process.env.BENCH_LITERAL_FRAC ?? 0.4),
        graphs: o.graphs ?? Number(process.env.BENCH_GRAPHS ?? 1),
    };
}

function gcd(a: number, b: number): number {
    while (b) [a, b] = [b, a % b];
    return a;
}

/** Per-role term counts: the requested values nudged up until pairwise coprime. */
export function moduli(n: number, opts: DatasetOpts = {}): {
    nSubj: number; nPred: number; nObj: number; nGraph: number; terms: number;
} {
    const o = datasetOpts(opts);
    const want = [
        Math.max(1, Math.round(n * o.subjectRatio)),
        Math.max(1, Math.round(o.predicates)),
        Math.max(1, Math.round(n * o.objectRatio)),
        Math.max(1, Math.round(o.graphs)),
    ];
    const got: number[] = [];
    for (const w of want) {
        let k = w;
        while (got.some((g) => gcd(g, k) !== 1)) k++;
        got.push(k);
    }
    const [nSubj, nPred, nObj, nGraph] = got;
    // Product == lcm because they are pairwise coprime; see the CRT note above.
    // Use logs: the product overflows the float mantissa long before it matters.
    const logProduct = got.reduce((acc, k) => acc + Math.log(k), 0);
    if (logProduct < Math.log(n)) {
        throw new Error(
            `dataset cardinality too low for ${n} distinct quads: ` +
            `${nSubj}x${nPred}x${nObj}x${nGraph} cannot cover it — raise a ratio`,
        );
    }
    // The default graph is a term too, and it is not one of the named graphs.
    const terms = nSubj + nPred + nObj + (nGraph === 1 ? 1 : nGraph);
    return { nSubj, nPred, nObj, nGraph, terms };
}

export const subjectTerm = (i: number) =>
    df.namedNode(`${BASE}/resource/2026/subject/${String(i).padStart(9, '0')}`);
export const predicateTerm = (i: number) =>
    df.namedNode(`${BASE}/ontology/2026/property/${String(i).padStart(4, '0')}`);
export const graphTerm = (i: number) =>
    df.namedNode(`${BASE}/graph/2026/named/${String(i).padStart(6, '0')}`);

/** Objects alternate IRI/literal deterministically, in `literalFrac` proportion. */
export function objectTerm(i: number, literalFrac: number): Term {
    return i % 10 < Math.round(literalFrac * 10)
        ? df.literal(`descriptive object value number ${String(i).padStart(9, '0')}`)
        : df.namedNode(`${BASE}/resource/2026/object/${String(i).padStart(9, '0')}`);
}

/** Quad `i` of the `n`-row dataset. Pure in `i`, so probes and prefixes can be
 *  derived without materializing the array. */
export function quadAt(i: number, m: ReturnType<typeof moduli>, literalFrac: number): Quad {
    return df.quad(
        subjectTerm(i % m.nSubj),
        predicateTerm(i % m.nPred),
        objectTerm(i % m.nObj, literalFrac) as never,
        m.nGraph === 1 ? df.defaultGraph() : graphTerm(i % m.nGraph),
    );
}

/** `n` distinct quads whose term cardinality follows `opts`. */
export function genDataset(n: number, opts: DatasetOpts = {}): Quad[] {
    const o = datasetOpts(opts);
    const m = moduli(n, opts);
    const out: Quad[] = new Array(n);
    for (let i = 0; i < n; i++) out[i] = quadAt(i, m, o.literalFrac);
    return out;
}

/** The first `take` quads `genDataset(n, opts)` would produce, without building
 *  the full array — the mutation worker needs only a MUT_BATCH-sized slice for
 *  the delete phase, and `n` can be a couple of million. */
export function genDatasetPrefix(n: number, take: number, opts: DatasetOpts = {}): Quad[] {
    const o = datasetOpts(opts);
    const m = moduli(n, opts);
    const k = Math.min(take, n);
    const out: Quad[] = new Array(k);
    for (let i = 0; i < k; i++) out[i] = quadAt(i, m, o.literalFrac);
    return out;
}

/** A batch of fresh quads in a disjoint namespace, for the add phase. */
export function genFresh(n: number): Quad[] {
    const out: Quad[] = new Array(n);
    for (let i = 0; i < n; i++) {
        out[i] = df.quad(
            df.namedNode(`${BASE}/fresh/2026/subject/${String(i).padStart(9, '0')}`),
            df.namedNode(`${BASE}/fresh/2026/property/0000`),
            df.namedNode(`${BASE}/fresh/2026/object/${String(i).padStart(9, '0')}`),
        );
    }
    return out;
}

// ─── Query patterns (probe terms fixed at index 0, so they always hit rows) ──
export type Pat = { name: string; s: Term | null; p: Term | null; o: Term | null; g: Term | null };
// 'full' (every variable unbound) is measured separately from the selective patterns,
// under FULL_SCAN_OPTS's much lower repetition count (see shared.ts): repeating a
// full-table materialization (all D³ rows, ~2M at the default D=128) ~15x (QUERY_OPTS's
// 5 warmup + 10 timed) reproducibly trips an internal `unreachable` trap in oxigraph's
// wasm build around the 5th repetition on the same store — confirmed in isolation with
// nothing else in the process, so it's not a cross-adapter memory issue. No other
// adapter under test showed any problem with that many repetitions, but a full-table
// dump repeated many times is a heavy op for any store (closer in spirit to `ingest`
// than to a selective query), so the lower budget applies to every adapter uniformly
// rather than singling out the one library that happens to crash on it.
export const FULL_SCAN_PATTERN: Pat = { name: 'full', s: null, p: null, o: null, g: null };

/**
 * Probes for an `n`-row dataset built with `opts`, all bound to term index 0 so
 * every one matches row 0 at minimum — no pattern can silently measure a
 * zero-row query.
 *
 * Selectivity now follows the data rather than the old shared namespace: at the
 * defaults `S` matches ~1 row (subjects are near-unique), `P` ~n/32, `O` ~2, and
 * the conjunctions narrow to a handful. That spread is what real queries look
 * like, but it is *not* comparable to the pre-cardinality numbers — the workers
 * record each pattern's matched-row count alongside its timing so the figures
 * stay interpretable.
 */
export function datasetProbes(n: number, opts: DatasetOpts = {}): {
    triples: Pat[]; quads: Pat[]; full: Pat;
} {
    const o = datasetOpts(opts);
    const m = moduli(n, opts);
    const s0 = subjectTerm(0);
    const p0 = predicateTerm(0);
    const o0 = objectTerm(0, o.literalFrac);
    const g0 = m.nGraph === 1 ? df.defaultGraph() : graphTerm(0);
    return {
        triples: [
            { name: 'S', s: s0, p: null, o: null, g: null },
            { name: 'P', s: null, p: p0, o: null, g: null },
            { name: 'O', s: null, p: null, o: o0, g: null },
            { name: 'PO', s: null, p: p0, o: o0, g: null },
            { name: 'SPO', s: s0, p: p0, o: o0, g: null },
        ],
        quads: [
            { name: 'G', s: null, p: null, o: null, g: g0 },
            { name: 'SPOG', s: s0, p: p0, o: o0, g: g0 },
        ],
        full: FULL_SCAN_PATTERN,
    };
}

// ─── Dense-cube datasets (the instrumented suite's routing-pattern data) ─────
//
// A d³/d⁴ cube over a tiny closed namespace: every routing decision is
// exercised while the dataset stays small enough for Valgrind. Deliberately
// NOT cardinality-realistic — 3·d distinct terms over d³ rows is exactly the
// pathology the generator above exists to avoid — so term-handling-sensitive
// tasks must use `genDataset` instead; the cube is for tasks where routing,
// not term volume, is the object of measurement.

export const EX = 'http://example.org/#';
export const nn = (n: number | string) => df.namedNode(EX + n);

/** d³ triples in the default graph: quad(ex#s, ex#p, ex#o). */
export function genTriples(d: number): Quad[] {
    const out: Quad[] = [];
    for (let s = 0; s < d; s++)
        for (let p = 0; p < d; p++)
            for (let o = 0; o < d; o++) out.push(df.quad(nn(s), nn(p), nn(o)));
    return out;
}

/** d⁴ quads across named graphs: quad(ex#s, ex#p, ex#o, ex#g). */
export function genQuads(d: number): Quad[] {
    const out: Quad[] = [];
    for (let s = 0; s < d; s++)
        for (let p = 0; p < d; p++)
            for (let o = 0; o < d; o++)
                for (let g = 0; g < d; g++) out.push(df.quad(nn(s), nn(p), nn(o), nn(g)));
    return out;
}

/** d³ triples whose objects are literals — plain, language-tagged, typed,
 * and escape-bearing (quotes, backslashes, newlines, and `"@`/`^^` lookalikes
 * inside the value). The other dense-cube datasets are named-node-only, so
 * the instrumented tasks over this one are the only dense-cube coverage of
 * literal escaping on ingest and the unescape path on decode/export. */
export function genLiteralTriples(d: number): Quad[] {
    const dt = df.namedNode('http://www.w3.org/2001/XMLSchema#integer');
    const out: Quad[] = [];
    let i = 0;
    for (let s = 0; s < d; s++)
        for (let p = 0; p < d; p++)
            for (let o = 0; o < d; o++) {
                const object =
                    i % 4 === 0 ? df.literal(`plain value ${o}`)
                    : i % 4 === 1 ? df.literal(`tagged value ${o}`, 'en')
                    : i % 4 === 2 ? df.literal(String(o), dt)
                    : df.literal(`say "hi"@home ^^ line\nbreak back\\slash ${o}`);
                out.push(df.quad(nn(s), nn(p), object));
                i++;
            }
    return out;
}

/** The first `n` triples genTriples(d) would emit — a delete phase only
 * needs a batch-sized slice of quads that actually exist in the store. */
export function genTriplesPrefix(d: number, n: number): Quad[] {
    const out: Quad[] = [];
    for (let s = 0; s < d && out.length < n; s++)
        for (let p = 0; p < d && out.length < n; p++)
            for (let o = 0; o < d && out.length < n; o++) out.push(df.quad(nn(s), nn(p), nn(o)));
    return out;
}
