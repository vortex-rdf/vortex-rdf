// Shared infrastructure for every multi-process bench instrument here: the
// comparative orchestrator (compare.bench.ts), its per-adapter worker
// (compare.worker.ts), and the dictionary-memory worker (dict-memory.worker.ts,
// which takes the dataset generator, the Vortex adapters and the memory probes).
//
// PURITY CONTRACT: each of those runs in its own child process and imports this
// file independently, so it must be pure/deterministic given the same env vars —
// no shared in-memory state crosses the process boundary, and every knob is an
// env var read here rather than a value passed between processes.
//
// codspeed.bench.ts deliberately does NOT import this file: the store libraries
// loaded below would put a foreign multi-MB wasm module into its Valgrind-
// instrumented process. The pure halves live elsewhere and are re-exported
// here so the workers keep their single import: generic helpers in ./util.ts,
// the dataset-generation layer in ./datasets.ts (both importable by
// codspeed.bench.ts).

import type { BenchOptions, Bench } from 'tinybench';
import type { Quad } from '@rdfjs/types';
import { readFileSync } from 'node:fs';

import { VortexRdfStore, type BuildOptions } from '../entry/node.js';
import { fmtNs, freeWasm } from './util.js';
import { df, type Pat } from './datasets.js';
import {
    RdfStore,
    RdfStoreIndexNestedMapQuoted,
    TermDictionaryQuotedIndexed,
    TermDictionaryNumberRecordFullTerms,
} from 'rdf-stores';
import { Store as OxiStore } from 'oxigraph';

export * from './datasets.js';

// ─── Config (env-tunable) ────────────────────────────────────────────────────
export const D = Number(process.env.BENCH_DIM ?? 128); // triples dataset: D³ triples
export const DQ = Number(process.env.BENCH_DIM_QUADS ?? 32); // quads dataset: DQ⁴ quads
export const MUT_BATCH = Number(process.env.MUT_BATCH ?? 10_000); // add/delete batch, independent of D
/** Row counts, kept as D³/DQ⁴ so the existing scale knobs mean what they always did. */
export const N_TRIPLES = D ** 3;
export const N_QUADS = DQ ** 4;

// The dataset-generation layer (genDataset and friends, the probe patterns,
// and the dense-cube generators) lives in ./datasets.ts — pure, so the
// instrumented CodSpeed suite can import it too — and is re-exported above.

// ─── Adapter interface ───────────────────────────────────────────────────────
export interface StoreAdapter<H = unknown> {
    slug: string; // dashboard id segment
    label: string; // display name
    build(quads: Quad[]): Promise<H> | H; // ingest (idiomatic bulk API per lib)
    newEmpty(): Promise<H> | H; // empty store (add phase)
    addAll(h: H, quads: Quad[]): Promise<void> | void; // per-quad add loop
    addBatch?(h: H, quads: Quad[]): Promise<void> | void; // optional batch add (Vortex)
    deleteAll(h: H, quads: Quad[]): Promise<void> | void; // per-quad delete loop
    countMatch(h: H, p: Pat): Promise<number> | number; // consume + count
    // Both Vortex and oxigraph are wasm-bindgen: `.free()` deterministically drops
    // the WASM-side allocation. The generated FinalizationRegistry is a fallback
    // only — V8 has no visibility into WASM linear-memory pressure, so it can defer
    // running finalizers arbitrarily long, letting dozens of multi-million-quad
    // stores queue up unfreed. Every store this bench builds must be disposed as
    // soon as its measurement is done, not left to GC.
    dispose?(h: H): void;
}

// Disposal alone isn't enough for the pure-JS adapters (rdf-stores): a 2M-quad store
// there is a large, long-lived Map-of-Maps graph, and merely dropping the reference
// only makes it *eligible* for GC — V8's generational collector has no obligation to
// actually reclaim it before the next multi-hundred-MB allocation piles on top. Run
// with `--expose-gc` (wired into every worker spawn) and force a collection at every
// isolation point, for every adapter, wasm-backed or not.
// Disposal is best-effort: a store that trapped mid-query can leave its wasm
// value borrowed, so `free()` itself throws ("attempted to take ownership of a
// Rust value while it was borrowed"). That is cleanup of an already-failed
// measurement — letting it propagate would discard every row the adapter had
// successfully produced, which is exactly backwards. The collection below still
// runs, and the process exits shortly after regardless.
export function reclaim(a: StoreAdapter, h: unknown): void {
    try {
        a.dispose?.(h);
    } catch (e) {
        console.error(`  !! ${a.label}: dispose failed (${e instanceof Error ? e.message : e})`);
    }
    global.gc?.();
}

// Vortex: one adapter per curated build variant (mirrors the Rust star design axes).
export const VORTEX_VARIANTS: { slug: string; label: string; options: BuildOptions }[] = [
    { slug: 'vortex_dict', label: 'Vortex Dict', options: { layout: 'dictionary' } },
    { slug: 'vortex_dict_byref', label: 'Vortex Dict+ByRef', options: { layout: 'dictionary', indexes: ['secondary-by-reference'] } },
    { slug: 'vortex_dict_bycopy', label: 'Vortex Dict+ByCopy', options: { layout: 'dictionary', indexes: ['secondary-by-copy'] } },
    { slug: 'vortex_default', label: 'Vortex Default', options: { layout: 'default' } },
    { slug: 'vortex_default_byref', label: 'Vortex Default+ByRef', options: { layout: 'default', indexes: ['secondary-by-reference'] } },
    { slug: 'vortex_default_bycopy', label: 'Vortex Default+ByCopy', options: { layout: 'default', indexes: ['secondary-by-copy'] } },
];

export function vortexAdapter(variant: { slug: string; label: string; options: BuildOptions }): StoreAdapter<VortexRdfStore> {
    return {
        slug: variant.slug,
        label: variant.label,
        build: (quads) => VortexRdfStore.fromQuads(quads, variant.options),
        newEmpty: () => VortexRdfStore.empty(),
        addAll: async (h, quads) => { for (const q of quads) await h.addQuad(q); },
        addBatch: (h, quads) => h.addQuads(quads),
        deleteAll: async (h, quads) => { for (const q of quads) await h.deleteQuad(q); },
        countMatch: async (h, p) => (await h.getQuads(p.s, p.p, p.o, p.g)).length,
        dispose: freeWasm,
    };
}

function newRdfStore(kind: 'default' | 'single'): RdfStore {
    if (kind === 'default') return RdfStore.createDefault();
    // Single GSPO index — the same constructors createDefault uses, one combination.
    // `createDefault` is rdf-stores's only factory and always wires in quoted-triple
    // (RDF-star) support (RdfStoreIndexNestedMapQuoted / TermDictionaryQuotedIndexed) —
    // there's no leaner non-quoted construction path in the library, so mirroring that
    // exact choice here (rather than substituting a plain, non-quoted index) is what
    // keeps this an apples-to-apples comparison with what real rdf-stores callers get.
    // Pin <number> (as createDefault does) so Q resolves to Quad, not BaseQuad.
    return new RdfStore<number>({
        indexCombinations: [['graph', 'subject', 'predicate', 'object']],
        indexConstructor: (o) => new RdfStoreIndexNestedMapQuoted(o),
        dictionary: new TermDictionaryQuotedIndexed(new TermDictionaryNumberRecordFullTerms()),
        dataFactory: df,
    });
}

export function rdfStoresAdapter(kind: 'default' | 'single', label: string): StoreAdapter<RdfStore> {
    return {
        slug: kind === 'default' ? 'rdfstores_default' : 'rdfstores_single',
        label,
        build: (quads) => { const s = newRdfStore(kind); for (const q of quads) s.addQuad(q); return s; },
        newEmpty: () => newRdfStore(kind),
        addAll: (h, quads) => { for (const q of quads) h.addQuad(q); },
        deleteAll: (h, quads) => { for (const q of quads) h.removeQuad(q); },
        countMatch: (h, p) => h.getQuads(p.s, p.p, p.o, p.g).length, // synchronous array
    };
}

export function oxigraphAdapter(): StoreAdapter<OxiStore> {
    return {
        slug: 'oxigraph',
        label: 'oxigraph',
        build: (quads) => new OxiStore(quads as unknown as Iterable<never>), // bulk via constructor
        newEmpty: () => new OxiStore(),
        addAll: (h, quads) => { for (const q of quads) h.add(q as never); },
        deleteAll: (h, quads) => { for (const q of quads) h.delete(q as never); },
        countMatch: (h, p) => h.match(p.s as never, p.p as never, p.o as never, p.g as never).length,
        dispose: freeWasm,
    };
}

// Full matrix for ingest + query.
export const ADAPTERS: StoreAdapter[] = [
    ...VORTEX_VARIANTS.map(vortexAdapter),
    rdfStoresAdapter('default', 'rdf-stores (default)'),
    rdfStoresAdapter('single', 'rdf-stores (1 index)'),
    oxigraphAdapter(),
];

// Representative subset for mutations (per-quad add/delete is the fair cross-lib path).
export const MUT_ADAPTERS: StoreAdapter[] = [
    vortexAdapter({ slug: 'vortex', label: 'Vortex', options: { layout: 'dictionary' } }),
    rdfStoresAdapter('default', 'rdf-stores (default)'),
    oxigraphAdapter(),
];

// ─── Result normalization (dashboard shape) ──────────────────────────────────
export interface Row {
    group: string; variant: string | null; id: string;
    fastest: string; slowest: string; median: string; mean: string;
    fastest_ns: number; slowest_ns: number; median_ns: number; mean_ns: number;
    samples: string;
}

export function collect(bench: Bench, results: Row[]): void {
    for (const task of bench.tasks) {
        const r = task.result;
        if (!r || !('latency' in r) || !r.latency) continue;
        const lat = r.latency; // tinybench reports milliseconds
        const mean_ns = lat.mean * 1e6;
        const median_ns = lat.p50 * 1e6;
        const fastest_ns = lat.min * 1e6;
        const slowest_ns = lat.max * 1e6;
        const idx = task.name.indexOf('::');
        const group = idx >= 0 ? task.name.slice(0, idx) : task.name;
        const variant = idx >= 0 ? task.name.slice(idx + 2) : null;
        results.push({
            group, variant, id: task.name,
            fastest: fmtNs(fastest_ns), slowest: fmtNs(slowest_ns),
            median: fmtNs(median_ns), mean: fmtNs(mean_ns),
            fastest_ns, slowest_ns, median_ns, mean_ns,
            samples: String(lat.samplesCount),
        });
    }
}

// tinybench options per phase. Query gets a time budget; the expensive build/mutation
// phases get a low fixed iteration count (each build is costly at D=128).
export const QUERY_OPTS: BenchOptions = { time: 500, iterations: 10, warmup: true, warmupIterations: 5, throws: true };
export const HEAVY_OPTS: BenchOptions = { time: 0, iterations: 3, warmup: false, warmupIterations: 0, throws: true };
// See the comment on FULL_SCAN_PATTERN (datasets.ts): far fewer repetitions of a
// full-table dump.
export const FULL_SCAN_OPTS: BenchOptions = { time: 0, iterations: 3, warmup: false, warmupIterations: 0, throws: true };

// ─── Memory instrumentation ──────────────────────────────────────────────────
//
// `peakRssMb` is the cross-library metric: it is the only figure that means the
// same thing for a wasm-backed store (Vortex, oxigraph) and a pure-JS one
// (rdf-stores). `wasmHeapMb` is for attribution *within* Vortex only — it is
// meaningless for rdf-stores and would understate oxigraph, which has its own
// module. Never rank adapters by it.

/** Linux-only: the kernel-tracked high-water mark, the true peak RSS for this
 * process's whole lifetime — unlike `process.memoryUsage().rss`, which is only a
 * point-in-time reading and can miss a spike between samples. */
export function peakRssMb(): number | null {
    try {
        const status = readFileSync('/proc/self/status', 'utf8');
        const m = status.match(/^VmHWM:\s+(\d+)\s+kB/m);
        return m ? Math.round(Number(m[1]) / 1024) : null;
    } catch {
        return null;
    }
}

/** Current (not peak) RSS. Paired with dropping the input quads and forcing a
 * collection, this isolates what a store actually holds: the generated `Quad[]`
 * is well over a gigabyte of JS objects at the default scale, is identical for
 * every adapter, and otherwise swamps the differences between them in the peak. */
export function rssMb(): number | null {
    try {
        const status = readFileSync('/proc/self/status', 'utf8');
        const m = status.match(/^VmRSS:\s+(\d+)\s+kB/m);
        return m ? Math.round(Number(m[1]) / 1024) : null;
    } catch {
        return null;
    }
}

/** JS heap in use, after forcing a collection. Requires `--expose-gc` (every
 * worker spawn wires it in); without it this is a point-in-time reading that may
 * include garbage. */
export function jsHeapMb(): number {
    global.gc?.();
    return Math.round(process.memoryUsage().heapUsed / 1048576);
}

/** Vortex's wasm linear memory, in MB.
 *
 * `__wbg_init` short-circuits and returns the exports when the module is already
 * initialized (`entry/node.js` does that at import time), so re-invoking it is
 * just how one gets at the namespace. Linear memory only ever grows, so this is
 * simultaneously the current and the peak figure. */
export async function wasmHeapMb(): Promise<number | null> {
    try {
        const mod = await import('../pkg/web/vortex_rdf.js');
        const wasm = await mod.default();
        return Math.round((wasm as { memory: WebAssembly.Memory }).memory.buffer.byteLength / 1048576);
    } catch {
        return null;
    }
}
