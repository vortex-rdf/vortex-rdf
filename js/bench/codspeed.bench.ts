// CodSpeed benchmark for the JS/WASM bindings — library-only (no rdf-stores /
// oxigraph comparison). This is the JavaScript counterpart of the Rust suite
// (core/benches/benchmark.rs): same "star" (one-factor-at-a-time) design over
// the store's real axes — layout × secondary index — swept across the query
// routing patterns, plus the build, read-back and mutation paths.
//
// Unlike js/bench/compare.bench.ts (wall-clock, comparative, feeds the Pages
// dashboard, NEVER uploaded), THIS file IS uploaded to CodSpeed: it runs under
// CodSpeedHQ/action in instrumentation mode (see .github/workflows/codspeed.yml),
// so every task gets deterministic instruction counts and a flamegraph published
// next to the Rust benchmarks. `withCodSpeed` is a no-op when not run under the
// action, so `npm run bench:codspeed` locally just produces wall-clock numbers.
//
// It supersedes the former core/benches/js_read_path.rs, which could only
// *approximate* this read path by timing its Rust-side stages in isolation;
// here the whole JS→WASM boundary (promise machinery, quad packing, the lazy
// read model) is measured for real, and the flamegraph attributes the cost.
//
// Single process by design: CodSpeed measures each task deterministically, and
// there are no competing libraries to isolate — so none of compare.bench.ts's
// process-per-adapter machinery is needed here.
//
// Run locally (after `npm run build`):
//   npm run bench:codspeed
//   CODSPEED_BENCH_DIM=24 npm run bench:codspeed   # bigger dataset

import { Bench, type BenchOptions } from 'tinybench';
import { withCodSpeed } from '@codspeed/tinybench-plugin';
import type { Quad } from '@rdfjs/types';

import { VortexRdfStore, type BuildOptions } from '../entry/node.js';
// Only the pure bench modules are importable here — ./util.js and
// ./datasets.js, the two with no store library behind them (see their purity
// contracts); shared.ts loads oxigraph and rdf-stores at module scope, which
// this instrumented process must never do.
import { decodeAll, fmtNs, freeWasm } from './util.js';
import {
    FULL_SCAN_PATTERN, genDataset, genFresh, genLiteralTriples, genQuads,
    genTriples, genTriplesPrefix, nn, type Pat,
} from './datasets.js';

// ─── Dataset shape (env-tunable) ─────────────────────────────────────────────
// Small by default: CodSpeed instrumentation runs under Valgrind (~50× slower)
// and counts instructions deterministically, so a representative size catches
// regressions on every path — a larger one only multiplies Valgrind cost.
//
// Two dataset shapes, per task sensitivity: the dense cube (genTriples and
// friends — 3·DIM distinct terms over DIM³ rows) for the tasks measuring
// routing and boundary cost, and the cardinality-realistic genDataset (terms
// scaling with rows; see datasets.ts) for the term-handling-sensitive tasks,
// where the cube's ~48 distinct terms would make dictionary build and
// per-distinct-term decode invisible.
const DIM = Number(process.env.CODSPEED_BENCH_DIM ?? 16); // triples: DIM³ rows
const DIM_QUADS = Number(process.env.CODSPEED_BENCH_DIM_QUADS ?? 8); // quads: DIM_QUADS⁴ rows
const MUT_N = Number(process.env.CODSPEED_MUT_N ?? 500); // add/delete batch size

// ─── Query patterns (probe terms fixed at index 0, so they always hit rows) ──
const t0 = nn(0);
const g0 = nn(0);
// The six routing classes the resolver branches on, split by which dataset can
// exercise them: S/P/O/PO/SPO on the triples store, G/SPOG on the quads store.
const TRIPLE_PATTERNS: Pat[] = [
    { name: 'S', s: t0, p: null, o: null, g: null },
    { name: 'P', s: null, p: t0, o: null, g: null },
    { name: 'O', s: null, p: null, o: t0, g: null },
    { name: 'PO', s: null, p: t0, o: t0, g: null },
    { name: 'SPO', s: t0, p: t0, o: t0, g: null },
];
const QUAD_PATTERNS: Pat[] = [
    { name: 'G', s: null, p: null, o: null, g: g0 },
    { name: 'SPOG', s: t0, p: t0, o: t0, g: g0 },
];

// ─── Store variants (mirror the Rust star-design axes) ───────────────────────
type Variant = { slug: string; options: BuildOptions };

// Build (write) path: sweep layout × index one factor at a time around a
// Dictionary baseline (the JS default), matching the Rust serialize group's
// axes.
const BUILD_VARIANTS: Variant[] = [
    { slug: 'dict', options: { layout: 'dictionary' } },
    { slug: 'default', options: { layout: 'default' } },
    { slug: 'typedobject', options: { layout: 'typed-object' } },
    { slug: 'dict_byref', options: { layout: 'dictionary', indexes: ['secondary-by-reference'] } },
    { slug: 'dict_bycopy', options: { layout: 'dictionary', indexes: ['secondary-by-copy'] } },
];

// Query (read) path: two representative configs — the JS default (subject
// binary search only, no secondary index) and the fully-indexed fast path
// (where a bound predicate/object binary-searches too).
const QUERY_VARIANTS: Variant[] = [
    { slug: 'dict', options: { layout: 'dictionary' } },
    { slug: 'dict_bycopy', options: { layout: 'dictionary', indexes: ['secondary-by-copy'] } },
];

async function drain(stream: unknown): Promise<number> {
    let n = 0;
    for await (const _ of stream as AsyncIterable<Quad>) n++;
    return n;
}

// tinybench options shape only the LOCAL wall-clock run. Under CodSpeed
// instrumentation the plugin ignores them: each task runs seven untimed warmup
// invocations (core.optimizeFunction), then global.gc(), then exactly ONE
// measured invocation. That single-shot model is why the runner passes
// --no-liftoff (codspeed.yml, package.json): with background wasm tier-up
// enabled, the measured invocation nondeterministically executed Liftoff or
// TurboFan code — a ~2x instruction-count swing on the build tasks at
// identical code. Reads get a time budget; the costly build/mutation phases
// get warmup plus a fixed iteration count so local numbers are stable too.
const READ_OPTS: BenchOptions = { time: 200, iterations: 10, warmup: true, warmupIterations: 3, throws: true };
const HEAVY_OPTS: BenchOptions = { time: 0, iterations: 7, warmup: true, warmupIterations: 2, throws: true };

async function runGroup(opts: BenchOptions, add: (b: Bench) => void): Promise<void> {
    const bench = withCodSpeed(new Bench(opts));
    add(bench);
    await bench.run();
    // Local wall-clock runs: print a compact mean per task. Under the CodSpeed
    // runner tinybench's result shape differs (the plugin logs its own
    // `[CodSpeed] Measured` lines), so only print when a latency is present.
    for (const task of bench.tasks) {
        const r = task.result;
        if (!r || !('latency' in r) || !r.latency) continue;
        console.log(`  ${task.name.padEnd(34)} ${fmtNs(r.latency.mean * 1e6)}`);
    }
}

// ─── Groups ──────────────────────────────────────────────────────────────────

/** build::<config> — VortexRdfStore.fromQuads over each star variant, plus a
 * fromString (parse + build) task to cover the RDF-text entry point and a
 * literal-bearing build covering term escaping on ingest. */
async function benchBuild(triples: Quad[], realistic: Quad[], literals: Quad[]): Promise<void> {
    // All generated terms are named nodes, so N-Triples serialization is trivial.
    const nquads = triples
        .map((q) => `<${q.subject.value}> <${q.predicate.value}> <${q.object.value}> .`)
        .join('\n');
    await runGroup(HEAVY_OPTS, (b) => {
        for (const v of BUILD_VARIANTS) {
            // build::dict is the guard on dictionary construction, so it
            // runs over the cardinality-realistic dataset — on the dense cube
            // the dictionary is ~48 terms and its build cost invisible. Its
            // numbers are therefore NOT comparable with the dense-cube
            // variants beside it.
            const data = v.slug === 'dict' ? realistic : triples;
            let h: VortexRdfStore | undefined;
            b.add(`build::${v.slug}`, async () => { h = await VortexRdfStore.fromQuads(data, v.options); }, {
                afterEach: () => { if (h) freeWasm(h); h = undefined; },
            });
        }
        let hs: VortexRdfStore | undefined;
        b.add('build::fromString_nquads', async () => {
            hs = await VortexRdfStore.fromString(nquads, 'nquads', { layout: 'dictionary' });
        }, { afterEach: () => { if (hs) freeWasm(hs); hs = undefined; } });
        let hl: VortexRdfStore | undefined;
        b.add('build::dict_literals', async () => {
            hl = await VortexRdfStore.fromQuads(literals, { layout: 'dictionary' });
        }, { afterEach: () => { if (hl) freeWasm(hl); hl = undefined; } });
    });
}

/** Literal-bearing read paths on a Dictionary store over
 * genLiteralTriples' dataset: toRdf re-escapes every literal on export, and
 * the decoded read parses every literal's serialized form on the JS side. */
async function benchLiterals(literals: Quad[]): Promise<void> {
    const store = await VortexRdfStore.fromQuads(literals, { layout: 'dictionary' });
    await runGroup(READ_OPTS, (b) => {
        b.add('readback::toRdf_nquads_literals', async () => { await store.toRdf('nquads'); });
        b.add('readpath::full_decoded_literals', async () => {
            decodeAll(await store.getQuads(null, null, null, null));
        });
    });
    freeWasm(store);
}

/** query_<config>::<pattern> — getQuads across every routing class, on each
 * query variant. Store built once per variant (untimed). */
async function benchQuery(triples: Quad[], quads: Quad[]): Promise<void> {
    for (const v of QUERY_VARIANTS) {
        const th = await VortexRdfStore.fromQuads(triples, v.options);
        await runGroup(READ_OPTS, (b) => {
            for (const p of TRIPLE_PATTERNS)
                b.add(`query_${v.slug}::${p.name}`, async () => { await th.getQuads(p.s, p.p, p.o, p.g); });
            b.add(`query_${v.slug}::full`, async () => {
                await th.getQuads(FULL_SCAN_PATTERN.s, FULL_SCAN_PATTERN.p, FULL_SCAN_PATTERN.o, FULL_SCAN_PATTERN.g);
            });
        });
        freeWasm(th);

        const qh = await VortexRdfStore.fromQuads(quads, v.options);
        await runGroup(READ_OPTS, (b) => {
            for (const p of QUAD_PATTERNS)
                b.add(`query_${v.slug}::${p.name}`, async () => { await qh.getQuads(p.s, p.p, p.o, p.g); });
        });
        freeWasm(qh);
    }
}

/** readpath::<variant> — the read entry points on the default store for one
 * selective pattern (S), isolating the boundary cost each carries: getQuads
 * (materialized array), match (lazy stream drain), matchCodes (zero-copy u32
 * columns, no term strings). Directly supports read-path tuning.
 *
 * The `_decoded` variants additionally read every term's `.value`. They are the
 * only benchmarks in this file that exercise term decoding at all — the others
 * stop at the lazy quads — so they are what guards the term dictionary's
 * per-lookup cost against regressions. `full_decoded` is the worst case: every
 * row, every term, i.e. every distinct code resolved once. */
async function benchReadPath(triples: Quad[], realistic: Quad[]): Promise<void> {
    const store = await VortexRdfStore.fromQuads(triples, { layout: 'dictionary' });
    // full_decoded resolves every distinct code once, so it runs over the
    // cardinality-realistic dataset: on the dense cube it would resolve ~48
    // codes and the per-distinct-term dictionary cost it exists to guard
    // would be invisible. Same store config, different data — do not compare
    // it row-for-row with the selective dense-cube tasks beside it.
    const storeReal = await VortexRdfStore.fromQuads(realistic, { layout: 'dictionary' });
    const p = TRIPLE_PATTERNS[0]; // S
    const f = FULL_SCAN_PATTERN;
    await runGroup(READ_OPTS, (b) => {
        b.add('readpath::getQuads', async () => { await store.getQuads(p.s, p.p, p.o, p.g); });
        b.add('readpath::match_stream', async () => { await drain(store.match(p.s, p.p, p.o, p.g)); });
        b.add('readpath::matchCodes', async () => { await store.matchCodes(p.s, p.p, p.o, p.g); });
        b.add('readpath::getQuads_decoded', async () => {
            decodeAll(await store.getQuads(p.s, p.p, p.o, p.g));
        });
        b.add('readpath::full_decoded', async () => {
            decodeAll(await storeReal.getQuads(f.s, f.p, f.o, f.g));
        });
    });
    freeWasm(store);
    freeWasm(storeReal);
}

/** readback::<op> — serialize/deserialize the store across the boundary:
 * toBytes (IPC out), fromBytes (IPC in), toRdf (N-Quads out). */
async function benchReadback(triples: Quad[]): Promise<void> {
    const store = await VortexRdfStore.fromQuads(triples, { layout: 'dictionary' });
    const bytes = await store.toBytes();
    await runGroup(READ_OPTS, (b) => {
        b.add('readback::toBytes', async () => { await store.toBytes(); });
        b.add('readback::toRdf_nquads', async () => { await store.toRdf('nquads'); });
        let h: VortexRdfStore | undefined;
        b.add('readback::fromBytes', async () => { h = await VortexRdfStore.fromBytes(bytes); }, {
            afterEach: () => { if (h) freeWasm(h); h = undefined; },
        });
    });
    freeWasm(store);
}

/** mutate::<op> — the mutation paths on the JS-default store: per-quad addQuad
 * loop, batched addQuads, per-quad deleteQuad loop. */
async function benchMutate(): Promise<void> {
    const fresh = genFresh(MUT_N);
    const delSlice = genTriplesPrefix(DIM, MUT_N);
    const opts: BuildOptions = { layout: 'dictionary' };

    await runGroup(HEAVY_OPTS, (b) => {
        let h: VortexRdfStore | undefined;
        b.add('mutate::addQuad_loop', async () => {
            h = VortexRdfStore.empty();
            for (const q of fresh) await h.addQuad(q);
        }, { afterEach: () => { if (h) freeWasm(h); h = undefined; } });

        let hb: VortexRdfStore | undefined;
        b.add('mutate::addQuads_batch', async () => {
            hb = VortexRdfStore.empty();
            await hb.addQuads(fresh);
        }, { afterEach: () => { if (hb) freeWasm(hb); hb = undefined; } });

        let hd: VortexRdfStore | undefined;
        b.add('mutate::deleteQuad_loop', async () => {
            for (const q of delSlice) await hd!.deleteQuad(q);
        }, {
            beforeEach: async () => { hd = await VortexRdfStore.fromQuads(delSlice, opts); },
            afterEach: () => { if (hd) freeWasm(hd); hd = undefined; },
        });
    });
}

async function main(): Promise<void> {
    console.log(
        `CodSpeed JS bench · triples DIM=${DIM} (${(DIM ** 3).toLocaleString()} rows), ` +
        `quads DIM_QUADS=${DIM_QUADS} (${(DIM_QUADS ** 4).toLocaleString()} rows), MUT_N=${MUT_N}`,
    );
    const triples = genTriples(DIM);
    const quads = genQuads(DIM_QUADS);
    const literals = genLiteralTriples(DIM);
    // Same row count as the dense cube, realistic term cardinality — for the
    // term-handling-sensitive tasks (build::dict, readpath::full_decoded).
    const realistic = genDataset(DIM ** 3);

    await benchBuild(triples, realistic, literals);
    await benchQuery(triples, quads);
    await benchReadPath(triples, realistic);
    await benchReadback(triples);
    await benchLiterals(literals);
    await benchMutate();
}

main().catch((e) => { console.error(e); process.exit(1); });
