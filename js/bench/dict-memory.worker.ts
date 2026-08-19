// Runs ONE dictionary-memory config in its own process, spawned by
// dict-memory.bench.ts.
//
// Process-per-config is not an optimization, it is a correctness requirement:
// wasm linear memory only ever grows (`memory.grow` is one-way, and freeing a
// store returns pages to the module's allocator, never to the engine). A sweep
// run in one process would report every config at the high-water mark of the
// largest one before it.
//
// ── Why this measures RETAINED memory by building several stores ─────────────
//
// The obvious instrument — build one store, read `memory.buffer.byteLength` —
// does not work, and it is worth recording why. Ingest allocates a large
// transient (the packed quad buffer crossing the boundary, plus the owned
// `RawQuad` strings) whose size tracks *rows*, not distinct terms. That
// transient sets the module's high-water mark; the dictionary is then allocated
// inside the space it freed, so linear memory does not grow and the dictionary
// is invisible. Measured directly: wasm memory sat at exactly 104 MB across a
// 900x sweep of term cardinality.
//
// So instead: build `STORES` stores of the same config, keeping them all alive,
// and read memory after each. The transient is reused across builds, but each
// *retained* store must add to the module. The slope of memory against store
// count is the retained per-store cost — which is what we actually want to
// attribute.

import { writeFileSync } from 'node:fs';

import {
    VORTEX_VARIANTS, vortexAdapter, genDataset, datasetProbes, moduli,
    reclaim, peakRssMb, rssMb, jsHeapMb, wasmHeapMb,
    type DatasetOpts,
} from './shared.js';
import { decodeAll } from './util.js';
import type { VortexRdfStore } from '../entry/node.js';

const [, , slugArg, nArg, subjRatioArg, objRatioArg, outFile] = process.argv;
if (!slugArg || !nArg || !subjRatioArg || !objRatioArg || !outFile) {
    console.error('usage: dict-memory.worker.ts <slug> <n> <subjRatio> <objRatio> <out-file>');
    process.exit(1);
}

const n = Number(nArg);
const opts: DatasetOpts = {
    subjectRatio: Number(subjRatioArg),
    objectRatio: Number(objRatioArg),
};

/** Concurrently-live stores used to read off the retained per-store cost. */
const STORES = Number(process.env.DICT_MEM_STORES ?? 4);
/** Deletes to run before re-querying, exercising the dictionary-view rebuild. */
const DELETES = Number(process.env.DICT_MEM_DELETES ?? 5);

async function main(): Promise<void> {
    const variant = VORTEX_VARIANTS.find((v) => v.slug === slugArg);
    if (!variant) {
        console.error(`unknown vortex variant '${slugArg}'`);
        process.exit(1);
    }
    const a = vortexAdapter(variant);
    const m = moduli(n, opts);
    const probes = datasetProbes(n, opts);

    const wasmAfterInit = await wasmHeapMb();
    const jsAfterInit = jsHeapMb();

    const quads = genDataset(n, opts);
    const wasmAfterGen = await wasmHeapMb();

    // ── retained per-store cost ──────────────────────────────────────────────
    const live: VortexRdfStore[] = [];
    const wasmPerStore: (number | null)[] = [];
    for (let k = 0; k < STORES; k++) {
        live.push(await a.build(quads));
        wasmPerStore.push(await wasmHeapMb());
    }
    const jsAfterBuild = jsHeapMb();
    const rssAfterBuild = rssMb();

    const h = live[0];

    // ── first Dictionary-layout read ─────────────────────────────────────────
    // This is what used to flatten the whole dictionary into wasm and copy it
    // again into the JS heap, however few terms the query touched.
    const pPat = probes.triples.find((p) => p.name === 'P')!;
    const firstQueryRows = await a.countMatch(h, pPat);
    const wasmAfterFirstQuery = await wasmHeapMb();
    const jsAfterFirstQuery = jsHeapMb();

    // ── full scan, terms actually materialized ───────────────────────────────
    // Under the on-demand dictionary this pays one boundary crossing per
    // distinct term; under the old bulk copy it paid one large copy. This is the
    // number that decides whether on-demand is affordable.
    const scanStart = performance.now();
    const all = await h.getQuads(null, null, null, null);
    const scanMs = performance.now() - scanStart;
    const decodeStart = performance.now();
    const decodedChars = decodeAll(all);
    const decodeMs = performance.now() - decodeStart;
    const fullRows = all.length;
    const wasmAfterFullScan = await wasmHeapMb();
    const jsAfterFullScan = jsHeapMb();

    // ── mutate-then-query ────────────────────────────────────────────────────
    // Each delete drops the cached dictionary view, so the next read rebuilds
    // it. That rebuild used to be O(dictionary); it should now be O(1).
    const delQuads = quads.slice(0, DELETES);
    const mutStart = performance.now();
    for (const q of delQuads) {
        await h.deleteQuad(q);
        await a.countMatch(h, pPat);
    }
    const mutateQueryMs = performance.now() - mutStart;
    const wasmAfterRebuild = await wasmHeapMb();
    const jsAfterRebuild = jsHeapMb();

    for (const s of live) reclaim(a, s);
    live.length = 0;
    const wasmAfterFree = await wasmHeapMb();

    writeFileSync(outFile, JSON.stringify({
        slug: slugArg,
        n,
        subjectRatio: opts.subjectRatio,
        objectRatio: opts.objectRatio,
        terms: m.terms,
        cardinality: m,
        stores: STORES,
        wasmPerStore,
        firstQueryRows,
        fullRows,
        decodedChars,
        scanMs,
        decodeMs,
        mutateQueryMs,
        deletes: DELETES,
        wasmAfterInit,
        wasmAfterGen,
        wasmAfterFirstQuery,
        wasmAfterFullScan,
        wasmAfterRebuild,
        wasmAfterFree,
        jsAfterInit,
        jsAfterBuild,
        jsAfterFirstQuery,
        jsAfterFullScan,
        jsAfterRebuild,
        rssAfterBuild,
        peakRssMb: peakRssMb(),
    }));
}

main().catch((e) => {
    console.error(e);
    process.exit(1);
});
