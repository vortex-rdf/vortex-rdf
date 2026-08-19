// How much of Vortex's wasm memory is the term dictionary?
//
// This is NOT a timing benchmark and is never uploaded anywhere. It is the only
// instrument that can attribute wasm memory at all: `compare.bench.ts` reports
// whole-process peak RSS, which is the fair cross-library number but says
// nothing about *what* is holding the memory.
//
// It has two modes.
//
// ── Regression check (the default) ───────────────────────────────────────────
//
//   npm run bench:dict-memory        # one config, one process, ~1 min
//
// Runs the single config every committed figure was measured at and prints the
// result against REFERENCE below. Use this after touching anything on the
// dictionary, ingest, or serialization path.
//
// ── Attribution sweep (opt-in) ───────────────────────────────────────────────
//
//   DICT_MEM_RATIOS=0.001,0.01,0.1,0.5,1.0 \
//   DICT_MEM_SLUGS=vortex_dict,vortex_default \
//   npm run bench:dict-memory
//
// Fits per-term cost. Needs >= 2 points, and is what produced the bytes/term
// numbers in the first place.
//
// Method — two nested differentials, because nothing can be read directly.
//
// Inner (per config, in the worker): several stores are held live at once and
// memory is read after each, so the slope against store count gives the RETAINED
// cost of one store. A single store tells you nothing — the ingest transient
// sets the high-water mark and the store is allocated inside the space it
// freed. See the worker's header for the measurement that established this.
//
// Outer (here, sweep mode only): hold rows fixed, sweep term cardinality, and fit
//
//   d(retained per store)/d(terms)  the dictionary's per-term cost
//   intercept as terms -> 0         quad columns + per-store overhead. For the
//                                   Dictionary layout this should land near
//                                   16 bytes/row (four u32 columns) — that
//                                   agreement is what makes the slope credible.
//
// Alongside: what the first Dictionary read costs (it used to copy the whole
// dictionary twice), what a full scan with every term materialized costs (the
// on-demand dictionary's worst case, one boundary crossing per distinct term),
// and whether repeated mutate-then-query grows the high-water mark.
//
// Every config runs in its own process because wasm memory never shrinks; see
// dict-memory.worker.ts.

import { writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, resolve } from 'node:path';

import { runWorkerProcess } from './util.js';

const here = dirname(fileURLToPath(import.meta.url));
const workerPath = resolve(here, 'dict-memory.worker.ts');
const OUT = resolve(here, process.env.DICT_MEM_OUT ?? 'dict-memory.json');

/** Rows held fixed across the sweep — only term cardinality varies.
 *
 *  200k is the scale every committed figure was taken at. Keeping it as the
 *  default is what makes a fresh run comparable to REFERENCE. */
const N = Number(process.env.DICT_MEM_N ?? 200_000);

/** Which build variants to run. Dictionary layout is the subject. Add
 *  `vortex_default` when sweeping: it has no term dictionary, so its
 *  slope isolates everything that is *not* the dictionary. */
const SLUGS = (process.env.DICT_MEM_SLUGS ?? 'vortex_dict').split(',');

/** Subject ratio per point; object ratio tracks it at half.
 *
 *  One point by default, at ratio 1.0 — a distinct subject per row, which is
 *  300,037 terms at N=200k. That is deliberately not the comparative
 *  benchmark's default (0.1): this tool measures the dictionary, so it wants
 *  the configuration that makes the dictionary largest. Pass several to sweep
 *  cardinality and fit the per-term cost; two orders of magnitude is what the
 *  original fit used. */
const RATIOS = (process.env.DICT_MEM_RATIOS ?? '1.0').split(',').map(Number);

/** Figures from the run that landed the interning ingest — commit
 *  `perf: intern terms at ingest ...`, at the default config above. (The FSST
 *  run this replaced measured retained 12.0 / first 93 / last 129.)
 *
 *  Memory is reproducible for a given wasm build and dataset, so drift here is
 *  a real regression. Timings are not compared: they track the host.
 *
 *  retainedPerStoreMb is a 4-point slope fit and carries ±2-3 MB of
 *  fragmentation noise between runs with identical store content; the
 *  high-water figures are the stable ones. */
const REFERENCE = { retainedPerStoreMb: 15.0, firstStoreMb: 51, lastStoreMb: 97, stores: 4 };

interface Point {
    slug: string; n: number; terms: number; stores: number;
    wasmPerStore: (number | null)[];
    wasmAfterInit: number | null; wasmAfterGen: number | null;
    wasmAfterFirstQuery: number | null; wasmAfterFullScan: number | null;
    wasmAfterRebuild: number | null; wasmAfterFree: number | null;
    jsAfterInit: number; jsAfterBuild: number; jsAfterFirstQuery: number;
    jsAfterFullScan: number; jsAfterRebuild: number;
    rssAfterBuild: number | null; peakRssMb: number | null;
    scanMs: number; decodeMs: number; mutateQueryMs: number;
    fullRows: number; firstQueryRows: number; decodedChars: number;
    [k: string]: unknown;
}

/** Retained MB per store: the slope of linear memory against live store count. */
function retainedPerStore(p: Point): number | null {
    const ys = p.wasmPerStore.filter((v): v is number => v !== null);
    if (ys.length < 2) return null;
    const xs = ys.map((_, i) => i + 1);
    return fit(xs, ys).slope;
}

/** One cardinality point, in its own process — wasm memory never shrinks, so a
 *  sweep sharing a process would report every point at the high-water mark of
 *  the largest one before it. */
function runPoint(slug: string, subjRatio: number, objRatio: number): Point | null {
    return runWorkerProcess<Point>(
        workerPath,
        [slug, String(N), String(subjRatio), String(objRatio)],
        `${slug} @ ratio ${subjRatio}`,
    );
}

/** True when this run is the exact config REFERENCE was measured at, and so is
 *  comparable to it. Any knob turned makes the comparison meaningless. */
function atReferenceConfig(points: Point[]): points is [Point] {
    return points.length === 1
        && N === 200_000
        && RATIOS.length === 1 && RATIOS[0] === 1.0
        && points[0].slug === 'vortex_dict'
        && points[0].stores === REFERENCE.stores;
}

/** The regression view: today's numbers beside the ones FSST landed with. */
function reportAgainstReference(p: Point): void {
    const show = (now: number | null | undefined, then: number): string => {
        if (now === null || now === undefined) return '?';
        const d = now - then;
        return `${now.toFixed(1)} MB  (${d >= 0 ? '+' : ''}${d.toFixed(1)} vs ${then.toFixed(1)})`;
    };
    console.log('\n─── vs. the FSST reference run ─────────────────────────────');
    console.log(`  retained/store   ${show(retainedPerStore(p), REFERENCE.retainedPerStoreMb)}`);
    console.log(`  1 store          ${show(p.wasmPerStore[0], REFERENCE.firstStoreMb)}`);
    console.log(`  ${REFERENCE.stores} stores         ${show(p.wasmPerStore.at(-1), REFERENCE.lastStoreMb)}`);
    console.log(`  full scan        ${p.scanMs.toFixed(0)} ms + ${p.decodeMs.toFixed(0)} ms decode `
        + `(${p.fullRows.toLocaleString()} rows; host-dependent, not compared)`);
}

/** Least-squares fit of `y = slope*x + intercept`. */
function fit(xs: number[], ys: number[]): { slope: number; intercept: number } {
    const k = xs.length;
    const mx = xs.reduce((a, b) => a + b, 0) / k;
    const my = ys.reduce((a, b) => a + b, 0) / k;
    let num = 0, den = 0;
    for (let i = 0; i < k; i++) {
        num += (xs[i] - mx) * (ys[i] - my);
        den += (xs[i] - mx) ** 2;
    }
    const slope = den === 0 ? 0 : num / den;
    return { slope, intercept: my - slope * mx };
}

function main(): void {
    const mode = RATIOS.length > 1 ? 'sweep' : 'regression check';
    console.log(`Dictionary memory ${mode}: n=${N.toLocaleString()} rows, `
        + `ratios ${RATIOS.join(', ')}, slugs ${SLUGS.join(', ')}\n`);
    const points: Point[] = [];

    for (const slug of SLUGS) {
        for (const r of RATIOS) {
            console.log(`\n── ${slug} @ subjectRatio=${r} ─────────────────────────`);
            const p = runPoint(slug, r, r / 2);
            if (!p) continue;
            points.push(p);
            const per = retainedPerStore(p);
            console.log(
                `   terms=${p.terms.toLocaleString()}  retained/store=${per === null ? '?' : per.toFixed(1)} MB  ` +
                `wasm: [${p.wasmPerStore.join(',')}] query=${p.wasmAfterFirstQuery} ` +
                `scan=${p.wasmAfterFullScan} rebuild=${p.wasmAfterRebuild} MB  ` +
                `scan=${p.scanMs.toFixed(0)}ms decode=${p.decodeMs.toFixed(0)}ms ` +
                `mutQuery=${p.mutateQueryMs.toFixed(0)}ms`,
            );
        }
    }

    const analysis: Record<string, unknown> = {};
    for (const slug of SLUGS) {
        const ps = points.filter((p) => p.slug === slug && retainedPerStore(p) !== null);
        if (ps.length < 2) continue;
        const f = fit(ps.map((p) => p.terms), ps.map((p) => retainedPerStore(p) as number));
        analysis[slug] = {
            bytesPerTerm: Math.round(f.slope * 1048576),
            interceptMb: Number(f.intercept.toFixed(1)),
            // The control: four u32 columns are 16 bytes/row regardless of terms.
            expectedQuadColumnsMb: Number(((N * 16) / 1048576).toFixed(1)),
            retainedPerStoreMb: ps.map((p) => ({
                terms: p.terms, mb: Number((retainedPerStore(p) as number).toFixed(1)),
            })),
            firstQueryDeltaMb: ps.map((p) => ({
                terms: p.terms,
                wasm: (p.wasmAfterFirstQuery ?? 0) - (p.wasmPerStore.at(-1) ?? 0)!,
                js: p.jsAfterFirstQuery - p.jsAfterBuild,
            })),
            rebuildGrowthMb: ps.map((p) => ({
                terms: p.terms,
                growth: (p.wasmAfterRebuild ?? 0) - (p.wasmAfterFullScan ?? 0),
                mutateQueryMs: Math.round(p.mutateQueryMs),
            })),
            fullScan: ps.map((p) => ({
                terms: p.terms, rows: p.fullRows,
                scanMs: Math.round(p.scanMs), decodeMs: Math.round(p.decodeMs),
                jsHeapDeltaMb: p.jsAfterFullScan - p.jsAfterFirstQuery,
            })),
        };
    }

    writeFileSync(OUT, JSON.stringify({ n: N, ratios: RATIOS, points, analysis }, null, 2));

    if (Object.keys(analysis).length > 0) {
        console.log('\n─── analysis ───────────────────────────────────────────────');
        for (const [slug, a] of Object.entries(analysis)) {
            const v = a as Record<string, unknown>;
            console.log(`${slug}:`);
            console.log(`  ${v.bytesPerTerm} bytes/term`);
            console.log(`  intercept ${v.interceptMb} MB (quad columns alone would be ${v.expectedQuadColumnsMb} MB)`);
        }
    } else if (atReferenceConfig(points)) {
        reportAgainstReference(points[0]);
    } else {
        console.log('\nNo per-term fit: that needs >= 2 cardinality points. See the header '
            + 'for the sweep invocation.');
    }

    console.log(`\nWrote ${points.length} point${points.length === 1 ? '' : 's'} → ${OUT}`);
}

main();
