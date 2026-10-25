// Pure bench helpers, shared by every instrument in this directory:
// codspeed.bench.ts, compare.bench.ts + shared.ts (and its workers), and
// dict-memory.bench.ts + its worker.
//
// PURITY CONTRACT: this module must import nothing but node builtins and types.
// No store library (oxigraph, rdf-stores, or the Vortex wasm module) may be
// reachable from here — codspeed.bench.ts imports it and runs under CodSpeed's
// Valgrind instrumentation, where loading a foreign multi-MB wasm module would
// pollute every measurement. shared.ts imports all three, so these helpers
// live here.

import { spawnSync } from 'node:child_process';
import { readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

import type { Quad } from '@rdfjs/types';
import type { Fn, FnOptions } from 'tinybench';

/** The quad shape every consume helper reads: the three mandatory term slots
 *  plus an optional graph, so rdf-stores, oxigraph and Vortex results all fit. */
type TermQuad = {
    subject: { value: string }; predicate: { value: string };
    object: { value: string }; graph?: { value: string };
};

/** Read every term value of `quads[from, to)`; returns the characters read. */
function readTerms(quads: ArrayLike<TermQuad>, from: number, to: number): number {
    let chars = 0;
    for (let i = from; i < to; i++) {
        const q = quads[i];
        chars += q.subject.value.length + q.predicate.value.length
            + q.object.value.length + (q.graph?.value.length ?? 0);
    }
    return chars;
}

/** Force every term of every quad to be materialized.
 *
 * Counting rows (or taking `.length`) never decodes a single term under the
 * Dictionary layout — the lazy read model hands back term *codes* and only
 * resolves them when `.value` is read. Without this the whole term-decoding
 * path, and the per-distinct-term boundary crossing the on-demand dictionary
 * makes, is invisible to any instrument here. */
export function decodeAll(quads: readonly Quad[]): number {
    return readTerms(quads, 0, quads.length);
}

// ─── Tasks that own a fresh handle ───────────────────────────────────────────
//
// A task that builds a store, or queries one opened for it alone, frees that
// store in `afterEach`, which tinybench does not time (and runs even when the
// task throws), so freeing never pollutes the measured cost. These return the
// task's body and hooks, spread into `bench.add(name, ...)`, rather than
// calling `bench.add` themselves: CodSpeed names a task by the file that calls
// `bench.add` as well as by the task's name, so that call stays in the bench
// file.

/** Time making a handle with `make`, then `fill` on it if given; `dispose`
 *  gets each handle made, or `undefined` if `make` threw, in `afterEach`. */
export function making<H>(
    make: () => Promise<H> | H, dispose: (h: H | undefined) => void, fill?: (h: H) => unknown,
): [Fn, FnOptions] {
    let h: H | undefined;
    return [
        async () => {
            h = await make();
            if (fill) await fill(h);
        },
        { afterEach: () => { dispose(h); h = undefined; } },
    ];
}

/** Time `run` on a handle `make` builds for each iteration in `beforeEach`;
 *  `dispose` gets it in `afterEach`. */
export function onFresh<H>(
    make: () => Promise<H> | H, run: (h: H) => unknown, dispose: (h: H | undefined) => void,
): [Fn, FnOptions] {
    let h: H | undefined;
    return [
        async () => { await run(h as H); },
        {
            beforeEach: async () => { h = await make(); },
            afterEach: () => { dispose(h); h = undefined; },
        },
    ];
}

/** Human-readable duration from nanoseconds. */
export function fmtNs(ns: number): string {
    if (ns < 1e3) return ns.toFixed(0) + ' ns';
    if (ns < 1e6) return (ns / 1e3).toPrecision(3) + ' µs';
    if (ns < 1e9) return (ns / 1e6).toPrecision(3) + ' ms';
    return (ns / 1e9).toPrecision(3) + ' s';
}

// ─── Consuming a result set ──────────────────────────────────────────────────

/** Wall-clock budget for one consume loop, ms. `0` disables the check.
 *
 *  A store that cannot materialize a result set does not fail fast — it
 *  death-marches, ballooning the heap towards the V8 cap (GC falls out of
 *  concurrent mode into back-to-back full collections) until its wasm module
 *  traps. The consume loop is where that time is spent, so this is where it
 *  can be cut short: the throw surfaces through the worker's per-phase catch
 *  as an ordinary `failures` entry and renders as a "failed" cell.
 *
 *  The default is orders of magnitude above any *successful* consume. */
const CONSUME_BUDGET_MS = Number(process.env.CONSUME_BUDGET_MS ?? 120_000);

/** Where every consume loop's reads escape to, so the JIT cannot drop them. */
let consumeSink = 0;

/** Thrown when a consume loop exceeds [`CONSUME_BUDGET_MS`]. The message is what
 *  lands in the run's `failures` list, so it says how far the loop got. */
class ConsumeBudgetExceeded extends Error {
    constructor(done: number, total: number, ms: number) {
        super(
            `consume budget exceeded after ${(ms / 1000).toFixed(0)}s ` +
            `(${done.toLocaleString()} of ${total.toLocaleString()} quads; ` +
            `raise CONSUME_BUDGET_MS to wait longer)`,
        );
        this.name = 'ConsumeBudgetExceeded';
    }
}

/** Rows per budget check: the budget is checked between windows, never per
 *  quad, so the timer stays out of the measured loop. Result sets below one
 *  window are never checked — a death march needs a large result set. */
const CONSUME_WINDOW = 65_536;

/** Count a result set by consuming it: read every term of every quad.
 *
 *  The stores return different amounts of *done* work: the pure-JS stores hold
 *  eager term objects, while Vortex quads are lazy wasm-backed proxies whose
 *  term values have not crossed the boundary yet. Counting `.length` alone lets
 *  Vortex skip that crossing entirely, while the Rust and Python tabs both
 *  materialize (quads_vec / N-Triples strings). Reading all four term values
 *  makes materialization part of the measured work everywhere, and puts the
 *  wasm boundary cost on the page. */
export function consumeQuads(quads: ArrayLike<TermQuad>): number {
    let acc = 0;
    const t0 = CONSUME_BUDGET_MS > 0 ? performance.now() : 0;
    for (let off = 0; off < quads.length; off += CONSUME_WINDOW) {
        if (CONSUME_BUDGET_MS > 0 && off > 0) {
            const ms = performance.now() - t0;
            if (ms > CONSUME_BUDGET_MS) throw new ConsumeBudgetExceeded(off, quads.length, ms);
        }
        acc += readTerms(quads, off, Math.min(off + CONSUME_WINDOW, quads.length));
    }
    consumeSink += acc;
    return quads.length;
}

/** [`consumeQuads`]'s contract for a store whose results are term strings
 *  not quad objects (hdt): read every string, escape the reads. */
export function consumeStrings(strings: string[]): void {
    let acc = 0;
    for (const s of strings) acc += s.length;
    consumeSink += acc;
}

const here = dirname(fileURLToPath(import.meta.url));
const tsxBin = resolve(here, '..', 'node_modules', '.bin', 'tsx');
/** `--expose-gc` is required by shared.ts's memory readings; the heap ceiling
 *  covers the multi-million-quad datasets the workers generate. */
const NODE_FLAGS = ['--expose-gc', '--max-old-space-size=8192'];

/** Wall-clock backstop for one worker process, ms; `0` disables it.
 *
 *  The last line of defense behind `consumeQuads`' in-loop budget: that check
 *  can only fire while JavaScript is running, so a phase that stalls *inside* a
 *  single wasm call (or any future pathology outside our own loops) would hang
 *  the run indefinitely. The default is far above any legitimate worker. */
const WORKER_TIMEOUT_MS = Number(process.env.WORKER_TIMEOUT_MS ?? 30 * 60_000);

/**
 * Run one bench worker to completion in its own process and return its parsed
 * JSON output, or null if it could not produce one.
 *
 * The worker receives `args` followed by the path it must write its JSON to;
 * that file is this function's to name and to remove. A non-zero exit is
 * reported and skipped, not thrown, so one failed point never costs the
 * whole run. A worker that outlives `WORKER_TIMEOUT_MS` is killed the same way
 * — SIGKILL, because a wasm-bound death march does not answer SIGTERM.
 *
 * Process-per-worker is a correctness requirement, not an optimization — see
 * the headers of compare.bench.ts (cross-adapter memory contamination) and
 * dict-memory.worker.ts (wasm linear memory never shrinks).
 */
export function runWorkerProcess<T>(workerPath: string, args: string[], label: string): T | null {
    const stem = label.replace(/[^A-Za-z0-9._-]+/g, '-');
    const outFile = join(tmpdir(), `vortex-bench-${stem}-${process.pid}.json`);
    const res = spawnSync(tsxBin, [...NODE_FLAGS, workerPath, ...args, outFile], {
        stdio: 'inherit',
        env: process.env,
        ...(WORKER_TIMEOUT_MS > 0 ? { timeout: WORKER_TIMEOUT_MS, killSignal: 'SIGKILL' as const } : {}),
    });
    try {
        if (res.status !== 0) {
            const why = res.signal
                ? `was killed (${res.signal}${res.signal === 'SIGKILL'
                    ? `, likely the ${WORKER_TIMEOUT_MS / 60_000}-minute WORKER_TIMEOUT_MS backstop` : ''})`
                : `exited ${res.status}`;
            console.error(`\n[${label}] worker ${why} — skipping; the rest of the run continues.`);
            return null;
        }
        return JSON.parse(readFileSync(outFile, 'utf8')) as T;
    } catch (e) {
        console.error(`[${label}] failed to read worker output:`, e);
        return null;
    } finally {
        rmSync(outFile, { force: true });
    }
}
