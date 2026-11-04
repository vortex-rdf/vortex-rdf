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
import { existsSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

import type { Quad, Stream } from '@rdfjs/types';
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

/** Time every `make`, then keep the last handle: `beforeEach` disposes the
 *  previous one, so only one handle lives at a time, and the final one is left
 *  for `last()` to hand back, undisposed, once the bench has run. */
export function makingLast<H>(
    make: () => Promise<H> | H, dispose: (h: H) => void,
): { task: [Fn, FnOptions]; last(): H | undefined } {
    let h: H | undefined;
    return {
        task: [
            async () => { h = await make(); },
            {
                beforeEach: () => {
                    if (h !== undefined) dispose(h);
                    h = undefined;
                },
            },
        ],
        last: () => h,
    };
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
    constructor(done: number, total: number | null, ms: number) {
        super(
            `consume budget exceeded after ${(ms / 1000).toFixed(0)}s ` +
            (total === null
                ? `(${done.toLocaleString()} quads read; `
                : `(${done.toLocaleString()} of ${total.toLocaleString()} quads; `) +
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

/** [`consumeQuads`]'s contract for a quad stream (RDF/JS `match()`): read every
 *  term of every quad as it arrives, resolve to the count at `end`.
 *
 *  `end` and `error` are attached before `data`, because attaching `data` can
 *  start the flow at once. The budget is checked every [`CONSUME_WINDOW`]
 *  quads; past it the stream is destroyed (if it can be) and the promise
 *  rejects. Events after the promise settles are ignored. */
export function consumeStream(
    stream: Pick<Stream, 'on'>, budgetMs: number = CONSUME_BUDGET_MS,
): Promise<number> {
    return new Promise<number>((resolve, reject) => {
        let n = 0;
        let acc = 0;
        let settled = false;
        const t0 = budgetMs > 0 ? performance.now() : 0;
        stream.on('end', () => {
            if (settled) return;
            settled = true;
            consumeSink += acc;
            resolve(n);
        });
        stream.on('error', (e: unknown) => {
            if (settled) return;
            settled = true;
            reject(e);
        });
        stream.on('data', (q: TermQuad) => {
            if (settled) return;
            if (budgetMs > 0 && n > 0 && n % CONSUME_WINDOW === 0) {
                const ms = performance.now() - t0;
                if (ms > budgetMs) {
                    settled = true;
                    (stream as { destroy?: () => unknown }).destroy?.();
                    reject(new ConsumeBudgetExceeded(n, null, ms));
                    return;
                }
            }
            // readTerms' reads, inline: no array per quad in the timed read.
            acc += q.subject.value.length + q.predicate.value.length
                + q.object.value.length + (q.graph?.value.length ?? 0);
            n++;
        });
    });
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

// ─── The oxigraph build's version ────────────────────────────────────────────
//
// The suite runs oxigraph from the bench's own build of its JS binding
// (bench/oxigraph-wasm/build.sh), at the version the Rust comparison pins, so
// the two tabs measure one oxigraph release. compare.bench.ts checks the build
// against that pin before it runs anything.

/** The Rust comparison's lock file, whose `oxigraph` entry is the pin. */
export const BENCH_COMPARE_LOCK = resolve(here, '../../bench-compare/Cargo.lock');
/** Where `npm run build:oxigraph-wasm` writes the build. */
export const OXIGRAPH_PKG_DIR = resolve(here, 'oxigraph-pkg');

/** The version `lock`, a Cargo.lock's text, pins for `crate`. Each
 *  `[[package]]` block is read for its own `name` and `version` lines, so a
 *  neighbouring block's version is never taken for it. Throws unless exactly
 *  one block names `crate`. */
export function lockedVersion(lock: string, crate: string): string {
    const versions = lock.split('[[package]]').flatMap((block) => {
        const name = /^name = "(.*)"$/m.exec(block)?.[1];
        const version = /^version = "(.*)"$/m.exec(block)?.[1];
        return name === crate && version !== undefined ? [version] : [];
    });
    if (versions.length === 0) throw new Error(`the lock pins no ${crate}`);
    if (versions.length > 1) throw new Error(`the lock pins ${versions.length} versions of ${crate}`);
    return versions[0];
}

/** Check that the oxigraph build in `pkgDir` is the version `lockPath` pins,
 *  and return that version. Throws when there is no build to check, and when
 *  the build is another version, naming both. */
export function checkOxigraphVersion(lockPath: string, pkgDir: string): string {
    const pinned = lockedVersion(readFileSync(lockPath, 'utf8'), 'oxigraph');
    const manifest = join(pkgDir, 'package.json');
    if (!existsSync(manifest)) {
        throw new Error(
            `no oxigraph build at ${pkgDir} — run \`npm run build:oxigraph-wasm\` `
            + '(scripts/refresh.sh --only js builds it)');
    }
    const built = (JSON.parse(readFileSync(manifest, 'utf8')) as { version?: string }).version;
    if (built !== pinned) {
        throw new Error(
            `${pkgDir} holds oxigraph ${built} but ${lockPath} pins ${pinned} — `
            + 'rebuild it: npm run build:oxigraph-wasm');
    }
    return pinned;
}

// ─── Memory runs ─────────────────────────────────────────────────────────────
//
// compare.worker.ts's `memory` role measures one store in a process of its
// own: the live bytes of the store's wasm module (the bench's counter, linked
// into every module it loads) plus the JS heap, read around the store's reads
// and its dispose. The pure parts of that run live here, so they are tested
// without loading a store.

/** The block each counter must see held at the start of a memory run. */
export const SELF_CHECK_BYTES = 8 << 20;

/** What the counter's `memoryStats()` returns: live and peak heap bytes since
 *  counting was switched on (signed: a block allocated before and freed after
 *  counts below 0), and the module's `memory.buffer.byteLength`. */
export interface MemoryStats { liveBytes: number; peakLiveBytes: number; heapBytes: number }

/** The counter's exports (`bench-memory-stats`), in a module built with it. */
export interface MemoryStatsExports {
    setMemoryCounting(on: boolean): void;
    memoryStats(): MemoryStats;
    resetMemoryPeak(): void;
}

/** One wasm module's counter. Every member is a closure over the module, with
 *  no `this`, so each can be passed around bare. */
export interface WasmCounter {
    name: string;
    setCounting(on: boolean): void;
    stats(): MemoryStats;
    /** `resetMemoryPeak()`: the peak becomes the current live bytes. */
    resetPeak(): void;
    /** Allocate `bytes` through the module's own allocator, which the counter
     *  wraps; the returned function frees them. */
    hold(bytes: number): () => void;
}

/** The counter of the wasm module `name`, from its JS exports and the exports
 *  of its instance (`InitOutput`), whose allocator `hold` goes through.
 *  Throws when the module was built without the counter. */
export function wasmCounter(
    name: string,
    exports: object,
    wasm: {
        __wbindgen_malloc(size: number, align: number): number;
        __wbindgen_free(ptr: number, size: number, align: number): void;
    },
): WasmCounter {
    const m = exports as Partial<MemoryStatsExports>;
    if (typeof m.setMemoryCounting !== 'function'
        || typeof m.memoryStats !== 'function'
        || typeof m.resetMemoryPeak !== 'function') {
        throw new Error(`${name} has no memory-stats exports — rebuild it with the bench's counter (bench-memory-stats)`);
    }
    const { setMemoryCounting, memoryStats, resetMemoryPeak } = m as MemoryStatsExports;
    return {
        name,
        setCounting: (on) => setMemoryCounting(on),
        stats: () => memoryStats(),
        resetPeak: () => resetMemoryPeak(),
        hold: (bytes) => {
            const ptr = wasm.__wbindgen_malloc(bytes, 8);
            return () => wasm.__wbindgen_free(ptr, bytes, 8);
        },
    };
}

/** Bytes as MiB, to one decimal. */
export function mib1(bytes: number): number {
    return Math.round((bytes / 1048576) * 10) / 10;
}

/** The self-check every memory run starts with: `read` must rise by the whole
 *  of a [`SELF_CHECK_BYTES`] block while `hold` holds it. Throws naming the
 *  counter otherwise; the block is freed either way. */
export function checkCounter(name: string, read: () => number, hold: (bytes: number) => () => void): void {
    const before = read();
    const release = hold(SELF_CHECK_BYTES);
    try {
        const rise = read() - before;
        if (rise < SELF_CHECK_BYTES) {
            throw new Error(
                `memory self-check: the ${name} counter rose by ${mib1(rise).toFixed(1)} MiB `
                + 'while an 8 MiB block was held (want at least 8)');
        }
    } finally {
        release();
    }
}

/** A settle ends at the first round that frees less than this. */
export const SETTLE_STEP_BYTES = 1 << 20;
/** The most rounds a settle runs. */
export const SETTLE_MAX_ROUNDS = 10;

/** Settle the live bytes `live` reads: run `round` until one frees less than
 *  [`SETTLE_STEP_BYTES`], compared with the reading before it, and at most
 *  [`SETTLE_MAX_ROUNDS`] times. Returns the rounds run; the first always
 *  runs. */
export async function settleRounds(round: () => Promise<void>, live: () => number): Promise<number> {
    let before = live();
    for (let rounds = 1; ; rounds++) {
        await round();
        const after = live();
        if (before - after < SETTLE_STEP_BYTES || rounds >= SETTLE_MAX_ROUNDS) return rounds;
        before = after;
    }
}

/** One store's two live-byte readings, each taken after settling: `held`
 *  after the reads, before the dispose, and `disposed` after it. */
export interface StoreReadings { held: number; disposed: number }

/** One memory run's figures, in MiB: the store built from the triples, the
 *  store loaded from a file, and the read peak, each null where the run did
 *  not measure it. */
export interface JsMemoryRun { storeBuiltMb: number | null; storeLoadedMb: number | null; peakLiveMb: number | null }

/** A memory run's figures from its readings, in live bytes: each store is
 *  what its dispose released after the reads, and the peak is the highest
 *  probe total above the baseline. A missing input gives null, and every
 *  figure is clamped at 0, since allocator noise can make a difference
 *  negative. */
export function memoryRunFigures(r: {
    baseline: number; built: StoreReadings | null; loaded: StoreReadings | null; probeTotals: number[];
}): JsMemoryRun {
    const mb = (bytes: number) => mib1(Math.max(0, bytes));
    const store = (s: StoreReadings | null) => (s ? mb(s.held - s.disposed) : null);
    return {
        storeBuiltMb: store(r.built),
        storeLoadedMb: store(r.loaded),
        peakLiveMb: r.probeTotals.length > 0 ? mb(Math.max(...r.probeTotals) - r.baseline) : null,
    };
}
