// Perf guard for the lazy, zero-copy read model. Run: `node bench/handles.bench.mjs`
// (after `npm run build`). `match`/`getQuads` return LazyQuads whose term strings
// decode only on `.value` access; under the Dictionary layout `.equals` is an
// integer code compare. `matchCodes` is the raw columnar floor.
import { VortexRdfStore, init_panic_hook } from '../entry/node.js';

init_panic_hook();

function makeNquads(nSubjects) {
    const types = ['Person', 'Org', 'Place'];
    let out = '';
    for (let i = 0; i < nSubjects; i++) {
        const s = `<http://ex.org/s${i}>`;
        out += `${s} <http://ex.org/type> <http://ex.org/${types[i % 3]}> .\n`;
        out += `${s} <http://ex.org/name> "name ${i}" .\n`;
        out += `${s} <http://ex.org/age> "${i % 100}"^^<http://www.w3.org/2001/XMLSchema#integer> .\n`;
        out += `${s} <http://ex.org/knows> <http://ex.org/s${(i + 1) % nSubjects}> .\n`;
        out += `${s} <http://ex.org/homepage> <http://ex.org/h${i % 7}> .\n`;
    }
    return out;
}

async function timeAsync(label, iters, fn) {
    for (let i = 0; i < 2; i++) await fn();
    const t0 = performance.now();
    let sink = 0;
    for (let i = 0; i < iters; i++) sink += await fn();
    console.log(`  ${label.padEnd(46)} ${((performance.now() - t0) / iters).toFixed(2)} ms/iter  (checksum ${sink})`);
}

function time(label, iters, fn) {
    for (let i = 0; i < 2; i++) fn();
    const t0 = performance.now();
    let sink = 0;
    for (let i = 0; i < iters; i++) sink += fn();
    console.log(`  ${label.padEnd(46)} ${((performance.now() - t0) / iters).toFixed(2)} ms/iter  (checksum ${sink})`);
}

async function main() {
    const store = await VortexRdfStore.fromString(makeNquads(20_000), 'nquads', { layout: 'Dictionary' });
    console.log(`\nDataset: ${await store.size()} quads, Dictionary layout\n`);

    const ITERS = 10;
    const target = { termType: 'NamedNode', value: 'http://ex.org/Person' };

    console.log('A. Produce the full result set:');
    await timeAsync('getQuads() [LazyQuad[]]', ITERS, async () => (await store.getQuads(null, null, null, null)).length);
    await timeAsync('matchCodes() [raw Uint32Array columns]', ITERS, async () => (await store.matchCodes(null, null, null, null)).length);

    const quads = await store.getQuads(null, null, null, null);

    console.log('\nB. Filter via .equals (count objects === <Person>):');
    time('LazyQuad object.equals(target)', ITERS, () => {
        let n = 0;
        for (const q of quads) if (q.object.equals(target)) n++;
        return n;
    });

    console.log('\nC. Read every term value (lazy decode, interned):');
    time('sum of .value lengths', ITERS, () => {
        let sum = 0;
        for (const q of quads) sum += q.subject.value.length + q.predicate.value.length + q.object.value.length + q.graph.value.length;
        return sum;
    });

    console.log('\nD. Bound-pattern match (predicate pushed down):');
    await timeAsync('getQuads(?, type, ?, ?).length', ITERS, async () =>
        (await store.getQuads(null, { termType: 'NamedNode', value: 'http://ex.org/type' }, null, null)).length);
    console.log();
}

main().catch((e) => { console.error(e); process.exit(1); });
