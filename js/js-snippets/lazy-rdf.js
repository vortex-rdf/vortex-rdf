// Lazy, zero-copy RDF/JS read model for the wasm bindings, implemented as a
// local wasm-bindgen snippet (copied verbatim into the generated pkg; no runtime
// npm dependency).
//
// `match()`/`getQuads()` hand back LazyQuads over columnar term data: `getQuads`
// over one payload of the whole match, `match()` over a payload of at most
// 8,192 rows at a time, which its stream pulls from a `QuadChunks` handle as
// its quads are taken (`makeLazyQuadStream` below). A term's string is decoded
// from UTF-8 bytes only when `.value`/`.termType` is read (and then interned),
// never eagerly and never per-term across the wasm boundary.
// Two column backings:
//   - Dictionary layout: a Float64Array of codes (exact integers) + a shared LazyDict over an
//     immutable snapshot of the store's term dictionary, decoding codes on
//     demand. `.equals` between terms of the same store is an integer code
//     compare, so it never decodes at all.
//   - Default/TypedObject: {offsets, bytes} of the matched rows' N-Triples term
//     strings; `.equals` falls back to value/termType compare.
//
// LazyQuad/LazyTerm satisfy the RDF/JS Quad/Term interface via getters.
//
// The code reads that answer in pieces (`matchCodeChunks`, `matchCodesMany`)
// hand back a wasm iterator handle wrapped by `makeHandleIterable` below.

const DEC = new TextDecoder();

// ── N-Triples term parsing ───────────────────────────────────────────────────
const XSD_STRING = 'http://www.w3.org/2001/XMLSchema#string';
const RDF_LANG_STRING = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#langString';

function unescape(lex) {
    if (lex.indexOf('\\') === -1) return lex;
    let out = '';
    for (let i = 0; i < lex.length; i++) {
        const ch = lex[i];
        if (ch !== '\\') { out += ch; continue; }
        const n = lex[++i];
        if (n === 't') out += '\t';
        else if (n === 'n') out += '\n';
        else if (n === 'r') out += '\r';
        else if (n === 'b') out += '\b';
        else if (n === 'f') out += '\f';
        else if (n === '"') out += '"';
        else if (n === "'") out += "'";
        else if (n === '\\') out += '\\';
        else if (n === 'u') { out += String.fromCharCode(parseInt(lex.slice(i + 1, i + 5), 16)); i += 4; }
        else if (n === 'U') { out += String.fromCodePoint(parseInt(lex.slice(i + 1, i + 9), 16)); i += 8; }
        else out += n;
    }
    return out;
}

/** Parse a canonical N-Triples term into {termType, value, language?, datatype?}. */
function parseNt(nt) {
    if (nt === '') return { termType: 'DefaultGraph', value: '' };
    const c = nt[0];
    if (c === '<') return { termType: 'NamedNode', value: nt.slice(1, -1) };
    if (c === '_') return { termType: 'BlankNode', value: nt.slice(2) };
    // Literal: "lex" | "lex"@lang | "lex"^^<datatype>  (the closing quote is the
    // last '"' — internal quotes are backslash-escaped, and neither @lang nor
    // ^^<...> contains a quote).
    const end = nt.lastIndexOf('"');
    const value = unescape(nt.slice(1, end));
    const rest = nt.slice(end + 1);
    if (rest.charCodeAt(0) === 64 /* @ */) {
        return { termType: 'Literal', value, language: rest.slice(1), datatype: namedNode(RDF_LANG_STRING) };
    }
    if (rest.startsWith('^^<')) {
        return { termType: 'Literal', value, language: '', datatype: namedNode(rest.slice(3, -1)) };
    }
    return { termType: 'Literal', value, language: '', datatype: namedNode(XSD_STRING) };
}

function namedNode(value) {
    return {
        termType: 'NamedNode',
        value,
        equals(other) { return !!other && other.termType === 'NamedNode' && other.value === value; },
    };
}

// ── Dictionary decoded on demand, interned per code ──────────────────────────
// Wraps a `TermDict` (an immutable snapshot of the store's dictionary).
// Decoding is on demand and memoized per code, so a query pays one boundary
// crossing per distinct code it observes; a query that only counts rows, or
// compares terms through the integer fast path below, never crosses.
class LazyDict {
    constructor(dict) {
        this.dict = dict;
        this.cache = new Map();
    }

    decode(code) {
        let s = this.cache.get(code);
        if (s === undefined) {
            s = this.dict.decode(code);
            this.cache.set(code, s);
        }
        return s;
    }
}

export function makeDictView(dict) {
    return new LazyDict(dict);
}

// ── A single column's backing: code+dict, or packed term bytes ───────────────
// `col` is either { dict: LazyDict, codes: Float64Array } or { offsets, bytes }.
function ntAt(col, i) {
    return col.dict
        ? col.dict.decode(col.codes[i])
        : DEC.decode(col.bytes.subarray(col.offsets[i], col.offsets[i + 1]));
}

class LazyTerm {
    constructor(col, i) {
        this.$col = col;
        this.$i = i;
        this.$p = undefined;
    }
    get $parsed() { return this.$p ?? (this.$p = parseNt(ntAt(this.$col, this.$i))); }
    get termType() { return this.$parsed.termType; }
    get value() { return this.$parsed.value; }
    get language() { return this.$parsed.language ?? ''; }
    get datatype() { return this.$parsed.datatype; }
    equals(other) {
        // Fast path: two code-backed terms sharing the same dictionary compare
        // as integers (dictionary codes are a global identity for the term).
        const col = this.$col;
        if (other && other.$col && col.dict && other.$col.dict === col.dict && typeof other.$i === 'number') {
            return other.$col.codes[other.$i] === col.codes[this.$i];
        }
        if (!other || other.termType !== this.termType || other.value !== this.value) return false;
        if (this.termType !== 'Literal') return true;
        return (this.language || '') === (other.language || '')
            && (!this.datatype ? !other.datatype : this.datatype.equals(other.datatype));
    }
}

class LazyQuad {
    constructor(cols, i) { this.$cols = cols; this.$i = i; }
    get subject() { return new LazyTerm(this.$cols.s, this.$i); }
    get predicate() { return new LazyTerm(this.$cols.p, this.$i); }
    get object() { return new LazyTerm(this.$cols.o, this.$i); }
    get graph() { return new LazyTerm(this.$cols.g, this.$i); }
    equals(other) {
        return !!other
            && this.subject.equals(other.subject) && this.predicate.equals(other.predicate)
            && this.object.equals(other.object) && this.graph.equals(other.graph);
    }
}

// Normalize a Rust-built payload into four self-describing column accessors.
// A code payload is `matchCodes`' columns plus the store's LazyDict as `dict`;
// a term payload holds `{offsets, bytes}` per column and no `dict`.
function normalize(payload) {
    const col = payload.dict
        ? (name) => ({ dict: payload.dict, codes: payload[name] })
        : (name) => ({ offsets: payload[name].offsets, bytes: payload[name].bytes });
    return { s: col('s'), p: col('p'), o: col('o'), g: col('g') };
}

export function buildLazyQuads(payload) {
    const cols = normalize(payload);
    const out = new Array(payload.length);
    for (let i = 0; i < payload.length; i++) out[i] = new LazyQuad(cols, i);
    return out;
}

export function makeLazyQuadStream(chunks) {
    return new QuadStream(chunks);
}

const NO_QUADS = [];

// ── Minimal RDF/JS Stream over a QuadChunks handle (data/end/error, read(),
// Symbol.asyncIterator). ─────────────────────────────────────────────────────
// The buffer holds one chunk's LazyQuads; only once it is empty is the next
// chunk pulled (`_pull`), by a `data` drain, `read()` or the async iterator.
// The handle's end ends the stream and a throw fails it, each emitting `end`
// or `error` in a microtask; `destroy()` stops it without either. All three
// free the handle and drop the buffered chunk and `_store`, the store it reads
// (the package entries set it), and nothing is pulled after them.
class QuadStream {
    constructor(chunks) {
        this._chunks = chunks;
        this._store = undefined;
        this._listeners = { data: [], end: [], error: [], readable: [] };
        this._buffer = NO_QUADS;
        this._pos = 0;
        this._started = false;
        this._draining = false;
        // Ended, failed or destroyed.
        this._done = false;
        this._ended = false;
        this._failed = false;
        this._error = undefined;
        // `match` returns first: listeners attached now see the events.
        queueMicrotask(() => this._start());
    }
    on(event, fn) {
        (this._listeners[event] ||= []).push(fn);
        if (event === 'data') { if (this._started) this._drain(); }
        else if (event === 'readable' && this._started && !this._done) fn();
        return this;
    }
    once(event, fn) {
        const wrapper = (...args) => { this.off(event, wrapper); fn(...args); };
        return this.on(event, wrapper);
    }
    off(event, fn) {
        const arr = this._listeners[event];
        if (arr) { const i = arr.indexOf(fn); if (i !== -1) arr.splice(i, 1); }
        return this;
    }
    addListener(event, fn) { return this.on(event, fn); }
    removeListener(event, fn) { return this.off(event, fn); }
    emit(event, ...args) { this._emit(event, ...args); return true; }
    // The next quad, pulling a chunk when the buffer is empty (before the
    // stream has started too); `null` once the stream is done.
    read() {
        for (;;) {
            if (this._done) return null;
            if (this._pos < this._buffer.length) return this._buffer[this._pos++];
            this._pull();
        }
    }
    async *[Symbol.asyncIterator]() {
        try {
            for (;;) {
                if (this._done) {
                    if (this._failed) throw this._error;
                    return;
                }
                if (this._pos < this._buffer.length) yield this._buffer[this._pos++];
                else this._pull();
            }
        } finally {
            // Left early (a `break`, a throw), or failed: free the handle.
            if (!this._ended) this.destroy();
        }
    }
    // Stop the stream: done, without `end` or `error`. Returns the stream, as
    // Node's `Readable#destroy` does.
    destroy() {
        this._done = true;
        this._release();
        return this;
    }
    _emit(event, ...args) {
        const arr = this._listeners[event];
        if (arr) for (const fn of arr.slice()) fn(...args);
    }
    _start() {
        this._started = true;
        if (this._done) return;
        this._emit('readable');
        if (this._listeners.data.length > 0) this._drain();
    }
    // Emit the quads as `data`, a chunk pulled each time the buffer empties,
    // until the stream is done; a call made while one drains returns.
    _drain() {
        if (this._draining) return;
        this._draining = true;
        try {
            for (;;) {
                if (this._done) return;
                if (this._pos < this._buffer.length) this._emit('data', this._buffer[this._pos++]);
                else this._pull();
            }
        } finally {
            this._draining = false;
        }
    }
    // Refill the empty buffer with the handle's next chunk, or end or fail
    // the stream.
    _pull() {
        if (this._done) return;
        let payload;
        try {
            payload = this._chunks.next();
        } catch (error) {
            this._failed = true;
            this._error = error;
            this._finish('error', error);
            return;
        }
        if (payload === undefined) {
            this._ended = true;
            this._finish('end');
            return;
        }
        this._buffer = buildLazyQuads(payload);
        this._pos = 0;
    }
    _finish(event, ...args) {
        this._done = true;
        this._release();
        queueMicrotask(() => this._emit(event, ...args));
    }
    // Free the handle, once, and drop the buffered chunk and the store it reads.
    _release() {
        const chunks = this._chunks;
        this._chunks = undefined;
        this._store = undefined;
        this._buffer = NO_QUADS;
        this._pos = 0;
        chunks?.free();
    }
}

// ── Iterables over a wasm iterator handle ────────────────────────────────────
// A `CodeChunks` or `ProbeAnswers` handle answers each `next()` with its next
// item, or `undefined` once there is none. HandleIterator is the synchronous
// iterator (and its own Iterable) over one handle, and is Disposable: the
// handle is freed at the end, at the first error, on `return()` (a `for…of`
// left early) and on `[Symbol.dispose]` (`using`). An aborted `signal` ends
// the iteration at the next `next()`, before the handle is pulled; with no
// handle the iteration has already finished. `_store` holds the store the
// handle reads (the package entries set it), so the store stays reachable
// until the handle is freed.
class HandleIterator {
    constructor(handle, signal) {
        this._handle = handle;
        this._signal = signal;
        this._store = undefined;
    }
    [Symbol.iterator]() { return this; }
    next() {
        const handle = this._handle;
        if (handle === undefined || this._signal?.aborted) return this.return();
        let value;
        try {
            value = handle.next();
        } catch (error) {
            this._free();
            throw error;
        }
        return value === undefined ? this.return() : { done: false, value };
    }
    return(value) {
        this._free();
        return { done: true, value };
    }
    _free() {
        const handle = this._handle;
        this._handle = undefined;
        this._store = undefined;
        handle?.free();
    }
}
if (Symbol.dispose) HandleIterator.prototype[Symbol.dispose] = HandleIterator.prototype._free;

export function makeHandleIterable(handle, signal) {
    return new HandleIterator(handle, signal);
}
