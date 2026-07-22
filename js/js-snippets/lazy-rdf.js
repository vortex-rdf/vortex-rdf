// Lazy, zero-copy RDF/JS read model for the wasm bindings, implemented as a
// local wasm-bindgen snippet (copied verbatim into the generated pkg; no runtime
// npm dependency).
//
// `match()`/`getQuads()` hand back LazyQuads over columnar term data rather than
// building eager Quad objects. A term's string is decoded from UTF-8 bytes only
// when `.value`/`.termType` is read (and then interned), never eagerly and never
// per-term across the wasm boundary. Two column backings:
//   - Dictionary layout: a Uint32Array of codes + a shared LazyDict (the term
//     dictionary shipped once). `.equals` between terms of the same store is an
//     integer code compare.
//   - Default/TypedObject: {offsets, bytes} of the matched rows' N-Triples term
//     strings; `.equals` falls back to value/termType compare.
//
// LazyQuad/LazyTerm satisfy the RDF/JS Quad/Term interface via getters.

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

// ── Dictionary shipped once; decode interned per code ────────────────────────
class LazyDict {
    constructor(offsets, bytes) { 
        this.offsets = offsets; 
        this.bytes = bytes; 
        this.cache = new Map(); 
    }

    decode(code) {
        let s = this.cache.get(code);
        if (s === undefined) {
            s = DEC.decode(
                this.bytes.subarray(this.offsets[code], this.offsets[code + 1])
            );
            this.cache.set(code, s);
        }
        return s;
    }
}

export function makeDictView(offsets, bytes) { return new LazyDict(offsets, bytes); }

// ── A single column's backing: code+dict, or packed term bytes ───────────────
// `col` is either { dict: LazyDict, codes: Uint32Array } or { offsets, bytes }.
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
function normalize(payload) {
    const col = payload.kind === 'code'
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

export function makeLazyQuadStream(payloadPromise) {
    return new QuadStream(Promise.resolve(payloadPromise).then(buildLazyQuads));
}

// ── Minimal RDF/JS Stream over a Promise<item[]> (data/end/error, read(),
// Symbol.asyncIterator). Same behavior as the former quad-stream.js. ─────────
class QuadStream {
    constructor(itemsPromise) {
        this._promise = Promise.resolve(itemsPromise);
        this._listeners = { data: [], end: [], error: [], readable: [] };
        this._buffer = null;
        this._pos = 0;
        this._flowing = false;
        this._done = false;
        this._promise.then(
            (items) => {
                this._buffer = Array.isArray(items) ? items : [...items];
                this._emit('readable');
                if (this._flowing) this._drain();
                else this._endIfDrained();
            },
            (err) => { this._done = true; this._emit('error', err); },
        );
    }
    on(event, fn) {
        (this._listeners[event] ||= []).push(fn);
        if (event === 'data') { this._flowing = true; if (this._buffer) this._drain(); }
        else if (event === 'readable' && this._buffer && this._pos < this._buffer.length) fn();
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
    read() {
        if (this._buffer && this._pos < this._buffer.length) {
            const item = this._buffer[this._pos++];
            this._endIfDrained();
            return item;
        }
        return null;
    }
    async *[Symbol.asyncIterator]() {
        const items = await this._promise;
        yield* items;
    }
    _emit(event, ...args) {
        const arr = this._listeners[event];
        if (arr) for (const fn of arr.slice()) fn(...args);
    }
    _drain() {
        if (this._done || !this._buffer) return;
        while (this._pos < this._buffer.length) this._emit('data', this._buffer[this._pos++]);
        this._endIfDrained();
    }
    _endIfDrained() {
        if (!this._done && this._buffer && this._pos >= this._buffer.length) {
            this._done = true;
            Promise.resolve().then(() => this._emit('end'));
        }
    }
}
