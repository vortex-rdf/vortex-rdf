// Quad packing for bulk ingestion — the write-path counterpart of the lazy
// read model in lazy-rdf.js, implemented as a local wasm-bindgen snippet
// (copied verbatim into the generated pkg; no runtime npm dependency).
//
// `fromQuads`/`addQuads` flatten a range of the array host-side into one
// length-prefixed byte buffer: one boundary crossing per chunk instead of one
// property read per term field. The Rust side (js/src/ingest.rs) decodes it
// with a linear cursor.
//
// Layout (little-endian u32 lengths):
//   u32 quadCount, then per quad the four terms s,p,o,g. Per term: 1 tag byte
//   0=NamedNode 1=BlankNode 2=simple Literal 3=lang Literal 4=typed Literal
//   5=DefaultGraph, then (tags 0–4) u32 byteLen + UTF-8 value bytes, then for
//   tag 3 a lang string and for tag 4 a datatype IRI (same u32+bytes shape).
// The tags are the `TermKind` discriminants in js/src/terms.rs, whose
// reflection reader (single quads, streams) classifies a term by the rule
// `term` applies here.
const ENC = new TextEncoder();

export function packQuads(quads, start = 0, end = quads.length) {
    let buf = new Uint8Array(1 << 16);
    let view = new DataView(buf.buffer);
    let pos = 0;
    const ensure = (n) => {
        if (pos + n <= buf.length) return;
        let cap = buf.length * 2;
        while (cap < pos + n) cap *= 2;
        const nb = new Uint8Array(cap);
        nb.set(buf.subarray(0, pos));
        buf = nb;
        view = new DataView(buf.buffer);
    };
    const invalid = (i) => new Error(`Invalid quad object at index ${i}`);
    const u32 = (v) => { ensure(4); view.setUint32(pos, v, true); pos += 4; };
    // A value, language or datatype IRI that is not a string makes the quad
    // malformed; TextEncoder would throw its own TypeError for it in Node and
    // stringify it in browsers.
    const str = (s, i) => {
        if (typeof s !== 'string') throw invalid(i);
        ensure(4 + s.length * 3); // ≤3 UTF-8 bytes per UTF-16 code unit
        const { written } = ENC.encodeInto(s, buf.subarray(pos + 4));
        view.setUint32(pos, written, true);
        pos += 4 + written;
    };
    const term = (t, i) => {
        switch (t == null ? undefined : t.termType) {
            case 'NamedNode': ensure(1); buf[pos++] = 0; str(t.value, i); return;
            case 'BlankNode': ensure(1); buf[pos++] = 1; str(t.value, i); return;
            case 'Literal': {
                ensure(1);
                if (t.language) { buf[pos++] = 3; str(t.value, i); str(t.language, i); return; }
                // A datatype is a NamedNode object or absent; a truthy
                // non-object, such as a bare IRI string, is malformed.
                const dt = t.datatype;
                if (dt && typeof dt !== 'object') throw invalid(i);
                const iri = dt && dt.value;
                if (iri) { buf[pos++] = 4; str(t.value, i); str(iri, i); }
                else { buf[pos++] = 2; str(t.value, i); }
                return;
            }
            case 'DefaultGraph': ensure(1); buf[pos++] = 5; return;
            default:
                // A missing graph position means the default graph; anything
                // else is malformed.
                if (t === undefined || t === null) { ensure(1); buf[pos++] = 5; return; }
                throw invalid(i);
        }
    };
    u32(end - start);
    for (let i = start; i < end; i++) {
        const q = quads[i];
        if (!q) throw invalid(i);
        term(q.subject, i);
        term(q.predicate, i);
        term(q.object, i);
        term(q.graph, i);
    }
    return buf.subarray(0, pos);
}
