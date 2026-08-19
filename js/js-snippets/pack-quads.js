// Quad packing for bulk ingestion — the write-path counterpart of the lazy
// read model in lazy-rdf.js, implemented as a local wasm-bindgen snippet
// (copied verbatim into the generated pkg; no runtime npm dependency).
//
// Converting RDF/JS quads term-by-term across the wasm boundary costs ~16–20
// Reflect calls per quad, which profiling showed dominating bulk ingestion.
// Instead `fromQuads`/`addQuads` flatten a range of the array host-side into
// one length-prefixed byte buffer and cross the boundary once; the Rust side
// (js/src/ingest.rs) decodes it with a linear cursor.
//
// Layout (little-endian u32 lengths):
//   u32 quadCount, then per quad the four terms s,p,o,g. Per term: 1 tag byte
//   0=NamedNode 1=BlankNode 2=simple Literal 3=lang Literal 4=typed Literal
//   5=DefaultGraph, then (tags 0–4) u32 byteLen + UTF-8 value bytes, then for
//   tag 3 a lang string and for tag 4 a datatype IRI (same u32+bytes shape).
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
    const u32 = (v) => { ensure(4); view.setUint32(pos, v, true); pos += 4; };
    const str = (s) => {
        ensure(4 + s.length * 3); // ≤3 UTF-8 bytes per UTF-16 code unit
        const { written } = ENC.encodeInto(s, buf.subarray(pos + 4));
        view.setUint32(pos, written, true);
        pos += 4 + written;
    };
    const term = (t, i) => {
        switch (t && t.termType) {
            case 'NamedNode': ensure(1); buf[pos++] = 0; str(t.value); return;
            case 'BlankNode': ensure(1); buf[pos++] = 1; str(t.value); return;
            case 'Literal': {
                ensure(1);
                if (t.language) { buf[pos++] = 3; str(t.value); str(t.language); }
                else if (t.datatype && t.datatype.value) { buf[pos++] = 4; str(t.value); str(t.datatype.value); }
                else { buf[pos++] = 2; str(t.value); }
                return;
            }
            case 'DefaultGraph': ensure(1); buf[pos++] = 5; return;
            default:
                // A missing graph position means the default graph (mirrors the
                // pre-packing per-term conversion); anything else is malformed.
                if (t === undefined || t === null) { ensure(1); buf[pos++] = 5; return; }
                throw new Error(`Invalid quad object at index ${i}`);
        }
    };
    u32(end - start);
    for (let i = start; i < end; i++) {
        const q = quads[i];
        if (!q) throw new Error(`Invalid quad object at index ${i}`);
        term(q.subject, i);
        term(q.predicate, i);
        term(q.object, i);
        term(q.graph, i);
    }
    return buf.subarray(0, pos);
}
