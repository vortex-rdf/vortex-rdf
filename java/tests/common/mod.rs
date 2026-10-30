//! Fixtures shared by the engine tests and the `fixtures` example: a deterministic quad set and the stores
//! built from it, one per layout and index set.
#![allow(dead_code)]

use std::cell::RefCell;
use std::collections::{BTreeSet, HashMap};

use futures::stream;
use vortex_rdf_core::io::quads_stream_to_vortex_file;
use vortex_rdf_core::{IndexType, LayoutStrategy, Probe, RawQuad, VortexRdfStore};
use vortex_rdf_jni::cursor::QuadCursor;

pub type Quad = [String; 4];

/// Spellings that are hard to carry across JNI: escapes (tab, quote, backslash, NUL and another control
/// character), characters outside the BMP in a literal and in an IRI, BMP characters of two and three UTF-8
/// bytes, a zero-width space, a region language tag and a blank object.
const SPELLINGS: &[[&str; 4]] = &[
    [
        "<http://example.org/s1>",
        "<http://example.org/p0>",
        "\"tab\\there \\\"q\\\" back\\\\slash\"",
        "",
    ],
    [
        "<http://example.org/s2>",
        "<http://example.org/p0>",
        "\"astral \u{1F600} \u{10348} end\"@en-gb",
        "<http://example.org/g1>",
    ],
    [
        "<http://example.org/s3>",
        "<http://example.org/p1>",
        "\"bmp é ß 中文 \u{200B}\"",
        "",
    ],
    [
        "<http://example.org/é/s7>",
        "<http://example.org/p1>",
        "_:b1",
        "<http://example.org/g/\u{1F600}>",
    ],
    [
        "<http://example.org/s4>",
        "<http://example.org/p2>",
        "\"nul \\u0000 ctl \\u0001\"",
        "<http://example.org/g2>",
    ],
];

/// `count` quads over named and blank subjects, IRI, blank and literal objects (plain, language-tagged,
/// typed, with non-BMP characters) and three graphs, the default one included, then the [`SPELLINGS`].
pub fn quads(count: usize) -> Vec<Quad> {
    let mut seen = BTreeSet::new();
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    let mut next = |n: u64| {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (state >> 33) % n
    };
    while seen.len() < count {
        let s = match next(8) {
            0 => format!("_:b{}", next(4)),
            _ => format!("<http://example.org/s{}>", next(30)),
        };
        let p = format!("<http://example.org/p{}>", next(5));
        let o = match next(6) {
            0 => format!("<http://example.org/s{}>", next(30)),
            1 => format!("\"plain {}\"", next(20)),
            2 => format!("\"tagged {}\"@en", next(10)),
            3 => format!(
                "\"{}\"^^<http://www.w3.org/2001/XMLSchema#integer>",
                next(15)
            ),
            4 => format!("\"emoji \u{1F600} {}\"", next(6)),
            _ => format!("_:b{}", next(4)),
        };
        let g = match next(3) {
            0 => String::new(),
            1 => "<http://example.org/g1>".to_string(),
            _ => "<http://example.org/g2>".to_string(),
        };
        let q = RawQuad::canonical(&s, &p, &o, &g).expect("canonical quad");
        seen.insert([q.s, q.p, q.o, q.g]);
    }
    for [s, p, o, g] in SPELLINGS {
        let q = RawQuad::canonical(s, p, o, g).expect("canonical quad");
        seen.insert([q.s, q.p, q.o, q.g]);
    }
    seen.into_iter().collect()
}

pub struct Built {
    /// The store's name, and its file's: `<name>.vortex`.
    pub name: &'static str,
    pub layout: LayoutStrategy,
    pub indexes: Vec<IndexType>,
}

pub fn builds() -> Vec<Built> {
    vec![
        Built {
            name: "dictionary",
            layout: LayoutStrategy::Dictionary,
            indexes: vec![],
        },
        Built {
            name: "copy",
            layout: LayoutStrategy::Dictionary,
            indexes: vec![IndexType::SecondaryByCopy],
        },
        Built {
            name: "reference",
            layout: LayoutStrategy::Dictionary,
            indexes: vec![IndexType::SecondaryByReference],
        },
        Built {
            name: "default",
            layout: LayoutStrategy::Default,
            indexes: vec![],
        },
        Built {
            name: "typed",
            layout: LayoutStrategy::TypedObject,
            indexes: vec![],
        },
    ]
}

/// Writes the store and returns its path.
pub fn write(dir: &std::path::Path, built: &Built, quads: &[Quad]) -> String {
    let path = dir.join(format!("{}.vortex", built.name));
    let raw: Vec<_> = quads
        .iter()
        .map(|q| {
            Ok(RawQuad {
                s: q[0].clone(),
                p: q[1].clone(),
                o: q[2].clone(),
                g: q[3].clone(),
            })
        })
        .collect();
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(quads_stream_to_vortex_file(
            stream::iter(raw),
            &path,
            built.layout,
            built.indexes.clone(),
        ))
        .expect("write the store");
    path.to_str().unwrap().to_string()
}

/// The reference reads' runtime and the stores they opened, by path and residency.
struct Reference {
    runtime: tokio::runtime::Runtime,
    opened: RefCell<HashMap<(String, bool), VortexRdfStore>>,
}

thread_local! {
    /// One [`Reference`] per test thread: opening a store costs far more than reading a small one, and each
    /// test writes its paths once, in its own temporary directory.
    static REFERENCE: Reference = Reference {
        runtime: tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap(),
        opened: RefCell::new(HashMap::new()),
    };
}

/// The rows a pattern matches, `limit` after `offset`, read one `String` per term as the bindings read them
/// before the packed transfer: a Dictionary store's matched codes decoded a column at a time, the matched
/// quads' terms otherwise. The tests' reference for the rows the engine hands over, their order included.
pub fn reference_quads(
    path: &str,
    in_memory: bool,
    pattern: [Option<&str>; 4],
    offset: u64,
    limit: Option<usize>,
) -> Vec<Quad> {
    REFERENCE.with(|Reference { runtime, opened }| {
        let store = opened
            .borrow_mut()
            .entry((path.to_string(), in_memory))
            .or_insert_with(|| {
                runtime
                    .block_on(async {
                        if in_memory {
                            VortexRdfStore::from_file_in_memory(path).await
                        } else {
                            VortexRdfStore::from_file(path).await
                        }
                    })
                    .expect("open the store")
            })
            .clone();
        runtime.block_on(async {
            let [s, p, o, g] = pattern;
            let probe = Probe::parse(s, p, o, g)
                .expect("parse the pattern")
                .window(offset as usize, limit);
            let view = store.run_probe(&probe).await.expect("run the probe");
            if let Some(dict) = store.dict_reader()
                && let Some(columns) = view.code_columns_gathered().await.expect("gather codes")
            {
                let mut decoded = Vec::with_capacity(4);
                for column in &columns {
                    decoded.push(dict.decode_many(column).await.expect("decode a column"));
                }
                return (0..decoded[0].len())
                    .map(|row| {
                        std::array::from_fn(|i| {
                            decoded[i][row]
                                .clone()
                                .expect("a matched row holds a code outside the dictionary")
                        })
                    })
                    .collect();
            }
            view.shared_quads_vec()
                .await
                .expect("read the quads")
                .iter()
                .map(|q| {
                    [
                        q.s.to_string(),
                        q.p.to_string(),
                        q.o.to_string(),
                        q.g.to_string(),
                    ]
                })
                .collect()
        })
    })
}

/// The rows a cursor sends, each chunk's positions read through `Payload::term`, four to a row; checks that
/// every chunk holds whole rows and that the rows add up to the cursor's count.
pub fn drain(mut cursor: QuadCursor) -> Vec<Quad> {
    let mut rows = Vec::with_capacity(cursor.rows());
    for payload in &mut cursor {
        let payload = payload.expect("read a chunk");
        let positions = payload.positions();
        assert!(positions > 0 && positions % 4 == 0, "{positions} positions");
        for row in 0..positions / 4 {
            rows.push(std::array::from_fn(|c| {
                payload
                    .term(4 * row + c)
                    .expect("a row position holds a term")
                    .to_string()
            }));
        }
    }
    assert_eq!(rows.len(), cursor.rows(), "the chunks hold the read's rows");
    rows
}
