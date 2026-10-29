//! Fixtures shared by the engine tests and the `fixtures` example: a deterministic quad set and the stores
//! built from it, one per layout and index set.
#![allow(dead_code)]

use std::collections::BTreeSet;

use futures::stream;
use vortex_rdf_core::io::quads_stream_to_vortex_file;
use vortex_rdf_core::{IndexType, LayoutStrategy, RawQuad};

pub type Quad = [String; 4];

/// Quads over named and blank subjects, IRI, blank and literal objects (plain, language-tagged, typed, with
/// non-BMP characters) and three graphs, the default one included.
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
