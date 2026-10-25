//! The out-of-core build's term interner: every distinct term of a build
//! held once under a provisional `u32` id, handed out in arrival order, with
//! the term behind an id at hand, so a quad kept as four ids sorts in its
//! terms' order ([`TermInterner::cmp_quads`]). [`TermInterner::finish`] ranks
//! the terms in byte order, the order of dictionary codes and of
//! [`RawQuad`]'s derived `Ord`, and maps every id to its term's rank.
//!
//! Terms are interned by the exact spelling given, so the input must be
//! canonical (as a builder's is): equal ids are equal terms, and a quad's
//! four ids name it.

use std::cmp::Ordering;
use std::hash::BuildHasher;

use hashbrown::{DefaultHashBuilder, HashTable};

use crate::error::{Result, VortexRdfError};
use crate::store::RawQuad;

/// The most distinct terms a build may hold: the term dictionary's cap
/// (`TermDictionary::from_sorted_column`), so every id and rank fits a `u32`.
const TERM_CAP: usize = i32::MAX as usize;

/// [`TERM_CAP`], or the lower cap of the tests' `TermCap` hook, which lets a
/// few terms reach the refusal.
fn term_cap() -> usize {
    #[cfg(test)]
    if let Some(cap) = crate::store::test_hooks::term_cap() {
        return cap;
    }
    TERM_CAP
}

/// Every distinct term of a build once, by provisional id: `terms[id]` is the
/// term `id` names, and `index` finds the id of a term by hashing it.
pub(crate) struct TermInterner {
    terms: Vec<Box<str>>,
    /// The ids, keyed by the hash of the term each names.
    index: HashTable<u32>,
    hasher: DefaultHashBuilder,
}

/// A build's terms ranked: `sorted[rank]` is the term of that rank in byte
/// order, and `rank_of[id]` the rank of the term provisional id `id` named.
pub(crate) struct FrozenTerms {
    pub(crate) sorted: Vec<Box<str>>,
    pub(crate) rank_of: Vec<u32>,
}

impl TermInterner {
    pub(crate) fn new() -> Self {
        Self {
            terms: Vec::new(),
            index: HashTable::new(),
            hasher: DefaultHashBuilder::default(),
        }
    }

    /// The id of `term`: the one an equal term already has, else the next
    /// id, `term` moved in. A new term past the cap is a `Serialization`
    /// error and is not held.
    pub(crate) fn intern(&mut self, term: String) -> Result<u32> {
        let hash = self.hasher.hash_one(term.as_str());
        let terms = &self.terms;
        if let Some(&id) = self.index.find(hash, |&id| *terms[id as usize] == *term) {
            return Ok(id);
        }
        let count = self.terms.len() + 1;
        let cap = term_cap();
        if count > cap {
            return Err(VortexRdfError::Serialization(format!(
                "Dictionary of {count} unique terms exceeds the supported maximum ({cap})"
            )));
        }
        // Below the cap, at most `i32::MAX`: the id fits a `u32`.
        let id = self.terms.len() as u32;
        // The term goes in first, so the table's rehash, which hashes the
        // terms behind the ids it holds, can read every id, the new one too.
        self.terms.push(term.into_boxed_str());
        let (terms, hasher) = (&self.terms, &self.hasher);
        self.index
            .insert_unique(hash, id, |&id| hasher.hash_one(&*terms[id as usize]));
        Ok(id)
    }

    /// The ids of `quad`'s terms, in `[s, p, o, g]` order.
    pub(crate) fn intern_quad(&mut self, quad: RawQuad) -> Result<[u32; 4]> {
        let RawQuad { s, p, o, g } = quad;
        Ok([
            self.intern(s)?,
            self.intern(p)?,
            self.intern(o)?,
            self.intern(g)?,
        ])
    }

    /// How many distinct terms are held.
    pub(crate) fn len(&self) -> usize {
        self.terms.len()
    }

    /// Two quads of ids in their terms' order: `(s, p, o, g)`, each term in
    /// byte order, as [`RawQuad`]'s `Ord` compares them. A position whose ids
    /// are equal is equal without reading its term.
    pub(crate) fn cmp_quads(&self, a: &[u32; 4], b: &[u32; 4]) -> Ordering {
        for (x, y) in a.iter().zip(b) {
            if x != y {
                // Distinct ids name distinct terms: never `Equal`.
                return self.terms[*x as usize].cmp(&self.terms[*y as usize]);
            }
        }
        Ordering::Equal
    }

    /// The terms in byte order beside each id's rank. Each term is moved
    /// into its place, never copied, and the id index is freed first.
    pub(crate) fn finish(self) -> FrozenTerms {
        let Self {
            mut terms, index, ..
        } = self;
        drop(index);
        // Below the cap, so every id and rank fits a `u32`.
        let mut order: Vec<u32> = (0..terms.len() as u32).collect();
        order.sort_unstable_by(|&a, &b| terms[a as usize].cmp(&terms[b as usize]));
        let mut rank_of = vec![0u32; terms.len()];
        let mut sorted = Vec::with_capacity(terms.len());
        for (rank, &id) in order.iter().enumerate() {
            rank_of[id as usize] = rank as u32;
            sorted.push(std::mem::take(&mut terms[id as usize]));
        }
        FrozenTerms { sorted, rank_of }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every term kind, a term that is a prefix of another, the empty
    /// default graph and a non-ASCII byte, so a comparison that is not plain
    /// byte order shows.
    const POOL: [&str; 9] = [
        "",
        "<a>",
        "<ab>",
        "<b>",
        "_:b0",
        "\"z\"",
        "\"é\"",
        "\"x\"@en",
        "\"1\"^^<http://www.w3.org/2001/XMLSchema#integer>",
    ];

    /// 300 quads drawn from [`POOL`] by a fixed linear congruential sequence,
    /// repeats included.
    fn pool_quads() -> Vec<RawQuad> {
        let mut state = 12_345u64;
        let mut pick = || {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            POOL[((state >> 33) % POOL.len() as u64) as usize].to_owned()
        };
        (0..300)
            .map(|_| RawQuad {
                s: pick(),
                p: pick(),
                o: pick(),
                g: pick(),
            })
            .collect()
    }

    #[test]
    fn interning_the_same_spelling_gives_the_same_id() {
        let mut terms = TermInterner::new();
        let a = terms.intern("<http://e.org/a>".into()).unwrap();
        let b = terms.intern("\"b\"@en".into()).unwrap();
        assert_eq!(terms.intern("<http://e.org/a>".into()).unwrap(), a);
        assert_ne!(a, b);
        assert_eq!(terms.len(), 2);
    }

    #[test]
    fn cmp_quads_agrees_with_raw_quad_order() {
        let quads = pool_quads();
        let mut terms = TermInterner::new();
        let ids: Vec<[u32; 4]> = quads
            .iter()
            .map(|q| terms.intern_quad(q.clone()).unwrap())
            .collect();
        for (i, a) in quads.iter().enumerate() {
            for (j, b) in quads.iter().enumerate() {
                assert_eq!(
                    terms.cmp_quads(&ids[i], &ids[j]),
                    a.cmp(b),
                    "quad {i} against quad {j}"
                );
            }
        }
    }

    #[test]
    fn finish_ranks_terms_in_byte_order() {
        // Interned in an order that is not byte order.
        let arrival = ["<b>", "\"é\"", "", "<ab>", "<b>", "_:b0", "<a>", ""];
        let mut terms = TermInterner::new();
        let ids: Vec<u32> = arrival
            .iter()
            .map(|t| terms.intern((*t).to_owned()).unwrap())
            .collect();
        let frozen = terms.finish();
        let sorted: Vec<&str> = frozen.sorted.iter().map(|t| &**t).collect();
        assert_eq!(sorted, ["", "\"é\"", "<a>", "<ab>", "<b>", "_:b0"]);
        for (term, id) in arrival.iter().zip(&ids) {
            let rank = frozen.rank_of[*id as usize] as usize;
            assert_eq!(&*frozen.sorted[rank], *term, "id {id}");
        }
    }

    #[test]
    fn the_term_cap_refuses_the_term_past_it() {
        let _cap = crate::store::test_hooks::TermCap::set(3);
        let mut terms = TermInterner::new();
        for t in ["<a>", "<b>", "<c>"] {
            terms.intern(t.into()).unwrap();
        }
        assert!(
            terms.intern("<b>".into()).is_ok(),
            "a held term is not a new one"
        );
        let error = terms.intern("<d>".into()).unwrap_err();
        assert!(
            matches!(&error, VortexRdfError::Serialization(m)
                if m == "Dictionary of 4 unique terms exceeds the supported maximum (3)"),
            "{error}"
        );
        assert_eq!(terms.len(), 3, "the refused term is not held");
    }
}
