//! Build-side term collection for the Dictionary layout: the ingest paths
//! that produce the frozen [`TermDictionary`], beside the coded quads (the
//! interning ingest) or beside a term → code map the builders encode
//! through. A term is interned by the canonical spelling the parser renders,
//! one per RDF term, so a dictionary holds one code per term; the interning
//! ingest drops repeated quads after its sort, before anything is derived
//! from them.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use futures::{Stream, StreamExt};
use vortex_array::arrays::VarBinViewArray;

use crate::debug;
use crate::error::Result;
use crate::store::RawQuad;
use crate::store::builders::{BuiltArray, build_components};
use crate::store::indexes::{IndexInput, Indexes};
use crate::store::schema::TermCode;

use super::codec::{QuadCodes, build_code_chunk};
use super::term_dict::TermDictionary;

/// Term → code lookup borrowing its keys from the quads being encoded.
pub(crate) type BorrowedTermCodeMap<'a> = HashMap<&'a str, TermCode>;

/// The unique terms of `quads`, sorted; each borrows from its quad, so
/// nothing is copied.
fn sorted_unique_terms(quads: &[RawQuad]) -> Vec<&str> {
    let mut set: HashSet<&str> = HashSet::new();
    for q in quads {
        set.insert(&q.s);
        set.insert(&q.p);
        set.insert(&q.o);
        set.insert(&q.g);
    }
    let mut terms: Vec<&str> = set.into_iter().collect();
    terms.sort_unstable();
    terms
}

/// Freeze `sorted` unique terms into the dictionary, FSST-compressed in
/// windows, beside their term → code map: a term's code is the code of its
/// rank in the dictionary's code space, and the keys are the terms moved in.
fn freeze(sorted: Vec<&str>) -> Result<(TermDictionary, BorrowedTermCodeMap<'_>)> {
    let plain = VarBinViewArray::from_iter_str(sorted.iter().copied());
    let dict = TermDictionary::from_sorted_column(plain)?;
    let code_space = dict.code_space();
    let code_map = sorted
        .into_iter()
        .enumerate()
        .map(|(rank, term)| (term, code_space.code_at(rank)))
        .collect();
    Ok((dict, code_map))
}

impl TermDictionary {
    /// The dictionary of `quads` ([`freeze`]), beside the term → code map
    /// borrowing its keys from them: one pointer pair per term, no string
    /// data.
    pub(crate) fn from_quads_with_map(
        quads: &[RawQuad],
    ) -> Result<(Self, BorrowedTermCodeMap<'_>)> {
        let start = debug::timer();
        let (dict, code_map) = freeze(sorted_unique_terms(quads))?;
        log::debug!(
            "[Dictionary] Built dictionary + borrowed code map from {} quads ({} unique terms) in {:?}",
            quads.len(),
            dict.len(),
            debug::elapsed(start)
        );
        Ok((dict, code_map))
    }
}

/// Freeze `interner`'s dictionary and build the single-chunk
/// Dictionary-layout array, the requested indexes' components beside it.
pub(crate) fn finish_interned(
    interner: InterningQuadBuilder,
    indexes: &Indexes,
) -> Result<BuiltArray> {
    let (dict, codes) = interner.finish()?;
    // The codes are in global (s, p, o, g) order, rows or none: `s` is
    // stamped sorted.
    let array = build_code_chunk(&codes, 0..codes.s.len(), true)?;
    let components = build_components(indexes, IndexInput::Codes(&codes))?;
    Ok(BuiltArray {
        array,
        components,
        dict: Some(Arc::new(dict)),
    })
}

/// Push-based Dictionary-layout ingest, for callers that produce quads one at
/// a time rather than as a `'static` stream (the wasm array path). Each
/// pushed quad's strings are interned and dropped on arrival; `finish` yields
/// the single-chunk array [`SortedInMemoryBuilder`] produces.
///
/// [`SortedInMemoryBuilder`]: crate::SortedInMemoryBuilder
pub struct DictionaryQuadSink {
    interner: InterningQuadBuilder,
    indexes: Indexes,
}

impl DictionaryQuadSink {
    /// An empty sink that builds `indexes` beside the quad columns on
    /// `finish`.
    pub fn new(indexes: Indexes) -> Self {
        Self {
            interner: InterningQuadBuilder::new(),
            indexes,
        }
    }

    /// Intern the quad's four terms and append their codes. The quad must be
    /// canonical (from `RawQuad::from_quad` or `RawQuad::canonical`): the
    /// spelling given is the term interned.
    pub fn push(&mut self, quad: RawQuad) {
        self.interner.push(quad);
    }

    /// Freeze the dictionary and build the single-chunk Dictionary-layout
    /// array, as the in-memory builder does for a stream.
    pub fn finish(self) -> Result<BuiltArray> {
        finish_interned(self.interner, &self.indexes)
    }
}

/// Ingest-time interner: each distinct term held once, each quad as four
/// provisional codes (insertion order), so what accumulates is one copy of
/// each distinct term plus 32 bytes per quad. [`finish`](Self::finish) sorts
/// the terms, remaps the provisional codes to their sorted ranks (the
/// dictionary codes) and sorts the coded quads, whose `[TermCode; 4]` order
/// is (s, p, o, g) term order, dropping adjacent repeats: terms are interned
/// once, so equal codes are equal quads.
pub(crate) struct InterningQuadBuilder {
    /// term → provisional code.
    codes: HashMap<Box<str>, TermCode>,
    /// One `[s, p, o, g]` of provisional codes per quad, in arrival order.
    quads: Vec<[TermCode; 4]>,
}

impl InterningQuadBuilder {
    pub(crate) fn new() -> Self {
        Self {
            codes: HashMap::new(),
            quads: Vec::new(),
        }
    }

    /// A fresh interner fed every quad of `quads_in`; a stream error is
    /// returned.
    pub(crate) async fn from_stream(
        mut quads_in: Box<dyn Stream<Item = Result<RawQuad>> + Unpin + Send + 'static>,
    ) -> Result<Self> {
        let mut interner = Self::new();
        while let Some(res) = quads_in.next().await {
            interner.push(res?);
        }
        Ok(interner)
    }

    fn intern(&mut self, term: String) -> TermCode {
        // Provisional codes are insertion ranks: below the term count, so
        // they index `rank_of` in `finish`.
        let next = self.codes.len() as TermCode;
        // `into_boxed_str` is free for exact-capacity Strings (the common
        // case from `RawQuad::from_quad`) and shrinks the rest.
        *self.codes.entry(term.into_boxed_str()).or_insert(next)
    }

    /// Intern the quad's four terms, keeping only their codes.
    pub(crate) fn push(&mut self, q: RawQuad) {
        let quad = [
            self.intern(q.s),
            self.intern(q.p),
            self.intern(q.o),
            self.intern(q.g),
        ];
        self.quads.push(quad);
    }

    /// Freeze the dictionary and produce the dataset's codes in global
    /// (s, p, o, g) order, each distinct quad once.
    pub(crate) fn finish(mut self) -> Result<(TermDictionary, QuadCodes)> {
        let total_start = debug::timer();
        let ingested = self.quads.len();

        let sort_start = debug::timer();
        // Unique terms, so the tuple Ord never reaches the code.
        let mut entries: Vec<(Box<str>, TermCode)> = self.codes.into_iter().collect();
        entries.sort_unstable();
        let sort_terms_elapsed = debug::elapsed(sort_start);

        // provisional code → sorted rank. A provisional code is an insertion
        // rank below `entries.len()`, so it indexes the table on every
        // target.
        let mut rank_of = vec![0usize; entries.len()];
        for (rank, (_, provisional)) in entries.iter().enumerate() {
            rank_of[*provisional as usize] = rank;
        }

        // Consuming the boxes frees each term as it is copied into the
        // column, so the boxes and the column never coexist in full.
        let freeze_start = debug::timer();
        let plain = VarBinViewArray::from_iter_str(entries.into_iter().map(|(t, _)| t));
        let dict = TermDictionary::from_sorted_column(plain)?;
        let freeze_elapsed = debug::elapsed(freeze_start);

        // provisional code → sorted rank → dictionary code.
        let remap_start = debug::timer();
        for quad in &mut self.quads {
            for code in quad.iter_mut() {
                *code = dict.code_at(rank_of[*code as usize]);
            }
        }
        self.quads.sort_unstable();
        // Equal quads are adjacent now; the dictionary already holds each
        // term once, so equal codes are equal terms.
        self.quads.dedup();
        let remap_elapsed = debug::elapsed(remap_start);
        let n = self.quads.len();

        let mut codes = QuadCodes::with_capacity(n);
        for quad in self.quads {
            codes.push(quad);
        }

        log::debug!(
            "[Dictionary] Interned {} quads ({} distinct, {} unique terms): sort terms {:?}, freeze {:?}, remap+sort quads {:?}, total {:?}",
            ingested,
            n,
            dict.len(),
            sort_terms_elapsed,
            freeze_elapsed,
            remap_elapsed,
            debug::elapsed(total_start)
        );
        Ok((dict, codes))
    }
}
