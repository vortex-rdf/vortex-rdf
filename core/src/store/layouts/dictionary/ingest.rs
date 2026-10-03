//! Build-side term collection for the Dictionary layout: the ingest paths
//! that consume a quad stream and produce the frozen [`TermDictionary`] —
//! either together with the coded quads (the interning ingest) or beside the
//! owned term → code map the streaming builders encode through.

use std::borrow::Borrow;
use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::sync::Arc;

use futures::{Stream, StreamExt};
use vortex_array::arrays::VarBinViewArray;

use crate::debug;
use crate::error::Result;
use crate::store::RawQuad;
use crate::store::builders::{BuiltArray, build_components_from_codes};
use crate::store::indexes::Indexes;

use super::codec::{QuadCodes, build_array};
use super::term_dict::TermDictionary;

/// Build-only term → code lookup keyed by owned terms, for the streaming
/// builders whose quads are moved or re-read from a spill file and cannot be
/// borrowed from. Dropped once every quad term has been encoded; stores
/// retain only the [`TermDictionary`]. Builders holding a live quad slice use
/// [`BorrowedTermCodeMap`] (see [`TermDictionary::from_quads_with_map`]).
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) type TermCodeMap = HashMap<String, u32>;

/// Term → code lookup borrowing its keys from the quads being encoded — the
/// allocation-free counterpart of `TermCodeMap`.
pub(crate) type BorrowedTermCodeMap<'a> = HashMap<&'a str, u32>;

/// The unique terms of `quads`, sorted; each borrows from its quad.
pub(crate) fn sorted_unique_terms(quads: &[RawQuad]) -> Vec<&str> {
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

/// The term → code map of `sorted` unique terms: a term's code is its
/// index. The map's keys are the terms moved in.
pub(crate) fn code_map<K>(sorted: Vec<K>) -> HashMap<K, u32>
where
    K: Borrow<str> + Eq + Hash,
{
    sorted
        .into_iter()
        .enumerate()
        .map(|(code, term)| (term, code as u32))
        .collect()
}

/// Freeze `sorted` unique terms into the columnar dictionary, beside their
/// [`code_map`].
fn freeze<K>(sorted: Vec<K>) -> Result<(TermDictionary, HashMap<K, u32>)>
where
    K: Borrow<str> + Eq + Hash,
{
    let dict =
        TermDictionary::from_sorted(sorted.iter().map(|term| <K as Borrow<str>>::borrow(term)))?;
    Ok((dict, code_map(sorted)))
}

impl TermDictionary {
    /// Build the dictionary and its term → code map in one pass; the map
    /// borrows its keys from `quads`, so it holds one pointer pair per term
    /// and no string data. The streaming builders, whose quads cannot be
    /// borrowed from, use [`TermDictionaryBuilder::finish`] instead.
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

/// Incrementally collects the unique term strings of a dataset during the
/// ingestion pass of a build. Owned strings exist only for the build's lifetime.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) struct TermDictionaryBuilder {
    set: HashSet<String>,
}

#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
impl TermDictionaryBuilder {
    pub(crate) fn new() -> Self {
        Self {
            set: HashSet::new(),
        }
    }

    pub(crate) fn insert_quad(&mut self, q: &RawQuad) {
        for term in [&q.s, &q.p, &q.o, &q.g] {
            if !self.set.contains(term.as_str()) {
                self.set.insert(term.clone());
            }
        }
    }

    /// Sort the unique terms, freeze them into the columnar dictionary, and
    /// hand back the term → code map beside it. The map's keys are this
    /// builder's sorted strings, moved in; a term's code is its sorted rank.
    pub(crate) fn finish(self) -> Result<(TermDictionary, TermCodeMap)> {
        let start = debug::timer();
        let mut terms: Vec<String> = self.set.into_iter().collect();
        terms.sort_unstable();
        let (dict, code_map) = freeze(terms)?;
        log::debug!(
            "[Dictionary] Finished incremental dictionary ({} unique terms) in {:?}",
            dict.len(),
            debug::elapsed(start)
        );
        Ok((dict, code_map))
    }
}

/// Freeze `interner`'s dictionary and build the single-chunk
/// Dictionary-layout array with the requested indexes' components beside it
/// — the in-memory Dictionary build every interning ingest ends with.
pub(crate) fn finish_interned(
    interner: InterningQuadBuilder,
    indexes: &Indexes,
) -> Result<BuiltArray> {
    let (dict, codes) = interner.finish()?;
    let array = build_array(&codes)?;
    let components = build_components_from_codes(indexes, &codes)?;
    Ok(BuiltArray {
        array,
        components,
        dict: Some(Arc::new(dict)),
    })
}

/// Push-based Dictionary-layout ingest for callers that produce quads one at
/// a time rather than as a `'static` stream — the wasm array path, whose
/// quads are decoded chunk-by-chunk from a packed JS buffer.
///
/// Each pushed quad's Strings are interned and dropped on arrival; `finish`
/// yields the same single-chunk Dictionary-layout array
/// [`SortedInMemoryBuilder`] produces.
///
/// [`SortedInMemoryBuilder`]: crate::SortedInMemoryBuilder
pub struct DictionaryQuadSink {
    interner: InterningQuadBuilder,
    indexes: Indexes,
}

impl DictionaryQuadSink {
    /// An empty sink that will build `indexes` beside the quad columns on
    /// `finish`.
    pub fn new(indexes: Indexes) -> Self {
        Self {
            interner: InterningQuadBuilder::new(),
            indexes,
        }
    }

    /// Intern the quad's four terms and append their codes to the pending
    /// quad columns.
    pub fn push(&mut self, quad: RawQuad) {
        self.interner.push(quad);
    }

    /// Freeze the dictionary and build the single-chunk Dictionary-layout
    /// array, exactly as the corresponding stream builder would.
    pub fn finish(self) -> Result<BuiltArray> {
        finish_interned(self.interner, &self.indexes)
    }
}

/// Ingest-time interner producing the dictionary and the coded quads in one
/// pass: quads are consumed as they arrive, each unique term is held once, and
/// each quad is kept as four u32 term codes.
///
/// The stream's per-quad Strings exist only transiently: they die inside
/// [`push`](Self::push), so what accumulates is one copy of each distinct
/// term plus 16 bytes per quad.
///
/// Codes handed out during ingest are provisional (insertion order).
/// [`finish`](Self::finish) sorts the unique terms, freezes them into the
/// [`TermDictionary`], and remaps every quad's provisional codes to its terms'
/// sorted ranks — the dictionary codes, since codes are lexicographic ranks.
/// It then sorts the coded quads directly: `[u32; 4]` lexicographic order
/// equals (s, p, o, g) term order because codes are sorted ranks.
pub(crate) struct InterningQuadBuilder {
    /// term → provisional code, owning each distinct term exactly once.
    codes: HashMap<Box<str>, u32>,
    /// One `[s, p, o, g]` of provisional codes per quad, in arrival order.
    quads: Vec<[u32; 4]>,
}

impl InterningQuadBuilder {
    pub(crate) fn new() -> Self {
        Self {
            codes: HashMap::new(),
            quads: Vec::new(),
        }
    }

    /// Drain a quad stream into a fresh interner: each quad's Strings die
    /// here, leaving one copy of every distinct term plus 16 bytes per quad.
    /// [`finish`](Self::finish) then yields the dictionary and the coded
    /// quads in global (s, p, o, g) order.
    pub(crate) async fn from_stream(
        mut quads_in: Box<dyn Stream<Item = Result<RawQuad>> + Unpin + Send + 'static>,
    ) -> Result<Self> {
        let mut interner = Self::new();
        while let Some(res) = quads_in.next().await {
            interner.push(res?);
        }
        Ok(interner)
    }

    fn intern(&mut self, term: String) -> u32 {
        let next = self.codes.len() as u32;
        // `into_boxed_str` is free for exact-capacity Strings (the common
        // case from `RawQuad::from_quad`) and shrinks the rest.
        *self.codes.entry(term.into_boxed_str()).or_insert(next)
    }

    /// Consume one quad: intern its four terms, keep only their codes.
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
    /// (s, p, o, g) order.
    pub(crate) fn finish(mut self) -> Result<(TermDictionary, QuadCodes)> {
        let start = debug::timer();
        let n = self.quads.len();

        // Unique terms, so the tuple Ord never reaches the code.
        let mut entries: Vec<(Box<str>, u32)> = self.codes.into_iter().collect();
        entries.sort_unstable();

        // provisional code → sorted rank == dictionary code.
        let mut rank_of = vec![0u32; entries.len()];
        for (rank, (_, provisional)) in entries.iter().enumerate() {
            rank_of[*provisional as usize] = rank as u32;
        }

        // Freeze by *consuming* the boxes: each term is freed as it is copied
        // into the plain column, so the boxes and the column never coexist in
        // full.
        let plain = VarBinViewArray::from_iter_str(entries.into_iter().map(|(t, _)| t));
        let dict = TermDictionary::from_sorted_column(plain)?;

        for quad in &mut self.quads {
            for code in quad.iter_mut() {
                *code = rank_of[*code as usize];
            }
        }
        self.quads.sort_unstable();

        let mut codes = QuadCodes {
            s: Vec::with_capacity(n),
            p: Vec::with_capacity(n),
            o: Vec::with_capacity(n),
            g: Vec::with_capacity(n),
        };
        for [s, p, o, g] in self.quads {
            codes.s.push(s);
            codes.p.push(p);
            codes.o.push(o);
            codes.g.push(g);
        }

        log::debug!(
            "[Dictionary] Interned {} quads ({} unique terms) in {:?}",
            n,
            dict.len(),
            debug::elapsed(start)
        );
        Ok((dict, codes))
    }
}
