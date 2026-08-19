//! Build-side term collection for the Dictionary layout: the ingest paths
//! that consume a quad stream and produce the frozen [`TermDictionary`] —
//! either together with the coded quads (the interning ingest) or beside the
//! owned term→ID map the streaming builders encode through.

use std::collections::HashMap;
// Only [`TermDictionaryBuilder`] collects terms as a set, and it is compiled
// out with the external-sort builder that drives it.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
use std::collections::HashSet;
use std::sync::Arc;
use web_time::Instant;

use futures::{Stream, StreamExt};
use vortex_array::arrays::VarBinViewArray;

use crate::error::Result;
use crate::store::RawQuad;
use crate::store::builders::BuiltArray;
use crate::store::indexes::Indexes;

use super::term_dict::TermDictionary;
use super::{QuadCodes, build_array};

/// Build-only term-to-ID lookup table, keyed by owned terms. It is deliberately
/// kept separate from [`TermDictionary`] so stores retain only the compact
/// columnar dictionary; builders drop this map as soon as all quad terms have
/// been encoded.
///
/// Prefer [`TermDictionary::from_quads_with_map`]'s borrowed map wherever the
/// quads outlive the encode: the owned keys here duplicate the entire term set
/// on the heap, which for a large dataset costs more than the dictionary
/// itself. This variant exists for the streaming builders, whose quads are
/// moved or re-read from a spill file and so cannot be borrowed from.
pub(crate) type TermIdMap = HashMap<String, u32>;

/// Term-to-ID lookup borrowing its keys from the quads being encoded — the
/// allocation-free counterpart of [`TermIdMap`].
pub(crate) type BorrowedTermIdMap<'a> = HashMap<&'a str, u32>;

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
    /// hand back the term→ID map beside it (a term's ID is its sorted rank).
    ///
    /// The map's owned keys are the sorted strings this builder already
    /// holds, moved rather than re-materialized: deriving the map from the
    /// frozen dictionary instead would decode every term back out of FSST
    /// and re-allocate it (see
    /// [`TermDictionary::from_quads_with_map`] for the same reasoning on the
    /// borrowed side).
    pub(crate) fn finish(self) -> Result<(TermDictionary, TermIdMap)> {
        let total_start = Instant::now();
        let collect_start = Instant::now();
        let mut terms: Vec<String> = self.set.into_iter().collect();
        let collect_elapsed = collect_start.elapsed();
        let sort_start = Instant::now();
        terms.sort_unstable();
        let sort_elapsed = sort_start.elapsed();
        let freeze_start = Instant::now();
        let dict = TermDictionary::from_sorted(terms.iter().map(String::as_str))?;
        let freeze_elapsed = freeze_start.elapsed();
        let map_start = Instant::now();
        let id_map: TermIdMap = terms
            .into_iter()
            .enumerate()
            .map(|(id, term)| (term, id as u32))
            .collect();
        log::debug!(
            "[Dictionary] Finished incremental dictionary ({} unique terms): collect {:?}, sort {:?}, freeze {:?}, map {:?}, total {:?}",
            dict.len(),
            collect_elapsed,
            sort_elapsed,
            freeze_elapsed,
            map_start.elapsed(),
            total_start.elapsed()
        );
        Ok((dict, id_map))
    }
}

/// Drain a quad stream into an [`InterningQuadBuilder`]: each quad's Strings
/// die here, leaving one copy of every distinct term plus 16 bytes per quad.
///
/// The Dictionary-layout in-memory ingest; `finish` then yields the
/// dictionary and the coded quads in global (s, p, o, g) order.
pub(crate) async fn ingest_interning(
    mut quads_in: Box<dyn Stream<Item = Result<RawQuad>> + Unpin + Send + 'static>,
) -> Result<InterningQuadBuilder> {
    let mut interner = InterningQuadBuilder::new();
    while let Some(res) = quads_in.next().await {
        interner.push(res?);
    }
    Ok(interner)
}

/// Push-based Dictionary-layout ingest for callers that produce quads one at
/// a time rather than as a `'static` stream — the wasm array path, whose
/// quads are decoded chunk-by-chunk from a packed JS buffer.
///
/// Feeding those quads through the stream builders would require collecting
/// them into a full `Vec<RawQuad>` first (a `'static` stream cannot borrow
/// from the decode loop), resurrecting exactly the four-Strings-per-quad
/// ingest high-water the interning ingest removes. Pushing into the sink
/// instead lets each quad's Strings die on arrival; `finish` builds the same
/// single-chunk array [`SortedInMemoryBuilder`] produces for the Dictionary
/// layout.
///
/// [`SortedInMemoryBuilder`]: crate::SortedInMemoryBuilder
pub struct DictionaryQuadSink {
    interner: InterningQuadBuilder,
    indexes: Indexes,
}

impl DictionaryQuadSink {
    pub fn new(indexes: Indexes) -> Self {
        Self {
            interner: InterningQuadBuilder::new(),
            indexes,
        }
    }

    /// Consume one quad: intern its four terms, keep only their ids.
    pub fn push(&mut self, quad: RawQuad) {
        self.interner.push(quad);
    }

    /// Freeze the dictionary and build the single-chunk Dictionary-layout
    /// array, exactly as the corresponding stream builder would.
    pub fn finish(self) -> Result<BuiltArray> {
        let (dict, codes) = self.interner.finish()?;
        let array = build_array(&codes, &self.indexes)?;
        Ok(BuiltArray {
            array,
            components: Vec::new(),
            dict: Some(Arc::new(dict)),
        })
    }
}

/// Ingest-time interner producing the dictionary and the coded quads in one
/// pass: quads are consumed as they arrive, each unique term is held once, and
/// each quad is kept as four u32 ids.
///
/// This replaces buffering the whole stream as a `Vec<RawQuad>` — four owned
/// `String`s per quad, held live until the dictionary and codes were derived
/// from them — which was the measured wasm ingest high-water mark (~377 B/row).
/// The per-quad Strings still exist transiently (the stream hands them over),
/// but they die inside [`push`](Self::push); what accumulates is one copy of
/// each distinct term plus 16 bytes per quad.
///
/// Ids handed out during ingest are provisional (insertion order).
/// [`finish`](Self::finish) sorts the unique terms, freezes them into the
/// [`TermDictionary`], and remaps every quad id to its term's sorted rank —
/// which *is* the dictionary code, since codes are lexicographic ranks. For
/// sorted builders it then sorts the coded quads directly: `[u32; 4]`
/// lexicographic order equals (s, p, o, g) term order (order-isomorphism
/// again), and sorting 16-byte rows is far cheaper than sorting four-String
/// structs.
pub(crate) struct InterningQuadBuilder {
    /// term → provisional id, owning each distinct term exactly once.
    ids: HashMap<Box<str>, u32>,
    /// One `[s, p, o, g]` of provisional ids per quad, in arrival order.
    quads: Vec<[u32; 4]>,
}

impl InterningQuadBuilder {
    pub(crate) fn new() -> Self {
        Self {
            ids: HashMap::new(),
            quads: Vec::new(),
        }
    }

    fn intern(&mut self, term: String) -> u32 {
        let next = self.ids.len() as u32;
        // `into_boxed_str` is free for exact-capacity Strings (the common
        // case from `RawQuad::from_quad`) and shrinks the rest.
        *self.ids.entry(term.into_boxed_str()).or_insert(next)
    }

    /// Consume one quad: intern its four terms, keep only their ids.
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
        let total_start = Instant::now();
        let n = self.quads.len();

        let sort_start = Instant::now();
        // Unique terms, so the tuple Ord never reaches the id.
        let mut entries: Vec<(Box<str>, u32)> = self.ids.into_iter().collect();
        entries.sort_unstable();
        let sort_terms_elapsed = sort_start.elapsed();

        // provisional id → sorted rank == dictionary code.
        let mut rank_of = vec![0u32; entries.len()];
        for (rank, (_, pid)) in entries.iter().enumerate() {
            rank_of[*pid as usize] = rank as u32;
        }

        // Freeze by *consuming* the boxes: each term is freed as it is copied
        // into the plain column, so the boxes and the column never coexist in
        // full — that stacking was the finish-phase memory peak.
        let freeze_start = Instant::now();
        let plain = VarBinViewArray::from_iter_str(entries.into_iter().map(|(t, _)| t));
        let dict = TermDictionary::from_sorted_column(plain)?;
        let freeze_elapsed = freeze_start.elapsed();

        let remap_start = Instant::now();
        for quad in &mut self.quads {
            for id in quad.iter_mut() {
                *id = rank_of[*id as usize];
            }
        }
        self.quads.sort_unstable();
        let remap_elapsed = remap_start.elapsed();

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
            "[Dictionary] Interned {} quads ({} unique terms): sort terms {:?}, freeze {:?}, remap+sort quads {:?}, total {:?}",
            n,
            dict.len(),
            sort_terms_elapsed,
            freeze_elapsed,
            remap_elapsed,
            total_start.elapsed()
        );
        Ok((dict, codes))
    }
}
