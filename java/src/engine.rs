//! The binding's logic without JNI: a read-only Vortex-RDF store, queried by term patterns (N-Triples
//! spellings) for every layout and by term codes for a Dictionary-layout store.
//!
//! A pattern has four positions, `(s, p, o, g)`; a free position is `None`. As in the Python bindings the
//! default graph is spelled `""`, which is also how a pattern selects it. A term code is the store's own,
//! starting at 0.

use vortex_rdf_core::{
    CodePattern, DictReader, KindRanges, Probe, Result, VortexRdfError, VortexRdfStore,
};

use crate::RUNTIME;
use crate::payload::{Limits, Payload, ROW_BYTES, encode_lookup, write_rows};
use crate::streams::{CodeStream, TermStream};

/// `InvalidInput` for a chunk of no rows: checked before a stream's pattern is matched.
fn check_batch_rows(batch_rows: usize) -> Result<()> {
    if batch_rows == 0 {
        return Err(VortexRdfError::InvalidInput(
            "batch_rows must be at least 1: the most rows a chunk holds".into(),
        ));
    }
    Ok(())
}

pub struct Store {
    store: VortexRdfStore,
    dict: Option<DictReader>,
    size: u64,
}

impl Store {
    /// Opens a `.vortex` file: memory-mapped and read in place, or with `in_memory` loaded whole.
    pub fn open(path: &str, in_memory: bool) -> Result<Store> {
        let store = RUNTIME.block_on(async {
            if in_memory {
                VortexRdfStore::from_file_in_memory(path).await
            } else {
                VortexRdfStore::from_file(path).await
            }
        })?;
        let size = RUNTIME.block_on(store.size())? as u64;
        let dict = store.dict_reader();
        Ok(Store { store, dict, size })
    }

    /// The number of quads.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// The column layout: `"default"`, `"typed-object"` or `"dictionary"`.
    pub fn layout(&self) -> String {
        self.store.layout().to_string()
    }

    /// The secondary indexes the file carries, by name (`"secondary-by-copy"`, `"secondary-by-reference"`).
    pub fn indexes(&self) -> Vec<String> {
        self.store
            .indexes()
            .iter()
            .map(ToString::to_string)
            .collect()
    }

    // ---- term patterns, every layout ----

    /// The number of quads matching a pattern of N-Triples spellings, counted without reading terms.
    pub fn count(
        &self,
        s: Option<&str>,
        p: Option<&str>,
        o: Option<&str>,
        g: Option<&str>,
    ) -> Result<u64> {
        let probe = Probe::parse(s, p, o, g)?;
        Ok(RUNTIME.block_on(self.store.count_probe(&probe))? as u64)
    }

    /// The quads matching a pattern of N-Triples spellings, read now, in the match's order (a
    /// `secondary-by-copy` index's for a pattern it serves, else base row order), beside their number: a stream
    /// that sends them in packed payloads under `limits` ([`TermStream`]), of a Dictionary store's matched codes,
    /// gathered, or of the matched quads' terms otherwise.
    pub fn quads(
        &self,
        s: Option<&str>,
        p: Option<&str>,
        o: Option<&str>,
        g: Option<&str>,
        limits: Limits,
    ) -> Result<(TermStream, usize)> {
        let probe = Probe::parse(s, p, o, g)?;
        let view = RUNTIME.block_on(self.store.run_probe(&probe))?;
        if let Some(dict) = &self.dict
            && let Some(columns) = RUNTIME.block_on(view.code_columns_gathered())?
        {
            let rows = columns[0].len();
            return Ok((TermStream::codes(dict.clone(), columns, limits), rows));
        }
        let quads = RUNTIME.block_on(view.shared_quads_vec())?;
        let rows = quads.len();
        Ok((TermStream::shared(quads, limits), rows))
    }

    /// The quads matching a pattern of N-Triples spellings as a stream, in the match's order (a
    /// `secondary-by-copy` index's for a pattern it serves, else base row order), read in chunks of at most
    /// `batch_rows` rows and sent as packed payloads under `limits` ([`TermStream`]). The pattern is parsed and
    /// matched now; the rows are read as the stream is pulled.
    pub fn term_stream(
        &self,
        s: Option<&str>,
        p: Option<&str>,
        o: Option<&str>,
        g: Option<&str>,
        batch_rows: usize,
        limits: Limits,
    ) -> Result<TermStream> {
        check_batch_rows(batch_rows)?;
        let probe = Probe::parse(s, p, o, g)?;
        let view = RUNTIME.block_on(self.store.run_probe(&probe))?;
        TermStream::open(view, self.dict.clone(), batch_rows, limits)
    }

    // ---- term codes, Dictionary layout ----

    fn dict(&self) -> Result<&DictReader> {
        self.dict.as_ref().ok_or_else(|| {
            VortexRdfError::InvalidOperation(format!(
                "a {} store has no term dictionary",
                self.layout()
            ))
        })
    }

    /// Whether the store has a term dictionary (Dictionary layout).
    pub fn has_dictionary(&self) -> bool {
        self.dict.is_some()
    }

    /// The number of terms in the dictionary.
    pub fn term_count(&self) -> Result<u64> {
        Ok(self.dict()?.len() as u64)
    }

    /// The code ranges of the term kinds.
    pub fn ranges(&self) -> Result<KindRanges> {
        RUNTIME.block_on(self.dict()?.kind_ranges())
    }

    /// The code of a term, or `None` when the dictionary lacks it. The spelling is tolerated as in the Python
    /// bindings (an IRI with or without angle brackets, the default graph as `""`).
    pub fn encode(&self, term: &str) -> Result<Option<u64>> {
        RUNTIME.block_on(self.dict()?.encode(term))
    }

    /// The N-Triples spelling of a code, or `None` for a code outside the dictionary.
    pub fn decode(&self, code: u64) -> Result<Option<String>> {
        RUNTIME.block_on(self.dict()?.decode(code))
    }

    /// [`decode`](Self::decode) of each code, in order, for codes in any order and with repeats.
    pub fn decode_many(&self, codes: &[u64]) -> Result<Vec<Option<String>>> {
        RUNTIME.block_on(self.dict()?.decode_many(codes))
    }

    /// The terms of `codes` from the first, as one packed chunk under `limits`, and how many codes it holds
    /// (at least one, unless `codes` is empty). A code outside the dictionary holds no term.
    pub fn decode_payload(&self, codes: &[u64], limits: Limits) -> Result<(Payload, usize)> {
        RUNTIME.block_on(encode_lookup(self.dict()?, codes, limits))
    }

    /// The quads matching a code pattern as a stream of their codes, the rows of [`match_codes`](Self::match_codes)
    /// in its order (a `secondary-by-copy` index's for a pattern it serves, else base row order), read in chunks of
    /// at most `batch_rows` rows ([`CodeStream`], from core's `match_code_chunks`): a located run's rows are read
    /// once, a block at a time, where each `match_codes` window checks a run with other bound slots again from its
    /// start.
    /// Empty for a pattern no quad can match (a code past the dictionary, or one in a position its term's kind
    /// cannot take).
    pub fn code_stream(&self, pattern: CodePattern, batch_rows: usize) -> Result<CodeStream> {
        check_batch_rows(batch_rows)?;
        let chunks = RUNTIME.block_on(self.store.match_code_chunks(pattern, batch_rows))?;
        Ok(CodeStream::pulling(chunks))
    }

    /// The number of quads matching a code pattern, exact and without reading rows.
    pub fn count_codes(&self, pattern: CodePattern) -> Result<u64> {
        RUNTIME.block_on(self.store.count_code_pattern(pattern))
    }

    /// Up to `out.len()` quads matching a code pattern after `offset`, one row of `out` each: the
    /// `(s, p, o, g)` codes, eight native-endian bytes apiece. Returns the rows written.
    pub fn match_codes(
        &self,
        pattern: CodePattern,
        offset: u64,
        out: &mut [[u8; ROW_BYTES]],
    ) -> Result<usize> {
        let columns = RUNTIME.block_on(self.store.match_code_pattern(
            pattern,
            offset as usize,
            Some(out.len()),
        ))?;
        Ok(write_rows(&columns, 0, out))
    }
}
