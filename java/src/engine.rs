//! The binding's logic without JNI: a read-only Vortex-RDF store, queried by term patterns (N-Triples
//! spellings) for every layout and by term codes for a Dictionary-layout store.
//!
//! A pattern has four positions, `(s, p, o, g)`; a free position is `None`. As in the Python bindings the
//! default graph is spelled `""`, which is also how a pattern selects it. A term code is the store's own,
//! starting at 0.

use std::sync::LazyLock;

use vortex_rdf_core::{
    CodePattern, DictReader, KindRanges, Probe, Result, VortexRdfError, VortexRdfStore,
};

use crate::cursor::QuadCursor;
use crate::payload::{Limits, Payload, encode_lookup};
use crate::streams::{CodeStream, TermStream, check_batch_rows};

/// Bytes per row [`Store::match_codes`] writes: four codes of eight bytes.
pub const ROW_BYTES: usize = 32;

/// One runtime for every store of the process, as in the Python bindings: a store's reads are async, the
/// caller's threads block on them.
pub(crate) static RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("start the vortex-rdf native library's async runtime")
});

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

    /// The quads matching a pattern of N-Triples spellings, in the store's row order, as a cursor that hands
    /// them out in packed chunks under `limits`: a Dictionary store's matched codes, gathered (in index order
    /// when an index serves the pattern), or the matched quads' terms otherwise.
    pub fn quads_cursor(
        &self,
        s: Option<&str>,
        p: Option<&str>,
        o: Option<&str>,
        g: Option<&str>,
        limits: Limits,
    ) -> Result<QuadCursor> {
        let probe = Probe::parse(s, p, o, g)?;
        let view = RUNTIME.block_on(self.store.run_probe(&probe))?;
        if let Some(dict) = &self.dict
            && let Some(columns) = RUNTIME.block_on(view.code_columns_gathered())?
        {
            return Ok(QuadCursor::codes(dict.clone(), columns, limits));
        }
        let rows = RUNTIME.block_on(view.shared_quads_vec())?;
        Ok(QuadCursor::shared(rows, limits))
    }

    /// The quads matching a pattern of N-Triples spellings as a stream, in base row order, read in chunks of
    /// at most `batch_rows` rows and sent as packed payloads ([`TermStream`]). The pattern is parsed and matched
    /// now; the rows are read as the stream is pulled.
    pub fn term_stream(
        &self,
        s: Option<&str>,
        p: Option<&str>,
        o: Option<&str>,
        g: Option<&str>,
        batch_rows: usize,
    ) -> Result<TermStream> {
        check_batch_rows(batch_rows)?;
        let probe = Probe::parse(s, p, o, g)?;
        let view = RUNTIME.block_on(self.store.run_probe(&probe))?;
        TermStream::open(view, self.dict.clone(), batch_rows)
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

    /// The quads matching a code pattern as a stream of their codes, in base row order, read in chunks of at
    /// most `batch_rows` rows ([`CodeStream`]); empty for a pattern no quad can match (a code past the
    /// dictionary, or one in a position its term's kind cannot take).
    pub fn code_stream(&self, pattern: CodePattern, batch_rows: usize) -> Result<CodeStream> {
        check_batch_rows(batch_rows)?;
        self.dict()?;
        match RUNTIME.block_on(self.store.code_probe(pattern))? {
            None => Ok(CodeStream::empty()),
            Some(probe) => {
                CodeStream::open(&RUNTIME.block_on(self.store.run_probe(&probe))?, batch_rows)
            }
        }
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
        let rows = columns[0].len();
        for (row, bytes) in out[..rows].iter_mut().enumerate() {
            for (code, column) in bytes.as_chunks_mut::<8>().0.iter_mut().zip(&columns) {
                *code = column[row].to_ne_bytes();
            }
        }
        Ok(rows)
    }
}
