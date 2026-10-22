//! The binding's logic without JNI: a read-only Vortex-RDF store, queried by term patterns (N-Triples
//! spellings) for every layout and by term codes for a Dictionary-layout store.
//!
//! A pattern has four positions, `(s, p, o, g)`; a free position is `None`. As in the Python bindings the
//! default graph is spelled `""`, which is also how a pattern selects it. A term code is the store's own,
//! starting at 0.

use std::fmt;
use std::sync::OnceLock;

use vortex_rdf_core::{CodePattern, DictReader, Probe, VortexRdfStore};

#[derive(Debug)]
pub struct Error(pub String);

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl<E: std::error::Error> From<E> for Error {
    fn from(e: E) -> Self {
        Error(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;

/// A code pattern: one optional term code per position of `(s, p, o, g)`.
pub type Codes = [Option<u64>; 4];

/// One runtime for every store of the process, as in the Python bindings: a store's reads are async, the
/// caller's threads block on them.
fn runtime() -> Result<&'static tokio::runtime::Runtime> {
    static RUNTIME: OnceLock<std::io::Result<tokio::runtime::Runtime>> = OnceLock::new();
    RUNTIME
        .get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
        })
        .as_ref()
        .map_err(|e| Error(format!("cannot start the async runtime: {e}")))
}

/// The code ranges of the dictionary's term kinds, ends exclusive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ranges {
    /// The code of `""`, the default graph's name, when the dictionary holds it.
    pub default_graph: Option<u64>,
    pub literals: (u64, u64),
    pub iris: (u64, u64),
    pub blanks: (u64, u64),
}

pub struct Store {
    rt: &'static tokio::runtime::Runtime,
    store: VortexRdfStore,
    dict: Option<DictReader>,
    size: u64,
}

impl Store {
    /// Opens a `.vortex` file: memory-mapped and read in place, or with `in_memory` loaded whole.
    pub fn open(path: &str, in_memory: bool) -> Result<Store> {
        let rt = runtime()?;
        let store = rt.block_on(async {
            if in_memory {
                VortexRdfStore::from_file_in_memory(path).await
            } else {
                VortexRdfStore::from_file(path).await
            }
        })?;
        let size = rt.block_on(store.size())? as u64;
        let dict = store.dict_reader();
        Ok(Store {
            rt,
            store,
            dict,
            size,
        })
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
        Ok(self.rt.block_on(self.store.count_probe(&probe))? as u64)
    }

    /// The quads matching a pattern as `(s, p, o, g)` N-Triples spellings, `limit` rows after `offset` in the
    /// store's row order.
    pub fn quads(
        &self,
        s: Option<&str>,
        p: Option<&str>,
        o: Option<&str>,
        g: Option<&str>,
        offset: u64,
        limit: Option<usize>,
    ) -> Result<Vec<[String; 4]>> {
        let probe = Probe::parse(s, p, o, g)?.window(offset as usize, limit);
        let view = self.rt.block_on(self.store.run_probe(&probe))?;
        if let Some(dict) = &self.dict
            && let Some(columns) = self.rt.block_on(view.code_columns_gathered())?
        {
            let mut decoded: Vec<Vec<String>> = Vec::with_capacity(4);
            for column in &columns {
                let terms = self.rt.block_on(dict.decode_many(column))?;
                decoded.push(
                    terms
                        .into_iter()
                        .map(|t| {
                            t.ok_or_else(|| {
                                Error("a matched row holds a code outside the dictionary".into())
                            })
                        })
                        .collect::<Result<_>>()?,
                );
            }
            let rows = decoded[0].len();
            let mut columns = decoded.into_iter().map(Vec::into_iter).collect::<Vec<_>>();
            return Ok((0..rows)
                .map(|_| std::array::from_fn(|i| columns[i].next().expect("equal column lengths")))
                .collect());
        }
        let rows = self.rt.block_on(view.shared_quads_vec())?;
        Ok(rows
            .iter()
            .map(|q| {
                [
                    q.s.to_string(),
                    q.p.to_string(),
                    q.o.to_string(),
                    q.g.to_string(),
                ]
            })
            .collect())
    }

    // ---- term codes, Dictionary layout ----

    fn dict(&self) -> Result<&DictReader> {
        self.dict
            .as_ref()
            .ok_or_else(|| Error(format!("a {} store has no term dictionary", self.layout())))
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
    pub fn ranges(&self) -> Result<Ranges> {
        let k = self.rt.block_on(self.dict()?.kind_ranges())?;
        Ok(Ranges {
            default_graph: k.default_graph,
            literals: (k.literals.start, k.literals.end),
            iris: (k.iris.start, k.iris.end),
            blanks: (k.blanks.start, k.blanks.end),
        })
    }

    /// The code of a term, or `None` when the dictionary lacks it. The spelling is tolerated as in the Python
    /// bindings (an IRI with or without angle brackets, the default graph as `""`).
    pub fn encode(&self, term: &str) -> Result<Option<u64>> {
        Ok(self.rt.block_on(self.dict()?.encode(term))?)
    }

    /// The N-Triples spelling of a code, or `None` for a code outside the dictionary.
    pub fn decode(&self, code: u64) -> Result<Option<String>> {
        Ok(self.rt.block_on(self.dict()?.decode(code))?)
    }

    /// [`decode`](Self::decode) of each code, in order, for codes in any order and with repeats.
    pub fn decode_many(&self, codes: &[u64]) -> Result<Vec<Option<String>>> {
        Ok(self.rt.block_on(self.dict()?.decode_many(codes))?)
    }

    /// The probe of a code pattern, spelled out, or `None` when it can match nothing: a code outside the
    /// dictionary, or a term in a position it cannot take (a literal subject, a non-IRI predicate).
    fn probe(&self, pattern: Codes) -> Result<Option<Probe>> {
        let dict = self.dict()?;
        let mut spelled: [Option<String>; 4] = [None, None, None, None];
        for (slot, code) in spelled.iter_mut().zip(pattern) {
            if let Some(code) = code {
                match self.rt.block_on(dict.decode(code))? {
                    Some(term) => *slot = Some(term),
                    None => return Ok(None),
                }
            }
        }
        let [s, p, o, g] = &spelled;
        Ok(Probe::parse(s.as_deref(), p.as_deref(), o.as_deref(), g.as_deref()).ok())
    }

    fn code_pattern(pattern: Codes) -> CodePattern {
        CodePattern::new(pattern[0], pattern[1], pattern[2], pattern[3])
    }

    /// The number of quads matching a code pattern, exact and without reading rows.
    pub fn count_codes(&self, pattern: Codes) -> Result<u64> {
        self.dict()?;
        // A point lookup in a sorted table of a file store is one located run; anything else takes the
        // general path through the pattern's terms.
        if let Some(found) = self.rt.block_on(self.store.locate_codes(
            Self::code_pattern(pattern),
            0,
            Some(0),
        ))? {
            return Ok(found.total);
        }
        let Some(probe) = self.probe(pattern)? else {
            return Ok(0);
        };
        Ok(self.rt.block_on(self.store.count_probe(&probe))? as u64)
    }

    /// Up to `limit` quads matching a code pattern after `offset`, written as `(s, p, o, g)` codes into
    /// `out`, which must hold `4 * limit` codes. Returns the rows written.
    pub fn match_codes(
        &self,
        pattern: Codes,
        offset: u64,
        limit: usize,
        out: &mut [u64],
    ) -> Result<usize> {
        assert!(out.len() >= 4 * limit, "output buffer too small");
        self.dict()?;
        let columns = if let Some(found) = self.rt.block_on(self.store.locate_codes(
            Self::code_pattern(pattern),
            offset as usize,
            Some(limit),
        ))? {
            found.columns
        } else {
            let Some(probe) = self.probe(pattern)? else {
                return Ok(0);
            };
            let view = self.rt.block_on(
                self.store
                    .run_probe(&probe.window(offset as usize, Some(limit))),
            )?;
            self.rt
                .block_on(view.code_columns_gathered())?
                .ok_or_else(|| {
                    Error("the store did not serve term codes for this pattern".into())
                })?
        };
        let rows = columns[0].len().min(limit);
        for row in 0..rows {
            for (position, column) in columns.iter().enumerate() {
                out[4 * row + position] = column[row];
            }
        }
        Ok(rows)
    }
}
