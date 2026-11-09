//! The benchmark dataset — one definition, shared by every Rust bench target.
//!
//! A port of `python/bench/datasets.py` (itself a port of `js/bench/datasets.ts`),
//! kept algorithmically identical: same moduli, same term spellings, same index-0
//! probes. That is what lets a row on the Rust tab measure the same data as its
//! twin on the Python and JavaScript tabs, and what lets the instrumented suite's
//! `S`/`P`/`O`/`SP`/`PO`/`SPO` cells be read beside the comparative suite's — they resolve
//! the same probe over the same term shape at the same row count, differing only
//! in how the rows are delivered (generated in-process here, read from an
//! N-Triples file there). Any drift here silently breaks that.
//!
//! # Cardinality
//!
//! Term cardinality is a knob independent of row count, sized so term handling
//! (dictionaries, interning, string storage) is exercised: ten triples per
//! subject, a 32-term predicate vocabulary, one distinct object per two rows,
//! `WANT_GRAPHS` named graphs.
//!
//! # Uniqueness
//!
//! Term indices are `i % k` per role, so quad `i` maps to the residue tuple
//! `(i % n_subj, i % n_pred, i % n_obj, i % n_graph)`. By the Chinese Remainder
//! Theorem that map is injective over `i < lcm(..)`, so making the four moduli
//! pairwise coprime — their lcm is then their product — and checking the product
//! covers `n` guarantees every generated quad is distinct with no dedupe set,
//! which at these row counts would itself dominate memory.
//!
//! Deliberate consequence: index 0 exists for every role, so row 0 satisfies all
//! four single-term probes and no pattern can silently measure a zero-row query.

use oxrdf::{GraphName, Literal, NamedNode, NamedOrBlankNode, Quad, Term};

const BASE: &str = "http://data.example.org";

/// Distinct subjects per row: 0.1 is ten triples describing each resource.
const SUBJECT_RATIO: f64 = 0.1;
/// Distinct predicates — a small closed vocabulary, as in real data.
const PREDICATES: usize = 32;
/// Distinct objects per row.
const OBJECT_RATIO: f64 = 0.5;
/// Objects out of every ten that are literals rather than IRIs
/// (`literal_frac = 0.4` in the Python and JavaScript generators).
const LITERALS_PER_TEN: usize = 4;

/// Named graphs the *generated* dataset asks for, before the coprimality nudge.
///
/// The comparative suite's N-Quads file requests the same 8 (`moduli` in
/// `bench-compare/benches/compare.rs`), so the graph role has the same shape
/// wherever a bench binds one. It cannot be 1: a single-graph dataset has no
/// graph term to bind, which would quietly turn `G` into a second full scan
/// and `SPOG` into `SPO`.
pub const WANT_GRAPHS: usize = 8;

/// Per-role term counts for an `n`-row dataset.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Moduli {
    pub n_subj: usize,
    pub n_pred: usize,
    pub n_obj: usize,
    pub n_graph: usize,
}

impl Moduli {
    /// Distinct terms across all four roles — what the dictionary has to hold.
    pub fn terms(&self) -> usize {
        // The default graph is a term too, and it is not one of the named graphs.
        self.n_subj + self.n_pred + self.n_obj + if self.n_graph == 1 { 1 } else { self.n_graph }
    }

    /// Rows an `i % k`-selected term matches in an `n`-row dataset. Ceiling, not
    /// division: the last partial period still contributes its `i ≡ 0` row.
    fn rows_every(n: usize, k: usize) -> usize {
        if n == 0 { 0 } else { (n - 1) / k + 1 }
    }

    /// Check a store's answer to `pattern` over `n` rows against the
    /// selectivity the `#dataset` line reports.
    pub fn assert_matched(&self, n: usize, pattern: Pattern, got: usize) {
        assert_eq!(
            got,
            self.matched_rows(n, pattern),
            "dataset selectivity drifted for {pattern:?}"
        );
    }

    /// Rows each probe pattern matches over `n` rows — the selectivity that makes
    /// a timing interpretable.
    fn matched_rows(&self, n: usize, pattern: Pattern) -> usize {
        // Saturating, not wrapping: a conjunction's period exceeds usize long
        // before it exceeds `n`, and all that matters then is that it is bigger.
        let both = |a: usize, b: usize| a.saturating_mul(b);
        match pattern {
            Pattern::S => Self::rows_every(n, self.n_subj),
            Pattern::SP => Self::rows_every(n, both(self.n_subj, self.n_pred)),
            Pattern::SPO => Self::rows_every(n, both(both(self.n_subj, self.n_pred), self.n_obj)),
            Pattern::P => Self::rows_every(n, self.n_pred),
            Pattern::O => Self::rows_every(n, self.n_obj),
            // Pairwise coprime, so the lcm of any subset is its product.
            Pattern::PO => Self::rows_every(n, both(self.n_pred, self.n_obj)),
            Pattern::G => Self::rows_every(n, self.n_graph),
            Pattern::SPOG => Self::rows_every(
                n,
                both(
                    both(self.n_subj, self.n_pred),
                    both(self.n_obj, self.n_graph),
                ),
            ),
        }
    }
}

fn gcd(mut a: usize, mut b: usize) -> usize {
    while b != 0 {
        let t = b;
        b = a % b;
        a = t;
    }
    a
}

/// Per-role term counts: the requested cardinalities nudged up until pairwise
/// coprime. `graphs` is the *wanted* named-graph count before that nudge — 8
/// becomes 9 at 32,768 rows and 17 at 1,048,576 (where even the 32-predicate
/// vocabulary nudges to 33), which is why nothing here reads a remembered
/// constant.
pub fn moduli(n: usize, graphs: usize) -> Moduli {
    let want = [
        (((n as f64) * SUBJECT_RATIO).round() as usize).max(1),
        PREDICATES.max(1),
        (((n as f64) * OBJECT_RATIO).round() as usize).max(1),
        graphs.max(1),
    ];
    let mut got: Vec<usize> = Vec::with_capacity(4);
    for w in want {
        let mut k = w;
        while got.iter().any(|&g| gcd(g, k) != 1) {
            k += 1;
        }
        got.push(k);
    }
    let m = Moduli {
        n_subj: got[0],
        n_pred: got[1],
        n_obj: got[2],
        n_graph: got[3],
    };
    // Product == lcm because they are pairwise coprime; see the CRT note above.
    // Compare logs: the product overflows long before it matters numerically.
    let log_product: f64 = got.iter().map(|&k| (k as f64).ln()).sum();
    assert!(
        n <= 1 || log_product >= (n as f64).ln(),
        "dataset cardinality too low for {n} distinct quads: \
         {}x{}x{}x{} cannot cover it",
        m.n_subj,
        m.n_pred,
        m.n_obj,
        m.n_graph
    );
    m
}

// ── term spellings (must match datasets.py / datasets.ts exactly) ────────────

pub fn subject_iri(i: usize) -> String {
    format!("{BASE}/resource/2026/subject/{i:09}")
}

pub fn predicate_iri(i: usize) -> String {
    format!("{BASE}/ontology/2026/property/{i:04}")
}

pub fn graph_iri(i: usize) -> String {
    format!("{BASE}/graph/2026/named/{i:06}")
}

/// Object `i`, in whichever of the two forms the caller needs: the in-process
/// generators build `oxrdf` terms, the file writers in
/// `bench-compare/benches/compare.rs` write N-Triples text, and both must agree
/// on the *same* term or the two suites stop measuring the same data.
pub enum ObjectTerm {
    Iri(String),
    /// A literal body carrying no quote, backslash or newline, so N-Triples
    /// quoting is a bare wrap — otherwise escaping cost lands inside every
    /// external adapter's parse timing. Escape coverage is
    /// `generate_literal_rdf_data_stream`'s job, on its own dataset.
    Literal(String),
}

/// Objects alternate IRI/literal deterministically, in `LITERALS_PER_TEN`
/// proportion. Index 0 is a literal, so the `O` probe binds one.
pub fn object_term(i: usize) -> ObjectTerm {
    if i % 10 < LITERALS_PER_TEN {
        ObjectTerm::Literal(format!("descriptive object value number {i:09}"))
    } else {
        ObjectTerm::Iri(format!("{BASE}/resource/2026/object/{i:09}"))
    }
}

impl ObjectTerm {
    pub fn into_oxrdf(self) -> Term {
        match self {
            Self::Iri(iri) => Term::NamedNode(NamedNode::new_unchecked(iri)),
            Self::Literal(value) => Term::Literal(Literal::new_simple_literal(value)),
        }
    }

    /// The N-Triples spelling — angle-bracketed IRI or quoted literal.
    pub fn to_ntriples(&self) -> String {
        match self {
            Self::Iri(iri) => format!("<{iri}>"),
            Self::Literal(value) => format!("\"{value}\""),
        }
    }
}

// ── rows ────────────────────────────────────────────────────────────────────

/// Row `i` of the generated dataset, under moduli `m`: one definition of what a
/// row *is*, shared by the in-process generator and by the mutation benchmarks,
/// whose delete batch has to name rows the store actually holds.
pub fn dataset_quad(i: usize, m: Moduli) -> Quad {
    Quad::new(
        NamedOrBlankNode::NamedNode(NamedNode::new_unchecked(subject_iri(i % m.n_subj))),
        NamedNode::new_unchecked(predicate_iri(i % m.n_pred)),
        object_term(i % m.n_obj).into_oxrdf(),
        GraphName::NamedNode(NamedNode::new_unchecked(graph_iri(i % m.n_graph))),
    )
}

/// The first `take` rows of an `n`-row dataset generated under moduli `m` —
/// the delete batch of every mutation bench, so each delete tombstones a row
/// the store holds.
pub fn dataset_prefix(n: usize, m: Moduli, take: usize) -> Vec<Quad> {
    (0..take.min(n)).map(|i| dataset_quad(i, m)).collect()
}

/// A batch in a namespace disjoint from the dataset's, so every add is a
/// genuine insert. Same spelling as `fresh_quads` in `python/bench/datasets.py`,
/// down to the default graph, so an add cell on every tab inserts the same
/// terms.
pub fn fresh_quads(n: usize) -> Vec<Quad> {
    (0..n)
        .map(|i| {
            Quad::new(
                NamedNode::new_unchecked(format!("{BASE}/fresh/2026/subject/{i:09}")),
                NamedNode::new_unchecked(format!("{BASE}/fresh/2026/property/0000")),
                Term::NamedNode(NamedNode::new_unchecked(format!(
                    "{BASE}/fresh/2026/object/{i:09}"
                ))),
                GraphName::DefaultGraph,
            )
        })
        .collect()
}

// ── probe patterns ──────────────────────────────────────────────────────────

// Each variant names the bound components by letter (Subject/Predicate/
// Object/Graph), so `SPOG` is consistent with its siblings, not a word to
// re-case.
#[allow(clippy::upper_case_acronyms)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Pattern {
    S,
    SP,
    SPO,
    P,
    O,
    PO,
    G,
    SPOG,
}

pub const PATTERNS: &[Pattern] = &[
    Pattern::S,
    Pattern::SP,
    Pattern::SPO,
    Pattern::P,
    Pattern::O,
    Pattern::PO,
    Pattern::G,
    Pattern::SPOG,
];

/// The bound terms for a pattern, all at index 0 — the same probes
/// `dataset_probes` builds for the Python tab and `datasetProbes` for the
/// JavaScript one.
#[allow(clippy::type_complexity)]
pub fn terms_for(
    pattern: Pattern,
) -> (
    Option<NamedOrBlankNode>,
    Option<NamedNode>,
    Option<Term>,
    Option<GraphName>,
) {
    let s = || NamedOrBlankNode::NamedNode(NamedNode::new_unchecked(subject_iri(0)));
    let p = || NamedNode::new_unchecked(predicate_iri(0));
    let o = || object_term(0).into_oxrdf();
    let g = || GraphName::NamedNode(NamedNode::new_unchecked(graph_iri(0)));

    match pattern {
        Pattern::S => (Some(s()), None, None, None),
        Pattern::SP => (Some(s()), Some(p()), None, None),
        Pattern::SPO => (Some(s()), Some(p()), Some(o()), None),
        Pattern::P => (None, Some(p()), None, None),
        Pattern::O => (None, None, Some(o()), None),
        Pattern::PO => (None, Some(p()), Some(o()), None),
        Pattern::G => (None, None, None, Some(g())),
        Pattern::SPOG => (Some(s()), Some(p()), Some(o()), Some(g())),
    }
}

/// The `#dataset {...}` JSON line a bench process prints once before timing:
/// row count, per-role moduli, distinct terms, and the rows each probe matches.
/// The dashboard reads its selectivity figures from this line (divan's parser
/// ignores it).
pub fn shape_line(n: usize, graphs: usize) -> String {
    let m = moduli(n, graphs);
    let rows = |p: Pattern| m.matched_rows(n, p);
    format!(
        "#dataset {{\"quads\":{},\"subjects\":{},\"predicates\":{},\"objects\":{},\
         \"graphs\":{},\"terms\":{},\"matched\":{{\"S\":{},\"SP\":{},\"SPO\":{},\"P\":{},\
         \"O\":{},\"PO\":{},\"G\":{},\"SPOG\":{}}}}}",
        n,
        m.n_subj,
        m.n_pred,
        m.n_obj,
        m.n_graph,
        m.terms(),
        rows(Pattern::S),
        rows(Pattern::SP),
        rows(Pattern::SPO),
        rows(Pattern::P),
        rows(Pattern::O),
        rows(Pattern::PO),
        rows(Pattern::G),
        rows(Pattern::SPOG),
    )
}
