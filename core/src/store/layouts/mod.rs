//! How quads become columns, and how a pattern becomes column constraints.
//!
//! This hub owns the vocabulary every layout is named through: the
//! [`LayoutStrategy`] a build requests, the `ResolvedLayout` a store actually
//! reads through (the strategy plus the state it cannot carry alone — a
//! Dictionary layout's term access), the `QuadPattern`/`PatternCodes` pattern
//! form, and the dispatch that turns either into per-layout column building,
//! chunk decoding, and constraint lowering.
//!
//! A leaf owns exactly one layout's physical schema — its column names, its
//! encode/decode loops (`default`, `typed_object`, `dictionary`) — and never
//! names another's. The Dictionary leaf is a folder module carrying the whole
//! term-dictionary subsystem (storage, ingest, residency) beside its
//! encode/decode paths.
//!
//! Secondary-index columns are *not* part of a layout. Every layout carries
//! whichever `_idx_*` columns the requested
//! [`IndexType`](crate::store::indexes::IndexType)s append, in that index's
//! own encoding for the layout; the index modules own those names.

use std::sync::Arc;

use oxrdf::{GraphName, NamedNode, NamedOrBlankNode, Quad, Term};
use vortex_array::arrays::struct_::{StructArray, StructArrayExt};
use vortex_array::arrays::{PrimitiveArray, VarBinViewArray};
use vortex_array::dtype::{DType, PType};
use vortex_array::scalar::Scalar;
use vortex_array::{ArrayRef, VortexSessionExecute};

use crate::error::{Result, VortexRdfError};
use crate::session::VORTEX_SESSION;
use crate::store::RawQuad;
use crate::store::array::StrColReader;

pub(crate) mod default;
pub(crate) mod dictionary;
pub(crate) mod typed_object;

use self::dictionary::TermDictionary;
pub(crate) use self::dictionary::access::DictAccess;
use self::typed_object::{COL_O_DATATYPE, COL_O_KIND, COL_O_LANG, COL_O_VALUE};
#[cfg(feature = "file-io")]
use crate::store::schema::PRIMARY_COLUMNS;
use crate::store::schema::{COL_G, COL_O, COL_P, COL_S};

/// Determines the columnar schema used to store RDF quads in the Vortex StructArray.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
#[cfg_attr(feature = "clap", derive(clap::ValueEnum))]
pub enum LayoutStrategy {
    /// ### `LayoutStrategy::Default` column schema
    ///
    /// All four quad fields stored as opaque UTF-8 strings in N-Triples
    /// serialization form. Vortex applies its own `DictLayout` internally to
    /// compress repeated values.
    ///
    /// | Column | Type              | Content                                                    |
    /// |--------|-------------------|------------------------------------------------------------|
    /// | `s`    | `VarBin<Utf8>`    | Subject: `<IRI>` or `_:blank`                              |
    /// | `p`    | `VarBin<Utf8>`    | Predicate: `<IRI>`                                         |
    /// | `o`    | `VarBin<Utf8>`    | Object: `<IRI>`, `_:blank`, `"lit"`, `"lit"@lang`, `"lit"^^<dt>` |
    /// | `g`    | `VarBin<Utf8>`    | Graph: `<IRI>`, `_:blank`, or `""` for DefaultGraph        |
    ///
    /// Each requested [`IndexType`] appends its own columns on top of these;
    /// see that enum's variant docs for the per-index column tables (they
    /// hold term strings here, as under `TypedObject`).
    ///
    /// [`IndexType`]: crate::store::indexes::IndexType
    Default,

    /// ### `LayoutStrategy::TypedObject` column schema
    /// Object column decomposed into typed sub-columns (kind, value, datatype, lang).
    /// Same as `Default` for `s`, `p`, `g`. The `o` column is decomposed into typed fields
    /// so that Vortex can apply datatype-appropriate encodings (delta, RLE, dictionary).
    ///
    /// | Column       | Type                  | Content                                     |
    /// |--------------|-----------------------|---------------------------------------------|
    /// | `s`          | `VarBin<Utf8>`        | (same as Default)                           |
    /// | `p`          | `VarBin<Utf8>`        | (same as Default)                           |
    /// | `o_kind`     | `PrimitiveArray<u8>`  | 0=IRI, 1=BlankNode, 2=PlainLiteral, 3=LangLiteral, 4=TypedLiteral |
    /// | `o_value`    | `VarBin<Utf8>`        | IRI string, blank node ID, or literal value |
    /// | `o_datatype` | `VarBin<Utf8>` (nullable) | Datatype IRI — non-null when `o_kind = 4`  |
    /// | `o_lang`     | `VarBin<Utf8>` (nullable) | Language tag — non-null when `o_kind = 3`  |
    /// | `g`          | `VarBin<Utf8>`        | (same as Default)                           |
    ///
    /// Index columns are unaffected by the object split: every requested
    /// [`IndexType`] appends the same term-string columns it would under
    /// `Default`, sorting whole object terms in N-Triples form.
    ///
    /// [`IndexType`]: crate::store::indexes::IndexType
    TypedObject,

    /// ### `LayoutStrategy::Dictionary` column schema
    /// All four quad fields stored as u32 codes into a single global term
    /// dictionary. In memory the dictionary lives beside the columns (see
    /// [`dictionary::term_dict`]); a serialized
    /// file carries it as the native
    /// container's `dictionary` child (see `crate::io::container`), so
    /// the quad columns stay bare.
    ///
    /// | Column        | Type                  | Content                                             |
    /// |---------------|-----------------------|-----------------------------------------------------|
    /// | `s`,`p`,`o`,`g` | `PrimitiveArray<u32>` | code = position of the term in the sorted dictionary |
    ///
    /// Term IDs are lexicographic ranks, so code comparisons are
    /// order-isomorphic to string comparisons (sorted builders keep the
    /// subject binary-search fast path on the u32 column).
    ///
    /// Every requested [`IndexType`] appends its usual columns, except that
    /// the term-valued ones hold u32 codes instead of strings (see
    /// `IndexType::append_dictionary_columns`); the `_idx_*_rid` row-id
    /// columns are `u32` under every layout.
    ///
    /// [`IndexType`]: crate::store::indexes::IndexType
    Dictionary,
}

/// The canonical strategy name: kebab-case (`"default"`, `"typed-object"`,
/// `"dictionary"`), the same spelling the `clap` derive exposes on the CLI —
/// so every frontend reports one vocabulary and a value printed by one can be
/// parsed by another.
impl std::fmt::Display for LayoutStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            LayoutStrategy::Default => "default",
            LayoutStrategy::TypedObject => "typed-object",
            LayoutStrategy::Dictionary => "dictionary",
        })
    }
}

/// Accepts exactly the canonical kebab-case names
/// [`Display`](std::fmt::Display) emits — `"default"`, `"typed-object"`,
/// `"dictionary"` — the one vocabulary every frontend shares.
impl std::str::FromStr for LayoutStrategy {
    type Err = VortexRdfError;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "default" => Ok(LayoutStrategy::Default),
            "typed-object" => Ok(LayoutStrategy::TypedObject),
            "dictionary" => Ok(LayoutStrategy::Dictionary),
            _ => Err(VortexRdfError::Deserialization(format!(
                "unknown layout strategy {s:?}; expected \"default\", \"typed-object\" or \
                 \"dictionary\""
            ))),
        }
    }
}

impl LayoutStrategy {
    /// Detect the column layout by inspecting the struct schema in the dtype,
    /// without materializing the array.
    pub(crate) fn from_dtype(dtype: &DType) -> LayoutStrategy {
        if let DType::Struct(fields, _) = dtype {
            // u32 code columns mean Dictionary; the dictionary itself rides
            // outside the schema, as the native container's dictionary child.
            if matches!(fields.field(COL_S), Some(DType::Primitive(ptype, _)) if ptype == PType::U32)
            {
                return LayoutStrategy::Dictionary;
            }
            // Presence of the typed-object kind column means TypedObject layout.
            if fields.names().iter().any(|n| n.as_ref() == COL_O_KIND) {
                return LayoutStrategy::TypedObject;
            }
        }
        // Neither marker column found: plain string columns, Default layout.
        LayoutStrategy::Default
    }

    /// Field names of the primary (non-index) columns for this layout.
    pub(crate) fn field_names(self) -> Vec<Arc<str>> {
        match self {
            LayoutStrategy::Default => default::field_names(),
            LayoutStrategy::TypedObject => typed_object::field_names(),
            LayoutStrategy::Dictionary => dictionary::field_names(),
        }
    }

    /// Build the primary column arrays for this layout from raw quads.
    /// An empty slice yields empty columns with the correct dtypes.
    ///
    /// Not available for `Dictionary`: encoding needs the global
    /// [`TermDictionary`], so Dictionary chunks are built by the dedicated
    /// [`dictionary::build_chunk`] pipeline instead.
    ///
    /// [`TermDictionary`]: dictionary::TermDictionary
    pub(crate) fn build_columns(self, quads: &[RawQuad]) -> Result<Vec<ArrayRef>> {
        match self {
            LayoutStrategy::Default => Ok(default::build_columns(quads)),
            LayoutStrategy::TypedObject => typed_object::build_columns(quads),
            LayoutStrategy::Dictionary => Err(crate::error::VortexRdfError::Serialization(
                "Dictionary layout chunks are built via the dictionary pipeline, \
                 not the generic column path"
                    .to_string(),
            )),
        }
    }
}

/// Query-time layout: the build-time [`LayoutStrategy`] resolved against a
/// constructed array, carrying any state intrinsic to the layout — for the
/// Dictionary layout, access to the global term dictionary. Holding the state
/// in the variant makes "Dictionary layout without a dictionary"
/// unrepresentable; [`DictAccess`] carries *how* the dictionary is reached
/// (resident today, file-backed planned).
#[derive(Clone)]
pub(crate) enum ResolvedLayout {
    Default,
    TypedObject,
    Dictionary(DictAccess),
}

/// One of the four term positions of a quad pattern.
#[derive(Clone, Copy)]
pub(crate) enum Role {
    S,
    P,
    O,
    G,
}

/// A quad pattern: the four term positions, each bound or free.
///
/// Travels as one value because every stage of a match needs all four together
/// — the fast paths clear whichever components they resolve, and whatever is
/// still bound afterwards is exactly what the residual filter must compare.
#[derive(Clone, Copy, Default)]
pub(crate) struct QuadPattern<'a> {
    pub(crate) subject: Option<&'a NamedOrBlankNode>,
    pub(crate) predicate: Option<&'a NamedNode>,
    pub(crate) object: Option<&'a Term>,
    pub(crate) graph: Option<&'a GraphName>,
}

impl<'a> QuadPattern<'a> {
    pub(crate) fn new(
        subject: Option<&'a NamedOrBlankNode>,
        predicate: Option<&'a NamedNode>,
        object: Option<&'a Term>,
        graph: Option<&'a GraphName>,
    ) -> Self {
        Self {
            subject,
            predicate,
            object,
            graph,
        }
    }

    /// Whether any component is still bound — i.e. whether there is anything
    /// left for a residual filter to do.
    pub(crate) fn any_bound(&self) -> bool {
        self.subject.is_some()
            || self.predicate.is_some()
            || self.object.is_some()
            || self.graph.is_some()
    }
}

/// A bound term of a quad pattern, tagged with the role it occupies.
///
/// Carrying the role with the term is what makes [`PatternCodes`] safe to share
/// across the match: a probe cannot be attributed to the wrong role, because
/// there is no way to name one separately from its term.
#[derive(Clone, Copy, Debug)]
pub(crate) enum TermRef<'a> {
    Subject(&'a NamedOrBlankNode),
    Predicate(&'a NamedNode),
    Object(&'a Term),
    Graph(&'a GraphName),
}

/// The term's N-Triples form, as the columns store it.
///
/// The default graph renders as the empty string — matching the `g` column —
/// rather than as oxrdf's own `Display`, which writes `DEFAULT`.
impl std::fmt::Display for TermRef<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TermRef::Subject(s) => write!(f, "{s}"),
            TermRef::Predicate(p) => write!(f, "{p}"),
            TermRef::Object(o) => write!(f, "{o}"),
            TermRef::Graph(GraphName::DefaultGraph) => Ok(()),
            TermRef::Graph(g) => write!(f, "{g}"),
        }
    }
}

impl TermRef<'_> {
    fn role(&self) -> Role {
        match self {
            TermRef::Subject(_) => Role::S,
            TermRef::Predicate(_) => Role::P,
            TermRef::Object(_) => Role::O,
            TermRef::Graph(_) => Role::G,
        }
    }

    /// Render into `out`, which is cleared first — the allocation-free form of
    /// [`Display`](std::fmt::Display), which defines what is written.
    fn write_nt(&self, out: &mut String) {
        use std::fmt::Write as _;
        out.clear();
        write!(out, "{self}").expect("writing to a String cannot fail");
    }
}

/// How a [`PatternCodes`] witness answers a probe its role cache does not
/// already hold — fixed at prelude time by the layout (and, for the
/// Dictionary layout, the dictionary's residency) the witness was prepared
/// under, so a probe can never be dispatched against the wrong access mode.
enum CodeResolver {
    /// `Default` layout: probes are the rendered N-Triples strings; there are
    /// no codes to resolve.
    Default,
    /// `TypedObject` layout: string probes like `Default`, except constraints
    /// decompose the object term into its typed sub-columns.
    TypedObject,
    /// Dictionary resident in memory: codes resolve on demand by in-memory
    /// binary search (memoized per role here, and across matches in the
    /// dictionary's own probe cache).
    Resident(Arc<TermDictionary>),
    /// Dictionary left in its file: the async prelude pre-resolved every
    /// bound role into the role cache, which is therefore the complete
    /// answer — there is deliberately no synchronous resolver to fall
    /// back to.
    #[cfg(feature = "file-io")]
    Preresolved,
}

/// One match's prepared pattern: the term → dictionary-code resolutions the
/// async prelude computed, the resolver later probes fall back to, and the
/// buffer bound terms are rendered into.
///
/// This is a *witness* type. Its only constructor is the async prelude
/// ([`prepare_pattern`](ResolvedLayout::prepare_pattern)) — the one place a
/// dictionary may perform I/O during a match — and every synchronous probe of
/// the match core is a method on the witness
/// ([`probe_scalar`](Self::probe_scalar), [`constraints`](Self::constraints)).
/// Holding one is therefore proof the prelude ran: "prelude skipped" is
/// unrepresentable rather than a defended runtime invariant, which is what
/// lets a file-backed dictionary confine its I/O to the prelude.
///
/// The role cache also serves memoization: `match_base` derives the same
/// term → code mapping in several stages — the unmatchable-pattern gate, the
/// sorted-subject probe, the index probes, and the residual constraints — so
/// caching per role keeps a fully-bound pattern to one dictionary search and
/// one render per bound term instead of one per stage.
///
/// The render goes into `scratch` rather than a fresh `String` per probe: a
/// term's N-Triples form is only needed long enough to search the dictionary
/// with it, so one buffer serves all four roles and every stage.
///
/// Scoped to a single `match_base`: the tail is matched under a *different*
/// layout (it stores terms as strings precisely so a term the base's dictionary
/// has never seen can still match), so a code cached here would be meaningless
/// there — the tail's match prepares its own witness.
pub(crate) struct PatternCodes {
    /// Per role: `None` = not resolved yet, `Some(None)` = resolved and absent
    /// from the dictionary (so the pattern cannot match).
    roles: [Option<Option<u32>>; 4],
    /// Reused render target for the bound terms; see the type docs.
    scratch: String,
    /// How probes beyond the prelude-seeded roles resolve; see the type docs.
    resolver: CodeResolver,
}

impl PatternCodes {
    /// A fresh witness resolving through `resolver` — reachable only from the
    /// async prelude (this module and the Dictionary layout's
    /// `DictAccess::resolve_pattern`), which is what makes holding a
    /// `PatternCodes` proof that the prelude ran.
    fn new(resolver: CodeResolver) -> Self {
        Self {
            roles: [None; 4],
            scratch: String::new(),
            resolver,
        }
    }

    /// The witness for a resident dictionary: probes beyond the prelude-seeded
    /// roles resolve by in-memory binary search.
    pub(in crate::store::layouts) fn resident(dict: Arc<TermDictionary>) -> Self {
        Self::new(CodeResolver::Resident(dict))
    }

    /// The witness for a file-backed dictionary: the prelude pre-resolves
    /// every bound role, and no synchronous resolver exists beyond them.
    #[cfg(feature = "file-io")]
    pub(in crate::store::layouts) fn preresolved() -> Self {
        Self::new(CodeResolver::Preresolved)
    }

    /// The code for `term`'s role, resolving it through `f` the first time
    /// only. `term` is rendered into the shared scratch buffer on a miss, and
    /// not rendered at all on a hit — how the prelude seeds the cache.
    pub(in crate::store::layouts) fn resolve(
        &mut self,
        term: TermRef<'_>,
        f: impl FnOnce(&str) -> Option<u32>,
    ) -> Option<u32> {
        let role = term.role() as usize;
        if let Some(cached) = self.roles[role] {
            return cached;
        }
        term.write_nt(&mut self.scratch);
        let resolved = f(&self.scratch);
        self.roles[role] = Some(resolved);
        resolved
    }

    /// `term`'s N-Triples form in the shared scratch buffer — for the layouts
    /// that probe with the string itself rather than a dictionary code, which
    /// have nothing to cache but still benefit from not allocating.
    fn render(&mut self, term: TermRef<'_>) -> &str {
        term.write_nt(&mut self.scratch);
        &self.scratch
    }

    /// The dictionary code for `term`'s role: the role cache first (the
    /// prelude seeded every bound role), then a resident dictionary's binary
    /// search for a witness that can run one synchronously.
    ///
    /// The error arm is the residual contract a witness cannot encode in its
    /// type: a probe for a role the prepared pattern never bound, on a
    /// witness with no synchronous resolver. `None` is reserved for
    /// "resolved and absent from the dictionary", so an unresolvable probe
    /// must be an error — silently answering `None` would fabricate an empty
    /// match result.
    fn code(&mut self, term: TermRef<'_>) -> Result<Option<u32>> {
        if let Some(cached) = self.roles[term.role() as usize] {
            return Ok(cached);
        }
        let CodeResolver::Resident(dict) = &self.resolver else {
            return Err(VortexRdfError::Deserialization(format!(
                "no synchronous code resolution for {term}: the async prelude resolves every \
                 bound role of the prepared pattern, and a file-backed dictionary cannot be \
                 probed outside it"
            )));
        };
        let dict = Arc::clone(dict);
        Ok(self.resolve(term, |s| dict.get_id(s)))
    }

    /// Scalar for probing a term column — the primary `s` column, a secondary
    /// index's value column, or a pushed-down filter equality. Under the
    /// Dictionary layout the term is translated to its u32 code
    /// (sorted-dictionary codes preserve lexicographic order); `None` means
    /// the term is absent from the dictionary and matches nothing. The string
    /// layouts probe with the rendered term itself.
    ///
    /// Memoized per role, so one match performs a single dictionary search
    /// and a single render per bound term however many stages and indexes ask
    /// for the same probe. There is deliberately no uncached variant: every
    /// probe in a match is for one of the pattern's four terms, so an
    /// uncached one could only ever repeat work already done.
    pub(crate) fn probe_scalar(&mut self, term: TermRef<'_>) -> Result<Option<Scalar>> {
        if matches!(
            self.resolver,
            CodeResolver::Default | CodeResolver::TypedObject
        ) {
            return Ok(Some(Scalar::from(self.render(term))));
        }
        Ok(self.code(term)?.map(Scalar::from))
    }

    /// Compile a quad pattern into per-column equality constraints — the
    /// layout-specific term → (column, scalar) mapping, the single source of
    /// truth consumed by both the in-memory mask scan and the pushed-down
    /// file filter.
    pub(crate) fn constraints(
        &mut self,
        subject: Option<&NamedOrBlankNode>,
        predicate: Option<&NamedNode>,
        object: Option<&Term>,
        graph: Option<&GraphName>,
    ) -> Result<Constraints> {
        let mut eqs: Vec<(&'static str, Scalar)> = Vec::new();
        match self.resolver {
            CodeResolver::Default => {
                if let Some(s) = subject {
                    eqs.push((COL_S, Scalar::from(self.render(TermRef::Subject(s)))));
                }
                if let Some(p) = predicate {
                    eqs.push((COL_P, Scalar::from(self.render(TermRef::Predicate(p)))));
                }
                if let Some(o) = object {
                    eqs.push((COL_O, Scalar::from(self.render(TermRef::Object(o)))));
                }
                if let Some(g) = graph {
                    eqs.push((COL_G, Scalar::from(self.render(TermRef::Graph(g)))));
                }
            }
            CodeResolver::TypedObject => {
                if let Some(s) = subject {
                    eqs.push((COL_S, Scalar::from(self.render(TermRef::Subject(s)))));
                }
                if let Some(p) = predicate {
                    eqs.push((COL_P, Scalar::from(self.render(TermRef::Predicate(p)))));
                }
                if let Some(o) = object {
                    let (kind, value, dt, lang) = typed_object::decompose_object(o);
                    eqs.push((COL_O_KIND, Scalar::from(kind)));
                    eqs.push((COL_O_VALUE, Scalar::from(value.as_str())));
                    if let Some(dt_str) = dt {
                        eqs.push((COL_O_DATATYPE, Scalar::from(dt_str.as_str())));
                    }
                    if let Some(lang_str) = lang {
                        eqs.push((COL_O_LANG, Scalar::from(lang_str.as_str())));
                    }
                }
                if let Some(g) = graph {
                    eqs.push((COL_G, Scalar::from(self.render(TermRef::Graph(g)))));
                }
            }
            // Dictionary witness (either residency): resolve every bound term
            // to its code — a term absent from the dictionary cannot match
            // any quad.
            _ => {
                macro_rules! bind {
                    ($opt:expr, $ctor:expr, $field:expr) => {
                        if let Some(term) = $opt {
                            match self.code($ctor(term))? {
                                Some(id) => eqs.push(($field, Scalar::from(id))),
                                None => return Ok(Constraints::AlwaysFalse),
                            }
                        }
                    };
                }
                bind!(subject, TermRef::Subject, COL_S);
                bind!(predicate, TermRef::Predicate, COL_P);
                bind!(object, TermRef::Object, COL_O);
                bind!(graph, TermRef::Graph, COL_G);
            }
        }
        Ok(Constraints::Eq(eqs))
    }
}

/// Column equality constraints a quad pattern compiles to under a given
/// layout: the single source of truth consumed by both the in-memory mask
/// scan and the pushed-down file filter in `match_pattern`.
pub(crate) enum Constraints {
    /// A bound term cannot match any quad (e.g. absent from the dictionary).
    AlwaysFalse,
    /// Conjunction of per-column equalities; empty means unconstrained.
    Eq(Vec<(&'static str, Scalar)>),
}

impl ResolvedLayout {
    /// The build-time strategy tag this layout was resolved from.
    pub(crate) fn strategy(&self) -> LayoutStrategy {
        match self {
            ResolvedLayout::Default => LayoutStrategy::Default,
            ResolvedLayout::TypedObject => LayoutStrategy::TypedObject,
            ResolvedLayout::Dictionary(_) => LayoutStrategy::Dictionary,
        }
    }

    /// Field names of the primary (non-index) columns.
    #[cfg(feature = "file-io")]
    pub(crate) fn primary_column_names(&self) -> Vec<&'static str> {
        match self {
            ResolvedLayout::Default | ResolvedLayout::Dictionary(_) => PRIMARY_COLUMNS.to_vec(),
            ResolvedLayout::TypedObject => {
                vec![
                    COL_S,
                    COL_P,
                    COL_O_KIND,
                    COL_O_VALUE,
                    COL_O_DATATYPE,
                    COL_O_LANG,
                    COL_G,
                ]
            }
        }
    }

    /// Decode a StructArray chunk into quads. Dictionary chunks are decoded
    /// through the layout's own dictionary.
    pub(crate) fn decode_chunk(&self, chunk: &ArrayRef) -> Vec<Result<Quad>> {
        match self {
            ResolvedLayout::Default => default::decode_chunk(chunk),
            ResolvedLayout::TypedObject => typed_object::decode_chunk(chunk),
            ResolvedLayout::Dictionary(access) => match access.resident() {
                Some(dict) => dictionary::decode_chunk(chunk, dict),
                // Defensive: the read paths route file-backed stores through
                // `decode_chunk_async`, which resolves each chunk's codes with
                // a scan; reaching here means one of them didn't.
                None => vec![Err(VortexRdfError::Deserialization(
                    "a file-backed dictionary decodes chunks through the async read path"
                        .to_string(),
                ))],
            },
        }
    }

    /// [`decode_chunk`](Self::decode_chunk) with the file-backed Dictionary
    /// case handled: the chunk's distinct codes are resolved to terms with one
    /// dictionary scan, and the chunk decodes against that map. Every other
    /// layout (and a resident dictionary) takes the sync path unchanged.
    #[cfg(feature = "file-io")]
    pub(crate) async fn decode_chunk_async(&self, chunk: &ArrayRef) -> Vec<Result<Quad>> {
        if let ResolvedLayout::Dictionary(DictAccess::FileBacked(fb)) = self {
            // Interpreting the chunk's code columns is layout logic; the
            // dictionary contributes only the code→string translation
            // (`resolve_terms`), so the composition lives here.
            let codes = match dictionary::unique_codes(chunk) {
                Ok(codes) => codes,
                Err(e) => return vec![Err(e)],
            };
            let terms: std::collections::HashMap<u32, String> = match fb.resolve_terms(&codes).await
            {
                Ok(terms) => codes.into_iter().zip(terms).collect(),
                Err(e) => return vec![Err(e)],
            };
            return dictionary::decode_chunk_mapped(chunk, &terms);
        }
        self.decode_chunk(chunk)
    }

    /// Prepare `pattern` for the synchronous match core: pre-resolve every
    /// bound term the layout probes by code, and hand back the
    /// [`PatternCodes`] witness the core's probes run on. The one point in a
    /// match where a dictionary may perform I/O (see
    /// [`DictAccess::resolve_pattern`]) — which is why this prelude is the
    /// witness's only constructor. The string layouts have nothing to
    /// resolve, so their preparation never suspends.
    pub(crate) async fn prepare_pattern(&self, pattern: QuadPattern<'_>) -> Result<PatternCodes> {
        match self {
            ResolvedLayout::Default => Ok(PatternCodes::new(CodeResolver::Default)),
            ResolvedLayout::TypedObject => Ok(PatternCodes::new(CodeResolver::TypedObject)),
            ResolvedLayout::Dictionary(access) => access.resolve_pattern(pattern).await,
        }
    }

    /// Decode an array of this layout's rows back into [`RawQuad`]s — each
    /// term in its N-Triples string form, without an oxrdf parse round-trip.
    ///
    /// The inverse of the build-time column encoding, for the operations that
    /// rebuild a store from its quads (compaction, and reads that must merge a
    /// string tail into a Dictionary-encoded base): Default reads its four
    /// string columns verbatim, TypedObject recomposes the object term from
    /// its typed sub-columns, and Dictionary resolves each u32 code through
    /// this layout's term dictionary.
    pub(crate) fn raw_quads(&self, rows: &ArrayRef) -> Result<Vec<RawQuad>> {
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let struct_arr = rows
            .clone()
            .execute::<StructArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?;
        let (s, p, o, g) = match self {
            ResolvedLayout::Default => (
                read_string_column(&struct_arr, COL_S)?,
                read_string_column(&struct_arr, COL_P)?,
                read_string_column(&struct_arr, COL_O)?,
                read_string_column(&struct_arr, COL_G)?,
            ),
            ResolvedLayout::TypedObject => (
                read_string_column(&struct_arr, COL_S)?,
                read_string_column(&struct_arr, COL_P)?,
                typed_object::object_terms(&struct_arr)?,
                read_string_column(&struct_arr, COL_G)?,
            ),
            ResolvedLayout::Dictionary(access) => {
                let dict = access.resident().ok_or_else(|| {
                    VortexRdfError::Deserialization(
                        "a file-backed dictionary reconstructs rows through raw_quads_async"
                            .to_string(),
                    )
                })?;
                (
                    dictionary::decode_code_column(dict, &read_u32_column(&struct_arr, COL_S)?)?,
                    dictionary::decode_code_column(dict, &read_u32_column(&struct_arr, COL_P)?)?,
                    dictionary::decode_code_column(dict, &read_u32_column(&struct_arr, COL_O)?)?,
                    dictionary::decode_code_column(dict, &read_u32_column(&struct_arr, COL_G)?)?,
                )
            }
        };
        Ok(s.into_iter()
            .zip(p)
            .zip(o)
            .zip(g)
            .map(|(((s, p), o), g)| RawQuad { s, p, o, g })
            .collect())
    }

    /// [`raw_quads`](Self::raw_quads) with the file-backed Dictionary case
    /// handled: the whole dictionary is lifted resident transiently (the
    /// callers — compaction and tail-merge re-encoding — touch most of it
    /// anyway), then the sync decode runs unchanged.
    #[cfg(feature = "file-io")]
    pub(crate) async fn raw_quads_async(&self, rows: &ArrayRef) -> Result<Vec<RawQuad>> {
        if let ResolvedLayout::Dictionary(access) = self
            && access.is_file_backed()
        {
            let dict = access.ensure_resident().await?;
            let resident = ResolvedLayout::Dictionary(DictAccess::Resident(dict));
            return resident.raw_quads(rows);
        }
        self.raw_quads(rows)
    }
}

/// Read a UTF-8 string column into owned term strings, one per row.
fn read_string_column(struct_arr: &StructArray, name: &str) -> Result<Vec<String>> {
    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let col = struct_arr
        .unmasked_field_by_name(name)
        .map_err(VortexRdfError::Vortex)?
        .clone()
        .execute::<VarBinViewArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    let reader = StrColReader::new(&col);
    (0..col.len())
        .map(|i| reader.str_at(i).map(str::to_string))
        .collect()
}

/// Read a u32 code column into owned codes, one per row.
fn read_u32_column(struct_arr: &StructArray, name: &str) -> Result<Vec<u32>> {
    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let col = struct_arr
        .unmasked_field_by_name(name)
        .map_err(VortexRdfError::Vortex)?
        .clone()
        .execute::<PrimitiveArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    Ok(col.as_slice::<u32>().to_vec())
}
