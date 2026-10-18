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
//! Secondary indexes are *not* part of a layout. They are built as their own
//! children beside a layout's quad rows, in that index's own encoding for the
//! layout (term strings, or the Dictionary layout's term codes); the index
//! modules own those columns and their names — see
//! [`IndexType`](crate::store::indexes::IndexType).

use std::sync::Arc;

#[cfg(feature = "file-io")]
use futures::FutureExt as _;
#[cfg(feature = "file-io")]
use futures::future::{self, BoxFuture};
use oxrdf::{GraphName, NamedNode, NamedOrBlankNode, Quad, Term};
use vortex_array::arrays::struct_::StructArray;
use vortex_array::arrays::{PrimitiveArray, VarBinViewArray};
use vortex_array::dtype::DType;
use vortex_array::scalar::Scalar;
use vortex_array::{ArrayRef, VortexSessionExecute};

use crate::common::quad::SharedQuad;
use crate::error::{Result, VortexRdfError};
use crate::session::VORTEX_SESSION;
use crate::store::RawQuad;
use crate::store::array::{StrColReader, field_as};

pub(crate) mod default;
pub(crate) mod dictionary;
pub(crate) mod typed_object;

pub(crate) use self::dictionary::DictAccess;
#[cfg(feature = "file-io")]
use self::dictionary::FileBackedDict;
use crate::store::schema::{
    CODE_PTYPE, COL_G, COL_O, COL_O_DATATYPE, COL_O_KIND, COL_O_LANG, COL_O_VALUE, COL_P, COL_S,
    PRIMARY_COLUMNS, QuadColumn, TermCode,
};

/// Determines the columnar schema used to store RDF quads in the Vortex StructArray.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
#[cfg_attr(feature = "clap", derive(clap::ValueEnum))]
pub enum LayoutStrategy {
    /// All four quad fields as opaque N-Triples strings.
    ///
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

    /// Object column split into typed sub-columns (kind, value, datatype, lang).
    ///
    /// ### `LayoutStrategy::TypedObject` column schema
    ///
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

    /// All four quad fields as term codes into one shared term dictionary.
    ///
    /// ### `LayoutStrategy::Dictionary` column schema
    ///
    /// All four quad fields stored as [`TermCode`]s (`u64`) into a single global term
    /// dictionary. In memory the dictionary lives beside the columns (see
    /// `dictionary::term_dict`); a serialized
    /// file carries it as the native
    /// container's `dictionary` child (see `crate::io::container`), so
    /// the quad columns stay bare.
    ///
    /// | Column        | Type                  | Content                                             |
    /// |---------------|-----------------------|-----------------------------------------------------|
    /// | `s`,`p`,`o`,`g` | `PrimitiveArray<u64>` | code = position of the term in the sorted dictionary |
    ///
    /// Term codes are lexicographic ranks, so code comparisons are
    /// order-isomorphic to string comparisons (sorted builders keep the
    /// subject binary-search fast path on the code column).
    ///
    /// Every requested [`IndexType`] builds its usual children, except that
    /// their term-valued columns hold codes instead of strings; the row-id
    /// columns are `u64` ([`RowId`](crate::store::RowId)) under every layout.
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
/// `"dictionary"` — the one vocabulary every frontend shares. Any other name
/// is an [`InvalidInput`](VortexRdfError::InvalidInput) error.
impl std::str::FromStr for LayoutStrategy {
    type Err = VortexRdfError;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "default" => Ok(LayoutStrategy::Default),
            "typed-object" => Ok(LayoutStrategy::TypedObject),
            "dictionary" => Ok(LayoutStrategy::Dictionary),
            _ => Err(VortexRdfError::InvalidInput(format!(
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
            // Code columns mean Dictionary; the dictionary itself rides
            // outside the schema, as the native container's dictionary child.
            // Another integer width is no layout of this crate's: the open
            // refuses it (`check_id_columns`) before anything reads it.
            if matches!(fields.field(COL_S), Some(DType::Primitive(ptype, _)) if ptype == CODE_PTYPE)
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

    /// Names of the primary (non-index) columns for this layout, in schema
    /// order.
    pub(crate) fn primary_column_names(self) -> &'static [&'static str] {
        match self {
            LayoutStrategy::Default => default::COLUMNS,
            LayoutStrategy::TypedObject => typed_object::COLUMNS,
            LayoutStrategy::Dictionary => &PRIMARY_COLUMNS,
        }
    }

    /// [`primary_column_names`](Self::primary_column_names) as owned field
    /// names, for building the struct dtype.
    pub(crate) fn field_names(self) -> Vec<Arc<str>> {
        self.primary_column_names()
            .iter()
            .map(|&n| n.into())
            .collect()
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
/// (resident, or left in its file).
#[derive(Clone)]
pub(crate) enum ResolvedLayout {
    Default,
    TypedObject,
    Dictionary(DictAccess),
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

    /// The bound terms, each tagged with its role, in `s`, `p`, `o`, `g`
    /// order.
    pub(crate) fn bound_roles(&self) -> impl Iterator<Item = TermRef<'a>> {
        [
            self.subject.map(TermRef::Subject),
            self.predicate.map(TermRef::Predicate),
            self.object.map(TermRef::Object),
            self.graph.map(TermRef::Graph),
        ]
        .into_iter()
        .flatten()
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
    /// The column the term's role occupies.
    fn role(&self) -> QuadColumn {
        match self {
            TermRef::Subject(_) => QuadColumn::S,
            TermRef::Predicate(_) => QuadColumn::P,
            TermRef::Object(_) => QuadColumn::O,
            TermRef::Graph(_) => QuadColumn::G,
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

/// One match's prepared pattern: the term → dictionary-code resolutions the
/// async prelude computed, the layout later probes resolve through, and the
/// buffer bound terms are rendered into.
///
/// This is a *witness* type. Its only constructor is the async prelude
/// ([`prepare_pattern`](ResolvedLayout::prepare_pattern)) — the one place a
/// dictionary may perform I/O during a match — and every synchronous probe of
/// the match core is a method on the witness
/// ([`probe_scalar`](Self::probe_scalar), [`constraints`](Self::constraints)).
/// Holding one is therefore proof the prelude ran — "prelude skipped" is
/// unrepresentable — which is what lets a file-backed dictionary confine
/// its I/O to the prelude.
///
/// The role cache also serves memoization: `match_base` derives the same
/// term → code mapping in several stages — the unmatchable-pattern gate, the
/// sorted-subject probe, the index probes, and the residual constraints — so
/// caching per role keeps a fully-bound pattern to one dictionary search and
/// one render per bound term instead of one per stage.
///
/// The render goes into `scratch`: a term's N-Triples form is only needed
/// long enough to search the dictionary with it, so one buffer serves all
/// four roles and every stage.
///
/// Scoped to a single `match_base`: the tail is matched under a *different*
/// layout (it stores terms as strings precisely so a term the base's dictionary
/// has never seen can still match), so a code cached here would be meaningless
/// there — the tail's match prepares its own witness.
pub(crate) struct PatternCodes<'a> {
    /// Per role: `None` = not resolved yet, `Some(None)` = resolved and absent
    /// from the dictionary (so the pattern cannot match).
    roles: [Option<Option<TermCode>>; 4],
    /// Reused render target for the bound terms; see the type docs.
    scratch: String,
    /// The layout the prelude prepared the pattern under, which fixes how a
    /// probe the role cache does not already hold is answered — so a probe
    /// can never be dispatched against the wrong access mode: the string
    /// layouts probe with the rendered term itself (`TypedObject`'s
    /// constraints decomposing the object into its typed sub-columns); a
    /// resident dictionary resolves on demand by in-memory binary search
    /// (memoized per role here); for a file-backed one the prelude
    /// pre-resolved every bound role, so the role cache is the complete
    /// answer — there is deliberately no synchronous resolver to fall back
    /// to.
    layout: &'a ResolvedLayout,
}

impl<'a> PatternCodes<'a> {
    /// A fresh witness prepared under `layout` — reachable only from the
    /// async prelude ([`prepare_pattern`](ResolvedLayout::prepare_pattern)),
    /// which is what makes holding a `PatternCodes` proof that the prelude
    /// ran.
    fn new(layout: &'a ResolvedLayout) -> Self {
        Self {
            roles: [None; 4],
            scratch: String::new(),
            layout,
        }
    }

    /// The code for `term`'s role, resolving it through `f` the first time
    /// only. `term` is rendered into the shared scratch buffer on a miss, and
    /// not rendered at all on a hit — how the prelude seeds the cache.
    pub(in crate::store::layouts) fn resolve(
        &mut self,
        term: TermRef<'_>,
        f: impl FnOnce(&str) -> Option<TermCode>,
    ) -> Option<TermCode> {
        let role = term.role().index();
        if let Some(cached) = self.roles[role] {
            return cached;
        }
        term.write_nt(&mut self.scratch);
        let resolved = f(&self.scratch);
        self.roles[role] = Some(resolved);
        resolved
    }

    /// `term`'s N-Triples form in the shared scratch buffer — for the layouts
    /// that probe with the string itself (no dictionary code to cache).
    fn render(&mut self, term: TermRef<'_>) -> &str {
        term.write_nt(&mut self.scratch);
        &self.scratch
    }

    /// Whether this witness probes with the rendered term itself rather than
    /// a dictionary code: the `Default` and `TypedObject` layouts.
    fn probes_by_string(&self) -> bool {
        !matches!(self.layout, ResolvedLayout::Dictionary(_))
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
    fn code(&mut self, term: TermRef<'_>) -> Result<Option<TermCode>> {
        if let Some(cached) = self.roles[term.role().index()] {
            return Ok(cached);
        }
        let layout = self.layout;
        let ResolvedLayout::Dictionary(DictAccess::Resident(dict)) = layout else {
            return Err(VortexRdfError::Deserialization(format!(
                "no synchronous code resolution for {term}: the async prelude resolves every \
                 bound role of the prepared pattern, and a file-backed dictionary cannot be \
                 probed outside it"
            )));
        };
        Ok(self.resolve(term, |s| dict.encode_exact(s)))
    }

    /// Scalar for probing a term column — the primary `s` column, a secondary
    /// index's value column, or a pushed-down filter equality. Under the
    /// Dictionary layout the term is translated to its code
    /// (sorted-dictionary codes preserve lexicographic order); `None` means
    /// the term is absent from the dictionary and matches nothing. The string
    /// layouts probe with the rendered term itself.
    ///
    /// Memoized per role, so one match performs a single dictionary search
    /// and a single render per bound term however many stages and indexes ask
    /// for the same probe.
    pub(crate) fn probe_scalar(&mut self, term: TermRef<'_>) -> Result<Option<Scalar>> {
        if self.probes_by_string() {
            return Ok(Some(Scalar::from(self.render(term))));
        }
        Ok(self.code(term)?.map(Scalar::from))
    }

    /// Whether the layout can prove the pattern matches no row: a bound term
    /// that resolved to no dictionary code.
    ///
    /// The allocation-free form of testing [`Constraints::AlwaysFalse`] — the
    /// async prelude resolved every bound role into the role cache, so this
    /// reads it instead of compiling the per-column equalities only to
    /// discard them. The string layouts probe with the term itself and have
    /// no code to miss, so they answer without touching the cache.
    ///
    /// A role the prelude left unresolved answers `false`, deferring to the
    /// constraint path rather than fabricating an empty result.
    pub(crate) fn provably_empty(&self, pattern: QuadPattern<'_>) -> bool {
        !self.probes_by_string()
            && pattern
                .bound_roles()
                .any(|term| matches!(self.roles[term.role().index()], Some(None)))
    }

    /// Compile a quad pattern into per-column equality constraints — the
    /// layout-specific term → (column, scalar) mapping, the single source of
    /// truth consumed by both the in-memory mask scan and the pushed-down
    /// file filter. Bound terms compile in `s`, `p`, `o`, `g` order: the
    /// rendered term per string column, the object's typed sub-columns under
    /// `TypedObject`, and under the Dictionary layout (either residency) the
    /// term's code — a term absent from the dictionary cannot match any quad.
    pub(crate) fn constraints(&mut self, pattern: QuadPattern<'_>) -> Result<Constraints> {
        let typed_object = matches!(self.layout, ResolvedLayout::TypedObject);
        let mut eqs: Vec<(&'static str, Scalar)> = Vec::new();
        for term in pattern.bound_roles() {
            if typed_object && let TermRef::Object(object) = term {
                let (kind, value, dt, lang) = typed_object::decompose_object(object);
                eqs.push((COL_O_KIND, Scalar::from(kind)));
                eqs.push((COL_O_VALUE, Scalar::from(value.as_str())));
                if let Some(dt_str) = dt {
                    eqs.push((COL_O_DATATYPE, Scalar::from(dt_str.as_str())));
                }
                if let Some(lang_str) = lang {
                    eqs.push((COL_O_LANG, Scalar::from(lang_str.as_str())));
                }
            } else if self.probes_by_string() {
                eqs.push((term.role().name(), Scalar::from(self.render(term))));
            } else {
                match self.code(term)? {
                    Some(code) => eqs.push((term.role().name(), Scalar::from(code))),
                    None => return Ok(Constraints::AlwaysFalse),
                }
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

    /// Decode a StructArray chunk into quads. Dictionary chunks are decoded
    /// through the layout's own dictionary.
    ///
    /// Executes the chunk to a `StructArray` and reads its columns. A
    /// chunk-level failure (execution, a required column missing or of the
    /// wrong type) is returned as a single `Err` element; a row whose terms
    /// fail to parse is an `Err` at that row's position, the other rows
    /// decoding normally. The shared-string variant below and every
    /// [`ChunkDecode`] follow the same contract.
    pub(crate) fn decode_chunk(&self, chunk: &ArrayRef) -> Vec<Result<Quad>> {
        match self {
            ResolvedLayout::Default => default::decode_chunk(chunk),
            ResolvedLayout::TypedObject => typed_object::decode_chunk(chunk),
            ResolvedLayout::Dictionary(access) => match access.resident() {
                Some(dict) => dictionary::decode_chunk(chunk, dict),
                // Defensive: the read paths decode a file-backed store's
                // chunks through `ChunkDecode::decode_async`, which resolves
                // each chunk's codes with a read of the dictionary; reaching
                // here means one of them didn't.
                None => vec![Err(VortexRdfError::Deserialization(
                    "a file-backed dictionary decodes chunks through the async read path"
                        .to_string(),
                ))],
            },
        }
    }

    /// [`decode_chunk`](Self::decode_chunk) into [`SharedQuad`]s — terms as
    /// shared N-Triples strings, decoded once per distinct code under the
    /// Dictionary layout; the string layouts read their columns as
    /// [`raw_quads`](Self::raw_quads) does, one string per row.
    pub(crate) fn decode_chunk_shared(&self, chunk: &ArrayRef) -> Vec<Result<SharedQuad>> {
        match self {
            ResolvedLayout::Default | ResolvedLayout::TypedObject => match self.raw_quads(chunk) {
                Ok(raws) => raws
                    .into_iter()
                    .map(|raw| Ok(SharedQuad::from(raw)))
                    .collect(),
                Err(e) => vec![Err(e)],
            },
            ResolvedLayout::Dictionary(access) => match access.resident() {
                Some(dict) => dictionary::decode_chunk_shared(chunk, dict),
                None => vec![Err(VortexRdfError::Deserialization(
                    "a file-backed dictionary decodes chunks through the async read path"
                        .to_string(),
                ))],
            },
        }
    }

    /// Prepare `pattern` for the synchronous match core: pre-resolve every
    /// bound term the layout probes by code, and hand back the
    /// [`PatternCodes`] witness the core's probes run on. The one point in a
    /// match where a dictionary may perform I/O (see
    /// [`DictAccess::resolve_pattern`]) — which is why this prelude is the
    /// witness's only constructor. The string layouts have nothing to
    /// resolve, so their preparation never suspends.
    pub(crate) async fn prepare_pattern(
        &self,
        pattern: QuadPattern<'_>,
    ) -> Result<PatternCodes<'_>> {
        let mut codes = PatternCodes::new(self);
        if let ResolvedLayout::Dictionary(access) = self {
            access.resolve_pattern(pattern, &mut codes).await?;
        }
        Ok(codes)
    }

    /// Decode an array of this layout's rows back into [`RawQuad`]s — each
    /// term in its N-Triples string form, without an oxrdf parse round-trip.
    ///
    /// The inverse of the build-time column encoding, for the operations that
    /// rebuild a store from its quads (compaction, and reads that must merge a
    /// string tail into a Dictionary-encoded base): Default reads its four
    /// string columns verbatim, TypedObject recomposes the object term from
    /// its typed sub-columns, and Dictionary resolves each code through
    /// this layout's term dictionary.
    pub(crate) fn raw_quads(&self, rows: &ArrayRef) -> Result<Vec<RawQuad>> {
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let struct_arr = rows.clone().execute::<StructArray>(&mut ctx)?;
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
                    dictionary::decode_code_column(dict, &read_code_column(&struct_arr, COL_S)?)?,
                    dictionary::decode_code_column(dict, &read_code_column(&struct_arr, COL_P)?)?,
                    dictionary::decode_code_column(dict, &read_code_column(&struct_arr, COL_O)?)?,
                    dictionary::decode_code_column(dict, &read_code_column(&struct_arr, COL_G)?)?,
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
    /// handled: the whole dictionary is lifted resident transiently, then the
    /// sync decode runs unchanged. Its one caller, `live_raw_quads`, serves
    /// whole-base reads only (compaction and serialization), which touch most
    /// of the dictionary anyway; every chunk a read streams decodes through
    /// [`ChunkDecode`] instead.
    pub(crate) async fn raw_quads_async(&self, rows: &ArrayRef) -> Result<Vec<RawQuad>> {
        #[cfg(feature = "file-io")]
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

/// The row representations a chunk decodes into: owned oxrdf [`Quad`]s,
/// [`SharedQuad`]s whose terms are shared strings, or [`RawQuad`]s holding
/// each term's N-Triples string. Each pipeline over chunks — the serve plans,
/// the reads by id, the scan streams, the raw export stream — is written once
/// against this trait for all three.
pub(crate) trait ChunkDecode: Sized + Send + 'static {
    /// `chunk`'s rows through `layout`, under
    /// [`ResolvedLayout::decode_chunk`]'s error convention; a file-backed
    /// dictionary is a chunk-level error.
    fn decode(layout: &ResolvedLayout, chunk: &ArrayRef) -> Vec<Result<Self>>;

    /// `chunk`'s rows against the file-backed dictionary `fb`: the chunk's
    /// code columns are read once and their distinct codes resolve to terms
    /// with one read of the dictionary windows holding them.
    #[cfg(feature = "file-io")]
    fn decode_file_backed<'a>(
        fb: &'a FileBackedDict,
        chunk: &'a ArrayRef,
    ) -> BoxFuture<'a, Vec<Result<Self>>>;

    /// [`decode`](Self::decode), through
    /// [`decode_file_backed`](Self::decode_file_backed) when the dictionary
    /// is file-backed — how a file view's chunks decode; every other layout
    /// (and a resident dictionary) decodes without suspending.
    #[cfg(feature = "file-io")]
    fn decode_async<'a>(
        layout: &'a ResolvedLayout,
        chunk: &'a ArrayRef,
    ) -> BoxFuture<'a, Vec<Result<Self>>> {
        if let ResolvedLayout::Dictionary(DictAccess::FileBacked(fb)) = layout {
            return Self::decode_file_backed(fb, chunk);
        }
        future::ready(Self::decode(layout, chunk)).boxed()
    }
}

impl ChunkDecode for Quad {
    fn decode(layout: &ResolvedLayout, chunk: &ArrayRef) -> Vec<Result<Self>> {
        layout.decode_chunk(chunk)
    }

    #[cfg(feature = "file-io")]
    fn decode_file_backed<'a>(
        fb: &'a FileBackedDict,
        chunk: &'a ArrayRef,
    ) -> BoxFuture<'a, Vec<Result<Self>>> {
        dictionary::decode_chunk_file_backed(fb, chunk).boxed()
    }
}

impl ChunkDecode for SharedQuad {
    fn decode(layout: &ResolvedLayout, chunk: &ArrayRef) -> Vec<Result<Self>> {
        layout.decode_chunk_shared(chunk)
    }

    #[cfg(feature = "file-io")]
    fn decode_file_backed<'a>(
        fb: &'a FileBackedDict,
        chunk: &'a ArrayRef,
    ) -> BoxFuture<'a, Vec<Result<Self>>> {
        dictionary::decode_chunk_shared_file_backed(fb, chunk).boxed()
    }
}

impl ChunkDecode for RawQuad {
    fn decode(layout: &ResolvedLayout, chunk: &ArrayRef) -> Vec<Result<Self>> {
        match layout.raw_quads(chunk) {
            Ok(raws) => raws.into_iter().map(Ok).collect(),
            Err(e) => vec![Err(e)],
        }
    }

    #[cfg(feature = "file-io")]
    fn decode_file_backed<'a>(
        fb: &'a FileBackedDict,
        chunk: &'a ArrayRef,
    ) -> BoxFuture<'a, Vec<Result<Self>>> {
        dictionary::decode_chunk_raw_file_backed(fb, chunk).boxed()
    }
}

/// Read a UTF-8 string column into owned term strings, one per row.
fn read_string_column(struct_arr: &StructArray, name: &str) -> Result<Vec<String>> {
    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let col = field_as::<VarBinViewArray>(struct_arr, name, &mut ctx)?;
    let reader = StrColReader::new(&col);
    (0..col.len())
        .map(|i| reader.str_at(i).map(str::to_string))
        .collect()
}

/// Read a code column into owned codes, one per row.
fn read_code_column(struct_arr: &StructArray, name: &str) -> Result<Vec<TermCode>> {
    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let col = field_as::<PrimitiveArray>(struct_arr, name, &mut ctx)?;
    Ok(col.as_slice::<TermCode>().to_vec())
}

#[cfg(all(test, feature = "file-io"))]
mod tests {
    use super::*;

    /// A pre-resolved witness — one prepared under a file-backed dictionary
    /// — answers a probe for a role the prelude never seeded with an error,
    /// and from the role cache once it is seeded.
    #[tokio::test]
    async fn preresolved_witness_declines_unseeded_role() {
        let (_dir, native) = crate::tests::mapped_file(1, Vec::new()).await;
        let fb = FileBackedDict::open(&native)
            .await
            .unwrap()
            .expect("the written dictionary child must be window-searchable");
        let layout = ResolvedLayout::Dictionary(DictAccess::FileBacked(fb));
        let subject = NamedOrBlankNode::from(NamedNode::new("http://example.org/s").unwrap());
        let pattern = QuadPattern::new(Some(&subject), None, None, None);
        let mut codes = PatternCodes::new(&layout);
        assert!(matches!(
            codes.constraints(pattern),
            Err(VortexRdfError::Deserialization(_))
        ));

        assert_eq!(
            codes.resolve(TermRef::Subject(&subject), |_| Some(7)),
            Some(7)
        );
        match codes.constraints(pattern).unwrap() {
            Constraints::Eq(eqs) => assert_eq!(eqs, vec![(COL_S, Scalar::from(7 as TermCode))]),
            Constraints::AlwaysFalse => panic!("a seeded role compiles to an equality"),
        }
    }
}
