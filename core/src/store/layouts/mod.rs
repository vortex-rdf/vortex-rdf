//! How quads become columns, and how a pattern becomes column constraints:
//! the [`LayoutStrategy`] a build requests, the `ResolvedLayout` a store
//! reads through (the strategy plus the state it cannot carry alone, a
//! Dictionary layout's term access), the `QuadPattern`/`PatternCodes` pattern
//! form, and the dispatch that turns them into per-layout column building
//! and constraint lowering. Chunk decoding, one dispatch over where a layout
//! keeps its terms for every representation, is `decode` (the string
//! layouts' reads in `strings`).
//!
//! A leaf (`default`, `typed_object`, `dictionary`) owns exactly one layout's
//! physical schema, its column names and encode/decode loops, and never names
//! another's. The Dictionary leaf is a folder module carrying the whole
//! term-dictionary subsystem (storage, ingest, residency) beside its
//! encode/decode paths.
//!
//! Secondary indexes are *not* part of a layout: they are built as their own
//! children beside a layout's quad rows, in the index's encoding for the
//! layout (term strings, or the Dictionary layout's term codes), and the
//! index modules own those columns and their names (see
//! [`IndexType`](crate::store::indexes::IndexType)).

use std::sync::Arc;

use oxrdf::{GraphName, NamedNode, NamedOrBlankNode, Term};
use vortex_array::ArrayRef;
use vortex_array::dtype::DType;
use vortex_array::scalar::Scalar;

use crate::error::{Result, VortexRdfError};
use crate::store::RawQuad;

mod decode;
pub(crate) mod default;
pub(crate) mod dictionary;
mod strings;
pub(crate) mod typed_object;

pub(crate) use self::decode::ChunkDecode;
pub(crate) use self::dictionary::DictAccess;
use crate::store::schema::{
    CODE_PTYPE, COL_O_DATATYPE, COL_O_KIND, COL_O_LANG, COL_O_VALUE, COL_S, PRIMARY_COLUMNS,
    QuadColumn, TermCode,
};

/// The columnar schema a store's quads are stored in.
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
    /// Each requested [`IndexType`] adds its own children beside these
    /// columns; see that enum's variant docs for their column tables (they
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
    /// Index children are unaffected by the object split: every requested
    /// [`IndexType`] builds the same term-string children it would under
    /// `Default`, sorting whole object terms in N-Triples form.
    ///
    /// [`IndexType`]: crate::store::indexes::IndexType
    TypedObject,

    /// All four quad fields as term codes into one shared term dictionary.
    ///
    /// ### `LayoutStrategy::Dictionary` column schema
    ///
    /// All four quad fields stored as [`TermCode`]s (`u64`) into a single
    /// global term dictionary. In memory the dictionary lives beside the
    /// columns (see `dictionary::term_dict`); a serialized file carries it as
    /// the native container's `dictionary` child (see `crate::io::container`),
    /// so the quad columns stay bare.
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
/// `"dictionary"`), the spelling the `clap` derive exposes on the CLI, so
/// every frontend reports one vocabulary and a value one prints another
/// parses.
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
/// [`Display`](std::fmt::Display) emits (`"default"`, `"typed-object"`,
/// `"dictionary"`), the one vocabulary every frontend shares. Any other name
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
    /// The layout a struct dtype's schema names, read off its columns without
    /// materializing anything: an `s` column of codes is Dictionary, an
    /// `o_kind` column TypedObject, anything else Default.
    pub(crate) fn from_dtype(dtype: &DType) -> LayoutStrategy {
        if let DType::Struct(fields, _) = dtype {
            // The dictionary itself rides outside the schema, as the native
            // container's dictionary child. Another integer width is no layout
            // of this crate's: the open refuses it (`check_id_columns`) before
            // anything reads it.
            if matches!(fields.field(COL_S), Some(DType::Primitive(ptype, _)) if ptype == CODE_PTYPE)
            {
                return LayoutStrategy::Dictionary;
            }
            if fields.names().iter().any(|n| n.as_ref() == COL_O_KIND) {
                return LayoutStrategy::TypedObject;
            }
        }
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

    /// This layout's primary column arrays for `quads`; an empty slice yields
    /// empty columns of the right dtypes.
    ///
    /// A `Serialization` error for `Dictionary`, whose encoding needs the
    /// global [`TermDictionary`]: its chunks are built from codes by
    /// [`dictionary::build_code_chunk`] instead.
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
/// constructed array, carrying any state intrinsic to the layout (for the
/// Dictionary layout, access to the global term dictionary). Holding the
/// state in the variant makes "Dictionary layout without a dictionary"
/// unrepresentable; [`DictAccess`] carries *how* the dictionary is reached
/// (resident, or left in its file).
#[derive(Clone)]
pub(crate) enum ResolvedLayout {
    Default,
    TypedObject,
    Dictionary(DictAccess),
}

/// A quad pattern: the four term positions, each bound or free, as one value
/// through every stage of a match. The fast paths clear the components they
/// resolve, and whatever is still bound afterwards is exactly what the
/// residual filter must compare.
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

    /// Whether any component is still bound, i.e. whether a residual filter
    /// has anything left to do.
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

    /// The bound roles behind the subject in `(p, o, g)` order, up to the
    /// first unbound one: the prefix a bound subject's run narrows by in a
    /// base sorted by `(s, p, o, g)`, each role searched for inside the run
    /// the previous one left.
    pub(crate) fn prefix_behind_subject(&self) -> impl Iterator<Item = TermRef<'a>> + use<'a> {
        [
            self.predicate.map(TermRef::Predicate),
            self.object.map(TermRef::Object),
            self.graph.map(TermRef::Graph),
        ]
        .into_iter()
        .map_while(|term| term)
    }

    /// Unbind the first `answered` roles of
    /// [`prefix_behind_subject`](Self::prefix_behind_subject): the ones a
    /// prefix probe searched, which the narrowed run now satisfies.
    pub(crate) fn clear_prefix_behind_subject(&mut self, answered: usize) {
        if answered > 0 {
            self.predicate = None;
        }
        if answered > 1 {
            self.object = None;
        }
        if answered > 2 {
            self.graph = None;
        }
    }
}

/// A [`QuadPattern`] that owns its terms, so a view can keep it: the pattern
/// whose matches a pending selection holds (see
/// [`LazyRowIds::matched_pattern`](crate::store::indexes::LazyRowIds::matched_pattern)).
#[derive(Clone, Debug)]
pub(crate) struct OwnedQuadPattern {
    subject: Option<NamedOrBlankNode>,
    predicate: Option<NamedNode>,
    object: Option<Term>,
    graph: Option<GraphName>,
}

impl OwnedQuadPattern {
    /// `pattern`'s terms, owned.
    pub(crate) fn of(pattern: QuadPattern<'_>) -> Self {
        Self {
            subject: pattern.subject.cloned(),
            predicate: pattern.predicate.cloned(),
            object: pattern.object.cloned(),
            graph: pattern.graph.cloned(),
        }
    }

    /// One pattern binding what this one and `other` bind: each role bound
    /// by either, to its term. `None` when the two bind a role to different
    /// terms: no quad holds both, but whether the layout spells them alike
    /// is the match's to decide, so the caller matches them one after the
    /// other.
    pub(crate) fn and<'a>(&'a self, other: QuadPattern<'a>) -> Option<QuadPattern<'a>> {
        fn both<'a, T: PartialEq>(
            mine: &'a Option<T>,
            theirs: Option<&'a T>,
        ) -> Option<Option<&'a T>> {
            match (mine.as_ref(), theirs) {
                (Some(a), Some(b)) if a != b => None,
                (a, b) => Some(a.or(b)),
            }
        }
        Some(QuadPattern::new(
            both(&self.subject, other.subject)?,
            both(&self.predicate, other.predicate)?,
            both(&self.object, other.object)?,
            both(&self.graph, other.graph)?,
        ))
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

/// The term's N-Triples form, as the columns store it: the default graph
/// renders as the empty string the `g` column holds, not as oxrdf's own
/// `Display` writes it (`DEFAULT`).
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
    pub(crate) fn role(&self) -> QuadColumn {
        match self {
            TermRef::Subject(_) => QuadColumn::S,
            TermRef::Predicate(_) => QuadColumn::P,
            TermRef::Object(_) => QuadColumn::O,
            TermRef::Graph(_) => QuadColumn::G,
        }
    }

    /// Render into `out`, which is cleared first: the allocation-free form of
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
/// A *witness* type: its only constructor is the async prelude
/// ([`prepare_pattern`](ResolvedLayout::prepare_pattern)), the one place a
/// dictionary may perform I/O during a match, and every synchronous probe of
/// the match core is a method on it ([`probe_scalar`](Self::probe_scalar),
/// [`constraints`](Self::constraints)). Holding one proves the prelude ran
/// ("prelude skipped" is unrepresentable), which lets a file-backed
/// dictionary confine its I/O to the prelude.
///
/// The role cache memoizes: `match_base`'s stages (the unmatchable-pattern
/// gate, the sorted-subject probe, the index probes, the residual
/// constraints) derive the same term → code mapping, and caching per role
/// keeps a fully bound pattern to one dictionary search and one render per
/// bound term. The render goes into `scratch`: a term's N-Triples form is
/// needed only long enough to search the dictionary with it, so one buffer
/// serves all four roles and every stage.
///
/// Scoped to a single `match_base`: the tail is matched under a *different*
/// layout (it stores terms as strings so a term the base's dictionary has
/// never seen can still match), so a code cached here would be meaningless
/// there, and the tail's match prepares its own witness.
pub(crate) struct PatternCodes {
    /// Per role: `None` = not resolved yet, `Some(None)` = resolved and absent
    /// from the dictionary (so the pattern cannot match).
    roles: [Option<Option<TermCode>>; 4],
    /// Reused render target for the bound terms; see the type docs.
    scratch: String,
    /// The layout the prelude prepared the pattern under, which fixes how a
    /// probe is answered: the string layouts probe with the rendered term
    /// itself (`TypedObject`'s constraints decomposing the object into its
    /// typed sub-columns), the Dictionary layout with the code the prelude
    /// resolved for the term's role under either residency, so the role
    /// cache is the complete answer and no probe reaches the dictionary.
    layout: LayoutStrategy,
}

impl PatternCodes {
    /// A fresh witness prepared under `layout`, reachable only from the async
    /// prelude ([`prepare_pattern`](ResolvedLayout::prepare_pattern)), which
    /// is what makes holding a `PatternCodes` proof that the prelude ran.
    fn new(layout: LayoutStrategy) -> Self {
        Self {
            roles: [None; 4],
            scratch: String::new(),
            layout,
        }
    }

    /// The code for `term`'s role, resolved through `f` the first time only:
    /// `term` is rendered into the shared scratch buffer on a miss and not
    /// rendered at all on a hit. How the prelude seeds the cache.
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
        #[cfg(test)]
        resolve_hooks::note_resolved();
        let resolved = f(&self.scratch);
        self.roles[role] = Some(resolved);
        resolved
    }

    /// Seed `term`'s role with `code`, resolved without the rendered term (a
    /// file-backed dictionary's window search), unless the role is cached
    /// already.
    #[cfg(feature = "file-io")]
    pub(in crate::store::layouts) fn seed(&mut self, term: TermRef<'_>, code: Option<TermCode>) {
        self.roles[term.role().index()].get_or_insert(code);
    }

    /// `term`'s N-Triples form in the shared scratch buffer, for the layouts
    /// that probe with the string itself (no dictionary code to cache).
    fn render(&mut self, term: TermRef<'_>) -> &str {
        term.write_nt(&mut self.scratch);
        &self.scratch
    }

    /// Whether this witness probes with the rendered term itself rather than
    /// a dictionary code: the `Default` and `TypedObject` layouts.
    fn probes_by_string(&self) -> bool {
        self.layout != LayoutStrategy::Dictionary
    }

    /// The dictionary code for `term`'s role, from the role cache the prelude
    /// seeded with every bound role.
    ///
    /// The error arm is the residual contract a witness cannot encode in its
    /// type: a probe for a role the prepared pattern never bound, which no
    /// synchronous resolver answers. `None` is reserved for "resolved and
    /// absent from the dictionary", so an unresolvable probe must be an
    /// error: answering `None` would fabricate an empty match result.
    fn code(&self, term: TermRef<'_>) -> Result<Option<TermCode>> {
        self.roles[term.role().index()].ok_or_else(|| {
            VortexRdfError::Deserialization(format!(
                "no code resolution for {term}: the async prelude resolves every bound role \
                 of the prepared pattern, and no probe reaches the dictionary outside it"
            ))
        })
    }

    /// The scalar a term column is probed with (the primary `s` column, a
    /// secondary index's value column, or a pushed-down filter equality).
    /// Under the Dictionary layout it is the term's code, codes being sorted
    /// ranks that preserve the terms' lexicographic order; `None` means the
    /// term is absent from the dictionary and matches nothing. The string
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

    /// The code a code column's probes search for `term`: its
    /// [`probe_scalar`](Self::probe_scalar) under the Dictionary layout.
    /// `None` for a term the dictionary lacks, and under the string layouts,
    /// whose columns hold terms rather than codes.
    pub(crate) fn probe_code(&self, term: TermRef<'_>) -> Result<Option<TermCode>> {
        if self.probes_by_string() {
            return Ok(None);
        }
        self.code(term)
    }

    /// Whether the layout can prove the pattern matches no row: a bound term
    /// that resolved to no dictionary code.
    ///
    /// The allocation-free form of testing [`Constraints::AlwaysFalse`]: the
    /// async prelude resolved every bound role into the role cache, so this
    /// reads it instead of compiling the per-column equalities only to
    /// discard them. The string layouts probe with the term itself and have
    /// no code to miss, so they answer `false` without touching the cache.
    ///
    /// The verdict is conservative: a role the prelude left unresolved
    /// answers `false`, deferring to the constraint path rather than
    /// fabricating an empty result.
    pub(crate) fn provably_empty(&self, pattern: QuadPattern<'_>) -> bool {
        !self.probes_by_string()
            && pattern
                .bound_roles()
                .any(|term| matches!(self.roles[term.role().index()], Some(None)))
    }

    /// `pattern` compiled into per-column equality constraints: the
    /// layout-specific term → (column, scalar) mapping, the single source of
    /// truth both the in-memory mask scan and the pushed-down file filter
    /// consume. Bound terms compile in `s`, `p`, `o`, `g` order: the rendered
    /// term per string column, the object's typed sub-columns under
    /// `TypedObject`, and under the Dictionary layout (either residency) the
    /// term's code; a term absent from the dictionary compiles to
    /// [`Constraints::AlwaysFalse`].
    pub(crate) fn constraints(&mut self, pattern: QuadPattern<'_>) -> Result<Constraints> {
        let typed_object = self.layout == LayoutStrategy::TypedObject;
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
    /// The term dictionary, when it is in memory: `None` for the string
    /// layouts and for a dictionary left in its file.
    pub(crate) fn resident_dictionary(&self) -> Option<&Arc<dictionary::TermDictionary>> {
        match self {
            ResolvedLayout::Dictionary(access) => access.resident(),
            _ => None,
        }
    }

    /// The build-time strategy tag this layout was resolved from.
    pub(crate) fn strategy(&self) -> LayoutStrategy {
        match self {
            ResolvedLayout::Default => LayoutStrategy::Default,
            ResolvedLayout::TypedObject => LayoutStrategy::TypedObject,
            ResolvedLayout::Dictionary(_) => LayoutStrategy::Dictionary,
        }
    }

    /// `pattern` prepared for the synchronous match core: every bound term
    /// the layout probes by code pre-resolved into the [`PatternCodes`]
    /// witness the core's probes run on. The one point in a match where a
    /// dictionary may perform I/O (see [`DictAccess::resolve_pattern`]),
    /// which is why this prelude is the witness's only constructor. The
    /// string layouts have nothing to resolve, so their preparation never
    /// suspends.
    pub(crate) async fn prepare_pattern(&self, pattern: QuadPattern<'_>) -> Result<PatternCodes> {
        let mut codes = PatternCodes::new(self.strategy());
        if let ResolvedLayout::Dictionary(access) = self {
            access.resolve_pattern(pattern, &mut codes).await?;
        }
        Ok(codes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A witness prepared under the Dictionary layout answers a probe for a
    /// role the prelude never seeded with an error, and from the role cache
    /// once it is seeded.
    #[test]
    fn preresolved_witness_declines_unseeded_role() {
        let subject = NamedOrBlankNode::from(NamedNode::new("http://example.org/s").unwrap());
        let pattern = QuadPattern::new(Some(&subject), None, None, None);
        let mut codes = PatternCodes::new(LayoutStrategy::Dictionary);
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

/// Test hook: how many bound terms this thread's match preludes looked up in
/// a resident dictionary.
#[cfg(test)]
pub(crate) mod resolve_hooks {
    use std::cell::Cell;

    thread_local! {
        static RESOLVED: Cell<usize> = const { Cell::new(0) };
    }

    /// Record one term looked up.
    pub(super) fn note_resolved() {
        RESOLVED.set(RESOLVED.get() + 1);
    }

    /// The terms looked up on this thread so far.
    pub(crate) fn resolved() -> usize {
        RESOLVED.get()
    }
}
