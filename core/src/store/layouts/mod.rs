//! Layouts: the [`LayoutStrategy`] a build requests, the [`ResolvedLayout`] a
//! store reads through (the strategy plus a Dictionary layout's term access),
//! the [`QuadPattern`]/[`PatternCodes`] pattern form, and the dispatch into
//! the leaves (`default`, `typed_object`, `dictionary`) for column building,
//! chunk decoding and constraint lowering. Secondary indexes are not part of
//! a layout: their children and column names belong to
//! [`IndexType`](crate::store::indexes::IndexType).

use std::sync::Arc;

use futures::FutureExt as _;
use futures::future::{self, BoxFuture};
use oxrdf::{GraphName, NamedNode, NamedOrBlankNode, Quad, Term};
use vortex_array::arrays::struct_::StructArray;
use vortex_array::arrays::{PrimitiveArray, VarBinViewArray};
use vortex_array::dtype::{DType, PType};
use vortex_array::scalar::Scalar;
use vortex_array::{ArrayRef, VortexSessionExecute};

use crate::common::quad::SharedQuad;
use crate::error::{Result, VortexRdfError};
use crate::session::VORTEX_SESSION;
use crate::store::RawQuad;
use crate::store::array::{StrColReader, field_as};
use crate::store::schema::QuadColumn;

pub(crate) mod default;
pub(crate) mod dictionary;
pub(crate) mod typed_object;

pub(crate) use self::dictionary::DictAccess;
use self::dictionary::TermDictionary;
use self::typed_object::{COL_O_DATATYPE, COL_O_KIND, COL_O_LANG, COL_O_VALUE};
use crate::store::schema::{COL_G, COL_O, COL_P, COL_S, PRIMARY_COLUMNS};

/// The columnar schema RDF quads are stored in.
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
#[cfg_attr(feature = "clap", derive(clap::ValueEnum))]
pub enum LayoutStrategy {
    /// All four quad fields as opaque N-Triples strings.
    ///
    /// ### `LayoutStrategy::Default` column schema
    ///
    /// All four quad fields stored as UTF-8 strings in N-Triples
    /// serialization form.
    ///
    /// | Column | Type              | Content                                                    |
    /// |--------|-------------------|------------------------------------------------------------|
    /// | `s`    | `VarBin<Utf8>`    | Subject: `<IRI>` or `_:blank`                              |
    /// | `p`    | `VarBin<Utf8>`    | Predicate: `<IRI>`                                         |
    /// | `o`    | `VarBin<Utf8>`    | Object: `<IRI>`, `_:blank`, `"lit"`, `"lit"@lang`, `"lit"^^<dt>` |
    /// | `g`    | `VarBin<Utf8>`    | Graph: `<IRI>`, `_:blank`, or `""` for DefaultGraph        |
    ///
    /// Each requested [`IndexType`] adds its own children beside these; see
    /// that enum's variant docs for the per-index column tables (term
    /// strings here, as under `TypedObject`).
    ///
    /// [`IndexType`]: crate::store::indexes::IndexType
    Default,

    /// Object column split into typed sub-columns (kind, value, datatype, lang).
    ///
    /// ### `LayoutStrategy::TypedObject` column schema
    ///
    /// Same as `Default` for `s`, `p`, `g`. The `o` column is decomposed into
    /// typed fields.
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
    /// [`IndexType`] holds the same term-string columns it would under
    /// `Default`, sorting whole object terms in N-Triples form.
    ///
    /// [`IndexType`]: crate::store::indexes::IndexType
    TypedObject,

    /// All four quad fields as u32 codes into one shared term dictionary.
    ///
    /// ### `LayoutStrategy::Dictionary` column schema
    ///
    /// All four quad fields stored as u32 codes into a single global term
    /// dictionary. In memory the dictionary lives beside the columns; a
    /// serialized file carries it as the native container's `dictionary`
    /// child (see `crate::io::container`).
    ///
    /// | Column        | Type                  | Content                                             |
    /// |---------------|-----------------------|-----------------------------------------------------|
    /// | `s`,`p`,`o`,`g` | `PrimitiveArray<u32>` | code = position of the term in the sorted dictionary |
    ///
    /// Term codes are lexicographic ranks, so code comparisons are
    /// order-isomorphic to string comparisons.
    ///
    /// Every requested [`IndexType`] builds its usual children, except that
    /// their term-valued columns hold u32 codes instead of strings; the
    /// row-id columns are `u32` under every layout.
    ///
    /// [`IndexType`]: crate::store::indexes::IndexType
    Dictionary,
}

/// The canonical strategy name: kebab-case (`"default"`, `"typed-object"`,
/// `"dictionary"`), the spelling the `clap` derive exposes.
impl std::fmt::Display for LayoutStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            LayoutStrategy::Default => "default",
            LayoutStrategy::TypedObject => "typed-object",
            LayoutStrategy::Dictionary => "dictionary",
        })
    }
}

/// Accepts exactly the kebab-case names [`Display`](std::fmt::Display)
/// emits: `"default"`, `"typed-object"`, `"dictionary"`.
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
    /// The layout a struct dtype is in: a u32 `s` column is Dictionary, an
    /// `o_kind` column TypedObject, anything else Default.
    pub(crate) fn from_dtype(dtype: &DType) -> LayoutStrategy {
        if let DType::Struct(fields, _) = dtype {
            if matches!(fields.field(COL_S), Some(DType::Primitive(ptype, _)) if ptype == PType::U32)
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

    /// [`primary_column_names`](Self::primary_column_names) as owned field
    /// names.
    pub(crate) fn field_names(self) -> Vec<Arc<str>> {
        self.primary_column_names()
            .iter()
            .map(|&n| n.into())
            .collect()
    }

    /// The primary column arrays of `quads`; an empty slice yields empty
    /// columns with the right dtypes. Not available for `Dictionary`, whose
    /// chunks need the term dictionary ([`dictionary::build_chunk`]).
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

/// The layout a store reads through: the [`LayoutStrategy`] plus, for the
/// Dictionary layout, how its term dictionary is reached ([`DictAccess`]).
#[derive(Clone)]
pub(crate) enum ResolvedLayout {
    Default,
    TypedObject,
    Dictionary(DictAccess),
}

/// A quad pattern: the four term positions, each bound or free. The match
/// stages clear the roles they resolve; what stays bound is the residual.
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

    /// Whether any role is bound.
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

/// A bound term of a quad pattern, tagged with its role.
#[derive(Clone, Copy, Debug)]
pub(crate) enum TermRef<'a> {
    Subject(&'a NamedOrBlankNode),
    Predicate(&'a NamedNode),
    Object(&'a Term),
    Graph(&'a GraphName),
}

/// The term's N-Triples form as the columns store it: the default graph is
/// the empty string, not oxrdf's `DEFAULT`.
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
    /// The column this term's role occupies.
    pub(crate) fn role(&self) -> QuadColumn {
        match self {
            TermRef::Subject(_) => QuadColumn::S,
            TermRef::Predicate(_) => QuadColumn::P,
            TermRef::Object(_) => QuadColumn::O,
            TermRef::Graph(_) => QuadColumn::G,
        }
    }

    /// Render into `out`, cleared first; writes what `Display` writes.
    fn write_nt(&self, out: &mut String) {
        use std::fmt::Write as _;
        out.clear();
        write!(out, "{self}").expect("writing to a String cannot fail");
    }
}

/// How a [`PatternCodes`] answers a probe its role cache does not hold.
enum CodeResolver {
    /// Probes are the rendered N-Triples strings.
    Default,
    /// String probes; constraints decompose the object into its typed
    /// sub-columns.
    TypedObject,
    /// Codes resolve by binary search of the resident dictionary.
    Resident(Arc<TermDictionary>),
    /// The prelude resolved every bound role; there is no synchronous
    /// resolver behind the cache.
    #[cfg(feature = "file-io")]
    Preresolved,
}

/// Per-role term -> code cache and render buffer for one base match; built
/// only by [`prepare_pattern`](ResolvedLayout::prepare_pattern), the one
/// point where a dictionary may do I/O during a match. Scoped to one
/// `match_base`: the tail is matched under its own layout and prepares its
/// own `PatternCodes`.
pub(crate) struct PatternCodes {
    /// Per role: `None` = not resolved yet, `Some(None)` = resolved and absent
    /// from the dictionary.
    roles: [Option<Option<u32>>; 4],
    /// Render target for bound terms, reused across roles.
    scratch: String,
    resolver: CodeResolver,
}

impl PatternCodes {
    fn new(resolver: CodeResolver) -> Self {
        Self {
            roles: [None; 4],
            scratch: String::new(),
            resolver,
        }
    }

    /// Codes for a resident dictionary: unseeded roles resolve by in-memory
    /// binary search.
    pub(in crate::store::layouts) fn resident(dict: Arc<TermDictionary>) -> Self {
        Self::new(CodeResolver::Resident(dict))
    }

    /// Codes for a file-backed dictionary: the prelude seeds every bound
    /// role, and nothing resolves beyond them.
    #[cfg(feature = "file-io")]
    pub(in crate::store::layouts) fn preresolved() -> Self {
        Self::new(CodeResolver::Preresolved)
    }

    /// The code for `term`'s role, resolving it through `f` on the first
    /// call only; `term` is rendered into the scratch buffer on a miss.
    pub(in crate::store::layouts) fn resolve(
        &mut self,
        term: TermRef<'_>,
        f: impl FnOnce(&str) -> Option<u32>,
    ) -> Option<u32> {
        let role = term.role().index();
        if let Some(cached) = self.roles[role] {
            return cached;
        }
        term.write_nt(&mut self.scratch);
        let resolved = f(&self.scratch);
        self.roles[role] = Some(resolved);
        resolved
    }

    /// `term`'s N-Triples form in the scratch buffer.
    fn render(&mut self, term: TermRef<'_>) -> &str {
        term.write_nt(&mut self.scratch);
        &self.scratch
    }

    /// Whether this layout probes with the rendered string rather than a
    /// code (the `Default` and `TypedObject` layouts).
    fn probes_by_string(&self) -> bool {
        matches!(
            self.resolver,
            CodeResolver::Default | CodeResolver::TypedObject
        )
    }

    /// The dictionary code for `term`'s role: the role cache, else a resident
    /// dictionary's binary search. `None` = absent from the dictionary; an
    /// unseeded role on a witness without a synchronous resolver is an error
    /// (answering `None` would fabricate an empty match).
    fn code(&mut self, term: TermRef<'_>) -> Result<Option<u32>> {
        if let Some(cached) = self.roles[term.role().index()] {
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
        Ok(self.resolve(term, |s| dict.encode(s)))
    }

    /// The scalar that probes a term column for `term`: its u32 code under
    /// the Dictionary layout (`None` = absent, matches nothing), the rendered
    /// term under the string layouts. Memoized per role.
    pub(crate) fn probe_scalar(&mut self, term: TermRef<'_>) -> Result<Option<Scalar>> {
        if self.probes_by_string() {
            return Ok(Some(Scalar::from(self.render(term))));
        }
        Ok(self.code(term)?.map(Scalar::from))
    }

    /// Whether a bound role resolved to no dictionary code, read off the role
    /// cache without compiling constraints. A role the prelude left
    /// unresolved answers `false`.
    pub(crate) fn provably_empty(&self, pattern: QuadPattern<'_>) -> bool {
        !self.probes_by_string()
            && pattern
                .bound_roles()
                .any(|term| matches!(self.roles[term.role().index()], Some(None)))
    }

    /// The pattern as per-column equalities under this layout, in `s`, `p`,
    /// `o`, `g` order: the rendered term per string column, the typed
    /// sub-columns of a `TypedObject` object, the code per Dictionary column
    /// (`AlwaysFalse` when a term has none).
    pub(crate) fn constraints(&mut self, pattern: QuadPattern<'_>) -> Result<Constraints> {
        let typed_object = matches!(self.resolver, CodeResolver::TypedObject);
        let mut eqs: Vec<(&'static str, Scalar)> = Vec::new();
        for term in pattern.bound_roles() {
            if typed_object && let TermRef::Object(object) = term {
                let (kind, value, datatype, lang) = typed_object::decompose_object(object);
                eqs.push((COL_O_KIND, Scalar::from(kind)));
                eqs.push((COL_O_VALUE, Scalar::from(value.as_str())));
                if let Some(datatype) = datatype {
                    eqs.push((COL_O_DATATYPE, Scalar::from(datatype.as_str())));
                }
                if let Some(lang) = lang {
                    eqs.push((COL_O_LANG, Scalar::from(lang.as_str())));
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

/// The column equalities a pattern compiles to under a layout; the in-memory
/// mask scan and the pushed-down file filter both consume them.
pub(crate) enum Constraints {
    /// A bound term cannot match any quad (absent from the dictionary).
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

    /// `chunk`'s rows as quads. A chunk-level failure (execution, a required
    /// column missing or mistyped) is a single `Err` element; a row whose
    /// terms fail to parse is an `Err` at that row's position. A file-backed
    /// dictionary is a chunk-level error here; it decodes through
    /// [`ChunkDecode::decode_async`].
    pub(crate) fn decode_chunk(&self, chunk: &ArrayRef) -> Vec<Result<Quad>> {
        match self {
            ResolvedLayout::Default => chunk_result(default::decode_chunk(chunk)),
            ResolvedLayout::TypedObject => chunk_result(typed_object::decode_chunk(chunk)),
            ResolvedLayout::Dictionary(access) => match access.resident() {
                Some(dict) => dictionary::decode_chunk(chunk, dict),
                None => file_backed_sync_error(),
            },
        }
    }

    /// [`decode_chunk`](Self::decode_chunk) into [`SharedQuad`]s: terms as
    /// shared N-Triples strings, decoded once per distinct code under the
    /// Dictionary layout.
    pub(crate) fn decode_chunk_shared(&self, chunk: &ArrayRef) -> Vec<Result<SharedQuad>> {
        match self {
            ResolvedLayout::Default | ResolvedLayout::TypedObject => {
                chunk_result(self.raw_quads(chunk).map(|raws| {
                    raws.into_iter()
                        .map(|raw| Ok(SharedQuad::from(raw)))
                        .collect()
                }))
            }
            ResolvedLayout::Dictionary(access) => match access.resident() {
                Some(dict) => dictionary::decode_chunk_shared(chunk, dict),
                None => file_backed_sync_error(),
            },
        }
    }

    /// `sync(self, chunk)`, except under a file-backed dictionary, where the
    /// chunk's distinct codes are resolved with one read of the dictionary
    /// child and `mapped` decodes against them.
    #[cfg(feature = "file-io")]
    async fn decode_resolving<T>(
        &self,
        chunk: &ArrayRef,
        mapped: MappedDecode<T>,
        sync: fn(&ResolvedLayout, &ArrayRef) -> Vec<Result<T>>,
    ) -> Vec<Result<T>> {
        if let ResolvedLayout::Dictionary(DictAccess::FileBacked(fb)) = self {
            return match dictionary::resolve_chunk_terms(fb, chunk).await {
                Ok(terms) => mapped(chunk, &terms),
                Err(e) => vec![Err(e)],
            };
        }
        sync(self, chunk)
    }

    /// The [`PatternCodes`] for `pattern`: every bound term the layout probes
    /// by code is resolved here, the one point in a match where a dictionary
    /// may do I/O. The string layouts resolve nothing.
    pub(crate) async fn prepare_pattern(&self, pattern: QuadPattern<'_>) -> Result<PatternCodes> {
        match self {
            ResolvedLayout::Default => Ok(PatternCodes::new(CodeResolver::Default)),
            ResolvedLayout::TypedObject => Ok(PatternCodes::new(CodeResolver::TypedObject)),
            ResolvedLayout::Dictionary(access) => access.resolve_pattern(pattern).await,
        }
    }

    /// `rows` of this layout as [`RawQuad`]s, each term in its N-Triples
    /// string form: Default reads its string columns, TypedObject recomposes
    /// the object from its sub-columns, Dictionary resolves each code through
    /// its resident dictionary (a file-backed one is an error here; see
    /// [`raw_quads_async`](Self::raw_quads_async)).
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

    /// [`raw_quads`](Self::raw_quads), lifting a file-backed dictionary
    /// resident for the decode.
    pub(crate) async fn raw_quads_async(&self, rows: &ArrayRef) -> Result<Vec<RawQuad>> {
        #[cfg(feature = "file-io")]
        if let ResolvedLayout::Dictionary(access) = self
            && access.is_file_backed()
        {
            let dict = access.ensure_resident().await?;
            return ResolvedLayout::Dictionary(DictAccess::Resident(dict)).raw_quads(rows);
        }
        self.raw_quads(rows)
    }
}

/// A decode of a Dictionary chunk against a resolved code -> term map.
#[cfg(feature = "file-io")]
type MappedDecode<T> = fn(&ArrayRef, &std::collections::HashMap<u32, Arc<str>>) -> Vec<Result<T>>;

/// The error chunk for a synchronous decode of a file-backed dictionary.
fn file_backed_sync_error<T>() -> Vec<Result<T>> {
    vec![Err(VortexRdfError::Deserialization(
        "a file-backed dictionary decodes chunks through the async read path".to_string(),
    ))]
}

/// A leaf's chunk result as the per-row convention: a chunk-level `Err` is
/// one `Err` element.
fn chunk_result<T>(result: Result<Vec<Result<T>>>) -> Vec<Result<T>> {
    match result {
        Ok(rows) => rows,
        Err(e) => vec![Err(e)],
    }
}

/// A row representation a chunk decodes into: owned [`Quad`]s, [`SharedQuad`]s
/// with shared-string terms, or [`RawQuad`]s. Every chunk pipeline (serve
/// plans, point reads, scan streams) is written once against this trait.
pub(crate) trait ChunkDecode: Sized + Send + 'static {
    /// `chunk`'s rows through `layout`, under [`ResolvedLayout::decode_chunk`]'s
    /// error convention; a file-backed dictionary is a chunk-level error.
    fn decode(layout: &ResolvedLayout, chunk: &ArrayRef) -> Vec<Result<Self>>;

    /// [`decode`](Self::decode), resolving a file-backed dictionary's codes
    /// with a read of its child; otherwise the synchronous decode.
    fn decode_async<'a>(
        layout: &'a ResolvedLayout,
        chunk: &'a ArrayRef,
    ) -> BoxFuture<'a, Vec<Result<Self>>> {
        future::ready(Self::decode(layout, chunk)).boxed()
    }
}

impl ChunkDecode for Quad {
    fn decode(layout: &ResolvedLayout, chunk: &ArrayRef) -> Vec<Result<Self>> {
        layout.decode_chunk(chunk)
    }

    #[cfg(feature = "file-io")]
    fn decode_async<'a>(
        layout: &'a ResolvedLayout,
        chunk: &'a ArrayRef,
    ) -> BoxFuture<'a, Vec<Result<Self>>> {
        layout
            .decode_resolving(chunk, dictionary::decode_chunk_mapped, Self::decode)
            .boxed()
    }
}

impl ChunkDecode for SharedQuad {
    fn decode(layout: &ResolvedLayout, chunk: &ArrayRef) -> Vec<Result<Self>> {
        layout.decode_chunk_shared(chunk)
    }

    #[cfg(feature = "file-io")]
    fn decode_async<'a>(
        layout: &'a ResolvedLayout,
        chunk: &'a ArrayRef,
    ) -> BoxFuture<'a, Vec<Result<Self>>> {
        layout
            .decode_resolving(chunk, dictionary::decode_chunk_mapped_shared, Self::decode)
            .boxed()
    }
}

impl ChunkDecode for RawQuad {
    fn decode(layout: &ResolvedLayout, chunk: &ArrayRef) -> Vec<Result<Self>> {
        chunk_result(
            layout
                .raw_quads(chunk)
                .map(|raws| raws.into_iter().map(Ok).collect()),
        )
    }

    fn decode_async<'a>(
        layout: &'a ResolvedLayout,
        chunk: &'a ArrayRef,
    ) -> BoxFuture<'a, Vec<Result<Self>>> {
        layout
            .raw_quads_async(chunk)
            .map(|raws| chunk_result(raws.map(|raws| raws.into_iter().map(Ok).collect())))
            .boxed()
    }
}

/// A UTF-8 string column as owned term strings, one per row.
fn read_string_column(struct_arr: &StructArray, name: &str) -> Result<Vec<String>> {
    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let col = field_as::<VarBinViewArray>(struct_arr, name, &mut ctx)?;
    let reader = StrColReader::new(&col);
    (0..col.len())
        .map(|i| reader.str_at(i).map(str::to_string))
        .collect()
}

/// A u32 code column as owned codes, one per row.
fn read_u32_column(struct_arr: &StructArray, name: &str) -> Result<Vec<u32>> {
    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let col = field_as::<PrimitiveArray>(struct_arr, name, &mut ctx)?;
    Ok(col.as_slice::<u32>().to_vec())
}

#[cfg(all(test, feature = "file-io"))]
mod tests {
    use super::*;

    /// A pre-resolved witness answers a probe for a role the prelude never
    /// seeded with an error, and from the role cache once it is seeded.
    #[test]
    fn preresolved_witness_declines_unseeded_role() {
        let subject = NamedOrBlankNode::from(NamedNode::new("http://example.org/s").unwrap());
        let pattern = QuadPattern::new(Some(&subject), None, None, None);
        let mut codes = PatternCodes::preresolved();
        assert!(matches!(
            codes.constraints(pattern),
            Err(VortexRdfError::Deserialization(_))
        ));

        assert_eq!(
            codes.resolve(TermRef::Subject(&subject), |_| Some(7)),
            Some(7)
        );
        match codes.constraints(pattern).unwrap() {
            Constraints::Eq(eqs) => assert_eq!(eqs, vec![(COL_S, Scalar::from(7u32))]),
            Constraints::AlwaysFalse => panic!("a seeded role compiles to an equality"),
        }
    }
}
