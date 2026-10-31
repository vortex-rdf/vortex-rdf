//! The persistence model of a secondary index: the *component*, the one form
//! index data takes. One [`IndexComponent`] is one child of a native store
//! file, or its in-memory twin: rows under the child's own column names,
//! carrying the writer's sortedness provenance. Builders emit components
//! beside primary-only quad rows; this module streams a child's rows to the
//! writer, which spells its descriptor ([`ChildStream`]), adopts a
//! persisted one back into memory ([`adopt_component`]), maps an
//! implementation slug onto its identity ([`known_component`]), and decodes a
//! rid column into the sorted base row ids every resolution answers in
//! ([`sorted_row_ids`]).
//!
//! The column names belong to the index modules (the row id every child
//! carries is the one exception, named once in the hub as
//! [`COL_RID`](super::COL_RID)): each leaf declares its identities in a
//! static [`ComponentIdentity`] table ([`IndexType::component_identities`]),
//! and the loops here are parameterized by its rows.

use std::ops::Range;
use std::sync::{Arc, OnceLock};

use vortex_array::arrays::struct_::StructArrayExt;
use vortex_array::arrays::{PrimitiveArray, StructArray};
use vortex_array::dtype::{DType, Nullability};
use vortex_array::scalar::Scalar;
use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute};
use vortex_buffer::{Buffer, BufferMut};

use crate::error::{Result, VortexRdfError};
use crate::io::container::sources::NativeComponentSource;
use crate::io::container::{
    INDEX_VERSION, NativeComponentWrite, StoreComponentDescriptor, StoreComponentRole,
    child_strategy,
};
use crate::observe::observe;
use crate::session::VORTEX_SESSION;
use crate::store::array::{into_struct_array, make_string_array, named_struct};
use crate::store::probes::StructProbes;
use crate::store::schema::{ROW_ID_PTYPE, RowId, TermCode};

use super::{ALL_INDEX_TYPES, IndexType, Indexes};

/// The identity a persisted index child carries on the wire (its component
/// name and implementation slug) and the index type that child makes
/// queryable: a row of a leaf's static table
/// ([`IndexType::component_identities`]), which an [`IndexComponent`] holds
/// and the slug registry scans. The columns behind an identity are the
/// leaf's ([`named_struct`](crate::store::array::named_struct) assembles
/// them).
pub(crate) struct ComponentIdentity {
    /// The component name (`index:posg`, `index:ref-o`, …) the persisted
    /// child carries.
    pub(crate) name: &'static str,
    /// The implementation slug (`secondary-by-copy/posg`, …).
    pub(crate) slug: &'static str,
    /// The index type owning the component.
    pub(crate) index: IndexType,
}

/// The slug registry: the identity row a persisted component's
/// implementation slug names in this version, or `None` for a foreign slug
/// (skippable when optional). A lookup over the per-leaf identity tables, so
/// a new index or slug is added in one place, its
/// [`IndexType::component_identities`] row.
pub(crate) fn known_component(implementation: &str) -> Option<&'static ComponentIdentity> {
    ALL_INDEX_TYPES.into_iter().find_map(|index| {
        index
            .component_identities()
            .iter()
            .find(|identity| identity.slug == implementation)
    })
}

/// Refuses an index child whose row count is not the quad count: a known
/// child holds exactly one row per quad, so a mismatch means a corrupt or
/// foreign file, and routing through it would return wrong matches silently.
pub(crate) fn check_component_rows(name: &str, component_rows: u64, quad_rows: u64) -> Result<()> {
    if component_rows != quad_rows {
        return Err(VortexRdfError::Deserialization(format!(
            "index component {} holds {} rows against {} quad rows",
            name, component_rows, quad_rows
        )));
    }
    Ok(())
}

/// A persisted child adopted as an in-memory [`IndexComponent`] under the
/// registry row `identity`, `sorted` the descriptor's provenance, reading
/// nothing of it now: its scan and canonicalization wait for the component's
/// first use (how `from_bytes` adopts its children, off the wire TOC alone).
/// `reader` MUST sit over a buffer-backed segment source (a `from_bytes`
/// buffer): [`IndexComponent::rows`] drives its scan synchronously, and only
/// buffer-backed segment reads resolve without pending. The open has already
/// checked the roster, row counts included (`ComponentRoster::of`), so a
/// corrupt roster fails at open, not at first probe.
pub(crate) fn adopt_component(
    identity: &'static ComponentIdentity,
    reader: vortex_layout::LayoutReaderRef,
    sorted: bool,
) -> IndexComponent {
    IndexComponent {
        identity,
        rows: Arc::new(ComponentRows {
            cell: OnceLock::new(),
            pending: Some(reader),
        }),
        sorted,
        probes: StructProbes::new(),
    }
}

/// One secondary-index component held in memory beside the store's primary
/// base: the in-memory twin of a native store file's index child, carrying
/// the same rows under the same child schema (plain column names: `s`, `p`,
/// `o`, `g`, `rid` for a copy family; `val`, `rid` for a reference family).
///
/// `rid` values address rows of the base the component was built against;
/// that is what keeps components valid across derived views (a
/// `RowSelection` narrows without renumbering) and what invalidates them on
/// any physical gather.
#[derive(Clone)]
pub(crate) struct IndexComponent {
    /// The identity the persisted child carries: component name
    /// (`index:posg`, `index:ref-o`, …), implementation slug and owning index.
    pub(crate) identity: &'static ComponentIdentity,
    /// The component's rows ([`rows`](Self::rows)): canonical from
    /// construction, or a deferred adoption canonicalized on first use.
    /// `Arc`-shared, so every clone of the component, and of the
    /// `Arc<[IndexComponent]>` roster the store's views share, sees one
    /// materialization.
    rows: Arc<ComponentRows>,
    /// Whether the sort-key columns are GLOBALLY sorted: the writer's
    /// provenance, not an inspection. Binary-search resolution is gated on
    /// it, and per-chunk-sorted data must never claim it (a false stamp
    /// corrupts query results; see the `sorted` field on the wire
    /// descriptor).
    pub(crate) sorted: bool,
    /// The encoded-search probes over the component's columns, resolved
    /// lazily and shared by every clone, so the resolvers' searches and the
    /// serve path's point reads skip the per-call encoding-tree walk. The
    /// rows are immutable once materialized, so the cache's array-identity
    /// guard holds for the component's lifetime; [`rebuilt`](Self::rebuilt)
    /// takes a fresh cache.
    probes: Arc<StructProbes>,
}

/// An [`IndexComponent`]'s rows: `cell` holds them as one struct in child
/// schema once canonical (from construction, for a builder's emission), and
/// `pending` is the reader a deferred adoption scans to fill it (`None` for
/// rows built in memory).
struct ComponentRows {
    cell: OnceLock<StructArray>,
    pending: Option<vortex_layout::LayoutReaderRef>,
}

impl IndexComponent {
    /// A component whose rows are already canonical in memory: the
    /// construction every builder's emission uses.
    pub(crate) fn built(
        identity: &'static ComponentIdentity,
        array: StructArray,
        sorted: bool,
    ) -> Self {
        Self {
            identity,
            rows: Arc::new(ComponentRows {
                cell: OnceLock::from(array),
                pending: None,
            }),
            sorted,
            probes: StructProbes::new(),
        }
    }

    /// The component's rows as one canonical struct in child schema, a
    /// deferred adoption materialized on the first call. Two consumers racing
    /// on first touch may both run the scan; the source is immutable, so they
    /// canonicalize the same rows, whichever stores first wins and both read
    /// the stored struct. No lock is held across the run.
    pub(crate) fn rows(&self) -> Result<&StructArray> {
        if let Some(array) = self.rows.cell.get() {
            return Ok(array);
        }
        let Some(reader) = &self.rows.pending else {
            unreachable!("a component without a pending adoption holds its built rows")
        };
        let executed = into_struct_array(crate::io::read::scan_all_reader_sync(reader.clone())?)?;
        Ok(self.rows.cell.get_or_init(|| executed))
    }

    /// This component as a [`ChildStream`] over its rows, carrying its own
    /// sortedness provenance: how a store's serialization hands its
    /// components to the writer. A deferred adoption materializes here, so
    /// the written child is byte-identical to an eagerly adopted one's.
    pub(crate) fn to_stream(&self) -> Result<ChildStream> {
        let array = self.rows()?.clone().into_array();
        Ok(ChildStream {
            identity: self.identity,
            sorted: self.sorted,
            source: Arc::new(crate::io::container::BufferedComponentSource::try_new(
                vec![array],
            )?),
        })
    }

    /// This component as the native child write the writer makes of it
    /// ([`to_stream`](Self::to_stream), [`ChildStream::into_write`]), for
    /// the tests that assemble store files by hand.
    #[cfg(test)]
    pub(crate) fn to_write(&self) -> Result<NativeComponentWrite> {
        self.to_stream()?.into_write()
    }

    /// The inverse of [`to_stream`](Self::to_stream): the child's streamed
    /// chunks executed to one struct in child schema, under the child's
    /// identity and with its `sorted` provenance.
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    pub(crate) async fn from_stream(child: ChildStream) -> Result<Self> {
        use futures::TryStreamExt as _;

        let chunks: Vec<ArrayRef> = child.source.open()?.try_collect().await?;
        let rows = crate::store::array::chunked_or_single(chunks, child.source.dtype().clone())?;
        Ok(Self::built(
            child.identity,
            into_struct_array(rows)?,
            child.sorted,
        ))
    }

    /// Whether the rows have been canonicalized yet (a test-hook probe).
    #[cfg(test)]
    pub(crate) fn is_materialized(&self) -> bool {
        self.rows.cell.get().is_some()
    }

    /// Whether the component's probe cache has resolved its columns yet (a
    /// test-hook probe).
    #[cfg(test)]
    pub(crate) fn probes_resolved(&self) -> bool {
        self.probes.is_resolved()
    }

    /// This component with its rows materialized and passed through
    /// `transform`, held as canonical built rows under a fresh probe cache
    /// (the rebuilt rows are a different array, so the old cache's identity
    /// guard would only ever decline against them). Identity and sortedness
    /// provenance carry across unchanged.
    fn rebuilt(self, transform: impl FnOnce(ArrayRef) -> Result<ArrayRef>) -> Result<Self> {
        let rows = self.rows()?.clone().into_array();
        // The transforms only ever hand back a struct (their input was one),
        // so the downcast is a cast, not work.
        let array = into_struct_array(transform(rows)?)?;
        Ok(Self {
            rows: Arc::new(ComponentRows {
                cell: OnceLock::from(array),
                pending: None,
            }),
            probes: StructProbes::new(),
            ..self
        })
    }

    /// This component in searchable form: rows materialized, integer
    /// children kept compressed wherever the encoded-search probes bind them
    /// and decoded to canonical primitives otherwise (see
    /// [`resident::with_searchable_int_children`]), so the sorted probes bind
    /// the value and `rid` columns directly instead of running the generic
    /// search kernel per call. The adoption step of a store loaded wholesale
    /// into memory.
    ///
    /// [`resident::with_searchable_int_children`]: crate::store::resident::with_searchable_int_children
    #[cfg(feature = "file-io")]
    pub(crate) fn into_searchable(self) -> Result<Self> {
        self.rebuilt(crate::store::resident::with_searchable_int_children)
    }

    /// This component with its integer children compressed into
    /// probe-supported encodings, the construction-side counterpart of
    /// [`into_searchable`](Self::into_searchable): a builder's canonical
    /// emission compresses here (see
    /// [`resident::with_compressed_int_children`]), without the base's payload
    /// wrapper, as no component hands out its columns as code primitives. The
    /// sorted probes bind the compressed columns directly.
    ///
    /// [`resident::with_compressed_int_children`]: crate::store::resident::with_compressed_int_children
    pub(crate) fn into_compressed(self) -> Result<Self> {
        self.rebuilt(|rows| {
            crate::store::resident::with_compressed_int_children(rows, false).map(|(rows, _)| rows)
        })
    }

    /// The cached encoded-search probe over the component's `column`, or
    /// `None` when the column's encoding declines (callers fall back to the
    /// per-call search). Resolving materializes a deferred component, exactly
    /// like [`rows`](Self::rows).
    fn probe(&self, column: &str) -> Option<Arc<vortex_rdf_encoded_search::OwnedSortedProbe>> {
        self.probes
            .by_name(self.rows().ok()?.as_ref(), column)
            .flatten()
            .cloned()
    }

    /// The component's shared probe cache, for a serve plan that outlives
    /// this reference.
    pub(crate) fn probes_arc(&self) -> Arc<StructProbes> {
        Arc::clone(&self.probes)
    }

    /// Resolve this component's probes at construction, ahead of the first
    /// query: the component half of the eager resolution every in-memory
    /// construction does (see [`StructProbes::warm`]). A *deferred* component
    /// is left alone, as materializing it here would undo the deferral of a
    /// `from_bytes` adoption.
    pub(crate) fn warm_probes(&self) {
        if self.rows.pending.is_some() {
            return;
        }
        if let Ok(rows) = self.rows() {
            self.probes.warm(rows.as_ref());
        }
    }

    /// The `[lo, hi)` run of rows of the component's sorted `column` equal to
    /// `native`, by binary search over the whole column or `within` a row
    /// range whose slice of the column is itself sorted (a lead run for a
    /// prefix probe): the probe step of the in-memory resolver.
    ///
    /// Through the column's cached probe when it resolves one, with no
    /// per-call slice or encoding-tree walk, searching only the window as
    /// the slice-then-search path does. String-valued components (whose probe
    /// scalars are not integers) and probe-declined encodings search the
    /// column itself.
    ///
    /// `None` when the column is missing or the probe value cannot cast to
    /// its dtype (the resolver declines and the store falls back to a mask
    /// scan); an empty range when the probed term is absent from the data.
    pub(crate) fn probe_run(
        &self,
        column: &'static str,
        native: &Scalar,
        within: Option<Range<usize>>,
    ) -> Result<Option<Range<usize>>> {
        use crate::store::array::search_sorted_bounds;

        if let Some(owned) = self.probe(column)
            && let Some(needle) = crate::store::array::unsigned_value(native)
        {
            observe!(ProbeBound { cached: true });
            let (lo, hi) = match within {
                None => owned.probe().bounds(needle),
                Some(range) => owned.probe().bounds_in(range, needle),
            };
            return Ok(Some(lo..hi));
        }
        let rows = self.rows()?;
        let within = within.unwrap_or(0..rows.len());
        let Ok(col) = rows.unmasked_field_by_name(column) else {
            return Ok(None);
        };
        let Ok(scalar) = native.cast(col.dtype()) else {
            return Ok(None);
        };
        let run = col.slice(within.clone())?;
        observe!(ProbeBound { cached: false });
        let (lo, hi) = search_sorted_bounds(&run, &scalar)?;
        Ok(Some(within.start + lo..within.start + hi))
    }

    /// The component named `name`, if its provenance says globally sorted:
    /// the front gate of the in-memory resolver, where an absent or not
    /// globally sorted component declines (per-chunk-sorted rows are not
    /// binary-searchable).
    pub(crate) fn find_sorted<'a>(
        components: &'a [IndexComponent],
        name: &str,
    ) -> Option<&'a IndexComponent> {
        components
            .iter()
            .find(|c| c.identity.name == name && c.sorted)
    }
}

/// An index child on its way to the writer: the registry row it is, the
/// writer's sortedness provenance, and the source of its rows. A store's
/// components ([`IndexComponent::to_stream`]) and the out-of-core builder's
/// merged children travel as one, and the writer alone spells its descriptor
/// ([`into_write`](Self::into_write)), as it does the dictionary's.
pub(crate) struct ChildStream {
    pub(crate) identity: &'static ComponentIdentity,
    pub(crate) sorted: bool,
    pub(crate) source: Arc<dyn NativeComponentSource>,
}

impl ChildStream {
    /// This child as a native child write: its descriptor names the
    /// component and its slug, carries `sorted` and the source's dtype, and
    /// the child is written through [`child_strategy`]. The one spelling of
    /// an index child's descriptor.
    pub(crate) fn into_write(self) -> Result<NativeComponentWrite> {
        let dtype = self.source.dtype().clone();
        let strategy = child_strategy(&dtype);
        Ok(NativeComponentWrite::new(
            StoreComponentDescriptor {
                name: self.identity.name.into(),
                role: StoreComponentRole::Index,
                implementation: self.identity.slug.into(),
                version: INDEX_VERSION,
                required: false,
                sorted: self.sorted,
                bracketed_iris: false,
                dtype,
            },
            self.source,
            strategy,
        )?)
    }
}

/// The index set a component roster implies, in declaration (preference)
/// order: the in-memory counterpart of reading a file's child roster.
pub(crate) fn indexes_from_components(components: &[IndexComponent]) -> Indexes {
    ALL_INDEX_TYPES
        .into_iter()
        .filter(|index| components.iter().any(|c| c.identity.index == *index))
        .collect()
}

/// An index column's term encoding (`String` terms, or [`TermCode`]s under
/// the Dictionary layout), with the array a column of them assembles into:
/// the one spelling both families' builders emit their term columns through.
pub(crate) trait TermColumn: Clone + Default + Ord {
    fn column<'a>(it: impl Iterator<Item = &'a Self>) -> ArrayRef
    where
        Self: 'a;
}

impl TermColumn for String {
    fn column<'a>(it: impl Iterator<Item = &'a Self>) -> ArrayRef {
        make_string_array(it.map(String::as_str))
    }
}

impl TermColumn for TermCode {
    fn column<'a>(it: impl Iterator<Item = &'a Self>) -> ArrayRef {
        PrimitiveArray::from_iter(it.copied()).into_array()
    }
}

/// An index's two persisted children from one family's columns each, under
/// `child_columns`: the in-memory builders' emission, globally sorted by
/// construction and handed over with that provenance.
pub(crate) fn components_from(
    child_columns: &[&'static str],
    families: [(&'static ComponentIdentity, Vec<ArrayRef>); 2],
) -> Result<Vec<IndexComponent>> {
    families
        .into_iter()
        .map(|(identity, columns)| {
            let len = columns.first().map_or(0, |column| column.len());
            let rows = named_struct(child_columns, columns, len)?;
            Ok(IndexComponent::built(identity, rows, true))
        })
        .collect()
}

/// A row-id column decoded into the ascending, unique `Buffer<RowId>` every
/// index resolution answers in: the base rows its ids name ([`base_row`]).
///
/// The ids come in the index's own order, and `Selection::IncludeByIndex`
/// and the selection algebra need them ascending, so they are sorted; each
/// index row references one quad row, so they are unique. Ids past what a
/// `usize` holds are refused ([`check_addressable`]).
///
/// [`base_row`]: super::base_row
pub(crate) fn sorted_row_ids(row_id_column: ArrayRef) -> Result<Buffer<RowId>> {
    if row_id_column.is_empty() {
        return Ok(Buffer::empty());
    }
    // The freshly-executed buffer is normally uniquely owned, so the sort
    // runs in place with no copy.
    let mut ids = owned(decoded_row_ids(row_id_column)?);
    sort_unique_ids(ids.as_mut_slice());
    check_addressable(&ids)?;
    Ok(ids.freeze())
}

/// A row-id column decoded into the base rows its ids name ([`base_row`]),
/// in the column's own order: a component run's ids in the order its index
/// keeps them. Ids past what a `usize` holds are refused.
///
/// [`base_row`]: super::base_row
pub(crate) fn run_order_row_ids(row_id_column: ArrayRef) -> Result<Buffer<RowId>> {
    if row_id_column.is_empty() {
        return Ok(Buffer::empty());
    }
    let ids = decoded_row_ids(row_id_column)?;
    check_all_addressable(&ids)?;
    Ok(ids)
}

/// `ids`, unique row ids in any order, sorted ascending into a buffer of
/// their own ([`sort_unique_ids`]).
pub(super) fn sorted_copy(ids: &[RowId]) -> Result<Buffer<RowId>> {
    let mut ids = BufferMut::copy_from(ids);
    sort_unique_ids(ids.as_mut_slice());
    check_addressable(&ids)?;
    Ok(ids.freeze())
}

/// `row_id_column` (non-empty) decoded into the base rows its ids name, in
/// the column's order.
fn decoded_row_ids(row_id_column: ArrayRef) -> Result<Buffer<RowId>> {
    use vortex_array::builtins::ArrayBuiltins;

    observe!(RidsDecoded(row_id_column.len()));
    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    // A no-op cast for the u64 column every writer here produces.
    let ids = row_id_column
        .cast(DType::Primitive(ROW_ID_PTYPE, Nullability::NonNullable))?
        .execute::<PrimitiveArray>(&mut ctx)?
        .into_buffer::<RowId>();
    #[cfg(test)]
    let ids = {
        let mut ids = owned(ids);
        for id in ids.as_mut_slice() {
            *id = super::base_row(*id);
        }
        ids.freeze()
    };
    Ok(ids)
}

/// `ids` as a buffer this call owns: the buffer itself when nothing else
/// holds it (a fresh execution's output), else one copy.
fn owned(ids: Buffer<RowId>) -> BufferMut<RowId> {
    match ids.try_into_mut() {
        Ok(ids) => ids,
        Err(ids) => BufferMut::copy_from(ids.as_slice()),
    }
}

/// Refuses ids, in any order, past what a `usize` holds: nothing to check on
/// a 64-bit target.
fn check_all_addressable(ids: &[RowId]) -> Result<()> {
    #[cfg(target_pointer_width = "64")]
    {
        let _ = ids;
        Ok(())
    }
    #[cfg(not(target_pointer_width = "64"))]
    {
        match ids.iter().max() {
            Some(&max) => super::row_index(max).map(|_| ()),
            None => Ok(()),
        }
    }
}

/// How [`sort_unique_ids`] sorted its ids.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SortPath {
    /// Already ascending, left as they are.
    Ascending,
    /// Compared.
    Compared,
    /// Set as bits of a bitmap over their span and read back in order.
    Bitmap,
}

/// Sort unique row ids ascending, in place: left as they are when already
/// ascending (a reference run: rows of one value are in row id order), set as
/// bits of a bitmap over their span and read back in order when the span is
/// under [`BITMAP_BITS_PER_ID`] bits per id (a copy run, whose ids scatter
/// over a window of the base), compared otherwise. Returns which.
pub(super) fn sort_unique_ids(ids: &mut [RowId]) -> SortPath {
    if ids.is_sorted() {
        return SortPath::Ascending;
    }
    let (min, max) = ids
        .iter()
        .fold((RowId::MAX, 0), |(lo, hi), &id| (lo.min(id), hi.max(id)));
    // The span less one, which no id range overflows.
    let reach = max - min;
    if reach / BITMAP_BITS_PER_ID >= ids.len() as u64 {
        ids.sort_unstable();
        return SortPath::Compared;
    }
    // Under a word per id, so the bitmap fits where the ids do.
    let mut words = vec![0u64; (reach / 64 + 1) as usize];
    for &id in ids.iter() {
        let bit = id - min;
        words[(bit / 64) as usize] |= 1 << (bit % 64);
    }
    let mut next = 0;
    for (w, &word) in (0u64..).zip(&words) {
        let mut word = word;
        while word != 0 {
            ids[next] = min + w * 64 + u64::from(word.trailing_zeros());
            next += 1;
            word &= word - 1;
        }
    }
    debug_assert_eq!(next, ids.len(), "row ids are unique");
    SortPath::Bitmap
}

/// Bits of span per id under which [`sort_unique_ids`] sorts by a bitmap: at
/// 64, the bitmap takes under a word per id, written and read in two linear
/// passes, where a comparison sort's `n log n` steps cost more from a few
/// hundred ids up.
const BITMAP_BITS_PER_ID: u64 = 64;

/// Refuses ascending base rows `ids` whose last one a `usize` cannot hold
/// (on a 32-bit target, wasm, a row id past `u32::MAX`, which no base there
/// holds), so every later use of an id as a position is exact, never
/// narrowed onto another row.
pub(super) fn check_addressable(ids: &[RowId]) -> Result<()> {
    match ids.last() {
        Some(&last) => super::row_index(last).map(|_| ()),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vortex_array::arrays::PrimitiveArray;

    /// Every way the row ids may come: ascending (left as they are), dense
    /// (by a bitmap, also past `u32::MAX`), sparse (compared), and one or two
    /// ids, each sorted ascending.
    #[test]
    fn sort_unique_ids_takes_the_path_their_spread_allows() {
        let wide: RowId = 1 << 40;
        let cases: [(Vec<RowId>, SortPath); 7] = [
            ((0..100).collect(), SortPath::Ascending),
            (vec![7], SortPath::Ascending),
            (vec![9, 2], SortPath::Bitmap),
            ((0..1_000).rev().map(|i| i * 3).collect(), SortPath::Bitmap),
            (
                (0..500).rev().map(|i| wide + i * 7).collect(),
                SortPath::Bitmap,
            ),
            ((0..100).rev().map(|i| i * 65).collect(), SortPath::Compared),
            (vec![wide, 1, RowId::MAX - 1, 5], SortPath::Compared),
        ];
        for (ids, path) in cases {
            let mut want = ids.clone();
            want.sort_unstable();
            let mut got = ids.clone();
            assert_eq!(sort_unique_ids(&mut got), path, "{ids:?}");
            assert_eq!(got, want, "{ids:?}");
        }
    }

    #[test]
    fn sorted_row_ids_sorts_whole_ids() {
        // A rid column comes back as ascending ids, past u32::MAX unchanged.
        let wide: RowId = 1 << 32;
        let column = PrimitiveArray::from_iter([wide + 5, 1, wide, 3]).into_array();
        let ids = sorted_row_ids(column).unwrap();
        assert_eq!(ids.as_slice(), &[1, 3, wide, wide + 5]);

        // An empty column short-circuits to an empty buffer.
        let empty = PrimitiveArray::from_iter(std::iter::empty::<RowId>()).into_array();
        assert!(sorted_row_ids(empty).unwrap().is_empty());
    }

    #[test]
    fn check_component_rows_rejects_mismatch() {
        assert!(matches!(
            check_component_rows("index:ref-o", 3, 4),
            Err(VortexRdfError::Deserialization(_))
        ));
        assert!(check_component_rows("index:ref-o", 4, 4).is_ok());
    }

    /// A tiny built component under `identity`, for roster-shape tests: one
    /// u64 code and its u64 row id.
    fn tiny(identity: &'static ComponentIdentity) -> IndexComponent {
        let val = PrimitiveArray::from_iter([0 as TermCode]).into_array();
        let rid = PrimitiveArray::from_iter([0 as RowId]).into_array();
        let rows = named_struct(&["val", "rid"], vec![val, rid], 1).unwrap();
        IndexComponent::built(identity, rows, true)
    }

    #[test]
    fn indexes_from_components_orders_by_preference() {
        use crate::store::indexes::{copy, reference};

        // Roster order is the wire order; the index set comes back in
        // preference order regardless, one entry per index.
        let roster = [
            tiny(&reference::IDENTITIES[0]),
            tiny(&copy::IDENTITIES[0]),
            tiny(&reference::IDENTITIES[1]),
        ];
        assert_eq!(
            indexes_from_components(&roster),
            vec![IndexType::SecondaryByCopy, IndexType::SecondaryByReference]
        );
    }
}
