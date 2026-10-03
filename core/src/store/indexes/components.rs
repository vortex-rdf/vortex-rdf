//! The persisted-component model: a child's wire identity and the slug
//! registry, [`IndexComponent`] (a child's rows held in memory with the
//! writer's sortedness provenance), and the child-schema helpers the leaves
//! and the builders share.

use std::ops::Range;
use std::sync::{Arc, OnceLock};

use vortex_array::arrays::struct_::StructArrayExt;
use vortex_array::arrays::{PrimitiveArray, StructArray};
use vortex_array::dtype::{DType, FieldName};
use vortex_array::scalar::Scalar;
use vortex_array::validity::Validity;
use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute};
use vortex_buffer::Buffer;

use crate::error::{Result, VortexRdfError};
use crate::session::VORTEX_SESSION;
use crate::store::array::{into_struct_array, make_string_array, search_sorted_bounds};
use crate::store::probes::StructProbes;

use super::{ALL_INDEX_TYPES, IndexType, Indexes};

/// A persisted index child's wire identity.
pub(crate) struct ComponentIdentity {
    /// The component name (`index:posg`, `index:ref-o`, …).
    pub(crate) name: &'static str,
    /// The implementation slug (`secondary-by-copy/posg`, …).
    pub(crate) slug: &'static str,
}

/// A registry row: the identity a persisted slug resolves to and the index it
/// belongs to.
pub(crate) struct KnownComponent {
    pub(crate) identity: &'static ComponentIdentity,
    pub(crate) index: IndexType,
}

/// What a persisted implementation slug means to this version; `None` for a
/// foreign slug.
pub(crate) fn known_component(implementation: &str) -> Option<KnownComponent> {
    ALL_INDEX_TYPES.into_iter().find_map(|index| {
        index
            .component_identities()
            .iter()
            .find(|identity| identity.slug == implementation)
            .map(|identity| KnownComponent { identity, index })
    })
}

/// The registry row of the child named `name` (`index:posg`, …); `None` for
/// a foreign name.
pub(crate) fn component_named(name: &str) -> Option<KnownComponent> {
    ALL_INDEX_TYPES.into_iter().find_map(|index| {
        index
            .component_identities()
            .iter()
            .find(|identity| identity.name == name)
            .map(|identity| KnownComponent { identity, index })
    })
}

/// Every known index child holds exactly one row per quad.
pub(crate) fn check_component_rows(name: &str, component_rows: u64, quad_rows: u64) -> Result<()> {
    if component_rows != quad_rows {
        return Err(VortexRdfError::Deserialization(format!(
            "index component {} holds {} rows against {} quad rows",
            name, component_rows, quad_rows
        )));
    }
    Ok(())
}

/// Adopt a persisted child as a component whose rows materialize on first
/// use. The row count is checked against the quad rows here. A `Reader`
/// source MUST sit over a buffer-backed segment source: its scan runs
/// synchronously.
pub(crate) fn adopt_component(
    known: &KnownComponent,
    source: DeferredSource,
    sorted: bool,
    quad_rows: u64,
) -> Result<IndexComponent> {
    check_component_rows(known.identity.name, source.row_count(), quad_rows)?;
    Ok(IndexComponent {
        identity: known.identity,
        rows: Arc::new(ComponentRows {
            cell: OnceLock::new(),
            pending: Some(source),
        }),
        sorted,
        probes: StructProbes::new(),
    })
}

/// What a deferred component still runs over its persisted child.
pub(crate) enum DeferredSource {
    /// The scan already ran; execution to one struct is deferred.
    #[cfg(feature = "file-io")]
    Scanned(ArrayRef),
    /// Nothing ran: scan and execution both defer. Only for a reader over a
    /// buffer-backed segment source.
    Reader(vortex_layout::LayoutReaderRef),
}

impl DeferredSource {
    fn row_count(&self) -> u64 {
        match self {
            #[cfg(feature = "file-io")]
            DeferredSource::Scanned(scanned) => scanned.len() as u64,
            DeferredSource::Reader(reader) => reader.row_count(),
        }
    }

    /// The child's un-executed scan output.
    fn scanned(&self) -> Result<ArrayRef> {
        match self {
            #[cfg(feature = "file-io")]
            DeferredSource::Scanned(scanned) => Ok(scanned.clone()),
            DeferredSource::Reader(reader) => crate::io::read::scan_all_reader_sync(reader.clone()),
        }
    }
}

/// One index child held in memory: its rows under the child's own column
/// names (`s`, `p`, `o`, `g`, `rid` for a copy family; `val`, `rid` for a
/// reference family) and the writer's sortedness provenance. `rid` values
/// address rows of the base the component was built against: valid across
/// `RowSelection` narrowing, invalid after any physical gather.
#[derive(Clone)]
pub(crate) struct IndexComponent {
    pub(crate) identity: &'static ComponentIdentity,
    rows: Arc<ComponentRows>,
    /// Whether the sort-key columns are GLOBALLY sorted: the writer's
    /// provenance, never an inspection. Binary-search routing is gated on it;
    /// per-chunk-sorted data must never claim it.
    pub(crate) sorted: bool,
    /// Encoded-search probes over the rows, shared by every clone.
    probes: Arc<StructProbes>,
}

/// A component's rows: `cell` once materialized, `pending` the adoption still
/// to run (`None` for rows built in memory).
struct ComponentRows {
    cell: OnceLock<StructArray>,
    pending: Option<DeferredSource>,
}

impl IndexComponent {
    /// A component over rows already in child schema.
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

    /// The row count when the rows are materialized.
    pub(crate) fn len_if_resident(&self) -> Option<usize> {
        self.rows.cell.get().map(StructArray::len)
    }

    /// The rows as one struct in child schema, materializing a deferred
    /// adoption on first call. Two callers racing on first touch both run the
    /// pipeline over the immutable source; the first store wins.
    pub(crate) fn rows(&self) -> Result<&StructArray> {
        if let Some(rows) = self.rows.cell.get() {
            return Ok(rows);
        }
        let Some(source) = &self.rows.pending else {
            unreachable!("a component without a pending source holds built rows")
        };
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let executed = source
            .scanned()?
            .execute::<StructArray>(&mut ctx)
            .map_err(VortexRdfError::Vortex)?;
        Ok(self.rows.cell.get_or_init(|| executed))
    }

    /// This component as a native child write carrying its sortedness
    /// provenance; a deferred adoption materializes here.
    #[cfg(any(feature = "file-io", target_arch = "wasm32"))]
    pub(crate) fn to_write(&self) -> Result<crate::io::container::NativeComponentWrite> {
        use crate::io::container::{
            BufferedComponentSource, NativeComponentWrite, StoreComponentDescriptor,
            StoreComponentRole, default_child_strategy,
        };
        let array = self.rows()?.clone().into_array();
        NativeComponentWrite::new(
            StoreComponentDescriptor {
                name: self.identity.name.into(),
                role: StoreComponentRole::Index,
                implementation: self.identity.slug.into(),
                version: 1,
                required: false,
                sorted: self.sorted,
                dtype: array.dtype().clone(),
            },
            Arc::new(
                BufferedComponentSource::try_new(vec![array]).map_err(VortexRdfError::Vortex)?,
            ),
            default_child_strategy(),
        )
        .map_err(VortexRdfError::Vortex)
    }

    /// Whether the rows are materialized.
    #[cfg(all(test, feature = "file-io"))]
    pub(crate) fn is_materialized(&self) -> bool {
        self.rows.cell.get().is_some()
    }

    /// This component with its rows passed through `transform`, under a fresh
    /// probe cache; identity and sortedness carry across.
    fn rebuilt(self, transform: impl FnOnce(ArrayRef) -> Result<ArrayRef>) -> Result<Self> {
        let rows = self.rows()?.clone().into_array();
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

    /// The rows materialized, integer children kept compressed where a probe
    /// binds them and decoded otherwise
    /// ([`with_searchable_int_children`](crate::store::array::with_searchable_int_children)).
    pub(crate) fn into_searchable(self) -> Result<Self> {
        self.rebuilt(crate::store::array::with_searchable_int_children)
    }

    /// The rows with their integer children compressed into probe-supported
    /// encodings
    /// ([`with_compressed_int_children`](crate::store::array::with_compressed_int_children)),
    /// without a payload wrapper.
    pub(crate) fn into_compressed(self) -> Result<Self> {
        self.rebuilt(|rows| crate::store::array::with_compressed_int_children(rows, false))
    }

    /// The cached probe over `column`, `None` when its encoding declines.
    /// Materializes a deferred component.
    fn probe(&self, column: &str) -> Option<Arc<vortex_rdf_encoded_search::OwnedSortedProbe>> {
        self.probes
            .by_name(self.rows().ok()?.as_ref(), column)
            .cloned()
    }

    /// The shared probe cache, for a serve plan outliving this reference.
    pub(crate) fn probes_arc(&self) -> Arc<StructProbes> {
        Arc::clone(&self.probes)
    }

    /// Resolve the probes of materialized rows now; a deferred component is
    /// left alone.
    pub(crate) fn warm_probes(&self) {
        if let Some(rows) = self.rows.cell.get() {
            self.probes.warm(rows.as_ref());
        }
    }

    /// The `[lo, hi)` run of rows whose `column` equals `native`, by binary
    /// search through the column's cached probe when it resolves one, else
    /// over the column itself. `within` must be a range whose slice of the
    /// column is sorted (the whole component, or a lead run for a prefix
    /// probe). `None` when the column is missing or the probe cannot cast to
    /// its dtype; an empty range when the term is absent.
    pub(crate) fn probe_run(
        &self,
        column: &'static str,
        native: &Scalar,
        within: Option<Range<usize>>,
    ) -> Result<Option<Range<usize>>> {
        if let Some(owned) = self.probe(column)
            && let Ok(needle) = u64::try_from(native)
        {
            let (lo, hi) = match within {
                None => owned.bounds(needle),
                Some(range) => owned.bounds_in(range, needle),
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
        let run = col.slice(within.clone()).map_err(VortexRdfError::Vortex)?;
        let (lo, hi) = search_sorted_bounds(&run, &scalar)?;
        Ok(Some(within.start + lo..within.start + hi))
    }

    /// The component named `name`, when present and globally sorted.
    pub(crate) fn find_sorted<'a>(
        components: &'a [IndexComponent],
        name: &str,
    ) -> Option<&'a IndexComponent> {
        components
            .iter()
            .find(|c| c.identity.name == name && c.sorted)
    }
}

/// The index set a component roster implies, in preference order.
pub(crate) fn indexes_from_components(components: &[IndexComponent]) -> Indexes {
    ALL_INDEX_TYPES
        .into_iter()
        .filter(|index| {
            components.iter().any(|c| {
                known_component(c.identity.slug).is_some_and(|known| known.index == *index)
            })
        })
        .collect()
}

/// A copy or reference column's term encoding: `String` terms, or `u32`
/// codes under the Dictionary layout.
pub(crate) trait TermColumn: Clone + Ord {
    /// A column of these terms.
    fn column<'a>(it: impl Iterator<Item = &'a Self>) -> ArrayRef
    where
        Self: 'a;
}

impl TermColumn for String {
    fn column<'a>(it: impl Iterator<Item = &'a Self>) -> ArrayRef {
        make_string_array(it.map(String::as_str))
    }
}

impl TermColumn for u32 {
    fn column<'a>(it: impl Iterator<Item = &'a Self>) -> ArrayRef {
        PrimitiveArray::from_iter(it.copied()).into_array()
    }
}

/// A child's rows from its column arrays: `columns[i]` under
/// `child_columns[i]`, non-nullable throughout.
pub(crate) fn child_struct(
    child_columns: &[&'static str],
    columns: Vec<ArrayRef>,
    len: usize,
) -> Result<StructArray> {
    StructArray::try_new(
        child_columns
            .iter()
            .map(|n| FieldName::from(*n))
            .collect::<Vec<_>>()
            .into(),
        columns,
        len,
        Validity::NonNullable,
    )
    .map_err(VortexRdfError::Vortex)
}

/// An index's children from one family's columns each, under
/// `child_columns`; both are globally sorted by construction.
pub(crate) fn components_from(
    child_columns: &[&'static str],
    families: [(&'static ComponentIdentity, Vec<ArrayRef>); 2],
) -> Result<Vec<IndexComponent>> {
    families
        .into_iter()
        .map(|(identity, columns)| {
            let len = columns.first().map_or(0, |column| column.len());
            let rows = child_struct(child_columns, columns, len)?;
            Ok(IndexComponent::built(identity, rows, true))
        })
        .collect()
}

/// The child struct dtype under `child_columns`: every column but the last
/// holds terms (`Utf8`, or u32 codes when `encoded`), the last the u32
/// primary row id.
#[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
pub(crate) fn child_struct_dtype(child_columns: &[&'static str], encoded: bool) -> DType {
    use vortex_array::dtype::{Nullability, PType, StructFields};
    let rid = DType::Primitive(PType::U32, Nullability::NonNullable);
    let term = if encoded {
        rid.clone()
    } else {
        DType::Utf8(Nullability::NonNullable)
    };
    let mut field_dtypes = vec![term; child_columns.len() - 1];
    field_dtypes.push(rid);
    DType::Struct(
        StructFields::new(
            child_columns
                .iter()
                .map(|n| (*n).into())
                .collect::<Vec<std::sync::Arc<str>>>()
                .into(),
            field_dtypes,
        ),
        Nullability::NonNullable,
    )
}

/// A row-id column as the ascending, unique `Buffer<u64>` every resolution
/// answers in. Ascending is required by `Selection::IncludeByIndex` and the
/// selection algebra; the ids are unique by construction, so sorting alone
/// suffices.
pub(crate) fn sorted_row_ids(row_id_column: ArrayRef) -> Result<Buffer<u64>> {
    use vortex_array::builtins::ArrayBuiltins;
    use vortex_array::dtype::{Nullability, PType};

    if row_id_column.is_empty() {
        return Ok(Buffer::empty());
    }
    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let ids = row_id_column
        .cast(DType::Primitive(PType::U64, Nullability::NonNullable))
        .map_err(VortexRdfError::Vortex)?
        .execute::<PrimitiveArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?
        .into_buffer::<u64>();

    // A uniquely owned buffer sorts in place; a shared one is copied once.
    match ids.try_into_mut() {
        Ok(mut ids) => {
            ids.as_mut_slice().sort_unstable();
            Ok(ids.freeze())
        }
        Err(ids) => {
            let mut sorted = ids.as_slice().to_vec();
            sorted.sort_unstable();
            Ok(Buffer::from(sorted))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::indexes::{copy, reference};

    #[test]
    fn sorted_row_ids_casts_and_sorts() {
        let column = PrimitiveArray::from_iter([5u32, 1, 3]).into_array();
        let ids = sorted_row_ids(column).unwrap();
        assert_eq!(ids.as_slice(), &[1u64, 3, 5]);

        let empty = PrimitiveArray::from_iter(std::iter::empty::<u32>()).into_array();
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

    /// A one-row built component under `identity`.
    fn tiny(identity: &'static ComponentIdentity) -> IndexComponent {
        let column = PrimitiveArray::from_iter([0u32]).into_array();
        let rows = child_struct(&["val", "rid"], vec![column.clone(), column], 1).unwrap();
        IndexComponent::built(identity, rows, true)
    }

    /// The index set comes back in preference order whatever the roster
    /// order, one entry per index; an unknown slug implies no index.
    #[test]
    fn indexes_from_components_orders_by_preference() {
        let roster = [
            tiny(&reference::IDENTITIES[0]),
            tiny(&copy::IDENTITIES[0]),
            tiny(&reference::IDENTITIES[1]),
        ];
        assert_eq!(
            indexes_from_components(&roster),
            vec![IndexType::SecondaryByCopy, IndexType::SecondaryByReference]
        );

        static FOREIGN: ComponentIdentity = ComponentIdentity {
            name: "index:spog",
            slug: "secondary-by-copy/spog",
        };
        assert!(indexes_from_components(&[tiny(&FOREIGN)]).is_empty());
    }

    #[test]
    #[cfg(not(all(target_arch = "wasm32", target_os = "unknown")))]
    fn child_struct_dtype_terms_then_rid() {
        use vortex_array::dtype::{Nullability, PType};
        let dtype = child_struct_dtype(&["val", "rid"], false);
        let fields = dtype.as_struct_fields();
        assert_eq!(
            fields.field_by_index(0).unwrap(),
            DType::Utf8(Nullability::NonNullable)
        );
        assert_eq!(
            fields.field_by_index(1).unwrap(),
            DType::Primitive(PType::U32, Nullability::NonNullable)
        );
        let encoded = child_struct_dtype(&["s", "p", "o", "g", "rid"], true);
        assert!(
            encoded
                .as_struct_fields()
                .fields()
                .all(|f| f == DType::Primitive(PType::U32, Nullability::NonNullable))
        );
    }
}
