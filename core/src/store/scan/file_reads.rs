//! The file-backed read tier: whole-scan and point-read helpers over the quad
//! table and the index children, run location through chunk probes,
//! pushed-down filter construction, and [`RowSelection::restrict_scan`].

use std::future::Future;
use std::ops::Range;
use std::sync::Arc;

use oxrdf::NamedOrBlankNode;
use vortex_array::arrays::StructArray;
use vortex_array::arrays::struct_::StructArrayExt;
use vortex_array::expr::forms::conjuncts;
use vortex_array::expr::{Expression, and_collect, eq, get_item, lit, root, select};
use vortex_array::scalar::Scalar;
use vortex_array::stream::ArrayStreamExt as _;
use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute};
use vortex_buffer::Buffer;
use vortex_error::VortexExpect as _;
use vortex_layout::scan::scan_builder::ScanBuilder;
use vortex_mask::{AllOr, Mask};
use vortex_rdf_encoded_search::ColumnChunks;
use vortex_scan::selection::Selection;
use vortex_scan::strict_sorted_buffer::StrictSortedBuffer;

use crate::error::{Result, VortexRdfError};
use crate::session::VORTEX_SESSION;
use crate::store::layouts::{Constraints, PatternCodes, QuadPattern, TermRef};
use crate::store::persist::native_file::NativeStoreFile;
use crate::store::scan::gather::primitive_from_u64_reads;
use crate::store::schema;
use crate::store::view::selection::RowSelection;

/// The bind-memo scope of expressions over the quad table's schema.
pub(crate) const QUAD_SCOPE: &str = "quads";

/// Every row `scan` yields, as one array.
pub(crate) async fn read_all_rows(scan: ScanBuilder<ArrayRef>) -> Result<ArrayRef> {
    scan.into_array_stream()
        .map_err(VortexRdfError::Vortex)?
        .read_all()
        .await
        .map_err(VortexRdfError::Vortex)
}

/// One column of every row `scan` yields.
pub(crate) async fn scan_column(scan: ScanBuilder<ArrayRef>, column: &str) -> Result<ArrayRef> {
    let rows = read_all_rows(scan).await?;
    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let struct_arr = rows
        .execute::<StructArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    struct_arr
        .unmasked_field_by_name(column)
        .cloned()
        .map_err(VortexRdfError::Vortex)
}

/// The rows of a point read when it answers, otherwise the rows of `scan`.
pub(crate) async fn point_rows_or_scan(
    point: impl Future<Output = Result<Option<ArrayRef>>>,
    scan: ScanBuilder<ArrayRef>,
) -> Result<ArrayRef> {
    match point.await? {
        Some(rows) => Ok(rows),
        None => read_all_rows(scan).await,
    }
}

impl RowSelection {
    /// Apply this selection and the tombstones to a file scan. A row range is
    /// never set together with an `IncludeByIndex` selection: `Range` uses the
    /// row range, `Ids` the selection knob; tombstones are subtracted from an
    /// `Ids` list up front and ride as `ExcludeByIndex` under `All`/`Range`.
    pub(crate) fn restrict_scan<A: 'static + Send>(
        &self,
        scan: ScanBuilder<A>,
        deleted: Option<&Mask>,
    ) -> ScanBuilder<A> {
        match (self, deleted) {
            (RowSelection::All, None) => scan,
            (RowSelection::All, Some(deleted)) => {
                scan.with_selection(Selection::ExcludeByIndex(deleted_ids(deleted)))
            }
            (RowSelection::Range(range), None) => scan.with_row_range(range.clone()),
            (RowSelection::Range(range), Some(deleted)) => scan
                .with_row_range(range.clone())
                .with_selection(Selection::ExcludeByIndex(deleted_ids(deleted))),
            (RowSelection::Ids(ids), None) => scan.with_row_indices(strict_ids(ids)),
            (RowSelection::Ids(ids), Some(deleted)) => {
                scan.with_row_indices(subtract_deleted(ids, deleted))
            }
        }
    }
}

/// The set positions of a tombstone mask as an ascending id list.
fn deleted_ids(deleted: &Mask) -> StrictSortedBuffer<u64> {
    let ids = match deleted.indices() {
        AllOr::All => Buffer::from_iter(0..deleted.len() as u64),
        AllOr::None => Buffer::empty(),
        AllOr::Some(indices) => Buffer::from_iter(indices.iter().map(|&i| i as u64)),
    };
    StrictSortedBuffer::try_new(ids).vortex_expect("mask indices are ascending and unique")
}

/// An ascending id list with the tombstoned rows removed.
fn subtract_deleted(ids: &Buffer<u64>, deleted: &Mask) -> StrictSortedBuffer<u64> {
    let ids = Buffer::from_iter(
        ids.iter()
            .copied()
            .filter(|&id| !deleted.value(id as usize)),
    );
    StrictSortedBuffer::try_new(ids).vortex_expect("a RowSelection id list is ascending and unique")
}

/// A [`RowSelection::Ids`] list as the strictly sorted buffer the scan takes;
/// ascending and unique is the variant's invariant.
pub(super) fn strict_ids(ids: &Buffer<u64>) -> StrictSortedBuffer<u64> {
    StrictSortedBuffer::try_new(ids.clone())
        .vortex_expect("a RowSelection id list is ascending and unique")
}

/// One `u32` column of the file at the rows `selection` covers, in file
/// order; positions align with `selection.apply`. Tombstones are not applied.
pub(crate) async fn read_column_codes(
    file: &NativeStoreFile,
    column: &'static str,
    selection: &RowSelection,
) -> Result<Vec<u32>> {
    use vortex_array::arrays::PrimitiveArray;

    let mut scan = file.scan().map_err(VortexRdfError::Vortex)?;
    let scope = scan.dtype().map_err(VortexRdfError::Vortex)?;
    scan = scan.with_projection(
        file.bound_exprs()
            .bind(QUAD_SCOPE, &select(&[column][..], root()), &scope)
            .map_err(VortexRdfError::Vortex)?,
    );
    let codes = scan_column(selection.restrict_scan(scan, None), column).await?;
    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let prim = codes
        .execute::<PrimitiveArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    Ok(prim.as_slice::<u32>().to_vec())
}

/// The pushed-down filter for a pattern under `codes`' layout: `AND` of
/// `column == code` per compiled constraint, `lit(false)` for an unmatchable
/// pattern, `None` when nothing is bound.
pub(crate) fn build_file_filter(
    pattern: QuadPattern<'_>,
    codes: &mut PatternCodes,
) -> Result<Option<Expression>> {
    Ok(match codes.constraints(pattern)? {
        Constraints::AlwaysFalse => Some(lit(false)),
        Constraints::Eq(eqs) => eq_conjunction(eqs),
    })
}

/// The conjunction of `column == value` equalities over root fields, `None`
/// for an empty constraint set.
pub(crate) fn eq_conjunction(
    constraints: impl IntoIterator<Item = (&'static str, Scalar)>,
) -> Option<Expression> {
    and_collect(
        constraints
            .into_iter()
            .map(|(column, value)| eq(get_item(column, root()), lit(value))),
    )
}

/// The `(column, code)` pairs of a filter built by [`build_file_filter`];
/// `None` for any other shape (the `AlwaysFalse` literal, a non-integer
/// value).
pub(crate) fn eq_code_pairs(filter: &Expression) -> Option<Vec<(String, u64)>> {
    use vortex_array::scalar_fn::fns::binary::Binary;
    use vortex_array::scalar_fn::fns::get_item::GetItem;
    use vortex_array::scalar_fn::fns::literal::Literal;
    use vortex_array::scalar_fn::fns::operators::Operator;

    conjuncts(filter)
        .iter()
        .map(|c| {
            let op = c.as_opt::<Binary>()?;
            if *op != Operator::Eq {
                return None;
            }
            let field = c.child(0).as_opt::<GetItem>()?;
            let scalar = c.child(1).as_opt::<Literal>()?;
            let code = u64::try_from(scalar).ok()?;
            Some((field.to_string(), code))
        })
        .collect()
}

/// The `[lo, hi)` run of rows equal to `native` in a sorted column, located
/// by binary search over the column's chunk probes (`chunks`, resolved only
/// when the gates pass). `within` must be a range whose slice of the column
/// is itself sorted. `Ok(None)` declines: an unsorted column, a non-integer
/// probe value, or no chunk handle.
pub(crate) async fn locate_run(
    file: &NativeStoreFile,
    sorted: bool,
    native: &Scalar,
    within: Option<Range<u64>>,
    chunks: impl FnOnce() -> Option<Arc<ColumnChunks>>,
) -> Result<Option<Range<u64>>> {
    if !sorted {
        return Ok(None);
    }
    let Ok(needle) = u64::try_from(native) else {
        return Ok(None);
    };
    let Some(chunks) = chunks() else {
        return Ok(None);
    };
    let source = file.segment_source();
    let session = file.session();
    match within {
        None => chunks.bounds(needle, &source, session).await,
        Some(range) => chunks.bounds_in(range, needle, &source, session).await,
    }
    .map_err(VortexRdfError::Vortex)
}

/// The exact row range of `subject`'s run in a sorted file. `None` declines:
/// an unsorted file, a subject without a native probe scalar, or a subject
/// column without a chunk handle.
pub(crate) async fn locate_subject_run(
    file: &NativeStoreFile,
    codes: &mut PatternCodes,
    subject: &NamedOrBlankNode,
) -> Result<Option<Range<u64>>> {
    if !file.quads_sorted() {
        return Ok(None);
    }
    let Ok(Some(probe)) = codes.probe_scalar(TermRef::Subject(subject)) else {
        return Ok(None);
    };
    locate_run(file, true, &probe, None, || {
        file.column_chunks(schema::COL_S)
    })
    .await
}

/// The rows of a point-sized exact selection, read point by point through
/// the quad columns' chunk probes, an eq-conjunction `filter` applied per row
/// (an eq column's value is its constant). `None` declines: a wide or
/// non-exact selection, a filter that is not an eq conjunction, a column
/// without a chunk handle, or a chunk declining the probe.
pub(crate) async fn file_point_rows(
    file: &NativeStoreFile,
    columns: &[&str],
    filter: Option<&Expression>,
    selection: &RowSelection,
    deleted: Option<&Mask>,
) -> Result<Option<ArrayRef>> {
    let Some(rows) = selection.point_sized_live_rows(deleted) else {
        return Ok(None);
    };
    let eqs = match filter {
        None => Vec::new(),
        Some(f) => match eq_code_pairs(f) {
            Some(pairs) => pairs,
            None => return Ok(None),
        },
    };
    let Some(handles) = columns
        .iter()
        .map(|c| file.column_chunks(c))
        .collect::<Option<Vec<_>>>()
    else {
        return Ok(None);
    };
    let source = file.segment_source();
    let session = file.session();

    let mut live = rows;
    for (col, code) in &eqs {
        let Some(idx) = columns.iter().position(|c| c == col) else {
            return Ok(None);
        };
        let mut kept = Vec::with_capacity(live.len());
        for &row in &live {
            match handles[idx]
                .value_at(row, &source, session)
                .await
                .map_err(VortexRdfError::Vortex)?
            {
                Some(v) if v == *code => kept.push(row),
                Some(_) => {}
                None => return Ok(None),
            }
        }
        live = kept;
        if live.is_empty() {
            break;
        }
    }

    point_read_struct(file, &handles, columns, &live, |col| {
        eqs.iter().find(|(c, _)| c == col).map(|(_, v)| *v)
    })
    .await
}

/// `columns` at `rows`, read point by point through their chunk `handles`
/// into one canonical struct; a column `constant` answers is filled with that
/// value. `Ok(None)` declines: a chunk declining the probe, or a column type
/// the canonical child cannot hold.
async fn point_read_struct(
    file: &NativeStoreFile,
    handles: &[Arc<ColumnChunks>],
    columns: &[&str],
    rows: &[u64],
    constant: impl Fn(&str) -> Option<u64>,
) -> Result<Option<ArrayRef>> {
    use vortex_array::dtype::FieldName;
    use vortex_array::validity::Validity;

    let source = file.segment_source();
    let session = file.session();
    let mut children = Vec::with_capacity(columns.len());
    for (handle, col) in handles.iter().zip(columns) {
        let mut values = Vec::with_capacity(rows.len());
        match constant(col) {
            Some(v) => values.extend(std::iter::repeat_n(v, rows.len())),
            None => {
                for &row in rows {
                    match handle
                        .value_at(row, &source, session)
                        .await
                        .map_err(VortexRdfError::Vortex)?
                    {
                        Some(v) => values.push(v),
                        None => return Ok(None),
                    }
                }
            }
        }
        let Some(child) = primitive_from_u64_reads(handle.dtype().as_ptype(), values.into_iter())
        else {
            return Ok(None);
        };
        children.push(child.into_array());
    }
    let names: Vec<FieldName> = columns.iter().map(|c| FieldName::from(*c)).collect();
    Ok(Some(
        StructArray::try_new(names.into(), children, rows.len(), Validity::NonNullable)
            .map_err(VortexRdfError::Vortex)?
            .into_array(),
    ))
}

/// `columns` of the component `component` over `range`, read point by point
/// through the component's chunk probes into one child-named struct.
/// `Ok(None)` declines: a column without a chunk handle, or a chunk declining
/// the probe.
pub(crate) async fn component_point_chunk(
    file: &NativeStoreFile,
    component: &str,
    columns: &[&'static str],
    range: Range<u64>,
) -> Result<Option<ArrayRef>> {
    let Some(handles) = columns
        .iter()
        .map(|c| file.component_column_chunks(component, c))
        .collect::<Option<Vec<_>>>()
    else {
        return Ok(None);
    };
    let rows: Vec<u64> = range.collect();
    point_read_struct(file, &handles, columns, &rows, |_| None).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use vortex_array::expr::{and, eq, get_item, root};

    /// Only a conjunction of `field == literal` over root fields decodes to
    /// `(column, code)` pairs.
    #[test]
    fn eq_code_pairs_accepts_only_eq_conjunctions() {
        let filter = and(
            eq(get_item("p", root()), lit(3u32)),
            eq(get_item("o", root()), lit(7u32)),
        );
        assert_eq!(
            eq_code_pairs(&filter),
            Some(vec![("p".to_string(), 3), ("o".to_string(), 7)])
        );
        assert!(eq_code_pairs(&lit(false)).is_none());
        assert!(eq_code_pairs(&eq(get_item("p", root()), lit("x"))).is_none());
    }

    /// `eq_conjunction` is the inverse of `eq_code_pairs` and answers `None`
    /// for no constraints.
    #[test]
    fn eq_conjunction_roundtrips_through_eq_code_pairs() {
        let filter =
            eq_conjunction([("p", Scalar::from(3u32)), ("o", Scalar::from(7u32))]).unwrap();
        assert_eq!(
            eq_code_pairs(&filter),
            Some(vec![("p".to_string(), 3), ("o".to_string(), 7)])
        );
        assert!(eq_conjunction(std::iter::empty()).is_none());
    }
}
