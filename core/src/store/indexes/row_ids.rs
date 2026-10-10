//! Row-id acquisition for index resolutions: decoding a component's rid
//! column into the ascending, unique `Buffer<RowId>` of base rows every
//! resolution answers in, and the file-backed readers (point reads through
//! cached chunk probes, rid-only pushed-down scans) that produce it from an
//! index child.

#[cfg(feature = "file-io")]
use std::ops::Range;

use vortex_array::arrays::PrimitiveArray;
#[cfg(feature = "file-io")]
use vortex_array::arrays::struct_::{StructArray, StructArrayExt};
use vortex_array::dtype::DType;
#[cfg(feature = "file-io")]
use vortex_array::expr::{Expression, and_collect, eq, get_item, lit, root, select};
#[cfg(feature = "file-io")]
use vortex_array::scalar::Scalar;
use vortex_array::{ArrayRef, VortexSessionExecute};
use vortex_buffer::{Buffer, BufferMut};

#[cfg(feature = "file-io")]
use super::{FileServePlan, IndexResolution, ResolvedRoles, ResolvedRowIds};
use crate::error::{Result, VortexRdfError};
use crate::session::VORTEX_SESSION;
use crate::store::schema::{ROW_ID_PTYPE, RowId};

/// The conjunction of `column == value` equalities over root fields — the
/// filter shape every pushed-down index probe and serve scan uses. `None`
/// for an empty constraint set.
#[cfg(feature = "file-io")]
pub(crate) fn eq_conjunction(
    constraints: impl IntoIterator<Item = (&'static str, Scalar)>,
) -> Option<Expression> {
    and_collect(
        constraints
            .into_iter()
            .map(|(column, value)| eq(get_item(column, root()), lit(value))),
    )
}

/// The `[lo, hi)` run of a sorted component column's rows equal to `native`,
/// located by binary search over the column's cached chunk probes — reading
/// only the chunk leaves the bisection crosses. Searched over the whole
/// column, or `within` a row range whose slice of the column is itself sorted
/// (a lead run for a prefix probe).
///
/// `Ok(None)` declines the location (the caller keeps its pushed-down scan):
/// a child not globally sorted, a probe value that is not an integer (string
/// value columns), or a column whose chunks resolve no probe.
#[cfg(feature = "file-io")]
pub(crate) async fn locate_component_run(
    file: &crate::store::native_file::NativeStoreFile,
    component: &str,
    column: &str,
    native: &Scalar,
    within: Option<Range<u64>>,
    sorted: bool,
) -> Result<Option<Range<u64>>> {
    if !sorted {
        return Ok(None);
    }
    let Ok(needle) = u64::try_from(native) else {
        return Ok(None);
    };
    let Some(chunks) = file.component_column_chunks(component, column) else {
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

/// The row ids of a located index-child run, read point-by-point from the
/// child's rid column through its cached chunk probes and re-sorted into
/// base row order — the file counterpart of slicing an in-memory rid run.
/// `Ok(None)` when the rid column's chunks decline (the caller keeps its
/// scan); rids are unique by construction, so sorting alone suffices.
///
/// `rid_column` comes from the calling index — the hub names no index's
/// columns.
#[cfg(feature = "file-io")]
pub(crate) async fn rid_point_reads(
    file: &crate::store::native_file::NativeStoreFile,
    component: &str,
    rid_column: &str,
    range: Range<u64>,
) -> Result<Option<Buffer<RowId>>> {
    let Some(chunks) = file.component_column_chunks(component, rid_column) else {
        return Ok(None);
    };
    let source = file.segment_source();
    let session = file.session();
    // A point-read run is at most `POINT_GATHER_MAX_ROWS` wide.
    let mut ids = Vec::with_capacity((range.end - range.start) as usize);
    for row in range {
        match chunks
            .value_at(row, &source, session)
            .await
            .map_err(VortexRdfError::Vortex)?
        {
            Some(rid) => ids.push(super::base_row(rid)),
            None => return Ok(None),
        }
    }
    ids.sort_unstable();
    check_addressable(&ids)?;
    Ok(Some(Buffer::from(ids)))
}

/// Decode a row-id column into the ascending, unique `Buffer<RowId>` every
/// index resolution answers in: the base rows its ids name ([`base_row`]).
///
/// Sorting is required, not incidental: the ids come out in the index's own
/// order, and both `Selection::IncludeByIndex` and the selection algebra need
/// them ascending. They are unique by construction (each index row references
/// one quad row), so sorting alone suffices.
///
/// [`base_row`]: super::base_row
pub(crate) fn sorted_row_ids(row_id_column: ArrayRef) -> Result<Buffer<RowId>> {
    use vortex_array::builtins::ArrayBuiltins;
    use vortex_array::dtype::Nullability;

    if row_id_column.is_empty() {
        return Ok(Buffer::empty());
    }
    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    // A no-op cast for the u64 column every writer here produces.
    let ids = row_id_column
        .cast(DType::Primitive(ROW_ID_PTYPE, Nullability::NonNullable))
        .map_err(VortexRdfError::Vortex)?
        .execute::<PrimitiveArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?
        .into_buffer::<RowId>();

    // The freshly-executed buffer is normally uniquely owned, so the sort
    // runs in place with no copy; a shared buffer (someone else still holds
    // the execution's output) falls back to one copy.
    let mut ids = match ids.try_into_mut() {
        Ok(ids) => ids,
        Err(ids) => BufferMut::copy_from(ids.as_slice()),
    };
    #[cfg(test)]
    for id in ids.as_mut_slice() {
        *id = super::base_row(*id);
    }
    ids.as_mut_slice().sort_unstable();
    check_addressable(&ids)?;
    Ok(ids.freeze())
}

/// Refuse ascending base rows `ids` whose last one a `usize` cannot hold —
/// on a 32-bit target (wasm) a row id past `u32::MAX`, which no base there
/// holds — so that every later use of an id as a position is exact rather
/// than narrowed onto another row.
fn check_addressable(ids: &[RowId]) -> Result<()> {
    match ids.last() {
        Some(&last) => super::row_index(last).map(|_| ()),
        None => Ok(()),
    }
}

/// Scan `rid_column` for the rows where every `(value_column, probe)`
/// equality holds, returning the primary row ids as an ascending, unique
/// buffer (the shape vortex's `Selection::IncludeByIndex` requires) — the
/// file-backed probe shared by the secondary indexes.
///
/// Each equality is a plain `eq`, the same encoding the serve-plan filter
/// uses: a binary `Eq` falsifies against the same zone min/max envelope as a
/// `>= probe AND <= probe` range pair (see vortex's
/// `stats/rewrite/builtins.rs`) while evaluating a single conjunct. Output
/// order is irrelevant (the ids are sorted afterwards), so the scan is built
/// unordered.
#[cfg(feature = "file-io")]
pub(crate) async fn scan_index_row_ids(
    reader: vortex_layout::LayoutReaderRef,
    value_constraints: &[(&'static str, Scalar)],
    rid_column: &'static str,
    memo: &crate::store::native_file::BoundExprMemo,
    scope: &'static str,
) -> Result<Buffer<RowId>> {
    // Every index probes at least one value column; an empty constraint set
    // would mean "all rows", which no resolver asks for.
    let Some(filter) = eq_conjunction(value_constraints.iter().cloned()) else {
        return Ok(Buffer::empty());
    };
    let filter = memo
        .bind(scope, &filter, reader.dtype())
        .map_err(VortexRdfError::Vortex)?;

    read_scanned_row_ids(
        rid_scan(reader, rid_column, memo, scope)?.with_filter(filter),
        rid_column,
    )
    .await
}

/// The row ids of a *located* index-child run — the rows a resolver bounded
/// by binary-searching the child's cached chunk probes — read by a rid-only
/// scan restricted to that range. The wide-run counterpart of
/// [`rid_point_reads`], for runs too large to read point by point.
///
/// The scan carries no filter: the location bounded exactly the rows the
/// constraints select, so re-testing the value columns would only re-read and
/// re-compare them. A resolver whose location covers only *some* of its
/// constraints must keep [`scan_index_row_ids`], whose filter tests them all.
#[cfg(feature = "file-io")]
pub(crate) async fn scan_located_row_ids(
    reader: vortex_layout::LayoutReaderRef,
    rid_column: &'static str,
    range: Range<u64>,
    memo: &crate::store::native_file::BoundExprMemo,
    scope: &'static str,
) -> Result<Buffer<RowId>> {
    read_scanned_row_ids(
        rid_scan(reader, rid_column, memo, scope)?.with_row_range(range),
        rid_column,
    )
    .await
}

/// The row ids of a located index-child run — or of a window of one — in base
/// row order: point reads through the rid column's chunk probes for a range
/// within the point-read cap, otherwise — or when a chunk declines mid-read —
/// a rid-only scan restricted to the range (the location bounded exactly the
/// matched rows, so no filter is re-tested).
///
/// `range` is not empty: an empty run or window reads nothing at all, so its
/// caller answers it without asking.
#[cfg(feature = "file-io")]
pub(crate) async fn read_located_rids(
    file: &crate::store::native_file::NativeStoreFile,
    component: &'static str,
    reader: &vortex_layout::LayoutReaderRef,
    rid_column: &'static str,
    range: Range<u64>,
    scope: &'static str,
) -> Result<Buffer<RowId>> {
    debug_assert!(
        range.start < range.end,
        "an empty run or window reads nothing: its caller does not ask"
    );
    #[cfg(test)]
    file.note_located_rid_reads(range.end - range.start);
    if crate::store::selection::point_sized(range.end - range.start)
        && let Some(ids) = rid_point_reads(file, component, rid_column, range.clone()).await?
    {
        return Ok(ids);
    }
    scan_located_row_ids(reader.clone(), rid_column, range, file.bound_exprs(), scope).await
}

/// A rid-only scan of an index child: just the row-id column, built unordered
/// (callers sort the ids anyway; [`read_scanned_row_ids`] drives it inline, in
/// split order, whatever the flag says). Restrictions — a filter, a row range
/// — are the caller's to add.
#[cfg(feature = "file-io")]
fn rid_scan(
    reader: vortex_layout::LayoutReaderRef,
    rid_column: &'static str,
    memo: &crate::store::native_file::BoundExprMemo,
    scope: &'static str,
) -> Result<vortex_layout::scan::scan_builder::ScanBuilder<ArrayRef>> {
    let projection = memo
        .bind(scope, &select([rid_column], root()), reader.dtype())
        .map_err(VortexRdfError::Vortex)?;
    Ok(
        vortex_layout::scan::scan_builder::ScanBuilder::new(VORTEX_SESSION.clone(), reader)
            .with_projection(projection)
            .with_ordered(false),
    )
}

/// Run a rid-only scan and decode its row-id column into the ascending,
/// unique buffer every index resolution answers in. The scan is driven inline
/// by `file_scan::read_index_row_ids`.
#[cfg(feature = "file-io")]
async fn read_scanned_row_ids(
    scan: vortex_layout::scan::scan_builder::ScanBuilder<ArrayRef>,
    rid_column: &'static str,
) -> Result<Buffer<RowId>> {
    let arr = crate::store::scan::file_scan::read_index_row_ids(scan).await?;

    if arr.is_empty() {
        return Ok(Buffer::empty());
    }

    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let struct_arr = arr
        .execute::<StructArray>(&mut ctx)
        .map_err(VortexRdfError::Vortex)?;
    sorted_row_ids(
        struct_arr
            .unmasked_field_by_name(rid_column)
            .cloned()
            .map_err(VortexRdfError::Vortex)?,
    )
}

/// An eager file resolution off the rid-only pushed-down scan — the shared
/// tail of the file resolvers whenever no serving plan defers the ids (a
/// back-reference child, or a copy resolution that couldn't build its plan):
/// `Empty` when the scan proves the probe matches nothing.
#[cfg(feature = "file-io")]
pub(crate) async fn resolve_eager_from_scan(
    reader: vortex_layout::LayoutReaderRef,
    constraints: &[(&'static str, Scalar)],
    rid_column: &'static str,
    resolves: ResolvedRoles,
    memo: &crate::store::native_file::BoundExprMemo,
    scope: &'static str,
) -> Result<IndexResolution<FileServePlan>> {
    let row_ids = scan_index_row_ids(reader, constraints, rid_column, memo, scope).await?;
    if row_ids.is_empty() {
        return Ok(IndexResolution::Empty);
    }
    Ok(IndexResolution::Resolved {
        row_ids: ResolvedRowIds::Eager(row_ids),
        resolves,
        serve: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use vortex_array::IntoArray;

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

    /// Both rid scans, the located run and the pushed-down equality, spawn
    /// nothing with the split limit at zero; a control row scan of the file
    /// spawns all of its splits.
    #[cfg(feature = "file-io")]
    #[tokio::test]
    async fn index_row_id_scans_drive_inline_whatever_the_limit() {
        use crate::IndexType;
        use crate::io::read::{FileAccess, open_vortex_file, scan_all_reader};
        use crate::store::array::{field_as, into_struct_array};
        use crate::store::indexes::COL_RID;
        use crate::store::native_file::NativeStoreFile;
        use crate::store::scan::file_scan::{driver_hooks, read_all_rows};

        const COMPONENT: &str = "index:ref-p";
        let rows = 50;
        let quads = crate::tests::modular_quads_for_tests(rows);
        let (_dir, path) =
            crate::tests::write_store_file_for_tests(quads, vec![IndexType::SecondaryByReference])
                .await;
        let file =
            NativeStoreFile::try_new(open_vortex_file(&path, FileAccess::Mapped).await.unwrap())
                .unwrap();
        let (_, reader) = file
            .component_reader(COMPONENT)
            .unwrap()
            .expect("the reference child of the predicate column");
        let _limit = driver_hooks::ForcedLimit::set(0);
        driver_hooks::take_spawned();

        // The located-run scan: every row of the child.
        let ids = scan_located_row_ids(
            reader.clone(),
            COL_RID,
            0..rows as u64,
            file.bound_exprs(),
            COMPONENT,
        )
        .await
        .unwrap();
        assert_eq!(driver_hooks::take_spawned(), 0, "located-run scan spawned");
        assert_eq!(ids.as_slice(), (0..rows as RowId).collect::<Vec<_>>());

        // The pushed-down equality, probing the child's first value.
        let child = scan_all_reader(reader.clone()).await.unwrap();
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let child = into_struct_array(child).unwrap();
        let first = field_as::<PrimitiveArray>(&child, "val", &mut ctx)
            .unwrap()
            .as_slice::<u64>()[0];
        let matched = scan_index_row_ids(
            reader,
            &[("val", Scalar::from(first))],
            COL_RID,
            file.bound_exprs(),
            COMPONENT,
        )
        .await
        .unwrap();
        assert_eq!(driver_hooks::take_spawned(), 0, "equality scan spawned");
        assert!(!matched.is_empty() && matched.len() < rows);
        assert!(matched.as_slice().windows(2).all(|pair| pair[0] < pair[1]));

        // Control: a row scan of the same file goes to the workers, every
        // split of it.
        let splits = file.scan().unwrap().build().unwrap().len();
        assert!(splits > 0);
        read_all_rows(file.scan().unwrap()).await.unwrap();
        assert_eq!(driver_hooks::take_spawned(), splits);
    }
}
