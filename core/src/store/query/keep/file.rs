//! The file backend of [`keep`](VortexRdfStore::keep): on a file sorted by
//! subject a keep on `s` is located as runs in the mapped leaf metadata, so
//! nothing is read and a pending filter stays pending; any other keep, and
//! any the location declines, is tested in memory over the column streamed
//! for the view's rows.

use super::Keep;
use crate::debug;
use crate::error::Result;
use crate::store::persist::native_file::NativeStoreFile;
use crate::store::scan::{file_filter, file_reads};
use crate::store::schema::QuadColumn;
use crate::store::view::selection::{RowSelection, ViewSelection};
use crate::store::{FileBase, VortexRdfStore};

impl VortexRdfStore {
    /// The file backend of [`keep`](Self::keep). On a file sorted by subject a
    /// keep on `s` is first located (see [`keep_located`](Self::keep_located)):
    /// the rows it admits are runs of the sorted column, found in the mapped
    /// leaf metadata, so the selection narrows to them and a pending filter
    /// stays pending. Every other keep, and any the location declines, is
    /// tested in memory: the view's rows resolve to exact ids (a pending
    /// filter evaluated, deferred row ids materialized), the column is
    /// streamed for them and each code is tested. A keep never becomes a
    /// filter expression: its code list is per query, and every distinct
    /// expression would pin new entries in vortex's identity-keyed reader
    /// caches.
    #[cfg(feature = "file-io")]
    pub(super) async fn keep_file(
        &self,
        base: &FileBase,
        column: QuadColumn,
        keep: &Keep,
    ) -> Result<Self> {
        let t = debug::timer();
        let FileBase { file, filter, .. } = base;
        let row_count = file.row_count() as usize;
        let selection = self.quads.selection.materialized_async().await?;
        if selection.is_empty(row_count) {
            return Ok(self.empty_view());
        }
        let located = match column {
            QuadColumn::S => Self::keep_located(file, &selection, keep, row_count).await?,
            _ => None,
        };
        let (filter, narrowed) = match located {
            Some(narrowed) => {
                debug::step!(
                    t,
                    "[keep] {:?} located as runs of the sorted column",
                    column
                );
                (filter.clone(), narrowed)
            }
            None => {
                let selection = match filter {
                    Some(f) => RowSelection::All
                        .refine(&file_filter::matching_file_rows(file, Some(f), &selection).await?),
                    None => selection,
                };
                let test = keep.test();
                let positions =
                    file_reads::column_positions(file, column.name(), &selection, |code| {
                        test.admits(code)
                    })
                    .await?;
                let selected = selection.len(row_count);
                let narrowed = selection.refine_positions(positions, selected);
                debug::step!(
                    t,
                    "[keep] {:?} tested in memory over {} file rows",
                    column,
                    selected
                );
                (None, narrowed)
            }
        };
        if narrowed.is_empty(row_count) {
            return Ok(self.empty_view());
        }
        Ok(self.derived(
            self.quads
                .file_with(base, filter, ViewSelection::Exact(narrowed), None),
            self.tail.clone(),
        ))
    }

    /// A keep on the subject column by located runs: in a file sorted by `s`
    /// the rows with `lo <= s < hi` are one run, and each of a set's codes one
    /// run, found by binary search over the mapped column's leaf metadata, so
    /// no column is read, no expression bound, and a pending filter stays
    /// pending beside the narrowed selection. The runs intersect `selection`,
    /// which stays a range wherever the admitted rows are one run of it.
    ///
    /// `None` declines to the stream: the file is not sorted by `s`, a chunk
    /// has no probe, or the set is too large to look up code by code (see
    /// [`set_is_located`]).
    #[cfg(feature = "file-io")]
    async fn keep_located(
        file: &NativeStoreFile,
        selection: &RowSelection,
        keep: &Keep,
        row_count: usize,
    ) -> Result<Option<RowSelection>> {
        Ok(match keep {
            Keep::Range(lo, hi) => file_reads::locate_subject_code_range(file, *lo..*hi)
                .await?
                .map(|run| selection.clone().intersect_range(run)),
            Keep::Set(codes) if set_is_located(codes.len(), selection.len(row_count)) => {
                file_reads::locate_subject_code_runs(file, codes.as_slice())
                    .await?
                    .map(|runs| selection.clone().intersect_runs(&runs))
            }
            Keep::Set(_) => None,
        })
    }
}

/// The selected rows one code of a subject set keep is worth when choosing
/// between looking the set up and streaming the column: a lookup costs two
/// binary searches over the mapped chunk metadata per code, whatever the
/// selection, the stream a little per selected row plus a fixed scan, and the
/// two meet near this many rows per code.
#[cfg(feature = "file-io")]
const LOCATED_SET_ROWS_PER_CODE: usize = 16;

/// Whether a set of `codes` codes on the subject column is looked up code by
/// code over a selection of `rows` rows rather than streamed: while it has at
/// most one code per [`LOCATED_SET_ROWS_PER_CODE`] rows, rounded up, so at
/// least one code over any non-empty selection.
#[cfg(feature = "file-io")]
fn set_is_located(codes: usize, rows: usize) -> bool {
    codes <= rows.div_ceil(LOCATED_SET_ROWS_PER_CODE)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A set is looked up while it has at most one code per sixteen rows of
    /// the selection, rounded up.
    #[test]
    fn located_sets_are_bounded_by_the_selection() {
        assert!(set_is_located(1, 1));
        assert!(set_is_located(1, 16));
        assert!(!set_is_located(2, 16));
        assert!(set_is_located(2, 17));
        assert!(set_is_located(500_000, 8_000_000));
        assert!(!set_is_located(500_001, 8_000_000));
        assert!(!set_is_located(1, 0));
    }
}
