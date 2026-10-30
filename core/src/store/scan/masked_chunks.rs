//! A filtered file view's rows as chunks, read by the filter's masks a few
//! splits at a time: the chunked twin of the one-shot read's masks-first read,
//! which evaluates every split's mask before reading the rows they keep. Here
//! the masks come from a [`SplitMaskStream`] as the stream reaches them, and
//! each group of whole splits keeping a chunk's rows is read as
//! [`MaskedRead`] chooses, so a chunk of a sparse filter costs what the
//! one-shot read of its rows does, not a decode of every row of its splits.

use std::ops::Range;
use std::sync::Arc;

use futures::stream::{self, Stream, StreamExt};
use vortex_array::ArrayRef;
use vortex_mask::Mask;

use crate::error::Result;
use crate::store::RowChunkStream;
use crate::store::array::rechunk;
use crate::store::persist::native_file::NativeStoreFile;
use crate::store::scan::file_filter::{SplitMaskStream, SplitMasks};
use crate::store::scan::file_reads::{file_point_rows, masked_projection, take_file_rows};
use crate::store::scan::read_plan::MaskedRead;

/// The rows the masks keep, of `columns`, in file order, in chunks of at most
/// `batch_rows` rows: whole splits are gathered ([`kept_groups`]) until they
/// keep `batch_rows` rows or the masks run out, and their kept rows read with
/// no filter left to check ([`read_kept`]), then cut to the cap. A group is
/// read when the stream reaches it, and the masks past it are evaluated only
/// as far as their look-ahead.
pub(crate) fn masked_row_chunks(
    file: Arc<NativeStoreFile>,
    masks: SplitMaskStream,
    columns: Arc<[&'static str]>,
    batch_rows: usize,
) -> RowChunkStream {
    kept_groups(masks, batch_rows)
        .then(move |group| {
            let (file, columns) = (Arc::clone(&file), Arc::clone(&columns));
            async move { read_kept(&file, &columns, &group?).await }
        })
        .flat_map(move |rows| {
            stream::iter(match rows {
                Ok(Some(rows)) => rechunk(rows, batch_rows).collect(),
                Ok(None) => Vec::new(),
                Err(error) => vec![Err(error)],
            })
        })
        .boxed()
}

/// Whole splits of `masks` in file order, a split's pieces joined into its
/// one mask, grouped until a group keeps `batch_rows` rows; the group that
/// ends the masks keeps what is left, and is not yielded when that is no
/// row.
fn kept_groups(
    masks: SplitMaskStream,
    batch_rows: usize,
) -> impl Stream<Item = Result<SplitMasks>> + Send + 'static {
    struct Groups {
        masks: SplitMaskStream,
        /// The first piece of the next group's first split, read past the
        /// group before it.
        next: Option<(Mask, Range<u64>, usize)>,
        done: bool,
    }
    let groups = Groups {
        masks,
        next: None,
        done: false,
    };
    stream::unfold(groups, move |mut groups| async move {
        if groups.done {
            return None;
        }
        let mut group: Vec<(Mask, Range<u64>)> = Vec::new();
        let (mut last_split, mut kept) = (None, 0usize);
        loop {
            let (mask, range, split) = match groups.next.take() {
                Some(piece) => piece,
                None => match groups.masks.next().await {
                    Some(Ok(piece)) => piece,
                    Some(Err(error)) => {
                        groups.done = true;
                        return Some((Err(error), groups));
                    }
                    None => {
                        groups.done = true;
                        break;
                    }
                },
            };
            let count = mask.true_count();
            if last_split == Some(split) {
                // The next piece of the split the group's last mask holds.
                let (last, last_range) = group.last_mut().expect("the split's first piece");
                let before = std::mem::replace(last, Mask::new_false(0));
                *last = [before, mask].into_iter().collect();
                last_range.end = range.end;
                kept += count;
                continue;
            }
            // A split begins: the group before it is whole, and full once it
            // keeps a chunk's rows.
            if kept >= batch_rows {
                groups.next = Some((mask, range, split));
                break;
            }
            group.push((mask, range));
            last_split = Some(split);
            kept += count;
        }
        let group = SplitMasks::from_masks(group);
        (group.kept() > 0).then_some((Ok(group), groups))
    })
}

/// The rows `masks` keep, of `columns`, read as [`MaskedRead::of`] chooses
/// them, with the one-shot read's fallbacks: point reads that decline take
/// the ids, and a take that declines projects each split at its mask.
/// `None` when they keep no row.
async fn read_kept(
    file: &Arc<NativeStoreFile>,
    columns: &[&'static str],
    masks: &SplitMasks,
) -> Result<Option<ArrayRef>> {
    if masks.kept() == 0 {
        return Ok(None);
    }
    let read = MaskedRead::of(file, masks)?;
    if read != MaskedRead::Project {
        let ids = masks.first_ids(usize::MAX);
        if read == MaskedRead::Points
            && let Some(rows) = file_point_rows(file, columns, None, ids.clone()).await?
        {
            return Ok(Some(rows));
        }
        if let Some(rows) = take_file_rows(file, columns, &ids).await? {
            return Ok(Some(rows));
        }
    }
    masked_projection(file, columns, masks).await.map(Some)
}
