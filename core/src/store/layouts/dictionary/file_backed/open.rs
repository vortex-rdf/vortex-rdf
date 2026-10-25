//! Opening a [`FileBackedDict`]: the dictionary child's term column is taken
//! apart into its flat leaves, one per FSST window, and each window's first
//! and last term are read from the column's exact zone maps when the zones
//! are cut at the leaves, else from the leaves themselves. A child no window
//! search can address opens nothing, and the caller lifts it resident;
//! windows out of term order fail the open.

use std::sync::{Arc, Mutex, OnceLock};

use vortex_array::arrays::VarBinViewArray;
use vortex_array::arrays::struct_::{StructArray, StructArrayExt};
use vortex_array::{ExecutionCtx, VortexSessionExecute};
use vortex_layout::layouts::flat::FlatLayout;
use vortex_layout::layouts::struct_::Struct as StructLayout;
use vortex_layout::layouts::zoned::Zoned;
use vortex_layout::segments::SegmentSource;
use vortex_layout::{LayoutChildType, LayoutRef};
use vortex_rdf_encoded_search::{fetch_flat, flat_leaves};

use crate::error::{Result, VortexRdfError};
use crate::io::container::DICT_COMPONENT_NAME;
use crate::session::VORTEX_SESSION;
use crate::store::array::StrColReader;
use crate::store::persist::native_file::NativeStoreFile;

use super::super::storage::{COL_DICT_TERM, TermChunk, window_bound_aggregates};
use super::super::term_dict::{CodeSpace, next_dictionary_id};
use super::cache::{WINDOW_CACHE_BYTES, WindowCache};
use super::{FileBackedDict, Window, Windows};

/// A window's first and last term.
type Bounds = (Box<[u8]>, Box<[u8]>);

/// The term column's flat leaves, each with its row count.
type Leaves = Vec<(FlatLayout, usize)>;

impl FileBackedDict {
    /// The file-backed form of `native`'s dictionary child: its reader, its
    /// leaves, and each window's bounds (from the exact zone maps, else from
    /// the leaves). `None` when the file has no dictionary component, or the
    /// child's shape cannot be searched by window (an empty child, a single
    /// flat struct leaf): the caller lifts it resident. Windows out of term
    /// order are an invalid dictionary, not a shape: a `Deserialization`
    /// error.
    pub(crate) async fn open(native: &NativeStoreFile) -> Result<Option<Self>> {
        Self::open_with_budget(native, WINDOW_CACHE_BYTES).await
    }

    /// [`open`](Self::open) with a window cache of `budget` bytes.
    pub(super) async fn open_with_budget(
        native: &NativeStoreFile,
        budget: usize,
    ) -> Result<Option<Self>> {
        let Some(reader) = native.component_reader(DICT_COMPONENT_NAME)? else {
            return Ok(None);
        };
        let Some(child) = native.component_layout(DICT_COMPONENT_NAME)? else {
            return Ok(None);
        };
        let Some((leaves, zones)) = term_column(&child) else {
            return Ok(None);
        };
        let source = native.segment_source();
        let zoned = match &zones {
            Some(zones) => zone_bounds(zones, &leaves, &source).await?,
            None => None,
        };
        let bounds = match zoned {
            Some(bounds) => bounds,
            None => leaf_bounds(&leaves, &source).await?,
        };
        if !ordered(&bounds) {
            return Err(VortexRdfError::Deserialization(
                "the dictionary child's windows are not in term order; rebuild the store from \
                 its RDF source"
                    .to_string(),
            ));
        }
        let mut windows = Vec::with_capacity(leaves.len());
        let mut starts = Vec::with_capacity(leaves.len());
        let mut start = 0usize;
        for ((layout, rows), (first, last)) in leaves.into_iter().zip(bounds) {
            starts.push(start);
            windows.push(Window {
                layout,
                start,
                rows,
                first,
                last,
            });
            start += rows;
        }
        Ok(Some(Self(Arc::new(Windows {
            id: next_dictionary_id(),
            reader,
            windows,
            starts,
            code_space: CodeSpace::new(start),
            source,
            kinds: OnceLock::new(),
            cache: Mutex::new(WindowCache::new(budget)),
            #[cfg(test)]
            rebuilt: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            requested: std::sync::atomic::AtomicUsize::new(0),
            #[cfg(test)]
            lifted: std::sync::atomic::AtomicUsize::new(0),
        }))))
    }
}

/// A zone-mapped term column's zone table, with the rows each zone covers.
struct Zones {
    /// The zones child: one row per zone, holding its bounds.
    table: LayoutRef,
    /// Rows per zone, the last zone's excepted.
    zone_len: usize,
}

/// The term column's flat leaves (one per FSST window, in row order, with
/// their row counts) and, when the column is zone-mapped with exact window
/// bounds, its zone table. `None` for any shape a window search cannot
/// address.
fn term_column(dict: &LayoutRef) -> Option<(Leaves, Option<Zones>)> {
    dict.as_opt::<StructLayout>()?;
    let column = (0..dict.nslots()).find_map(|i| {
        matches!(dict.slot_type(i), Some(LayoutChildType::Field(ref n)) if n.as_ref() == COL_DICT_TERM)
            .then(|| dict.slot(i).ok().flatten())
            .flatten()
    })?;
    let zones = match column.as_opt::<Zoned>() {
        Some(zoned) if exact_bounds(&zoned.present_aggregates()) => {
            column.slot(1).ok().flatten().map(|table| Zones {
                table,
                zone_len: zoned.zone_len(),
            })
        }
        _ => None,
    };
    // An empty child has nothing to search; a term count past what a `usize`
    // holds cannot be ranked on this target.
    if !usize::try_from(column.row_count()).is_ok_and(|rows| rows > 0) {
        return None;
    }
    // A dictionary layout's codes leaves hold codes, not terms.
    let leaves = flat_leaves(&column)?
        .into_iter()
        .map(|leaf| {
            let rows = usize::try_from(leaf.layout.row_count()).ok()?;
            leaf.dict_values.is_none().then_some((leaf.layout, rows))
        })
        .collect::<Option<Leaves>>()?;
    Some((leaves, zones))
}

/// Whether a zone map records this crate's exact window bounds.
fn exact_bounds(present: &[String]) -> bool {
    window_bound_aggregates()
        .iter()
        .all(|aggregate| present.contains(&aggregate.to_string()))
}

/// Whether zone `i` of `zones` covers exactly leaf `i`: one zone per leaf,
/// every leaf but the last holding `zone_len` rows and the last at most that.
/// A column written with its windows as zones is cut so; a foreign writer's
/// may have as many zones as leaves and still cut them elsewhere, which would
/// hand a window the bounds of other rows.
fn zones_cut_at_leaves(zones: &Zones, leaves: &[(FlatLayout, usize)]) -> bool {
    let Some(((_, last), rest)) = leaves.split_last() else {
        return false;
    };
    zones.table.row_count() == leaves.len() as u64
        && zones.zone_len > 0
        && rest.iter().all(|(_, rows)| *rows == zones.zone_len)
        && *last <= zones.zone_len
}

/// Each window's bounds from the term column's zone table, one zone per
/// window; `None` when the zones are not cut at the leaves
/// ([`zones_cut_at_leaves`]) or a bound is null.
async fn zone_bounds(
    zones: &Zones,
    leaves: &[(FlatLayout, usize)],
    source: &Arc<dyn SegmentSource>,
) -> Result<Option<Vec<Bounds>>> {
    if !zones_cut_at_leaves(zones, leaves) {
        return Ok(None);
    }
    let windows = leaves.len();
    let reader = zones.table.new_reader(
        "dictionary-zones".into(),
        Arc::clone(source),
        &VORTEX_SESSION,
        &Default::default(),
    )?;
    let table = crate::io::read::scan_all_reader(reader).await?;
    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let table = table.execute::<StructArray>(&mut ctx)?;
    let aggregates = window_bound_aggregates();
    let Some(maxes) = bound_column(&table, &aggregates[0].to_string(), &mut ctx)? else {
        return Ok(None);
    };
    let Some(mins) = bound_column(&table, &aggregates[1].to_string(), &mut ctx)? else {
        return Ok(None);
    };
    let (maxes, mins) = (StrColReader::new(&maxes), StrColReader::new(&mins));
    Ok(Some(
        (0..windows)
            .map(|w| (Box::from(mins.bytes_at(w)), Box::from(maxes.bytes_at(w))))
            .collect(),
    ))
}

/// One bound column of the zone table, `None` when absent or holding a null.
fn bound_column(
    table: &StructArray,
    name: &str,
    ctx: &mut ExecutionCtx,
) -> Result<Option<VarBinViewArray>> {
    let Ok(column) = table.unmasked_field_by_name(name) else {
        return Ok(None);
    };
    if !column.all_valid(ctx)? {
        return Ok(None);
    }
    Ok(Some(column.clone().execute::<VarBinViewArray>(ctx)?))
}

/// Each window's bounds read from its own leaf, for a term column without
/// exact zone maps (chunks of uneven length, or a foreign writer's): every
/// leaf is fetched at open for two single-term reads, and none is cached.
async fn leaf_bounds(
    leaves: &[(FlatLayout, usize)],
    source: &Arc<dyn SegmentSource>,
) -> Result<Vec<Bounds>> {
    let mut ctx = VORTEX_SESSION.create_execution_ctx();
    let mut bounds = Vec::with_capacity(leaves.len());
    for (layout, rows) in leaves {
        let leaf = fetch_flat(layout, source, &VORTEX_SESSION).await?;
        let chunk = TermChunk::from_wire(leaf, &mut ctx)?;
        let mut cursor = chunk.cursor();
        let first = Box::from(cursor.bytes_at(0));
        let last = Box::from(cursor.bytes_at(rows - 1));
        bounds.push((first, last));
    }
    Ok(bounds)
}

/// Whether window bounds are sorted and disjoint (`first <= last` within a
/// window, `last < next first` across), as the window search requires.
fn ordered(bounds: &[Bounds]) -> bool {
    bounds.iter().all(|(first, last)| first <= last)
        && bounds.windows(2).all(|pair| pair[0].1 < pair[1].0)
}

#[cfg(test)]
mod tests {
    use super::super::fixtures::windowed_handle;
    use super::*;
    use crate::store::layouts::dictionary::TermDictionary;
    use crate::store::schema::TermCode;
    use vortex_buffer::ByteBuffer;
    use vortex_file::OpenOptionsSessionExt as _;

    /// Windows that are each sorted but out of term order across the child
    /// are an invalid dictionary: the open fails with a `Deserialization`
    /// error.
    #[tokio::test]
    async fn windows_out_of_term_order_fail_the_open() {
        let terms: Vec<String> = (0..600)
            .map(|i| format!("<http://example.org/term/{i:04}>"))
            .collect();
        let swapped: Vec<&str> = [0usize, 2, 1, 3, 4, 5]
            .iter()
            .flat_map(|w| &terms[w * 100..(w + 1) * 100])
            .map(String::as_str)
            .collect();
        let plain = VarBinViewArray::from_iter_str(swapped);
        let d = TermDictionary::compress_windowed(plain, 100).unwrap();
        let bytes = crate::tests::write_dict_only_store(&d).await;
        let file = VORTEX_SESSION
            .open_options()
            .open_buffer(ByteBuffer::from(bytes))
            .unwrap();
        let native = NativeStoreFile::try_new(file).unwrap();

        let error = FileBackedDict::open(&native)
            .await
            .err()
            .expect("windows out of term order must not open");

        assert!(
            matches!(&error, VortexRdfError::Deserialization(message) if message.contains("term order")),
            "{error}"
        );
    }

    /// The compression windows survive as the child's leaves — one per window,
    /// none merged or re-cut — and every term resolves both ways.
    #[tokio::test]
    async fn windowed_dict_child_chunk_leaves() {
        let (fbd, terms) = windowed_handle(600, 100).await;
        assert_eq!(fbd.debug_window_count(), 6);
        assert_eq!(fbd.len(), 600);
        for (i, term) in terms.iter().enumerate() {
            assert_eq!(
                fbd.encode_exact(term).await.unwrap(),
                Some(i as TermCode),
                "{term}"
            );
        }
        let codes: Vec<TermCode> = (0..600).collect();
        let decoded = fbd.decode_many(&codes).await.unwrap();
        assert!(
            decoded
                .iter()
                .zip(&terms)
                .all(|(got, want)| &**got == want.as_str())
        );
        assert_eq!(fbd.encode_exact("<http://zzz>").await.unwrap(), None);
    }

    /// The dictionary child's term column carries exact window bounds: one
    /// zone per FSST window whose `vortex.min()`/`vortex.max()` are the
    /// window's first and last terms; the component is stamped version 2.
    /// The terms share a 100-byte prefix, so bounds cut anywhere inside it
    /// could not tell the windows apart.
    #[tokio::test]
    async fn dict_child_zone_maps_hold_window_bounds() {
        let prefix = format!("<http://example.org/{}", "p".repeat(80));
        assert_eq!(prefix.len(), 100);
        let terms: Vec<String> = (0..600).map(|i| format!("{prefix}/term/{i:04}>")).collect();
        let plain = VarBinViewArray::from_iter_str(terms.iter().map(String::as_str));
        let d = TermDictionary::compress_windowed(plain, 100).unwrap();
        let bytes = crate::tests::write_dict_only_store(&d).await;
        let (_, components) = crate::io::container::store_metadata_of_bytes(&bytes);
        let descriptor = components
            .iter()
            .find(|c| c.name == DICT_COMPONENT_NAME)
            .unwrap();
        assert_eq!(descriptor.version, 2);
        let native = crate::tests::open_native_bytes(bytes);

        let column = crate::tests::dict_term_column(&native);
        let zoned = column
            .as_opt::<Zoned>()
            .expect("the term column is zone-mapped");
        assert_eq!(zoned.nzones(), 6);
        assert_eq!(zoned.zone_len(), 100);
        // The zone table holds the maximum, then the minimum.
        let names: Vec<String> = window_bound_aggregates()
            .iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(names, ["vortex.max()", "vortex.min()"]);
        for name in ["vortex.max()", "vortex.min()"] {
            assert!(
                zoned.present_aggregates().contains(&name.to_string()),
                "{name}"
            );
        }
        let zones = column.slot(1).unwrap().unwrap();
        let reader = zones
            .new_reader(
                "zones".into(),
                native.segment_source(),
                &VORTEX_SESSION,
                &Default::default(),
            )
            .unwrap();
        let table = crate::io::read::scan_all_reader(reader).await.unwrap();
        let mut ctx = VORTEX_SESSION.create_execution_ctx();
        let table = table.execute::<StructArray>(&mut ctx).unwrap();
        let maxes = table
            .unmasked_field_by_name("vortex.max()")
            .unwrap()
            .clone()
            .execute::<VarBinViewArray>(&mut ctx)
            .unwrap();
        let mins = table
            .unmasked_field_by_name("vortex.min()")
            .unwrap()
            .clone()
            .execute::<VarBinViewArray>(&mut ctx)
            .unwrap();
        for w in 0..6 {
            assert_eq!(StrColReader::new(&mins).str_at(w).unwrap(), terms[w * 100]);
            assert_eq!(
                StrColReader::new(&maxes).str_at(w).unwrap(),
                terms[w * 100 + 99]
            );
        }
    }

    /// Terms far past any string-bound truncation length still resolve through
    /// the zone maps: the table holds each window's first and last term
    /// whole, so every term — a window's first and last included — finds its
    /// own code, and those edge terms are answered from the bounds alone.
    #[tokio::test]
    async fn zone_bounds_of_long_terms_are_exact() {
        // 326-byte terms sharing a 321-byte prefix: they differ only in the
        // last five bytes.
        let terms: Vec<String> = (0..400)
            .map(|i| format!("<http://example.org/{}/{i:04}>", "x".repeat(300)))
            .collect();
        assert!(terms.iter().all(|term| term.len() > 256));
        let plain = VarBinViewArray::from_iter_str(terms.iter().map(String::as_str));
        let d = TermDictionary::compress_windowed(plain, 100).unwrap();
        let native = crate::tests::open_native_bytes(crate::tests::write_dict_only_store(&d).await);
        assert!(crate::tests::dict_term_column(&native).is::<Zoned>());

        // The zone table itself — not the leaves — holds the whole terms.
        let child = native
            .component_layout(DICT_COMPONENT_NAME)
            .unwrap()
            .unwrap();
        let (leaves, zones) = term_column(&child).expect("a zoned, chunked term column");
        let zones = zones.expect("the zone maps record exact window bounds");
        let bounds = zone_bounds(&zones, &leaves, &native.segment_source())
            .await
            .unwrap()
            .expect("one zone per window");
        assert_eq!(bounds.len(), 4);
        for (w, (first, last)) in bounds.iter().enumerate() {
            let (first, last) = (std::str::from_utf8(first), std::str::from_utf8(last));
            assert_eq!(first.unwrap(), terms[w * 100], "window {w}'s first");
            assert_eq!(last.unwrap(), terms[w * 100 + 99], "window {w}'s last");
        }

        let fbd = FileBackedDict::open(&native).await.unwrap().unwrap();
        assert_eq!(fbd.debug_window_count(), 4);
        // Eight edge terms (a window's first and last) read no leaf.
        for i in (0..4).flat_map(|w| [w * 100, w * 100 + 99]) {
            assert_eq!(
                fbd.encode_exact(&terms[i]).await.unwrap(),
                Some(i as TermCode)
            );
        }
        assert_eq!(fbd.debug_windows_rebuilt(), 0);
        // Every term finds its own code; each window is rebuilt once.
        for (i, term) in terms.iter().enumerate() {
            assert_eq!(
                fbd.encode_exact(term).await.unwrap(),
                Some(i as TermCode),
                "{i}"
            );
        }
        assert_eq!(fbd.debug_windows_rebuilt(), 4);
        // A near miss — the last byte changed — is absent, not a neighbour.
        let near = format!("{}?", &terms[150][..terms[150].len() - 1]);
        assert_eq!(fbd.encode_exact(&near).await.unwrap(), None);
        assert_eq!(fbd.lower_bound(near.as_bytes()).await.unwrap(), 151);
    }

    /// A child written without zone maps (chunks of uneven length have no
    /// uniform zone to record) with many windows opens mapped with every
    /// window's bounds read from its own leaf, and answers like the plain
    /// column in both directions.
    #[tokio::test]
    async fn unzoned_child_with_many_windows_reads_every_bound_from_leaves() {
        let terms: Vec<String> = (0..600)
            .map(|i| format!("<http://example.org/term/{i:04}>"))
            .collect();
        let plain = VarBinViewArray::from_iter_str(terms.iter().map(String::as_str));
        let d = TermDictionary::compress_windowed(plain.clone(), 100).unwrap();
        let bytes =
            crate::tests::write_dict_only_store_as(&d, crate::io::container::DICT_VERSION, false)
                .await;
        let (_dir, path) = crate::tests::temp_store_file(&bytes);
        let native = crate::tests::open_mapped(&path).await;
        let (_, components) = crate::io::container::store_metadata_of_bytes(&bytes);
        let descriptor = components
            .iter()
            .find(|c| c.name == DICT_COMPONENT_NAME)
            .unwrap();
        assert_eq!(descriptor.version, crate::io::container::DICT_VERSION);
        assert!(!crate::tests::dict_term_column(&native).is::<Zoned>());

        let fbd = FileBackedDict::open(&native)
            .await
            .unwrap()
            .expect("a chunked child is window-searchable");
        assert_eq!(fbd.debug_window_count(), 6);
        assert_eq!(fbd.len(), 600);
        // Reading the bounds from the leaves asks for no cached window.
        assert!(fbd.debug_cached_windows().is_empty());
        assert_eq!(fbd.debug_cached_bytes(), 0);
        assert_eq!(fbd.debug_windows_requested(), 0);
        // Every window's bounds are its first and last term, read from the leaf.
        for (w, window) in fbd.0.windows.iter().enumerate() {
            assert_eq!((window.start, window.rows), (w * 100, 100), "window {w}");
            let (first, last) = (
                std::str::from_utf8(&window.first),
                std::str::from_utf8(&window.last),
            );
            assert_eq!(first.unwrap(), terms[w * 100], "window {w}'s first");
            assert_eq!(last.unwrap(), terms[w * 100 + 99], "window {w}'s last");
        }

        // Encode and decode parity with the plain column, term by term.
        let column = StrColReader::new(&plain);
        // The edge terms are answered from the bounds read at open.
        for code in (0..6).flat_map(|w| [w * 100, w * 100 + 99]) {
            let term = column.str_at(code).unwrap();
            assert_eq!(
                fbd.encode_exact(term).await.unwrap(),
                Some(code as TermCode)
            );
        }
        assert_eq!(fbd.debug_windows_rebuilt(), 0);
        for code in 0..600 {
            let term = column.str_at(code).unwrap();
            assert_eq!(
                fbd.encode_exact(term).await.unwrap(),
                Some(code as TermCode),
                "{term}"
            );
        }
        // Each window is rebuilt once.
        assert_eq!(fbd.debug_windows_rebuilt(), 6);
        let codes: Vec<TermCode> = (0..600).collect();
        let decoded = fbd.decode_many(&codes).await.unwrap();
        for (code, term) in decoded.iter().enumerate() {
            assert_eq!(&**term, column.str_at(code).unwrap(), "code {code}");
        }
        assert_eq!(fbd.encode_exact("<http://zzz>").await.unwrap(), None);
        assert_eq!(
            fbd.encode_exact("<http://example.org/term/0100>~")
                .await
                .unwrap(),
            None
        );
    }

    /// Zone `i` stands for window `i` only when the zones are cut where the
    /// leaves are: every leaf but the last holds exactly `zone_len` rows, the
    /// last at most that. The same zone count over leaves cut elsewhere (a
    /// foreign writer's) would hand a window the bounds of other rows, so the
    /// bounds are read from the leaves instead, as they are for a table cut
    /// at the leaves that lacks a bound column or holds a null bound.
    #[tokio::test]
    async fn zone_bounds_need_zones_cut_at_the_leaves() {
        let terms: Vec<String> = (0..400)
            .map(|i| format!("<http://example.org/term/{i:04}>"))
            .collect();
        let plain = VarBinViewArray::from_iter_str(terms.iter().map(String::as_str));
        let d = TermDictionary::compress_windowed(plain, 100).unwrap();
        let native = crate::tests::open_native_bytes(crate::tests::write_dict_only_store(&d).await);
        let child = native
            .component_layout(DICT_COMPONENT_NAME)
            .unwrap()
            .unwrap();
        let (leaves, zones) = term_column(&child).expect("a zoned, chunked term column");
        let zones = zones.expect("the zone maps record exact window bounds");
        assert_eq!(zones.zone_len, 100);
        let source = native.segment_source();

        // As written, zone i is leaf i.
        let bounds = zone_bounds(&zones, &leaves, &source)
            .await
            .unwrap()
            .expect("zones cut at the leaves");
        assert_eq!(bounds.len(), 4);

        let recut = |rows: [usize; 4]| -> Leaves {
            leaves
                .iter()
                .zip(rows)
                .map(|((layout, _), rows)| (layout.clone(), rows))
                .collect()
        };
        // A short last leaf is still aligned.
        assert!(
            zone_bounds(&zones, &recut([100, 100, 100, 50]), &source)
                .await
                .unwrap()
                .is_some()
        );
        // Same count, other cuts: no bounds, whichever leaf is off.
        for rows in [
            [50, 150, 100, 100],
            [99, 101, 100, 100],
            [100, 150, 50, 100],
            [100, 100, 99, 101],
            [100, 100, 100, 150],
        ] {
            assert!(
                zone_bounds(&zones, &recut(rows), &source)
                    .await
                    .unwrap()
                    .is_none(),
                "{rows:?}"
            );
        }

        // Tables cut at the leaves, as a foreign writer could leave them:
        // without the maximum, without the minimum, or with a null bound. No
        // bounds come from them, and the leaves give the open the same bounds
        // the zones did.
        use vortex_array::IntoArray as _;
        use vortex_file::WriteOptionsSessionExt as _;
        let names: Vec<String> = window_bound_aggregates()
            .iter()
            .map(ToString::to_string)
            .collect();
        let (max, min) = (names[0].as_str(), names[1].as_str());
        let firsts =
            || VarBinViewArray::from_iter_str((0..4).map(|w| terms[w * 100].as_str())).into_array();
        let lasts = |null: Option<usize>| {
            VarBinViewArray::from_iter_nullable_str(
                (0..4).map(|w| (Some(w) != null).then(|| terms[w * 100 + 99].as_str())),
            )
            .into_array()
        };
        for columns in [
            vec![(min, firsts())],
            vec![(max, lasts(None))],
            vec![(max, lasts(Some(2))), (min, firsts())],
        ] {
            let (fields, arrays): (Vec<&str>, Vec<_>) = columns.into_iter().unzip();
            let table = StructArray::try_new(
                fields.into(),
                arrays,
                4,
                vortex_array::validity::Validity::NonNullable,
            )
            .unwrap()
            .into_array();
            let stream = vortex_array::stream::ArrayStreamAdapter::new(
                table.dtype().clone(),
                futures::stream::iter([Ok(table)]),
            );
            let mut bytes = Vec::new();
            VORTEX_SESSION
                .write_options()
                .write(&mut bytes, stream)
                .await
                .unwrap();
            let file = VORTEX_SESSION
                .open_options()
                .open_buffer(ByteBuffer::from(bytes))
                .unwrap();
            let hand = Zones {
                table: file.footer().layout().clone(),
                zone_len: 100,
            };
            assert!(
                zone_bounds(&hand, &leaves, &file.segment_source())
                    .await
                    .unwrap()
                    .is_none()
            );
        }
        assert_eq!(leaf_bounds(&leaves, &source).await.unwrap(), bounds);
    }
}
