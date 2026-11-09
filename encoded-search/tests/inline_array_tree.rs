//! Layout-feature test, in a binary of its own: Vortex's flat writer reads
//! its `FLAT_LAYOUT_INLINE_ARRAY_NODE` switch once per process, and with it
//! set writes each leaf's array tree inline in the layout instead of in the
//! leaf's segment. Such leaves fetch and probe exactly as the default ones
//! do.

mod common;

use common::{canonical_bounds, data_node, struct_chunk, taken_u32, write_file, writer_session};
use vortex_array::ArrayRef;
use vortex_rdf_encoded_search::{ColumnChunks, fetch_flat, flat_leaves};

/// The column of `layout.rs`' `column_chunks_match_canonical`, written with
/// the switch on: every leaf carries its tree, and the column's bounds, point
/// reads and leaf fetches answer what the default file's do.
#[test]
fn leaves_with_an_inline_array_tree_probe_like_any_other() {
    // SAFETY: the first statement of this binary's only test, before any
    // other thread reads the environment; the writer reads the switch at
    // its first flat leaf.
    unsafe { std::env::set_var("FLAT_LAYOUT_INLINE_ARRAY_NODE", "1") };
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let session = writer_session();
            let data: Vec<u32> = (0..600_000).map(|i| (i / 11) as u32).collect();
            let chunks: Vec<ArrayRef> = data
                .chunks(200_000)
                .map(|c| struct_chunk(&[("s", c)]))
                .collect();
            let file = write_file(&session, chunks, None).await;
            let root = file.footer().layout().clone();
            let leaves = flat_leaves(&data_node(&root, "s")).expect("the shape resolves");
            assert!(leaves.len() >= 2, "several leaves: {}", leaves.len());
            for leaf in &leaves {
                assert!(
                    leaf.layout.array_tree().is_some(),
                    "the switch inlines the tree of the leaf at row {}",
                    leaf.row_offset
                );
            }

            let source = file.segment_source();
            for leaf in &leaves {
                let rows = fetch_flat(&leaf.layout, &source, &session).await.unwrap();
                let start = leaf.row_offset as usize;
                let end = start + leaf.layout.row_count() as usize;
                assert_eq!(taken_u32(rows, &session), &data[start..end]);
            }
            let column = ColumnChunks::from_struct_layout(&root, "s").expect("the shape resolves");
            let max = u64::from(*data.last().unwrap());
            for needle in [0, 1, 18_181, 18_182, 36_363, max, max + 1, u64::MAX] {
                assert_eq!(
                    column.bounds(needle, &source, &session).await.unwrap(),
                    Some(canonical_bounds(&data, needle)),
                    "needle {needle}"
                );
            }
            for row in [0u64, 199_999, 200_000, 399_999, 400_000, 599_999] {
                assert_eq!(
                    column.value_at(row, &source, &session).await.unwrap(),
                    Some(u64::from(data[row as usize])),
                    "row {row}"
                );
            }
        });
}
