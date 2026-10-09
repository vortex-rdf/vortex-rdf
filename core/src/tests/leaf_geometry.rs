//! The leaf geometry of the columns a store writes: how many rows each flat
//! leaf of a column holds. Vortex coalesces a column to 1 MiB uncompressed
//! per leaf, so widening the term codes from u32 to u64 would halve the rows
//! per leaf of every code column; `child_strategy` writes the code columns
//! at 2 MiB so each keeps the rows per leaf it had as u32 (262,144 for a
//! plain column), and leaves every other column on the stock strategy.

use std::sync::Arc;

use super::*;
use crate::io::container::{self, ONE_MEG, child_strategy, child_strategy_with};
use vortex_array::IntoArray as _;
use vortex_array::arrays::StructArray;
use vortex_array::dtype::FieldNames;
use vortex_array::validity::Validity;
use vortex_buffer::{Buffer, ByteBuffer};
use vortex_file::OpenOptionsSessionExt as _;
use vortex_layout::LayoutRef;
use vortex_layout::layouts::flat::Flat;

/// Rows of the synthetic columns: three plain u64 leaves' worth, at 262,144
/// rows each, so every geometry shows more than one leaf.
const ROWS: u64 = 600_000;

/// The row counts of a column's flat leaves, in row order. A zone-mapped
/// column is read through its data child; a dictionary-encoded one through
/// its codes, whose leaves are the ones its rows are cut into.
fn leaf_rows(node: &LayoutRef) -> Vec<u64> {
    if node.is::<Flat>() {
        return vec![node.row_count()];
    }
    let names: Vec<Arc<str>> = node.child_names().collect();
    let children = node.children().unwrap();
    let pick = |wanted: &str| names.iter().position(|name| name.as_ref() == wanted);
    if let Some(data) = pick("data") {
        return leaf_rows(&children[data]);
    }
    if let Some(codes) = pick("codes") {
        return leaf_rows(&children[codes]);
    }
    children.iter().flat_map(leaf_rows).collect()
}

/// Each column of `child`, by name, with its leaves' row counts.
fn column_leaves(child: &LayoutRef) -> Vec<(String, Vec<u64>)> {
    child
        .child_names()
        .zip(child.children().unwrap())
        .map(|(name, column)| (name.to_string(), leaf_rows(&column)))
        .collect()
}

/// The leaves of every column of every child of the store file `bytes`,
/// keyed `child/column`.
fn store_leaves(bytes: Vec<u8>) -> Vec<(String, Vec<u64>)> {
    let file = crate::session::VORTEX_SESSION
        .open_options()
        .open_buffer(ByteBuffer::from(bytes))
        .unwrap();
    let root = file.footer().layout().clone();
    root.child_names()
        .zip(root.children().unwrap())
        .filter(|(name, _)| name.as_ref() != container::DICT_COMPONENT_NAME)
        .flat_map(|(name, child)| {
            column_leaves(&child)
                .into_iter()
                .map(move |(column, leaves)| (format!("{name}/{column}"), leaves))
        })
        .collect()
}

/// A quad-table-shaped struct of `ROWS` rows: `s` unique and ascending (a
/// plain column), `p` of 1,000 values spread over the whole u32 range (a
/// column that dictionary-encodes, with u16 codes) and
/// `o` from a scrambled range (plain), as u32 codes when `narrow` (the width
/// before the widening) or u64 ones, beside a u32 `rid`.
fn table(narrow: bool) -> vortex_array::ArrayRef {
    let column = |value: &dyn Fn(u64) -> u64| -> vortex_array::ArrayRef {
        if narrow {
            (0..ROWS)
                .map(|i| value(i) as u32)
                .collect::<Buffer<u32>>()
                .into_array()
        } else {
            (0..ROWS).map(value).collect::<Buffer<u64>>().into_array()
        }
    };
    let rid: Buffer<u32> = (0..ROWS).map(|i| i as u32).collect();
    StructArray::try_new(
        FieldNames::from(["s", "p", "o", "rid"]),
        vec![
            column(&|i| i),
            column(&|i| (i % 1_000) * 4_000_000),
            column(&|i| (i * 7_919) % 1_000_003),
            rid.into_array(),
        ],
        ROWS as usize,
        Validity::NonNullable,
    )
    .unwrap()
    .into_array()
}

/// `rows` written as a store's quad child through `strategy`.
async fn written(
    rows: vortex_array::ArrayRef,
    strategy: Arc<dyn vortex_layout::LayoutStrategy>,
) -> Vec<u8> {
    let dtype = rows.dtype().clone();
    let mut bytes = Vec::new();
    container::write_store(
        &crate::session::VORTEX_SESSION,
        &mut bytes,
        vortex_array::stream::ArrayStreamAdapter::new(dtype, futures::stream::iter([Ok(rows)])),
        strategy,
        false,
        Vec::new(),
    )
    .await
    .unwrap();
    bytes
}

/// The u64 code columns `child_strategy` writes have the leaves the same
/// columns had as u32 under the stock strategy — the geometry before the
/// widening: 262,144 rows per plain leaf, 524,288 per leaf of a
/// dictionary's u16 codes. The stock strategy alone would cut the u64 plain
/// columns in half. The u32 `rid` keeps the stock geometry either way.
#[tokio::test]
async fn code_columns_keep_their_u32_rows_per_leaf() {
    let narrow = table(true);
    let wide = table(false);
    let before = store_leaves(written(narrow, container::default_child_strategy()).await);
    let after = store_leaves(written(wide.clone(), child_strategy(wide.dtype())).await);
    assert_eq!(after, before, "every column keeps its u32-era leaves");

    let plain = vec![262_144, 262_144, ROWS - 2 * 262_144];
    let codes = vec![524_288, ROWS - 524_288];
    assert_eq!(
        after,
        vec![
            ("quad-source/s".to_string(), plain.clone()),
            ("quad-source/p".to_string(), codes.clone()),
            ("quad-source/o".to_string(), plain.clone()),
            ("quad-source/rid".to_string(), plain.clone()),
        ]
    );

    // The stock strategy alone halves the rows of a plain u64 leaf; the
    // dictionary codes and the u32 column keep theirs.
    let stock = store_leaves(written(wide, container::default_child_strategy()).await);
    let halved = vec![131_072, 131_072, 131_072, 131_072, ROWS - 4 * 131_072];
    assert_eq!(
        stock,
        vec![
            ("quad-source/s".to_string(), halved.clone()),
            ("quad-source/p".to_string(), codes),
            ("quad-source/o".to_string(), halved),
            ("quad-source/rid".to_string(), plain),
        ]
    );
}

/// At 1 MiB for both targets the code-column override is the stock
/// pipeline, byte for byte, over a plain and a dictionary-encoded column —
/// so an upgrade that changes Vortex's stock pipeline fails here rather than
/// leaving the code columns on a stale copy of it.
#[tokio::test]
async fn code_column_override_at_one_mebibyte_is_the_stock_pipeline() {
    let wide = table(false);
    let stock = written(wide.clone(), container::default_child_strategy()).await;
    let overridden = written(
        wide.clone(),
        child_strategy_with(wide.dtype(), ONE_MEG, ONE_MEG),
    )
    .await;
    assert_eq!(overridden.len(), stock.len());
    assert!(overridden == stock, "the override writes the stock bytes");
}

/// Only the term-code fields take the override: string columns of the same
/// names and the u32 row ids are written by the stock strategy.
#[tokio::test]
async fn only_code_fields_take_the_override() {
    use crate::store::schema::is_code_field;
    use vortex_array::dtype::{DType, Nullability, PType};

    let u64_dtype = DType::Primitive(PType::U64, Nullability::NonNullable);
    let u32_dtype = DType::Primitive(PType::U32, Nullability::NonNullable);
    let utf8 = DType::Utf8(Nullability::NonNullable);
    for name in ["s", "p", "o", "g", "val"] {
        assert!(is_code_field(name, &u64_dtype), "{name}");
        assert!(!is_code_field(name, &u32_dtype), "{name}");
        assert!(!is_code_field(name, &utf8), "{name}");
    }
    for name in ["rid", "o_kind", "o_value", "_dict_term"] {
        assert!(!is_code_field(name, &u64_dtype), "{name}");
    }
    assert!(!is_code_field(
        "s",
        &DType::Primitive(PType::U64, Nullability::Nullable)
    ));

    // A Default-layout table: strings under the code names, written alike
    // by `child_strategy` and the stock strategy.
    let strings: Vec<String> = (0..20_000)
        .map(|i| format!("<http://example.org/{i}>"))
        .collect();
    let column = || {
        vortex_array::arrays::VarBinViewArray::from_iter_str(strings.iter().map(String::as_str))
            .into_array()
    };
    let rows = StructArray::try_new(
        FieldNames::from(["s", "p", "o", "g"]),
        vec![column(), column(), column(), column()],
        strings.len(),
        Validity::NonNullable,
    )
    .unwrap()
    .into_array();
    let stock = written(rows.clone(), container::default_child_strategy()).await;
    let chosen = written(rows.clone(), child_strategy(rows.dtype())).await;
    assert!(chosen == stock, "a string table takes no override");
}

/// The three paths that write a store's children — the out-of-core
/// builder's quad table and merged index children, and an in-memory store's
/// serialization of its quad table and index components — all give the code
/// columns their u32-era leaves: 262,144 rows per plain leaf. Subjects and
/// objects are unique, so every `s` and `o` column, and the reference
/// index's object values, are plain columns.
#[cfg(feature = "file-io")]
#[tokio::test]
async fn every_store_write_path_keeps_code_leaves_at_262144_rows() {
    let rows = 263_000usize;
    let quads: Vec<Quad> = (0..rows)
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{i:06}"),
                &format!("http://example.org/p{}", i % 3),
                &format!("object {i:06}"),
                GraphName::DefaultGraph,
            )
        })
        .collect();
    let indexes = vec![IndexType::SecondaryByCopy, IndexType::SecondaryByReference];
    let leaves_of = |leaves: &[(String, Vec<u64>)], key: &str| {
        leaves
            .iter()
            .find(|(name, _)| name == key)
            .unwrap_or_else(|| panic!("{key} in {leaves:?}"))
            .1
            .clone()
    };

    // The out-of-core builder, straight to a file.
    let (_dir, path) = write_store_file(quads, LayoutStrategy::Dictionary, indexes).await;
    let streamed = store_leaves(std::fs::read(&path).unwrap());
    // An in-memory store's serialization: its quad table and components.
    let store = VortexRdfStore::from_file_in_memory(&path).await.unwrap();
    let serialized = store_leaves(store.to_bytes().await.unwrap());

    let plain = vec![262_144, rows as u64 - 262_144];
    for (label, leaves) in [("streamed", &streamed), ("serialized", &serialized)] {
        for key in [
            "quad-source/s",
            "quad-source/o",
            "index:posg/s",
            "index:posg/o",
            "index:ospg/s",
            "index:ospg/o",
            "index:ref-o/val",
            // The u32 row ids keep the stock 1 MiB leaves: as many rows.
            "index:ref-o/rid",
            "index:posg/rid",
        ] {
            assert_eq!(leaves_of(leaves, key), plain, "{label}: {key}");
        }
    }
}
