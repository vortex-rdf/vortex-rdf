//! The leaf geometry of the columns a store writes: how many rows each flat
//! leaf of a column holds. Vortex coalesces a column to 1 MiB uncompressed
//! per leaf, so widening the term codes and the row ids from u32 to u64
//! would halve the rows per leaf of every plain id column; `child_strategy`
//! writes the id columns at 2 MiB so each keeps the rows per leaf it had as
//! u32 (262,144 for a plain column), keeps a dictionary's codes at the stock
//! 1 MiB, and leaves every other column on the stock strategy.

use std::sync::Arc;

use super::*;
use crate::io::container::{self, ONE_MEG, child_strategy, child_strategy_with, id_fields};
use vortex_array::IntoArray as _;
use vortex_array::arrays::StructArray;
use vortex_array::dtype::FieldNames;
use vortex_array::validity::Validity;
use vortex_buffer::{Buffer, ByteBuffer};
use vortex_file::OpenOptionsSessionExt as _;
use vortex_layout::LayoutRef;
use vortex_layout::layouts::flat::Flat;

/// `rows` cut into leaves of `per_leaf`: the full leaves, then the rest.
fn cut(rows: u64, per_leaf: u64) -> Vec<u64> {
    let mut leaves = vec![per_leaf; (rows / per_leaf) as usize];
    if !rows.is_multiple_of(per_leaf) {
        leaves.push(rows % per_leaf);
    }
    leaves
}

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

/// A quad-table-shaped struct of `rows` rows: `s` unique and ascending (a
/// plain u64 column), `p` of 1,000 values spread over the whole u32 range (a
/// u64 column that dictionary-encodes, with u16 codes), beside a u64 `rid`
/// (unique, so plain).
fn table(rows: u64) -> vortex_array::ArrayRef {
    let s: Buffer<u64> = (0..rows).collect();
    let p: Buffer<u64> = (0..rows).map(|i| (i % 1_000) * 4_000_000).collect();
    let rid: Buffer<u64> = (0..rows).rev().collect();
    StructArray::try_new(
        FieldNames::from(["s", "p", "rid"]),
        vec![s.into_array(), p.into_array(), rid.into_array()],
        rows as usize,
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

/// The u64 code and row-id columns `child_strategy` writes have the leaves
/// the same columns had as u32 under the stock strategy — the geometry
/// before the widening: 262,144 rows per plain leaf (the stock 1 MiB would
/// cut a u64 one at 131,072), 524,288 per leaf of a dictionary's u16 codes,
/// which stay at 1 MiB. 600,000 rows reach past one leaf of each.
#[tokio::test]
async fn id_columns_keep_their_u32_rows_per_leaf() {
    let rows = 600_000;
    let wide = table(rows);
    let leaves = store_leaves(written(wide.clone(), child_strategy(wide.dtype())).await);
    assert_eq!(
        leaves,
        vec![
            ("quad-source/s".to_string(), cut(rows, 262_144)),
            ("quad-source/p".to_string(), cut(rows, 524_288)),
            ("quad-source/rid".to_string(), cut(rows, 262_144)),
        ]
    );
}

/// At 1 MiB for both targets the id-column override is the stock pipeline,
/// byte for byte, over plain and dictionary-encoded columns — so an upgrade
/// that changes Vortex's stock pipeline fails here rather than leaving the id
/// columns on a stale copy of it. 600,000 rows cut the plain columns into
/// five 1 MiB leaves and the dictionary-encoded column's u16 codes into two,
/// so the coalescing of both the plain data and the dictionary codes is
/// compared across a leaf boundary.
#[tokio::test]
async fn id_column_override_at_one_mebibyte_is_the_stock_pipeline() {
    let rows = 600_000;
    let wide = table(rows);
    let stock = written(wide.clone(), container::default_child_strategy()).await;
    let overridden = written(
        wide.clone(),
        child_strategy_with(wide.dtype(), ONE_MEG, ONE_MEG),
    )
    .await;
    assert_eq!(overridden.len(), stock.len());
    assert!(overridden == stock, "the override writes the stock bytes");
    assert_eq!(
        store_leaves(stock),
        vec![
            ("quad-source/s".to_string(), cut(rows, 131_072)),
            ("quad-source/p".to_string(), cut(rows, 524_288)),
            ("quad-source/rid".to_string(), cut(rows, 131_072)),
        ],
        "the comparison crosses a leaf of each column"
    );
}

/// Only the id fields take the override: non-nullable u64 columns named
/// `s`, `p`, `o`, `g` or `val` (term codes) or `rid` (row ids) — not string
/// columns of the same names, not narrower or nullable integers, not other
/// columns — so a Default-layout table's strings are written by the stock
/// strategy, and its index children's row ids by the override.
#[test]
fn only_id_fields_take_the_override() {
    use crate::store::schema::{is_code_field, is_id_field, is_row_id_field};
    use vortex_array::dtype::{DType, Nullability, PType, StructFields};

    let u64_dtype = DType::Primitive(PType::U64, Nullability::NonNullable);
    let u32_dtype = DType::Primitive(PType::U32, Nullability::NonNullable);
    let utf8 = DType::Utf8(Nullability::NonNullable);
    for name in ["s", "p", "o", "g", "val"] {
        assert!(is_code_field(name, &u64_dtype), "{name}");
        assert!(!is_code_field(name, &u32_dtype), "{name}");
        assert!(!is_code_field(name, &utf8), "{name}");
        assert!(!is_row_id_field(name, &u64_dtype), "{name}");
    }
    assert!(is_row_id_field("rid", &u64_dtype));
    assert!(!is_row_id_field("rid", &u32_dtype));
    assert!(!is_code_field("rid", &u64_dtype));
    for name in ["o_kind", "o_value", "_dict_term"] {
        assert!(!is_id_field(name, &u64_dtype), "{name}");
    }
    for name in ["s", "rid"] {
        assert!(is_id_field(name, &u64_dtype), "{name}");
        assert!(!is_id_field(
            name,
            &DType::Primitive(PType::U64, Nullability::Nullable)
        ));
    }

    let names =
        |dtype: &DType| -> Vec<String> { id_fields(dtype).iter().map(|n| n.to_string()).collect() };
    let table = |fields: &[(&str, &DType)]| {
        DType::Struct(
            StructFields::new(
                FieldNames::from_iter(fields.iter().map(|(name, _)| *name)),
                fields.iter().map(|(_, dtype)| (*dtype).clone()).collect(),
            ),
            Nullability::NonNullable,
        )
    };
    // A Dictionary-layout index child: its codes and its row ids.
    assert_eq!(
        names(&table(&[
            ("p", &u64_dtype),
            ("o", &u64_dtype),
            ("rid", &u64_dtype),
            ("val", &u64_dtype),
        ])),
        ["p", "o", "rid", "val"]
    );
    // A Default-layout index child: its row ids only.
    assert_eq!(
        names(&table(&[("val", &utf8), ("rid", &u64_dtype)])),
        ["rid"]
    );
    // A Default-layout quad table: strings under the code names.
    assert!(
        names(&table(&[
            ("s", &utf8),
            ("p", &utf8),
            ("o", &utf8),
            ("g", &utf8)
        ]))
        .is_empty()
    );
    assert!(names(&u64_dtype).is_empty(), "a bare column is no table");
}

/// The three paths that write a store's children — the out-of-core
/// builder's quad table and merged index children, and an in-memory store's
/// serialization of its quad table and index components — all route the
/// plain id columns through the override: 140,000 rows fit one leaf of a
/// plain u64 column there, where the stock 1 MiB would cut two (131,072 and
/// 8,928). Subjects are unique and each object is its quad's subject, so
/// every `s` and `o` column, the reference index's object values and every
/// row-id column are plain columns.
#[cfg(feature = "file-io")]
#[tokio::test]
async fn every_store_write_path_takes_the_id_column_override() {
    let rows = 140_000usize;
    let quads: Vec<Quad> = (0..rows)
        .map(|i| {
            let subject = format!("http://example.org/s{i:06}");
            Quad::new(
                NamedNode::new(&subject).unwrap(),
                NamedNode::new(format!("http://example.org/p{}", i % 3)).unwrap(),
                NamedNode::new(&subject).unwrap(),
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

    for (label, leaves) in [("streamed", &streamed), ("serialized", &serialized)] {
        for key in [
            "quad-source/s",
            "quad-source/o",
            "index:posg/s",
            "index:posg/o",
            "index:ospg/s",
            "index:ospg/o",
            "index:ref-o/val",
            "index:posg/rid",
            "index:ospg/rid",
            "index:ref-o/rid",
            "index:ref-p/rid",
        ] {
            assert_eq!(leaves_of(leaves, key), [rows as u64], "{label}: {key}");
        }
    }
}
