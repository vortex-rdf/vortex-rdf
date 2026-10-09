//! The leaf geometry of the columns a store writes: how many rows each flat
//! leaf of a column holds. Vortex coalesces a column to 1 MiB uncompressed
//! per leaf, so widening the term codes from u32 to u64 would halve the rows
//! per leaf of every plain code column; `child_strategy` writes the code
//! columns through the stock pipeline at 2 MiB instead — a plain one keeps
//! the 262,144 rows per leaf it had as u32, a dictionary-encoded one
//! coalesces 2 MiB of its narrow codes — and leaves every other column at
//! the stock 1 MiB.

use std::sync::Arc;

use super::*;
use crate::io::container::{self, CODE_BLOCK_TARGET, ONE_MEG, child_strategy, code_fields};
use vortex_array::IntoArray as _;
use vortex_array::arrays::StructArray;
use vortex_array::dtype::FieldNames;
use vortex_array::validity::Validity;
use vortex_buffer::{Buffer, ByteBuffer};
use vortex_file::OpenOptionsSessionExt as _;
use vortex_layout::LayoutRef;
use vortex_layout::layouts::flat::Flat;

/// Rows of the synthetic table: past one leaf of every column's geometry,
/// the widest being 2 MiB of a dictionary's u16 codes — 1,048,576 rows.
const ROWS: u64 = 1_100_000;

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

/// A quad-table-shaped struct of `ROWS` rows: `s` unique and ascending (a
/// plain u64 column), `p` of 1,000 values spread over the whole u32 range (a
/// u64 column that dictionary-encodes, with u16 codes), beside a u32 `rid`.
fn table() -> vortex_array::ArrayRef {
    let s: Buffer<u64> = (0..ROWS).collect();
    let p: Buffer<u64> = (0..ROWS).map(|i| (i % 1_000) * 4_000_000).collect();
    let rid: Buffer<u32> = (0..ROWS).map(|i| i as u32).collect();
    StructArray::try_new(
        FieldNames::from(["s", "p", "rid"]),
        vec![s.into_array(), p.into_array(), rid.into_array()],
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

/// `child_strategy` coalesces the code columns to 2 MiB: 262,144 rows per
/// leaf of a plain u64 column (the stock 1 MiB would cut 131,072), 1,048,576
/// per leaf of a dictionary's u16 codes (stock: 524,288). The u32 `rid`
/// keeps the stock 1 MiB: 262,144 rows (at 2 MiB it would hold 524,288).
#[tokio::test]
async fn code_columns_coalesce_at_two_mebibytes() {
    assert_eq!(CODE_BLOCK_TARGET, 2 * ONE_MEG);
    let rows = table();
    let leaves = store_leaves(written(rows.clone(), child_strategy(rows.dtype())).await);
    assert_eq!(
        leaves,
        vec![
            ("quad-source/s".to_string(), cut(ROWS, 262_144)),
            ("quad-source/p".to_string(), cut(ROWS, 1_048_576)),
            ("quad-source/rid".to_string(), cut(ROWS, 262_144)),
        ]
    );
}

/// Only the term-code fields take the override: non-nullable u64 columns
/// named `s`, `p`, `o`, `g` or `val` — not string columns of the same names,
/// not the u32 row ids — so a Default-layout table's strings are written by
/// the stock strategy.
#[test]
fn only_code_fields_take_the_override() {
    use crate::store::schema::is_code_field;
    use vortex_array::dtype::{DType, Nullability, PType, StructFields};

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

    let names = |dtype: &DType| -> Vec<String> {
        code_fields(dtype).iter().map(|n| n.to_string()).collect()
    };
    let table = |fields: &[(&str, &DType)]| {
        DType::Struct(
            StructFields::new(
                FieldNames::from_iter(fields.iter().map(|(name, _)| *name)),
                fields.iter().map(|(_, dtype)| (*dtype).clone()).collect(),
            ),
            Nullability::NonNullable,
        )
    };
    // A Dictionary-layout index child: its codes, not its row ids.
    assert_eq!(
        names(&table(&[
            ("p", &u64_dtype),
            ("o", &u64_dtype),
            ("rid", &u32_dtype),
            ("val", &u64_dtype),
        ])),
        ["p", "o", "val"]
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
/// code columns through the 2 MiB override: 140,000 rows fit one leaf of a
/// plain u64 column there, where the stock 1 MiB would cut two (131,072 and
/// 8,928). Subjects are unique and each object is its quad's subject, so
/// every `s` and `o` column, and the reference index's object values, are
/// plain columns.
#[cfg(feature = "file-io")]
#[tokio::test]
async fn every_store_write_path_routes_code_columns_to_two_mebibytes() {
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
        ] {
            assert_eq!(leaves_of(leaves, key), [rows as u64], "{label}: {key}");
        }
    }
}
