//! Arrow record-batch export: schema and encodings, buffer sharing,
//! equivalence with the quad streams, projection, and the file-backed
//! chunk pipeline.

use std::sync::Arc;

use super::*;
use crate::arrow::{META_TERM_ENCODING, QuadColumn, TermEncoding};
use arrow_array::cast::AsArray;
use arrow_array::types::UInt32Type;
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, SchemaRef};

/// Collect an export, checking every batch carries the stream's schema.
async fn batches(
    store: &VortexRdfStore,
    encoding: TermEncoding,
    projection: Option<&[QuadColumn]>,
) -> (SchemaRef, Vec<RecordBatch>) {
    let stream = store
        .to_record_batches(encoding, projection)
        .await
        .unwrap();
    let schema = stream.schema();
    let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    for batch in &batches {
        assert_eq!(batch.schema(), schema);
        assert!(batch.num_rows() > 0, "empty batches are skipped");
    }
    (schema, batches)
}

/// A column's cells as strings: codes decoded through `dict`, dictionary
/// keys through their values, strings as they are.
fn column_strings(column: &arrow_array::ArrayRef, dict: Option<&DictSnapshot>) -> Vec<String> {
    match column.data_type() {
        DataType::UInt32 => column
            .as_primitive::<UInt32Type>()
            .values()
            .iter()
            .map(|&code| dict.expect("codes need a dictionary").decode(code).unwrap())
            .collect(),
        DataType::Dictionary(..) => {
            let dictionary = column.as_dictionary::<UInt32Type>();
            let values = dictionary.values().as_string_view();
            dictionary
                .keys()
                .values()
                .iter()
                .map(|&key| values.value(key as usize).to_string())
                .collect()
        }
        DataType::Utf8View => column
            .as_string_view()
            .iter()
            .map(|cell| cell.unwrap().to_string())
            .collect(),
        other => panic!("unexpected column type {other}"),
    }
}

/// Every row of `batches`, as its columns' strings in schema order.
fn batch_rows(batches: &[RecordBatch], dict: Option<&DictSnapshot>) -> Vec<Vec<String>> {
    let mut rows = Vec::new();
    for batch in batches {
        let columns: Vec<Vec<String>> = batch
            .columns()
            .iter()
            .map(|column| column_strings(column, dict))
            .collect();
        for i in 0..batch.num_rows() {
            rows.push(columns.iter().map(|column| column[i].clone()).collect());
        }
    }
    rows
}

fn shared_rows(quads: &[SharedQuad]) -> Vec<Vec<String>> {
    quads
        .iter()
        .map(|q| vec![q.s.to_string(), q.p.to_string(), q.o.to_string(), q.g.to_string()])
        .collect()
}

/// The `u32` cells of every `codes` batch, per column, concatenated.
fn code_columns_of(batches: &[RecordBatch]) -> Vec<Vec<u32>> {
    let width = batches.first().map_or(0, |b| b.num_columns());
    let mut columns = vec![Vec::new(); width];
    for batch in batches {
        for (i, column) in batch.columns().iter().enumerate() {
            columns[i].extend_from_slice(column.as_primitive::<UInt32Type>().values());
        }
    }
    columns
}

async fn store(quads: Vec<Quad>, layout: LayoutStrategy) -> VortexRdfStore {
    VortexRdfStore::from_quads(quad_stream(quads), layout, vec![])
        .await
        .unwrap()
}

/// A full in-memory Dictionary store exports its code columns as the base's
/// own buffers: the same values, at the same addresses.
#[tokio::test]
async fn codes_share_the_base_buffers_of_a_full_dictionary_store() {
    let store = store(modular_quads(50, 5, 7), LayoutStrategy::Dictionary).await;
    let expected = store
        .code_columns()
        .expect("a full in-memory dictionary store serves its codes");
    let (schema, batches) = batches(&store, TermEncoding::Codes, None).await;
    assert_eq!(schema.metadata()[META_TERM_ENCODING], "codes");
    assert_eq!(batches.len(), 1);
    let batch = &batches[0];
    assert_eq!(batch.num_rows(), 50);
    for (i, buffer) in expected.iter().enumerate() {
        let column = batch.column(i).as_primitive::<UInt32Type>();
        assert_eq!(column.values().as_ref(), buffer.as_slice(), "column {i}");
        assert_eq!(
            column.values().as_ptr(),
            buffer.as_slice().as_ptr(),
            "column {i} must share the base buffer"
        );
    }
}

/// On a matched view, `codes` are the gathered code columns, and `terms`
/// are those codes keyed over one values array — the dictionary's own Arrow
/// export, shared by every column — that decodes to the view's quads.
#[tokio::test]
async fn codes_and_terms_agree_with_the_gathered_codes_of_a_view() {
    let store = store(modular_quads(60, 4, 9), LayoutStrategy::Dictionary).await;
    let p1 = NamedNode::new("http://example.org/p1").unwrap();
    let view = store
        .match_pattern(None, Some(&p1), None, None)
        .await
        .unwrap();
    let expected = view.code_columns_gathered().await.unwrap().unwrap();
    let dict = view.code_read_snapshot().unwrap();

    let (_, code_batches) = batches(&view, TermEncoding::Codes, None).await;
    let columns = code_columns_of(&code_batches);
    for (i, buffer) in expected.iter().enumerate() {
        assert_eq!(columns[i], buffer.as_slice(), "codes column {i}");
    }

    let (schema, term_batches) = batches(&view, TermEncoding::Terms, None).await;
    assert_eq!(schema.metadata()[META_TERM_ENCODING], "terms");
    let values = dict.to_arrow().unwrap();
    for batch in &term_batches {
        for (i, column) in batch.columns().iter().enumerate() {
            let dictionary = column.as_dictionary::<UInt32Type>();
            assert_eq!(dictionary.keys().values().as_ref(), expected[i].as_slice());
            assert!(
                Arc::ptr_eq(dictionary.values(), &values),
                "column {i} must share the dictionary's values array"
            );
        }
    }
    assert_eq!(
        batch_rows(&term_batches, None),
        shared_rows(&view.shared_quads_vec().await.unwrap())
    );
    assert_eq!(
        batch_rows(&code_batches, Some(&dict)),
        batch_rows(&term_batches, None)
    );
}

/// `strings` are the shared-quad rows, in the same order, on both string-
/// capable layouts — through appends (a tail) and deletes (tombstones in the
/// base and in the tail). The code encodings reject the tailed view.
#[tokio::test]
async fn strings_match_the_shared_quads_through_a_tail_and_deletes() {
    for layout in [LayoutStrategy::Default, LayoutStrategy::Dictionary] {
        let base_quads = modular_quads(30, 3, 4);
        let base = store(base_quads.clone(), layout).await;
        let appended: Vec<Quad> = (0..5)
            .map(|i| {
                make_quad(
                    &format!("http://example.org/tail{i}"),
                    "http://example.org/p1",
                    &format!("appended {i}"),
                    GraphName::DefaultGraph,
                )
            })
            .collect();
        let tailed = base.add_quads(appended.clone()).await.unwrap();
        let view = tailed
            .delete_quad(&base_quads[7])
            .await
            .unwrap()
            .delete_quad(&appended[2])
            .await
            .unwrap();
        assert_eq!(view.size().await.unwrap(), 33, "{layout}");

        let (schema, strings) = batches(&view, TermEncoding::Strings, None).await;
        assert_eq!(schema.metadata()[META_TERM_ENCODING], "strings");
        assert_eq!(
            batch_rows(&strings, None),
            shared_rows(&view.shared_quads_vec().await.unwrap()),
            "{layout}"
        );

        if layout == LayoutStrategy::Dictionary {
            for encoding in [TermEncoding::Codes, TermEncoding::Terms] {
                assert!(
                    view.to_record_batches(encoding, None).await.is_err(),
                    "{encoding} must reject a tailed view"
                );
            }
        }
    }
}

/// A projection picks columns, in the caller's order, on every encoding;
/// an empty or repeating one is rejected.
#[tokio::test]
async fn projection_picks_columns_in_order() {
    let store = store(dictionary_test_quads(), LayoutStrategy::Dictionary).await;
    let dict = store.code_read_snapshot().unwrap();
    let projection = [QuadColumn::O, QuadColumn::S];
    for encoding in [TermEncoding::Codes, TermEncoding::Terms, TermEncoding::Strings] {
        let (schema, projected) = batches(&store, encoding, Some(&projection)).await;
        assert_eq!(
            schema.fields().iter().map(|f| f.name().as_str()).collect::<Vec<_>>(),
            ["o", "s"],
            "{encoding}"
        );
        let (_, full) = batches(&store, encoding, None).await;
        let full_rows = batch_rows(&full, Some(&dict));
        let expected: Vec<Vec<String>> = full_rows
            .iter()
            .map(|row| vec![row[2].clone(), row[0].clone()])
            .collect();
        assert_eq!(batch_rows(&projected, Some(&dict)), expected, "{encoding}");
    }
    assert!(
        store
            .to_record_batches(TermEncoding::Codes, Some(&[]))
            .await
            .is_err()
    );
    assert!(
        store
            .to_record_batches(TermEncoding::Codes, Some(&[QuadColumn::S, QuadColumn::S]))
            .await
            .is_err()
    );
}

/// The TypedObject layout has no Arrow export, and the code encodings need
/// the Dictionary layout.
#[tokio::test]
async fn unsupported_layouts_and_encodings_are_rejected() {
    let typed = store(modular_quads(5, 2, 2), LayoutStrategy::TypedObject).await;
    for encoding in [TermEncoding::Codes, TermEncoding::Terms, TermEncoding::Strings] {
        assert!(typed.to_record_batches(encoding, None).await.is_err());
    }
    let default = store(modular_quads(5, 2, 2), LayoutStrategy::Default).await;
    for encoding in [TermEncoding::Codes, TermEncoding::Terms] {
        assert!(default.to_record_batches(encoding, None).await.is_err());
    }
    let (_, strings) = batches(&default, TermEncoding::Strings, None).await;
    assert_eq!(batch_rows(&strings, None).len(), 5);
}

/// A file-backed store exports through the scan — codes, terms and strings
/// agree with the in-memory readers on the whole file and on a matched view,
/// and a projection reads only its columns off the file.
#[cfg(feature = "file-io")]
#[tokio::test]
async fn file_backed_export_agrees_with_the_readers() {
    let quads = modular_quads(3_000, 7, 11);
    let (_dir, path) = write_store_file(quads, LayoutStrategy::Dictionary, vec![]).await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let p3 = NamedNode::new("http://example.org/p3").unwrap();
    let view = store
        .match_pattern(None, Some(&p3), None, None)
        .await
        .unwrap();

    for (tag, target) in [("file", &store), ("view", &view)] {
        let expected = target.code_columns_gathered().await.unwrap().unwrap();
        let (_, codes) = batches(target, TermEncoding::Codes, None).await;
        let columns = code_columns_of(&codes);
        for (i, buffer) in expected.iter().enumerate() {
            assert_eq!(columns[i], buffer.as_slice(), "{tag}: codes column {i}");
        }

        let shared = shared_rows(&target.shared_quads_vec().await.unwrap());
        let (_, terms) = batches(target, TermEncoding::Terms, None).await;
        assert_eq!(batch_rows(&terms, None), shared, "{tag}: terms");
        let mut values = None;
        for batch in &terms {
            for column in batch.columns() {
                let shared_values = column.as_dictionary::<UInt32Type>().values();
                match &values {
                    None => values = Some(shared_values.clone()),
                    Some(first) => assert!(Arc::ptr_eq(first, shared_values), "{tag}: one values array"),
                }
            }
        }

        let (_, strings) = batches(target, TermEncoding::Strings, None).await;
        assert_eq!(batch_rows(&strings, None), shared, "{tag}: strings");

        let projection = [QuadColumn::P, QuadColumn::G];
        let (schema, projected) = batches(target, TermEncoding::Codes, Some(&projection)).await;
        assert_eq!(
            schema.fields().iter().map(|f| f.name().as_str()).collect::<Vec<_>>(),
            ["p", "g"]
        );
        let columns = code_columns_of(&projected);
        assert_eq!(columns[0], expected[1].as_slice(), "{tag}: projected p");
        assert_eq!(columns[1], expected[3].as_slice(), "{tag}: projected g");
    }
}
