//! The Arrow recipe an engine applies to a view's chunks: vortex's own
//! converter on `row_chunks`, codes as `UInt32`/`Int32` ids, and terms
//! decoded through the dictionary handle into a dictionary-encoded array.

use std::sync::Arc;

use datafusion::arrow::array::{AsArray, DictionaryArray, RecordBatch, StringArray, UInt32Array};
use datafusion::arrow::datatypes::{DataType, Int32Type, UInt32Type};
use futures::StreamExt as _;
use vortex_arrow::ArrowSessionExt as _;
use vortex_array::VortexSessionExecute as _;
use vortex_rdf_core::{IndexType, LayoutStrategy, QuadColumn, vortex_session};
use vortex_rdf_interface_tests::{memory_store, modular_quads, view_codes};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn row_chunks_convert_to_record_batches() {
    let store = memory_store(&modular_quads(2_000, 4, 5, 3), LayoutStrategy::Dictionary, vec![IndexType::SecondaryByCopy]).await;
    let source = store.data_source().await.unwrap();
    let schema = Arc::new(vortex_session().arrow().to_arrow_schema(source.dtype()).unwrap());
    assert!(schema.fields().iter().all(|f| f.data_type() == &DataType::UInt32 && !f.is_nullable()));

    let expected = view_codes(&store).await;
    let mut chunks = store.row_chunks(512).unwrap();
    let mut rows = Vec::new();
    while let Some(chunk) = chunks.next().await {
        let chunk = chunk.unwrap();
        let mut ctx = vortex_session().create_execution_ctx();
        let arrow = vortex_session().arrow().execute_arrow(chunk, None, &mut ctx).unwrap();
        let batch = RecordBatch::from(arrow.as_struct().clone());
        assert_eq!(batch.schema().fields(), schema.fields());
        assert!(batch.num_rows() <= 512);
        let columns: Vec<&UInt32Array> = (0..4).map(|c| batch.column(c).as_primitive::<UInt32Type>()).collect();
        for i in 0..batch.num_rows() {
            rows.push(columns.iter().map(|col| col.value(i)).collect::<Vec<u32>>());
        }
    }
    assert_eq!(rows, expected);
}

/// Codes travel as `UInt32`; an engine with `Int32` object ids reinterprets
/// them when the dictionary fits, and decodes a batch's distinct codes once
/// into a dictionary-encoded term array.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn codes_as_ids_and_decoded_terms() {
    let store = memory_store(&modular_quads(1_000, 4, 5, 3), LayoutStrategy::Dictionary, vec![]).await;
    let dict = store.dict_reader().unwrap();
    assert!(dict.len() <= i32::MAX as usize, "codes fit Int32 ids");

    let mut chunks = store.code_chunks(&[QuadColumn::O], 300).unwrap();
    let mut terms_seen = 0;
    while let Some(chunk) = chunks.next().await {
        let codes = chunk.unwrap().remove(0);
        // Int32 ids: the same bytes, no copy of the values.
        let ids: Vec<i32> = codes.iter().map(|&c| i32::try_from(c).unwrap()).collect();
        assert_eq!(ids.len(), codes.len());

        // Terms: decode the batch's distinct codes once, then key into them.
        let distinct = vortex_rdf_core::columns::distinct_first_seen(codes.as_slice());
        let decoded = dict.decode_many(distinct.as_slice()).await.unwrap();
        let values = StringArray::from_iter(decoded.iter().map(|t| t.as_deref()));
        let keys: Vec<i32> = codes
            .iter()
            .map(|c| distinct.iter().position(|d| d == c).unwrap() as i32)
            .collect();
        let array = DictionaryArray::<Int32Type>::try_new(keys.into(), Arc::new(values)).unwrap();
        assert_eq!(array.len(), codes.len());
        let strings = datafusion::arrow::compute::cast(&array, &DataType::Utf8).unwrap();
        let strings = strings.as_string::<i32>();
        for (i, code) in codes.iter().enumerate() {
            assert_eq!(strings.value(i), dict.decode(*code).await.unwrap().unwrap(), "row {i}");
        }
        terms_seen += codes.len();
    }
    assert_eq!(terms_seen, 1_000);

    // The default graph is code 0 exactly when some quad is in it.
    let kinds = dict.kind_ranges().await.unwrap();
    assert_eq!(kinds.default_graph, Some(0));
    assert_eq!(dict.decode(0).await.unwrap().as_deref(), Some(""));
}
