//! Arrow interop (`arrow` feature): [`ArrowBuilder`]'s column extraction —
//! the encodings a real Arrow producer (pandas, pyarrow, a Rust
//! `StringBuilder`, …) might hand in for the `s`/`p`/`o`/`g` columns, and
//! the error paths (a missing column, a null term, a non-Dictionary
//! layout).

use super::*;
use crate::store::ArrowBuilder;
use arrow_array::types::Int32Type;
use arrow_array::{
    ArrayRef, DictionaryArray, LargeStringArray, RecordBatch, StringArray, StringViewArray,
};
use arrow_schema::{Field, Schema};
use std::sync::Arc;

/// `quads`' four term columns as the raw strings [`RawQuad::from_quad`]
/// produces — the same spelling the store's own trusted decode path uses,
/// so a round-tripped store's [`view_strings`] can be compared directly
/// against [`quad_strings`]`(quads)`.
fn raw_columns(quads: &[Quad]) -> [Vec<Box<str>>; 4] {
    let raws: Vec<RawQuad> = quads.iter().map(RawQuad::from_quad).collect();
    [
        raws.iter().map(|r| r.s.as_str().into()).collect(),
        raws.iter().map(|r| r.p.as_str().into()).collect(),
        raws.iter().map(|r| r.o.as_str().into()).collect(),
        raws.iter().map(|r| r.g.as_str().into()).collect(),
    ]
}

fn utf8_col(values: &[Box<str>]) -> ArrayRef {
    Arc::new(StringArray::from_iter_values(values.iter().map(AsRef::as_ref)))
}

fn large_utf8_col(values: &[Box<str>]) -> ArrayRef {
    Arc::new(LargeStringArray::from_iter_values(
        values.iter().map(AsRef::as_ref),
    ))
}

fn utf8_view_col(values: &[Box<str>]) -> ArrayRef {
    Arc::new(StringViewArray::from_iter_values(
        values.iter().map(AsRef::as_ref),
    ))
}

fn dictionary_col(values: &[Box<str>]) -> ArrayRef {
    let arr: DictionaryArray<Int32Type> =
        values.iter().map(|v| Some(v.as_ref())).collect();
    Arc::new(arr)
}

/// A `RecordBatch` from four named, already-built columns, nullable so a
/// caller can put a null in without a schema mismatch.
fn batch_from(columns: [(&str, ArrayRef); 4]) -> RecordBatch {
    let fields: Vec<Field> = columns
        .iter()
        .map(|(name, array)| Field::new(*name, array.data_type().clone(), true))
        .collect();
    let arrays: Vec<ArrayRef> = columns.into_iter().map(|(_, array)| array).collect();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
}

/// A well-formed `s`/`p`/`o`/`g` batch of `quads`, one column per encoding
/// in `[s, p, o, g]` order.
fn quad_batch(quads: &[Quad], encodings: [fn(&[Box<str>]) -> ArrayRef; 4]) -> RecordBatch {
    let [s, p, o, g] = raw_columns(quads);
    batch_from([
        ("s", encodings[0](&s)),
        ("p", encodings[1](&p)),
        ("o", encodings[2](&o)),
        ("g", encodings[3](&g)),
    ])
}

/// A dataset exercising repeated terms (so the Dictionary layout actually
/// interns) across the default graph and a named graph.
fn round_trip_quads() -> Vec<Quad> {
    graph_modular_quads(
        12,
        2,
        3,
        4,
        &[
            GraphName::DefaultGraph,
            GraphName::NamedNode(NamedNode::new("http://example.org/g").unwrap()),
        ],
    )
}

/// `batch` converts into a store whose quads are exactly `quads`.
async fn assert_round_trips(batch: RecordBatch, quads: &[Quad]) {
    let built = batch
        .into_vortex_array(LayoutStrategy::Dictionary, vec![])
        .await
        .unwrap();
    let store = VortexRdfStore::from_built(built).unwrap();
    assert_eq!(view_strings(&store).await, quad_strings(quads));
}

#[tokio::test]
async fn utf8_batch_round_trips() {
    let quads = round_trip_quads();
    let batch = quad_batch(&quads, [utf8_col, utf8_col, utf8_col, utf8_col]);
    assert_round_trips(batch, &quads).await;
}

#[tokio::test]
async fn mixed_string_encodings_round_trip() {
    // pandas/pyarrow don't agree on one string representation: a real
    // batch may mix plain, large, view, and dictionary-encoded columns.
    let quads = round_trip_quads();
    let batch = quad_batch(
        &quads,
        [large_utf8_col, dictionary_col, utf8_view_col, utf8_col],
    );
    assert_round_trips(batch, &quads).await;
}

#[tokio::test]
async fn empty_batch_round_trips() {
    let batch = quad_batch(&[], [utf8_col, utf8_col, utf8_col, utf8_col]);
    assert_round_trips(batch, &[]).await;
}

#[tokio::test]
async fn null_term_is_refused() {
    for missing in ["s", "p", "o", "g"] {
        let mut columns: [(&str, ArrayRef); 4] = [
            ("s", utf8_col(&["s1".into()])),
            ("p", utf8_col(&["p1".into()])),
            ("o", utf8_col(&["o1".into()])),
            ("g", utf8_col(&["g1".into()])),
        ];
        for (name, array) in &mut columns {
            if *name == missing {
                *array = Arc::new(StringArray::from(vec![None::<&str>]));
            }
        }
        let batch = batch_from(columns);
        let err = batch
            .into_vortex_array(LayoutStrategy::Dictionary, vec![])
            .await
            .err()
            .unwrap();
        let msg = err.to_string();
        assert!(
            msg.contains("contains a null term") && msg.contains(&format!("{missing:?}")),
            "column {missing}: unexpected error: {msg}"
        );
    }
}

#[tokio::test]
async fn missing_column_is_refused() {
    let all: [(&str, &str); 4] = [("s", "s1"), ("p", "p1"), ("o", "o1"), ("g", "g1")];
    for missing in ["s", "p", "o", "g"] {
        let columns: Vec<(&str, ArrayRef)> = all
            .iter()
            .filter(|(name, _)| *name != missing)
            .map(|(name, value)| (*name, utf8_col(&[(*value).into()])))
            .collect();
        let fields: Vec<Field> = columns
            .iter()
            .map(|(name, array)| Field::new(*name, array.data_type().clone(), true))
            .collect();
        let arrays: Vec<ArrayRef> = columns.into_iter().map(|(_, a)| a).collect();
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap();

        let err = batch
            .into_vortex_array(LayoutStrategy::Dictionary, vec![])
            .await
            .err()
            .unwrap();
        let msg = err.to_string();
        assert!(
            msg.contains("missing required column") && msg.contains(&format!("{missing:?}")),
            "missing {missing}: unexpected error: {msg}"
        );
    }
}

#[tokio::test]
async fn non_dictionary_layout_is_refused_before_touching_columns() {
    // A batch missing `g` would trip the missing-column error under the
    // Dictionary layout; under any other layout `string_column` is never
    // called at all — the layout check runs first and short-circuits it.
    for layout in [LayoutStrategy::Default, LayoutStrategy::TypedObject] {
        let columns: Vec<(&str, ArrayRef)> = vec![
            ("s", utf8_col(&["s1".into()])),
            ("p", utf8_col(&["p1".into()])),
            ("o", utf8_col(&["o1".into()])),
        ];
        let fields: Vec<Field> = columns
            .iter()
            .map(|(name, array)| Field::new(*name, array.data_type().clone(), true))
            .collect();
        let arrays: Vec<ArrayRef> = columns.into_iter().map(|(_, a)| a).collect();
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap();
        let err = batch.into_vortex_array(layout, vec![]).await.err().unwrap();
        let msg = err.to_string();
        assert!(
            msg.contains("only suppots dictionary layout for now"),
            "{layout:?}: unexpected error: {msg}"
        );
    }
}
