//! Located runs decided zone by zone: the zone maps of a store file's
//! columns read as Vortex `ZoneMap`s.

use super::*;
use crate::io::zone_map::ColumnZones;

/// A store file's `g` column read as a zone map: 20,000 rows of `g1` then
/// `g2` make three zones, and Vortex's falsifier of `g = g1` holds on the
/// last one only.
#[tokio::test]
async fn column_zones_read_from_a_written_column() {
    use vortex_array::dtype::{DType, Nullability, PType};
    use vortex_array::expr::{eq, lit, root};

    let quads: Vec<Quad> = (0..20_000)
        .map(|i| {
            make_quad(
                &format!("http://example.org/s{i:05}"),
                "http://example.org/p",
                "o",
                numbered_graph(1 + i / 10_000),
            )
        })
        .collect();
    let (_dir, path) = write_store_file(quads, LayoutStrategy::Dictionary, vec![]).await;
    let store = VortexRdfStore::from_file(&path).await.unwrap();
    let g1 = row_list(&store).await[0][3];
    let native = open_mapped(&path).await;
    let column = native
        .table_column_layout(None, "g")
        .expect("the quad table has a g column");
    let zones = ColumnZones::read(&column, &native.segment_source())
        .await
        .unwrap()
        .expect("a written code column is zoned with exact bounds");
    assert_eq!((zones.zones(), zones.zone_len()), (3, 8_192));
    let none = eq(root(), lit(g1))
        .bind(&DType::Primitive(PType::U64, Nullability::NonNullable))
        .unwrap()
        .falsify(&crate::session::VORTEX_SESSION)
        .unwrap()
        .unwrap();
    let mask = zones.prune(0..3, &none).unwrap();
    assert_eq!(
        (0..3).map(|i| mask.value(i)).collect::<Vec<_>>(),
        [false, false, true]
    );
}

/// A file written in seven-row leaves has no zone maps, so its columns
/// have none to read.
#[tokio::test]
async fn column_zones_absent_without_zone_maps() {
    let (_dir, path) = write_chunked_store_file(&fixture_quads(), 7).await;
    let native = open_mapped(&path).await;
    let column = native.table_column_layout(None, "g").unwrap();
    assert!(
        ColumnZones::read(&column, &native.segment_source())
            .await
            .unwrap()
            .is_none()
    );
}
