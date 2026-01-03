use oxrdf::Quad;
pub use oxrdfio::RdfFormat;
use oxrdfio::{RdfParser, RdfSerializer};
use std::io::{Read, Write};
use crate::error;
use vortex_io::VortexWrite;
use vortex_io::file::IntoReadSource;
use futures::StreamExt;
use vortex::buffer::Buffer;
use crate::store::dictionary::Dictionary;
use std::time::Instant;

pub mod de;
pub mod ser;

/// High-level function to serialize RDF from a reader directly to a Vortex-RDF writer.
pub async fn serialize<R: Read + Send + 'static, W: VortexWrite + Unpin + Send>(
    reader: R,
    writer: W,
    format: RdfFormat,
) -> error::Result<()> {
    let start = Instant::now();
    let parser = RdfParser::from_format(format);
    let (tx, mut rx) = tokio::sync::mpsc::channel(100);

    // Run the parser in a blocking task since oxrdfio is sync
    tokio::task::spawn_blocking(move || {
        let quads_iter = parser.for_reader(reader);
        for quad_res in quads_iter {
            if tx.blocking_send(quad_res).is_err() {
                break;
            }
        }
    });

    let mut dict = Dictionary::new();
    let mut s_ids = Vec::new();
    let mut p_ids = Vec::new();
    let mut o_ids = Vec::new();
    let mut g_ids = Vec::new();

    let convert_start = Instant::now();
    while let Some(quad_res) = rx.recv().await {
        let quad = quad_res.map_err(|e| error::VortexRdfError::Serialization(e.to_string()))?;
        s_ids.push(dict.get_or_insert(&quad.subject.to_string()));
        p_ids.push(dict.get_or_insert(&quad.predicate.to_string()));
        o_ids.push(dict.get_or_insert(&quad.object.to_string()));
        g_ids.push(dict.get_or_insert(&quad.graph_name.to_string()));
    }
    log::debug!("[serialize] RDF to Oxigraph Quads Conversion took {:?}", convert_start.elapsed());

    let bundle_start = Instant::now();
    let vortex_array = ser::bundle_as_struct(dict, s_ids, p_ids, o_ids, g_ids)?;
    log::debug!("[serialize] Vortex Struct Array Bundling took {:?}", bundle_start.elapsed());

    let writing_start = Instant::now();
    let converted = ser::write_array_to_vortex(vortex_array, writer).await;
    log::debug!("[serialize] Vortex Array Writing took {:?}", writing_start.elapsed());
    log::debug!("[serialize] Full serialization took {:?}", start.elapsed());

    converted
}

/// High-level function to deserialize Vortex-RDF data from a reader directly to an RDF writer.
pub async fn deserialize<S, W>(
    source: S,
    writer: W,
    format: RdfFormat,
) -> error::Result<()>
where
    S: IntoReadSource,
    W: Write,
{
    let start = Instant::now();
    let vortex_array = de::read_array_from_vortex(source).await?;
    log::debug!("[deserialize] Reading vortex array took {:?}", start.elapsed());

    let decode_start = Instant::now();
    let mut quads_stream = de::decode_quads_stream(vortex_array)?;
    log::debug!("[deserialize] Stream setup took {:?}", decode_start.elapsed());

    let write_start = Instant::now();
    let mut serializer = RdfSerializer::from_format(format).for_writer(writer);
    while let Some(quad_res) = quads_stream.next().await {
        let quad = quad_res?;
        serializer
            .serialize_quad(&quad)
            .map_err(|e| error::VortexRdfError::Deserialization(e.to_string()))?;
    }
    serializer
        .finish()
        .map_err(|e| error::VortexRdfError::Deserialization(e.to_string()))?;
    
    log::debug!("[deserialize] Serialization/write loop took {:?}", write_start.elapsed());
    log::debug!("[deserialize] Total deserialization took {:?}", start.elapsed());
    
    Ok(())
}

/// High-level function to serialize a stream of quads to a Vortex-RDF writer.
pub async fn quads_stream_to_vortex_writer<S, W>(quads: S, writer: W) -> error::Result<()>
where
    S: futures::Stream<Item = error::Result<Quad>> + Unpin,
    W: VortexWrite + Unpin + Send,
{
    let array = ser::encode_quads(quads).await?;
    ser::write_array_to_vortex(array, writer).await
}

/// High-level function to serialize a stream of quads directly to a byte buffer.
pub async fn quads_stream_to_vortex<S>(quads: S) -> error::Result<Vec<u8>>
where
    S: futures::Stream<Item = error::Result<Quad>> + Unpin,
{
    let mut buffer = Vec::new();
    quads_stream_to_vortex_writer(quads, &mut buffer).await?;
    Ok(buffer)
}

/// High-level function to deserialize Vortex-RDF data to a list of quads.
pub async fn vortex_to_quads(bytes: &[u8]) -> error::Result<Vec<Quad>> {
    let buffer = Buffer::from(bytes.to_vec());
    let array = de::read_array_from_vortex(buffer).await?;
    de::decode_quads(array).await
}
