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

pub mod de;
pub mod ser;

/// High-level function to serialize RDF from a reader directly to a Vortex-RDF writer.
pub async fn serialize<R: Read + Send + 'static, W: VortexWrite + Unpin + Send>(
    reader: R,
    writer: W,
    format: RdfFormat,
) -> error::Result<()> {
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

    while let Some(quad_res) = rx.recv().await {
        let quad = quad_res.map_err(|e| error::VortexRdfError::Serialization(e.to_string()))?;
        s_ids.push(dict.get_or_insert(&quad.subject.to_string()));
        p_ids.push(dict.get_or_insert(&quad.predicate.to_string()));
        o_ids.push(dict.get_or_insert(&quad.object.to_string()));
        g_ids.push(dict.get_or_insert(&quad.graph_name.to_string()));
    }

    let root = ser::bundle_as_struct(dict, s_ids, p_ids, o_ids, g_ids)?;

    ser::write_array_to_vortex(root, writer).await
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
    let array = de::read_array_from_vortex(source).await?;
    let mut quads_stream = de::decode_quads_stream(array)?;

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
    Ok(())
}

/// High-level function to serialize an iterator of quads to a Vortex-RDF writer.
pub async fn quads_to_vortex_writer<I, W>(quads: I, writer: W) -> error::Result<()>
where
    I: IntoIterator<Item = Quad>,
    W: VortexWrite + Unpin + Send,
{
    let array = ser::encode_quads(quads)?;
    ser::write_array_to_vortex(array, writer).await
}

/// High-level function to serialize an iterator of quads directly to a byte buffer.
pub async fn quads_to_vortex<I>(quads: I) -> error::Result<Vec<u8>>
where
    I: IntoIterator<Item = Quad>,
{
    let mut buffer = Vec::new();
    quads_to_vortex_writer(quads, &mut buffer).await?;
    Ok(buffer)
}

/// High-level function to deserialize Vortex-RDF data to a list of quads.
pub async fn vortex_to_quads(bytes: &[u8]) -> error::Result<Vec<Quad>> {
    let buffer = Buffer::from(bytes.to_vec());
    let array = de::read_array_from_vortex(buffer).await?;
    de::decode_quads(array).await
}
