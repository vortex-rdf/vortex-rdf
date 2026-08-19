//! Exporting a store back to textual RDF: the write direction of the textual
//! boundary whose read direction is
//! [`parse_quads_from_reader`](super::terms::parse_quads_from_reader) — the
//! counterpart of parsing, not of the Vortex byte format (writing that is
//! `io::ser`'s job, reading it `io::native_file`'s).

use crate::error::{self, VortexRdfError};
use crate::store::VortexRdfStore;

use futures::StreamExt;
use oxrdfio::{RdfFormat, RdfSerializer};
use std::io::Write;
use web_time::Instant;

/// Serialize a Vortex-RDF store to textual RDF in the specified format
/// (Turtle, N-Triples, etc.), streaming quads sequentially into `writer`.
///
/// N-Triples and N-Quads are written straight from the store's raw columns:
/// the columns hold every term in exactly the N-Triples string form those
/// line formats need, so the per-quad oxrdf parse and re-escape the
/// structured formats require is skipped entirely.
pub async fn export_rdf<W: Write>(
    store: VortexRdfStore,
    writer: W,
    format: RdfFormat,
) -> error::Result<()> {
    match format {
        RdfFormat::NQuads | RdfFormat::NTriples => write_raw(store, writer, format).await,
        _ => write_structured(store, writer, format).await,
    }
}

/// The N-Triples/N-Quads fast path: the store's raw term strings are the
/// serialization, byte for byte. Both formats are `s p o [g] .` lines of
/// N-Triples-form terms, and the columns store exactly that form (with the
/// empty string marking the default graph) — so each line is assembled from
/// the raw strings with no term ever parsed or re-escaped. Only the graph
/// needs a check: N-Triples cannot represent a named graph, and a non-default
/// graph fails exactly as oxrdfio's serializer would.
async fn write_raw<W: Write>(
    store: VortexRdfStore,
    mut writer: W,
    format: RdfFormat,
) -> error::Result<()> {
    let decode_start = Instant::now();
    // The raw chunk stream (in-memory, or the lazy file-backed scan).
    let mut chunks = store.raw_quad_chunks().await?;
    log::debug!(
        "[export_rdf] Raw chunk stream setup took {:?}",
        decode_start.elapsed()
    );

    let write_start = Instant::now();
    let named_graphs = format == RdfFormat::NQuads;
    while let Some(chunk) = chunks.next().await {
        for quad in chunk {
            let quad = quad?;
            if quad.g.is_empty() {
                writeln!(writer, "{} {} {} .", quad.s, quad.p, quad.o)?;
            } else if named_graphs {
                writeln!(writer, "{} {} {} {} .", quad.s, quad.p, quad.o, quad.g)?;
            } else {
                // The same refusal oxrdfio's N-Triples serializer gives.
                return Err(VortexRdfError::Serialization(
                    "Only quads in the default graph can be serialized to a RDF graph format"
                        .to_string(),
                ));
            }
        }
    }
    log::debug!(
        "[export_rdf] Raw write loop took {:?}",
        write_start.elapsed()
    );
    Ok(())
}

/// The structured-format path (Turtle, TriG, RDF/XML, …): decode each quad to
/// oxrdf terms and drive [`RdfSerializer`], which owns the format's syntax
/// state (prefixes, closing blocks).
async fn write_structured<W: Write>(
    store: VortexRdfStore,
    writer: W,
    format: RdfFormat,
) -> error::Result<()> {
    let decode_start = Instant::now();
    // Retrieve the quad stream (either in-memory or lazy file-backed stream).
    let mut quads_stream = store.quads()?;
    log::debug!(
        "[export_rdf] Quad stream setup took {:?}",
        decode_start.elapsed()
    );

    let write_start = Instant::now();
    // Construct the oxrdf serialization helper for streaming output.
    let mut rdf_serializer = RdfSerializer::from_format(format).for_writer(writer);

    // Dynamically iterate over each quad and push it to the output writer.
    while let Some(quad_res) = quads_stream.next().await {
        let quad = quad_res?;
        rdf_serializer
            .serialize_quad(&quad)
            .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    }

    // Finalize the serialization output (e.g. closing syntax blocks).
    rdf_serializer
        .finish()
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;

    log::debug!(
        "[export_rdf] Serialization/write loop took {:?}",
        write_start.elapsed()
    );

    Ok(())
}
