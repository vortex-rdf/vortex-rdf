//! Exporting a store back to textual RDF: the counterpart of
//! [`parse_quads_from_reader`](crate::common::terms::parse_quads_from_reader),
//! not of the Vortex byte format.

use crate::error::{self, VortexRdfError};
use crate::store::VortexRdfStore;

use crate::debug;
use futures::StreamExt;
use oxrdfio::{RdfFormat, RdfSerializer};
use std::io::Write;

/// Serialize the store as `format` into `writer`, streaming quads
/// sequentially. N-Triples and N-Quads are written straight from the store's
/// raw columns; every other format goes through oxrdfio's serializer.
///
/// The writer receives each quad as several small writes and is neither
/// buffered nor flushed here. Wrap one that makes a system call per write (a
/// `File`) or per line (`Stdout`, which is line-buffered) in a
/// [`BufWriter`](std::io::BufWriter), pass it by `&mut`, and flush it once the
/// export returns: a `BufWriter` dropped unflushed discards the error of its
/// last write.
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

/// The N-Triples/N-Quads fast path: both formats are `s p o [g] .` lines of
/// N-Triples-form terms, the form the columns store (the empty string marking
/// the default graph), so each line is assembled from the raw strings with no
/// term parsed or re-escaped. N-Triples cannot represent a named graph: a quad
/// in one fails as oxrdfio's serializer fails it.
async fn write_raw<W: Write>(
    store: VortexRdfStore,
    mut writer: W,
    format: RdfFormat,
) -> error::Result<()> {
    // The view's raw chunks, read from the first poll on, so the loop's
    // timing below includes the read.
    let mut chunks = store.raw_quad_chunks();

    let write_start = debug::timer();
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
        "[export_rdf] Raw read and write loop took {:?}",
        debug::elapsed(write_start)
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
    // The quad stream's read is set up on the first poll, so the loop's
    // timing below includes it.
    let mut quads_stream = store.quads();

    let write_start = debug::timer();
    let mut rdf_serializer = RdfSerializer::from_format(format).for_writer(writer);

    while let Some(quad_res) = quads_stream.next().await {
        let quad = quad_res?;
        rdf_serializer
            .serialize_quad(&quad)
            .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;
    }

    rdf_serializer
        .finish()
        .map_err(|e| VortexRdfError::Serialization(e.to_string()))?;

    log::debug!(
        "[export_rdf] Structured read and write loop took {:?}",
        debug::elapsed(write_start)
    );

    Ok(())
}
