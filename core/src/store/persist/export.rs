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
/// N-Triples and N-Quads lines reach the writer a chunk at a time (a scan
/// split, a read by id, or a whole in-memory view); the other formats'
/// serializer writes each quad as several small writes. The writer is not flushed here. Wrap one that makes a system call
/// per write (a `File`) or per line (`Stdout`, which is line-buffered) in a
/// [`BufWriter`](std::io::BufWriter), pass it by `&mut`, and flush it once the
/// export returns: a `BufWriter` dropped unflushed discards the error of its
/// last write.
///
/// A writer's failure is an [`Io`](VortexRdfError::Io) error carrying the
/// writer's `io::Error`, in every format; a quad the format cannot represent
/// (a named graph in N-Triples, say) is a `Serialization` error.
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
/// the default graph), so each chunk's lines are written straight from its
/// columns ([`NQuadLines`](crate::common::quad::NQuadLines)) with no term
/// parsed or re-escaped and no string per row, and handed to the writer a
/// chunk at a time. N-Triples cannot represent a named graph: a quad in one
/// fails as oxrdfio's serializer fails it.
async fn write_raw<W: Write>(
    store: VortexRdfStore,
    mut writer: W,
    format: RdfFormat,
) -> error::Result<()> {
    // The view's chunks, read from the first poll on, so the loop's timing
    // below includes the read.
    let mut chunks = store.nquad_line_chunks();

    let write_start = debug::timer();
    let named_graphs = format == RdfFormat::NQuads;
    while let Some(chunk) = chunks.next().await {
        for lines in chunk {
            let lines = lines?;
            if lines.named_graph && !named_graphs {
                // The same refusal oxrdfio's N-Triples serializer gives.
                return Err(VortexRdfError::Serialization(
                    "Only quads in the default graph can be serialized to a RDF graph format"
                        .to_string(),
                ));
            }
            writer.write_all(lines.text.as_bytes())?;
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
            .map_err(serializer_error)?;
    }

    rdf_serializer.finish().map_err(serializer_error)?;

    log::debug!(
        "[export_rdf] Structured read and write loop took {:?}",
        debug::elapsed(write_start)
    );

    Ok(())
}

/// An error from [`RdfSerializer`]: `InvalidInput` is its refusal of a quad the
/// format cannot represent (a named graph in a graph format, an RDF/XML term),
/// a `Serialization` error as on the N-Triples/N-Quads path; any other kind is
/// the writer's own failure, an `Io` error that keeps its kind.
fn serializer_error(e: std::io::Error) -> VortexRdfError {
    if e.kind() == std::io::ErrorKind::InvalidInput {
        VortexRdfError::Serialization(e.to_string())
    } else {
        VortexRdfError::Io(e)
    }
}
