//! Exporting a store as textual RDF: N-Triples and N-Quads straight from the
//! stored term strings, every other format through oxrdfio's serializer.

use crate::error::{self, VortexRdfError};
use crate::store::VortexRdfStore;

use crate::debug;
use futures::StreamExt;
use oxrdfio::{RdfFormat, RdfSerializer};
use std::io::Write;

/// Serialize the store as `format` into `writer`, streaming quads
/// sequentially.
pub async fn export_rdf<W: Write>(
    store: VortexRdfStore,
    writer: W,
    format: RdfFormat,
) -> error::Result<()> {
    let start = debug::timer();
    match format {
        RdfFormat::NQuads | RdfFormat::NTriples => write_raw(store, writer, format).await?,
        _ => write_structured(store, writer, format).await?,
    }
    log::debug!(
        "[export_rdf] Wrote {format:?} in {:?}",
        debug::elapsed(start)
    );
    Ok(())
}

/// The N-Triples/N-Quads path: each line is assembled from the stored term
/// strings, which are already in N-Triples form (`g == ""` is the default
/// graph). A named graph under N-Triples is the error oxrdfio's serializer
/// gives.
async fn write_raw<W: Write>(
    store: VortexRdfStore,
    mut writer: W,
    format: RdfFormat,
) -> error::Result<()> {
    let mut chunks = store.raw_quad_chunks();
    let named_graphs = format == RdfFormat::NQuads;
    while let Some(chunk) = chunks.next().await {
        for quad in chunk {
            let quad = quad?;
            if quad.g.is_empty() {
                writeln!(writer, "{} {} {} .", quad.s, quad.p, quad.o)?;
            } else if named_graphs {
                writeln!(writer, "{} {} {} {} .", quad.s, quad.p, quad.o, quad.g)?;
            } else {
                return Err(VortexRdfError::Serialization(
                    "Only quads in the default graph can be serialized to a RDF graph format"
                        .to_string(),
                ));
            }
        }
    }
    Ok(())
}

/// The structured-format path (Turtle, TriG, RDF/XML, …): each quad decoded
/// to oxrdf terms and fed to [`RdfSerializer`], which owns the format's
/// syntax state.
async fn write_structured<W: Write>(
    store: VortexRdfStore,
    writer: W,
    format: RdfFormat,
) -> error::Result<()> {
    let mut quads_stream = store.quads()?;
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
    Ok(())
}
