//! The `serialize` arm: RDF text (file or stdin) → a Vortex-RDF file.

use anyhow::{Context, Result};
use log::info;
use std::fs::File;
use std::io::{Read, stdin};
use std::time::Instant;

use vortex_rdf_core::common::terms::parse_quads_from_reader;
use vortex_rdf_core::io::quads_stream_to_vortex_file;

use super::input_format;
use crate::SerializeArgs;

pub async fn run(args: SerializeArgs) -> Result<()> {
    let SerializeArgs {
        layout,
        indexes,
        input,
        output,
        format,
    } = args;

    let start = Instant::now();
    let format = input_format(format, input.as_deref(), "--format")?;

    let reader: Box<dyn Read + Send> = match &input {
        Some(p) => Box::new(File::open(p).context("Failed to open input file")?),
        None => Box::new(stdin()),
    };
    let quads_stream = parse_quads_from_reader(reader, format);

    // Chunks are streamed into the Vortex writer as they are built: the
    // out-of-core sort never materializes the full dataset.
    quads_stream_to_vortex_file(quads_stream, &output, layout, indexes)
        .await
        .context("Failed to serialize to Vortex")?;
    info!("Fully serialized to Vortex-RDF in {:?}", start.elapsed());

    Ok(())
}
