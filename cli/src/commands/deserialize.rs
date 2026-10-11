//! The `deserialize` arm: a Vortex-RDF file (or stdin bytes) → RDF text.

use anyhow::{Context, Result};
use log::info;
use oxrdfio::RdfFormat;
use std::fs::File;
use std::io::{Read, Write, stdin, stdout};
use std::time::Instant;

use vortex_rdf_core::common::formats::detect_format;
use vortex_rdf_core::{VortexRdfStore, export_rdf};

use crate::DeserializeArgs;

pub async fn run(args: DeserializeArgs) -> Result<()> {
    let DeserializeArgs {
        input,
        output,
        format,
    } = args;

    let start = Instant::now();
    let format = format
        .or_else(|| detect_format(output.as_deref()))
        .unwrap_or(RdfFormat::NQuads);

    // The input is read before the output file is created, so a store that
    // cannot be opened leaves an existing output file as it was.
    let store = match &input {
        Some(path) => VortexRdfStore::from_file(path)
            .await
            .map_err(|e| anyhow::anyhow!(e))?,
        None => {
            let mut buffer = Vec::new();
            stdin()
                .read_to_end(&mut buffer)
                .context("Failed to read from stdin")?;
            VortexRdfStore::from_bytes(&buffer)
                .await
                .map_err(|e| anyhow::anyhow!(e))?
        }
    };

    let writer: Box<dyn Write> = match &output {
        Some(p) => Box::new(File::create(p).context("Failed to create output file")?),
        None => Box::new(stdout()),
    };
    export_rdf(store, writer, format)
        .await
        .context("Failed to export RDF from Vortex")?;
    info!("Deserialization took {:?}", start.elapsed());

    Ok(())
}
