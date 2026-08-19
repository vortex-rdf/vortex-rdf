//! The `deserialize` arm: a Vortex-RDF file (or stdin bytes) → RDF text.

use anyhow::{Context, Result};
use log::info;
use oxrdfio::RdfFormat;
use std::fs::File;
use std::io::{Read, Write, stdin, stdout};
use std::time::Instant;

use vortex_rdf_core::VortexRdfStore;
use vortex_rdf_core::common::export::export_rdf;
use vortex_rdf_core::common::formats::detect_format;

use crate::DeserializeArgs;

pub async fn run(args: DeserializeArgs) -> Result<()> {
    let DeserializeArgs {
        input,
        output,
        format,
    } = args;

    let start = Instant::now();
    let format = format
        .or_else(|| detect_format(&output))
        .unwrap_or(RdfFormat::NQuads);

    let writer: Box<dyn Write> = match &output {
        Some(p) => Box::new(File::create(p).context("Failed to create output file")?),
        None => Box::new(stdout()),
    };

    match &input {
        Some(path) => {
            let store = VortexRdfStore::from_file(path)
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
            export_rdf(store, writer, format)
                .await
                .context("Failed to export RDF from Vortex")?;
        }
        None => {
            let mut buffer = Vec::new();
            stdin()
                .read_to_end(&mut buffer)
                .context("Failed to read from stdin")?;
            let store = VortexRdfStore::from_bytes(&buffer)
                .await
                .map_err(|e| anyhow::anyhow!(e))?;
            export_rdf(store, writer, format)
                .await
                .context("Failed to export RDF from Vortex")?;
        }
    }
    info!("Deserialization took {:?}", start.elapsed());

    Ok(())
}
