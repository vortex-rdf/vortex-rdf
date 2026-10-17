//! The `deserialize` arm: a Vortex-RDF file (or stdin bytes) → RDF text.

use anyhow::{Context, Result};
use log::info;
use std::io::{Read, stdin};
use std::time::Instant;

use vortex_rdf_core::VortexRdfStore;

use super::{export_to_output, output_format};
use crate::DeserializeArgs;

pub async fn run(args: DeserializeArgs) -> Result<()> {
    let DeserializeArgs {
        input,
        output,
        format,
    } = args;

    let start = Instant::now();
    let format = output_format(format, output.as_deref());

    // The input is read before the output file is created, so a store that
    // cannot be opened leaves an existing output file as it was.
    let store = match &input {
        Some(path) => VortexRdfStore::from_file(path).await?,
        None => {
            let mut buffer = Vec::new();
            stdin()
                .read_to_end(&mut buffer)
                .context("Failed to read from stdin")?;
            // The store takes ownership of the buffer, so the bytes are not
            // copied.
            VortexRdfStore::from_bytes_owned(buffer).await?
        }
    };

    export_to_output(
        store,
        output.as_deref(),
        format,
        "Failed to export RDF from Vortex",
    )
    .await?;
    info!("Deserialization took {:?}", start.elapsed());

    Ok(())
}
