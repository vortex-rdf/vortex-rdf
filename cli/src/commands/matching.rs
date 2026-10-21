//! The `match` arm (named `matching` because `match` is a keyword): filter a
//! store by a quad pattern and export the matches as RDF text.

use anyhow::{Context, Result};
use log::{debug, info};
use std::fs::File;
use std::time::Instant;

use vortex_rdf_core::common::terms::parse_quads_from_reader;
use vortex_rdf_core::{LayoutStrategy, Probe, VortexRdfError, VortexRdfStore};

use super::{export_to_output, input_format, output_format};
use crate::MatchArgs;

/// What each pattern flag accepts, keyed by the position core names at the
/// head of a refusal (`pattern subject: …`): the context a refused flag is
/// reported under.
const FLAG_HINTS: [(&str, &str); 4] = [
    (
        "pattern subject:",
        "Invalid --subject pattern (expected <iri>, an IRI, or _:blank)",
    ),
    (
        "pattern predicate:",
        "Invalid --predicate pattern (expected <iri> or an IRI)",
    ),
    (
        "pattern object:",
        "Invalid --object pattern (expected <iri>, _:blank, \"literal\", \
         \"literal\"@lang, or \"literal\"^^<datatype>)",
    ),
    (
        "pattern graph:",
        "Invalid --graph pattern (expected <iri>, _:blank, or default)",
    ),
];

/// `error`, a refused pattern, under the hint of the flag it names.
fn flag_error(error: VortexRdfError) -> anyhow::Error {
    let hint = match &error {
        VortexRdfError::InvalidInput(message) => FLAG_HINTS
            .iter()
            .find(|(position, _)| message.starts_with(position))
            .map(|(_, hint)| *hint),
        _ => None,
    };
    match hint {
        Some(hint) => anyhow::Error::new(error).context(hint),
        None => error.into(),
    }
}

pub async fn run(args: MatchArgs) -> Result<()> {
    let MatchArgs {
        input,
        output,
        input_format: explicit_input,
        output_format: explicit_format,
        subject,
        predicate,
        object,
        graph,
    } = args;

    let start = Instant::now();

    // Pattern arguments are user-typed, so `Probe::parse` validates them
    // rather than the store's trusted decode path.
    let probe = Probe::parse(
        subject.as_deref(),
        predicate.as_deref(),
        object.as_deref(),
        graph.as_deref(),
    )
    .map_err(flag_error)?;

    let output_format = output_format(explicit_format, output.as_deref());

    let is_vortex = input.extension().map(|e| e == "vortex").unwrap_or(false);

    let load_start = Instant::now();
    let store = if is_vortex {
        let store = VortexRdfStore::from_file(&input).await?;
        debug!("Opened Vortex file in {:?}", load_start.elapsed());
        store
    } else {
        let input_format = input_format(explicit_input, Some(input.as_path()), "--input-format")?;

        let reader = Box::new(File::open(&input).context("Failed to open input file")?);
        let quads_stream = parse_quads_from_reader(reader, input_format);

        let store =
            VortexRdfStore::from_quads(quads_stream, LayoutStrategy::Dictionary, vec![]).await?;
        debug!("Vortex store built in {:?}", load_start.elapsed());
        store
    };

    let match_start = Instant::now();
    let filtered = store
        .run_probe(&probe)
        .await
        .context("Failed to match pattern")?;
    debug!("Applying match pattern took {:?}", match_start.elapsed());

    // The output file is created once the input has been read and matched, so
    // a failure before that leaves an existing output file as it was.
    export_to_output(
        filtered,
        output.as_deref(),
        output_format,
        "Failed to export filtered results",
    )
    .await?;

    info!("Full matching operation took {:?}", start.elapsed());

    Ok(())
}
