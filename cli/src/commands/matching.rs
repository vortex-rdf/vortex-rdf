//! The `match` arm (named `matching` because `match` is a keyword): filter a
//! store by a quad pattern and export the matches as RDF text.

use anyhow::{Context, Result, anyhow};
use log::{debug, info};
use oxrdfio::RdfFormat;
use std::fs::File;
use std::io::{Write, stdout};
use std::time::Instant;

use vortex_rdf_core::common::export::export_rdf;
use vortex_rdf_core::common::formats::detect_format;
use vortex_rdf_core::common::terms::{parse_pattern_checked, parse_quads_from_reader};
use vortex_rdf_core::{LayoutStrategy, SortedStreamBuilder, VortexArrayBuilder, VortexRdfStore};

use crate::MatchArgs;

pub async fn run(args: MatchArgs) -> Result<()> {
    let MatchArgs {
        input,
        output,
        input_format,
        output_format,
        subject,
        predicate,
        object,
        graph,
    } = args;

    let start = Instant::now();

    // Pattern arguments are user-typed, so they take core's validating
    // `parse_pattern_checked` rather than the store's trusted decode path.
    // Core reports the first invalid slot without naming it, so each slot
    // goes through it alone to keep the failing flag in the error.
    let (subject_node, ..) = parse_pattern_checked(subject.as_deref(), None, None, None)
        .context("Invalid --subject pattern (expected <iri>, an IRI, or _:blank)")?;
    let (_, predicate_node, ..) = parse_pattern_checked(None, predicate.as_deref(), None, None)
        .context("Invalid --predicate pattern (expected <iri> or an IRI)")?;
    let (.., object_node, _) = parse_pattern_checked(None, None, object.as_deref(), None).context(
        "Invalid --object pattern (expected <iri>, _:blank, \"literal\", \
         \"literal\"@lang, or \"literal\"^^<datatype>)",
    )?;
    let (.., graph_node) = parse_pattern_checked(None, None, None, graph.as_deref())
        .context("Invalid --graph pattern (expected <iri>, _:blank, or default)")?;

    let output_format = output_format
        .or_else(|| detect_format(&output))
        .unwrap_or(RdfFormat::NQuads);

    let writer: Box<dyn Write> = match &output {
        Some(p) => Box::new(File::create(p).context("Failed to create output file")?),
        None => Box::new(stdout()),
    };

    let is_vortex = input.extension().map(|e| e == "vortex").unwrap_or(false);

    let load_start = Instant::now();
    let store = if is_vortex {
        let store = VortexRdfStore::from_file(&input)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
        debug!("Opened Vortex file in {:?}", load_start.elapsed());
        store
    } else {
        let input_format = input_format
            .or_else(|| detect_format(&Some(input.clone())))
            .ok_or_else(|| {
                anyhow!("Could not detect RDF format. Please specify it with --input-format")
            })?;

        let reader = Box::new(File::open(&input).context("Failed to open input file")?);
        let quads_stream = parse_quads_from_reader(reader, input_format);

        let built = SortedStreamBuilder::build_vortex_array(
            Box::new(quads_stream),
            LayoutStrategy::Default,
            vec![],
        )
        .await?;
        let store = VortexRdfStore::from_built(built).map_err(|e| anyhow::anyhow!(e))?;
        debug!("Vortex store built in {:?}", load_start.elapsed());
        store
    };

    let match_start = Instant::now();
    let filtered = store
        .match_pattern(
            subject_node.as_ref(),
            predicate_node.as_ref(),
            object_node.as_ref(),
            graph_node.as_ref(),
        )
        .await
        .context("Failed to match pattern")?;
    debug!("Applying match pattern took {:?}", match_start.elapsed());

    export_rdf(filtered, writer, output_format)
        .await
        .context("Failed to export filtered results")?;

    info!("Full matching operation took {:?}", start.elapsed());

    Ok(())
}
