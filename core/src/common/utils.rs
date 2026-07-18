use crate::error::{VortexRdfError, Result};
use crate::io::VORTEX_LIGHT_SESSION;

use oxrdf::{
    Quad,
    Term,
    NamedNode,
    BlankNode,
    NamedOrBlankNode,
    Literal,
    GraphName
};
use oxrdfio::{RdfFormat, RdfParser};
use futures::{stream, Stream};

use vortex_array::{ArrayRef, IntoArray, VortexSessionExecute};
use vortex_array::arrays::{BoolArray, VarBinViewArray};
use vortex_mask::Mask;

/// Build a Vortex string array (`VarBinView<Utf8>`, non-nullable) from string refs.
///
/// Values are copied once, directly into the array's buffer — no intermediate
/// owned `String` per value.
pub fn make_string_array(values: impl IntoIterator<Item = impl AsRef<str>>) -> ArrayRef {
    VarBinViewArray::from_iter_str(values).into_array()
}

/// Build a nullable Vortex string array for optional fields (e.g. o_datatype, o_lang).
pub fn make_nullable_string_array(values: impl IntoIterator<Item = Option<String>>) -> ArrayRef {
    VarBinViewArray::from_iter_nullable_str(values).into_array()
}

/// Stamp the exact `IsSorted` statistic on an array.
///
/// Only call when the array is sorted by construction: `match_pattern` trusts
/// this stat to binary-search the column, so a false stamp corrupts query
/// results.
pub(crate) fn stamp_is_sorted(arr: &ArrayRef) {
    use vortex_array::expr::stats::{Stat, Precision};
    arr.statistics().set(Stat::IsSorted, Precision::Exact(true.into()));
}

/// Read back the `IsSorted` statistic written by [`stamp_is_sorted`]. An
/// absent stat counts as unsorted — order is never assumed, only trusted
/// when explicitly recorded.
pub(crate) fn column_is_sorted(arr: &ArrayRef) -> bool {
    use vortex_array::expr::stats::{Stat, StatsProvider, Precision};
    match arr.statistics().get(Stat::IsSorted) {
        Precision::Exact(sc) | Precision::Inexact(sc) 
            => bool::try_from(&sc).unwrap_or(false),
        Precision::Absent => false,
    }
}

/// Binary-search a sorted column for the `[lo, hi)` run of rows equal to
/// `probe` (`lo == hi` means the value is absent). Only meaningful on
/// columns [`column_is_sorted`] reports as sorted.
pub(crate) fn search_sorted_bounds(
    arr: &ArrayRef,
    probe: &vortex_array::scalar::Scalar,
) -> Result<(usize, usize)> {
    use vortex_array::search_sorted::{SearchSorted, SearchSortedSide, SearchResult};
    let index_of = |result: SearchResult| match result {
        SearchResult::Found(i) | SearchResult::NotFound(i) => i,
    };
    let lo = arr
        .search_sorted(probe, SearchSortedSide::Left)
        .map_err(VortexRdfError::Vortex)?;
    let hi = arr
        .search_sorted(probe, SearchSortedSide::Right)
        .map_err(VortexRdfError::Vortex)?;
    Ok((index_of(lo), index_of(hi)))
}

/// Convert a boolean ArrayRef into a `vortex_mask::Mask` for use with `ArrayRef::filter`.
pub(crate) fn bool_array_to_mask(arr: ArrayRef) -> Result<Mask> {
    // Canonicalize to a concrete boolean array, then reinterpret its packed
    // bit buffer directly as a Mask (no per-bit conversion loop).
    let mut ctx = VORTEX_LIGHT_SESSION.create_execution_ctx();
    let bool_arr = arr.execute::<BoolArray>(&mut ctx).map_err(VortexRdfError::Vortex)?;
    Ok(Mask::from_buffer(bool_arr.into_bit_buffer()))
}

/// Parses a string representation of an RDF named node (URI), stripping optional `<` and `>` boundaries.
pub fn parse_named_node(s: &str) -> Result<NamedNode> {
    let s = s.trim_matches(|c| c == '<' || c == '>');
    NamedNode::new(s)
        .map_err(|e| VortexRdfError::Deserialization(
            format!("Invalid NamedNode '{}': {}", s, e)
        ))
}

/// Parses a string representation of an RDF blank node, stripping the `_:` prefix if present.
pub fn parse_blank_node(s: &str) -> Result<BlankNode> {
    let s = s.trim_start_matches("_:");
    BlankNode::new(s)
        .map_err(|e| VortexRdfError::Deserialization(
            format!("Invalid BlankNode '{}': {}", s, e)
        ))
}

/// Parses an RDF subject node, which can either be a NamedNode (URI) or a BlankNode.
pub fn parse_subject(s: &str) -> Result<NamedOrBlankNode> {
    if s.starts_with("_:") {
        Ok(NamedOrBlankNode::BlankNode(parse_blank_node(s)?))
    } else {
        Ok(NamedOrBlankNode::NamedNode(parse_named_node(s)?))
    }
}

/// Parses an arbitrary RDF term (blank node, literal, or named node) from its string form.
pub fn parse_term(s: &str) -> Result<Term> {
    if s.starts_with('_') {
        Ok(Term::BlankNode(parse_blank_node(s)?))
    } else if s.starts_with('"') {
        let val = s.trim_matches('"');
        Ok(Term::Literal(Literal::new_simple_literal(val)))
    } else {
        Ok(Term::NamedNode(parse_named_node(s)?))
    }
}

/// Parses an RDF graph name, which can be the default graph, a named node, or a blank node.
pub fn parse_graph_name(s: &str) -> Result<GraphName> {
    if s.is_empty() || s.eq_ignore_ascii_case("default") || s == "[]" {
        Ok(GraphName::DefaultGraph)
    } else if s.starts_with("_:") {
        Ok(GraphName::BlankNode(parse_blank_node(s)?))
    } else {
        Ok(GraphName::NamedNode(parse_named_node(s)?))
    }
}

/// Canonical N-Triples string for a graph name: the empty string denotes the
/// default graph.
pub(crate) fn graph_name_str(g: &GraphName) -> String {
    match g {
        GraphName::DefaultGraph => String::new(),
        other => other.to_string(),
    }
}

/// Reconstructs a full structural oxrdf `Term` from its raw serialized string representation.
/// Handles URIs, Blank Nodes, simple literals, language-tagged literals, and typed literals.
pub fn get_as_term(s: &str) -> Option<Term> {
    if s.starts_with('<') {
        Some(Term::NamedNode(
            NamedNode::new(s.trim_matches(|c| c == '<' || c == '>')).ok()?,
        ))
    } else if s.starts_with("_:") {
        Some(Term::BlankNode(
            BlankNode::new(s.trim_start_matches("_:")).ok()?,
        ))
    } else if s.starts_with('"') {
        if s.contains("^^") {
            let parts: Vec<&str> = s.splitn(2, "^^").collect();
            let val = parts[0].trim_matches('"');
            let dt = parts[1].trim_matches(|c| c == '<' || c == '>');
            Some(Term::Literal(Literal::new_typed_literal(
                val,
                NamedNode::new(dt).ok()?,
            )))
        } else if let Some(at_pos) = s.rfind('@') {
            if at_pos > 0 && s.as_bytes()[at_pos - 1] == b'"' {
                let val = s[..at_pos].trim_matches('"');
                let lang = &s[at_pos + 1..];
                Some(Term::Literal(
                    Literal::new_language_tagged_literal(val, lang).ok()?,
                ))
            } else {
                Some(Term::Literal(Literal::new_simple_literal(s.trim_matches('"'))))
            }
        } else {
            Some(Term::Literal(Literal::new_simple_literal(s.trim_matches('"'))))
        }
    } else {
        None
    }
}

/// Borrow the bytes of a UTF-8 string column value as `&str` without copying.
/// The `Utf8` dtype guarantees valid UTF-8, so this only validates.
pub(crate) fn buf_as_str(buf: &[u8]) -> Result<&str> {
    std::str::from_utf8(buf).map_err(|e| {
        VortexRdfError::Deserialization(format!("Invalid UTF-8 in string column: {}", e))
    })
}

/// Parses a stream of RDF quads from any reader using the specified RDF format.
pub fn parse_quads_from_reader<R: std::io::Read + Send + 'static>(
    reader: R,
    format: RdfFormat,
) -> impl Stream<Item = Result<Quad>> {
    let parser = RdfParser::from_format(format);
    let iter = parser.for_reader(reader).map(|x| {
        x.map_err(|e| VortexRdfError::Deserialization(format!("Parse error: {}", e)))
    });
    stream::iter(iter)
}

/// Helper function to generate a stream of mock RDF quads for benchmark and test workflows.
/// Generates triples evenly distributed across 10 named graphs.
pub fn generate_rdf_data_stream(size: usize) -> impl Stream<Item = Result<Quad>> {
    const EX: &str = "http://example.org/";
    const NUM_GRAPHS: u64 = 10;

    stream::iter((0..size).map(|i| {
        let subject = NamedOrBlankNode::NamedNode(
            NamedNode::new_unchecked(format!("{}subject/{}", EX, i))
        );
        let predicate = NamedNode::new_unchecked(format!("{}predicate/{}", EX, i % 100));
        let object = Term::NamedNode(
            NamedNode::new_unchecked(format!("{}object/{}", EX, i % 50))
        );
        let graph = GraphName::NamedNode(
            NamedNode::new_unchecked(format!("{}graph/{}", EX, (i as u64) % NUM_GRAPHS))
        );

        Ok(Quad::new(subject, predicate, object, graph))
    }))
}
