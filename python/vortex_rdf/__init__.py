"""Python bindings for Vortex-RDF: file-backed columnar RDF stores.

This package is the native binding layer. For querying `.vortex` files with
SPARQL through rdflib, use the `vortex-rdflib` package, which builds its
rdflib Store on top of these bindings.
"""

from ._native import (
    CancelToken,
    Cancelled,
    CodeChunks,
    CodeGroups,
    JoinTooLarge,
    ProbeAnswers,
    TermDict,
    U64Column,
    VortexRdfError,
    VortexRdfStore,
    __version__,
    engine_counters,
    serialize_rdf,
)

# The join's pair count, read from the exception's `args`.
JoinTooLarge.pairs = property(lambda self: self.args[1])

__all__ = [
    "CancelToken",
    "Cancelled",
    "CodeChunks",
    "CodeGroups",
    "JoinTooLarge",
    "ProbeAnswers",
    "TermDict",
    "U64Column",
    "VortexRdfError",
    "VortexRdfStore",
    "__version__",
    "engine_counters",
    "serialize_rdf",
]
