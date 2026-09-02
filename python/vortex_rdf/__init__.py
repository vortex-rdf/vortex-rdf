"""Python bindings for Vortex-RDF: file-backed columnar RDF stores.

This package is the native binding layer. For querying `.vortex` files with
SPARQL through rdflib, use the `vortex-rdflib` package, which builds its
rdflib Store on top of these bindings.
"""

from ._native import (
    ArrowQuadStream,
    TermDict,
    U32Column,
    VortexRdfError,
    VortexRdfStore,
    __version__,
    serialize_rdf,
)

__all__ = [
    "ArrowQuadStream",
    "TermDict",
    "U32Column",
    "VortexRdfError",
    "VortexRdfStore",
    "__version__",
    "serialize_rdf",
]
