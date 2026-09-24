"""Type stubs for the private native extension module."""

import os
from typing import List, Optional, Sequence, Tuple, Union
from pyarrow 

__version__: str

# Path arguments are `PathBuf` on the Rust side, so any `os.PathLike[str]` is
# accepted alongside `str`.
_StrPath = Union[str, "os.PathLike[str]"]

class VortexRdfError(Exception):
    """Raised when a Vortex-RDF store operation fails."""

class TermDict:
    def decode(self, code: int) -> Optional[str]:
        """The N-Triples string for `code`, or None when the code is out of
        this dictionary's range."""
        ...
    def encode(self, term: str) -> Optional[int]:
        """The code of the N-Triples term `term`, or None when the dictionary
        does not hold it; the inverse of `decode`."""
        ...
    def decode_many(
        self,
        codes: Union[Sequence[int], memoryview, bytes, bytearray],
    ) -> List[Optional[str]]:
        """Decode a batch of codes in one GIL-released call.

        A u32 buffer (``memoryview(col).cast("I")``, ``array("I", ...)``, a
        uint32 NumPy array) or the raw byte view a `U32Column` exports is read
        in one copy; any int sequence works element by element. Repeated codes
        share one string object."""
        ...
    def __len__(self) -> int: ...
    def __repr__(self) -> str: ...

class U32Column:
    """Read-only u32 column; supports the buffer protocol
    (``memoryview(col).cast("I")`` is a zero-copy view)."""

    def __len__(self) -> int: ...
    def __repr__(self) -> str: ...
    # The buffer protocol is implemented natively (`__getbuffer__`);
    # `__buffer__` is the Python 3.12+ spelling `memoryview()` reports for it.
    def __buffer__(self, flags: int, /) -> memoryview: ...

class VortexRdfStore:
    def __init__(
        self,
        path: _StrPath,
        max_resident_bytes: Optional[int] = None,
        in_memory: bool = False,
    ) -> None: ...
    @staticmethod
    def from_pyarrow(data:"pyarrow.RecordBatchReader") -> "VortexRdfStore"
    @staticmethod
    def from_bytes(data: bytes) -> "VortexRdfStore": ...
    def to_bytes(self) -> bytes: ...
    def layout(self) -> str: ...
    def indexes(self) -> List[str]:
        """The store's secondary indexes as kebab-case names
        ("secondary-by-copy", "secondary-by-reference")."""
        ...
    def __len__(self) -> int: ...
    def __repr__(self) -> str: ...
    def term_dict(self) -> Optional[TermDict]: ...
    def match_codes(
        self,
        s: Optional[str] = None,
        p: Optional[str] = None,
        o: Optional[str] = None,
        g: Optional[str] = None,
    ) -> Optional[Tuple[U32Column, U32Column, U32Column, U32Column]]: ...
    def get_quads(
        self,
        s: Optional[str] = None,
        p: Optional[str] = None,
        o: Optional[str] = None,
        g: Optional[str] = None,
    ) -> List[Tuple[str, str, str, str]]:
        """Matching quads as (subject, predicate, object, graph) N-Triples
        strings; the default graph is the empty string."""
        ...
    def count_quads(
        self,
        s: Optional[str] = None,
        p: Optional[str] = None,
        o: Optional[str] = None,
        g: Optional[str] = None,
    ) -> int:
        """Number of quads matching the pattern, counted from the row
        selection; no term is materialized."""
        ...
    def match_columns(
        self,
        s: Optional[str] = None,
        p: Optional[str] = None,
        o: Optional[str] = None,
        g: Optional[str] = None,
    ) -> Tuple[List[str], List[str], List[str], List[str]]:
        """The same rows as `get_quads`, as four parallel columns."""
        ...

def serialize_rdf(
    input_path: _StrPath,
    output_path: _StrPath,
    *,
    format: Optional[str] = None,
    layout: str = "dictionary",
    indexes: Sequence[str] = ...,
) -> None:
    """Serialize an RDF file into a `.vortex` store file.

    The options after the two paths are keyword-only. `format` is detected
    from the input file extension when omitted; `layout` defaults to
    `"dictionary"`, the default shared by the JS bindings and the CLI.
    """
    ...
