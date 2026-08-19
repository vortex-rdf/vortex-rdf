"""Type stubs for the private native extension module."""

import os
from typing import List, Optional, Sequence, Tuple, Union

__version__: str

# `serialize_rdf`'s path arguments are `PathBuf` on the Rust side, so any
# `os.PathLike[str]` is accepted alongside `str`.
StrPath = Union[str, "os.PathLike[str]"]

class VortexRdfError(Exception):
    """Raised when a Vortex-RDF store operation fails."""

class TermDict:
    def decode(self, code: int) -> Optional[str]: ...
    def decode_many(
        self,
        # Preferably a u32 buffer (``memoryview(col).cast("I")``,
        # ``array("I", ...)``, a uint32 NumPy array) or the raw byte view a
        # `U32Column` exports, read in one bulk copy; any int sequence also
        # works, at one Python-int extraction per code.
        codes: Union[Sequence[int], memoryview, bytes, bytearray],
    ) -> List[Optional[str]]: ...
    def __len__(self) -> int: ...

class U32Column:
    """Read-only u32 column; supports the buffer protocol
    (``memoryview(col).cast("I")`` is a zero-copy view)."""

    def __len__(self) -> int: ...
    def __buffer__(self, flags: int, /) -> memoryview: ...

class VortexRdfStore:
    def __init__(
        self,
        path: str,
        max_resident_bytes: Optional[int] = None,
        in_memory: bool = False,
    ) -> None: ...
    @staticmethod
    def from_bytes(data: bytes) -> "VortexRdfStore": ...
    def to_bytes(self) -> bytes: ...
    def layout(self) -> str: ...
    def __len__(self) -> int: ...
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
    input_path: StrPath,
    output_path: StrPath,
    layout: str = "default",
    format: Optional[str] = None,
    indexes: List[str] = [],
) -> None: ...
