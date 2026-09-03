"""Type stubs for the private native extension module."""

import os
from typing import List, Mapping, Optional, Protocol, Sequence, Tuple, Union

__version__: str

# Path arguments are `PathBuf` on the Rust side, so any `os.PathLike[str]` is
# accepted alongside `str`.
_StrPath = Union[str, "os.PathLike[str]"]
class _ArrowArray(Protocol):
    """Anything speaking the Arrow PyCapsule array interface — a
    `pyarrow.Array`, a code column read back out of `match_arrow`."""

    def __arrow_c_array__(self, requested_schema: object = None) -> Tuple[object, object]: ...

# Term codes in any form `TermDict.decode_many` accepts: a u32 buffer, a
# `uint32` Arrow array, or a sequence of ints.
_Codes = Union[Sequence[int], memoryview, bytes, bytearray, "U32Column", _ArrowArray]
# A quad pattern as four optional N-Triples term strings.
_Pattern = Tuple[Optional[str], Optional[str], Optional[str], Optional[str]]

class VortexRdfError(Exception):
    """Raised when a Vortex-RDF store operation fails."""

class TermDict:
    def decode(self, code: int) -> Optional[str]:
        """The N-Triples string for `code`, or None when the code is out of
        this dictionary's range."""
        ...
    def encode(self, term: str) -> Optional[int]:
        """The code of the term `term`, or None when the dictionary does not
        hold it; the inverse of `decode`. Looked up as spelled, then in its
        canonical N-Triples form (an ``xsd:string``-typed literal is a plain
        one, escapes normalize)."""
        ...
    def encode_many(self, terms: Sequence[str]) -> List[Optional[int]]:
        """`encode` for many terms in one GIL-released call, in input order."""
        ...
    def lower_bound(self, term: str) -> int:
        """The first code whose term is ``>= term`` in byte order (``len(self)``
        when every term is smaller): ``lower_bound(a)..lower_bound(b)`` is
        exactly the codes of the terms in ``a..b``."""
        ...
    def prefix_range(self, prefix: str) -> Tuple[int, int]:
        """The half-open code range ``(lo, hi)`` of the terms whose N-Triples
        spelling starts with ``prefix`` — an IRI namespace as ``"<http://…/"``,
        a kind as its first byte. Pass it as ``range(lo, hi)`` to
        ``match_arrow(keep=...)``."""
        ...
    def filter_codes(self, kind: str, arg: str) -> Tuple[U32Column, U32Column]:
        """The codes for which the term predicate ``kind`` with argument
        ``arg`` is definitely true, and the codes it cannot decide — two
        ascending code columns; the rest are definitely false. Kinds:
        ``is_literal``, ``is_iri``, ``is_blank``, ``datatype``, ``lang``,
        ``lang_matches``, ``str_prefix``, ``num_lt``/``num_le``/``num_gt``/
        ``num_ge``/``num_eq``/``num_ne``. Memoized per predicate."""
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
    def __arrow_c_array__(self, requested_schema: object = None) -> Tuple[object, object]:
        """The Arrow PyCapsule interface: the whole dictionary as a
        ``string_view`` array whose element ``i`` is the term of code ``i``
        (``pyarrow.array(dictionary)``). ``requested_schema`` is not applied."""
        ...

class U32Column:
    """Read-only u32 column; supports the buffer protocol
    (``memoryview(col).cast("I")`` is a zero-copy view) and the Arrow
    PyCapsule interface (``pyarrow.array(col)`` shares the same memory)."""

    def __len__(self) -> int: ...
    def __repr__(self) -> str: ...
    # The buffer protocol is implemented natively (`__getbuffer__`);
    # `__buffer__` is the Python 3.12+ spelling `memoryview()` reports for it.
    def __buffer__(self, flags: int, /) -> memoryview: ...
    def __arrow_c_array__(self, requested_schema: object = None) -> Tuple[object, object]:
        """The column as a zero-copy ``uint32`` Arrow array; ``requested_schema``
        is not applied."""
        ...

class ArrowQuadStream:
    """The record batches of one `VortexRdfStore.match_arrow` call, handed to
    an Arrow consumer through the PyCapsule interface — consumable once
    (``pyarrow.RecordBatchReader.from_stream(stream)``, ``polars.DataFrame(stream)``,
    a DuckDB query over it); the schema is readable any number of times
    (``pyarrow.schema(stream)``)."""

    @property
    def encoding(self) -> str:
        """The term encoding of the batches: "codes", "terms" or "strings"."""
        ...
    def __arrow_c_schema__(self) -> object: ...
    def __arrow_c_stream__(self, requested_schema: object = None) -> object:
        """The batches as an ``arrow_array_stream`` capsule; a second call
        raises ``ValueError``. ``requested_schema`` is not applied."""
        ...
    def __repr__(self) -> str: ...

class VortexRdfStore:
    def __init__(
        self,
        path: _StrPath,
        max_resident_bytes: Optional[int] = None,
        in_memory: bool = False,
        dictionary: Optional[str] = None,
    ) -> None:
        """Open ``path``: file-backed and lazy by default, wholly in memory
        with ``in_memory=True``. ``dictionary`` — ``"as-written"`` or
        ``"plaintext"`` — picks the resident form of an in-memory store's
        term dictionary: the file's own FSST chunks, decoded one term per
        read, or the column decoded once into one canonical form that every
        probe, decode and Arrow export then reads in place. Only with
        ``in_memory=True``; ``ValueError`` otherwise."""
        ...
    @staticmethod
    def from_bytes(data: bytes, dictionary: Optional[str] = None) -> "VortexRdfStore":
        """Open native-container bytes as an in-memory store; ``dictionary``
        as for ``__init__``."""
        ...
    def to_bytes(self) -> bytes: ...
    def layout(self) -> str: ...
    def indexes(self) -> List[str]:
        """The store's secondary indexes as kebab-case names
        ("secondary-by-copy", "secondary-by-reference")."""
        ...
    def __len__(self) -> int: ...
    def __repr__(self) -> str: ...
    def term_dict(self) -> Optional[TermDict]: ...
    def count_quads_many(self, patterns: Sequence[_Pattern]) -> List[int]:
        """``count_quads`` for a batch of ``(s, p, o, g)`` patterns, evaluated
        like ``match_arrow_many``."""
        ...
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
        *,
        limit: Optional[int] = None,
    ) -> int:
        """Number of quads matching the pattern, counted from the row
        selection; no term is materialized. With ``limit`` the count stops
        there (``limit=1`` is an existence test reading one row)."""
        ...
    def match_arrow(
        self,
        s: Optional[str] = None,
        p: Optional[str] = None,
        o: Optional[str] = None,
        g: Optional[str] = None,
        *,
        encoding: str = "codes",
        projection: Optional[Sequence[str]] = None,
        keep: Optional[Mapping[str, Union[range, _Codes]]] = None,
        limit: Optional[int] = None,
        offset: int = 0,
    ) -> ArrowQuadStream:
        """The matching rows as a stream of Arrow record batches (columns
        ``s``, ``p``, ``o``, ``g``, or the ``projection`` subset in that order).

        ``encoding`` is ``"codes"`` (``uint32`` term codes sharing the store's
        buffers), ``"terms"`` (the codes as dictionary keys over the whole term
        dictionary) or ``"strings"`` (``string_view`` N-Triples strings, the
        one encoding every layout serves). Codes and terms need the
        Dictionary layout; the TypedObject layout has no Arrow export.

        ``keep`` restricts positions by code before any row is gathered: a
        mapping from column name (``"s"``, ``"p"``, ``"o"``, ``"g"``) to a
        ``range`` of codes (what ``TermDict.prefix_range`` yields) or to a
        set of codes in any form ``TermDict.decode_many`` accepts, a
        ``uint32`` Arrow array included. ``limit``/``offset`` window the rows
        in match order, after ``keep``."""
        ...
    def match_arrow_many(
        self,
        patterns: Sequence[_Pattern],
        *,
        encoding: str = "codes",
        projection: Optional[Sequence[str]] = None,
    ) -> List[ArrowQuadStream]:
        """``match_arrow`` for a batch of ``(s, p, o, g)`` patterns: every
        pattern is parsed first (a malformed one raises ``ValueError`` before
        anything is evaluated), the matches run concurrently under one GIL
        release, one stream per pattern in input order, all under the same
        ``encoding`` and ``projection``."""
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
