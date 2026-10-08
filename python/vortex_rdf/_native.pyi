"""Type stubs for the private native extension module."""

import os
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

__version__: str

# Path arguments are `PathBuf` on the Rust side, so any `os.PathLike[str]` is
# accepted alongside `str`.
_StrPath = Union[str, "os.PathLike[str]"]
# Codes or indices: a U32Column, a u32 buffer, the raw byte view a U32Column
# exports, or any sequence of ints.
_U32s = Union["U32Column", Sequence[int], memoryview, bytes, bytearray]
# A keep per position: a code set (U32Column, u32 buffer, int sequence) or a
# code range (``range`` with step 1, or ``(lo, hi)``).
_KeepSpec = Dict[Union[str, int], Union[_U32s, range, Tuple[int, int]]]
# A probe of the batch calls: ``(s, p, o, g)`` or a dict with keys s, p, o,
# g, keep, limit, offset.
_Probe = Union[Tuple[Optional[str], Optional[str], Optional[str], Optional[str]], Dict[str, Any]]

class VortexRdfError(Exception):
    """Raised when a Vortex-RDF store operation fails."""

class TermDict:
    def decode(self, code: int) -> Optional[str]:
        """The N-Triples string for `code`, or None when the code is out of
        this dictionary's range."""
        ...
    def encode(self, term: str) -> Optional[int]:
        """The code of the term `term`, or None when the dictionary does not
        hold it; the inverse of `decode`.

        Tolerant of spelling: an IRI with or without angle brackets, a
        literal with escape variants or an explicit ``xsd:string`` type, an
        upper-case language tag, and the default graph as ``""``,
        ``default`` or ``[]`` resolve to the stored form's code. A malformed
        term raises ``ValueError``."""
        ...
    def encode_many(self, terms: Sequence[str]) -> List[Optional[int]]:
        """`encode` over a sequence of terms, in order, in one GIL-released
        call; a malformed term raises ``ValueError``."""
        ...
    def filter_codes(self, kind: str, arg: str, codes: _U32s) -> Tuple[U32Column, U32Column]:
        """`kind` over the candidate `codes` (sorted and unique, else
        ``ValueError``): ``(passed, undecided)``, both subsets of `codes`; a
        candidate in neither fails. Kinds: ``is_literal``, ``is_iri``,
        ``is_blank``, ``datatype``, ``lang``, ``lang_matches``,
        ``str_prefix``, ``num_lt``, ``num_le``, ``num_gt``, ``num_ge``,
        ``num_eq``, ``num_ne``. Nothing is memoized."""
        ...
    def prefix_range(self, prefix: str) -> Tuple[int, int]:
        """The half-open code range ``(lo, hi)`` of the terms whose
        N-Triples spelling starts with `prefix`: codes rank spellings in
        byte order, so a term kind (``"``, ``<``, ``_:``) and an IRI
        namespace are each one range."""
        ...
    def lower_bound(self, term: str) -> int:
        """The code of the first term not below `term` in byte order: a
        present term's own code, where an absent one would sort, or
        ``len(self)`` when every term is below it."""
        ...
    @property
    def file_backed(self) -> bool:
        """Whether terms are read from the store's file on demand rather
        than held in memory."""
        ...
    def decode_many(self, codes: _U32s) -> List[Optional[str]]:
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

    def __init__(self, values: _U32s) -> None:
        """A column holding `values`: another column (shared), a u32 buffer
        or raw byte view (one copy), or any sequence of ints."""
        ...
    def __len__(self) -> int: ...
    def __repr__(self) -> str: ...
    def distinct(self) -> "U32Column":
        """The distinct values, each at its first occurrence, in that
        order."""
        ...
    def value_counts(self) -> Tuple["U32Column", "U32Column"]:
        """``(values, counts)``: the distinct values in first-seen order and
        how often each occurs."""
        ...
    def take(self, indices: _U32s) -> "U32Column":
        """The values at `indices`, in that order; an index past the end
        raises ``IndexError``."""
        ...
    def join_indices(self, other: _U32s) -> Tuple["U32Column", "U32Column"]:
        """``(left_indices, right_indices)`` of the rows where this column's
        value equals `other`'s — an equi-join on the two as keys, in
        nested-loop order (this column's rows in order, each with its matches
        in `other` in their original order). Gather joined columns with
        `take`."""
        ...
    # The buffer protocol is implemented natively (`__getbuffer__`);
    # `__buffer__` is the Python 3.12+ spelling `memoryview()` reports for it.
    def __buffer__(self, flags: int, /) -> memoryview: ...

class VortexRdfStore:
    def __init__(self, path: _StrPath, *, in_memory: bool = False) -> None:
        """Open a `.vortex` file memory-mapped, or load it whole with
        ``in_memory=True``. A file store is memory-mapped: replace its file by
        renaming a new one over it, never by truncating or rewriting it in
        place (a reader of the mapping would be killed with SIGBUS)."""
        ...
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
    def term_dict(self) -> Optional[TermDict]:
        """The store's term dictionary, or None when the code path does not
        apply (a non-Dictionary layout, or an append tail). A dictionary left
        in the file is served by reading it on demand (`TermDict.file_backed`)."""
        ...
    def match_codes(
        self,
        s: Optional[str] = None,
        p: Optional[str] = None,
        o: Optional[str] = None,
        g: Optional[str] = None,
        *,
        keep: Optional[_KeepSpec] = None,
        limit: Optional[int] = None,
        offset: int = 0,
    ) -> Optional[Tuple[U32Column, U32Column, U32Column, U32Column]]:
        """The matching rows as four zero-copy u32 code columns ``(s, p, o,
        g)`` decodable through `term_dict`, or None when the code path does
        not apply.

        `keep` narrows the match inside the store: a dict from position
        (``"s"``, ``"p"``, ``"o"``, ``"g"`` or 0-3) to the codes to keep
        there, a code set (`U32Column`, u32 buffer or int sequence) or a code
        range (a ``range`` with step 1, or ``(lo, hi)``). `offset` and
        `limit` window the rows in base order; a filtered file scan stops at
        the first block that fills the window."""
        ...
    def match_codes_many(
        self, probes: Sequence[_Probe]
    ) -> List[Optional[Tuple[U32Column, U32Column, U32Column, U32Column]]]:
        """`match_codes` for a batch of probes in one GIL-released call,
        answering in input order. A probe is an ``(s, p, o, g)`` tuple of
        optional term strings or a dict with keys ``s``, ``p``, ``o``,
        ``g``, ``keep``, ``limit``, ``offset``. Every probe is parsed before
        any is evaluated (``ValueError`` first); the probes run concurrently."""
        ...
    def count_quads_many(self, probes: Sequence[_Probe]) -> List[int]:
        """`count_quads` for a batch of probes (see `match_codes_many`), in
        input order."""
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
        keep: Optional[_KeepSpec] = None,
        limit: Optional[int] = None,
    ) -> int:
        """Number of quads matching the pattern, counted from the row
        selection; no term is materialized. `keep` narrows as for
        `match_codes` (Dictionary layout only); `limit` caps the count and
        stops the read once that many rows are known to exist."""
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
