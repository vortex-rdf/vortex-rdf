"""Type stubs for the private native extension module."""

import os
import sys
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

if sys.version_info >= (3, 12):
    from collections.abc import Buffer
else:
    from typing_extensions import Buffer

__version__: str

# Path arguments are `PathBuf` on the Rust side, so any `os.PathLike[str]` is
# accepted alongside `str`.
_StrPath = Union[str, "os.PathLike[str]"]
# Codes or indices: a U64Column, a buffer of u64 (or non-negative int64)
# items (``memoryview``, ``array``, a NumPy array, the raw byte view a
# U64Column exports), or any sequence of ints from 0 to 2**64 - 1. A buffer of
# other items (a ``cast("I")`` view, a u32 array) raises ``ValueError``.
_U64s = Union["U64Column", Sequence[int], Buffer]
# A keep per position: a code set (U64Column, u64 buffer, or a list of ints)
# or a code range (``range`` with step 1, or a 2-tuple ``(lo, hi)``, the
# half-open codes ``lo <= code < hi``). A 2-tuple is always a range, never a
# set: pass ``list(codes)``, not ``tuple(codes)``, for a set.
_KeepSpec = Dict[Union[str, int], Union[_U64s, range, Tuple[int, int]]]
# A probe of the batch calls: ``(s, p, o, g)`` or a dict with keys s, p, o,
# g, keep, limit, offset.
_Probe = Union[Tuple[Optional[str], Optional[str], Optional[str], Optional[str]], Dict[str, Any]]

class VortexRdfError(Exception):
    """Raised when a Vortex-RDF store operation fails."""

class TermDict:
    def decode(self, code: int) -> Optional[str]:
        """The N-Triples string for `code`, or None when the code is out of
        this dictionary's range. A code is an int from 0 to 2**64 - 1: a
        negative one, or one of 2**64 or more, raises ``OverflowError``."""
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
    def filter_codes(
        self,
        kind: str,
        arg: str,
        codes: _U64s,
        *,
        flags: str = "",
        case: Optional[str] = None,
        as_str: bool = False,
    ) -> Tuple[U64Column, U64Column]:
        r"""`kind` over the candidate `codes`: ``(passed, undecided)``, both
        ascending subsets of `codes`; a candidate in neither fails, and no
        candidates give two empty columns. `codes` must be sorted, unique and
        inside the dictionary, else ``ValueError`` (a code past the end
        included).

        String kinds (``str_prefix``, ``contains``, ``strstarts``,
        ``strends``, ``regex``) test a term's text. By default that is
        rdflib's ``string()`` of the term (string literals only: an IRI or a
        blank node fails); with `as_str` it is SPARQL ``STR()`` (an IRI's
        string, a literal's lexical form; a blank node, or a literal whose
        datatype rdflib normalizes, is undecided). `case` (``"lower"`` or
        ``"upper"``) then wraps that text, so ``contains`` with ``as_str=True,
        case="lower"`` is ``CONTAINS(LCASE(STR(?x)), c)``: ``STR()`` is read
        first and the case wrapper applies to its result (``LCASE`` of an IRI
        itself raises in SPARQL, which is why the order matters). The wrapper
        is Python's ``str.lower()``/``str.upper()``, decided on ASCII text
        only; a non-ASCII text is undecided.

        `arg` is the raw prefix for ``str_prefix``. For ``contains``,
        ``strstarts`` and ``strends`` it is the constant's strict N-Triples
        spelling (``"text"``, ``"text"@tag``, ``"text"^^<datatype>``,
        ``<iri>`` or ``_:label``): a malformed spelling raises
        ``ValueError``, a language-tagged constant needs the same tag on the
        text (compared as written), and a constant that is no string literal
        matches nothing. For ``regex`` it is the pattern as written, and
        `flags` (the one kind that takes them) are the SPARQL flags: as in
        rdflib, ``i``, ``s`` and ``m`` apply and any other letter is ignored,
        and the test is Python's ``re.search``.

        Only a subset of patterns is evaluated natively: literals, ``.``,
        classes (``[a-z]``, ``[^...]``), the shorthands ``\d \w \s \D \W \S
        \b \B``, the anchors ``^``, ``$`` and ``\A``, groups ``(...)`` and
        ``(?:...)``, alternation, and the quantifiers ``* + ? {n} {n,}
        {n,m}`` with their lazy forms. Any other pattern (backreferences,
        lookaround, inline flags, named groups, other escapes, whatever
        Python rejects or reads differently from the native engine, and any
        pattern whose compiled program is large, such as counted repetitions
        that multiply) leaves every text undecided, as does flag ``i`` on a
        pattern with a non-ASCII character. A pattern using the shorthands,
        ``\b``/``\B`` or flag ``i`` decides ASCII texts only; a pattern with
        ``$`` and without flag ``m`` leaves a text ending in a newline
        undecided; a pattern with ``\B`` leaves the empty text undecided.

        Other kinds: ``is_literal``, ``is_iri``, ``is_blank``, ``datatype``,
        ``lang``, ``lang_matches``, ``num_lt`` … ``num_ne``. ``lang_matches``
        is BCP 47 basic filtering, decided for ``*`` and for a range of ASCII
        letters, digits and hyphens (``en``, ``en-GB``); any other range,
        which rdflib may read differently (``en-*``, padding whitespace),
        leaves the language-tagged literals undecided. An unknown kind, an
        invalid argument or an option that does not apply to the kind raises
        ``ValueError``. Nothing is memoized."""
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
    def decode_many(self, codes: _U64s) -> List[Optional[str]]:
        """Decode a batch of codes in one GIL-released call.

        A `U64Column`, a u64 buffer (``memoryview(col).cast("Q")``,
        ``array("Q", ...)``, a uint64 NumPy array), a non-negative int64
        buffer or the raw byte view a `U64Column` exports is read in one
        copy; a buffer of other items (a ``cast("I")`` view, a u32 array)
        raises ``ValueError``. Any int sequence works element by element.
        A repeated code usually shares one string object: a small cache of
        recently decoded codes serves repeats, and a code it has dropped is
        decoded again as an equal, separate string."""
        ...
    def __len__(self) -> int: ...
    def __repr__(self) -> str: ...

class U64Column:
    """Read-only u64 column of term codes, row indices or counts; supports
    the buffer protocol (``memoryview(col).cast("Q")`` is a zero-copy view)."""

    def __init__(self, values: _U64s) -> None:
        """A column holding `values`: another column (shared), a buffer of
        u64 or non-negative int64 items or a raw byte view (one copy), or any
        sequence of ints from 0 to 2**64 - 1 (an int outside that range raises
        ``OverflowError``). A buffer of other items raises ``ValueError``."""
        ...
    def __len__(self) -> int: ...
    def __repr__(self) -> str: ...
    def distinct(self) -> "U64Column":
        """The distinct values, each at its first occurrence, in that
        order."""
        ...
    def value_counts(self) -> Tuple["U64Column", "U64Column"]:
        """``(values, counts)``: the distinct values in first-seen order and
        how often each occurs."""
        ...
    def take(self, indices: _U64s) -> "U64Column":
        """The values at `indices`, in that order; an index past the end
        raises ``IndexError``."""
        ...
    def join_indices(self, other: _U64s) -> Tuple["U64Column", "U64Column"]:
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
        place (a reader of the mapping would be killed with SIGBUS);
        ``serialize_rdf`` already writes that way (on Windows it fails until
        the store mapping the file, and every `TermDict` and `U64Column` taken
        from it, is dropped). The mapping lives as long as the store or any
        `TermDict` or `U64Column` taken from it. A file written by
        vortex-rdf 0.11 or earlier is refused (``VortexRdfError``): rebuild
        it from its RDF source with ``serialize_rdf``."""
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
        apply (a non-Dictionary layout). A dictionary left in the file is
        served by reading it on demand (`TermDict.file_backed`)."""
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
    ) -> Optional[Tuple[U64Column, U64Column, U64Column, U64Column]]:
        """The matching rows as four zero-copy u64 code columns ``(s, p, o,
        g)`` decodable through `term_dict`, or None when the code path does
        not apply.

        `keep` narrows the match inside the store: a dict from position
        (``"s"``, ``"p"``, ``"o"``, ``"g"`` or 0-3) to the codes to keep
        there, a code set (`U64Column`, u64 buffer or int sequence) or a code
        range (a ``range`` with step 1, or ``(lo, hi)``). `offset` and
        `limit` window the rows in base order; a filtered file scan stops at
        the first block that fills the window."""
        ...
    def match_codes_many(
        self, probes: Sequence[_Probe]
    ) -> List[Optional[Tuple[U64Column, U64Column, U64Column, U64Column]]]:
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
