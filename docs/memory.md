# Resident memory: the store's forms, and every cache it keeps

What a `VortexRdfStore` holds in memory depends on where its base came from,
and on what has been read from it since. This document names the three
forms, says what each holds resident, and lists every cache and memo a store
creates: what fills it, what bounds it, and when it is freed. It is the
reference for "why is this process holding this much memory" and for "what
does this read leave behind".

Related: [serialization.md §10](serialization.md#10-adopting-a-build-in-memory)
(how a build becomes a resident store), [matching.md §6](matching.md#6-the-in-memory-path)
(how the in-memory forms are searched), [arrow.md §3.3](arrow.md#33-what-is-and-is-not-copied)
(what the Arrow surface copies).

## 1. The three forms

A store is a base — an array in memory, or a file read on demand — plus a
view over it (a row selection, tombstones, an append tail). The base's form
is fixed by its provenance:

| Form | Comes from | The base | Index components | Dictionary |
|---|---|---|---|---|
| **Built** | [`from_quads`](../core/src/store/mod.rs#L198), [`from_built`](../core/src/store/mod.rs#L264), compaction's rebuild ([`from_raw_quads`](../core/src/store/mutation/compaction.rs#L146)) | one struct whose `u32` code columns are flat canonical primitives ([`with_canonical_int_children`](../core/src/store/array.rs#L289)) | compressed into probe-supported encodings — `Constant`, `RunEnd`, bit-packed ([`with_compressed_int_children`](../core/src/store/array.rs#L311)) | one canonical `string_view` column, as the builder froze it ([`from_sorted_column`](../core/src/store/layouts/dictionary/term_dict.rs#L350)); FSST windows are made at write ([`fsst_windows`](../core/src/store/layouts/dictionary/term_dict.rs#L452)) |
| **Adopted** | [`from_bytes`](../core/src/store/persist/open.rs#L266), [`from_parts`](../core/src/store/mod.rs#L237) — the bindings' `in_memory=True` / `fromBytes` | the writer's own encodings, as refcounted views into the file bytes ([`with_searchable_int_children`](../core/src/store/array.rs#L278)) | `from_parts`: the writer's encodings, made probeable ([`into_searchable`](../core/src/store/indexes/components.rs#L365)); `from_bytes`: deferred, un-executed until first use | decoded once into one canonical column — the bindings' default, `dictionary='plaintext'` — or, as written, the FSST chunks inside the bytes, anything else canonicalized ([`DictForm`](../core/src/store/layouts/dictionary/term_dict.rs#L88)) |
| **File-backed** | [`from_file`](../core/src/store/persist/open.rs#L131) | nothing resident — every read scans the file and is transient (the file is opened with no decoded-data cache) | on disk, resolved through pushed-down scans and cached chunk probes | resident, or left in the file and point-read by leaf when it outweighs the residency budget |

The rule behind the split is provenance, not policy. Where the store makes
the columns itself they are canonical, because that is what every code
read hands out zero-copy. Where the columns arrive already encoded — inside
bytes the store holds anyway, or inside a file — they stay encoded, because
decoding them would add a copy of every column on top of the bytes, and
the match paths bind either form through the encoded-search probes at the
same speed. Index components are compressed in every form: the probes bind
them as they are, and the one read that wants a component's columns as a
payload — the code export of a served run — decodes them into the
component's own live canonical cache ([§3.7](#37-index-components)).

The dictionary follows the same rule with one difference: nothing binds a
compressed dictionary at the speed of a plaintext one. Every probe, every
decode, every predicate pass and every `terms` export reads it, and an FSST
chunk decodes on each of those reads, so a built dictionary is held as the
canonical column the builder froze, and the bindings' in-memory opens
decode an adopted dictionary into the same form up front by default
(`dictionary='plaintext'`); `'as-written'` keeps the file's FSST chunks and
decodes on demand, the lean load.

The code columns have the same two forms, chosen beside the dictionary's
([`ResidentForm`](../core/src/store/persist/forms.rs#L57)): as written, the
default of the Rust constructors, keeps the writer's encodings and decodes a
column into the live canonical form only for a wide read and only while some
holder keeps it; `codes='canonical'`, the bindings' in-memory default, decodes
the base's columns once into the form a built base holds — every code read
and every Arrow export a slice of them, with nothing to hold — and pins each
index component's live canonical form once a served read fills it. The
canonical columns are 16 B/row; measured at 1M quads they replace the file
bytes they make redundant rather than add to them ([§1.1](#11-measured)).

### 1.1 Measured

1,048,576 quads of the comparative benchmark dataset (629,199 distinct
terms), Dictionary layout, no index, exact heap bytes from a counting
allocator ([`dict_memory`](../core/examples/dict_memory.rs)), one process,
2026-09-03 (the code-read rows: 2026-09-02, unchanged by the dictionary):

| | Built | Adopted as written | Adopted plaintext | Adopted plaintext, canonical codes |
|---|---|---|---|---|
| retained after construction | 57.7 MiB | 19.5 MiB — 16.9 MiB of file bytes, 2.6 MiB of probes and dictionary state | 57.7 MiB — the same 19.5 MiB plus the decoded column | 56.7 MiB — the canonical columns are the last thing that read the file bytes, so the bytes go |
| of which the base's code columns | 16.0 MiB canonical (16 B/row) | 4.5 MiB in the writer's encodings, inside the bytes | the same | 16.0 MiB canonical (16 B/row), in place of the 16.9 MiB of file bytes they make redundant |
| of which the dictionary | 38.2 MiB canonical (64 B/term: 16 B of view, 48 B of term bytes) | 12.9 MiB FSST (21.5 B/term), inside the bytes | 38.2 MiB canonical, plus the 12.9 MiB of FSST chunks left inside the bytes, which nothing reads any more | 38.2 MiB canonical |
| construction | 2.3 s (sort, intern) | 3.0 ms | 14.0 ms (the 3.0 ms lift plus one bulk decode) | 11.2 ms (the lift, one bulk decode of the dictionary and one of the code columns) |
| whole-store code read | 6 µs, retains nothing | 2.3 ms; +16.0 MiB while the result is held, +0 after it is dropped | the same | +0: its own buffers, retains nothing |
| a second whole-store read while the first is held | 6 µs | 1.6 µs, +0 (shares the first) | the same | +0 |
| the dictionary as an Arrow array | +0: its own buffers | +40.7 MiB while held (68 B/term), +0 after | +0: its own buffers | +0: its own buffers |

The forms differ by what is held canonical. A built store carries the
canonical copy of its code columns always, a store adopted as written only
while some reader holds it, one adopted with canonical codes always — and
where nothing else keeps the file bytes (a plaintext dictionary, no index),
those 16 B/row take the place of the 16.9 MiB of bytes they make redundant,
so the store ends up 1 MiB smaller than the as-written one and every code
read is a slice; with an index child adopted from the bytes they stay, and
the canonical columns are 16 MiB on top. A canonical dictionary is 25 MiB
more than its FSST chunks, and buys the `terms` export, every probe, decode
and predicate pass a read in place — so a store that exports terms at all
is smaller with it (57.7 MiB steady, against 19.5 + 40.7 MiB while an
as-written store's export is held), and a store read only through codes is
38 MiB larger. On the pushdown and Arrow workloads every store is read
wide, so the canonical forms' constant cost is the cheaper steady state —
the bindings' in-memory default on both counts; an adopted store that is
opened, queried and dropped never pays it at all.

## 2. What a read leaves behind

Nothing a read allocates outlives the result it returns, with the
exceptions listed in [§3](#3-the-caches-and-memos) — and none of those is
proportional to the rows a read returns. Per operation:

| Operation | Built base | Adopted base | File-backed |
|---|---|---|---|
| `match_pattern`, `size`, `count` | nothing (the probes are resolved at construction) | nothing | the file handle's chunk and bind caches ([§3.6](#36-the-file-handle)) |
| point-sized code read (≤ 256 rows) | zero-copy slices, or a gather of the rows | point reads through the probes; no column decoded | point reads through the cached chunk probes |
| contiguous wide code read (`s`/`sp`/`spo` prefix, whole store, served run) | zero-copy slices of the base | one decode per column into the live canonical form ([§3.2](#32-the-in-memory-base)), shared by every holder, freed with the last | a scan; the result owns its rows |
| scattered id read (a mask scan's ids, a tombstoned view) | a gather of the rows | a `take` over the encoded base — or a gather from the live form while some holder keeps it alive | a scan |
| `keep` | a binary search on a sorted prefix column, else a slice compare over the canonical column | the column through the live form; dropped with the scan unless held | a pushed-down filter |
| `quads` / `strings` reads | decoded rows, dropped with the stream | the same | the same |
| Arrow `codes` export | the same buffers as the code read above | the same | one decode per scan split, owned by the batch |
| Arrow `terms` export | the keys above, plus the dictionary's Arrow values ([§3.4](#34-the-dictionary)) held by the batches | the same | the same |
| `to_bytes` | the bytes; nothing retained | the same | the same |
| `window`, `match_pattern_many` | a selection; no rows | the same | the same |

## 3. The caches and memos

Every slot the store fills lazily, grouped by owner. *Bound* is what the
slot can grow to; *lifetime* is what drops it. Unless noted, a cache is
shared by every view derived from the same base (`Arc`), and a mutation
that builds a new base starts with empty caches.

### 3.1 Process-wide

| Slot | Holds | Filled by | Bound | Lifetime |
|---|---|---|---|---|
| [`VORTEX_SESSION`](../core/src/session.rs#L25) | the Vortex session: registered encodings and layouts, the Arrow conversion registry | first use | fixed | the process |
| [`AVAILABLE_PARALLELISM`](../core/src/io/read.rs#L23) | the scan concurrency | first use | one integer | the process |

### 3.2 The in-memory base

| Slot | Holds | Filled by | Bound | Lifetime |
|---|---|---|---|---|
| [`StructProbes`](../core/src/store/view/probes.rs#L19) | one resolved encoded-search probe per base column — the walk of the column's encoding tree, and per chunk the memoized first and last values ([`Chunk`](../encoded-search/src/node.rs#L72)) | construction ([`warm`](../core/src/store/view/probes.rs#L43)); the chunk extremes on the first bounds search that touches them | a few words per column and per chunk | the base; a fresh base takes a fresh cache |
| [`LiveCanonical`](../core/src/store/view/canonical.rs#L29) | the canonical `u32` form of an encoded base's code columns, one weak slot per column | a contiguous wide code read, or `keep`, over an adopted base ([`code_columns_shared`](../core/src/store/read/rows.rs#L247)) | 4 B/row per column, and only while some reader holds a buffer of it — every buffer handed out, and every slice of one, holds the decoded column; it is freed with the last of them, in Rust or across the C Data Interface alike | what its holders decide; never the base's. A built base never fills it, nor a base adopted with `codes='canonical'`, which is canonical itself |
| tombstones (`deleted`) | one bit per base row | the first delete | 1 bit/row | the base, until compaction |

### 3.3 Views

| Slot | Holds | Filled by | Bound | Lifetime |
|---|---|---|---|---|
| [`LazyRowIds`](../core/src/store/indexes/mod.rs#L313) (a [`ViewSelection::Pending`](../core/src/store/view/selection.rs#L49)) | the exact base row ids of an index-served match | the first consumer that needs exact ids — a base-order read, a count under tombstones, a chained match, a delete; never by iterating the served rows | 8 B per matched row | the view and its clones |
| [`FileServePlan::bound`](../core/src/store/indexes/serve.rs#L564) | the plan's bound projection and filter | the first read through the plan | two expression trees | the view |

### 3.4 The dictionary

| Slot | Holds | Filled by | Bound | Lifetime |
|---|---|---|---|---|
| [`ProbeCache`](../core/src/store/layouts/dictionary/term_dict.rs#L794) | term → code lookups, absence included, direct-mapped | every `encode` (a bound pattern term, `encode_many`) | [`PROBE_CACHE_SLOTS`](../core/src/store/layouts/dictionary/term_dict.rs#L784) = 256 entries, overwritten on collision | the dictionary |
| [`arrow_values`](../core/src/store/layouts/dictionary/term_dict.rs#L219) | for an FSST dictionary (adopted as written) the decoded term column as the buffers of one Arrow `string_view` array ([`ArrowValuesOwner`](../core/src/store/layouts/dictionary/term_dict.rs#L183]); a canonical dictionary — every built one, an adopted one in the plaintext form — hands out its own buffers and caches nothing | `to_arrow`, `__arrow_c_array__` on a term dictionary, every `terms` export | 16 B/term of views plus the term bytes (68 B/term measured), only while some array over it is held — a `terms` batch, a pyarrow array, a polars frame | freed with the last holder; rebuilt on the next use |
| [`PredicateMemo`](../core/src/store/layouts/dictionary/term_dict.rs#L289) | per term predicate, the codes it holds for and the codes it cannot decide ([`filter_codes`](../core/src/store/layouts/dictionary/term_dict.rs#L736)) | each distinct predicate asked | [`PREDICATE_MEMO_SLOTS`](../core/src/store/layouts/dictionary/term_dict.rs#L285) = 32 entries, oldest dropped first; an entry is at most 8 B per term | the dictionary |
| `DictSnapshot` handles (Python `term_dict()`, JS `termDict()`) | an `Arc` to the dictionary | the caller | — | keep the whole dictionary alive after the store is dropped |

### 3.5 The file-backed dictionary

| Slot | Holds | Filled by | Bound | Lifetime |
|---|---|---|---|---|
| [`TermChunks`](../core/src/store/layouts/dictionary/file_backed.rs#L46) leaves ([`ChunkSpec`](../core/src/store/layouts/dictionary/file_backed.rs#L58)) | each wire leaf of the `dictionary` child that a point read touched, in its wire encoding (one leaf = 64 Ki FSST terms) | the first decode or encode that lands in the leaf | up to the dictionary child's compressed size, if every leaf is touched | the store |
| [`FileBackedDict`](../core/src/store/layouts/dictionary/file_backed.rs#L231)'s bound projection | the bound scan projection of the term column | the first leaf read | one expression tree | the store |
| its `ProbeCache` | as [§3.4](#34-the-dictionary) | | 256 entries | the store |

### 3.6 The file handle

Shared by every view over a file-backed store
([`NativeStoreFile`](../core/src/store/persist/native_file.rs#L30)); a compaction
reopens the file and starts afresh.

| Slot | Holds | Filled by | Bound | Lifetime |
|---|---|---|---|---|
| `child_readers` | one Vortex layout reader per child (`quad-source`, `dictionary`, `index:*`) | the first scan of that child | Vortex's per-reader state: the layout tree, and the pruning statistics and evaluation caches it keys by bound-expression identity | the handle |
| `splits` | the root's scan splits | the first scan | one range per split | the handle |
| `column_chunks` ([`ColumnChunks`](../encoded-search/src/layout.rs#L89)) | per (child, column), the column's chunk leaves; per leaf, the wire-encoded chunk and its resolved probe ([`ChunkLeaf`](../encoded-search/src/layout.rs#L74)), and the values leaf a dictionary-encoded run shares ([`DictValues`](../encoded-search/src/layout.rs#L41)) | a subject or index probe, a point read: the leaves the bisection touches | up to the column's compressed size, if every leaf is touched | the handle |
| [`BoundExprMemo`](../core/src/store/persist/native_file.rs#L71) | bound filter and projection expressions, keyed by shape and schema | every pushed-down filter and projection | [`BIND_MEMO_MAX`](../core/src/store/persist/native_file.rs#L75) = 4096 entries; the map is cleared wholesale past it | the handle |
| segment cache | none — the file is opened with Vortex's no-op segment cache, so decoded data is never retained between reads | | | |

### 3.7 Index components

| Slot | Holds | Filled by | Bound | Lifetime |
|---|---|---|---|---|
| [`DeferredRows`](../core/src/store/indexes/components.rs#L217) | the rows of a component adopted un-executed by `from_bytes`, in searchable form | the first query routed to that index | the component's size: a full quad copy for `secondary-by-copy`, `{val, rid}` pairs for `secondary-by-reference` | the store |
| a component's [`StructProbes`](../core/src/store/view/probes.rs#L19) | as [§3.2](#32-the-in-memory-base), over the component's columns | construction, or the deferred materialization | a few words per column and chunk | the store |
| a component's [`LiveCanonical`](../core/src/store/view/canonical.rs#L29) | the canonical `u32` form of the component's code columns, one weak slot per column — pinned strongly under `codes='canonical'` | a served run's code export wider than a point read, when the cache is pinned, the run covers at least 1/32 of the component or a holder already keeps the column alive | 4 B/row per projected column: only while some batch holds it, or for the store's lifetime when pinned | what its holders decide, or the store's when pinned |

## 4. Keeping memory bounded

- **Hold results only as long as they are needed.** On a base adopted as
  written, an Arrow `codes` batch, or a polars frame built from one, is what
  keeps the canonical form of the columns alive; on a dictionary adopted as
  written, a `terms` batch or an exported dictionary is what keeps the
  decoded Arrow values alive (a canonical dictionary exports its own buffers
  and holds nothing extra). Two consumers alive at the same time share one
  copy; the memory returns with the last of them. Under the bindings'
  default `codes='canonical'` the base's columns are their own canonical
  form and nothing is held by a result.
- **Point reads never decode a column**, on any form: a selection of at
  most 256 rows is read point by point through the probes.
- **Scattered reads allocate their own result**, a `take` over the base;
  they neither fill nor need the live form.
- **What grows with use is bounded and small**: the term probe cache (256
  entries), the predicate memo (32 entries), the bind memo (4096
  entries). What grows with the data touched is bounded by the data: a
  deferred component materializes once, a file column's chunk cache and a
  file-backed dictionary's leaves fill only with the chunks a query lands
  in.
- **The build transient is not a cache.** Constructing a store peaks far
  above its resident size (hundreds of bytes per row during the sort and
  intern passes) and releases it when construction ends. On wasm, linear
  memory never shrinks, so that peak is what the page keeps.
- **A file-backed store holds no rows at all** between reads; its cost is
  per read, by design.

## 5. Source map

| Concern | Where |
|---|---|
| The resident forms: canonical base, compressed components, encoded adoption | [`core/src/store/array.rs`](../core/src/store/array.rs), [`mod.rs`](../core/src/store/mod.rs) ([`resident_built_parts`](../core/src/store/mod.rs#L167), [`from_parts`](../core/src/store/mod.rs#L237)) |
| The live canonical form of an encoded base | [`core/src/store/view/canonical.rs`](../core/src/store/view/canonical.rs), [`rows.rs`](../core/src/store/read/rows.rs) ([`code_columns`](../core/src/store/read/rows.rs#L192), [`code_columns_shared`](../core/src/store/read/rows.rs#L247), [`code_columns_gathered`](../core/src/store/read/rows.rs#L309)) |
| Point reads and gathers | [`core/src/store/scan/gather.rs`](../core/src/store/scan/gather.rs) ([`gather_by_point_reads`](../core/src/store/scan/gather.rs#L51)) |
| Probes | [`core/src/store/view/probes.rs`](../core/src/store/view/probes.rs), [`encoded-search/src/node.rs`](../encoded-search/src/node.rs) |
| Deferred row ids and serve plans | [`core/src/store/indexes/mod.rs`](../core/src/store/indexes/mod.rs), [`serve.rs`](../core/src/store/indexes/serve.rs), [`selection.rs`](../core/src/store/view/selection.rs) |
| The dictionary's caches | [`core/src/store/layouts/dictionary/term_dict.rs`](../core/src/store/layouts/dictionary/term_dict.rs), [`file_backed.rs`](../core/src/store/layouts/dictionary/file_backed.rs) |
| The file handle's caches | [`core/src/store/persist/native_file.rs`](../core/src/store/persist/native_file.rs), [`encoded-search/src/layout.rs`](../encoded-search/src/layout.rs) |
| Deferred components | [`core/src/store/indexes/components.rs`](../core/src/store/indexes/components.rs) |
| Tests | [`core/src/tests/resident.rs`](../core/src/tests/resident.rs) (what each form decodes, shares and frees), [`core/src/store/view/canonical.rs`](../core/src/store/view/canonical.rs) (the weak slot), [`python/tests/test_arrow.py`](../python/tests/test_arrow.py) (sharing across the PyCapsule boundary) |
