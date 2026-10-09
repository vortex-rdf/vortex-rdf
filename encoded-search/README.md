# vortex-rdf-encoded-search
[![Crates.io](https://img.shields.io/crates/v/vortex-rdf-encoded-search.svg)](https://crates.io/crates/vortex-rdf-encoded-search)

Bounds search and point access over compressed [Vortex](https://github.com/spiraldb/vortex)
arrays, without decoding them.

Vortex's generic `search_sorted` is correct for every encoding, but it goes
through the compute kernel machinery: an `ExecutionCtx`, scalar boxing, and —
for encodings without a specialised kernel — canonicalization of the array
being searched. When the column is a sorted, non-nullable unsigned integer
column (a dictionary code column, a row-id column, an index key), that adds significan overhead to answer  queries like "where does value `v` start and end".

This crate resolves such an array once into a borrowed tree of typed probe
nodes, then answers queries against the compressed representation directly:
slice reads, integer arithmetic, and single bit-packed word extraction. No
`ExecutionCtx`, no scalars, no canonicalization, no allocation per query.

It is not RDF-specific: it is independent from
[vortex-rdf](https://github.com/vortex-rdf/vortex-rdf), which uses it to probe
its sorted code columns.

## Example

```rust
use vortex_array::arrays::PrimitiveArray;
use vortex_array::scalar_fn::session::ScalarFnSession;
use vortex_array::session::ArraySession;
use vortex_array::{IntoArray, VortexSessionExecute};
use vortex_btrblocks::BtrBlocksCompressorBuilder;
use vortex_btrblocks::schemes::integer::{BitPackingScheme, RunEndScheme, SequenceScheme};
use vortex_rdf_encoded_search::SortedProbe;
use vortex_session::VortexSession;

let session = VortexSession::empty()
    .with::<ArraySession>()
    .with::<ScalarFnSession>();
let mut ctx = session.create_execution_ctx();
let compressor = BtrBlocksCompressorBuilder::empty()
    .with_new_scheme(&RunEndScheme)
    .with_new_scheme(&SequenceScheme)
    .with_new_scheme(&BitPackingScheme)
    .build();

// A sorted column with 11-row runs, compressed into whatever encoding the
// cascade picks for it (here: run-end over a sequence of run ends and a
// sequence of values).
let data: Vec<u32> = (0..2_097_152).map(|i| (i / 11) as u32).collect();
let canonical = PrimitiveArray::from_iter(data.iter().copied()).into_array();
let encoded = compressor.compress(&canonical, &mut ctx)?;

// Resolve once; probe many times.
let probe = SortedProbe::resolve(&encoded).expect("supported encoding tree");

assert_eq!(probe.bounds(1_000), (11_000, 11_011));
assert_eq!(probe.value_at(11_000), 1_000);

// Restrict a search to a window — useful when a column is sorted only
// within the run of a preceding key.
assert_eq!(probe.bounds_in(11_000..22_000, 1_500), (16_500, 16_511));
# Ok::<(), vortex_error::VortexError>(())
```

`resolve` returns `None` rather than failing: any array it does not support —
a nullable dtype, a signed or floating dtype, a non-host buffer, an encoding
outside the supported set — declines, and the caller falls back to its generic
search path. Supporting a new encoding is therefore always additive.

If the probe must outlive the borrow of the array (caching it beside the data,
for instance), `OwnedSortedProbe` holds the `ArrayRef` and the probe tree
together in one self-referential value.

## Supported encodings

`Primitive`, `Constant`, `Sequence`, `RunEnd`, `FoR` (with one reference for
the whole array), `BitPacked` (with one bit width, including its patches),
`Delta` (FastLanes delta, decoded one 1,024-value block at a time on first
touch), `Slice`, `Chunked`, and `Dict`, composed arbitrarily; the transparent
`Shared` wrapper resolves to whatever it wraps. `NodeKind` reports the resolved
tree's shape for tests and diagnostics. Frame-of-reference with a reference per
1,024-value chunk (the `fastlanes.for.v2` wire form) and bit-packing with a
width per block decline. BtrBlocks' frame-of-reference scheme takes per-chunk
references whenever its builder allows `fastlanes.for.v2`: `empty()` allows
every wire form, `from_session` the ones the session's enabled editions
include.

## The sortedness contract

`lower_bound`, `upper_bound`, and `bounds` require the array to be sorted
ascending. This is a caller contract, exactly as it is for
`slice::partition_point`: an unsorted array yields an unspecified — but never
panicking, never unsound — answer. `bounds_in` requires only its window to be
sorted, and never reads a row outside it. `value_at` is exact regardless of
order.

## The `layout` feature

Off by default. With it, `ColumnChunks` extends the same treatment to a column
inside a written Vortex file: it locates one field's flat chunk leaves in a
layout tree, then answers global bounds queries and point reads by fetching
only the chunks a binary search over chunk extremes actually touches. Each
fetched chunk is reconstructed in its wire encoding (`SerializedArray::decode`
rebuilds metadata over the segment buffers — it does not decompress), resolved
into a probe once, and cached, so repeated queries against a hot file do no
further I/O.

A column the writer dictionary-encoded at the layout level (a `vortex.dict`
node — a values leaf beside a codes subtree, possibly one of a chunked run of
such dictionaries) is probed through its codes leaves: each is composed with
the dictionary's values, fetched once and shared, into a dictionary array the
probe resolves like any other. Because the probe's dictionary node bisects the
decoded values, the order the writer assigned codes in never matters.

```console
cargo add vortex-rdf-encoded-search --features layout
```

## Performance

Measured against Vortex 0.85 with [`benches/probe.rs`](./benches/probe.rs) (fastest of 100
samples, Intel Core Ultra 7 155H, 2026-08). One two-sided `bounds` query on a
2M-row column, by fixture:

| Fixture | `SortedProbe::bounds` | Vortex `search_sorted` | Decoded `Vec<u32>` (`partition_point`) |
|---|---|---|---|
| `runend_2m` | 0.69 µs | 249 µs | 0.08 µs |
| `for_bitpacked_2m` | 0.39 µs | 14.4 µs | 0.07 µs |
| `dict_2m` | 0.31 µs | 13.2 µs | 0.07 µs |

The gap against the generic kernel is widest on `runend_2m` (~360×), where
Vortex has no specialised `search_sorted` kernel and canonicalizes all 2M rows
on every call; where a kernel does exist it is ~40×. Either way the probe stays
within 4–9× of a fully decoded binary search while leaving the column
compressed, and resolving one costs 130–240 ns, once. The figures are refreshed on every new release.

Reproduce with:

```console
cargo bench -p vortex-rdf-encoded-search --bench probe
```

## Compatibility

Each release tracks Vortex new releases. This release is built
against Vortex `0.85`.

Minimum supported Rust version: 1.95 (edition 2024).

## License

MIT. See [LICENSE](https://github.com/vortex-rdf/vortex-rdf/blob/main/encoded-search/LICENSE).
