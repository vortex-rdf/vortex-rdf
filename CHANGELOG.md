# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-07-21

Initial release.

### Added

- `vortex-rdf-core`: a columnar RDF quad store built on [Vortex](https://docs.vortex.dev),
  with serialization to/from `.vortex` files and Vortex IPC streams.
- Three column layouts — `Default`, `TypedObject`, and `Dictionary` — trading off
  compression strategy and query characteristics.
- Two secondary index types — `SecondaryByReference` and `SecondaryByCopy` — for
  accelerating pattern matching beyond the primary sort order.
- Three ingestion builders — `UnsortedStream`, `SortedInMemory`, and `SortedStream`
  (out-of-core, spill-to-disk) — for building a store from a quad stream.
- `VortexRdfStore` query API: pattern matching, mutation (add/delete), and
  compaction, with row selections composing over both in-memory and file-backed
  stores.
- `vortex-rdf-cli`: a command-line interface for converting between RDF formats
  and Vortex-RDF, and for querying `.vortex` files.
- `vortex-rdf` (npm): WebAssembly bindings exposing a `VortexStore` with an
  RDF-JS-compatible `DatasetCore` interface.

[Unreleased]: https://github.com/vortex-rdf/vortex-rdf/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/vortex-rdf/vortex-rdf/releases/tag/v0.1.0
