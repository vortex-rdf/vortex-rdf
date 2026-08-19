# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased](https://github.com/vortex-rdf/vortex-rdf/compare/v0.5.0...HEAD)

### Added

- Trigger releases on v*.*.* tag pushes ([`6ed1eb3`](https://github.com/vortex-rdf/vortex-rdf/commit/6ed1eb35b40c38f7c789e13f72ca5d382cf9f643) by @julianrojas87)
- Embed the dictionary and a manifest as file metadata segments ([`4f482e3`](https://github.com/vortex-rdf/vortex-rdf/commit/4f482e34ceb2391abacb4f791afa8bd9f9da4351) by @julianrojas87)
- Track the embedded dictionary format in the bindings (python) ([`03ad85c`](https://github.com/vortex-rdf/vortex-rdf/commit/03ad85ca5271fb990a410d64f62c478e0c3913e4) by @julianrojas87)
- Adopt the native vortex-rdf.store.v1 container as the only format ([`a749aaf`](https://github.com/vortex-rdf/vortex-rdf/commit/a749aafa77776076c6caa8412b9b56ea4ac7894b) by @julianrojas87)
- Add checked term parsing and value dispatch, and fix escaped literals ([`128c2d1`](https://github.com/vortex-rdf/vortex-rdf/commit/128c2d1f9da1997d2c79ddff8a52d3801b1eb797) by @julianrojas87)
- Stabilize the instrumented JS suite and cover literal decoding (bench) ([`685e66e`](https://github.com/vortex-rdf/vortex-rdf/commit/685e66e56adc8be061bdd6465c2cae1f1c65fbf4) by @julianrojas87)
- Restructure around subsystems and prune the API (core) ([`e09d37d`](https://github.com/vortex-rdf/vortex-rdf/commit/e09d37dd18b94602cf931b8b02390fcf9e97fcce) by @julianrojas87)
- Canonical vocabulary, termDict door, owned byte loads (js) ([`8e365ee`](https://github.com/vortex-rdf/vortex-rdf/commit/8e365eea6f3f9d33d16038c4efe99b0494125ce4) by @julianrojas87)
- Bytes round-trip, index building, buffer decode (python) ([`944142e`](https://github.com/vortex-rdf/vortex-rdf/commit/944142e92108d2be557b4a015342104457b0fb5d) by @julianrojas87)
- Subcommand modules and split match formats (cli) ([`e036435`](https://github.com/vortex-rdf/vortex-rdf/commit/e036435465e62203f7ecaef245a5834b470f451c) by @julianrojas87)
- Order the serialize panel fastest to slowest (bench) ([`39f4862`](https://github.com/vortex-rdf/vortex-rdf/commit/39f48621d4eaf575806289f57cc8335e1d8c459a) by @julianrojas87)
- Replace match_triples with get_quads and serve matches from term codes (python) ([`d6beae1`](https://github.com/vortex-rdf/vortex-rdf/commit/d6beae10fbda145ec81f2ea7012708aa235893b7) by @julianrojas87)
- Compare the Python bindings against four RDF libraries (bench) ([`4e54368`](https://github.com/vortex-rdf/vortex-rdf/commit/4e54368f3b10635fc94001868e9dfd30612f6641) by @julianrojas87)
- Carry the graph in match_compact and serve it from term codes (python) ([`6ef2a45`](https://github.com/vortex-rdf/vortex-rdf/commit/6ef2a4500e530c49de03bd578349ab8c995a0ec4) by @julianrojas87)
- Cross residency with the secondary index in the Python tab (bench) ([`080201c`](https://github.com/vortex-rdf/vortex-rdf/commit/080201c5c177908e760438deb1a079ec4620afc1) by @julianrojas87)
- Add workspace crate with encoded sorted-probe resolution (encoded-search) ([`ce3aef8`](https://github.com/vortex-rdf/vortex-rdf/commit/ce3aef8ce778b599d1ef0ad85cb761be02e9c58c) by @julianrojas87)
- Probe encoded columns in search_sorted_bounds (core) ([`a8c48e8`](https://github.com/vortex-rdf/vortex-rdf/commit/a8c48e8a2afa71f80100aee605a0dcd416bcba81) by @julianrojas87)
- Bind encoded columns in the typed residual filter (core) ([`2587853`](https://github.com/vortex-rdf/vortex-rdf/commit/2587853767dd128478f10942599b84602ef4ba20) by @julianrojas87)
- Bound-subject row bounds on sorted files via chunk probes (core) ([`bdfe2e3`](https://github.com/vortex-rdf/vortex-rdf/commit/bdfe2e32df0cd54fb70bd26e94cea0f8f8652680) by @julianrojas87)
- Retain compressed columns through resident adoption (core) ([`9742500`](https://github.com/vortex-rdf/vortex-rdf/commit/97425009c78f5393ce8097e063eed8b6ef81530d) by @julianrojas87)
- Dict probe nodes, owned probes, and chunk point reads (encoded-search) ([`1a5fb5f`](https://github.com/vortex-rdf/vortex-rdf/commit/1a5fb5f4c00574f7d785d9925ab6961e7bdfb267) by @julianrojas87)
- Cache base probes and point-read tiny file selections (core) ([`2cc274f`](https://github.com/vortex-rdf/vortex-rdf/commit/2cc274fe1d1734325d8cb601c14053a9b394e73c) by @julianrojas87)
- Windowed bounds over sorted sub-ranges (encoded-search) ([`1192b78`](https://github.com/vortex-rdf/vortex-rdf/commit/1192b78f16e7083c5e895f02fdbc1208614e5f09) by @julianrojas87)
- Point-read the file-backed dictionary through cached wire chunks (core) ([`1b3da12`](https://github.com/vortex-rdf/vortex-rdf/commit/1b3da1288f9b9c81d1c1692a669212360854cb53) by @julianrojas87)
- Resolve through shared lazy wrappers (encoded-search) ([`562a2be`](https://github.com/vortex-rdf/vortex-rdf/commit/562a2be8a37fbdbbb26b519152e13282d7c41b8a) by @julianrojas87)
- Compress built stores into probe-supported resident form (core) ([`c3eb922`](https://github.com/vortex-rdf/vortex-rdf/commit/c3eb92200e89f8be6be3d2b48c4af1c1f95b877e) by @julianrojas87)
- Measure the dictionary regimes the residency axis now turns on (bench) ([`876b4d5`](https://github.com/vortex-rdf/vortex-rdf/commit/876b4d5f878e5ff6ab97ca70c71c1664f285a164) by @julianrojas87)
- Build every store sorted, chosen by the target (core) ([`1573187`](https://github.com/vortex-rdf/vortex-rdf/commit/1573187e65fde559f1468bc9badc76cb642329cd) by @julianrojas87)
- Add a light/dark theme switch to the dashboard (bench) ([`9628229`](https://github.com/vortex-rdf/vortex-rdf/commit/96282293e211f63835db6ed4a15e3d356e1132b3) by julianrojas87)

### Changed

- Memoize the embedded dictionary blob across serializations (core) ([`4e2e28d`](https://github.com/vortex-rdf/vortex-rdf/commit/4e2e28d984e0dc6d5477fd380ab4f1a7e1d21e7d) by @julianrojas87)
- Stamp index leads when a full first chunk is the whole dataset (core) ([`cd4260f`](https://github.com/vortex-rdf/vortex-rdf/commit/cd4260f98d08e5ab328f2d4a7aa77a80eba2114a) by @julianrojas87)
- Memoize per-role term decoding of a dictionary chunk (core) ([`6b3ebf8`](https://github.com/vortex-rdf/vortex-rdf/commit/6b3ebf8caefd31215b4090320b958c27584bd243) by @julianrojas87)
- Adopt resident bases with canonical integer columns (core) ([`5184c02`](https://github.com/vortex-rdf/vortex-rdf/commit/5184c02a8946a520b5644e61b3495b9796543bff) by @julianrojas87)
- Adopt index components in resident form too (core) ([`74690e5`](https://github.com/vortex-rdf/vortex-rdf/commit/74690e5766db38506e8742cfc6178538c106849b) by @julianrojas87)
- Point-read small gathers through encoded probes (core) ([`8b2afda`](https://github.com/vortex-rdf/vortex-rdf/commit/8b2afda4abbc0be479a91cd78f3e07cb378076be) by @julianrojas87)
- Memoize chunk extremes in chunked probes (encoded-search) ([`aeb022a`](https://github.com/vortex-rdf/vortex-rdf/commit/aeb022a855ae723b76da21358c205b02b28351f0) by @julianrojas87)
- Locate and point-read index-served runs through cached probes (core) ([`ff1c724`](https://github.com/vortex-rdf/vortex-rdf/commit/ff1c7244604b285e6ceefb881fc9dc6c53c793ba) by @julianrojas87)
- Freeze live objects out of the python timing loop (bench) ([`6307d4e`](https://github.com/vortex-rdf/vortex-rdf/commit/6307d4e6ad3d581e0726d7d422f4800257cd97c5) by @julianrojas87)
- Drop the file-backed dictionary's fence fallback (core) ([`cff8998`](https://github.com/vortex-rdf/vortex-rdf/commit/cff899866b94f6b888d6a929f6e05de4ee18957e) by @julianrojas87)
- Read pattern terms through cached property keys (js) ([`6814a5d`](https://github.com/vortex-rdf/vortex-rdf/commit/6814a5d5bf1997e518691e529623b0e73df48c30) by @julianrojas87)
- Serve narrow code reads from the answering index (core) ([`55bed92`](https://github.com/vortex-rdf/vortex-rdf/commit/55bed92eb6f4d7625081399e7f3d115e46878d83) by @julianrojas87)
- Resolve reads synchronously (js) ([`5ccfe4a`](https://github.com/vortex-rdf/vortex-rdf/commit/5ccfe4a83c941a150777f5dd7a41ee6a1c37042b) by @julianrojas87)
- Bind wide residual scans through the shared canonical cache (core) ([`61e3cd2`](https://github.com/vortex-rdf/vortex-rdf/commit/61e3cd2c7ceb6b644b71802bc899c0532645eef3) by @julianrojas87)
- Size-gate the resident compression pass (core) ([`500ce7b`](https://github.com/vortex-rdf/vortex-rdf/commit/500ce7b8d72b549e5b1e94000bc0cfa51acac96e) by @julianrojas87)
- Drop the resident compression size gate (core) ([`34fcd71`](https://github.com/vortex-rdf/vortex-rdf/commit/34fcd718aa6ae1f1fa9389abe8c7f0924713e803) by @julianrojas87)
- Drop "sorted" from every benchmark name and label (bench) ([`dd2564b`](https://github.com/vortex-rdf/vortex-rdf/commit/dd2564b88072bef20767989394b387e408d274b5) by @julianrojas87)
- Locate reference-index runs through cached chunk probes (core) ([`b4a1e4f`](https://github.com/vortex-rdf/vortex-rdf/commit/b4a1e4f9d96f2f5399265c52f78cbd0e97bd16b0) by julianrojas87)

### Removed

- Drop match_compact in favour of get_quads (python) ([`cb369ee`](https://github.com/vortex-rdf/vortex-rdf/commit/cb369eeb5228edb996c3560b2619c54eff87d658) by @julianrojas87)

### Fixed

- IDE complaints about missing dependencies (js) ([`2db6d8b`](https://github.com/vortex-rdf/vortex-rdf/commit/2db6d8b01c87f0bc16f1bee65abaf5f646ac8e44) by @julianrojas87)
- Realign the type stub with the compiled surface (python) ([`0f48ecc`](https://github.com/vortex-rdf/vortex-rdf/commit/0f48ecc4831ac30f7eceeca5ccf4bee603d4698c) by @julianrojas87)
- Ship the wasm payload and the curated types (js) ([`6469bb3`](https://github.com/vortex-rdf/vortex-rdf/commit/6469bb3c2f49bc106a4555f98833201d36b0374d) by @julianrojas87)
- Decontaminate shared state and measure real term handling (bench) ([`b718ef3`](https://github.com/vortex-rdf/vortex-rdf/commit/b718ef3172723c9005250465d649cdaf8f77ff2f) by @julianrojas87)
- Align feature gates with each item's real consumers (core) ([`aa91257`](https://github.com/vortex-rdf/vortex-rdf/commit/aa91257c9e6c7241a459f83fe4d7cce5b07b3939) by @julianrojas87)
- Read the dashboard's BENCH_SIZE from its post-reorg home [skip ci] (bench) ([`c1a446a`](https://github.com/vortex-rdf/vortex-rdf/commit/c1a446a44f343051369271848ae4ecd02ea0a177) by @julianrojas87)
- Drop the dead call that discarded a completed run (bench) ([`205df5e`](https://github.com/vortex-rdf/vortex-rdf/commit/205df5ede2750138e50d5c1537f3a0a15e248375) by @julianrojas87)
- Search only the asserted window in sliced probes (encoded-search) ([`fca93f1`](https://github.com/vortex-rdf/vortex-rdf/commit/fca93f1504c431556b68f55c9b272972fd017ec5) by @julianrojas87)
- Drop the removed builder argument from the Python adapters (bench) ([`db92557`](https://github.com/vortex-rdf/vortex-rdf/commit/db925576fc93c9221ffcf40fe0bbd23d099c770b) by @julianrojas87)

## [0.5.0](https://github.com/vortex-rdf/vortex-rdf/compare/v0.4.0...v0.5.0) - 2026-07-29

### Added

- Add PyO3 bindings with an rdflib Store integration (python) ([`c851392`](https://github.com/vortex-rdf/vortex-rdf/commit/c8513929cd96ca1f2f0675812eb1c4faf50e6f49) by @julianrojas87)
- Port the DBBench harness from feat/cottas-bench (bench) ([`946211b`](https://github.com/vortex-rdf/vortex-rdf/commit/946211baebc485befd3a69f0e560d34499963651) by @julianrojas87)
- Push SPARQL BGP evaluation down into code space (python) ([`5c77b33`](https://github.com/vortex-rdf/vortex-rdf/commit/5c77b335d7e4b5b6ea6c29b39e8266c8e0f9a835) by @julianrojas87)
- Split the bindings into a standalone vortex-rdf package (python) ([`9e01f89`](https://github.com/vortex-rdf/vortex-rdf/commit/9e01f894d9293d0d1397cae635fe918ba2583b6f) by @julianrojas87)
- PyPi badges for published Python bindings ([`372b800`](https://github.com/vortex-rdf/vortex-rdf/commit/372b8005c9bddb5e2d31e703dab9073836878dc3) by @julianrojas87)

### Fixed

- Parse language-tagged and typed literals in parse_term (core) ([`618a7a8`](https://github.com/vortex-rdf/vortex-rdf/commit/618a7a83f67352b8924c5dcbcdd189ac8e624a05) by @julianrojas87)

## [0.4.0](https://github.com/vortex-rdf/vortex-rdf/compare/v0.3.0...v0.4.0) - 2026-07-29

### Added

- Store the term dictionary as a scannable column, not a list cell ([`30c0fa0`](https://github.com/vortex-rdf/vortex-rdf/commit/30c0fa042c5756d22a36148cc01ef146a467faff) by @julianrojas87)
- Choose the dictionary placement when writing files (core) ([`210db2e`](https://github.com/vortex-rdf/vortex-rdf/commit/210db2e06b807d2df90e937bbdd1d11a3caf73e6) by @julianrojas87)
- File-backed term dictionary with auto residency (core) ([`fb469c5`](https://github.com/vortex-rdf/vortex-rdf/commit/fb469c57fd618a9d223bff0020b38b2ae6de12d2) by @julianrojas87)
- Avoid action runs on docs-only commits ([`d620bb3`](https://github.com/vortex-rdf/vortex-rdf/commit/d620bb3af328e22541d088b1e0c45e1338fa0427) by @julianrojas87)

### Changed

- Cut wasm dictionary and ingest memory ([`2573626`](https://github.com/vortex-rdf/vortex-rdf/commit/25736260ef0bff113d34030cbe1efb16357ddb7a) by @julianrojas87)
- Hold and serialize the term dictionary FSST-compressed ([`75a9389`](https://github.com/vortex-rdf/vortex-rdf/commit/75a9389dbab2836d55b4a384e138c08917e927d3) by @julianrojas87)
- Share one term resolution across a match's index probes (core) ([`d442fff`](https://github.com/vortex-rdf/vortex-rdf/commit/d442ffffcc124c25ef17acdf30c60eac3432e663) by @julianrojas87)
- Stop pessimizing views a fast path already narrowed (core) ([`9d2ea13`](https://github.com/vortex-rdf/vortex-rdf/commit/9d2ea13ab24d7ee6217891965ba5716872d7fa76) by @julianrojas87)
- Memoize term to code lookups per dictionary (core) ([`8da6c31`](https://github.com/vortex-rdf/vortex-rdf/commit/8da6c312243e03522fc471c89335ab4504d463da) by @julianrojas87)
- Intern terms at ingest instead of buffering owned quads ([`276089d`](https://github.com/vortex-rdf/vortex-rdf/commit/276089de0675b9f02a9b340898177d43db514c28) by @julianrojas87)
- Seam the dictionary behind DictAccess with an async match prelude ([`7f06209`](https://github.com/vortex-rdf/vortex-rdf/commit/7f06209077bd75e03f73eed7d8ea43a17e554cb5) by @julianrojas87)
- Give shared types dedicated source modules (core) ([`5b6b867`](https://github.com/vortex-rdf/vortex-rdf/commit/5b6b867f9003664ee06d29ff1b86787b67471652) by @julianrojas87)
- Split the crate test suite by area (core) ([`7fae164`](https://github.com/vortex-rdf/vortex-rdf/commit/7fae164e1854b0826d5066a01f2ba69cdab6d813) by @julianrojas87)
- Split the wasm binding crate into modules (js) ([`c4dea32`](https://github.com/vortex-rdf/vortex-rdf/commit/c4dea32405405453062c38b3a6cb044a5f294477) by @julianrojas87)
- Share the RDF format-name table with core (js) ([`46a7668`](https://github.com/vortex-rdf/vortex-rdf/commit/46a76680078129b22e30c06853f2a82f3d2e30d6) by @julianrojas87)
- Derive the padded dictionary extent from footer statistics (core) ([`7f9eceb`](https://github.com/vortex-rdf/vortex-rdf/commit/7f9eceb9aadec3d0cb0c4cf74bc6e8591d5b3287) by @julianrojas87)
- Erase the retired _dict_terms format (core) ([`c947d1a`](https://github.com/vortex-rdf/vortex-rdf/commit/c947d1a8fc77d6b95fe6ab23d209b7d1c38d4ad9) by @julianrojas87)
- Fence-guided probes for the file-backed dictionary (core) ([`1720a44`](https://github.com/vortex-rdf/vortex-rdf/commit/1720a440d535b1b1fd216cb79640a2753d516171) by @julianrojas87)
- Compose typed objects without re-validating stored terms (core) ([`eb78570`](https://github.com/vortex-rdf/vortex-rdf/commit/eb78570b2af8fd0c23930da6f13d08b49ed663a5) by @julianrojas87)
- Materialize quads chunk-wise into an exactly-sized vec (core) ([`b1c01f3`](https://github.com/vortex-rdf/vortex-rdf/commit/b1c01f3cd78bf671721ab8b6ca01be6a1b0d8720) by @julianrojas87)

### Fixed

- Polluted CHANGELOG ([`b5951c0`](https://github.com/vortex-rdf/vortex-rdf/commit/b5951c04fe6527630e73cd2178f899fba1422f89) by @julianrojas87)

## [0.3.0](https://github.com/vortex-rdf/vortex-rdf/compare/v0.2.1...v0.3.0) - 2026-07-25

### Changed

- Implement StrColReader for faster string column reads ([`3813ce2`](https://github.com/vortex-rdf/vortex-rdf/commit/3813ce2d7087ef64d226f2742aea5491672c44d1) by @julianrojas87)
- Reuse payload buffer when serializing against disk ([`ab92cf4`](https://github.com/vortex-rdf/vortex-rdf/commit/ab92cf4aef3754e3061f3f2b395e6a6845da27be) by @julianrojas87)
- Decoding vortex into quads uses unchecked path ([`659273c`](https://github.com/vortex-rdf/vortex-rdf/commit/659273c591f7000494a5467e5113e4057b87a42a) by @julianrojas87)
- True zero-copy for IPC-based exchange ([`3a5d314`](https://github.com/vortex-rdf/vortex-rdf/commit/3a5d314b2e91993285e17c15af3b307b496c913b) by @julianrojas87)
- Serialize to disk only when needed for stream builders ([`a2ab133`](https://github.com/vortex-rdf/vortex-rdf/commit/a2ab1333ad22afa1444f8656ffdd0c49e5e07fec) by @julianrojas87)

### Fixed

- WASM compilation time (from 15m to 40s) ([`c7b0225`](https://github.com/vortex-rdf/vortex-rdf/commit/c7b0225d3882fc36ca6672604c28f65bf42ea4d0) by @julianrojas87)
- Changelog update process ([`1702cef`](https://github.com/vortex-rdf/vortex-rdf/commit/1702cef296cc90c50fcdaf944b1be9c0fc8a4cc5) by @julianrojas87)

## [0.2.1] - 2026-07-25

### Added

- Add proper codspeed benchmark that uploads (js) ([`320bb5b`](https://github.com/vortex-rdf/vortex-rdf/commit/320bb5b030f80f0c94d5eed94fcb002026df57ad) by @julianrojas87).
- Add CHANGELOG management scripts ([`e7ce0f7`](https://github.com/vortex-rdf/vortex-rdf/commit/e7ce0f7cb3fa94b0433e136b218c5fd13282b390) by @julianrojas87).

### Changed

- Enable the `+simd128` target feature for WASM builds ([`6e8e8b6`](https://github.com/vortex-rdf/vortex-rdf/commit/6e8e8b6d77be89d05431239444595092fdf9bd72) by @julianrojas87).

### Removed

- Remove the format-specific `nquads_to_vortex` / `vortex_to_nquads` helpers from
  the JS/WASM bindings ([`12c58c7`](https://github.com/vortex-rdf/vortex-rdf/commit/12c58c7602f2afff6fb77657d5ab5d39f13b573e) by @julianrojas87).

### Fixed

- Scale of JS benchmark bar charts ([`606a08f`](https://github.com/vortex-rdf/vortex-rdf/commit/606a08f1139ea46bf42686955582b1c232690fc6) by @julianrojas87).

## [0.2.0] - 2026-07-24

### Changed

- Major performance improvements to the JS/WASM bindings: ingestion quads are
  packed into a single byte buffer before crossing into WASM, pattern matching
  runs directly over column buffers, quads serialize via unchecked IRI builders,
  and chained filters match over already-filtered row selections — roughly
  halving serialization cost ([`c777712`](https://github.com/vortex-rdf/vortex-rdf/commit/c7777125cd07aa101bf58e10ce9bd62502c76639) by @julianrojas87).

### Removed

- `init_panic_hook` is no longer part of the WASM export surface ([`b1512c0`](https://github.com/vortex-rdf/vortex-rdf/commit/b1512c0533485947725f2318078cce98c895d4e6) by @julianrojas87).

## [0.1.0](https://github.com/vortex-rdf/vortex-rdf/releases/tag/v0.1.0) - 2026-07-21

Initial release ([79 commits](https://github.com/vortex-rdf/vortex-rdf/commits/v0.1.0) by @julianrojas87). The entries below summarise features built across
many commits, so they are not attributed individually.

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
