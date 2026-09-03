# Contributing

## Git hooks and the local CI mirror

Run `./scripts/install-git-hooks.sh` once per clone to enable the git hooks. The
`pre-push` hook mirrors the `lint`, `rust-tests`, `python-tests` and `js-tests`
jobs in [`.github/workflows/ci.yml`](.github/workflows/ci.yml) — `cargo fmt
--check`, both `cargo clippy` runs (workspace, and core with
`--no-default-features`), both `cargo test` variants, `uv sync --locked && uv
run pytest tests -q` in `python/`, and the wasm-pack build + `npm run
typecheck` + `npm test` + `npm run test:zero-copy` in `js/`. You can also run it manually with
`./scripts/ci-check.sh`. Skip it for one push with `git push --no-verify`.

The python and js blocks are soft skips when their tooling is missing (`uv`;
`npm`, `js/node_modules`, or the `wasm32-unknown-unknown` target): the script
warns and continues, so CI remains the source of truth for those jobs on a
clone without the tools.

The local `js-tests` mirror trades exactness for wall clock: it builds with
`npm run build:fast` (wasm-pack without wasm-opt, which dominates the full
build's wall clock and buys no memory headroom) and caps `CARGO_BUILD_JOBS`
(default 4; export it to override) so a cold wasm build's rustc fan-out doesn't
exhaust memory. The tests therefore run against an unoptimized wasm binary — CI
stays the source of truth for the exact artifact that ships.

Node's version is pinned once in [`.tool-versions`](.tool-versions); every
workflow reads it via `node-version-file`, and mise and asdf pick it up natively,
so a local shell matches CI. Keep the version the last token on its line —
`actions/setup-node` does not accept a trailing comment there.

## Documentation anchors

The documents under `docs/` (and the READMEs) link into the source with
`path#Lnn` anchors. `scripts/check-doc-anchors.sh` verifies every relative
markdown link in `README.md`, `CONTRIBUTING.md`, `docs/*.md`, `js/README.md`,
`python/README.md`, `js/bench/README.md` and `encoded-search/README.md`: the
target file exists, an anchored line is within the file, and a link whose text
is a single backticked identifier (a `` `foo` `` label on a `#L42` anchor) names
something that appears on that line. It runs in `scripts/ci-check.sh` and therefore in the
`pre-push` hook; run it by itself after moving code the docs point at, and
re-point the anchors it reports.

The root `README.md` is a front page: it links only into `docs/`, the binding
READMEs and this file, never into `core/src`, so it needs no regeneration when
code moves. Its Rust snippets are compiled as doctests of `vortex-rdf-core`
(`cargo test -p vortex-rdf-core --doc`); its Python and JavaScript snippets are
byte-identical copies of the Quick start blocks in `python/README.md` and
`js/README.md`, so change those first and copy.

## Changelog

[`CHANGELOG.md`](CHANGELOG.md) follows [Keep a Changelog](https://keepachangelog.com/)
and is generated from [Conventional Commits](https://www.conventionalcommits.org)
by [`scripts/update-changelog.sh`](scripts/update-changelog.sh), which uses
[git-cliff](https://git-cliff.org) — install it once with `cargo binstall git-cliff`
(or `cargo install git-cliff`). The `commit-msg` hook from `install-git-hooks.sh`
enforces the commit format the generator relies on.

- **Refresh the `[Unreleased]` section:** `./scripts/update-changelog.sh`
- **Cut a release:** `./scripts/update-changelog.sh vX.Y.Z` moves `[Unreleased]`
  into a dated version section. Follow it with `git tag vX.Y.Z` — the tag is what
  advances the boundary for the next release.

Each entry links its commit and author. Author names resolve to GitHub `@handles`
when a token is available (`GITHUB_TOKEN`, or borrowed from `gh auth token`) **and
the commit has been pushed to GitHub** — so push your branch before generating if
you want linked handles; otherwise the plain git author name is used. Set
`SKIP_AUTHOR_ENRICH=1` to skip the lookups (e.g. offline).
