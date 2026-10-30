# Contributing

## Git hooks and the local CI mirror

Run `./scripts/install-git-hooks.sh` once per clone to enable the git hooks.
[`scripts/ci-check.sh`](scripts/ci-check.sh) holds the commands of every CI
job, one function per job, and the workflows
([`ci.yml`](.github/workflows/ci.yml), [`docs.yml`](.github/workflows/docs.yml))
run their jobs through it. The `pre-push` hook runs every job; run
`./scripts/ci-check.sh` yourself the same way, or name the jobs to run
(`./scripts/ci-check.sh lint rust-tests`). Skip the hook for one push with
`git push --no-verify`. The script's header lists the jobs and what a local
run does differently: it skips a step whose tool is missing, builds the wasm
package without wasm-opt, and caps the compile jobs of `js-tests`.

Core builds in two configurations: natively with its default `file-io`
feature, and for wasm32 without it, as the wasm bindings take it. A native
build without `file-io` stops with a compile error; the wasm32 clippy run is
what checks the other configuration.

Node's version is pinned once in [`.tool-versions`](.tool-versions); every
workflow reads it via `node-version-file`, and mise and asdf pick it up natively,
so a local shell matches CI. Keep the version the last token on its line —
`actions/setup-node` does not accept a trailing comment there. Rust's version is
pinned once too, in [`.github/actions/setup-rust`](.github/actions/setup-rust/action.yml),
which every workflow's Rust jobs use; `rustup override set` that version in a
clone to match CI.

## Documentation anchors

The documents under `docs/` (and the READMEs) link into the source with
`path#Lnn` anchors. `scripts/check-doc-anchors.sh` verifies every relative
markdown link in `README.md`, `CONTRIBUTING.md`, `docs/*.md`, `js/README.md`,
`python/README.md`, `js/bench/README.md`, `python/bench/README.md` and
`encoded-search/README.md`: the
target file exists, an anchored line is within the file, and every line anchor
names what it points at, which has to appear on the anchored lines: the link's
label when it is a single backticked identifier, else the link's title, else the
identifier that opens the table row the link sits in. A line anchor with none
of these fails. It is the `docs` job of `scripts/ci-check.sh`, so it runs in the
`pre-push` hook and, through `docs.yml`, on every pull request and every push to
`main`, doc-only ones included. Run it by itself after moving code the docs
point at, and re-point the anchors it reports.

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
enforces the commit format the generator relies on. The script's header says how
to refresh the `[Unreleased]` section and how to cut a release, which also sets
every declared version.

Each entry links its commit and author. Author names resolve to GitHub `@handles`
when a token is available (`GITHUB_TOKEN`, or borrowed from `gh auth token`) **and
the commit has been pushed to GitHub** — so push your branch before generating if
you want linked handles; otherwise the plain git author name is used. Set
`SKIP_AUTHOR_ENRICH=1` to skip the lookups (e.g. offline).
