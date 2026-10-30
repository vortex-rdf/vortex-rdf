#!/usr/bin/env bash
# One-time setup: points this clone's git hooks at scripts/hooks, so `git push`
# first runs scripts/ci-check.sh, every job of the CI workflows (its header
# says what a local run skips or does differently).
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
git -C "$repo_root" config core.hooksPath scripts/hooks
echo "Installed git hooks (skip once with --no-verify):"
echo "  commit-msg : enforces Conventional Commits (for CHANGELOG generation)."
echo "  pre-push   : runs scripts/ci-check.sh, every CI job (docs, lint,"
echo "               rust-tests, python-tests, js-tests and java-tests)."
