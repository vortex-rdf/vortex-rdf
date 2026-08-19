#!/usr/bin/env bash
# One-time setup: points this clone's git hooks at scripts/hooks, so `git push`
# runs scripts/ci-check.sh locally first (the .github/workflows/ci.yml jobs
# except js-tests, which needs CI's memory tuning).
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
git -C "$repo_root" config core.hooksPath scripts/hooks
echo "Installed git hooks (skip once with --no-verify):"
echo "  commit-msg : enforces Conventional Commits (for CHANGELOG generation)."
echo "  pre-push   : runs scripts/ci-check.sh (CI's lint, rust-tests and"
echo "               python-tests jobs; js-tests stays CI-only)."
