#!/usr/bin/env bash
# One-time setup: points this clone's git hooks at scripts/hooks, so `git push`
# runs scripts/ci-check.sh locally first (same checks as .github/workflows/ci.yml).
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
git -C "$repo_root" config core.hooksPath scripts/hooks
echo "Installed: git push will now run scripts/ci-check.sh (skip once with --no-verify)."
