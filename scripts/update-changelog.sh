#!/usr/bin/env bash
# Maintains CHANGELOG.md (Keep a Changelog format) from Conventional Commits,
# through git-cliff (configured in cliff.toml), and cuts releases.
#
#   scripts/update-changelog.sh           # regenerate the [Unreleased] section
#   scripts/update-changelog.sh vX.Y.Z    # cut release X.Y.Z
#
# Both forms take the commits since the latest tag. Without an argument the
# script only rewrites [Unreleased], a preview that changes no version. A cut
# also sets X.Y.Z everywhere a version is declared: the three version lines of
# Cargo.toml ([workspace.package] and the two path dependencies), the workspace
# entries of Cargo.lock, interface-tests/Cargo.lock and bench-compare/Cargo.lock
# (`cargo update -w`), js/package.json with its lock (`npm version`), and the
# project version of java/pom.xml (its first <version>, the line release.yml
# checks). Python inherits the workspace version through maturin. Released
# sections (## [x.y.z]) are kept verbatim.
#
# Cut the changelog last, once nothing else is waiting to land: a cut section
# is frozen (the script refuses to cut the same version twice), so a commit
# that lands after the cut gets its entry added by hand. Commit the cut as
# `chore(release): X.Y.Z`, merge it, then tag the merge commit
# (`git tag vX.Y.Z <merge-sha> && git push origin vX.Y.Z`): the tag publishes
# the release (.github/workflows/release.yml) and starts the next changelog.
#
# Needs git-cliff (`cargo binstall git-cliff`, or `cargo install git-cliff`);
# a cut also needs cargo and npm.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

# A GitHub token lets us resolve each commit's author to their GitHub @handle. Prefer
# an explicit GITHUB_TOKEN; otherwise borrow one from an authenticated `gh` CLI.
# Used by both git-cliff and enrich_handles() below. No token/offline -> plain name.
if [ -z "${GITHUB_TOKEN:-}" ] && command -v gh >/dev/null 2>&1; then
  GITHUB_TOKEN="$(gh auth token 2>/dev/null || true)"
  [ -n "$GITHUB_TOKEN" ] && export GITHUB_TOKEN
fi

# Rewrite "by <name>" -> a bare GitHub @handle (GitHub auto-links a leading @, so no
# markdown link is needed), resolved PER COMMIT via the API
# (GET /repos/:o/:r/commits/:sha -> author.login). Unlike git-cliff's built-in
# enrichment (which only matches the DEFAULT branch), this resolves any commit that
# is PUSHED to GitHub — including feature branches. Reads git-cliff markdown on
# stdin, writes it back enriched; an entry is left untouched when its commit isn't
# pushed, the author has no GitHub account, or there's no token/network. One request
# per unique SHA (cached). Opt out with SKIP_AUTHOR_ENRICH=1. Only ever runs over
# git-cliff output (the [Unreleased]/new release section), never the frozen entries.
enrich_handles() {
  if [ -n "${SKIP_AUTHOR_ENRICH:-}" ] || ! command -v python3 >/dev/null 2>&1; then
    cat; return
  fi
  python3 <(cat <<'PY'
import os, sys, re, json, urllib.request
token = os.environ.get("GITHUB_TOKEN", "")
cache = {}
def login_for(owner, repo, sha):
    key = (owner, repo, sha)
    if key not in cache:
        req = urllib.request.Request(
            f"https://api.github.com/repos/{owner}/{repo}/commits/{sha}",
            headers={"Accept": "application/vnd.github+json", "User-Agent": "update-changelog"},
        )
        if token:
            req.add_header("Authorization", f"Bearer {token}")
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                cache[key] = (json.load(r).get("author") or {}).get("login")
        except Exception:
            cache[key] = None
    return cache[key]

# Matches the entry tail: ([`sha`](https://github.com/OWNER/REPO/commit/FULLSHA) by AUTHOR)
entry = re.compile(
    r'(\(\[`[0-9a-f]+`\]\(https://github\.com/([^/)]+)/([^/)]+)/commit/([0-9a-f]{7,40})\) by )'
    r'(.+?)(\)\.?)$'
)
def repl(m):
    login = login_for(m.group(2), m.group(3), m.group(4))
    return f'{m.group(1)}@{login}{m.group(6)}' if login else m.group(0)

for raw in sys.stdin:
    sys.stdout.write(entry.sub(repl, raw.rstrip("\n")) + "\n")
PY
)
}

CHANGELOG='CHANGELOG.md'
version="${1:-}"

command -v git-cliff >/dev/null || { echo "error: git-cliff not found; install with: cargo binstall git-cliff  (or: cargo install git-cliff)" >&2; exit 1; }
# The boundary: the commits after the latest tag make up the generated section.
BASE_REF="$(git describe --tags --abbrev=0)" || { echo "error: no tag to start from; tag the last release" >&2; exit 1; }

# Regenerates the top of CHANGELOG.md (git-cliff's header and newest section, with
# the given git-cliff arguments) above the frozen released sections.
write_changelog() {
  local frozen_tail managed_top
  frozen_tail="$(awk '/^## \[[0-9]/{f=1} f' "$CHANGELOG")"
  managed_top="$(git-cliff "${BASE_REF}.." "$@" | enrich_handles)"
  printf '%s\n\n%s\n' "$managed_top" "$frozen_tail" \
    | awk 'NR>1 && /^## \[/ && prev != "" { print "" } { print; prev=$0 }' > "$CHANGELOG"
}

if [ -z "$version" ]; then
  write_changelog
  echo "Refreshed [Unreleased] from the Conventional Commits since $BASE_REF."
  exit 0
fi

# ---- cut a release: vX.Y.Z ----
tag_pattern='^v([0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?)$'
[[ "$version" =~ $tag_pattern ]] || { echo "error: expected a version tag vX.Y.Z, got '$version'" >&2; exit 2; }
ver="${BASH_REMATCH[1]}"
if awk -v h="## [$ver]" 'index($0, h) == 1 { found = 1 } END { exit !found }' "$CHANGELOG"; then
  echo "error: $CHANGELOG already has a [$ver] section; add later commits to it by hand" >&2
  exit 1
fi
command -v cargo >/dev/null || { echo "error: cargo not found" >&2; exit 1; }
command -v npm >/dev/null || { echo "error: npm not found" >&2; exit 1; }

sed -i.bak -E \
  -e "s/^version = \"[^\"]*\"/version = \"$ver\"/" \
  -e "s/^(vortex-rdf-(core|encoded-search) = \{ path = \"[^\"]*\", version = \")[^\"]*\"/\1$ver\"/" \
  Cargo.toml
rm Cargo.toml.bak
set_lines="$(grep -cE "^version = \"$ver\"\$|^vortex-rdf-(core|encoded-search) = \{ path = \"[^\"]*\", version = \"$ver\" \}\$" Cargo.toml || true)"
[ "$set_lines" = 3 ] || { echo "error: set $set_lines of Cargo.toml's 3 version lines; check it by hand" >&2; exit 1; }
cargo update -w
cargo update -w --manifest-path interface-tests/Cargo.toml
cargo update -w --manifest-path bench-compare/Cargo.toml
(cd js && npm version "$ver" --no-git-tag-version --allow-same-version)
# The pom's first <version> is the project's own (release.yml reads the same line).
awk -v ver="$ver" '!done && sub(/<version>[^<]*<\/version>/, "<version>" ver "</version>") { done = 1 } { print }' \
  java/pom.xml > java/pom.xml.tmp
mv java/pom.xml.tmp java/pom.xml
pom_version="$(sed -n 's:.*<version>\(.*\)</version>.*:\1:p' java/pom.xml | head -n 1)"
[ "$pom_version" = "$ver" ] || { echo "error: java/pom.xml's version is '$pom_version', not $ver; check it by hand" >&2; exit 1; }

write_changelog --tag "$version"
echo "Cut release $version: versions set to $ver, and its CHANGELOG.md section written."
echo "Review, commit as \`chore(release): $ver\`, merge, then tag the merge commit:"
echo "  git tag $version <merge-sha> && git push origin $version"
