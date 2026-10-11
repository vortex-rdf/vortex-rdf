#!/usr/bin/env bash
# Verifies the relative markdown links in the hand-written docs: every linked
# file exists and every `#Lnn` / `#Lnn-Lmm` anchor is within the file's line
# count. A line anchor also has to name what it points at, so that code that
# moves breaks the link. The name is checked against the anchored lines and
# comes from, in this order:
#   - the label, when it is a single backticked identifier
#     ([`foo`](path#Lnn));
#   - the link title ([in memory](path#Lnn "deleted"));
#   - the identifier that opens the table row the link sits in
#     (| `FOO` | 4 | [`file.rs`](path#Lnn) |).
# A line anchor with none of these is an error.
#
# Prints one `doc:line: message` per failure and exits 1 if there is any.
# Run directly (`scripts/check-doc-anchors.sh`) or through scripts/ci-check.sh.
# Needs only bash and python3.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

docs=(README.md CONTRIBUTING.md docs/*.md js/README.md python/README.md
  js/bench/README.md encoded-search/README.md)

existing=()
for doc in "${docs[@]}"; do
  [ -f "$doc" ] && existing+=("$doc")
done

python3 - "${existing[@]}" <<'PY'
import os
import re
import sys

LINK = re.compile(r"\]\(([^)\s]+)(?:\s+\"([^\"]*)\")?\)")
ANCHOR = re.compile(r"^L(\d+)(?:-L(\d+))?$")
IDENT = r"[A-Za-z_][A-Za-z0-9_]*(?:::[A-Za-z_][A-Za-z0-9_]*)*"
LABEL_IDENT = re.compile(rf"^`({IDENT})`$")
ROW_KEY = re.compile(rf"^\s*\|\s*`({IDENT})`\s*\|")

failures = 0
file_lines = {}


def lines_of(path):
    if path not in file_lines:
        with open(path, encoding="utf-8", errors="replace") as f:
            file_lines[path] = f.read().splitlines()
    return file_lines[path]


def word(name):
    """`name` as a whole identifier, not a part of a longer one."""
    return re.compile(rf"(?<![A-Za-z0-9_]){re.escape(name)}(?![A-Za-z0-9_])")


def label_before(line, close):
    """The text of the bracket pair that closes at line[close]."""
    depth = 0
    for i in range(close - 1, -1, -1):
        if line[i] == "]":
            depth += 1
        elif line[i] == "[":
            if depth == 0:
                return line[i + 1 : close]
            depth -= 1
    return None


def fail(doc, lineno, msg):
    global failures
    failures += 1
    print(f"{doc}:{lineno}: {msg}")


for doc in sys.argv[1:]:
    base = os.path.dirname(doc)
    in_fence = False
    with open(doc, encoding="utf-8") as f:
        for lineno, line in enumerate(f, 1):
            if line.lstrip().startswith("```"):
                in_fence = not in_fence
                continue
            if in_fence:
                continue
            row_key = ROW_KEY.match(line)
            for m in LINK.finditer(line):
                target, title = m.group(1), m.group(2)
                label = label_before(line, m.start())
                shown = f"[{label}]({target})"
                if re.match(r"^[a-z][a-z0-9+.-]*:", target) or target.startswith("#"):
                    continue  # external URL or same-document heading
                path, _, frag = target.partition("#")
                resolved = os.path.normpath(os.path.join(base, path))
                if not os.path.isfile(resolved):
                    fail(doc, lineno, f"{shown}: {resolved} does not exist")
                    continue
                a = ANCHOR.match(frag)
                if not a:
                    continue  # heading anchor or no anchor: existence is enough
                start = int(a.group(1))
                end = int(a.group(2)) if a.group(2) else start
                text = lines_of(resolved)
                if start < 1 or end < start or end > len(text):
                    fail(doc, lineno, f"{shown}: #{frag} outside 1-{len(text)}")
                    continue
                ident = LABEL_IDENT.match((label or "").strip())
                if ident:
                    name = ident.group(1).rsplit("::", 1)[-1]
                    found = word(name)
                elif title:
                    name = title
                    found = re.compile(re.escape(title))
                elif row_key:
                    name = row_key.group(1).rsplit("::", 1)[-1]
                    found = word(name)
                else:
                    fail(
                        doc,
                        lineno,
                        f"{shown}: nothing says what the anchor points at: label the "
                        "link with the identifier, give it a title that appears on "
                        "the line, or put it in a table row keyed by the identifier",
                    )
                    continue
                if not any(found.search(text[n - 1]) for n in range(start, end + 1)):
                    fail(doc, lineno, f"{shown}: `{name}` not on {path} line {frag}")

if failures:
    print(f"{failures} broken doc link(s)")
    sys.exit(1)
PY
