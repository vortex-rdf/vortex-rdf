#!/usr/bin/env python3
"""Turn a `cargo bench --bench benchmark` run into the static HTML dashboard.

Usage:
    cargo bench --bench benchmark | tee bench_output.txt
    python3 scripts/render_bench_dashboard.py bench_output.txt public/index.html

Divan (via codspeed-divan-compat) has no machine-readable output mode, so this
parses its tree-table text output directly. The tree-drawing glyph (U+2502,
"│") is reused for both the indentation guide on nested rows and the column
separator, so a naive split on "│" misaligns nested rows by one column --
the tree prefix is stripped first, and the name/first-value pair (which share
a cell with no separator between them) is split on runs of 2+ spaces instead.
"""
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
PREFIX_RE = re.compile(r"^[\s│]*([├╰])─\s*(.*)$")
UNIT_NS = {"ns": 1.0, "µs": 1_000.0, "us": 1_000.0, "ms": 1_000_000.0, "s": 1_000_000_000.0}


def to_ns(value_str):
    value_str = value_str.strip()
    if not value_str:
        return None
    m = re.match(r"^([0-9.]+)\s*(ns|µs|us|ms|s)$", value_str)
    if not m:
        return None
    num, unit = m.groups()
    return float(num) * UNIT_NS[unit]


def parse_bench_output(text):
    lines = text.splitlines()
    start = 0
    timer_precision = None
    for i, line in enumerate(lines):
        m = re.match(r"^Timer precision:\s*(.+)$", line)
        if m:
            timer_precision = m.group(1).strip()
        if line.startswith("benchmark") and "fastest" in line:
            start = i + 1
            break

    results = []
    current_group = None

    for line in lines[start:]:
        if not line.strip():
            continue
        m = PREFIX_RE.match(line)
        if not m:
            continue

        is_top = line[0] in "├╰"
        rest = m.group(2)

        cols = rest.split("│")
        name_and_fastest = cols[0].strip()
        rest_values = [c.strip() for c in cols[1:]]

        pieces = re.split(r"\s{2,}", name_and_fastest) if name_and_fastest else []
        name = pieces[0] if pieces else ""
        fastest = pieces[1] if len(pieces) > 1 else ""

        values = [fastest] + rest_values
        has_values = any(v for v in values)

        if is_top and not has_values:
            current_group = name
            continue

        if is_top and has_values:
            current_group = None
            group, variant = name, None
        else:
            group, variant = current_group, name

        while len(values) < 6:
            values.append("")
        fastest, slowest, median, mean, samples, iters = values[:6]

        results.append({
            "group": group,
            "variant": variant,
            "id": f"{group}::{variant}" if variant else group,
            "fastest": fastest,
            "slowest": slowest,
            "median": median,
            "mean": mean,
            "fastest_ns": to_ns(fastest),
            "slowest_ns": to_ns(slowest),
            "median_ns": to_ns(median),
            "mean_ns": to_ns(mean),
            "samples": samples,
            "iters": iters,
        })

    return results, timer_precision


def git(*args, default=""):
    try:
        return subprocess.check_output(["git", *args], cwd=REPO_ROOT, text=True).strip()
    except Exception:
        return default


def cpu_model():
    try:
        with open("/proc/cpuinfo", encoding="utf-8") as f:
            for line in f:
                if line.lower().startswith("model name"):
                    return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return "unknown CPU"


def bench_size():
    src = (REPO_ROOT / "core" / "benches" / "benchmark.rs").read_text(encoding="utf-8")
    m = re.search(r"const BENCH_SIZE:\s*usize\s*=\s*([\d_]+);", src)
    return int(m.group(1).replace("_", "")) if m else None


def build_provenance(results, timer_precision):
    commit = os.environ.get("GITHUB_SHA", git("rev-parse", "HEAD"))[:7]
    branch = os.environ.get("GITHUB_REF_NAME", git("rev-parse", "--abbrev-ref", "HEAD", default="unknown"))
    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    samples = results[0]["samples"] if results else "?"
    size = bench_size()
    size_str = f"{size:,}" if size else "unknown"
    precision = f", {timer_precision} precision" if timer_precision else ""

    return (
        f"Measured {date} · commit {commit} ({branch}) · {cpu_model()}, {os.cpu_count()} threads · "
        f"BENCH_SIZE = {size_str} quads · {samples} samples/benchmark · "
        f"codspeed-divan-compat, wall-clock (os) timer{precision}"
    )


def main():
    if len(sys.argv) != 3:
        print(f"usage: {sys.argv[0]} <bench-output.txt> <output.html>", file=sys.stderr)
        return 1

    in_path, out_path = Path(sys.argv[1]), Path(sys.argv[2])
    template_path = Path(__file__).resolve().parent / "bench_dashboard_template.html"

    results, timer_precision = parse_bench_output(in_path.read_text(encoding="utf-8"))
    if not results:
        print("no benchmark results parsed -- is the input the raw `cargo bench` output?", file=sys.stderr)
        return 1

    provenance = build_provenance(results, timer_precision)
    template = template_path.read_text(encoding="utf-8")
    out = template.replace("__BENCH_DATA__", json.dumps(results)).replace(
        "__PROVENANCE__", json.dumps(provenance)
    )

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(out, encoding="utf-8")
    print(f"parsed {len(results)} benchmark results -> {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
