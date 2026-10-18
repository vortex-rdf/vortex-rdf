#!/usr/bin/env python3
"""Turn the four benchmark suites' results into the static HTML dashboard.

Usage (`scripts/refresh.sh` runs the suites, then this over their outputs):
    python3 scripts/render_bench_dashboard.py target/bench/rust.txt target/bench/public/index.html
        [--rust-compare target/bench/rust-compare.json] [--js target/bench/js.json]
        [--python target/bench/python.json]

The first argument is the log of `cargo bench --bench benchmark` and
`--bench match_lazy`, whose `#dataset` line gives the run's dataset; the
other three suites write dashboard-shaped JSON (see `load_suite`), and a
suite left out renders as its tab's placeholder.

Divan (via codspeed-divan-compat) has no machine-readable output mode, so this
parses its tree-table text output directly. The tree-drawing glyph (U+2502,
"│") is reused for both the indentation guide on nested rows and the column
separator, so a naive split on "│" misaligns nested rows by one column --
the tree prefix is stripped first, and the name/first-value pair (which share
a cell with no separator between them) is split on runs of 2+ spaces instead.
"""
import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
TEMPLATE = Path(__file__).resolve().parent / "bench_dashboard_template.html"
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


def parse_dataset_shape(text):
    """The `#dataset {...}` line each Rust bench target prints before measuring.

    The moduli follow from the row count through a coprimality nudge, so the
    selectivity of a run is not something the page can restate from a formula --
    it changes with `BENCH_SIZE`. Reading the shape off the run makes the page's
    matched-row figures a record rather than a re-derivation, which any
    hardcoded arithmetic would silently outlive.

    Both targets stamp their own line into the shared log; they run at one
    `BENCH_SIZE`, so a disagreement means the two halves of the tab measured
    different data and is worth saying out loud.

    """
    found = []
    for line in text.splitlines():
        if line.startswith("#dataset "):
            try:
                found.append(json.loads(line[len("#dataset "):]))
            except json.JSONDecodeError:
                print(f"note: unparseable dataset line: {line!r}", file=sys.stderr)
    if not found:
        return None
    if any(d != found[0] for d in found[1:]):
        print("WARNING: the bench targets report different dataset shapes; using the first",
              file=sys.stderr)
    return found[0]


def fmt_duration(ns):
    """One duration format for the whole page: at most 2 decimals, and none on ns.

    The four sources spell their own numbers differently -- divan uses 4
    significant digits, and the three harnesses each format their own strings --
    so a page built from all of them shows `281.0000 ns` beside `3.427 µs` beside
    `2.781 ms`. Rendering every duration from its `*_ns` value here is the only
    place that sees all four, so it is the only place the page can be made to
    read consistently. Sub-nanosecond counts round to whole nanoseconds because a
    fractional nanosecond is below any timer this dashboard uses.
    """
    if ns is None:
        return None
    # 999.6 must promote to 1 µs, not round to "1000 ns".
    if ns < 999.5:
        return f"{ns:.0f} ns"
    for unit, scale in (("µs", 1_000.0), ("ms", 1_000_000.0), ("s", 1_000_000_000.0)):
        if ns < scale * 1_000 or unit == "s":
            v = ns / scale
            # Trim trailing zeros so 3.40 reads as 3.4 and 216.00 as 216.
            return f"{v:.2f}".rstrip("0").rstrip(".") + f" {unit}"
    return f"{ns:.0f} ns"


def normalize_durations(rows):
    """Rewrite every row's display strings from its `*_ns` values.

    Leaves a row alone where the value is None -- an `unsupported` cell carries a
    word, not a duration, and must not be turned into `0 ns`.
    """
    for r in rows:
        for key in ("fastest", "slowest", "median", "mean"):
            ns = r.get(f"{key}_ns")
            if ns is not None:
                r[key] = fmt_duration(ns)
    return rows


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


def measured_at(source=None):
    """The dashboard's shared "Measured …" stamp: UTC, to the minute.

    All three tabs date themselves this way — `js/bench/compare.bench.ts` and
    `python/bench/run.py` build the same string for the lines they emit — so one
    page never shows three formats (or three timezones) for one run.

    `source` is the file whose measurement is being stamped; its mtime is when
    that suite finished writing, which is what the other two tabs report. Only
    when it is missing does this fall back to now, which for a render long after
    the bench would overstate how fresh the numbers are.
    """
    when = datetime.now(timezone.utc)
    if source is not None:
        try:
            when = datetime.fromtimestamp(source.stat().st_mtime, timezone.utc)
        except OSError:
            pass
    return when.strftime("%Y-%m-%d %H:%M UTC")


def sample_counts(results):
    """The divan sample counts the run actually used, most-used first.

    The suite is not uniform: queries carry `QUERY_SAMPLES` and the
    seconds-per-iteration phases carry `HEAVY_SAMPLES` (see
    `core/benches/support/mod.rs`), so no single number describes it. Reading
    them off the rows rather than the source reports what this run did.
    """
    counts = {}
    for r in results:
        counts[r["samples"]] = counts.get(r["samples"], 0) + 1
    return [c for c, _ in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))]


def build_provenance(results, timer_precision, quads, source):
    """The Rust tab's provenance line. Divan's text output carries no config,
    so the run's repetition counts are read off its rows and its size off its
    `#dataset` stamp: `quads` is the row count the binary generated."""
    commit = os.environ.get("GITHUB_SHA", git("rev-parse", "HEAD"))[:7]
    branch = os.environ.get("GITHUB_REF_NAME", git("rev-parse", "--abbrev-ref", "HEAD", default="unknown"))
    samples = sample_counts(results)
    samples_str = f"{samples[0]} samples/benchmark" if samples else "? samples/benchmark"
    if len(samples) > 1:
        samples_str += f" ({', '.join(samples[1:])} for the heavy phases)"
    precision = f", {timer_precision} precision" if timer_precision else ""

    return (
        f"Measured {measured_at(source)} · commit {commit} ({branch}) · {cpu_model()}, {os.cpu_count()} threads · "
        f"BENCH_SIZE = {quads:,} quads · {samples_str} · "
        f"codspeed-divan-compat, wall-clock (os) timer{precision}"
    )


def check_script_syntax(html, out_path):
    """Parse the page's inline JavaScript, and fail the render if it does not.

    One unescaped quote inside a `note:"..."` string is a syntax error, and a
    syntax error in the single inline script takes down EVERY table, tile and
    panel on all three tabs -- the page still opens, still looks structurally
    fine in a text editor, and renders nothing. That shipped once (an
    unescaped `class="mono"` in a panel note), so it is worth a gate rather
    than a habit.

    Uses `node --check`, which is present wherever the JavaScript suite runs.
    Where node is missing the check is skipped with a warning rather than
    blocking a render that is probably fine.
    """
    blocks = re.findall(r"<script>(.*?)</script>", html, re.DOTALL)
    if not blocks:
        return True
    with tempfile.NamedTemporaryFile("w", suffix=".js", encoding="utf-8", delete=False) as f:
        f.write("\n".join(blocks))
        tmp = f.name
    try:
        proc = subprocess.run(["node", "--check", tmp], capture_output=True, text=True)
    except FileNotFoundError:
        print("note: node not found, skipping the page's JavaScript syntax check", file=sys.stderr)
        return True
    finally:
        os.unlink(tmp)
    if proc.returncode == 0:
        return True
    print(
        f"ERROR: {out_path} contains invalid JavaScript -- the whole page would render "
        f"blank. Most likely an unescaped quote in a panel note.\n{proc.stderr.strip()}",
        file=sys.stderr,
    )
    return False


def check_scale_agreement(rust_quads, suites):
    """Say out loud when a suite measured a different row count than the Rust run.

    Every tab is rendered from whatever result file happens to be on disk, and a
    partial refresh (`refresh.sh --only rust,render`) leaves the untouched
    suites' files alone. A stale pilot run is then mixed into the page beside a
    full-scale one -- the KPI row reads its quad count off the Rust run and its
    distinct-term count off the cross-library config, so the tiles can state a
    dataset that never existed. The page cannot detect this; only the render,
    which sees all four configs at once, can.

    Every suite records its row count as `triplesCount`.
    """
    for name, config in suites:
        rows = config.get("triplesCount")
        if rows and rows != rust_quads:
            print(
                f"WARNING: the {name} results measured {rows:,} quads but the Rust run "
                f"measured {rust_quads:,} -- the page will mix both scales. "
                f"Re-run that suite (scripts/refresh.sh --only <stage>,render).",
                file=sys.stderr,
            )


class SuiteError(Exception):
    """A suite's results file the page cannot be built from."""


def load_suite(path, stage):
    """One comparative suite's results, with every key the page reads.

    `core/benches/compare.rs`, `js/bench/compare.bench.ts` and
    `python/bench/run.py` each write one dashboard-shaped JSON object, with the
    keys that suite has:
      - `results`: the rows. A row for an operation a library's API does not
        offer carries `"unsupported": true`, a `reason` and `median_ns: null`,
        so the panels leave it out of the column's best/ratio arithmetic.
      - `memory`: one `{slug, label, role, peakRssMb}` entry per library process.
      - `sizes` (Rust cross-library, Python): one `{slug, label, bytes}` entry
        per library's on-disk artifact; `bytes` is null for a store that lives
        only in memory.
      - `provenance` (JavaScript, Python): the suite's own provenance line.
      - `failures` (JavaScript): the phases a worker could not measure.
      - `config`: the dataset sizes and iteration counts the run used.

    A suite with no file gets every default, and the page renders that tab's
    placeholder instead of its tables. A file that does not read as one JSON
    object raises `SuiteError`, naming the refresh.sh `stage` that writes it.
    """
    data = {}
    if path:
        rerun = f"re-run its stage (scripts/refresh.sh --only {stage},render)"
        try:
            data = json.loads(Path(path).read_text(encoding="utf-8"))
        except (OSError, ValueError) as e:
            raise SuiteError(f"cannot read {path} ({e}) -- {rerun}.") from None
        if not isinstance(data, dict):
            raise SuiteError(f"{path} holds a JSON {type(data).__name__}, not a results object -- {rerun}.")
    defaults = {"results": [], "provenance": "", "memory": [], "failures": [], "sizes": [], "config": {}}
    return {key: data.get(key, default) for key, default in defaults.items()}


def main():
    parser = argparse.ArgumentParser(description="Render the benchmark dashboard (see the module docstring).")
    parser.add_argument("rust_log", type=Path, help="the Rust internals' `cargo bench` log, with its `#dataset` line")
    parser.add_argument("out", type=Path, help="the page to write")
    parser.add_argument("--rust-compare", type=Path, help="core/benches/compare.rs's results")
    parser.add_argument("--js", type=Path, help="js/bench/compare.bench.ts's results")
    parser.add_argument("--python", type=Path, help="python/bench/run.py's results")
    args = parser.parse_args()

    try:
        bench_text = args.rust_log.read_text(encoding="utf-8")
    except OSError as e:
        print(f"ERROR: cannot read the Rust log ({e}) -- run the rust stage first "
              "(scripts/refresh.sh --only rust,render).", file=sys.stderr)
        return 1
    results, timer_precision = parse_bench_output(bench_text)
    if not results:
        print("no benchmark results parsed -- is the input the raw `cargo bench` output?", file=sys.stderr)
        return 1
    dataset = parse_dataset_shape(bench_text)
    if not dataset or not isinstance(dataset.get("quads"), int):
        print(f"ERROR: {args.rust_log} has no `#dataset` line with a quad count. Each Rust bench "
              "target prints one before it measures, and the page takes the run's dataset size "
              "from it; re-run the rust stage (scripts/refresh.sh --only rust,render).",
              file=sys.stderr)
        return 1
    quads = dataset["quads"]

    try:
        compare = load_suite(args.rust_compare, "compare")
        js = load_suite(args.js, "js")
        py = load_suite(args.python, "python")
    except SuiteError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 1
    if args.rust_compare:
        compare["config"]["measuredAt"] = measured_at(args.rust_compare)
    check_scale_agreement(
        quads, [("Rust cross-library", compare["config"]), ("JavaScript", js["config"]), ("Python", py["config"])],
    )

    # One duration format across all four sources (see `fmt_duration`).
    for rows in (results, compare["results"], js["results"], py["results"]):
        normalize_durations(rows)

    # The template's placeholders, each filled with its value as JSON.
    values = {
        "__BENCH_DATA__": results,
        "__PROVENANCE__": build_provenance(results, timer_precision, quads, args.rust_log),
        "__RUST_CONFIG_DATA__": {"benchSize": quads, "dataset": dataset},
        "__RUST_COMPARE_DATA__": compare["results"],
        "__RUST_COMPARE_SIZES__": compare["sizes"],
        "__RUST_COMPARE_MEMORY__": compare["memory"],
        "__RUST_COMPARE_CONFIG__": compare["config"],
        "__JS_BENCH_DATA__": js["results"],
        "__JS_PROVENANCE__": js["provenance"],
        "__JS_MEMORY_DATA__": js["memory"],
        "__JS_FAILURE_DATA__": js["failures"],
        "__JS_CONFIG_DATA__": js["config"],
        "__PY_BENCH_DATA__": py["results"],
        "__PY_PROVENANCE__": py["provenance"],
        "__PY_MEMORY_DATA__": py["memory"],
        "__PY_SIZE_DATA__": py["sizes"],
        "__PY_CONFIG_DATA__": py["config"],
    }
    out = re.sub(r"__[A-Z_]+__", lambda m: json.dumps(values[m.group(0)]),
                 TEMPLATE.read_text(encoding="utf-8"))

    if not check_script_syntax(out, args.out):
        return 1

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(out, encoding="utf-8")
    print(f"parsed {len(results)} Rust + {len(compare['results'])} Rust-compare + {len(js['results'])} JS + "
          f"{len(py['results'])} Python benchmark results ({len(js['memory']) + len(py['memory'])} memory "
          f"readings, {len(py['sizes'])} artifact sizes) -> {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
