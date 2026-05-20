import subprocess
import os
import re
import statistics
import matplotlib.pyplot as plt
import numpy as np

# -----------------------------
# CONFIGURATION
# -----------------------------
BASE_CMD = ["target/release/vortex-rdf-cli.exe"]

INPUT = "kg.nq"
VORTEX_FILE = "temp.vortex"
MATCH_OUTPUT = "temp.nq"

RUNS_PER_CONFIG = 10

MATCH_SUBJECT = "<http://example.org/ocel/event/E8>"

CONFIGS = [
    {
        "name": "chained-hash + cottas-spog",
        "args": ["serialize", "--index-type", "chained-hash", "--storage-layout", "cottas-spog"]
    },
    {
        "name": "simple-dictionary",
        "args": ["serialize", "--index-type", "simple-dictionary"]
    },
    {
        "name": "chained-hash",
        "args": ["serialize", "--index-type", "chained-hash"]
    },
    {
        "name": "simple-dictionary + cottas-spog",
        "args": ["serialize", "--index-type", "simple-dictionary", "--storage-layout", "cottas-spog"]
    }
]

# -----------------------------
# SERIALIZE METRICS
# -----------------------------
SERIALIZE_PATTERNS = {
    "collect": r"Collected .* in ([\d\.]+)(ms|µs)",
    "dict_build": r"Dictionary building and sort took ([\d\.]+)(ms|µs)",
    "vortex_encode": r"Vortex encoding .* took ([\d\.]+)(ms|µs)",
    "dict_encode": r"Dictionary encoding took ([\d\.]+)(ms|µs)",
    "write": r"Vortex writing took ([\d\.]+)(ms|µs)",
    "total": r"Fully serialized .* in ([\d\.]+)(ms|µs)"
}

# -----------------------------
# MATCH METRICS
# -----------------------------
MATCH_PATTERNS = {
    "index_ref": r"index reference created in ([\d\.]+)(ms|µs)",
    "subject_cmp": r"Subject comparison took ([\d\.]+)(ms|µs)",
    "pattern_total": r"Pattern matching took overall ([\d\.]+)(ms|µs)",
    "apply_pattern": r"Applying match pattern took ([\d\.]+)(ms|µs)",
    "write_loop": r"Serialization/write loop took ([\d\.]+)(ms|µs)",
    "total": r"Full matching operation took ([\d\.]+)(ms|µs)"
}


def convert_to_ms(value, unit):
    val = float(value)
    return val / 1000 if unit == "µs" else val


def parse_output(output, patterns):
    results = {}
    for key, pattern in patterns.items():
        match = re.search(pattern, output)
        if match:
            results[key] = convert_to_ms(*match.groups())
    return results


def run_command(cmd):
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        env={**os.environ, "RUST_LOG": "vortex_rdf_cli=debug,vortex_rdf_core=debug"}
    )
    return result.stdout + "\n" + result.stderr


# -----------------------------
# RUN BENCHMARKS
# -----------------------------
serialize_results = {}
match_results = {}

for cfg in CONFIGS:
    print(f"Running config: {cfg['name']}")

    serialize_metrics = {k: [] for k in SERIALIZE_PATTERNS}
    match_metrics = {k: [] for k in MATCH_PATTERNS}

    for _ in range(RUNS_PER_CONFIG):

        # ---- SERIALIZE ----
        serialize_cmd = (
            BASE_CMD + cfg["args"] +
            ["--input", INPUT, "--output", VORTEX_FILE]
        )

        out_ser = run_command(serialize_cmd)
        parsed_ser = parse_output(out_ser, SERIALIZE_PATTERNS)

        for k, v in parsed_ser.items():
            serialize_metrics[k].append(v)

        # ---- MATCH ----
        match_cmd = [
            *BASE_CMD,
            "match",
            "-i", VORTEX_FILE,
            "-o", MATCH_OUTPUT,
            "--subject", MATCH_SUBJECT
        ]

        out_match = run_command(match_cmd)
        parsed_match = parse_output(out_match, MATCH_PATTERNS)

        for k, v in parsed_match.items():
            match_metrics[k].append(v)

    serialize_results[cfg["name"]] = serialize_metrics
    match_results[cfg["name"]] = match_metrics


# -----------------------------
# AGGREGATION
# -----------------------------
def compute_stats(results):
    means, stds = {}, {}
    for cfg, metrics in results.items():
        means[cfg], stds[cfg] = {}, {}

        for m, vals in metrics.items():
            if vals:
                means[cfg][m] = statistics.mean(vals)
                stds[cfg][m] = statistics.stdev(vals) if len(vals) > 1 else 0
            else:
                means[cfg][m] = 0
                stds[cfg][m] = 0
    return means, stds


ser_means, ser_stds = compute_stats(serialize_results)
match_means, match_stds = compute_stats(match_results)


# -----------------------------
# PLOTTING FUNCTION
# -----------------------------
def plot_grid(means, stds, title):
    metrics = list(next(iter(means.values())).keys())

    cols = 3
    rows = int(np.ceil(len(metrics) / cols))

    fig, axes = plt.subplots(rows, cols, figsize=(16, 10))
    axes = axes.flatten()

    cfg_names = list(means.keys())

    for i, metric in enumerate(metrics):
        ax = axes[i]

        vals = [means[cfg][metric] for cfg in cfg_names]
        errs = [stds[cfg][metric] for cfg in cfg_names]

        x = np.arange(len(cfg_names))

        ax.bar(x, vals, yerr=errs)
        ax.set_title(metric)
        ax.set_xticks(x)
        ax.set_xticklabels(cfg_names, rotation=30, ha="right")
        ax.set_ylabel("ms")

    for j in range(i + 1, len(axes)):
        fig.delaxes(axes[j])

    fig.suptitle(title)
    plt.tight_layout()
    plt.show()


# -----------------------------
# PLOTS
# -----------------------------
plot_grid(ser_means, ser_stds, "Serialize Benchmark")
plot_grid(match_means, match_stds, "Match Benchmark")