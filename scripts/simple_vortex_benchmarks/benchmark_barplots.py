#!/usr/bin/env python3

import argparse
import csv
import os
import statistics
import subprocess
import time
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np


# =========================
# Default query values
# =========================
DEFAULT_SUBJECT = "<http://dbpedia.org/resource/James_A._Michener>"
DEFAULT_PREDICATE = "<http://www.w3.org/2004/02/skos/core#subject1>"
DEFAULT_OBJECT = \
    "<http://dbpedia.org/resource/Category%3AUniversity_of_Tartu_faculty>"

# Available dataset sizes
VALID_SIZES = ["500K", "1M", "5M", "10M"]

# Benchmark dimensions
ORDERINGS = ["none", "SPO", "OSP", "PSO"]
PROFILES = ["balanced", "compact"]

# Match scenarios
MATCH_SCENARIOS = {
    "S": ("subject",),
    "P": ("predicate",),
    "O": ("object",),
    "SP": ("subject", "predicate"),
    "SO": ("subject", "object"),
    "PO": ("predicate", "object"),
    "SPO": ("subject", "predicate", "object"),
}


# =========================
# Helpers
# =========================
def mean_std(values):
    if not values:
        return 0.0, 0.0
    if len(values) == 1:
        return values[0], 0.0
    return statistics.mean(values), statistics.stdev(values)


def ensure_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)


def build_configs(input_file: str, work_dir: Path):
    """
    Build all cottas-native-strings configurations:
    2 compression profiles x 4 orderings = 8 configurations.
    """
    configs = []

    for profile in PROFILES:
        for ordering in ORDERINGS:
            config_name = f"cns_{profile}_{ordering.lower()}"
            output_file = work_dir / f"{config_name}.vortex"

            serialize_cmd = [
                "target/release/vortex-rdf-cli", "serialize",
                "--index-type", "simple-dictionary",
                "--storage-layout", "cottas-native-strings",
                "--compression-profile", profile,
                "--ordering", ordering,
                "--input", input_file,
                "--output", str(output_file),
            ]

            configs.append({
                "name": config_name,
                "label": f"{profile}-{ordering}",
                "profile": profile,
                "ordering": ordering,
                "output_file": output_file,
                "serialize_cmd": serialize_cmd,
            })

    return configs


def build_match_cmd(config, scenario_name,
                    scenario_fields, values, work_dir: Path):
    """
    Build a match command for one configuration and one match scenario.
    """
    output_file = work_dir / f"{config['name']}_{scenario_name}_result.nq"

    cmd = [
        "target/release/vortex-rdf-cli", "match",
        "--input", str(config["output_file"]),
        "--storage-layout", "cottas-native-strings",
        "--index-type", "simple-dictionary",
        "--output", str(output_file),
    ]

    for field in scenario_fields:
        if field == "subject":
            cmd += ["--subject", values["subject"]]
        elif field == "predicate":
            cmd += ["--predicate", values["predicate"]]
        elif field == "object":
            cmd += ["--object", values["object"]]

    return cmd


def run_command(cmd, verbose=True):
    if verbose:
        print(" ".join(cmd))
    subprocess.run(cmd, check=True)


# =========================
# Plotting
# =========================
def plot_serialize_times(configs, serialize_results,
                         size_label, runs, out_path: Path):
    labels = [cfg["label"] for cfg in configs]
    means = [mean_std(serialize_results[cfg["name"]])[0] for cfg in configs]
    stds = [mean_std(serialize_results[cfg["name"]])[1] for cfg in configs]

    x = np.arange(len(labels))

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.bar(x, means, yerr=stds, capsize=5)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=30, ha="right")
    ax.set_ylabel("Seconds")
    ax.set_title(
        f"Serialization Time for cottas-native-strings "
        f"(dbpedia_{size_label}.nt, {runs} runs)"
    )

    plt.tight_layout()
    plt.savefig(out_path, dpi=300)
    plt.close(fig)


def plot_file_sizes(configs, file_sizes, original_size_mb,
                    size_label, runs, out_path: Path):
    labels = ["original"] + [cfg["label"] for cfg in configs]
    means = [original_size_mb] + \
        [mean_std(file_sizes[cfg["name"]])[0] for cfg in configs]
    stds = [0.0] + [mean_std(file_sizes[cfg["name"]])[1] for cfg in configs]

    x = np.arange(len(labels))

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.bar(x, means, yerr=stds, capsize=5)
    ax.set_xticks(x)
    ax.set_xticklabels(labels, rotation=30, ha="right")
    ax.set_ylabel("MB")
    ax.set_title(
        f"Serialized File Sizes for cottas-native-strings "
        f"(dbpedia_{size_label}.nt, {runs} runs)"
    )

    plt.tight_layout()
    plt.savefig(out_path, dpi=300)
    plt.close(fig)


def plot_match_times(configs, match_results, size_label, runs, out_path: Path):
    scenario_names = list(MATCH_SCENARIOS.keys())

    n_scenarios = len(scenario_names)
    n_configs = len(configs)
    x = np.arange(n_scenarios)

    width = 0.1
    # Center bars around each scenario tick
    offsets = [(i - (n_configs - 1) / 2) * width for i in range(n_configs)]

    fig, ax = plt.subplots(figsize=(16, 7))

    for i, cfg in enumerate(configs):
        means = []
        stds = []

        for scenario in scenario_names:
            key = (cfg["name"], scenario)
            mean_val, std_val = mean_std(match_results[key])
            means.append(mean_val)
            stds.append(std_val)

        ax.bar(
            x + offsets[i],
            means,
            width=width,
            yerr=stds,
            capsize=3,
            label=cfg["label"]
        )

    ax.set_xticks(x)
    ax.set_xticklabels(scenario_names)
    ax.set_ylabel("Seconds")
    ax.set_title(
        f"Match Time by Query Pattern for cottas-native-strings "
        f"(dbpedia_{size_label}.nt, {runs} runs)"
    )
    ax.legend(title="Configuration", bbox_to_anchor=(
        1.02, 1), loc="upper left")

    plt.tight_layout()
    plt.savefig(out_path, dpi=300, bbox_inches="tight")
    plt.close(fig)


# =========================
# CSV export
# =========================
def save_summary_csv(configs, serialize_results, file_sizes,
                     match_results, size_label, out_path: Path):
    """
    One row per configuration with serialization and size summary.
    """
    with open(out_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "dataset_size",
            "configuration",
            "compression_profile",
            "ordering",
            "serialize_mean_s",
            "serialize_std_s",
            "file_size_mean_mb",
            "file_size_std_mb",
        ])

        for cfg in configs:
            s_mean, s_std = mean_std(serialize_results[cfg["name"]])
            fs_mean, fs_std = mean_std(file_sizes[cfg["name"]])

            writer.writerow([
                size_label,
                cfg["label"],
                cfg["profile"],
                cfg["ordering"],
                s_mean,
                s_std,
                fs_mean,
                fs_std,
            ])


def save_summary_csv_no_ser(configs, file_sizes,
                            match_results, size_label, out_path: Path):
    """
    One row per configuration with serialization and size summary.
    """
    with open(out_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "dataset_size",
            "configuration",
            "compression_profile",
            "ordering",
            "serialize_mean_s",
            "serialize_std_s",
            "file_size_mean_mb",
            "file_size_std_mb",
        ])

        for cfg in configs:
            fs_mean, fs_std = mean_std(file_sizes[cfg["name"]])

            writer.writerow([
                size_label,
                cfg["label"],
                cfg["profile"],
                cfg["ordering"],
                fs_mean,
                fs_std,
            ])


def save_match_csv(configs, match_results, size_label, out_path: Path):
    """
    One row per configuration per match scenario.
    """
    with open(out_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "dataset_size",
            "configuration",
            "scenario",
            "match_mean_s",
            "match_std_s",
        ])

        for cfg in configs:
            for scenario in MATCH_SCENARIOS.keys():
                m_mean, m_std = mean_std(
                    match_results[(cfg["name"], scenario)])
                writer.writerow([
                    size_label,
                    cfg["label"],
                    scenario,
                    m_mean,
                    m_std,
                ])


# =========================
# Benchmark execution
# =========================
def benchmark_dataset(size_label, runs, subject, predicate,
                      obj, output_root: Path, verbose=True):
    input_file = f"dbpedia_{size_label}.nt"

    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Input file not found: {input_file}")

    dataset_dir = output_root / f"dbpedia_{size_label}"
    ensure_dir(dataset_dir)

    vortex_dir = dataset_dir / "vortex_files"
    result_dir = dataset_dir / "match_outputs"
    plot_dir = dataset_dir / "plots"
    csv_dir = dataset_dir / "csv"

    ensure_dir(vortex_dir)
    ensure_dir(result_dir)
    ensure_dir(plot_dir)
    ensure_dir(csv_dir)

    configs = build_configs(input_file, vortex_dir)

    query_values = {
        "subject": subject,
        "predicate": predicate,
        "object": obj,
    }

    # Result storage
    # serialize_results = {cfg["name"]: [] for cfg in configs}
    file_sizes = {cfg["name"]: [] for cfg in configs}
    match_results = {
        (cfg["name"], scenario): []
        for cfg in configs
        for scenario in MATCH_SCENARIOS.keys()
    }

    original_size_mb = os.path.getsize(input_file) / (1024 * 1024)

    print(f"\n=== Benchmark: dbpedia_{size_label}.nt ===")
    print(f"Original file size: {original_size_mb:.2f} MB")

    for run_idx in range(runs):
        print(f"\nRun {run_idx + 1}/{runs}")

        # -------------------------
        # Serialize all configs
        # -------------------------
        # for cfg in configs:
        # start = time.perf_counter()
        # run_command(cfg["serialize_cmd"], verbose=verbose)
        # end = time.perf_counter()

        # elapsed = end - start
        # serialize_results[cfg["name"]].append(elapsed)

        # size_mb = os.path.getsize(cfg["output_file"]) / (1024 * 1024)
        # file_sizes[cfg["name"]].append(size_mb)

        # print(
        #    f" serialize {cfg['label']:<16} "
        #    f"{elapsed:>8.3f}s   {size_mb:>8.2f} MB"
        # )

        # -------------------------
        # Match all scenarios
        # -------------------------
        for cfg in configs:
            for scenario_name, scenario_fields in MATCH_SCENARIOS.items():
                cmd = build_match_cmd(
                    cfg,
                    scenario_name,
                    scenario_fields,
                    query_values,
                    result_dir
                )

                start = time.perf_counter()
                run_command(cmd, verbose=verbose)
                end = time.perf_counter()

                elapsed = end - start
                match_results[(cfg["name"], scenario_name)].append(elapsed)

                print(
                    f" match     {cfg['label']:<16} "
                    f"{scenario_name:<3}  {elapsed:>8.3f}s"
                )

    # Save CSV summaries
    save_summary_csv_no_ser(
        configs=configs,
        file_sizes=file_sizes,
        match_results=match_results,
        size_label=size_label,
        out_path=csv_dir / f"summary_dbpedia_{size_label}.csv",
    )

    save_match_csv(
        configs=configs,
        match_results=match_results,
        size_label=size_label,
        out_path=csv_dir / f"match_summary_dbpedia_{size_label}.csv",
    )

    # Save plots
    #plot_serialize_times(
    #    configs=configs,
    #    serialize_results=serialize_results,
    #    size_label=size_label,
    #    runs=runs,
    #    out_path=plot_dir / f"serialize_times_dbpedia_{size_label}.png",
    #)

    plot_file_sizes(
        configs=configs,
        file_sizes=file_sizes,
        original_size_mb=original_size_mb,
        size_label=size_label,
        runs=runs,
        out_path=plot_dir / f"file_sizes_dbpedia_{size_label}.png",
    )

    plot_match_times(
        configs=configs,
        match_results=match_results,
        size_label=size_label,
        runs=runs,
        out_path=plot_dir / f"match_times_dbpedia_{size_label}.png",
    )

    print(f"\nFinished dbpedia_{size_label}. Results saved in: {dataset_dir}")


# =========================
# Main
# =========================
def main():
    parser = argparse.ArgumentParser(
        description=("Benchmark cottas-native-strings with multiple orderings,"
                     " compression profiles, dataset sizes,"
                     " and match scenarios.")
    )

    parser.add_argument(
        "--sizes",
        nargs="+",
        choices=VALID_SIZES,
        default=VALID_SIZES,
        help="Dataset sizes to benchmark (default: all)."
    )

    parser.add_argument(
        "--runs",
        type=int,
        default=10,
        help="Number of runs per configuration (default: 10)."
    )

    parser.add_argument(
        "--subject",
        default=DEFAULT_SUBJECT,
        help="Subject URI used in match benchmarks."
    )

    parser.add_argument(
        "--predicate",
        default=DEFAULT_PREDICATE,
        help="Predicate URI used in match benchmarks."
    )

    parser.add_argument(
        "--object",
        dest="obj",
        default=DEFAULT_OBJECT,
        help="Object URI used in match benchmarks."
    )

    parser.add_argument(
        "--output-root",
        default="benchmark_results",
        help="Root directory where results will be stored."
    )

    parser.add_argument(
        "--no-build",
        action="store_true",
        help="Skip cargo build --release."
    )

    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Do not print full commands before execution."
    )

    args = parser.parse_args()

    output_root = Path(args.output_root)
    ensure_dir(output_root)

    # Build once before all experiments
    if not args.no_build:
        print("Building project in release mode...")
        subprocess.run(["cargo", "build", "--release"], check=True)

    for size_label in args.sizes:
        benchmark_dataset(
            size_label=size_label,
            runs=args.runs,
            subject=args.subject,
            predicate=args.predicate,
            obj=args.obj,
            output_root=output_root,
            verbose=not args.quiet,
        )


if __name__ == "__main__":
    main()
