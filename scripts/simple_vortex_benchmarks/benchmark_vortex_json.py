#!/usr/bin/env python3

import argparse
import json
import os
import statistics
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path


DEFAULT_SUBJECT = "<http://dbpedia.org/resource/James_A._Michener>"
DEFAULT_PREDICATE = "<http://www.w3.org/2004/02/skos/core#subject1>"
DEFAULT_OBJECT =\
    "<http://dbpedia.org/resource/Category%3AUniversity_of_Tartu_faculty>"

VALID_SIZES = ["500K", "1M", "5M", "10M"]
ORDERINGS = ["none", "SPO", "OSP", "PSO"]
PROFILES = ["balanced", "compact"]

MATCH_SCENARIOS = {
    "S": ("subject",),
    "P": ("predicate",),
    "O": ("object",),
    "SP": ("subject", "predicate"),
    "SO": ("subject", "object"),
    "PO": ("predicate", "object"),
    "SPO": ("subject", "predicate", "object"),
}


def mean_std(values):
    if not values:
        return 0.0, 0.0
    if len(values) == 1:
        return values[0], 0.0
    return statistics.mean(values), statistics.stdev(values)


def ensure_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)


def build_configs(input_file: str, work_dir: Path):
    configs = []

    for profile in PROFILES:
        for ordering in ORDERINGS:
            config_name = f"vortex_{profile}_{ordering.lower()}"
            output_file = work_dir / f"{config_name}.vortex"

            serialize_cmd = [
                "target/release/vortex-rdf-cli",
                "serialize",
                "--index-type",
                "simple-dictionary",
                "--storage-layout",
                "cottas-native-strings",
                "--compression-profile",
                profile,
                "--ordering",
                ordering,
                "--input",
                input_file,
                "--output",
                str(output_file),
            ]

            configs.append({
                "system": "vortex",
                "name": config_name,
                "label": f"vortex-{profile}-{ordering}",
                "profile": profile,
                "ordering": ordering,
                "output_file": output_file,
                "serialize_cmd": serialize_cmd,
            })

    return configs


def build_match_cmd(config, scenario_name, scenario_fields, query_values,
                    result_dir: Path):
    output_file = result_dir / f"{config['name']}_{scenario_name}.nq"

    cmd = [
        "target/release/vortex-rdf-cli",
        "match",
        "--input",
        str(config["output_file"]),
        "--storage-layout",
        "cottas-native-strings",
        "--index-type",
        "simple-dictionary",
        "--output",
        str(output_file),
    ]

    for field in scenario_fields:
        if field == "subject":
            cmd += ["--subject", query_values["subject"]]
        elif field == "predicate":
            cmd += ["--predicate", query_values["predicate"]]
        elif field == "object":
            cmd += ["--object", query_values["object"]]

    return cmd, output_file


def run_command(cmd, verbose=True):
    if verbose:
        print(" ".join(cmd))
    subprocess.run(cmd, check=True)


def count_lines(file_path: Path):
    if not file_path.exists():
        return 0
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        return sum(1 for _ in f)


def benchmark_dataset(size_label, runs, query_values, output_root: Path,
                      skip_serialize=False, verbose=True):
    input_file = f"dbpedia_{size_label}.nt"

    dataset_dir = output_root / f"dbpedia_{size_label}"
    vortex_dir = dataset_dir / "vortex_files"
    result_dir = dataset_dir / "match_outputs"

    ensure_dir(dataset_dir)
    ensure_dir(vortex_dir)
    ensure_dir(result_dir)

    # Only require the input .nt file when serialization is enabled
    if not skip_serialize and not os.path.exists(input_file):
        raise FileNotFoundError(f"Input file not found: {input_file}")

    configs = build_configs(input_file, vortex_dir)

    if os.path.exists(input_file):
        input_size_mb = os.path.getsize(input_file) / (1024 * 1024)
    else:
        input_size_mb = None

    dataset_result = {
        "dataset_size": size_label,
        "input_file": input_file,
        "input_size_mb": input_size_mb,
        "runs": runs,
        "query_values": query_values,
        "results": [],
    }

    print(f"\n=== Vortex benchmark: dbpedia_{size_label} ===")
    if input_size_mb is not None:
        print(f"Input size: {input_size_mb:.2f} MB")
    else:
        print("Input size: (skip-serialize mode and input file not present")

    for config in configs:
        serialize_runs = []
        file_size_runs = []
        match_runs = {scenario: [] for scenario in MATCH_SCENARIOS.keys()}
        result_counts = {}

        # In skip-serialize mode, validate once and record existing file size
        if skip_serialize:
            if not config["output_file"].exists():
                raise FileNotFoundError(
                    f"Missing serialized file for configuration "
                    f"{config['label']}: {config['output_file']}"
                )
            existing_size_mb = os.path.getsize(config["output_file"])\
                / (1024 * 1024)
            file_size_runs.append(existing_size_mb)

        for run_idx in range(runs):
            print(f"\n[{config['label']}] run {run_idx + 1}/{runs}")

            # Serialize (optional)
            if not skip_serialize:
                start = time.perf_counter()
                run_command(config["serialize_cmd"], verbose=verbose)
                end = time.perf_counter()

                serialize_elapsed = end - start
                serialize_runs.append(serialize_elapsed)

                output_size_mb = os.path.getsize(config["output_file"])\
                    / (1024 * 1024)
                file_size_runs.append(output_size_mb)

                print(
                    f" serialize {config['label']:<24} "
                    f"{serialize_elapsed:>8.3f}s   {output_size_mb:>8.2f} MB"
                )

            # Match scenarios
            for scenario_name, scenario_fields in MATCH_SCENARIOS.items():
                cmd, out_file = build_match_cmd(
                    config=config,
                    scenario_name=scenario_name,
                    scenario_fields=scenario_fields,
                    query_values=query_values,
                    result_dir=result_dir,
                )

                start = time.perf_counter()
                run_command(cmd, verbose=verbose)
                end = time.perf_counter()

                elapsed = end - start
                match_runs[scenario_name].append(elapsed)

                if scenario_name not in result_counts:
                    result_counts[scenario_name] = count_lines(out_file)

                print(
                    f" match     {config['label']:<24} "
                    f"{scenario_name:<3} {elapsed:>8.3f}s   "
                    f"rows={result_counts[scenario_name]}"
                )

        serialize_mean, serialize_std = mean_std(serialize_runs)
        file_size_mean, file_size_std = mean_std(file_size_runs)

        config_result = {
            "system": "vortex",
            "configuration": {
                "profile": config["profile"],
                "ordering": config["ordering"],
                "storage_layout": "cottas-native-strings",
                "index_type": "simple-dictionary",
            },
            "serialize": {
                "skipped": skip_serialize,
                "runs_s": serialize_runs,
                "mean_s": serialize_mean,
                "std_s": serialize_std,
            },
            "file_size": {
                "runs_mb": file_size_runs,
                "mean_mb": file_size_mean,
                "std_mb": file_size_std,
            },
            "match": {},
        }

        for scenario_name, scenario_values in match_runs.items():
            m_mean, m_std = mean_std(scenario_values)
            config_result["match"][scenario_name] = {
                "runs_s": scenario_values,
                "mean_s": m_mean,
                "std_s": m_std,
                "result_count": result_counts.get(scenario_name, 0),
            }

        dataset_result["results"].append(config_result)

    return dataset_result


def main():
    parser = argparse.ArgumentParser(
        description="Benchmark Vortex RDF CLI and export JSON results."
    )
    parser.add_argument(
        "--sizes",
        nargs="+",
        choices=VALID_SIZES,
        default=VALID_SIZES,
        help="Dataset sizes to benchmark."
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=10,
        help="Runs per configuration."
    )
    parser.add_argument("--subject", default=DEFAULT_SUBJECT)
    parser.add_argument("--predicate", default=DEFAULT_PREDICATE)
    parser.add_argument("--object", dest="obj", default=DEFAULT_OBJECT)
    parser.add_argument("--output-root", default="benchmark_vortex_results")
    parser.add_argument(
        "--json-out",
        default="benchmark_vortex_results/vortex_results.json"
    )
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--quiet", action="store_true")
    parser.add_argument(
        "--skip-serialize",
        action="store_true",
        help="Skip serialization and reuse existing .vortex files."
    )

    args = parser.parse_args()

    output_root = Path(args.output_root)
    ensure_dir(output_root)

    if not args.no_build:
        print("Building project in release mode...")
        subprocess.run(["cargo", "build", "--release"], check=True)

    query_values = {
        "subject": args.subject,
        "predicate": args.predicate,
        "object": args.obj,
    }

    all_results = {
        "benchmark_name": "vortex",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "runs": args.runs,
        "skip_serialize": args.skip_serialize,
        "datasets": [],
    }

    for size_label in args.sizes:
        dataset_result = benchmark_dataset(
            size_label=size_label,
            runs=args.runs,
            query_values=query_values,
            output_root=output_root,
            skip_serialize=args.skip_serialize,
            verbose=not args.quiet,
        )
        all_results["datasets"].append(dataset_result)

    json_out = Path(args.json_out)
    ensure_dir(json_out.parent)
    with open(json_out, "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2)

    print(f"\nSaved Vortex JSON results to: {json_out}")


if __name__ == "__main__":
    main()
