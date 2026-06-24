#!/usr/bin/env python3

import argparse
import json
import os
import statistics
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple


DEFAULT_SUBJECT = "<http://dbpedia.org/resource/James_A._Michener>"
DEFAULT_PREDICATE = "<http://www.w3.org/2004/02/skos/core#subject1>"
DEFAULT_OBJECT = "<http://dbpedia.org/resource/Category%3AUniversity_of_Tartu_faculty>"
DEFAULT_MISS_SUBJECT = "<http://example.org/definitely-not-present-subject>"

VALID_SIZES = ["500K", "1M", "5M", "10M"]
ORDERINGS = ["none", "SPO", "OSP", "PSO"]
PROFILES = ["balanced", "compact"]

# Your original benchmark scenarios
MATCH_SCENARIOS = {
    "S": ("subject",),
    "P": ("predicate",),
    "O": ("object",),
    "SP": ("subject", "predicate"),
    "SO": ("subject", "object"),
    "PO": ("predicate", "object"),
    "SPO": ("subject", "predicate", "object"),
    # Useful for proving pruning: should ideally touch ~0 candidate row groups
    "MISS_S": ("miss_subject",),
}


# ---------- helpers ----------

def mean_std(values: List[float]) -> Tuple[float, float]:
    if not values:
        return 0.0, 0.0
    if len(values) == 1:
        return values[0], 0.0
    return statistics.mean(values), statistics.stdev(values)


def ensure_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)


def run_command(cmd: List[str], verbose: bool = True):
    if verbose:
        print(" ".join(cmd))
    subprocess.run(cmd, check=True)


def count_lines(file_path: Path) -> int:
    if not file_path.exists():
        return 0
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        return sum(1 for _ in f)


def load_json(path: Path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


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


def build_match_diag_cmd(
    config: Dict,
    scenario_name: str,
    scenario_fields: Tuple[str, ...],
    query_values: Dict[str, str],
    result_dir: Path,
    diagnostics_dir: Path,
    diagnostics_mode: str,
):
    output_file = result_dir / f"{config['name']}_{scenario_name}.nq"
    diagnostics_file = diagnostics_dir / f"{config['name']}_{scenario_name}.json"

    # Assumed CLI contract for the new Rust diagnostics path.
    # Adjust the flag/subcommand names here if your CLI differs.
    if diagnostics_mode == "match-with-diagnostics":
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
            "--diagnostics-out",
            str(diagnostics_file),
        ]
    elif diagnostics_mode == "match-plus-flags":
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
            "--diagnostics-out",
            str(diagnostics_file),
        ]
    else:
        raise ValueError(f"Unsupported diagnostics mode: {diagnostics_mode}")

    for field in scenario_fields:
        if field == "subject":
            cmd += ["--subject", query_values["subject"]]
        elif field == "predicate":
            cmd += ["--predicate", query_values["predicate"]]
        elif field == "object":
            cmd += ["--object", query_values["object"]]
        elif field == "miss_subject":
            cmd += ["--subject", query_values["miss_subject"]]
        else:
            raise ValueError(f"Unknown scenario field: {field}")

    return cmd, output_file, diagnostics_file


def make_metric_bucket() -> Dict[str, List[float]]:
    return {
        "open_ms": [],
        "scan_build_ms": [],
        "stream_init_ms": [],
        "read_all_ms": [],
        "serialize_ms": [],
        "total_ms": [],
        "rows_out": [],
        "vortex_can_prune_numeric": [],
        "total_row_groups": [],
        "candidate_row_groups": [],
        "candidate_rows_upper_bound": [],
        "candidate_group_ratio": [],
        "candidate_row_ratio_upper_bound": [],
        "rchar_delta": [],
        "wchar_delta": [],
        "syscr_delta": [],
        "syscw_delta": [],
        "read_bytes_delta": [],
        "write_bytes_delta": [],
    }


def bool_to_numeric(v: Optional[bool]) -> Optional[float]:
    if v is None:
        return None
    return 1.0 if v else 0.0


def add_if_not_none(bucket: Dict[str, List[float]], key: str, value):
    if value is not None:
        bucket[key].append(value)


def summarize_bucket(bucket: Dict[str, List[float]]) -> Dict[str, Dict[str, float]]:
    out = {}
    for key, values in bucket.items():
        mean_v, std_v = mean_std(values)
        out[key] = {
            "runs": values,
            "mean": mean_v,
            "std": std_v,
            "count": len(values),
        }
    return out


def extract_diagnostics(diag_json: Dict):
    # Expected shape from the Rust code you added:
    # {
    #   "timings": {
    #     "open_ms": ..., "scan_build_ms": ..., "stream_init_ms": ...,
    #     "read_all_ms": ..., "serialize_ms": ..., "total_ms": ...,
    #     "rows_out": ..., "vortex_can_prune": ..., "total_row_groups": ...,
    #     "candidate_row_groups": ..., "candidate_rows_upper_bound": ...
    #   },
    #   "proc_io": {
    #     "rchar_delta": ..., "wchar_delta": ..., "syscr_delta": ...,
    #     "syscw_delta": ..., "read_bytes_delta": ..., "write_bytes_delta": ...
    #   }
    # }
    timings = diag_json.get("timings", {})
    proc_io = diag_json.get("proc_io") or {}

    total_row_groups = timings.get("total_row_groups")
    candidate_row_groups = timings.get("candidate_row_groups")
    candidate_rows_upper_bound = timings.get("candidate_rows_upper_bound")
    rows_out = timings.get("rows_out")

    candidate_group_ratio = None
    if total_row_groups not in (None, 0) and candidate_row_groups is not None:
        candidate_group_ratio = candidate_row_groups / total_row_groups

    candidate_row_ratio_upper_bound = None
    if candidate_rows_upper_bound is not None and rows_out not in (None, 0):
        candidate_row_ratio_upper_bound = candidate_rows_upper_bound / rows_out

    return {
        "open_ms": timings.get("open_ms"),
        "scan_build_ms": timings.get("scan_build_ms"),
        "stream_init_ms": timings.get("stream_init_ms"),
        "read_all_ms": timings.get("read_all_ms"),
        "serialize_ms": timings.get("serialize_ms"),
        "total_ms": timings.get("total_ms"),
        "rows_out": rows_out,
        "vortex_can_prune_numeric": bool_to_numeric(timings.get("vortex_can_prune")),
        "total_row_groups": total_row_groups,
        "candidate_row_groups": candidate_row_groups,
        "candidate_rows_upper_bound": candidate_rows_upper_bound,
        "candidate_group_ratio": candidate_group_ratio,
        "candidate_row_ratio_upper_bound": candidate_row_ratio_upper_bound,
        "rchar_delta": proc_io.get("rchar_delta"),
        "wchar_delta": proc_io.get("wchar_delta"),
        "syscr_delta": proc_io.get("syscr_delta"),
        "syscw_delta": proc_io.get("syscw_delta"),
        "read_bytes_delta": proc_io.get("read_bytes_delta"),
        "write_bytes_delta": proc_io.get("write_bytes_delta"),
        # keep the raw JSON too, in case you later extend the Rust struct
        "_raw": diag_json,
    }


def benchmark_dataset(
    size_label,
    runs,
    query_values,
    output_root: Path,
    skip_serialize=False,
    verbose=True,
    diagnostics_mode="match-with-diagnostics",
):
    input_file = f"dbpedia_{size_label}.nt"

    dataset_dir = output_root / f"dbpedia_{size_label}"
    vortex_dir = dataset_dir / "vortex_files"
    result_dir = dataset_dir / "match_outputs"
    diagnostics_dir = dataset_dir / "diagnostics"

    ensure_dir(dataset_dir)
    ensure_dir(vortex_dir)
    ensure_dir(result_dir)
    ensure_dir(diagnostics_dir)

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

    print(f"\n=== Vortex diagnostics benchmark: dbpedia_{size_label} ===")
    if input_size_mb is not None:
        print(f"Input size: {input_size_mb:.2f} MB")
    else:
        print("Input size: (skip-serialize mode and input file not present)")

    for config in configs:
        serialize_runs = []
        file_size_runs = []
        scenario_runtime_runs = {scenario: [] for scenario in MATCH_SCENARIOS.keys()}
        scenario_diag_buckets = {scenario: make_metric_bucket() for scenario in MATCH_SCENARIOS.keys()}
        scenario_result_counts = {}
        raw_diagnostics_by_scenario = {scenario: [] for scenario in MATCH_SCENARIOS.keys()}

        if skip_serialize:
            if not config["output_file"].exists():
                raise FileNotFoundError(
                    f"Missing serialized file for configuration {config['label']}: {config['output_file']}"
                )
            existing_size_mb = os.path.getsize(config["output_file"]) / (1024 * 1024)
            file_size_runs.append(existing_size_mb)

        for run_idx in range(runs):
            print(f"\n[{config['label']}] run {run_idx + 1}/{runs}")

            if not skip_serialize:
                start = time.perf_counter()
                run_command(config["serialize_cmd"], verbose=verbose)
                end = time.perf_counter()

                serialize_elapsed = end - start
                serialize_runs.append(serialize_elapsed)

                output_size_mb = os.path.getsize(config["output_file"]) / (1024 * 1024)
                file_size_runs.append(output_size_mb)

                print(
                    f" serialize {config['label']:<24} {serialize_elapsed:>8.3f}s   {output_size_mb:>8.2f} MB"
                )

            for scenario_name, scenario_fields in MATCH_SCENARIOS.items():
                cmd, out_file, diag_file = build_match_diag_cmd(
                    config=config,
                    scenario_name=scenario_name,
                    scenario_fields=scenario_fields,
                    query_values=query_values,
                    result_dir=result_dir,
                    diagnostics_dir=diagnostics_dir,
                    diagnostics_mode=diagnostics_mode,
                )

                if diag_file.exists():
                    diag_file.unlink()

                start = time.perf_counter()
                run_command(cmd, verbose=verbose)
                end = time.perf_counter()
                wall_elapsed = end - start
                scenario_runtime_runs[scenario_name].append(wall_elapsed)

                if not diag_file.exists():
                    raise FileNotFoundError(
                        f"Diagnostics file was not created for {config['label']} / {scenario_name}: {diag_file}\n"
                        f"Make sure your CLI writes JSON diagnostics to --diagnostics-out."
                    )

                diag_json = load_json(diag_file)
                extracted = extract_diagnostics(diag_json)
                raw_diagnostics_by_scenario[scenario_name].append(extracted["_raw"])

                for metric_name, metric_value in extracted.items():
                    if metric_name == "_raw":
                        continue
                    add_if_not_none(scenario_diag_buckets[scenario_name], metric_name, metric_value)

                if scenario_name not in scenario_result_counts:
                    scenario_result_counts[scenario_name] = count_lines(out_file)

                prune_flag = diag_json.get("timings", {}).get("vortex_can_prune")
                candidate_groups = diag_json.get("timings", {}).get("candidate_row_groups")
                total_groups = diag_json.get("timings", {}).get("total_row_groups")
                read_all_ms = diag_json.get("timings", {}).get("read_all_ms")
                total_ms = diag_json.get("timings", {}).get("total_ms")
                read_bytes = (diag_json.get("proc_io") or {}).get("read_bytes_delta")

                print(
                    f" diag      {config['label']:<24} {scenario_name:<6} "
                    f"wall={wall_elapsed:>7.3f}s total_ms={float(total_ms or 0):>8.2f} "
                    f"read_all_ms={float(read_all_ms or 0):>8.2f} rows={scenario_result_counts[scenario_name]:>6} "
                    f"prune={str(prune_flag):<5} groups={candidate_groups}/{total_groups} read_bytes={read_bytes}"
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
            "diagnostics": {},
        }

        for scenario_name in MATCH_SCENARIOS.keys():
            wall_mean, wall_std = mean_std(scenario_runtime_runs[scenario_name])

            config_result["diagnostics"][scenario_name] = {
                "wall_clock": {
                    "runs_s": scenario_runtime_runs[scenario_name],
                    "mean_s": wall_mean,
                    "std_s": wall_std,
                    "result_count": scenario_result_counts.get(scenario_name, 0),
                },
                "metrics": summarize_bucket(scenario_diag_buckets[scenario_name]),
                # keep a copy of raw diagnostics per run for future debugging
                "raw_runs": raw_diagnostics_by_scenario[scenario_name],
            }

        dataset_result["results"].append(config_result)

    return dataset_result


def main():
    parser = argparse.ArgumentParser(
        description="Benchmark Vortex RDF CLI with pruning/timing diagnostics and export JSON results."
    )
    parser.add_argument(
        "--sizes",
        nargs="+",
        choices=VALID_SIZES,
        default=VALID_SIZES,
        help="Dataset sizes to benchmark.",
    )
    parser.add_argument("--runs", type=int, default=10, help="Runs per configuration.")
    parser.add_argument("--subject", default=DEFAULT_SUBJECT)
    parser.add_argument("--predicate", default=DEFAULT_PREDICATE)
    parser.add_argument("--object", dest="obj", default=DEFAULT_OBJECT)
    parser.add_argument(
        "--miss-subject",
        default=DEFAULT_MISS_SUBJECT,
        help="Subject for a guaranteed-miss pruning sanity check.",
    )
    parser.add_argument("--output-root", default="benchmark_vortex_diagnostics_results")
    parser.add_argument(
        "--json-out",
        default="benchmark_vortex_diagnostics_results/vortex_diagnostics_results.json",
    )
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--quiet", action="store_true")
    parser.add_argument(
        "--skip-serialize",
        action="store_true",
        help="Skip serialization and reuse existing .vortex files.",
    )
    parser.add_argument(
        "--diagnostics-mode",
        choices=["match-with-diagnostics", "match-plus-flags"],
        default="match-with-diagnostics",
        help=(
            "How your CLI exposes diagnostics. "
            "Use 'match-with-diagnostics' if you added a dedicated subcommand, "
            "or 'match-plus-flags' if you kept the original 'match' subcommand and added --diagnostics-out."
        ),
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
        "miss_subject": args.miss_subject,
    }

    all_results = {
        "benchmark_name": "vortex_diagnostics",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "runs": args.runs,
        "skip_serialize": args.skip_serialize,
        "diagnostics_mode": args.diagnostics_mode,
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
            diagnostics_mode=args.diagnostics_mode,
        )
        all_results["datasets"].append(dataset_result)

    json_out = Path(args.json_out)
    ensure_dir(json_out.parent)
    with open(json_out, "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2)

    print(f"\nSaved Vortex diagnostics JSON results to: {json_out}")


if __name__ == "__main__":
    main()
