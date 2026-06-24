#!/usr/bin/env python3
from rdflib import Graph
from pathlib import Path
import time
import statistics
import random
import json
import io
import gc
import argparse
import contextlib
import csv


def split_dbbench_queries(path: Path):
    """
    DBBench files appear to contain one SPARQL SELECT query per non-empty line.
    Keep each line as-is; do not rewrite LIMIT/OFFSET/etc.
    """
    queries = []
    raw = path.read_text(encoding="utf-8", errors="replace")

    for line_no, line in enumerate(raw.splitlines(), start=1):
        q = line.strip()
        if not q:
            continue
        if not q.lower().startswith("select"):
            continue
        queries.append((line_no, q))

    return queries


def iter_query_files(query_root: Path, dataset: str, groups, join_sizes):
    files = []

    if "TP" in groups:
        tp_root = query_root / "TP" / dataset
        if tp_root.exists():
            files.extend(sorted(tp_root.glob("*.txt")))

    if "JOINS" in groups:
        for size in join_sizes:
            join_root = query_root / "JOINS" / dataset / size
            if join_root.exists():
                files.extend(sorted(join_root.glob("*.txt")))

    return files


def build_query_records(query_root: Path, dataset: str, groups, join_sizes):
    records = []

    for path in iter_query_files(query_root, dataset, groups, join_sizes):
        rel = path.relative_to(query_root)
        queries = split_dbbench_queries(path)

        for idx, (line_no, query) in enumerate(queries):
            parts = rel.parts
            top_group = parts[0]
            size_group = None

            if top_group == "JOINS":
                size_group = parts[2]

            records.append({
                "query_id": f"{rel}::q{idx:04d}",
                "relative_path": str(rel),
                "top_group": top_group,
                "dataset": dataset,
                "size_group": size_group,
                "file_name": path.name,
                "query_index_in_file": idx,
                "line_no": line_no,
                "contains_limit": "limit" in query.lower(),
                "query": query,
            })

    return records


def make_graph(engine: str, cottas_path: str, vortex_path: str, vortex_layout: str):
    if engine == "cottas":
        from pycottas.cottas_store import COTTASStore
        return Graph(store=COTTASStore(cottas_path))

    if engine == "vortex":
        from vortex_rdflib import VortexStore
        return Graph(store=VortexStore(
            vortex_path,
            layout=vortex_layout,
            backend="native",
        ))

    if engine == "vortex-duckdb":
        from vortex_rdflib import VortexStore
        return Graph(store=VortexStore(
            vortex_path,
            layout=vortex_layout,
            backend="duckdb",
        ))

    raise ValueError(f"Unknown engine: {engine}")


def run_one_query(graph: Graph, query: str, silence_stdout: bool):
    """
    Apples-to-apples:
    - use RDFLib Graph.query for both
    - fully materialize result list for both
    - no custom LIMIT pushdown
    - no query rewriting
    """
    gc.collect()

    sink = io.StringIO()
    stdout_ctx = contextlib.redirect_stdout(
        sink) if silence_stdout else contextlib.nullcontext()

    start_ns = time.perf_counter_ns()
    with stdout_ctx:
        rows = list(graph.query(query))
    end_ns = time.perf_counter_ns()

    return {
        "elapsed_s": (end_ns - start_ns) / 1_000_000_000,
        "result_count": len(rows),
    }


def summarize(results):
    grouped = {}

    for row in results:
        if row["status"] != "ok" or row["phase"] != "measured":
            continue

        key = (
            row["engine"],
            row["query_id"],
            row["relative_path"],
            row["top_group"],
            row["size_group"],
        )
        grouped.setdefault(key, []).append(row["elapsed_s"])

    out = []

    for key, times in grouped.items():
        engine, query_id, relative_path, top_group, size_group = key
        out.append({
            "engine": engine,
            "query_id": query_id,
            "relative_path": relative_path,
            "top_group": top_group,
            "size_group": size_group,
            "runs": len(times),
            "mean_s": statistics.mean(times),
            "median_s": statistics.median(times),
            "min_s": min(times),
            "max_s": max(times),
            "stdev_s": statistics.stdev(times) if len(times) > 1 else 0.0,
        })

    return out


def write_csv(path: Path, rows):
    if not rows:
        path.write_text("", encoding="utf-8")
        return

    fieldnames = list(rows[0].keys())

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--query-root", required=True)
    parser.add_argument("--dataset", default="dbpedia")
    parser.add_argument("--groups", nargs="+",
                        default=["TP", "JOINS"], choices=["TP", "JOINS"])
    parser.add_argument("--join-sizes", nargs="+",
                        default=["small", "big"], choices=["small", "big"])

    parser.add_argument("--cottas-path", required=True)
    parser.add_argument("--vortex-path", required=True)
    parser.add_argument("--vortex-layout", default="cottas-native-strings")

    parser.add_argument("--warmup-runs", type=int, default=1)
    parser.add_argument("--measured-runs", type=int, default=5)
    parser.add_argument("--shuffle", action="store_true")
    parser.add_argument("--seed", type=int, default=42)

    parser.add_argument("--max-queries", type=int, default=None)
    parser.add_argument("--only-file-contains", default=None)

    parser.add_argument("--out-prefix", default="dbbench_rdflib")
    parser.add_argument("--no-silence-stdout", action="store_true")
    parser.add_argument(
        "--engines",
        nargs="+",
        default=["cottas", "vortex"],
        choices=["cottas", "vortex", "vortex-duckdb"],
    )

    args = parser.parse_args()

    query_root = Path(args.query_root)
    out_prefix = Path(args.out_prefix)

    query_records = build_query_records(
        query_root=query_root,
        dataset=args.dataset,
        groups=args.groups,
        join_sizes=args.join_sizes,
    )

    if args.only_file_contains:
        query_records = [
            r for r in query_records
            if args.only_file_contains in r["relative_path"]
        ]

    if args.shuffle:
        rng = random.Random(args.seed)
        rng.shuffle(query_records)

    if args.max_queries is not None:
        query_records = query_records[:args.max_queries]

    print(f"Loaded {len(query_records)} individual queries")

    inventory_path = out_prefix.with_suffix(".queries.json")
    inventory_path.write_text(json.dumps(
        query_records, indent=2), encoding="utf-8")
    print(f"Wrote query inventory: {inventory_path}")

    graphs = {
        engine: make_graph(engine, args.cottas_path,
                           args.vortex_path, args.vortex_layout)
        for engine in args.engines
    }

    results = []
    silence_stdout = not args.no_silence_stdout

    for qrec_idx, qrec in enumerate(query_records):
        print(f"[{qrec_idx + 1}/{len(query_records)}] {qrec['query_id']}")

        counts_by_engine = {}

        for engine in args.engines:
            graph = graphs[engine]

            total_runs = args.warmup_runs + args.measured_runs

            for run_idx in range(total_runs):
                phase = "warmup" if run_idx < args.warmup_runs else "measured"
                measured_run_idx = run_idx - args.warmup_runs

                row = {
                    "engine": engine,
                    "phase": phase,
                    "run": measured_run_idx if phase == "measured" else run_idx,
                    "query_id": qrec["query_id"],
                    "relative_path": qrec["relative_path"],
                    "top_group": qrec["top_group"],
                    "dataset": qrec["dataset"],
                    "size_group": qrec["size_group"],
                    "file_name": qrec["file_name"],
                    "query_index_in_file": qrec["query_index_in_file"],
                    "line_no": qrec["line_no"],
                    "contains_limit": qrec["contains_limit"],
                    "status": "ok",
                    "elapsed_s": None,
                    "result_count": None,
                    "error": None,
                }

                try:
                    out = run_one_query(
                        graph, qrec["query"], silence_stdout=silence_stdout)
                    row.update(out)

                    if phase == "measured" and measured_run_idx == 0:
                        counts_by_engine[engine] = row["result_count"]

                except Exception as e:
                    row["status"] = "error"
                    row["error"] = repr(e)

                results.append(row)

                print(
                    f"  {engine:13s} {phase:8s} run={row['run']} "
                    f"status={row['status']} time={row['elapsed_s']} rows={row['result_count']}"
                )

        if set(args.engines) == {"cottas", "vortex"} and "cottas" in counts_by_engine and "vortex" in counts_by_engine:
            if counts_by_engine["cottas"] != counts_by_engine["vortex"]:
                print(
                    "  WARNING count mismatch: "
                    f"cottas={counts_by_engine['cottas']} "
                    f"vortex={counts_by_engine['vortex']}"
                )

    raw_json = out_prefix.with_suffix(".raw.json")
    raw_csv = out_prefix.with_suffix(".raw.csv")
    summary_json = out_prefix.with_suffix(".summary.json")
    summary_csv = out_prefix.with_suffix(".summary.csv")

    raw_json.write_text(json.dumps(results, indent=2), encoding="utf-8")
    write_csv(raw_csv, results)

    summary_rows = summarize(results)
    summary_json.write_text(json.dumps(
        summary_rows, indent=2), encoding="utf-8")
    write_csv(summary_csv, summary_rows)

    print(f"Wrote {raw_json}")
    print(f"Wrote {raw_csv}")
    print(f"Wrote {summary_json}")
    print(f"Wrote {summary_csv}")


if __name__ == "__main__":
    main()
