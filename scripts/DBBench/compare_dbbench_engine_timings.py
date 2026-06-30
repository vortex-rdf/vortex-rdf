#!/usr/bin/env python3
import argparse
import csv
from pathlib import Path
from statistics import mean, median


def read_raw(path: Path):
    rows = []
    with path.open(newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row["status"] != "ok":
                continue
            if row["phase"] != "measured":
                continue

            row["elapsed_s"] = float(row["elapsed_s"])
            row["result_count"] = int(row["result_count"])
            rows.append(row)

    return rows


def summarize_engine(rows):
    grouped = {}

    for r in rows:
        key = (
            r["query_id"],
            r["relative_path"],
            r["file_name"],
            r["query_index_in_file"],
            r["line_no"],
        )
        grouped.setdefault(key, []).append(r)

    out = {}

    for key, rs in grouped.items():
        times = [r["elapsed_s"] for r in rs]
        counts = {r["result_count"] for r in rs}

        out[key] = {
            "runs": len(rs),
            "mean_s": mean(times),
            "median_s": median(times),
            "min_s": min(times),
            "max_s": max(times),
            "result_counts": sorted(counts),
        }

    return out


def fmt(x):
    if x is None:
        return ""
    return f"{x:.6f}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--dataset", default="dbpedia")
    ap.add_argument(
        "--engines",
        nargs="+",
        default=["cottas", "vortex", "vortex-duckdb"],
    )
    args = ap.parse_args()

    out_dir = Path(args.out_dir)

    summaries = {}

    for engine in args.engines:
        raw_path = out_dir / f"{args.dataset}_{engine}.raw.csv"
        if not raw_path.exists():
            raise FileNotFoundError(raw_path)

        summaries[engine] = summarize_engine(read_raw(raw_path))

    all_keys = sorted(set().union(*(s.keys() for s in summaries.values())))

    rows = []

    for key in all_keys:
        query_id, relative_path, file_name, query_index_in_file, line_no = key

        row = {
            "query_id": query_id,
            "relative_path": relative_path,
            "file_name": file_name,
            "query_index_in_file": query_index_in_file,
            "line_no": line_no,
        }

        counts_by_engine = {}

        for engine in args.engines:
            s = summaries[engine].get(key)

            if s is None:
                row[f"{engine}_median_s"] = None
                row[f"{engine}_mean_s"] = None
                row[f"{engine}_min_s"] = None
                row[f"{engine}_max_s"] = None
                row[f"{engine}_runs"] = None
                row[f"{engine}_result_counts"] = None
                continue

            row[f"{engine}_median_s"] = s["median_s"]
            row[f"{engine}_mean_s"] = s["mean_s"]
            row[f"{engine}_min_s"] = s["min_s"]
            row[f"{engine}_max_s"] = s["max_s"]
            row[f"{engine}_runs"] = s["runs"]
            row[f"{engine}_result_counts"] = ";".join(map(str, s["result_counts"]))

            counts_by_engine[engine] = tuple(s["result_counts"])

        if "cottas" in summaries and "vortex" in summaries:
            c = summaries["cottas"].get(key)
            v = summaries["vortex"].get(key)

            if c and v:
                row["vortex_vs_cottas_median_speedup"] = (
                    c["median_s"] / v["median_s"]
                    if v["median_s"] > 0
                    else None
                )
                row["vortex_minus_cottas_median_s"] = v["median_s"] - c["median_s"]
            else:
                row["vortex_vs_cottas_median_speedup"] = None
                row["vortex_minus_cottas_median_s"] = None

        if "vortex-duckdb" in summaries and "vortex" in summaries:
            d = summaries["vortex-duckdb"].get(key)
            v = summaries["vortex"].get(key)

            if d and v:
                row["vortex_vs_duckdb_median_speedup"] = (
                    d["median_s"] / v["median_s"]
                    if v["median_s"] > 0
                    else None
                )
            else:
                row["vortex_vs_duckdb_median_speedup"] = None

        unique_counts = set(counts_by_engine.values())
        row["count_match"] = len(unique_counts) == 1

        rows.append(row)

    out_csv = out_dir / f"{args.dataset}_engine_comparison.csv"

    fieldnames = list(rows[0].keys()) if rows else []
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)

    print(f"Wrote {out_csv}")

    print()
    print("Slowest vortex queries:")
    vortex_rows = [
        r for r in rows
        if r.get("vortex_median_s") is not None
    ]
    vortex_rows.sort(key=lambda r: r["vortex_median_s"], reverse=True)

    for r in vortex_rows[:20]:
        print(
            f"{r['vortex_median_s']:.6f}s "
            f"count_match={r['count_match']} "
            f"speedup_vs_cottas={r.get('vortex_vs_cottas_median_speedup')} "
            f"{r['query_id']}"
        )


if __name__ == "__main__":
    main()