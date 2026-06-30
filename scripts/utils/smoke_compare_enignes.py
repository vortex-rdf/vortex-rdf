#!/usr/bin/env python3

import argparse
import json
import subprocess
import sys
import tempfile
from pathlib import Path


QUERY_ROOT = Path("/Users/fotioskalioras/Documents/Github/DBPedia/queries")
COTTAS = "/Users/fotioskalioras/Documents/Github/DBPedia/data/dbpedia_en_all.cottas"
VORTEX = "/Users/fotioskalioras/Documents/Github/vortex-rdf/data/vortex_cottas_strings/dbpedia_CNSCompact.vortex"
VORTEX_LAYOUT = "cottas-native-strings"

ENGINES = ["cottas", "vortex", "vortex-duckdb"]


def split_dbbench_queries(path: Path):
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


def build_first_queries(max_queries: int):
    records = []
    tp_root = QUERY_ROOT / "TP" / "dbpedia"

    for path in sorted(tp_root.glob("*.txt")):
        rel = path.relative_to(QUERY_ROOT)
        queries = split_dbbench_queries(path)

        for idx, (line_no, query) in enumerate(queries):
            records.append({
                "query_id": f"{rel}::q{idx:04d}",
                "relative_path": str(rel),
                "line_no": line_no,
                "query": query,
            })

            if len(records) >= max_queries:
                return records

    return records


def make_graph(engine: str):
    from rdflib import Graph

    if engine == "cottas":
        from pycottas.cottas_store import COTTASStore
        return Graph(store=COTTASStore(COTTAS))

    if engine == "vortex":
        from vortex_rdflib import VortexStore
        return Graph(store=VortexStore(
            VORTEX,
            layout=VORTEX_LAYOUT,
            backend="native",
        ))

    if engine == "vortex-duckdb":
        from vortex_rdflib import VortexStore
        return Graph(store=VortexStore(
            VORTEX,
            layout=VORTEX_LAYOUT,
            backend="duckdb",
        ))

    raise ValueError(engine)


def serialize_result_row(row):
    """
    Convert RDFLib ResultRow to deterministic comparable JSON.
    Keeps variable names and RDF terms in n3 form.
    """
    d = row.asdict()
    return {
        str(var): term.n3() if term is not None else None
        for var, term in d.items()
    }


def run_worker(engine: str, queries_path: Path, out_path: Path):
    queries = json.loads(queries_path.read_text())
    graph = make_graph(engine)

    out = []

    for i, qrec in enumerate(queries, start=1):
        print(f"[{engine}] [{i}/{len(queries)}] {qrec['query_id']}", flush=True)

        row = {
            "engine": engine,
            "query_id": qrec["query_id"],
            "relative_path": qrec["relative_path"],
            "line_no": qrec["line_no"],
            "status": "ok",
            "result_count": None,
            "results": None,
            "error": None,
        }

        try:
            result_rows = list(graph.query(qrec["query"]))

            serialized = [
                serialize_result_row(r)
                for r in result_rows
            ]

            # Sort deterministically but preserve duplicates.
            serialized_sorted = sorted(
                serialized,
                key=lambda x: json.dumps(x, sort_keys=True),
            )

            row["result_count"] = len(serialized_sorted)
            row["results"] = serialized_sorted

        except Exception as e:
            row["status"] = "error"
            row["error"] = repr(e)
            row["result_count"] = None
            row["results"] = None

        out.append(row)

    out_path.write_text(json.dumps(out, indent=2), encoding="utf-8")


def run_parent(max_queries: int):
    tmp = Path(tempfile.mkdtemp(prefix="smoke_compare_exact_"))
    queries_path = tmp / "queries.json"

    queries = build_first_queries(max_queries)
    queries_path.write_text(json.dumps(queries, indent=2), encoding="utf-8")

    print("Temp dir:", tmp)
    print("Queries:", len(queries))
    print("First query:", queries[0]["query_id"] if queries else None)

    paths = {}

    for engine in ENGINES:
        out_path = tmp / f"{engine}.json"
        paths[engine] = out_path

        cmd = [
            sys.executable,
            __file__,
            "--worker",
            "--engine", engine,
            "--queries", str(queries_path),
            "--out", str(out_path),
        ]

        print(f"\n=== Running {engine} ===")
        subprocess.run(cmd, check=True)

    loaded = {
        engine: {
            row["query_id"]: row
            for row in json.loads(path.read_text())
        }
        for engine, path in paths.items()
    }

    print("\n=== Comparing exact results ===")

    mismatches = []
    all_query_ids = [q["query_id"] for q in queries]

    for qid in all_query_ids:
        rows_by_engine = {
            engine: loaded[engine].get(qid)
            for engine in ENGINES
        }

        # Missing row safety.
        if any(v is None for v in rows_by_engine.values()):
            mismatches.append((qid, "missing", rows_by_engine))
            continue

        statuses = {
            engine: rows_by_engine[engine]["status"]
            for engine in ENGINES
        }

        counts = {
            engine: rows_by_engine[engine]["result_count"]
            for engine in ENGINES
        }

        results = {
            engine: rows_by_engine[engine]["results"]
            for engine in ENGINES
        }

        same_status = len(set(statuses.values())) == 1
        same_counts = len(set(counts.values())) == 1
        same_results = (
            results["cottas"] == results["vortex"] ==
            results["vortex-duckdb"]
        )

        if not (same_status and same_counts and same_results):
            mismatches.append((qid, "different", rows_by_engine))

    print(f"Total queries: {len(all_query_ids)}")
    print(f"Mismatches:    {len(mismatches)}")

    if not mismatches:
        print("✅ All exact results match.")
    else:
        print("❌ Mismatches found.")

        for qid, kind, rows_by_engine in mismatches[:20]:
            print("\n---")
            print("Query:", qid)
            print("Kind:", kind)

            for engine in ENGINES:
                r = rows_by_engine.get(engine)
                if r is None:
                    print(f"{engine:15} MISSING")
                    continue

                print(
                    f"{engine:15} "
                    f"status={r['status']} "
                    f"count={r['result_count']} "
                    f"error={r['error']}"
                )

            # Print small result samples for diagnosis.
            print("Samples:")
            for engine in ENGINES:
                r = rows_by_engine.get(engine)
                if r and r["results"] is not None:
                    print(f"  {engine}:")
                    for item in r["results"][:3]:
                        print("   ", item)

        mismatch_path = tmp / "mismatches.json"
        mismatch_path.write_text(
            json.dumps([
                {
                    "query_id": qid,
                    "kind": kind,
                    "rows_by_engine": rows_by_engine,
                }
                for qid, kind, rows_by_engine in mismatches
            ], indent=2),
            encoding="utf-8",
        )

        print("\nWrote mismatch details:", mismatch_path)

    print("\nAll outputs in:", tmp)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-queries", type=int, default=20)

    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--engine")
    parser.add_argument("--queries")
    parser.add_argument("--out")

    args = parser.parse_args()

    if args.worker:
        run_worker(
            engine=args.engine,
            queries_path=Path(args.queries),
            out_path=Path(args.out),
        )
    else:
        run_parent(args.max_queries)


if __name__ == "__main__":
    main()