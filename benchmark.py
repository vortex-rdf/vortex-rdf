#!/usr/bin/env python3
import subprocess
import time
import statistics
import matplotlib.pyplot as plt

INPUT = "dbpedia_5M.nt"
SUBJECT = "<http://dbpedia.org/resource/James_A._Michener>"
RUNS = 10

commands = {
    "serialize_flat": [
        "target/release/vortex-rdf-cli", "serialize",
        "--index-type", "simple-dictionary",
        "--storage-layout", "default",
        "--input", INPUT,
        "--output", "flat.vortex"
    ],
    "serialize_cottas_eager": [
        "target/release/vortex-rdf-cli", "serialize",
        "--index-type", "simple-dictionary",
        "--storage-layout", "cottas-spog",
        "--input", INPUT,
        "--output", "cottas_eager.vortex"
    ],
    "serialize_cottas_native": [
        "target/release/vortex-rdf-cli", "serialize",
        "--index-type", "simple-dictionary",
        "--storage-layout", "cottas-native",
        "--input", INPUT,
        "--output", "cottas_native.vortex"
    ],
    "match_flat": [
        "target/release/vortex-rdf-cli", "match",
        "--input", "flat.vortex",
        "--storage-layout", "default",
        "--index-type", "simple-dictionary",
        "--subject", SUBJECT,
        "--output", "flat_result.nq"
    ],
    "match_cottas_eager": [
        "target/release/vortex-rdf-cli", "match",
        "--input", "cottas_eager.vortex",
        "--storage-layout", "cottas-spog",
        "--index-type", "simple-dictionary",
        "--subject", SUBJECT,
        "--output", "cottas_eager_result.nq"
    ],
    "match_cottas_native": [
        "target/release/vortex-rdf-cli", "match",
        "--input", "cottas_native.vortex",
        "--storage-layout", "cottas-native",
        "--index-type", "simple-dictionary",
        "--subject", SUBJECT,
        "--output", "cottas_native_result.nq"
    ]
}

results = {key: [] for key in commands}

subprocess.run(["cargo", "build", "--release"], check=True)

for i in range(RUNS):
    print(f"Run {i+1}/{RUNS}")
    for name, cmd in commands.items():
        start = time.perf_counter()
        subprocess.run(cmd, check=True)
        end = time.perf_counter()
        duration = end - start
        results[name].append(duration)
        print(f"  {name}: {duration:.3f}s")

avg_results = {k: statistics.mean(v) for k, v in results.items()}

plt.figure(figsize=(10,6))
for name, times in results.items():
    plt.plot(range(1, RUNS+1), times, marker='o', label=name)

plt.xlabel("Run")
plt.ylabel("Time (seconds)")
plt.title("Benchmark Results over 10 Runs")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.savefig("benchmark_plot.png")

print("\nAverage times:")
for k, v in avg_results.items():
    print(f"{k}: {v:.3f}s")

print("\nPlot saved to benchmark_plot.png")
