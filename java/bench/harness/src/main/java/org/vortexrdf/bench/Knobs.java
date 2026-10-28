package org.vortexrdf.bench;

/**
 * The repetition counts, as {@code python/bench/worker.py} has them: a selective query is cheap enough to repeat,
 * a build or a full materialization is not.
 *
 * @param slowPhaseNs a phase whose single run already exceeds this keeps one sample; 0 disables the rule
 * @param jitWarmupMs the JIT warm-up budget, split evenly between the patterns: each runs, discarded, for its share
 *        before any pattern is timed
 */
public record Knobs(int queryIters, int queryWarmup, int heavyIters, int fullScanIters, long slowPhaseNs,
		long jitWarmupMs) {
	public static Knobs fromEnv() {
		return new Knobs(intEnv("JAVA_BENCH_QUERY_ITERS", 10), intEnv("JAVA_BENCH_QUERY_WARMUP", 5),
				intEnv("JAVA_BENCH_HEAVY_ITERS", 3), intEnv("JAVA_BENCH_FULL_SCAN_ITERS", 3),
				(long) (Double.parseDouble(env("BENCH_SLOW_PHASE_MS", "30000")) * 1_000_000),
				Long.parseLong(env("JAVA_BENCH_JIT_WARMUP_MS", "2000")));
	}

	private static String env(String name, String fallback) {
		String v = System.getenv(name);
		return v == null || v.isBlank() ? fallback : v.trim();
	}

	private static int intEnv(String name, int fallback) {
		return Integer.parseInt(env(name, Integer.toString(fallback)));
	}
}
