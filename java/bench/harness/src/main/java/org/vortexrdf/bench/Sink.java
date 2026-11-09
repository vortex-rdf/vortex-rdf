package org.vortexrdf.bench;

/**
 * Where every materialized term's length lands. An adapter sums its lengths locally and folds the sum in once per
 * call, so the JIT cannot drop the reads that produce them: the same contract as the Rust and JavaScript harnesses'
 * black-boxed accumulators and the Python harness's {@code consume}.
 */
public final class Sink {
	private static volatile long published;

	private Sink() {
	}

	public static void fold(long n) {
		published = published + n;
	}

	public static long published() {
		return published;
	}
}
