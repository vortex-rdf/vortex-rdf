package org.vortexrdf.bench;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Times one cell and appends its dashboard-shaped row, by the rules of {@code measure} in
 * {@code python/bench/worker.py}: setup and teardown run outside the timed region, warm-up runs are discarded, and
 * a phase whose single run exceeds the slow-phase threshold keeps one sample.
 *
 * <p>Garbage is collected once before each cell, never per sample: with a gigabyte-sized store live in the heap, a
 * full collection per sample would cost more than the queries it separates. The median absorbs the occasional
 * pause instead.
 */
public final class Measure {
	@FunctionalInterface
	public interface ThrowingSupplier<T> {
		T get() throws Exception;
	}

	@FunctionalInterface
	public interface ThrowingFunction<T, R> {
		R apply(T t) throws Exception;
	}

	@FunctionalInterface
	public interface ThrowingBiConsumer<T, U> {
		void accept(T t, U u) throws Exception;
	}

	@FunctionalInterface
	public interface ThrowingRunnable {
		void run() throws Exception;
	}

	private final Knobs knobs;

	public Measure(Knobs knobs) {
		this.knobs = knobs;
	}

	/**
	 * Run {@code body} {@code warmup + iters} times, each on the state {@code setup} returns (null when there is no
	 * setup), with {@code teardown} given that state and the body's result, and append one row for {@code id}.
	 * {@code regime} ({@code warm} or {@code cold}) is written only when not null.
	 */
	public <S> void time(String id, String regime, int iters, int warmup, ThrowingSupplier<S> setup,
			ThrowingFunction<S, Object> body, ThrowingBiConsumer<S, Object> teardown, List<Map<String, Object>> rows)
			throws Exception {
		long slow = knobs.slowPhaseNs();
		List<Long> samples = new ArrayList<>(iters);
		System.gc();
		int remainingWarmup = warmup;
		while (samples.size() < iters) {
			S state = setup == null ? null : setup.get();
			long t0 = System.nanoTime();
			Object out = body.apply(state);
			long elapsed = System.nanoTime() - t0;
			if (teardown != null) {
				teardown.accept(state, out);
			}
			if (remainingWarmup > 0) {
				remainingWarmup--;
				// A warm-up run this long says the phase needs no more warming.
				if (slow > 0 && elapsed > slow) {
					remainingWarmup = 0;
				}
				continue;
			}
			samples.add(elapsed);
			if (slow > 0 && samples.size() == 1 && elapsed > slow) {
				break;
			}
		}
		Collections.sort(samples);
		int n = samples.size();
		double median = n % 2 == 1 ? samples.get(n / 2) : (samples.get(n / 2 - 1) + samples.get(n / 2)) / 2.0;
		double mean = samples.stream().mapToLong(Long::longValue).average().orElseThrow();
		Map<String, Object> row = head(id);
		double fastest = samples.get(0), slowest = samples.get(n - 1);
		row.put("fastest", fmt(fastest));
		row.put("slowest", fmt(slowest));
		row.put("median", fmt(median));
		row.put("mean", fmt(mean));
		row.put("fastest_ns", fastest);
		row.put("slowest_ns", slowest);
		row.put("median_ns", median);
		row.put("mean_ns", mean);
		row.put("samples", Integer.toString(n));
		if (regime != null) {
			row.put("regime", regime);
		}
		rows.add(row);
	}

	/**
	 * A row the dashboard renders as "unsupported" rather than a missing cell: no numbers, so the panel leaves it
	 * out of the column's best and ratio arithmetic, and the reason as its tooltip.
	 */
	public static Map<String, Object> unsupported(String id, String reason) {
		Map<String, Object> row = head(id);
		row.put("unsupported", true);
		row.put("reason", reason);
		for (String key : List.of("fastest", "slowest", "median", "mean")) {
			row.put(key, "unsupported");
		}
		for (String key : List.of("fastest_ns", "slowest_ns", "median_ns", "mean_ns")) {
			row.put(key, null);
		}
		row.put("samples", "0");
		return row;
	}

	/** Repeat {@code round} until the JIT warm-up budget has passed; nothing is recorded. */
	public void warmJit(ThrowingRunnable round) throws Exception {
		if (knobs.jitWarmupMs() <= 0) {
			return;
		}
		long end = System.nanoTime() + knobs.jitWarmupMs() * 1_000_000;
		do {
			round.run();
		} while (System.nanoTime() < end);
	}

	/** A row's group, variant and id: the id split at its first {@code ::}, as worker.py splits it. */
	private static Map<String, Object> head(String id) {
		int cut = id.indexOf("::");
		Map<String, Object> row = new LinkedHashMap<>();
		row.put("group", cut < 0 ? id : id.substring(0, cut));
		row.put("variant", cut < 0 || cut + 2 == id.length() ? null : id.substring(cut + 2));
		row.put("id", id);
		return row;
	}

	/** The duration format of {@code fmt_duration} in {@code scripts/render_bench_dashboard.py}. */
	static String fmt(double ns) {
		if (ns < 999.5) {
			return String.format(Locale.ROOT, "%.0f ns", ns);
		}
		String[] units = { "µs", "ms", "s" };
		double[] scales = { 1e3, 1e6, 1e9 };
		for (int i = 0; i < units.length; i++) {
			if (ns < scales[i] * 1000 || i == units.length - 1) {
				String v = String.format(Locale.ROOT, "%.2f", ns / scales[i]).replaceAll("0+$", "")
						.replaceAll("\\.$", "");
				return v + " " + units[i];
			}
		}
		throw new AssertionError("unreachable");
	}
}
