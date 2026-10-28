package org.vortexrdf.bench;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Measures ONE adapter in its own JVM, the way {@code python/bench/worker.py} measures one Python library in its
 * own process: peak memory is attributable to one library, and no library's garbage or JIT profile taints another's
 * timings.
 *
 * <p>The result goes to the {@code --out} file as one JSON object, never to stdout, which the libraries' own
 * logging may write to. A phase that throws becomes a failure naming the cells it leaves empty, and the worker
 * carries on with the next phase.
 */
public final class Worker {
	static final String OPEN_UNSUPPORTED = "no artifact to reopen: a fresh process re-parses the source, which is the Build column";
	static final String NO_GRAPHS = "this library's model has no graphs -- it reads the triples projection";

	private Worker() {
	}

	public static Map<String, Object> query(Adapter<?, ?> a, WorkerFiles f, Path workdir, Knobs k) {
		Query<?, ?> q = new Query<>(a, f, workdir, k);
		try {
			return q.run();
		} catch (Throwable t) {
			// An error no phase expects ends the role, but not the rows it already measured.
			return q.ended(t);
		}
	}

	public static Map<String, Object> mutate(Adapter<?, ?> a, WorkerFiles f, Path workdir, Knobs k) {
		List<Map<String, Object>> rows = new ArrayList<>();
		List<Map<String, Object>> failures = new ArrayList<>();
		try {
			return runMutate(a, f, workdir, k, rows, failures);
		} catch (Throwable t) {
			failures.add(failure("worker", List.of(), t));
			return result(a, "mutate", rows, Map.of(), failures, false, k, null);
		}
	}

	/** What an empty worker JVM reports: which JDK it ran on, with no library loaded. */
	public static Map<String, Object> baseline() {
		Map<String, Object> out = new LinkedHashMap<>();
		out.put("role", "baseline");
		out.put("jdk", jdk());
		return out;
	}

	/** {@code --slug S --role query|mutate|baseline --quads --triples --probes --fresh --prefix --empty --workdir --out}. */
	public static void main(String[] args) throws Exception {
		Map<String, String> opt = new HashMap<>();
		for (int i = 0; i + 1 < args.length; i += 2) {
			if (!args[i].startsWith("--")) {
				throw new IllegalArgumentException("expected --name value pairs, got " + args[i]);
			}
			opt.put(args[i].substring(2), args[i + 1]);
		}
		String role = required(opt, "role");
		Map<String, Object> result;
		if (role.equals("baseline")) {
			result = baseline();
		} else {
			Adapter<?, ?> adapter = find(required(opt, "slug"));
			WorkerFiles files = new WorkerFiles(path(opt, "quads"), path(opt, "triples"), path(opt, "probes"),
					path(opt, "fresh"), path(opt, "prefix"), path(opt, "empty"));
			Path workdir = Files.createDirectories(path(opt, "workdir"));
			Knobs knobs = Knobs.fromEnv();
			result = switch (role) {
			case "query" -> query(adapter, files, workdir, knobs);
			case "mutate" -> mutate(adapter, files, workdir, knobs);
			default -> throw new IllegalArgumentException("unknown role " + role);
			};
		}
		Files.writeString(path(opt, "out"), Json.write(result), StandardCharsets.UTF_8);
	}

	/**
	 * The query role: build, open, then every pattern warm, cold, and the full scan, and what the open store retains:
	 * the memory, in the heap and outside it, that disposing the warm handle releases.
	 */
	private static final class Query<H, Q> {
		private final Adapter<H, Q> a;
		private final WorkerFiles f;
		private final Knobs k;
		private final Measure m;
		private final String slug;
		private final Path src;
		private final Path location;
		private final boolean cold;
		private final List<Map<String, Object>> rows = new ArrayList<>();
		private final Map<String, Object> counts = new LinkedHashMap<>();
		private final List<Map<String, Object>> failures = new ArrayList<>();
		private Footprint retained;

		Query(Adapter<H, Q> a, WorkerFiles f, Path workdir, Knobs k) {
			this.a = a;
			this.f = f;
			this.k = k;
			this.m = new Measure(k);
			this.slug = a.slug();
			this.src = a.supportsQuads() ? f.quads() : f.triples();
			this.location = workdir.resolve("store");
			this.cold = a.persistent();
		}

		@SuppressWarnings("unchecked")
		Map<String, Object> run() {
			List<Pattern> probes;
			try {
				probes = Inputs.probes(f.probes());
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			}
			// One pattern set: the triple patterns, plus the graph-bound ones when the library has graphs.
			List<Pattern> patterns = probes.stream().filter(p -> p.kind() == Pattern.Kind.TRIPLES
					|| (p.kind() == Pattern.Kind.QUADS && a.supportsQuads())).toList();
			Pattern full = probes.stream().filter(p -> p.kind() == Pattern.Kind.FULL).findFirst().orElseThrow();

			try {
				m.time("ingest::" + slug, null, k.heavyIters(), 0, () -> {
					clear(location);
					return null;
				}, s -> a.build(src, location), (s, h) -> a.dispose((H) h), rows);
			} catch (Exception | OutOfMemoryError e) {
				// Nothing else in this role can run without a store.
				List<String> lost = new ArrayList<>(List.of("ingest::" + slug));
				if (cold) {
					lost.add("open::" + slug);
				}
				for (Pattern p : patterns) {
					lost.addAll(keys(p.name()));
				}
				lost.addAll(keys(full.name()));
				failures.add(failure("ingest", lost, e));
				return queryResult(null);
			}

			// A store that lives only in memory has no artifact to open: opening it is re-parsing the source,
			// which is what Build already reports, so it has no Open cell and no cold rows.
			if (cold) {
				try {
					m.time("open::" + slug, null, k.queryIters(), k.queryWarmup(), null, s -> a.open(src, location),
							(s, h) -> a.dispose((H) h), rows);
				} catch (Exception | OutOfMemoryError e) {
					failures.add(failure("open", List.of("open::" + slug), e));
				}
			} else {
				rows.add(Measure.unsupported("open::" + slug, OPEN_UNSUPPORTED));
			}
			if (!a.supportsQuads()) {
				for (Pattern p : probes) {
					if (p.kind() != Pattern.Kind.QUADS) {
						continue;
					}
					for (String regime : cold ? List.of("warm", "cold") : List.of("warm")) {
						for (String id : List.of(slug + "::" + p.name(), slug + "::" + p.name() + "::count")) {
							Map<String, Object> row = Measure.unsupported(id, NO_GRAPHS);
							row.put("regime", regime);
							rows.add(row);
						}
					}
				}
			}

			// Term parsing happens once per pattern, outside every timed region.
			Map<String, Q> prepared = new LinkedHashMap<>();
			Set<String> failed = new HashSet<>();
			// The warm handle lives its whole life on a thread of its own, so what its library keeps per thread ends
			// with it: Jena's in-memory dataset leaves its data reachable from the thread that queried it, through a
			// thread-local it never clears.
			Footprint held = onItsOwnThread(() -> warm(patterns, full, prepared, failed));
			if (held == null) {
				return queryResult(artifact());
			}
			// The store is unreachable now, so what it held is what this sample no longer finds.
			retained = held.minus(Footprint.sample());
			// A cold reading opens a store of its own, so it starts only once the warm handle is gone: a store that
			// locks its files refuses a second handle, and one that hands every open the same connection would shut
			// the warm handle down under it.
			if (cold) {
				measureCold(patterns, full, prepared, failed);
			}
			return queryResult(artifact());
		}

		/** The role's result, with what the open store retained when the role got that far. */
		private Map<String, Object> queryResult(String artifact) {
			Map<String, Object> out = result(a, "query", rows, counts, failures, false, k, artifact);
			out.put("retained", retained == null ? null : retained.toJson());
			return out;
		}

		/**
		 * Open the warm handle, take every pattern's counts and warm readings on it, sample what the process holds
		 * with it open, then dispose it. Null when the store does not open.
		 */
		private Footprint warm(List<Pattern> patterns, Pattern full, Map<String, Q> prepared, Set<String> failed) {
			H handle;
			try {
				handle = a.open(src, location);
			} catch (Exception | OutOfMemoryError e) {
				List<String> lost = new ArrayList<>();
				for (Pattern p : patterns) {
					lost.addAll(keys(p.name()));
				}
				lost.addAll(keys(full.name()));
				failures.add(failure("open", lost, e));
				return null;
			}
			try {
				measureWarm(handle, patterns, full, prepared, failed);
				return Footprint.sample();
			} finally {
				try {
					a.dispose(handle);
				} catch (Exception | OutOfMemoryError e) {
					failures.add(failure("dispose", List.of(), e));
				}
			}
		}

		/** The store this role built, when the adapter keeps one. */
		private String artifact() {
			return cold ? location.toString() : null;
		}

		/** The result of a role that {@code t} ended: what it measured, and the error. */
		Map<String, Object> ended(Throwable t) {
			failures.add(failure("worker", List.of(), t));
			return queryResult(artifact());
		}

		/** Every pattern and the full scan on the open handle: their counts, then their warm readings. */
		private void measureWarm(H handle, List<Pattern> patterns, Pattern full, Map<String, Q> prepared,
				Set<String> failed) {
			for (Pattern p : Stream.concat(patterns.stream(), Stream.of(full)).toList()) {
				try {
					prepared.put(p.name(), a.prepare(p));
				} catch (Exception | OutOfMemoryError e) {
					failures.add(failure(p.name(), keys(p.name()), e));
					failed.add(p.name());
				}
			}
			// The JIT compiles each library's query path before anything is timed, so no library is measured in
			// the interpreter. Each pattern gets its own share of the budget, so a pattern that answers in
			// microseconds is compiled as surely as one that takes milliseconds. A pattern that throws here throws
			// again below, where it is recorded.
			long share = k.jitWarmupMs() <= 0 ? 0 : Math.max(1, k.jitWarmupMs() / Math.max(1, patterns.size()));
			for (Pattern p : patterns) {
				Q q = prepared.get(p.name());
				if (q == null) {
					continue;
				}
				try {
					m.warmJit(share, () -> {
						a.count(handle, q);
						a.countOnly(handle, q);
					});
				} catch (Exception | OutOfMemoryError e) {
					// recorded when the pattern is measured
				}
			}

			for (Pattern p : Stream.concat(patterns.stream(), Stream.of(full)).toList()) {
				if (failed.contains(p.name())) {
					continue;
				}
				boolean scan = p == full;
				try {
					Q q = prepared.get(p.name());
					counts.put(p.name(), counted(handle, p.name(), q));
					String id = slug + "::" + p.name();
					int iters = scan ? k.fullScanIters() : k.queryIters(), warmup = scan ? 0 : k.queryWarmup();
					m.time(id, "warm", iters, warmup, null, s -> a.count(handle, q), null, rows);
					m.time(id + "::count", "warm", iters, warmup, null, s -> a.countOnly(handle, q), null, rows);
				} catch (Exception | OutOfMemoryError e) {
					// A pattern that fails warm fails cold too: its cold cells go with it.
					failures.add(failure(p.name(), keys(p.name()), e));
					failed.add(p.name());
				}
			}
		}

		/** Every pattern and the full scan again, each reading on a store opened in its setup and disposed after. */
		private void measureCold(List<Pattern> patterns, Pattern full, Map<String, Q> prepared, Set<String> failed) {
			for (Pattern p : Stream.concat(patterns.stream(), Stream.of(full)).toList()) {
				if (failed.contains(p.name())) {
					continue;
				}
				boolean scan = p == full;
				Q q = prepared.get(p.name());
				String id = slug + "::" + p.name();
				int iters = scan ? k.fullScanIters() : k.queryIters(), warmup = scan ? 0 : k.queryWarmup();
				try {
					m.<H>time(id, "cold", iters, warmup, () -> a.open(src, location), h -> a.count(h, q),
							(h, out) -> a.dispose(h), rows);
					m.<H>time(id + "::count", "cold", iters, warmup, () -> a.open(src, location),
							h -> a.countOnly(h, q), (h, out) -> a.dispose(h), rows);
				} catch (Exception | OutOfMemoryError e) {
					failures.add(failure(p.name(), coldKeys(p.name()), e));
				}
			}
		}

		/** {@code count}'s answer, checked against {@code countOnly}'s: a count path that disagrees fails here. */
		private long counted(H handle, String name, Q q) throws Exception {
			long count = a.count(handle, q), only = a.countOnly(handle, q);
			if (only != count) {
				throw new IllegalStateException("countOnly disagrees for " + name + ": " + only + " != " + count);
			}
			return count;
		}

		/** A pattern's cells as the dashboard keys them: warm, then cold when the store has a cold regime. */
		private List<String> keys(String name) {
			List<String> out = new ArrayList<>(List.of(slug + "::" + name, slug + "::" + name + "::count"));
			if (cold) {
				out.addAll(coldKeys(name));
			}
			return out;
		}

		private List<String> coldKeys(String name) {
			return List.of(slug + "::" + name + "#cold", slug + "::" + name + "::count#cold");
		}
	}

	private static <H, Q> Map<String, Object> runMutate(Adapter<H, Q> a, WorkerFiles f, Path workdir, Knobs k,
			List<Map<String, Object>> rows, List<Map<String, Object>> failures) {
		String slug = a.slug();
		String reason = a.mutationUnsupported();
		if (reason != null) {
			rows.add(Measure.unsupported("add::" + slug, reason));
			rows.add(Measure.unsupported("delete::" + slug, reason));
			return result(a, "mutate", rows, Map.of(), failures, true, k, null);
		}
		Path src = a.supportsQuads() ? f.quads() : f.triples();
		Path location = workdir.resolve("mutate");
		Measure m = new Measure(k);
		try {
			List<Quad> fresh = Inputs.rows(f.fresh());
			// Add into an empty store, so the measurement is the insert path and not a lookup against however many
			// rows the dataset holds.
			m.<H>time("add::" + slug, null, k.heavyIters(), 0, () -> {
				clear(location);
				return a.build(f.empty(), location);
			}, h -> {
				a.add(h, fresh);
				return null;
			}, (h, out) -> a.dispose(h), rows);
		} catch (Exception | OutOfMemoryError e) {
			failures.add(failure("add", List.of("add::" + slug), e));
		}
		try {
			List<Quad> prefix = Inputs.rows(f.prefix());
			// Delete from a store that holds the batch.
			m.<H>time("delete::" + slug, null, k.heavyIters(), 0, () -> {
				clear(location);
				return a.build(src, location);
			}, h -> {
				a.delete(h, prefix);
				return null;
			}, (h, out) -> a.dispose(h), rows);
		} catch (Exception | OutOfMemoryError e) {
			failures.add(failure("delete", List.of("delete::" + slug), e));
		}
		return result(a, "mutate", rows, Map.of(), failures, false, k, null);
	}

	/** The worker's result; {@code artifact} is the store a query role built, for the driver to size, or null. */
	private static Map<String, Object> result(Adapter<?, ?> a, String role, List<Map<String, Object>> rows,
			Map<String, Object> counts, List<Map<String, Object>> failures, boolean skippedRole, Knobs k,
			String artifact) {
		Map<String, Object> out = new LinkedHashMap<>();
		out.put("slug", a.slug());
		out.put("label", a.label());
		out.put("role", role);
		out.put("rows", rows);
		out.put("counts", counts);
		if (role.equals("query")) {
			// Reported here because they are env-tunable: the figure that is certainly right is the one the
			// process that did the timing used.
			Map<String, Object> iters = new LinkedHashMap<>();
			iters.put("query", k.queryIters());
			iters.put("queryWarmup", k.queryWarmup());
			iters.put("heavy", k.heavyIters());
			iters.put("fullScan", k.fullScanIters());
			out.put("iters", iters);
		}
		out.put("artifact", artifact);
		out.put("failures", failures);
		out.put("skippedRole", skippedRole);
		out.put("supportsQuads", a.supportsQuads());
		out.put("jdk", jdk());
		return out;
	}

	/** {@code body}'s result, computed on a thread that has ended by the time this returns; what it throws, thrown. */
	static <T> T onItsOwnThread(Supplier<T> body) {
		List<T> result = new ArrayList<>(1);
		Throwable[] thrown = new Throwable[1];
		Thread thread = Thread.ofPlatform().name("warm-handle").start(() -> {
			try {
				result.add(body.get());
			} catch (Throwable t) {
				thrown[0] = t;
			}
		});
		try {
			thread.join();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("interrupted waiting for the warm handle's thread", e);
		}
		if (thrown[0] instanceof Error e) {
			throw e;
		}
		if (thrown[0] != null) {
			throw thrown[0] instanceof RuntimeException e ? e : new IllegalStateException(thrown[0]);
		}
		return result.get(0);
	}

	private static Map<String, Object> failure(String phase, List<String> keys, Throwable e) {
		Map<String, Object> out = new LinkedHashMap<>();
		out.put("phase", phase);
		out.put("keys", keys);
		out.put("error", e.toString());
		return out;
	}

	/** Empty {@code dir}, creating it when absent. */
	static void clear(Path dir) throws IOException {
		if (Files.exists(dir)) {
			try (Stream<Path> walk = Files.walk(dir)) {
				for (Path p : walk.sorted(Comparator.reverseOrder()).toList()) {
					Files.delete(p);
				}
			}
		}
		Files.createDirectories(dir);
	}

	static Adapter<?, ?> find(String slug) {
		for (AdapterFactory factory : ServiceLoader.load(AdapterFactory.class)) {
			if (factory.slugs().contains(slug)) {
				return factory.create(slug);
			}
		}
		throw new IllegalArgumentException("no adapter on this classpath serves " + slug);
	}

	static String jdk() {
		return System.getProperty("java.version") + " (" + System.getProperty("java.vendor") + ")";
	}

	private static String required(Map<String, String> opt, String name) {
		String v = opt.get(name);
		if (v == null) {
			throw new IllegalArgumentException("missing --" + name);
		}
		return v;
	}

	private static Path path(Map<String, String> opt, String name) {
		return Path.of(required(opt, name));
	}
}
