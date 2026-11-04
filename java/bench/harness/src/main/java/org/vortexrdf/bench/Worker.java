package org.vortexrdf.bench;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.foreign.Arena;
import java.lang.ref.Reference;
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
 * logging may write to. In the query and mutate roles, a phase that throws becomes a failure naming the cells it
 * leaves empty, and the worker carries on with the next phase. The memory role has no partial result: an error ends
 * the JVM, non-zero, with the error on stderr.
 */
public final class Worker {
	static final String OPEN_UNSUPPORTED = "no artifact to reopen: a fresh process re-parses the source, which is the Build column";
	static final String NO_GRAPHS = "this library's model has no graphs -- it reads the triples projection";
	/** What the self-check holds, in the heap and outside it, and the least the heap counter must rise by. */
	private static final int SELF_CHECK_BYTES = 8 << 20;
	/** The native block's parts, each under glibc's 128 KiB mmap threshold. */
	private static final int SELF_CHECK_PART = 64 << 10;
	/**
	 * The least the native counter must rise by while the self-check holds its 8 MiB block. The JVM's own threads, the
	 * JIT's and the collector's, allocate natively during the check: measured, that moved the rise by up to 446 KB in a
	 * fresh JVM and by about 5 KB in a warm one, while a counter that misses the block misses it by about 8 MiB.
	 */
	private static final int SELF_CHECK_NATIVE_BAR = 7 << 20;

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

	/**
	 * The memory role: what one store holds, measured in a JVM of its own. It opens the store the query role built and
	 * disposes it before any read, then opens it again, reads every pattern once and disposes it, and reports the
	 * counts it read and three figures in bytes: {@code storeBytes}, the live bytes right after the store is obtained
	 * minus those after its dispose ({@link Footprint}), on a store no read has touched, so it leaves out the caches
	 * reads fill; and {@code peakAnonBytes} and {@code peakFileBytes}, the highest {@link Rss} sample during the
	 * reads, above a baseline read before the first open. It first checks that both counters
	 * see a block held ({@link #selfCheck}). Any error is thrown, so the JVM exits non-zero.
	 */
	public static Map<String, Object> memory(Adapter<?, ?> a, WorkerFiles f, Path workdir) throws Exception {
		return new Memory<>(a, f, workdir).run();
	}

	/** What an empty worker JVM reports: which JDK it ran on, with no library loaded. */
	public static Map<String, Object> baseline() {
		Map<String, Object> out = new LinkedHashMap<>();
		out.put("role", "baseline");
		out.put("jdk", jdk());
		return out;
	}

	/**
	 * {@code --slug S --role query|mutate|memory|baseline --quads --triples --probes --fresh --prefix --empty
	 * --workdir --out}.
	 */
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
			case "memory" -> memory(adapter, files, workdir);
			default -> throw new IllegalArgumentException("unknown role " + role);
			};
		}
		Files.writeString(path(opt, "out"), Json.write(result), StandardCharsets.UTF_8);
	}

	/** The query role: build, open, then every pattern warm, cold, and the full scan. */
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
			List<Pattern> patterns = patterns(a, probes);
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
			// The warm handle lives its whole life on a thread of its own, so what its library keeps per thread is gone
			// with it before the cold readings: a library may leave a store's data reachable from the thread that
			// queried it, as Jena's in-memory dataset does through a thread-local it never clears.
			if (!onItsOwnThread(() -> warm(patterns, full, prepared, failed))) {
				return queryResult(artifact());
			}
			// A cold reading opens a store of its own, so it starts only once the warm handle is gone: a store that
			// locks its files refuses a second handle, and one that hands every open the same connection would shut
			// the warm handle down under it.
			if (cold) {
				measureCold(patterns, full, prepared, failed);
			}
			return queryResult(artifact());
		}

		/** The role's result; {@code artifact} is the store it built, or null. */
		private Map<String, Object> queryResult(String artifact) {
			return result(a, "query", rows, counts, failures, false, k, artifact);
		}

		/**
		 * Open the warm handle, take every pattern's counts and warm readings on it, then dispose it. False when the
		 * store does not open.
		 */
		private boolean warm(List<Pattern> patterns, Pattern full, Map<String, Q> prepared, Set<String> failed) {
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
				return false;
			}
			try {
				measureWarm(handle, patterns, full, prepared, failed);
				return true;
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
					m.timeQuery(id, "warm", iters, warmup, null, s -> a.count(handle, q), null, rows);
					m.timeQuery(id + "::count", "warm", iters, warmup, null, s -> a.countOnly(handle, q), null, rows);
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
					m.<H>timeQuery(id, "cold", iters, warmup, () -> a.open(src, location), h -> a.count(h, q),
							(h, out) -> a.dispose(h), rows);
					m.<H>timeQuery(id + "::count", "cold", iters, warmup, () -> a.open(src, location),
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

	/**
	 * The memory role ({@link #memory}). Each of the two stores lives its whole life on a thread of its own, as the query
	 * role's warm handle does, so a thread-local its library leaves holding the store's data dies with the thread, and what
	 * it held counts as the store's.
	 */
	private static final class Memory<H, Q> {
		private final Adapter<H, Q> a;
		private final WorkerFiles f;
		private final Path src;
		private final Path location;
		private final Map<String, Object> counts = new LinkedHashMap<>();
		private Rss peak;

		Memory(Adapter<H, Q> a, WorkerFiles f, Path workdir) {
			this.a = a;
			this.f = f;
			this.src = a.supportsQuads() ? f.quads() : f.triples();
			this.location = workdir.resolve("store");
		}

		Map<String, Object> run() throws Exception {
			List<Pattern> probes = Inputs.probes(f.probes());
			Pattern full = probes.stream().filter(p -> p.kind() == Pattern.Kind.FULL).findFirst().orElseThrow();
			Map<String, Q> prepared = new LinkedHashMap<>();
			for (Pattern p : Stream.concat(patterns(a, probes).stream(), Stream.of(full)).toList()) {
				prepared.put(p.name(), a.prepare(p));
			}
			// Thrown away: a JVM's first sample resolves the mallinfo2 downcall between reading the tracking and
			// reading mallinfo2, which throws that sample off by a few hundred KB.
			Footprint.sample();
			selfCheck(Footprint::sample);
			// Sampled for its collection: the baseline holds no garbage.
			Footprint.sample();
			Rss base = Rss.read();
			// The first store is opened and disposed before any read: it holds nothing a read left behind. Its thread is
			// gone: what it held is what this sample no longer finds.
			Footprint opened = onItsOwnThread(this::opened);
			Footprint store = opened.minus(Footprint.sample());
			onItsOwnThread(() -> {
				read(prepared);
				return null;
			});

			Map<String, Object> out = new LinkedHashMap<>();
			out.put("slug", a.slug());
			out.put("label", a.label());
			out.put("role", "memory");
			out.put("obtained", a.obtained().json());
			out.put("counts", counts);
			out.put("storeBytes", store.heapBytes() + store.nativeBytes());
			out.put("peakAnonBytes", peak.anonBytes() - base.anonBytes());
			out.put("peakFileBytes", peak.fileBytes() - base.fileBytes());
			out.put("jdk", jdk());
			return out;
		}

		/**
		 * Open the store, sample what the process holds with it open, and dispose it, before any read. The sample's
		 * collection drops the build's garbage, so its reading is the store as opened.
		 */
		private Footprint opened() throws Exception {
			H handle = a.open(src, location);
			Footprint opened;
			try {
				opened = Footprint.sample();
			} catch (Throwable t) {
				// The sample's error is the run's: one the dispose adds after it rides along, rather than hiding it.
				try {
					a.dispose(handle);
				} catch (Throwable suppressed) {
					t.addSuppressed(suppressed);
				}
				throw t;
			}
			a.dispose(handle);
			return opened;
		}

		/** Open the store again, read every pattern once under a {@link Rss.Sampler}, and dispose it. */
		private void read(Map<String, Q> prepared) throws Exception {
			H handle = a.open(src, location);
			try {
				// Sampled for its collection: an open that builds its store leaves the build's garbage behind.
				Footprint.sample();
				Rss.Sampler sampler = Rss.Sampler.start();
				try (sampler) {
					for (Map.Entry<String, Q> p : prepared.entrySet()) {
						counts.put(p.getKey(), a.count(handle, p.getValue()));
					}
				}
				peak = sampler.peak();
			} catch (Throwable t) {
				// The read's error is the run's: one the dispose adds after it rides along, rather than hiding it.
				try {
					a.dispose(handle);
				} catch (Throwable suppressed) {
					t.addSuppressed(suppressed);
				}
				throw t;
			}
			a.dispose(handle);
		}
	}

	/**
	 * Throw, naming the counter, unless both of {@code sample}'s counters see the 8 MiB the check holds: an array in
	 * the heap, whose counter must rise by all of it, and 128 blocks of 64 KiB from one arena outside it, whose counter
	 * must rise by at least {@link #SELF_CHECK_NATIVE_BAR}. No block is large enough for glibc to mmap, so freeing them
	 * leaves its dynamic mmap threshold, and with it how the store's allocations are served, as it was.
	 */
	static void selfCheck(Supplier<Footprint> sample) {
		Footprint before = sample.get();
		byte[] array = new byte[SELF_CHECK_BYTES];
		Footprint held;
		try (Arena arena = Arena.ofConfined()) {
			for (int i = 0; i < SELF_CHECK_BYTES / SELF_CHECK_PART; i++) {
				arena.allocate(SELF_CHECK_PART);
			}
			held = sample.get();
			// Nothing reads the array, so without this the sample's collection could free it.
			Reference.reachabilityFence(array);
		}
		long heapRise = held.heapBytes() - before.heapBytes(), nativeRise = held.nativeBytes() - before.nativeBytes();
		if (heapRise < SELF_CHECK_BYTES) {
			throw new IllegalStateException("self-check failed: the heap counter (used heap after a full GC) rose by "
					+ heapRise + " bytes while the worker held an 8 MiB array");
		}
		if (nativeRise < SELF_CHECK_NATIVE_BAR) {
			throw new IllegalStateException("self-check failed: the native counter (mallinfo2 less the JVM's own "
					+ "malloc) rose by " + nativeRise + " bytes while the worker held 8 MiB of native memory (want at "
					+ "least " + (SELF_CHECK_NATIVE_BAR >> 20) + " MiB)");
		}
	}

	/**
	 * The patterns the query and memory roles read: the triple patterns, plus the graph-bound ones when the library
	 * has graphs.
	 */
	static List<Pattern> patterns(Adapter<?, ?> a, List<Pattern> probes) {
		return probes.stream().filter(p -> p.kind() == Pattern.Kind.TRIPLES
				|| (p.kind() == Pattern.Kind.QUADS && a.supportsQuads())).toList();
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

	/**
	 * {@code body}'s result, computed on a thread that has ended by the time this returns; what it throws, thrown, a
	 * checked exception wrapped in an {@link IllegalStateException}.
	 */
	static <T> T onItsOwnThread(Measure.ThrowingSupplier<T> body) {
		List<T> result = new ArrayList<>(1);
		Throwable[] thrown = new Throwable[1];
		Thread thread = Thread.ofPlatform().name("store-handle").start(() -> {
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
			throw new IllegalStateException("interrupted waiting for the store handle's thread", e);
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
