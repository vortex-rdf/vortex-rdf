package org.vortexrdf.bench;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WorkerTest {
	private static final Knobs QUICK = new Knobs(2, 1, 1, 1, 0, 10);
	private static final List<String> NAMES = List.of("S", "P", "O", "SP", "PO", "SPO", "G", "SPOG", "full");

	@TempDir
	Path tmp;
	private WorkerFiles files;

	@BeforeEach
	void extract() throws Exception {
		files = Fixture.extract(tmp.resolve("fixture"));
	}

	@SuppressWarnings("unchecked")
	private static List<Map<String, Object>> rows(Map<String, Object> result) {
		return (List<Map<String, Object>>) result.get("rows");
	}

	/** A row's key as the dashboard's template forms it: its id, with `#cold` for a cold reading. */
	private static String key(Map<String, Object> row) {
		return "cold".equals(row.get("regime")) ? row.get("id") + "#cold" : (String) row.get("id");
	}

	private static Set<String> keys(Map<String, Object> result) {
		return rows(result).stream().map(WorkerTest::key).collect(Collectors.toSet());
	}

	@Test
	void queryRowsMatchPythonIds() {
		Map<String, Object> result = Worker.query(ListAdapter.persistent("list"), files, tmp.resolve("work"), QUICK);

		Set<String> want = new HashSet<>(List.of("ingest::list", "open::list"));
		for (String name : NAMES) {
			for (String suffix : List.of("", "::count")) {
				want.add("list::" + name + suffix);
				want.add("list::" + name + suffix + "#cold");
			}
		}
		assertEquals(want, keys(result));
		assertEquals(List.of(), result.get("failures"));
		assertEquals(tmp.resolve("work").resolve("store").toString(), result.get("artifact"));
		assertEquals(false, result.get("skippedRole"));
		assertEquals(true, result.get("supportsQuads"));
		assertNotNull(result.get("jdk"));
		assertFalse(result.containsKey("retained"));
	}

	@Test
	void triplesOnlyAdaptersReportGraphPatternsUnsupported() {
		Map<String, Object> result = Worker.query(ListAdapter.triplesOnly("tri"), files, tmp.resolve("work"), QUICK);

		List<Map<String, Object>> graphRows = rows(result).stream()
				.filter(r -> Set.of("G", "SPOG").contains(((String) r.get("variant")).split("::")[0])).toList();
		assertEquals(Set.of("tri::G", "tri::G::count", "tri::SPOG", "tri::SPOG::count", "tri::G#cold",
				"tri::G::count#cold", "tri::SPOG#cold", "tri::SPOG::count#cold"),
				graphRows.stream().map(WorkerTest::key).collect(Collectors.toSet()));
		for (Map<String, Object> r : graphRows) {
			assertEquals(true, r.get("unsupported"));
			assertEquals("this library's model has no graphs -- it reads the triples projection", r.get("reason"));
		}
		assertEquals(false, result.get("supportsQuads"));
	}

	@Test
	void inMemoryAdaptersHaveNoOpenOrColdRows() {
		Map<String, Object> result = Worker.query(ListAdapter.inMemory("mem"), files, tmp.resolve("work"), QUICK);

		Map<String, Object> open = rows(result).stream().filter(r -> "open::mem".equals(r.get("id"))).findFirst()
				.orElseThrow();
		assertEquals(true, open.get("unsupported"));
		assertEquals("no artifact to reopen: a fresh process re-parses the source, which is the Build column",
				open.get("reason"));
		assertTrue(rows(result).stream().noneMatch(r -> "cold".equals(r.get("regime"))));
		assertNull(result.get("artifact"));
	}

	@Test
	void countsMatchABruteForceScan() {
		Map<String, Object> result = Worker.query(ListAdapter.persistent("list"), files, tmp.resolve("work"), QUICK);

		assertEquals(Fixture.MATCHES, result.get("counts"));
	}

	@Test
	@SuppressWarnings("unchecked")
	void aFailingPatternMarksOnlyItsCells() {
		Map<String, Object> result = Worker.query(ListAdapter.failingOn("list", "O"), files, tmp.resolve("work"),
				QUICK);

		List<Map<String, Object>> failures = (List<Map<String, Object>>) result.get("failures");
		assertEquals(1, failures.size());
		assertEquals("O", failures.get(0).get("phase"));
		assertEquals(List.of("list::O", "list::O::count", "list::O#cold", "list::O::count#cold"),
				failures.get(0).get("keys"));
		assertTrue(((String) failures.get(0).get("error")).contains("failing on purpose at O"));
		assertTrue(keys(result).containsAll(Set.of("list::S", "list::S::count", "list::S#cold", "list::S::count#cold")));
		assertFalse(keys(result).contains("list::O"));
	}

	/** The worker's arguments for {@code role} on the test classpath's {@code list_stdout}, in {@code tmp/work}. */
	private String[] cli(String role, Path out) {
		return new String[] { "--slug", "list_stdout", "--role", role, "--quads", files.quads().toString(),
				"--triples", files.triples().toString(), "--probes", files.probes().toString(), "--fresh",
				files.fresh().toString(), "--prefix", files.prefix().toString(), "--empty", files.empty().toString(),
				"--workdir", tmp.resolve("work").toString(), "--out", out.toString() };
	}

	@Test
	void stdoutFromAnAdapterLeavesTheResultIntact() throws Exception {
		Path out = tmp.resolve("out.json");

		Worker.main(cli("query", out));

		String json = Files.readString(out);
		assertTrue(json.startsWith("{\"slug\":\"list_stdout\",\"label\":\"List list_stdout\",\"role\":\"query\""),
				json.substring(0, Math.min(120, json.length())));
		assertTrue(json.contains("\"rows\":[{\"group\":\"ingest\""));
		assertFalse(json.contains("noise from the adapter"));
		assertTrue(json.endsWith("}"));
	}

	/**
	 * A store that locks its files, or one whose reopen hands back a shared connection, cannot have a cold reading
	 * open beside the warm handle: every cold cell would fail, or shut the warm handle down under it.
	 */
	@Test
	void aStoreThatLocksItsFilesIsOpenedOneHandleAtATime() {
		Map<String, Object> result = Worker.query(ListAdapter.exclusive("lock"), files, tmp.resolve("work"), QUICK);

		assertEquals(List.of(), result.get("failures"));
		assertTrue(keys(result).containsAll(Set.of("lock::S#cold", "lock::S::count#cold", "lock::full",
				"lock::full#cold", "lock::SPOG#cold")));
	}

	@SuppressWarnings("unchecked")
	private static List<Map<String, Object>> failures(Map<String, Object> result) {
		return (List<Map<String, Object>>) result.get("failures");
	}

	/** A heap exhausted by one pattern, during the JIT warm-up or after it, fails that pattern and nothing else. */
	@Test
	void anOutOfMemoryErrorInOnePatternFailsOnlyThatPattern() {
		ListAdapter a = ListAdapter.persistent("oom").throwing("O", () -> new OutOfMemoryError("test heap"));

		Map<String, Object> result = Worker.query(a, files, tmp.resolve("work"), QUICK);

		assertEquals(List.of("O"), failures(result).stream().map(f -> f.get("phase")).toList());
		assertTrue(keys(result).containsAll(Set.of("ingest::oom", "open::oom", "oom::S", "oom::S#cold", "oom::full#cold")));
	}

	/** An error no phase expects ends the role, but what the role measured before it stays, with the error. */
	@Test
	void anUnexpectedErrorKeepsTheRowsAlreadyMeasured() {
		ListAdapter a = ListAdapter.persistent("err").throwing("O", () -> new AssertionError("unexpected"));

		Map<String, Object> result = Worker.query(a, files, tmp.resolve("work"), QUICK);

		assertTrue(keys(result).containsAll(Set.of("ingest::err", "open::err")));
		Map<String, Object> last = failures(result).get(failures(result).size() - 1);
		assertEquals("worker", last.get("phase"));
		assertEquals(List.of(), last.get("keys"));
		assertTrue(((String) last.get("error")).contains("unexpected"));
	}

	@Test
	void anUnexpectedErrorInAMutationKeepsTheAddRow() {
		ListAdapter a = ListAdapter.persistent("mut").throwing("delete", () -> new AssertionError("unexpected"));

		Map<String, Object> result = Worker.mutate(a, files, tmp.resolve("work"), QUICK);

		assertTrue(keys(result).contains("add::mut"));
		assertEquals("worker", failures(result).get(failures(result).size() - 1).get("phase"));
	}

	/**
	 * The warm-up budget is split between the patterns, so a pattern that answers in microseconds is compiled by
	 * the JIT as surely as one that takes milliseconds. A round over every pattern would give S one call for each
	 * of P's slow ones.
	 */
	@Test
	void everyPatternGetsItsShareOfTheWarmUp() {
		ListAdapter a = ListAdapter.persistent("warm").slowOn("P", 20);

		Worker.query(a, files, tmp.resolve("work"), new Knobs(2, 1, 1, 1, 0, 400));

		assertTrue(a.calls("S") >= 100, "S was materialized " + a.calls("S") + " times");
	}

	/** A memory run of {@code a} on the store its build left in {@code tmp/work/store}. */
	private Map<String, Object> memoryRun(ListAdapter a) throws Exception {
		Path work = tmp.resolve("work");
		a.dispose(a.build(files.quads(), Files.createDirectories(work.resolve("store"))));
		return Worker.memory(a, files, work);
	}

	private static long figure(Map<String, Object> r, String key) {
		return ((Number) r.get(key)).longValue();
	}

	@Test
	void aMemoryRunReadsEveryProbeOnce() throws Exception {
		ListAdapter a = ListAdapter.persistent("list");

		Map<String, Object> r = memoryRun(a);

		assertEquals(List.of("slug", "label", "role", "obtained", "counts", "storeBytes", "peakAnonBytes",
				"peakFileBytes", "jdk"), List.copyOf(r.keySet()));
		assertEquals(List.of("memory", "loaded"), List.of(r.get("role"), r.get("obtained")));
		assertEquals(Fixture.MATCHES, r.get("counts"));
		for (String name : NAMES) {
			assertEquals(1, a.calls(name), name);
		}
	}

	/**
	 * What a store holds outside the heap is the native memory that disposing it releases, to within the JVM's own
	 * bookkeeping: the per-allocation overhead its tracking does not count moves by about a megabyte.
	 */
	@Test
	void theStoreFigureIsTheNativeMemoryDisposingReleases() throws Exception {
		Map<String, Object> r = memoryRun(ListAdapter.inMemory("native").holding(0, 32L << 20));

		assertTrue(Math.abs(figure(r, "storeBytes") - (32L << 20)) < 2L << 20, r.toString());
		assertTrue(figure(r, "peakAnonBytes") >= 30L << 20, r.toString()); // zeroed at the open, so resident
	}

	/**
	 * Store is read on a store no read has touched: what the reads cache in the store, and what the adapter keeps
	 * from its first open and from every read after the dispose, are not in it.
	 */
	@Test
	void theStoreFigureLeavesOutWhatTheReadsAndTheFirstOpenLeaveBehind() throws Exception {
		Map<String, Object> r = memoryRun(
				ListAdapter.inMemory("reads").holding(32 << 20, 0).cachingOnRead(2 << 20).leaking(8 << 20, 1 << 20));

		assertTrue(Math.abs(figure(r, "storeBytes") - (32L << 20)) < 2L << 20, r.toString());
	}

	/** A store that locks its files opens a second time only after the first is disposed. */
	@Test
	void aMemoryRunOfAStoreThatLocksItsFilesSucceeds() throws Exception {
		Map<String, Object> r = memoryRun(ListAdapter.exclusive("locked"));

		assertEquals(Fixture.MATCHES, r.get("counts"));
	}

	/**
	 * A store kept in the heap holds what it keeps there, so its figure does not follow the heap limit as a peak
	 * does. That includes data its library leaves reachable from the thread that opened it after the store is gone,
	 * as Jena's in-memory dataset does through a thread-local it never clears.
	 */
	@Test
	void theStoreFigureIncludesHeapDataPinnedToTheThreadThatOpenedIt() throws Exception {
		long store = figure(memoryRun(ListAdapter.inMemory("pinned").holding(64 << 20, 0).pinnedToItsThreads()),
				"storeBytes");

		assertTrue(store >= 64L << 20 && store < 80L << 20, "store " + store);
	}

	@Test
	void aCounterThatMissesTheBlockFailsTheSelfCheck() {
		// Thrown away, as the memory role does: a JVM's first sample is off by a few hundred KB.
		Footprint.sample();
		assertDoesNotThrow(() -> Worker.selfCheck(Footprint::sample));
		assertTrue(assertThrows(IllegalStateException.class, () -> Worker.selfCheck(() -> new Footprint(0, 0)))
				.getMessage().contains("heap"));
		Iterator<Footprint> seen = List.of(new Footprint(0, 0), new Footprint(16L << 20, 0)).iterator();
		assertTrue(assertThrows(IllegalStateException.class, () -> Worker.selfCheck(seen::next)).getMessage()
				.contains("mallinfo2"));
	}

	/** The self-check on two samples: one before its block is held, and one while it is. */
	private static void selfCheckSeeing(Footprint before, Footprint held) {
		Iterator<Footprint> samples = List.of(before, held).iterator();
		Worker.selfCheck(samples::next);
	}

	/**
	 * The heap counter must see the whole 8 MiB array, and the native one 7 MiB of the 8 MiB block, since the JVM's own
	 * threads allocate natively while the check runs.
	 */
	@Test
	void theSelfCheckWantsTheWholeHeapBlockAndSevenMiBOfTheNativeOne() {
		Footprint none = new Footprint(0, 0);

		assertDoesNotThrow(() -> selfCheckSeeing(none, new Footprint(8L << 20, 7L << 20)));
		String nativeShort = assertThrows(IllegalStateException.class,
				() -> selfCheckSeeing(none, new Footprint(8L << 20, (7L << 20) - 1))).getMessage();
		assertTrue(nativeShort.contains("mallinfo2") && nativeShort.contains("want at least 7 MiB"), nativeShort);
		assertTrue(assertThrows(IllegalStateException.class,
				() -> selfCheckSeeing(none, new Footprint((8L << 20) - 1, 7L << 20))).getMessage().contains("heap"));
	}

	@Test
	void theWorkerRunsTheMemoryRole() throws Exception {
		ListAdapter.persistent("x").build(files.quads(), Files.createDirectories(tmp.resolve("work/store")));
		Path out = tmp.resolve("memory.json");

		Worker.main(cli("memory", out));

		assertTrue(Files.readString(out).startsWith(
				"{\"slug\":\"list_stdout\",\"label\":\"List list_stdout\",\"role\":\"memory\",\"obtained\":\"loaded\""));
	}

	@Test
	void unsupportedMutationSkipsTheRole() {
		Map<String, Object> result = Worker.mutate(ListAdapter.immutable("ro"), files, tmp.resolve("work"), QUICK);

		assertEquals(true, result.get("skippedRole"));
		assertEquals(Set.of("add::ro", "delete::ro"), keys(result));
		assertTrue(rows(result).stream().allMatch(r -> Boolean.TRUE.equals(r.get("unsupported"))));
		assertEquals("a test store that cannot change", rows(result).get(0).get("reason"));
	}

	@Test
	void mutationRowsAreMeasured() {
		Map<String, Object> result = Worker.mutate(ListAdapter.persistent("rw"), files, tmp.resolve("work"), QUICK);

		assertEquals(false, result.get("skippedRole"));
		assertEquals(Set.of("add::rw", "delete::rw"), keys(result));
		assertTrue(rows(result).stream().allMatch(r -> r.get("median_ns") != null));
		assertEquals(List.of(), result.get("failures"));
	}

	@Test
	void theBaselineReportsItsJdk() {
		Map<String, Object> result = Worker.baseline();

		assertEquals("baseline", result.get("role"));
		assertTrue(((String) result.get("jdk")).contains(System.getProperty("java.version")));
	}
}
