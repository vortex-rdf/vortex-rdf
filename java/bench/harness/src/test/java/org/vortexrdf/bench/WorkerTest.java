package org.vortexrdf.bench;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
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

	@Test
	void stdoutFromAnAdapterLeavesTheResultIntact() throws Exception {
		Path out = tmp.resolve("out.json");

		Worker.main(new String[] { "--slug", "list_stdout", "--role", "query", "--quads", files.quads().toString(),
				"--triples", files.triples().toString(), "--probes", files.probes().toString(), "--fresh",
				files.fresh().toString(), "--prefix", files.prefix().toString(), "--empty", files.empty().toString(),
				"--workdir", tmp.resolve("work").toString(), "--out", out.toString() });

		String json = Files.readString(out);
		assertTrue(json.startsWith("{\"slug\":\"list_stdout\",\"label\":\"List list_stdout\",\"role\":\"query\""),
				json.substring(0, Math.min(120, json.length())));
		assertTrue(json.contains("\"rows\":[{\"group\":\"ingest\""));
		assertFalse(json.contains("noise from the adapter"));
		assertTrue(json.endsWith("}"));
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
