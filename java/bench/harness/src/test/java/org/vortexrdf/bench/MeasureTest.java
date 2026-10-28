package org.vortexrdf.bench;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class MeasureTest {
	private static Knobs slowPhaseNs(long ns) {
		return new Knobs(10, 5, 3, 3, ns, 0);
	}

	@Test
	void warmupRunsAreDiscarded() throws Exception {
		AtomicInteger bodies = new AtomicInteger(), teardowns = new AtomicInteger();
		List<Map<String, Object>> rows = new ArrayList<>();

		new Measure(slowPhaseNs(0)).time("list::S", "warm", 10, 5, null, s -> bodies.incrementAndGet(),
				(s, out) -> teardowns.incrementAndGet(), rows);

		assertEquals(15, bodies.get());
		assertEquals(15, teardowns.get());
		Map<String, Object> row = rows.get(0);
		assertEquals("10", row.get("samples"));
		assertEquals("warm", row.get("regime"));
		assertEquals("list", row.get("group"));
		assertEquals("S", row.get("variant"));
		assertEquals("list::S", row.get("id"));
	}

	@Test
	void aSlowPhaseKeepsOneSample() throws Exception {
		List<Map<String, Object>> rows = new ArrayList<>();

		new Measure(slowPhaseNs(1)).time("ingest::list", null, 10, 5, null, s -> {
			Thread.sleep(2);
			return null;
		}, null, rows);

		assertEquals("1", rows.get(0).get("samples"));
		assertTrue(!rows.get(0).containsKey("regime"));
	}

	@Test
	void setupAndTeardownStayOutsideTheTiming() throws Exception {
		List<Map<String, Object>> rows = new ArrayList<>();

		new Measure(slowPhaseNs(0)).time("open::list", null, 3, 0, () -> {
			Thread.sleep(50);
			return null;
		}, s -> null, (s, out) -> Thread.sleep(50), rows);

		assertTrue((double) rows.get(0).get("median_ns") < 25_000_000, "median " + rows.get(0).get("median_ns"));
	}

	@Test
	void anUnsupportedCellCarriesItsReasonAndNoNumbers() {
		Map<String, Object> row = Measure.unsupported("open::x", "r");

		assertEquals(true, row.get("unsupported"));
		assertEquals("r", row.get("reason"));
		assertNull(row.get("median_ns"));
		assertEquals("0", row.get("samples"));
		assertEquals("unsupported", row.get("median"));
		assertEquals("open", row.get("group"));
		assertEquals("x", row.get("variant"));
	}
}
