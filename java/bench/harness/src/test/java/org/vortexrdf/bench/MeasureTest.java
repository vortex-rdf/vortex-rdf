package org.vortexrdf.bench;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class MeasureTest {
	private static Knobs timeoutNs(long ns) {
		return new Knobs(10, 5, 3, 3, ns, 0);
	}

	@Test
	void warmupRunsAreDiscarded() throws Exception {
		AtomicInteger bodies = new AtomicInteger(), teardowns = new AtomicInteger();
		List<Map<String, Object>> rows = new ArrayList<>();

		new Measure(timeoutNs(0)).time("list::S", "warm", 10, 5, null, s -> bodies.incrementAndGet(),
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
	void aFirstRunPastTheLimitTimesTheCellOut() throws Exception {
		AtomicInteger bodies = new AtomicInteger();
		List<Map<String, Object>> rows = new ArrayList<>();

		new Measure(timeoutNs(1)).time("ingest::list", null, 10, 5, null, s -> {
			bodies.incrementAndGet();
			Thread.sleep(2);
			return null;
		}, null, rows);

		assertEquals(1, bodies.get());
		Map<String, Object> row = rows.get(0);
		assertEquals(true, row.get("timeout"));
		assertEquals("timeout", row.get("median"));
		assertNull(row.get("median_ns"));
		assertEquals("0", row.get("samples"));
		assertTrue(((String) row.get("reason")).startsWith("not measured: one run took "), (String) row.get("reason"));
		assertTrue(!row.containsKey("regime"));
	}

	@Test
	void onlyTheFirstRunIsHeldToTheLimit() throws Exception {
		AtomicInteger bodies = new AtomicInteger();
		List<Map<String, Object>> rows = new ArrayList<>();

		new Measure(timeoutNs(1_000_000)).time("list::S", "warm", 3, 1, null, s -> {
			if (bodies.getAndIncrement() > 0) {
				Thread.sleep(2);
			}
			return null;
		}, null, rows);

		assertEquals(4, bodies.get());
		assertEquals("3", rows.get(0).get("samples"));
	}

	@Test
	void setupAndTeardownStayOutsideTheTiming() throws Exception {
		List<Map<String, Object>> rows = new ArrayList<>();

		new Measure(timeoutNs(0)).time("open::list", null, 3, 0, () -> {
			Thread.sleep(50);
			return null;
		}, s -> null, (s, out) -> Thread.sleep(50), rows);

		assertTrue((double) rows.get(0).get("median_ns") < 25_000_000, "median " + rows.get(0).get("median_ns"));
	}

	/** A cold reading's body that throws must still dispose the store its setup opened. */
	@Test
	void aThrowingBodyStillTearsItsStateDown() {
		Object state = new Object();
		List<Object> tornDown = new ArrayList<>();

		IllegalStateException thrown = assertThrows(IllegalStateException.class,
				() -> new Measure(timeoutNs(0)).time("list::S", "cold", 3, 0, () -> state, s -> {
					throw new IllegalStateException("boom");
				}, (s, out) -> tornDown.add(s), new ArrayList<>()));

		assertEquals("boom", thrown.getMessage());
		assertEquals(List.of(state), tornDown);
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
