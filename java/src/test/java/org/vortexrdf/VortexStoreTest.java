package org.vortexrdf;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The Java API through the native library, against the quads the fixture stores were built from. The engine's own
 * tests (tests/engine.rs) cover every pattern shape, layout and window; these cover what crosses the JNI boundary:
 * each native method, how its arguments arrive (null, "", -1, offsets, limits, buffers), its errors, and a store's
 * lifetime and threads.
 */
class VortexStoreTest {
	private static Path fixtures;
	static List<String[]> quads;

	@BeforeAll
	static void loadOracle() throws IOException {
		fixtures = Paths.get(System.getProperty("vortexrdf.fixtures"));
		quads = new ArrayList<>();
		for (String line : Files.readAllLines(fixtures.resolve("oracle.tsv"), StandardCharsets.UTF_8)) {
			String[] f = line.split("\t", -1);
			assertEquals(4, f.length, line);
			quads.add(f);
		}
		assertTrue(quads.size() > 100);
	}

	static VortexStore open(String name, boolean inMemory) {
		return VortexStore.open(fixtures.resolve(name + ".vortex"), inMemory);
	}

	static String key(String[] quad) {
		return String.join("\t", quad);
	}

	private static Set<String> keys(List<String[]> rows) {
		return rows.stream().map(VortexStoreTest::key).collect(Collectors.toCollection(TreeSet::new));
	}

	/** The oracle's quads matching a pattern, by scan. */
	static Set<String> scan(String[] pattern) {
		Set<String> out = new TreeSet<>();
		for (String[] q : quads) {
			boolean all = true;
			for (int i = 0; i < 4; i++) {
				all &= pattern[i] == null || pattern[i].equals(q[i]);
			}
			if (all) {
				out.add(key(q));
			}
		}
		return out;
	}

	/** One pattern per way an argument arrives: all free, each position bound, all four, "", an absent term. */
	static List<String[]> patterns() {
		String[] q = quads.get(quads.size() / 2);
		return List.of(new String[] { null, null, null, null }, new String[] { q[0], null, null, null },
				new String[] { null, q[1], null, null }, new String[] { null, null, q[2], null },
				new String[] { null, null, null, q[3] }, q.clone(), new String[] { null, null, null, "" },
				new String[] { null, null, "<http://example.org/absent>", null });
	}

	@ParameterizedTest
	@ValueSource(strings = { "dictionary", "copy", "reference", "default", "typed" })
	void termPatternsCrossTheBoundary(String name) {
		try (VortexStore store = open(name, false); VortexStore loaded = open(name, true)) {
			assertEquals(quads.size(), store.size());
			assertEquals(store.size(), loaded.size());
			assertEquals(!name.equals("default") && !name.equals("typed"), store.dictionary().isPresent());
			for (String[] p : patterns()) {
				Set<String> want = scan(p);
				String tag = name + " " + Arrays.toString(p);
				assertEquals(want.size(), store.count(p[0], p[1], p[2], p[3]), tag);
				List<String[]> all = store.quads(p[0], p[1], p[2], p[3]);
				assertEquals(want, keys(all), tag);
				assertEquals(want.size(), all.size(), tag + " no duplicate rows");
			}
		}
	}

	@ParameterizedTest
	@ValueSource(strings = { "dictionary", "copy", "reference" })
	void codesCrossTheBoundary(String name) {
		try (VortexStore store = open(name, false)) {
			TermDictionary dict = store.dictionary().get();
			long terms = dict.size();
			TermDictionary.Ranges ranges = dict.ranges();
			long kinds = 0;
			for (TermDictionary.CodeRange r : List.of(ranges.literals(), ranges.iris(), ranges.blanks())) {
				kinds += r.end() - r.start();
			}
			assertEquals(dict.encode(""), ranges.defaultGraph());
			assertEquals(terms, kinds + (ranges.defaultGraph() == VortexStore.NONE ? 0 : 1));

			Set<String> distinct = new TreeSet<>();
			for (String[] q : quads) {
				distinct.addAll(Arrays.asList(q));
			}
			List<String> spellings = new ArrayList<>(distinct);
			long[] codes = new long[spellings.size()];
			for (int i = 0; i < codes.length; i++) {
				String t = spellings.get(i);
				codes[i] = dict.encode(t);
				assertNotEquals(VortexStore.NONE, codes[i], t);
				assertEquals(t, dict.decode(codes[i]));
				TermDictionary.CodeRange kind = t.startsWith("\"") ? ranges.literals()
						: t.startsWith("_:") ? ranges.blanks() : ranges.iris();
				assertEquals(!t.isEmpty(), kind.contains(codes[i]), t);
			}
			assertArrayEquals(spellings.toArray(new String[0]), dict.decodeMany(codes));
			assertEquals(VortexStore.NONE, dict.encode("<http://example.org/absent>"));
			assertNull(dict.decode(terms + 5));
			assertNull(dict.decode(VortexStore.NONE));
			assertArrayEquals(new String[] { spellings.get(0), null, null },
					dict.decodeMany(new long[] { codes[0], VortexStore.NONE, terms + 1 }));

			ByteBuffer out = ByteBuffer.allocateDirect(5 * VortexStore.ROW_BYTES).order(ByteOrder.nativeOrder());
			for (String[] p : patterns()) {
				Set<String> want = scan(p);
				long[] pattern = new long[4];
				for (int i = 0; i < 4; i++) {
					// An absent term: a code past the dictionary, not NONE, which would free the position.
					long code = p[i] == null ? VortexStore.NONE : dict.encode(p[i]);
					pattern[i] = p[i] != null && code == VortexStore.NONE ? terms + 7 : code;
				}
				String tag = name + " " + Arrays.toString(p);
				assertEquals(want.size(), store.countCodes(pattern[0], pattern[1], pattern[2], pattern[3]), tag);
				Set<String> seen = new HashSet<>();
				for (long offset = 0;; offset += 5) {
					int n = store.matchCodes(pattern[0], pattern[1], pattern[2], pattern[3], offset, out, 5);
					for (int row = 0; row < n; row++) {
						String[] q = new String[4];
						for (int i = 0; i < 4; i++) {
							q[i] = dict.decode(out.getLong(row * VortexStore.ROW_BYTES + 8 * i));
						}
						assertTrue(seen.add(key(q)), tag + " repeated row");
					}
					if (n < 5) {
						break;
					}
				}
				assertEquals(want, new TreeSet<>(seen), tag);
				int chunked = 0;
				try (CodeChunks chunks = store.matchCodeChunks(pattern[0], pattern[1], pattern[2], pattern[3])) {
					for (int n; (n = chunks.next(out, 5)) > 0;) {
						chunked += n;
					}
				}
				assertEquals(want.size(), chunked, tag + " chunks");
			}
		}
	}

	@Test
	void termsOutsideTheBasicMultilingualPlaneSurviveTheBoundary() {
		String emoji = null;
		for (String[] q : quads) {
			if (q[2].contains("😀")) {
				emoji = q[2];
				break;
			}
		}
		assertTrue(emoji != null, "the oracle holds a literal with a non-BMP character");
		try (VortexStore store = open("dictionary", false)) {
			List<String[]> rows = store.quads(null, null, emoji, null);
			assertFalse(rows.isEmpty());
			for (String[] row : rows) {
				assertEquals(emoji, row[2]);
			}
			long code = store.dictionary().get().encode(emoji);
			assertNotEquals(VortexStore.NONE, code);
			assertEquals(emoji, store.dictionary().get().decode(code));
		}
	}

	@Test
	void engineCountersNameEveryValueAndStayZeroWhenOff() {
		Map<String, Long> counters = VortexStore.engineCounters();
		assertEquals(18, counters.size());
		assertTrue(counters.containsKey("leaf_decoded") && counters.containsKey("typed_eq_probe_values"));
		if (!"1".equals(System.getenv("VORTEX_RDF_COUNTERS"))) {
			assertTrue(counters.values().stream().allMatch(v -> v == 0), "nothing counts without the switch");
		}
	}

	@Test
	void layoutsIndexesAndTheDefaultGraphAreReported() {
		try (VortexStore plain = open("dictionary", false); VortexStore copy = open("copy", false);
				VortexStore reference = open("reference", true); VortexStore flat = open("default", false)) {
			assertEquals("dictionary", plain.layout());
			assertEquals("default", flat.layout());
			assertTrue(plain.indexes().isEmpty());
			assertEquals(List.of("secondary-by-copy"), copy.indexes());
			assertEquals(List.of("secondary-by-reference"), reference.indexes());
			long inDefault = quads.stream().filter(q -> q[3].isEmpty()).count();
			assertTrue(inDefault > 0 && inDefault < quads.size());
			assertEquals(inDefault, plain.count(null, null, null, ""));
			assertEquals(quads.size(), plain.count(null, null, null, null));
		}
	}

	@Test
	void callersMistakesAreReportedNotAnswered() {
		try (VortexStore store = open("dictionary", false); VortexStore flat = open("default", false)) {
			VortexException subject = assertThrows(VortexException.class,
					() -> store.count("\"a literal\"", null, null, null));
			assertTrue(subject.getMessage().contains("pattern subject"), subject.getMessage());
			VortexException predicate = assertThrows(VortexException.class,
					() -> store.quads(null, "not a term", null, null));
			assertTrue(predicate.getMessage().contains("pattern predicate"), predicate.getMessage());

			assertTrue(flat.dictionary().isEmpty());
			VortexException none = assertThrows(VortexException.class,
					() -> flat.countCodes(VortexStore.NONE, VortexStore.NONE, VortexStore.NONE, VortexStore.NONE));
			assertTrue(none.getMessage().contains("no term dictionary"), none.getMessage());

			// A literal's code as a subject cannot match: no rows, no error.
			long literal = store.dictionary().get().encode(quads.stream().filter(q -> q[2].startsWith("\"")).findFirst().get()[2]);
			assertEquals(0, store.countCodes(literal, VortexStore.NONE, VortexStore.NONE, VortexStore.NONE));

			ByteBuffer heap = ByteBuffer.allocate(64);
			ByteBuffer small = ByteBuffer.allocateDirect(8).order(ByteOrder.nativeOrder());
			ByteBuffer swapped = ByteBuffer.allocateDirect(64)
					.order(ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
			ByteBuffer readOnly = ByteBuffer.allocateDirect(64).asReadOnlyBuffer().order(ByteOrder.nativeOrder());
			for (ByteBuffer bad : new ByteBuffer[] { heap, small, swapped, readOnly }) {
				assertThrows(IllegalArgumentException.class,
						() -> store.matchCodes(-1, -1, -1, -1, 0, bad, 2));
			}
			ByteBuffer good = ByteBuffer.allocateDirect(64).order(ByteOrder.nativeOrder());
			assertThrows(IllegalArgumentException.class, () -> store.matchCodes(-1, -1, -1, -1, -1, good, 2));
		}
		assertThrows(VortexException.class, () -> VortexStore.open(fixtures.resolve("missing.vortex")));
	}

	@Test
	void aClosedStoreRefusesCallsAndClosingTwiceIsHarmless() {
		VortexStore store = open("dictionary", false);
		TermDictionary dict = store.dictionary().get();
		store.close();
		store.close();
		assertThrows(IllegalStateException.class, store::size);
		assertThrows(IllegalStateException.class, () -> store.count(null, null, null, null));
		assertThrows(IllegalStateException.class, () -> dict.decode(0));
	}

	@Test
	void oneStoreServesManyThreads() throws Exception {
		ExecutorService pool = Executors.newFixedThreadPool(8);
		try (VortexStore store = open("copy", false)) {
			TermDictionary dict = store.dictionary().get();
			List<Future<Long>> results = new ArrayList<>();
			for (int t = 0; t < 8; t++) {
				results.add(pool.submit(() -> {
					long total = 0;
					for (int i = 0; i < 300; i++) {
						String[] q = quads.get((i * 7) % quads.size());
						total += store.count(q[0], null, null, null);
						total += store.countCodes(dict.encode(q[0]), VortexStore.NONE, dict.encode(q[2]), VortexStore.NONE);
					}
					return total;
				}));
			}
			long first = results.get(0).get();
			for (Future<Long> result : results) {
				assertEquals(first, result.get());
			}
		} finally {
			pool.shutdown();
		}
	}
}
