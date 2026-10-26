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

/** The Java API against a scan of the quads the fixture stores were built from. */
class VortexStoreTest {
	private static Path fixtures;
	private static List<String[]> quads;

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

	private static VortexStore open(String name, boolean inMemory) {
		return VortexStore.open(fixtures.resolve(name + ".vortex"), inMemory);
	}

	private static String key(String[] quad) {
		return String.join("\t", quad);
	}

	private static Set<String> keys(List<String[]> rows) {
		return rows.stream().map(VortexStoreTest::key).collect(Collectors.toCollection(TreeSet::new));
	}

	/** The oracle's quads matching a pattern, by scan. */
	private static Set<String> scan(String[] pattern) {
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

	/** Every subset of positions bound to a stored quad's terms, for a spread of quads, plus absent terms. */
	private static List<String[]> patterns() {
		List<String[]> out = new ArrayList<>();
		for (int row = 0; row < quads.size(); row += 31) {
			for (int mask = 0; mask < 16; mask++) {
				String[] p = new String[4];
				for (int i = 0; i < 4; i++) {
					p[i] = (mask & (1 << i)) != 0 ? quads.get(row)[i] : null;
				}
				out.add(p);
			}
		}
		for (int i = 0; i < 4; i++) {
			String[] p = new String[4];
			p[i] = "<http://example.org/absent>";
			out.add(p);
		}
		return out;
	}

	@ParameterizedTest
	@ValueSource(strings = { "dictionary", "copy", "reference", "default" })
	void termPatternsMatchAScanOnEveryStore(String name) {
		for (boolean inMemory : new boolean[] { false, true }) {
			try (VortexStore store = open(name, inMemory)) {
				assertEquals(quads.size(), store.size());
				assertEquals(!name.equals("default"), store.dictionary().isPresent());
				for (String[] p : patterns()) {
					Set<String> want = scan(p);
					String tag = name + " " + Arrays.toString(p);
					assertEquals(want.size(), store.count(p[0], p[1], p[2], p[3]), tag);
					List<String[]> all = store.quads(p[0], p[1], p[2], p[3]);
					assertEquals(want, keys(all), tag);
					assertEquals(want.size(), all.size(), tag + " no duplicate rows");
					Set<String> paged = new TreeSet<>();
					for (long offset = 0; offset < want.size(); offset += 7) {
						List<String[]> page = store.quads(p[0], p[1], p[2], p[3], offset, 7);
						assertEquals(Math.min(7, want.size() - offset), page.size(), tag + " page at " + offset);
						paged.addAll(keys(page));
					}
					assertEquals(want, paged, tag + " pages");
				}
			}
		}
	}

	@ParameterizedTest
	@ValueSource(strings = { "dictionary", "copy", "reference" })
	void codePatternsMatchAScanAndRoundTripThroughTheDictionary(String name) {
		for (boolean inMemory : new boolean[] { false, true }) {
			try (VortexStore store = open(name, inMemory)) {
				TermDictionary dict = store.dictionary().get();
				long terms = dict.size();
				TermDictionary.Ranges ranges = dict.ranges();
				long kinds = 0;
				for (long[] r : new long[][] { ranges.literals(), ranges.iris(), ranges.blanks() }) {
					kinds += r[1] - r[0];
				}
				assertEquals(dict.encode(""), ranges.defaultGraph());
				assertEquals(terms, kinds + (ranges.defaultGraph() == VortexStore.NONE ? 0 : 1));

				Set<String> distinct = new TreeSet<>();
				for (String[] q : quads) {
					distinct.addAll(Arrays.asList(q));
				}
				List<String> terms2 = new ArrayList<>(distinct);
				long[] codes = new long[terms2.size()];
				for (int i = 0; i < codes.length; i++) {
					codes[i] = dict.encode(terms2.get(i));
					assertNotEquals(VortexStore.NONE, codes[i], terms2.get(i));
					assertEquals(terms2.get(i), dict.decode(codes[i]));
				}
				assertArrayEquals(terms2.toArray(new String[0]), dict.decodeMany(codes));
				assertEquals(VortexStore.NONE, dict.encode("<http://example.org/absent>"));
				assertNull(dict.decode(terms + 5));
				assertNull(dict.decode(VortexStore.NONE));
				String[] decoded = dict.decodeMany(new long[] { codes[0], VortexStore.NONE, terms + 1 });
				assertEquals(terms2.get(0), decoded[0]);
				assertNull(decoded[1]);
				assertNull(decoded[2]);

				ByteBuffer out = ByteBuffer.allocateDirect(5 * VortexStore.ROW_BYTES).order(ByteOrder.nativeOrder());
				for (String[] p : patterns()) {
					Set<String> want = scan(p);
					long[] pattern = new long[4];
					for (int i = 0; i < 4; i++) {
						pattern[i] = p[i] == null ? VortexStore.NONE : dict.encode(p[i]) == VortexStore.NONE ? terms + 7 : dict.encode(p[i]);
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
					long[] flat = store.matchCodes(pattern[0], pattern[1], pattern[2], pattern[3], 0, 1000);
					assertEquals(4 * want.size(), flat.length, tag + " array form");
				}
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
			assertThrows(IllegalArgumentException.class, () -> store.quads(null, null, null, null, -1, 1));
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
