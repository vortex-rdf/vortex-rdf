package org.vortexrdf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * {@code quads()} and {@code decodeMany()} through packed chunks: the rows and their order, under chunk limits that
 * cut every chunk and send every term over 16 bytes on its own, the rows a caller may change, and the native cursors
 * each call frees.
 */
class ChunkedTransferTest {
	@BeforeAll
	static void loadOracle() throws IOException {
		VortexStoreTest.loadOracle();
	}

	/** The oracle's rows matching a pattern, tab-joined, in the oracle's order. */
	private static List<String> inOracleOrder(String[] pattern) {
		return VortexStoreTest.quads.stream().filter(q -> {
			for (int i = 0; i < 4; i++) {
				if (pattern[i] != null && !pattern[i].equals(q[i])) {
					return false;
				}
			}
			return true;
		}).map(VortexStoreTest::key).collect(Collectors.toList());
	}

	/** Rows tab-joined, in their order. */
	private static List<String> joined(List<String[]> rows) {
		return rows.stream().map(VortexStoreTest::key).collect(Collectors.toList());
	}

	@ParameterizedTest
	@ValueSource(strings = { "dictionary", "default", "typed" })
	void quadsComeInOracleOrderOnUnindexedStores(String name) {
		for (boolean inMemory : new boolean[] { false, true }) {
			try (VortexStore store = VortexStoreTest.open(name, inMemory)) {
				for (String[] p : VortexStoreTest.patterns()) {
					String tag = name + " inMemory=" + inMemory + " " + Arrays.toString(p);
					List<String> want = inOracleOrder(p);
					assertEquals(want, joined(store.quads(p[0], p[1], p[2], p[3])), tag);
					List<String> paged = new ArrayList<>();
					for (long offset = 0;; offset += 7) {
						List<String[]> page = store.quads(p[0], p[1], p[2], p[3], offset, 7);
						paged.addAll(joined(page));
						if (page.size() < 7) {
							break;
						}
					}
					assertEquals(want, paged, tag + " pages of 7");
				}
			}
		}
	}

	@ParameterizedTest
	@ValueSource(strings = { "dictionary", "copy", "reference", "default", "typed" })
	void quadsSurviveEveryChunkCut(String name) {
		for (boolean inMemory : new boolean[] { false, true }) {
			try (VortexStore store = VortexStoreTest.open(name, inMemory)) {
				for (String[] p : VortexStoreTest.patterns()) {
					String tag = name + " inMemory=" + inMemory + " " + Arrays.toString(p);
					List<String> whole = joined(store.quads(p[0], p[1], p[2], p[3]));
					assertEquals(whole, joined(store.quads(p[0], p[1], p[2], p[3], 0, -1, 1, 64, 16)), tag + " a row a chunk");
					assertEquals(whole, joined(store.quads(p[0], p[1], p[2], p[3], 0, -1, 7, 256, 64)), tag + " 7 rows a chunk");
				}
			}
		}
	}

	@ParameterizedTest
	@ValueSource(strings = { "dictionary", "copy", "reference", "default", "typed" })
	void largeTermsCrossTheBoundary(String name) {
		try (VortexStore store = VortexStoreTest.open(name, false)) {
			List<String> whole = joined(store.quads(null, null, null, null));
			// Every term over 16 bytes (most of them) crosses on its own.
			List<String> large = joined(store.quads(null, null, null, null, 0, -1, Payload.CHUNK_ROWS, 64, 16));
			assertEquals(whole, large, name);
			assertEquals(new TreeSet<>(inOracleOrder(new String[4])), new TreeSet<>(large), name);
			// The hard spellings: escapes, and characters outside ASCII and outside the BMP.
			int hard = 0;
			for (String[] q : VortexStoreTest.quads) {
				String row = VortexStoreTest.key(q);
				if (row.contains("\\") || row.chars().anyMatch(c -> c > 0x7F)) {
					assertTrue(large.contains(row), name + " " + row);
					hard++;
				}
			}
			assertTrue(hard >= 5, name + ": the oracle holds the hard spellings");
		}
	}

	@Test
	void rowsAreFreshArraysSharingStrings() {
		try (VortexStore store = VortexStoreTest.open("dictionary", false)) {
			String predicate = VortexStoreTest.quads.get(0)[1];
			List<String[]> rows = store.quads(null, predicate, null, null);
			assertTrue(rows.size() > 1);
			// One String per distinct term of a chunk; a fresh array per row.
			assertSame(rows.get(0)[1], rows.get(1)[1]);
			assertNotSame(rows.get(0), rows.get(1));
			rows.get(0)[1] = "x";
			assertEquals(predicate, rows.get(1)[1]);
			rows.sort(Comparator.comparing(r -> r[0]));
			rows.add(new String[4]);
		}
	}

	@ParameterizedTest
	@ValueSource(strings = { "dictionary", "copy", "reference" })
	void decodeManyAcrossSlices(String name) {
		try (VortexStore store = VortexStoreTest.open(name, false)) {
			TermDictionary dict = store.dictionary().get();
			Set<String> distinct = new TreeSet<>();
			for (String[] q : VortexStoreTest.quads) {
				distinct.addAll(Arrays.asList(q));
			}
			List<Long> list = distinct.stream().map(dict::encode).collect(Collectors.toList());
			list.addAll(List.of(VortexStore.NONE, dict.size() + 1, list.get(0), list.get(3), list.get(0)));
			long[] codes = list.stream().mapToLong(Long::longValue).toArray();
			for (int[] limits : new int[][] { { Payload.CHUNK_BYTES, Payload.LARGE_TERM }, { 64, 16 } }) {
				String tag = name + " " + Arrays.toString(limits);
				String[] terms = dict.decodeMany(codes, 3, limits[0], limits[1]);
				assertEquals(codes.length, terms.length, tag);
				for (int i = 0; i < codes.length; i++) {
					assertEquals(dict.decode(codes[i]), terms[i], tag + " code " + codes[i]);
				}
				assertNull(terms[distinct.size()], tag + " NONE");
				assertNull(terms[distinct.size() + 1], tag + " past the dictionary");
			}
			assertEquals(0, dict.decodeMany(new long[0]).length);
			// A slice of no codes would never advance.
			assertThrows(IllegalArgumentException.class,
					() -> dict.decodeMany(codes, 0, Payload.CHUNK_BYTES, Payload.LARGE_TERM));
		}
		VortexStore closed = VortexStoreTest.open(name, false);
		TermDictionary dict = closed.dictionary().get();
		closed.close();
		assertThrows(IllegalStateException.class, () -> dict.decodeMany(new long[0]));
	}

	@Test
	void noHandleOutlivesACall() {
		try (VortexStore store = VortexStoreTest.open("dictionary", false)) {
			assertEquals(0, Native.openHandles());
			for (String[] p : VortexStoreTest.patterns()) {
				store.quads(p[0], p[1], p[2], p[3]);
				assertEquals(0, Native.openHandles(), Arrays.toString(p));
			}
			assertEquals(VortexStoreTest.quads.size(), store.quads(null, null, null, null, 0, -1, 1, 64, 16).size());
			assertEquals(0, Native.openHandles(), "after a read of a row a chunk");
			assertThrows(VortexException.class, () -> store.quads(null, "not a term", null, null));
			assertEquals(0, Native.openHandles(), "after a malformed term");
			store.dictionary().get().decodeMany(new long[] { 0, 1, 2, 3, 4 });
			assertEquals(0, Native.openHandles(), "after decodeMany");
		}
	}
}
