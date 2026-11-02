package org.vortexrdf;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
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
		VortexRdfStoreTest.loadOracle();
	}

	/** Rows tab-joined, in their order. */
	static List<String> joined(List<String[]> rows) {
		return rows.stream().map(VortexRdfStoreTest::key).collect(Collectors.toList());
	}

	@ParameterizedTest
	@ValueSource(strings = { "dictionary", "copy", "reference", "default", "typed" })
	void quadsSurviveEveryChunkCut(String name) {
		for (boolean inMemory : new boolean[] { false, true }) {
			try (VortexRdfStore store = VortexRdfStoreTest.open(name, inMemory)) {
				for (String[] p : VortexRdfStoreTest.patterns()) {
					String tag = name + " inMemory=" + inMemory + " " + Arrays.toString(p);
					List<String> whole = joined(store.quads(p[0], p[1], p[2], p[3]));
					assertEquals(whole, joined(store.quads(p[0], p[1], p[2], p[3], 1, 64, 16)), tag + " a row a chunk");
					assertEquals(whole, joined(store.quads(p[0], p[1], p[2], p[3], 7, 256, 64)), tag + " 7 rows a chunk");
				}
			}
		}
	}

	@ParameterizedTest
	@ValueSource(strings = { "dictionary", "copy", "reference", "default", "typed" })
	void largeTermsCrossTheBoundary(String name) {
		try (VortexRdfStore store = VortexRdfStoreTest.open(name, false)) {
			List<String> whole = joined(store.quads(null, null, null, null));
			// Every term over 16 bytes (most of them) crosses on its own.
			List<String> large = joined(store.quads(null, null, null, null, Payload.CHUNK_ROWS, 64, 16));
			assertEquals(whole, large, name);
			assertEquals(new TreeSet<>(VortexRdfStoreTest.inOracleOrder(new String[4])), new TreeSet<>(large), name);
			// The hard spellings: escapes, and characters outside ASCII and outside the BMP.
			int hard = 0;
			for (String[] q : VortexRdfStoreTest.quads) {
				String row = VortexRdfStoreTest.key(q);
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
		try (VortexRdfStore store = VortexRdfStoreTest.open("dictionary", false)) {
			String predicate = VortexRdfStoreTest.quads.get(0)[1];
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
		try (VortexRdfStore store = VortexRdfStoreTest.open(name, false)) {
			TermDictionary dict = store.dictionary().get();
			Set<String> distinct = new TreeSet<>();
			for (String[] q : VortexRdfStoreTest.quads) {
				distinct.addAll(Arrays.asList(q));
			}
			List<Long> list = distinct.stream().map(dict::encode).collect(Collectors.toList());
			list.addAll(List.of(VortexRdfStore.NONE, dict.size() + 1, list.get(0), list.get(3), list.get(0)));
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
			// Fewer codes than a chunk pays for cross a string apiece: the same answers, null for a miss.
			long[] few = { codes[0], VortexRdfStore.NONE, codes[1], dict.size() + 1, codes[0] };
			for (int n = 1; n < TermDictionary.CHUNKED_CODES && n <= few.length; n++) {
				long[] some = Arrays.copyOf(few, n);
				String[] want = new String[n];
				for (int i = 0; i < n; i++) {
					want[i] = dict.decode(some[i]);
				}
				assertArrayEquals(want, dict.decodeMany(some), name + " " + n + " codes");
			}
			// A slice of no codes would never advance.
			assertThrows(IllegalArgumentException.class,
					() -> dict.decodeMany(codes, 0, Payload.CHUNK_BYTES, Payload.LARGE_TERM));
		}
		VortexRdfStore closed = VortexRdfStoreTest.open(name, false);
		TermDictionary dict = closed.dictionary().get();
		closed.close();
		assertThrows(IllegalStateException.class, () -> dict.decodeMany(new long[0]));
	}

	@Test
	void noHandleOutlivesACall() {
		try (VortexRdfStore store = VortexRdfStoreTest.open("dictionary", false)) {
			assertEquals(0, Native.openHandles());
			for (String[] p : VortexRdfStoreTest.patterns()) {
				store.quads(p[0], p[1], p[2], p[3]);
				assertEquals(0, Native.openHandles(), Arrays.toString(p));
			}
			assertEquals(VortexRdfStoreTest.quads.size(), store.quads(null, null, null, null, 1, 64, 16).size());
			assertEquals(0, Native.openHandles(), "after a read of a row a chunk");
			assertThrows(VortexException.class, () -> store.quads(null, "not a term", null, null));
			assertEquals(0, Native.openHandles(), "after a malformed term");
			store.dictionary().get().decodeMany(new long[] { 0, 1, 2, 3, 4 });
			assertEquals(0, Native.openHandles(), "after decodeMany");
		}
	}
}
