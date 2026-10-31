package org.vortexrdf;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * {@code iterateQuads} and {@code matchCodeChunks}: the rows of {@code quads()} in its order at every chunk size (the
 * oracle's rows, in the oracle's order but on a pattern the copy index serves), and the lifetime of their native
 * streams, freed at the end, on close, with their store, and when forgotten, across threads.
 */
class IteratorTest {
	@BeforeAll
	static void loadOracle() throws IOException {
		VortexStoreTest.loadOracle();
	}

	private static List<String> drained(QuadIterator rows) {
		List<String> out = new ArrayList<>();
		while (rows.hasNext()) {
			out.add(VortexStoreTest.key(rows.next()));
		}
		return out;
	}

	/**
	 * The rows a stream of pattern {@code p} must yield, in order: {@code quads()}'s, whose rows are the oracle's, in
	 * the oracle's order on a store without the copy index (a pattern the copy index serves reads in its order).
	 */
	private static List<String> readOrder(VortexStore store, String name, String[] p, String tag) {
		List<String> want = ChunkedTransferTest.joined(store.quads(p[0], p[1], p[2], p[3]));
		List<String> oracle = ChunkedTransferTest.inOracleOrder(p);
		assertEquals(new TreeSet<>(oracle), new TreeSet<>(want), tag + ": the oracle's rows");
		assertEquals(oracle.size(), want.size(), tag + ": each row once");
		if (!name.equals("copy")) {
			assertEquals(oracle, want, tag + ": the oracle's order");
		}
		return want;
	}

	/** The rows of a code-chunk stream, spelled through {@code decode}. */
	private static List<String> drainedCodes(CodeChunks chunks, TermDictionary dict, int maxRows) {
		ByteBuffer out = ByteBuffer.allocateDirect(maxRows * VortexStore.ROW_BYTES).order(ByteOrder.nativeOrder());
		List<String> rows = new ArrayList<>();
		for (int n; (n = chunks.next(out, maxRows)) > 0;) {
			assertTrue(n <= maxRows);
			for (int row = 0; row < n; row++) {
				String[] q = new String[4];
				for (int i = 0; i < 4; i++) {
					q[i] = dict.decode(out.getLong(row * VortexStore.ROW_BYTES + 8 * i));
				}
				rows.add(VortexStoreTest.key(q));
			}
		}
		return rows;
	}

	@ParameterizedTest
	@ValueSource(strings = { "dictionary", "copy", "reference", "default", "typed" })
	void iteratorsYieldTheReadInOrder(String name) {
		for (boolean inMemory : new boolean[] { false, true }) {
			try (VortexStore store = VortexStoreTest.open(name, inMemory)) {
				for (String[] p : VortexStoreTest.patterns()) {
					String read = name + " inMemory=" + inMemory + " " + Arrays.toString(p);
					List<String> want = readOrder(store, name, p, read);
					for (int batchRows : new int[] { 1, 7, VortexStore.DEFAULT_BATCH_ROWS }) {
						String tag = read + " batch " + batchRows;
						try (QuadIterator rows = store.iterateQuads(p[0], p[1], p[2], p[3], batchRows)) {
							assertEquals(want, drained(rows), tag);
						}
					}
				}
				assertEquals(0, Native.openHandles(), name);
			}
		}
	}

	@ParameterizedTest
	@ValueSource(strings = { "dictionary", "copy", "reference" })
	void codeChunksYieldTheReadInOrder(String name) {
		try (VortexStore store = VortexStoreTest.open(name, false)) {
			TermDictionary dict = store.dictionary().get();
			long terms = dict.size();
			for (String[] p : VortexStoreTest.patterns()) {
				long[] pattern = new long[4];
				for (int i = 0; i < 4; i++) {
					// An absent term: a code past the dictionary, not NONE, which would free the position.
					long code = p[i] == null ? VortexStore.NONE : dict.encode(p[i]);
					pattern[i] = p[i] != null && code == VortexStore.NONE ? terms + 7 : code;
				}
				List<String> want = readOrder(store, name, p, name + " " + Arrays.toString(p));
				for (int maxRows : new int[] { 1, 5, 4_096 }) {
					String tag = name + " " + Arrays.toString(p) + " maxRows " + maxRows;
					try (CodeChunks chunks = store.matchCodeChunks(pattern[0], pattern[1], pattern[2], pattern[3], 7)) {
						assertEquals(want, drainedCodes(chunks, dict, maxRows), tag);
					}
				}
			}
			assertEquals(0, Native.openHandles(), name);
		}
	}

	@Test
	void streamClosesItsIterator() {
		try (VortexStore store = VortexStoreTest.open("dictionary", false)) {
			try (Stream<String[]> rows = store.iterateQuads(null, null, null, null).stream()) {
				assertEquals(2, rows.limit(2).count());
			}
			assertEquals(0, Native.openHandles());
			List<String> all = store.iterateQuads(null, null, null, null).stream().map(VortexStoreTest::key)
					.collect(Collectors.toList());
			assertEquals(ChunkedTransferTest.inOracleOrder(new String[4]), all);
			assertEquals(0, Native.openHandles(), "a drained iterator frees its stream");
		}
	}

	@Test
	void storeCloseEndsIteratorsOnOtherThreads() throws Exception {
		VortexStore store = VortexStoreTest.open("dictionary", false);
		CountDownLatch started = new CountDownLatch(1);
		AtomicReference<Throwable> thrown = new AtomicReference<>();
		int[] rows = { 0 };
		Thread puller = new Thread(() -> {
			try (QuadIterator it = store.iterateQuads(null, null, null, null, 1)) {
				while (it.hasNext()) {
					it.next();
					rows[0]++;
					started.countDown();
				}
			} catch (Throwable t) {
				thrown.set(t);
			}
		});
		puller.start();
		started.await();
		store.close();
		puller.join(10_000);
		assertFalse(puller.isAlive());
		assertTrue(rows[0] > 0, "rows before the close");
		Throwable t = thrown.get();
		// The store may close after the puller drained every row; otherwise the puller sees the close.
		if (rows[0] < VortexStoreTest.quads.size()) {
			assertTrue(t instanceof IllegalStateException, String.valueOf(t));
		} else {
			assertNull(t);
		}
		assertEquals(0, Native.openHandles());
	}

	@Test
	void forgottenIteratorsAreFreedByTheStore() {
		VortexStore store = VortexStoreTest.open("dictionary", false);
		List<Object> forgotten = new ArrayList<>();
		for (int i = 0; i < 3; i++) {
			QuadIterator it = store.iterateQuads(null, null, null, null, 7);
			it.next();
			forgotten.add(it);
		}
		ByteBuffer out = ByteBuffer.allocateDirect(VortexStore.ROW_BYTES).order(ByteOrder.nativeOrder());
		for (int i = 0; i < 2; i++) {
			CodeChunks chunks = store.matchCodeChunks(VortexStore.NONE, VortexStore.NONE, VortexStore.NONE,
					VortexStore.NONE, 7);
			assertEquals(1, chunks.next(out, 1));
			forgotten.add(chunks);
		}
		assertEquals(5, Native.openHandles());
		store.close();
		assertEquals(0, Native.openHandles());
		assertEquals(5, forgotten.size());
	}

	@Test
	void closedOrExhaustedIteratorsAnswerNoMoreRows() {
		VortexStore store = VortexStoreTest.open("dictionary", false);
		String subject = VortexStoreTest.quads.get(0)[0];
		QuadIterator exhausted = store.iterateQuads(subject, null, null, null);
		drained(exhausted);
		assertThrows(NoSuchElementException.class, exhausted::next);
		QuadIterator closed = store.iterateQuads(null, null, null, null);
		closed.close();
		closed.close();
		assertFalse(closed.hasNext());
		CodeChunks chunks = store.matchCodeChunks(VortexStore.NONE, VortexStore.NONE, VortexStore.NONE,
				VortexStore.NONE);
		chunks.close();
		chunks.close();
		ByteBuffer out = ByteBuffer.allocateDirect(VortexStore.ROW_BYTES).order(ByteOrder.nativeOrder());
		assertEquals(0, chunks.next(out, 1));
		QuadIterator open = store.iterateQuads(null, null, null, null, 1);
		open.next();
		store.close();
		// Whatever the store's state: no more rows from an ended or closed iterator, and a refusal from an open one.
		assertFalse(exhausted.hasNext());
		assertFalse(closed.hasNext());
		assertEquals(0, chunks.next(out, 1));
		assertThrows(IllegalStateException.class, open::hasNext);
		assertThrows(IllegalStateException.class, open::next);
		open.close();
		assertFalse(open.hasNext());
		assertEquals(0, Native.openHandles());
	}

	@Test
	void twoThreadsPullingOneIteratorDoNotCrash() throws Exception {
		try (VortexStore store = VortexStoreTest.open("dictionary", false)) {
			QuadIterator it = store.iterateQuads(null, null, null, null, 1);
			Runnable drain = () -> {
				try {
					while (it.hasNext()) {
						it.next();
					}
				} catch (RuntimeException expected) {
					// No Iterator is thread-safe: interleaved pulls may throw, but never crash the JVM.
				}
			};
			Thread a = new Thread(drain);
			Thread b = new Thread(drain);
			a.start();
			b.start();
			a.join(30_000);
			b.join(30_000);
			assertFalse(a.isAlive() || b.isAlive());
			it.close();
		}
		assertEquals(0, Native.openHandles());
	}

	@Test
	void badArgumentsAreRefused() {
		try (VortexStore store = VortexStoreTest.open("dictionary", false);
				VortexStore flat = VortexStoreTest.open("default", false)) {
			assertThrows(IllegalArgumentException.class, () -> store.iterateQuads(null, null, null, null, 0));
			assertThrows(IllegalArgumentException.class,
					() -> store.matchCodeChunks(VortexStore.NONE, VortexStore.NONE, VortexStore.NONE, VortexStore.NONE, 0));
			VortexException malformed = assertThrows(VortexException.class,
					() -> store.iterateQuads(null, "not a term", null, null));
			assertTrue(malformed.getMessage().contains("pattern predicate"), malformed.getMessage());
			VortexException none = assertThrows(VortexException.class,
					() -> flat.matchCodeChunks(VortexStore.NONE, VortexStore.NONE, VortexStore.NONE, VortexStore.NONE));
			assertTrue(none.getMessage().contains("no term dictionary"), none.getMessage());
			try (CodeChunks chunks = store.matchCodeChunks(VortexStore.NONE, VortexStore.NONE, VortexStore.NONE,
					VortexStore.NONE)) {
				ByteBuffer heap = ByteBuffer.allocate(64);
				ByteBuffer small = ByteBuffer.allocateDirect(8).order(ByteOrder.nativeOrder());
				ByteBuffer swapped = ByteBuffer.allocateDirect(64)
						.order(ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
				ByteBuffer readOnly = ByteBuffer.allocateDirect(64).asReadOnlyBuffer().order(ByteOrder.nativeOrder());
				for (ByteBuffer bad : new ByteBuffer[] { heap, small, swapped, readOnly }) {
					assertThrows(IllegalArgumentException.class, () -> chunks.next(bad, 2));
				}
				ByteBuffer good = ByteBuffer.allocateDirect(64).order(ByteOrder.nativeOrder());
				assertThrows(IllegalArgumentException.class, () -> chunks.next(good, 0));
				assertNotEquals(0, chunks.next(good, 2), "still readable after refusals");
			}
		}
		assertEquals(0, Native.openHandles());
	}

	@Test
	void anUnreachableIteratorIsFreed() throws InterruptedException {
		try (VortexStore store = VortexStoreTest.open("dictionary", false)) {
			forget(store);
			long deadline = System.currentTimeMillis() + 10_000;
			while (Native.openHandles() != 0) {
				if (System.currentTimeMillis() > deadline) {
					fail("the forgotten iterator's stream was not freed");
				}
				System.gc();
				Thread.sleep(100);
			}
		}
	}

	/** Opens an iterator, pulls a row and drops it, in a frame of its own so nothing keeps it reachable. */
	private static void forget(VortexStore store) {
		QuadIterator it = store.iterateQuads(null, null, null, null, 7);
		it.next();
		assertEquals(1, Native.openHandles());
	}
}
