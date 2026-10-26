package org.vortexrdf;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A read-only <a href="https://github.com/vortex-rdf/vortex-rdf">Vortex-RDF</a> store opened from a
 * {@code .vortex} file and queried in place: by <b>terms</b> (N-Triples spellings), for every layout, or by
 * <b>term codes</b> (the store's own integers), for a store with a term dictionary.
 *
 * <p>A pattern has four positions, {@code (s, p, o, g)}; a {@code null} position is free. Terms are N-Triples
 * spellings ({@code <iri>}, {@code _:b0}, {@code "lit"@en}, {@code "3"^^<http://www.w3.org/2001/XMLSchema#integer>});
 * the default graph is the empty string {@code ""}, which is also how a pattern selects it. A malformed term raises
 * {@link VortexException} naming the position.
 *
 * <p>A term code is a non-negative {@code long}; {@link #NONE} ({@code -1}) is a free position of a code pattern, and
 * also what {@link TermDictionary#encode} answers for a term the store lacks: check that answer before putting it into
 * a pattern, where it would match every quad. Codes are the ones the Python and JavaScript bindings call term codes.
 * Which store has a dictionary is told by {@link #dictionary()} ({@code dictionary} layout); a code call on another
 * store raises {@link VortexException}.
 *
 * <p>Thread safe: any number of threads may query one store. {@link #close()} waits for the calls in progress and
 * later calls raise {@link IllegalStateException}. The native library is located as described in {@code Native}.
 */
public final class VortexStore implements AutoCloseable {
	/**
	 * "No code": a free position of a code pattern, and what {@link TermDictionary#encode} answers for an unknown term.
	 * Check {@code encode}'s answer before putting it into a pattern, where {@code NONE} matches every quad.
	 */
	public static final long NONE = -1;

	/** Bytes per row written by {@link #matchCodes(long, long, long, long, long, ByteBuffer, int)}: four longs. */
	public static final int ROW_BYTES = 32;

	private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	private long handle;
	private final TermDictionary dictionary;

	private VortexStore(long handle) {
		this.handle = handle;
		this.dictionary = Native.hasDictionary(handle) ? new TermDictionary(this) : null;
	}

	/** Opens a store memory-mapped: the operating system's page cache holds what queries touch. */
	public static VortexStore open(Path file) {
		return open(file, false);
	}

	/**
	 * Opens a store; with {@code inMemory} the whole file is loaded and later queries do not read it again.
	 * A mapped file must not be modified while open: replace it by renaming a new file over it.
	 */
	public static VortexStore open(Path file, boolean inMemory) {
		Objects.requireNonNull(file, "file");
		return new VortexStore(Native.open(file.toAbsolutePath().toString(), inMemory));
	}

	/** Takes the handle for one call; {@link #exit()} releases it. */
	long enter() {
		lock.readLock().lock();
		if (handle == 0) {
			lock.readLock().unlock();
			throw new IllegalStateException("the store is closed");
		}
		return handle;
	}

	void exit() {
		lock.readLock().unlock();
	}

	/** Closes the store, after the calls in progress; closing twice is harmless. */
	@Override
	public void close() {
		lock.writeLock().lock();
		try {
			if (handle != 0) {
				Native.close(handle);
				handle = 0;
			}
		} finally {
			lock.writeLock().unlock();
		}
	}

	/** The number of quads. */
	public long size() {
		long h = enter();
		try {
			return Native.size(h);
		} finally {
			exit();
		}
	}

	/** The column layout: {@code "dictionary"}, {@code "default"} or {@code "typed-object"}. */
	public String layout() {
		long h = enter();
		try {
			return Native.layout(h);
		} finally {
			exit();
		}
	}

	/** The secondary indexes the file carries: {@code "secondary-by-copy"}, {@code "secondary-by-reference"}. */
	public List<String> indexes() {
		long h = enter();
		try {
			return Collections.unmodifiableList(Arrays.asList(Native.indexes(h)));
		} finally {
			exit();
		}
	}

	/** The term dictionary, present for a store of the {@code dictionary} layout. */
	public Optional<TermDictionary> dictionary() {
		return Optional.ofNullable(dictionary);
	}

	// ---- term patterns ----

	/** The number of quads matching a pattern of terms, counted without reading any term. */
	public long count(String s, String p, String o, String g) {
		long h = enter();
		try {
			return Native.count(h, s, p, o, g);
		} finally {
			exit();
		}
	}

	/** Every quad matching a pattern of terms, as {@code {s, p, o, g}} spellings, in the store's row order. */
	public List<String[]> quads(String s, String p, String o, String g) {
		return quads(s, p, o, g, 0, -1);
	}

	/** {@code limit} quads (all when negative) after {@code offset} rows of the match, in the store's row order. */
	public List<String[]> quads(String s, String p, String o, String g, long offset, int limit) {
		if (offset < 0) {
			throw new IllegalArgumentException("offset < 0");
		}
		long h = enter();
		String[] flat;
		try {
			flat = Native.quads(h, s, p, o, g, offset, limit);
		} finally {
			exit();
		}
		List<String[]> rows = new ArrayList<>(flat.length / 4);
		for (int i = 0; i < flat.length; i += 4) {
			rows.add(new String[] { flat[i], flat[i + 1], flat[i + 2], flat[i + 3] });
		}
		return rows;
	}

	// ---- term codes ----

	/** The number of quads matching a pattern of codes, exact and without reading rows; negative is free. */
	public long countCodes(long s, long p, long o, long g) {
		long h = enter();
		try {
			return Native.countCodes(h, s, p, o, g);
		} finally {
			exit();
		}
	}

	/**
	 * Writes up to {@code maxRows} quads matching a pattern of codes, after {@code offset} rows of the match, as
	 * {@code (s, p, o, g)} codes of eight bytes each ({@link #ROW_BYTES} per row), from the start of {@code out}
	 * (its position and limit are neither read nor changed), and returns the rows written; negative is free. A code
	 * that cannot match (past the dictionary, or a term in a position it cannot take) gives no rows.
	 *
	 * @param out a writable direct buffer in native byte order
	 *            ({@code ByteBuffer.allocateDirect(n).order(ByteOrder.nativeOrder())}) of at least
	 *            {@code maxRows * ROW_BYTES} bytes; reuse one across calls
	 */
	public int matchCodes(long s, long p, long o, long g, long offset, ByteBuffer out, int maxRows) {
		Objects.requireNonNull(out, "out");
		checkWindow(offset, maxRows);
		if (!out.isDirect() || out.isReadOnly() || out.order() != ByteOrder.nativeOrder()
				|| out.capacity() < (long) maxRows * ROW_BYTES) {
			throw new IllegalArgumentException("out must be a writable, direct, native-order buffer of at least maxRows * "
					+ ROW_BYTES + " bytes");
		}
		long h = enter();
		try {
			return Native.matchCodes(h, s, p, o, g, out, offset, maxRows);
		} finally {
			exit();
		}
	}

	/** {@link #matchCodes(long, long, long, long, long, ByteBuffer, int)} into a new array of four codes per row. */
	public long[] matchCodes(long s, long p, long o, long g, long offset, int limit) {
		checkWindow(offset, limit);
		ByteBuffer out = ByteBuffer.allocateDirect(Math.multiplyExact(limit, ROW_BYTES)).order(ByteOrder.nativeOrder());
		int rows = matchCodes(s, p, o, g, offset, out, limit);
		long[] codes = new long[4 * rows];
		out.asLongBuffer().get(codes);
		return codes;
	}

	private static void checkWindow(long offset, int rows) {
		if (offset < 0 || rows < 0) {
			throw new IllegalArgumentException("offset and maxRows must not be negative");
		}
	}
}
