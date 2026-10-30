package org.vortexrdf;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.LongFunction;
import java.util.function.LongToIntFunction;
import java.util.function.LongUnaryOperator;

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

	/** The most rows {@link #iterateQuads} and {@link #matchCodeChunks} read at a time, unless told otherwise. */
	public static final int DEFAULT_BATCH_ROWS = 65_536;

	private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	private long handle;
	private final TermDictionary dictionary;
	/** The native streams of this store's open iterators, not the iterators, so a forgotten one can be collected. */
	private final Set<NativeStream> streams = new HashSet<>();
	/** Set by {@link #close()}: an iterator that has not ended refuses to go on. */
	private volatile boolean closed;

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

	/** {@code call}'s answer for the open store's handle, the store held open until it returns. */
	<T> T call(LongFunction<T> call) {
		long h = enter();
		try {
			return call.apply(h);
		} finally {
			lock.readLock().unlock();
		}
	}

	/** {@link #call} of a {@code long} answer. */
	long callLong(LongUnaryOperator call) {
		long h = enter();
		try {
			return call.applyAsLong(h);
		} finally {
			lock.readLock().unlock();
		}
	}

	/** {@link #call} of an {@code int} answer. */
	int callInt(LongToIntFunction call) {
		long h = enter();
		try {
			return call.applyAsInt(h);
		} finally {
			lock.readLock().unlock();
		}
	}

	/** The handle, read-locked: the caller unlocks. */
	private long enter() {
		lock.readLock().lock();
		if (handle == 0) {
			lock.readLock().unlock();
			throw new IllegalStateException("the store is closed");
		}
		return handle;
	}

	/**
	 * Closes the store, after the calls in progress, and frees its open iterators' native streams; closing twice is
	 * harmless.
	 */
	@Override
	public void close() {
		lock.writeLock().lock();
		try {
			if (handle != 0) {
				closed = true;
				NativeStream[] open;
				synchronized (streams) {
					open = streams.toArray(new NativeStream[0]);
				}
				for (NativeStream stream : open) {
					stream.free();
				}
				Native.close(handle);
				handle = 0;
			}
		} finally {
			lock.writeLock().unlock();
		}
	}

	/** Whether {@link #close()} has run. */
	boolean isClosed() {
		return closed;
	}

	/** Tracks an iterator's native stream, for {@link #close()} to free. */
	void track(NativeStream stream) {
		synchronized (streams) {
			streams.add(stream);
		}
	}

	/** Stops tracking a freed native stream. */
	void forget(NativeStream stream) {
		synchronized (streams) {
			streams.remove(stream);
		}
	}

	/** The number of quads. */
	public long size() {
		return callLong(Native::size);
	}

	/** The column layout: {@code "dictionary"}, {@code "default"} or {@code "typed-object"}. */
	public String layout() {
		return call(Native::layout);
	}

	/** The secondary indexes the file carries: {@code "secondary-by-copy"}, {@code "secondary-by-reference"}. */
	public List<String> indexes() {
		return List.of(call(Native::indexes));
	}

	/** The term dictionary, present for a store of the {@code dictionary} layout. */
	/**
	 * vortex-rdf's encoded-search work counters by name, cumulative in this process: the chunk leaves searched
	 * encoded or decoded, resident probes, gathers, shared canonical decodes and equality-scan binds, each also by the
	 * number of values. They count only with {@code VORTEX_RDF_COUNTERS=1} set when the native library loads; otherwise
	 * every value is 0. Meant for measurements, not for applications.
	 */
	public static Map<String, Long> engineCounters() {
		String[] names = Native.engineCounterNames();
		long[] values = Native.engineCounters();
		Map<String, Long> counters = new LinkedHashMap<>();
		for (int i = 0; i < names.length; i++) {
			counters.put(names[i], values[i]);
		}
		return Collections.unmodifiableMap(counters);
	}

	public Optional<TermDictionary> dictionary() {
		return Optional.ofNullable(dictionary);
	}

	// ---- term patterns ----

	/** The number of quads matching a pattern of terms, counted without reading any term. */
	public long count(String s, String p, String o, String g) {
		return callLong(h -> Native.count(h, s, p, o, g));
	}

	/** Every quad matching a pattern of terms, as {@code {s, p, o, g}} spellings, in the store's row order. */
	public List<String[]> quads(String s, String p, String o, String g) {
		return quads(s, p, o, g, Payload.CHUNK_ROWS, Payload.CHUNK_BYTES, Payload.LARGE_TERM);
	}

	/**
	 * {@link #quads(String, String, String, String)} with the chunk limits the rows cross in, which tests set small to
	 * cut every chunk. The read's cursor lives within this call: freed before it returns or throws.
	 */
	List<String[]> quads(String s, String p, String o, String g, int chunkRows, int chunkBytes, int largeTerm) {
		return call(h -> {
			Object[] first = Native.quadsOpen(h, s, p, o, g, chunkRows, chunkBytes, largeTerm);
			long cursor = Payload.cursor(first);
			try {
				List<String[]> rows = new ArrayList<>(Payload.rows(first));
				Payload.addQuads(first, rows);
				Object[] next;
				while (cursor != 0 && (next = Native.cursorNext(cursor)) != null) {
					Payload.addQuads(next, rows);
				}
				return rows;
			} finally {
				if (cursor != 0) {
					Native.cursorClose(cursor);
				}
			}
		});
	}

	/** {@link #iterateQuads(String, String, String, String, int)} {@link #DEFAULT_BATCH_ROWS} rows at a time. */
	public QuadIterator iterateQuads(String s, String p, String o, String g) {
		return iterateQuads(s, p, o, g, DEFAULT_BATCH_ROWS);
	}

	/**
	 * The quads matching a pattern of terms, read at most {@code batchRows} rows at a time, in base row order
	 * ({@code (s, p, o, g)}): the rows of {@link #quads(String, String, String, String)}, which on a pattern a
	 * {@code secondary-by-copy} index serves may return them in the index's order instead. The pattern is checked
	 * and matched now; the rows are read as the iterator is pulled. Close the iterator, or drain it, when done.
	 *
	 * @param batchRows the most rows read at a time, at least 1
	 */
	public QuadIterator iterateQuads(String s, String p, String o, String g, int batchRows) {
		checkBatchRows(batchRows);
		return call(h -> {
			NativeStream stream = new NativeStream(this, Native.termStreamOpen(h, s, p, o, g, batchRows));
			track(stream);
			return new QuadIterator(this, stream, Payload.CHUNK_ROWS, Payload.CHUNK_BYTES, Payload.LARGE_TERM);
		});
	}

	private static void checkBatchRows(int batchRows) {
		if (batchRows < 1) {
			throw new IllegalArgumentException("batchRows must be at least 1");
		}
	}

	// ---- term codes ----

	/** {@link #matchCodeChunks(long, long, long, long, int)} {@link #DEFAULT_BATCH_ROWS} rows at a time. */
	public CodeChunks matchCodeChunks(long s, long p, long o, long g) {
		return matchCodeChunks(s, p, o, g, DEFAULT_BATCH_ROWS);
	}

	/**
	 * The quads matching a pattern of codes (negative is free) as their codes, read at most {@code batchRows} rows at
	 * a time, in base row order: the rows of {@link #matchCodes(long, long, long, long, long, ByteBuffer, int)}, which
	 * on a pattern a {@code secondary-by-copy} index serves may return them in the index's order instead. A code
	 * that cannot match gives no rows. Close the chunks, or drain them, when done.
	 *
	 * @param batchRows the most rows read at a time, at least 1
	 */
	public CodeChunks matchCodeChunks(long s, long p, long o, long g, int batchRows) {
		checkBatchRows(batchRows);
		return call(h -> {
			NativeStream stream = new NativeStream(this, Native.codeStreamOpen(h, s, p, o, g, batchRows));
			track(stream);
			return new CodeChunks(this, stream);
		});
	}

	/** The number of quads matching a pattern of codes, exact and without reading rows; negative is free. */
	public long countCodes(long s, long p, long o, long g) {
		return callLong(h -> Native.countCodes(h, s, p, o, g));
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
		checkCodeBuffer(out, maxRows);
		return callInt(h -> Native.matchCodes(h, s, p, o, g, out, offset, maxRows));
	}

	/** Refuses a buffer codes cannot be written into: not writable, direct and native-order, or under maxRows rows. */
	static void checkCodeBuffer(ByteBuffer out, int maxRows) {
		Objects.requireNonNull(out, "out");
		if (!out.isDirect() || out.isReadOnly() || out.order() != ByteOrder.nativeOrder()
				|| out.capacity() < (long) maxRows * ROW_BYTES) {
			throw new IllegalArgumentException("out must be a writable, direct, native-order buffer of at least maxRows * "
					+ ROW_BYTES + " bytes");
		}
	}

	private static void checkWindow(long offset, int rows) {
		if (offset < 0 || rows < 0) {
			throw new IllegalArgumentException("offset and maxRows must not be negative");
		}
	}
}
