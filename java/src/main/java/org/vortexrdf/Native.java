package org.vortexrdf;

import java.nio.ByteBuffer;

/**
 * The native methods of {@code libvortex_rdf_jni}, one JNI function each; the public classes check their
 * arguments and hold the handle. A handle is the address of a native store and is never used after close.
 * <p>
 * The library is found, in this order, at the path in the system property {@code vortexrdf.library}, at the path
 * in the environment variable {@code VORTEX_RDF_JNI_LIB}, as the copy the jar carries for this platform, or by
 * {@link System#loadLibrary} under the name {@code vortex_rdf_jni} (on {@code java.library.path}); see
 * {@link NativeLibrary}. On Java 24 and later loading it is a restricted method:
 * {@code --enable-native-access=ALL-UNNAMED} (or the jar's module) keeps the JVM from warning.
 */
final class Native {
	static {
		NativeLibrary.load();
	}

	private Native() {
	}

	static native long open(String path, boolean inMemory);

	static native void close(long handle);

	static native long size(long handle);

	static native String layout(long handle);

	static native String[] indexes(long handle);

	/** Terms are N-Triples spellings; null is a free position, "" the default graph. */
	static native long count(long handle, String s, String p, String o, String g);

	/**
	 * A read of every quad matching a pattern as its first chunk ({@link Payload}), whose last slot is
	 * {@code {stream, rows}}: the term stream of the chunks left ({@link #termStreamNext}), 0 when none are, and the
	 * read's rows. A chunk holds at most {@code chunkRows} rows and {@code chunkBytes} bytes of terms; terms over
	 * {@code largeTerm} bytes cross on their own. The caller frees the stream within the call that opened it.
	 */
	static native Object[] quadsOpen(long handle, String s, String p, String o, String g, int chunkRows,
			int chunkBytes, int largeTerm);

	/** The native objects held by handle and not yet freed, in this process: term and code streams. */
	static native long openHandles();

	/**
	 * A read of the matching quads as a stream of chunks of at most {@code batchRows} rows, sent as {@link #quadsOpen}
	 * sends its chunks; its handle.
	 */
	static native long termStreamOpen(long handle, String s, String p, String o, String g, int batchRows,
			int chunkRows, int chunkBytes, int largeTerm);

	/** A term stream's next chunk ({@link Payload}, its last slot null); null at the end. */
	static native Object[] termStreamNext(long stream);

	/** A read of the quads matching a pattern of codes (-1 is free) as a stream of their codes; its handle. */
	static native long codeStreamOpen(long handle, long s, long p, long o, long g, int batchRows);

	/** Writes a code stream's next rows, up to {@code maxRows}, as {@link #matchCodes} does; 0 at the end. */
	static native int codeStreamNext(long stream, ByteBuffer out, int maxRows);

	/** Frees a term or code stream; 0 is a no-op. */
	static native void streamClose(long stream);

	static native boolean hasDictionary(long handle);

	/** The number of terms of a store with a term dictionary. */
	static native long termCount(long handle);

	/** vortex-rdf's encoded-search work counters, in {@link #engineCounterNames()} order. */
	static native long[] engineCounters();

	static native String[] engineCounterNames();

	/** {defaultGraph or -1, literals start, end, iris start, end, blanks start, end}; null without a dictionary. */
	static native long[] ranges(long handle);

	static native long encode(long handle, String term);

	static native String decode(long handle, long code);

	/** Each code's term, one string apiece (null for a negative code or one outside the dictionary). */
	static native String[] decodeStrings(long handle, long[] codes);

	/**
	 * The terms of {@code codes[from, from + count)} from the first, as one chunk ({@link Payload}) of at most
	 * {@code chunkBytes} bytes of terms and at least one code; a negative code and one outside the dictionary hold
	 * no term. The last slot is {@code {next}}, the codes the next slice should offer.
	 */
	static native Object[] decodeMany(long handle, long[] codes, int from, int count, int chunkBytes, int largeTerm);

	/** Codes are non-negative; -1 is a free position. */
	static native long countCodes(long handle, long s, long p, long o, long g);

	/** Writes {@code (s, p, o, g)} codes, native byte order, from the start of the direct buffer. */
	static native int matchCodes(long handle, long s, long p, long o, long g, ByteBuffer out, long offset, int maxRows);
}
