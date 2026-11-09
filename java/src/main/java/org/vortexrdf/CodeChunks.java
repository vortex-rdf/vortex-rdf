package org.vortexrdf;

import java.nio.ByteBuffer;

/**
 * The term codes of the quads matching a pattern of codes, read a chunk at a time
 * ({@link VortexRdfStore#matchCodeChunks}), in the match's order, written into the caller's buffer as
 * {@link VortexRdfStore#matchCodes(long, long, long, long, long, ByteBuffer, int)} writes them. Close it when done, or
 * drain it: either frees its native stream, as does closing its store, and one left unreachable unclosed is freed by a
 * cleaner.
 *
 * <p>After its store is closed, one that has not ended raises {@link IllegalStateException}; one that ended or was
 * closed writes no more rows. Not thread-safe.
 */
public final class CodeChunks implements AutoCloseable {
	private final NativeStream stream;

	CodeChunks(NativeStream stream) {
		this.stream = stream;
		stream.freeWith(this);
	}

	/**
	 * Writes the next rows, up to {@code maxRows}, as {@code (s, p, o, g)} codes of eight bytes each
	 * ({@link VortexRdfStore#ROW_BYTES} a row), from the start of {@code out} (its position and limit are neither read
	 * nor changed), and returns the rows written: 0 once every row was.
	 *
	 * @param out a writable direct buffer in native byte order of at least {@code maxRows * ROW_BYTES} bytes
	 * @param maxRows at least 1
	 * @return the number of rows written, 0 once every row was
	 * @throws IllegalStateException if the store was closed before this stream ended
	 */
	public int next(ByteBuffer out, int maxRows) {
		VortexRdfStore.checkCodeBuffer(out, maxRows);
		if (maxRows < 1) {
			throw new IllegalArgumentException("maxRows must be at least 1");
		}
		return stream.live() ? stream.codesNext(out, maxRows) : 0;
	}

	/** Ends the stream and frees it natively; closing twice is harmless. */
	@Override
	public void close() {
		stream.close();
	}
}
