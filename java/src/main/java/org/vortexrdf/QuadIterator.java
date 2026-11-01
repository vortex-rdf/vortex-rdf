package org.vortexrdf;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * The quads matching a pattern, read a chunk at a time ({@link VortexRdfStore#iterateQuads}): rows {@code {s, p, o, g}}
 * of N-Triples spellings, in the match's order, each a fresh array; rows of one chunk that hold the same term share its
 * {@code String}. Close it when done, or drain it: either frees its native stream, as does closing its store, and an
 * iterator left unreachable unclosed is freed by a cleaner.
 *
 * <p>After its store is closed, an iterator that has not ended raises {@link IllegalStateException}; one that ended
 * or was closed answers no more rows. Not thread-safe, as no {@code Iterator} is.
 */
public final class QuadIterator implements Iterator<String[]>, AutoCloseable {
	private final NativeStream stream;
	private final List<String[]> rows = new ArrayList<>();
	private int next;

	QuadIterator(NativeStream stream) {
		this.stream = stream;
		stream.freeWith(this);
	}

	/**
	 * Whether a row is left, read a chunk ahead when the last is spent.
	 *
	 * @throws IllegalStateException if the store was closed before this iterator ended
	 */
	@Override
	public boolean hasNext() {
		if (!stream.live()) {
			return false;
		}
		if (next < rows.size()) {
			return true;
		}
		rows.clear();
		next = 0;
		Object[] payload = stream.termsNext();
		if (payload == null) {
			return false;
		}
		Payload.addQuads(payload, rows);
		return true;
	}

	/**
	 * The next row.
	 *
	 * @throws NoSuchElementException past the last row
	 * @throws IllegalStateException if the store was closed before this iterator ended
	 */
	@Override
	public String[] next() {
		if (!hasNext()) {
			throw new NoSuchElementException();
		}
		String[] row = rows.get(next);
		rows.set(next++, null);
		return row;
	}

	/** Ends the iteration and frees its native stream; closing twice is harmless. */
	@Override
	public void close() {
		rows.clear();
		stream.close();
	}

	/** The rows left as a sequential, ordered stream, whose {@code close()} closes this iterator. */
	public Stream<String[]> stream() {
		Spliterator<String[]> rows = Spliterators.spliteratorUnknownSize(this,
				Spliterator.ORDERED | Spliterator.NONNULL);
		return StreamSupport.stream(rows, false).onClose(this::close);
	}
}
