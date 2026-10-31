package org.vortexrdf;

import java.lang.ref.Cleaner;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * The quads matching a pattern, read a chunk at a time ({@link VortexStore#iterateQuads}): rows {@code {s, p, o, g}}
 * of N-Triples spellings, in the match's order, each a fresh array; rows of one chunk that hold the same term share its
 * {@code String}. Close it when done, or drain it: either frees its native stream, as does closing its store, and an
 * iterator left unreachable unclosed is freed by a cleaner.
 *
 * <p>After its store is closed, an iterator that has not ended raises {@link IllegalStateException}; one that ended
 * or was closed answers no more rows. Not thread-safe, as no {@code Iterator} is.
 */
public final class QuadIterator implements Iterator<String[]>, AutoCloseable {
	static final Cleaner CLEANER = Cleaner.create();

	private final VortexStore store;
	private final NativeStream stream;
	private final Cleaner.Cleanable cleanable;
	private final int chunkRows;
	private final int chunkBytes;
	private final int largeTerm;
	private final List<String[]> rows = new ArrayList<>();
	private int next;
	private boolean ended;

	QuadIterator(VortexStore store, NativeStream stream, int chunkRows, int chunkBytes, int largeTerm) {
		this.store = store;
		this.stream = stream;
		this.cleanable = CLEANER.register(this, stream);
		this.chunkRows = chunkRows;
		this.chunkBytes = chunkBytes;
		this.largeTerm = largeTerm;
	}

	/**
	 * Whether a row is left, read a chunk ahead when the last is spent.
	 *
	 * @throws IllegalStateException if the store was closed before this iterator ended
	 */
	@Override
	public boolean hasNext() {
		if (ended) {
			return false;
		}
		if (store.isClosed()) {
			throw new IllegalStateException("the store is closed");
		}
		if (next < rows.size()) {
			return true;
		}
		rows.clear();
		next = 0;
		Object[] payload;
		try {
			payload = store.call(h -> stream.termsNext(chunkRows, chunkBytes, largeTerm));
		} catch (IllegalStateException closed) {
			throw closed;
		} catch (RuntimeException failed) {
			close();
			throw failed;
		}
		if (payload == null) {
			close();
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
		ended = true;
		rows.clear();
		cleanable.clean();
	}

	/** The rows left as a sequential, ordered stream, whose {@code close()} closes this iterator. */
	public Stream<String[]> stream() {
		Spliterator<String[]> rows = Spliterators.spliteratorUnknownSize(this,
				Spliterator.ORDERED | Spliterator.NONNULL);
		return StreamSupport.stream(rows, false).onClose(this::close);
	}
}
