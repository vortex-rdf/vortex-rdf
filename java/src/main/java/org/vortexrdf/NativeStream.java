package org.vortexrdf;

import java.lang.ref.Cleaner;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.function.LongFunction;

/**
 * The native stream behind one iterator ({@link QuadIterator}, {@link CodeChunks}) and the iteration's lifecycle,
 * which the iterator only adapts. It ends at its end, on a read error, or when its iterator closes, and is freed
 * exactly once: then, when its store closes, or, as the iterator's {@link Cleaner} action, when the iterator becomes
 * unreachable unclosed. It holds no reference to its iterator, so the iterator can become unreachable while its store
 * still tracks this. Its native calls and its freeing are serialized on it, so threads misusing one iterator can
 * interleave rows but never reach a freed stream. Lock order: the store's lock, then this.
 */
final class NativeStream {
	private static final Cleaner CLEANER = Cleaner.create();

	private final VortexRdfStore store;
	private long handle;
	private Cleaner.Cleanable cleanable;
	/** Set once the iteration ended or closed: it reads no more. */
	private boolean ended;

	NativeStream(VortexRdfStore store, long handle) {
		this.store = store;
		this.handle = handle;
	}

	/** Frees this stream when {@code iterator}, which reads it, becomes unreachable unclosed. */
	void freeWith(Object iterator) {
		cleanable = CLEANER.register(iterator, this::free);
	}

	/**
	 * Whether the iteration reads on: false once it ended or closed.
	 *
	 * @throws IllegalStateException if the store was closed before the iteration ended
	 */
	boolean live() {
		if (ended) {
			return false;
		}
		if (store.isClosed()) {
			throw new IllegalStateException("the store is closed");
		}
		return true;
	}

	/** A term stream's next payload; null at its end, which ends the iteration. */
	Object[] termsNext() {
		return pull(Native::termStreamNext, null);
	}

	/** The rows a code stream wrote into {@code out}; 0 at its end, which ends the iteration. */
	int codesNext(ByteBuffer out, int maxRows) {
		return pull(stream -> Native.codeStreamNext(stream, out, maxRows), 0);
	}

	/**
	 * What {@code next} reads off the native stream, the store held open meanwhile; {@code end}, which ends the
	 * iteration, at its end or once it was freed. A failed read ends the iteration too, whatever was thrown (an
	 * {@link Error} as well: the native stream may have moved past the rows it failed to hand over); a store closed
	 * meanwhile does not, so the iteration goes on refusing.
	 */
	private <T> T pull(LongFunction<T> next, T end) {
		T read;
		try {
			read = store.call(h -> read(next, end));
		} catch (IllegalStateException closed) {
			throw closed;
		} catch (Throwable failed) {
			close();
			throw failed;
		}
		if (Objects.equals(read, end)) {
			close();
		}
		return read;
	}

	private synchronized <T> T read(LongFunction<T> next, T end) {
		return handle == 0 ? end : next.apply(handle);
	}

	/** Ends the iteration and frees the native stream; closing twice is harmless. */
	void close() {
		ended = true;
		cleanable.clean();
	}

	/**
	 * Frees the native stream, once, and stops its store tracking it. The handle is cleared first, so a native close
	 * that fails can never be retried on a stream already released.
	 */
	synchronized void free() {
		long freed = handle;
		if (freed != 0) {
			handle = 0;
			store.forget(this);
			Native.streamClose(freed);
		}
	}
}
