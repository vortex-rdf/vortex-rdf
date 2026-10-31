package org.vortexrdf;

import java.nio.ByteBuffer;

/**
 * The native stream behind one iterator, freed exactly once: at its end, on a read error, when its iterator closes,
 * when its store closes, or, as the iterator's {@link java.lang.ref.Cleaner} action, when the iterator becomes
 * unreachable unclosed. It holds no reference to its iterator, so the iterator can become unreachable while its store
 * still tracks this. Its native calls and its freeing are serialized on it, so threads misusing one iterator can
 * interleave rows but never reach a freed stream. Lock order: the store's lock, then this, then the store's set of
 * streams.
 */
final class NativeStream implements Runnable {
	private final VortexStore store;
	private long handle;

	NativeStream(VortexStore store, long handle) {
		this.store = store;
		this.handle = handle;
	}

	/** The next payload of a term stream; null at its end, or once freed. */
	synchronized Object[] termsNext(int chunkRows, int chunkBytes, int largeTerm) {
		return handle == 0 ? null : Native.termStreamNext(handle, chunkRows, chunkBytes, largeTerm);
	}

	/** The rows a code stream wrote into {@code out}; 0 at its end, or once freed. */
	synchronized int codesNext(ByteBuffer out, int maxRows) {
		return handle == 0 ? 0 : Native.codeStreamNext(handle, out, maxRows);
	}

	/** Frees the native stream, once, and stops its store tracking it. */
	synchronized void free() {
		if (handle != 0) {
			Native.streamClose(handle);
			handle = 0;
			store.forget(this);
		}
	}

	/** The cleaner's action: {@link #free()}. */
	@Override
	public void run() {
		free();
	}
}
