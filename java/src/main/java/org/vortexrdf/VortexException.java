package org.vortexrdf;

/**
 * A failure of the native library: a store that does not open or read, a malformed pattern term (the message
 * names the position that held it, as {@code pattern subject: ...}), or a call the store's layout cannot serve.
 * Unchecked, like the {@code IllegalArgumentException}s of the same API.
 */
public class VortexException extends RuntimeException {
	private static final long serialVersionUID = 1L;

	public VortexException(String message) {
		super(message);
	}
}
