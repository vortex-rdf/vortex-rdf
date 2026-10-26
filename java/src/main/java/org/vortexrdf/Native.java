package org.vortexrdf;

import java.nio.ByteBuffer;
import java.nio.file.Paths;

/**
 * The native methods of {@code libvortex_rdf_jni}, one JNI function each; the public classes check their
 * arguments and hold the handle. A handle is the address of a native store and is never used after close.
 * <p>
 * The library is found, in this order, at the path in the system property {@code vortexrdf.library}, at the path
 * in the environment variable {@code VORTEX_RDF_JNI_LIB}, or by {@link System#loadLibrary} under the name
 * {@code vortex_rdf_jni} (on {@code java.library.path}). On Java 24 and later loading it is a restricted method:
 * {@code --enable-native-access=ALL-UNNAMED} (or the jar's module) keeps the JVM from warning.
 */
final class Native {
	static {
		String path = System.getProperty("vortexrdf.library");
		if (path == null) {
			path = System.getenv("VORTEX_RDF_JNI_LIB");
		}
		try {
			if (path != null) {
				System.load(Paths.get(path).toAbsolutePath().toString());
			} else {
				System.loadLibrary("vortex_rdf_jni");
			}
		} catch (UnsatisfiedLinkError e) {
			throw new UnsatisfiedLinkError("cannot load the vortex-rdf native library ("
					+ (path != null ? path : "vortex_rdf_jni on java.library.path")
					+ "); set -Dvortexrdf.library=<path to libvortex_rdf_jni> or VORTEX_RDF_JNI_LIB: " + e.getMessage());
		}
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

	/** The matching quads, four terms each, flat; a negative limit takes every row. */
	static native String[] quads(long handle, String s, String p, String o, String g, long offset, int limit);

	static native boolean hasDictionary(long handle);

	/** The number of terms of a store with a term dictionary. */
	static native long termCount(long handle);

	/** {defaultGraph or -1, literals start, end, iris start, end, blanks start, end}; null without a dictionary. */
	static native long[] ranges(long handle);

	static native long encode(long handle, String term);

	static native String decode(long handle, long code);

	static native String[] decodeMany(long handle, long[] codes);

	/** Codes are non-negative; -1 is a free position. */
	static native long countCodes(long handle, long s, long p, long o, long g);

	/** Writes {@code (s, p, o, g)} codes, native byte order, from the start of the direct buffer. */
	static native int matchCodes(long handle, long s, long p, long o, long g, ByteBuffer out, long offset, int maxRows);
}
