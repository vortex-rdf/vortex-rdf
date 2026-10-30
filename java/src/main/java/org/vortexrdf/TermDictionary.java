package org.vortexrdf;

import java.util.Objects;

/**
 * The terms of a store of the {@code dictionary} layout, by code: the translation between a term's N-Triples
 * spelling and the integer the store's columns hold. Obtained from {@link VortexStore#dictionary()}; it lives as
 * long as the store and fails with {@link IllegalStateException} after it is closed. A file store reads terms from
 * the mapped file on demand.
 */
public final class TermDictionary {
	private final VortexStore store;

	TermDictionary(VortexStore store) {
		this.store = store;
	}

	/** The number of terms; codes are {@code 0} to {@code size() - 1}. */
	public long size() {
		return store.callLong(Native::termCount);
	}

	/**
	 * The code of a term, or {@link VortexStore#NONE} when the dictionary lacks it, which in a code pattern is a free
	 * position: check for it before putting the code into one. The spelling is tolerated: an IRI with or without
	 * angle brackets, a literal with an explicit {@code xsd:string} type or an upper-case language tag, and the
	 * default graph as {@code ""} all resolve to the stored form's code.
	 */
	public long encode(String term) {
		Objects.requireNonNull(term, "term");
		return store.callLong(h -> Native.encode(h, term));
	}

	/** The N-Triples spelling of a code; {@code null} for a code outside the dictionary. */
	public String decode(long code) {
		return store.call(h -> Native.decode(h, code));
	}

	/** {@link #decode} of each code, in order, for codes in any order and with repeats. */
	public String[] decodeMany(long[] codes) {
		Objects.requireNonNull(codes, "codes");
		return decodeMany(codes, Payload.CHUNK_ROWS, Payload.CHUNK_BYTES, Payload.LARGE_TERM);
	}

	/**
	 * {@link #decodeMany(long[])} in native calls of at most {@code sliceCodes} codes and {@code chunkBytes} bytes of
	 * terms, terms over {@code largeTerm} bytes crossing on their own; tests set them small to cut every chunk.
	 */
	String[] decodeMany(long[] codes, int sliceCodes, int chunkBytes, int largeTerm) {
		if (sliceCodes < 1) {
			throw new IllegalArgumentException("sliceCodes < 1");
		}
		return store.call(h -> {
			String[] terms = new String[codes.length];
			int slice = sliceCodes;
			for (int from = 0; from < codes.length;) {
				Object[] payload = Native.decodeMany(h, codes, from, Math.min(slice, codes.length - from), chunkBytes,
						largeTerm);
				from += Payload.fill(payload, terms, from);
				slice = Payload.nextSlice(payload, sliceCodes);
			}
			return terms;
		});
	}

	/** The code ranges of the term kinds. Terms of a kind are contiguous, so a code's kind is a range check. */
	public Ranges ranges() {
		return new Ranges(store.call(Native::ranges));
	}

	/** The code ranges of the term kinds, and the default graph's code. */
	public static final class Ranges {
		private final long defaultGraph;
		private final CodeRange literals;
		private final CodeRange iris;
		private final CodeRange blanks;

		/** From the native layout {@code {defaultGraph or -1, literals, iris, blanks}}, each range start then end. */
		private Ranges(long[] r) {
			defaultGraph = r[0];
			literals = new CodeRange(r[1], r[2]);
			iris = new CodeRange(r[3], r[4]);
			blanks = new CodeRange(r[5], r[6]);
		}

		/** The code of {@code ""}, the default graph's name, or {@link VortexStore#NONE} when it is not a term. */
		public long defaultGraph() {
			return defaultGraph;
		}

		/** The literals' codes. */
		public CodeRange literals() {
			return literals;
		}

		/** The IRIs' codes. */
		public CodeRange iris() {
			return iris;
		}

		/** The blank nodes' codes. */
		public CodeRange blanks() {
			return blanks;
		}
	}

	/** The codes {@code start} (inclusive) to {@code end} (exclusive). */
	public static final class CodeRange {
		private final long start;
		private final long end;

		private CodeRange(long start, long end) {
			this.start = start;
			this.end = end;
		}

		/** The first code. */
		public long start() {
			return start;
		}

		/** One past the last code. */
		public long end() {
			return end;
		}

		/** Whether {@code code} is in the range. */
		public boolean contains(long code) {
			return start <= code && code < end;
		}
	}
}
