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
		long h = store.enter();
		try {
			return Native.termCount(h);
		} finally {
			store.exit();
		}
	}

	/**
	 * The code of a term, or {@link VortexStore#NONE} when the dictionary lacks it, which in a code pattern is a free
	 * position: check for it before putting the code into one. The spelling is tolerated: an
	 * IRI with or without angle brackets, a literal with an explicit {@code xsd:string} type or an upper-case
	 * language tag, and the default graph as {@code ""} all resolve to the stored form's code.
	 */
	public long encode(String term) {
		Objects.requireNonNull(term, "term");
		long h = store.enter();
		try {
			return Native.encode(h, term);
		} finally {
			store.exit();
		}
	}

	/** The N-Triples spelling of a code; {@code null} for a code outside the dictionary. */
	public String decode(long code) {
		long h = store.enter();
		try {
			return Native.decode(h, code);
		} finally {
			store.exit();
		}
	}

	/** {@link #decode} of each code, in order, for codes in any order and with repeats; one native call. */
	public String[] decodeMany(long[] codes) {
		Objects.requireNonNull(codes, "codes");
		long h = store.enter();
		try {
			return Native.decodeMany(h, codes);
		} finally {
			store.exit();
		}
	}

	/** The code ranges of the term kinds. Terms of a kind are contiguous, so a code's kind is a comparison. */
	public Ranges ranges() {
		long h = store.enter();
		try {
			return new Ranges(Native.ranges(h));
		} finally {
			store.exit();
		}
	}

	/** Code ranges, ends exclusive. */
	public static final class Ranges {
		private final long[] r;

		private Ranges(long[] r) {
			this.r = r;
		}

		/** The code of {@code ""}, the default graph's name, or {@link VortexStore#NONE} when it is not a term. */
		public long defaultGraph() {
			return r[0];
		}

		/** {@code {start, end}} of the literals' codes. */
		public long[] literals() {
			return new long[] { r[1], r[2] };
		}

		/** {@code {start, end}} of the IRIs' codes. */
		public long[] iris() {
			return new long[] { r[3], r[4] };
		}

		/** {@code {start, end}} of the blank nodes' codes. */
		public long[] blanks() {
			return new long[] { r[5], r[6] };
		}
	}
}
