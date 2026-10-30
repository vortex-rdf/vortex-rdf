package org.vortexrdf;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * A chunk of terms as the native library sends it, an {@code Object[5]}: the UTF-8 text of the chunk's distinct
 * terms ({@code byte[]}); where each distinct term starts and ends in it ({@code int[]}, two per term); one entry per
 * position ({@code int[]}: {@code k >= 0} is distinct term {@code k}, {@code -1} no term, {@code -2 - j} large term
 * {@code j}); the terms over the large-term size ({@code String[]}, or null when there are none); and a call's own
 * {@code long[]}, or null. Each distinct term becomes one {@code String}, shared by the positions that hold it.
 */
final class Payload {
	/** The most rows a chunk holds. */
	static final int CHUNK_ROWS = 65_536;

	/** The most bytes of terms a chunk holds, large terms included; one row may pass it by its large terms. */
	static final int CHUNK_BYTES = 16 << 20;

	/** Terms of more bytes than this cross on their own, never in a chunk's shared text. */
	static final int LARGE_TERM = 1 << 20;

	private Payload() {
	}

	/** The chunk's distinct terms of its shared text, each built once. */
	private static String[] distinct(Object[] payload) {
		byte[] text = (byte[]) payload[0];
		int[] bounds = (int[]) payload[1];
		String[] terms = new String[bounds.length / 2];
		for (int k = 0; k < terms.length; k++) {
			int start = bounds[2 * k];
			terms[k] = new String(text, start, bounds[2 * k + 1] - start, StandardCharsets.UTF_8);
		}
		return terms;
	}

	/** A position's term, from its entry. */
	private static String term(int entry, String[] distinct, String[] large) {
		return entry >= 0 ? distinct[entry] : entry == -1 ? null : large[-2 - entry];
	}

	/** Adds the chunk's rows, four positions each, to {@code rows}: a fresh {@code String[4]} per row. */
	static void addQuads(Object[] payload, List<String[]> rows) {
		String[] distinct = distinct(payload);
		int[] terms = (int[]) payload[2];
		String[] large = (String[]) payload[3];
		for (int i = 0; i < terms.length; i += 4) {
			rows.add(new String[] { term(terms[i], distinct, large), term(terms[i + 1], distinct, large),
					term(terms[i + 2], distinct, large), term(terms[i + 3], distinct, large) });
		}
	}

	/** Writes the chunk's terms, a position each, into {@code out} from {@code from}; returns how many. */
	static int fill(Object[] payload, String[] out, int from) {
		String[] distinct = distinct(payload);
		int[] terms = (int[]) payload[2];
		String[] large = (String[]) payload[3];
		for (int i = 0; i < terms.length; i++) {
			out[from + i] = term(terms[i], distinct, large);
		}
		return terms.length;
	}

	/** A read's first chunk's cursor of the chunks left; 0 when none are. */
	static long cursor(Object[] payload) {
		return ((long[]) payload[4])[0];
	}

	/** A read's rows, from its first chunk. */
	static int rows(Object[] payload) {
		return (int) ((long[]) payload[4])[1];
	}

	/** The codes a lookup's next slice should offer, at most {@code max}. */
	static int nextSlice(Object[] payload, int max) {
		return (int) Math.min(max, ((long[]) payload[4])[0]);
	}
}
