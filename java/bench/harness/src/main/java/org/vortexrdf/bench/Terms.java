package org.vortexrdf.bench;

/**
 * The N-Triples spellings the generator writes, split into their parts for the libraries that need term objects.
 *
 * <p>Deliberately narrow, as {@code split_term} in {@code python/bench/adapters.py} is: plain IRIs, quote-delimited
 * literals without escapes, their language and datatype suffixes, and blank nodes. Anything else is refused rather
 * than guessed at, so a generator change fails here instead of silently mismatching every library's probes.
 */
public final class Terms {
	public enum Kind {
		IRI, LITERAL, BLANK
	}

	/** A term's parts; {@code lang} and {@code datatype} are null unless the literal has one. */
	public record Term(Kind kind, String value, String lang, String datatype) {
	}

	private Terms() {
	}

	public static Term parse(String t) {
		if (t.startsWith("<") && t.endsWith(">")) {
			return new Term(Kind.IRI, t.substring(1, t.length() - 1), null, null);
		}
		if (t.startsWith("\"")) {
			int end = t.lastIndexOf('"');
			if (end <= 0) {
				throw new IllegalArgumentException("unterminated literal: " + t);
			}
			String value = t.substring(1, end), suffix = t.substring(end + 1);
			if (suffix.startsWith("@")) {
				return new Term(Kind.LITERAL, value, suffix.substring(1), null);
			}
			if (suffix.startsWith("^^")) {
				String dt = suffix.substring(2);
				return new Term(Kind.LITERAL, value, null,
						dt.startsWith("<") && dt.endsWith(">") ? dt.substring(1, dt.length() - 1) : dt);
			}
			if (!suffix.isEmpty()) {
				throw new IllegalArgumentException("unexpected literal suffix " + suffix + " in " + t);
			}
			return new Term(Kind.LITERAL, value, null, null);
		}
		if (t.startsWith("_:")) {
			return new Term(Kind.BLANK, t.substring(2), null, null);
		}
		throw new IllegalArgumentException("unrecognized N-Triples term: " + t);
	}
}
