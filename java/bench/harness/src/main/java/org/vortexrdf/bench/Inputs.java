package org.vortexrdf.bench;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/** The files {@code java/bench/run.py} writes for a worker, read back. */
public final class Inputs {
	private Inputs() {
	}

	/**
	 * The probes file: one probe per line, as name, s, p, o, g and kind ({@code triples}, {@code quads} or
	 * {@code full}), tab-separated, with an empty field for a wildcard.
	 */
	public static List<Pattern> probes(Path tsv) throws IOException {
		List<Pattern> out = new ArrayList<>();
		for (String line : Files.readAllLines(tsv, StandardCharsets.UTF_8)) {
			if (line.isEmpty()) {
				continue;
			}
			String[] f = line.split("\t", -1);
			if (f.length != 6) {
				throw new IllegalArgumentException("a probe line has 6 fields, not " + f.length + ": " + line);
			}
			out.add(new Pattern(f[0], term(f[1]), term(f[2]), term(f[3]), term(f[4]),
					Pattern.Kind.valueOf(f[5].toUpperCase(Locale.ROOT))));
		}
		return out;
	}

	/** Every row of an N-Triples or N-Quads file the generator wrote. */
	public static List<Quad> rows(Path file) throws IOException {
		List<Quad> out = new ArrayList<>();
		for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
			if (!line.isEmpty()) {
				out.add(line(line));
			}
		}
		return out;
	}

	/**
	 * One line, {@code s p o [g] .}: IRIs and blank nodes are single tokens, and a literal runs from its opening
	 * quote to its closing one (the generator writes no escapes) plus any suffix up to the next space.
	 */
	static Quad line(String line) {
		List<String> terms = new ArrayList<>(4);
		int i = 0, n = line.length();
		while (true) {
			while (i < n && line.charAt(i) == ' ') {
				i++;
			}
			if (i >= n) {
				throw new IllegalArgumentException("a line ends with ' .': " + line);
			}
			if (i == n - 1 && line.charAt(i) == '.') {
				break;
			}
			int from = line.charAt(i) == '"' ? line.indexOf('"', i + 1) : i;
			int end = from < 0 ? -1 : line.indexOf(' ', from);
			if (end < 0) {
				throw new IllegalArgumentException("a line ends with ' .': " + line);
			}
			terms.add(line.substring(i, end));
			i = end;
		}
		if (terms.size() < 3 || terms.size() > 4) {
			throw new IllegalArgumentException("a line has 3 or 4 terms, not " + terms.size() + ": " + line);
		}
		return new Quad(terms.get(0), terms.get(1), terms.get(2), terms.size() == 4 ? terms.get(3) : null);
	}

	private static String term(String field) {
		return field.isEmpty() ? null : field;
	}
}
