package org.vortexrdf.bench;

/**
 * A query probe: N-Triples spellings at its bound positions, null at its wildcards.
 *
 * @param kind which probe set it belongs to
 */
public record Pattern(String name, String s, String p, String o, String g, Kind kind) {
	/** Triple patterns, graph-bound patterns, and the all-wildcard full scan. */
	public enum Kind {
		TRIPLES, QUADS, FULL
	}
}
