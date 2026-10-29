package org.vortexrdf.bench;

/** A row of an input file, as N-Triples spellings; a null {@code g} is the default graph. */
public record Quad(String s, String p, String o, String g) {
}
