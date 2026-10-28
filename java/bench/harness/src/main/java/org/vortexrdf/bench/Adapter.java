package org.vortexrdf.bench;

import java.nio.file.Path;
import java.util.List;

/**
 * One library under test, driven through the API a Java program would call.
 *
 * <p>{@link #count} is the MEASURED UNIT of {@code python/bench/adapters.py}: consume every match of a prepared
 * pattern in the library's natural result form, read each term as a {@link String} (an IRI's text, a literal's
 * lexical form plus its language or a datatype other than xsd:string, a blank node's label), fold their lengths
 * into {@link Sink} once per call, and return how many rows matched. {@link #countOnly} is the COUNT/ASK twin: the
 * library's cheapest correct public count path, reading no term; looking a pattern's terms up stays inside it.
 *
 * @param <H> an open store
 * @param <Q> a pattern converted into the library's own terms
 */
public interface Adapter<H, Q> {
	String slug();

	String label();

	/** False for a library whose model has no graphs: it builds from the triples projection. */
	default boolean supportsQuads() {
		return true;
	}

	/** Why the library cannot add or delete, shown as the cells' tooltip; null when it can. */
	default String mutationUnsupported() {
		return null;
	}

	/** Whether the build leaves an artifact at its location that a fresh process can reopen, and that is sized. */
	boolean persistent();

	/** Ingest {@code src} (an .nq or .nt file) into a queryable store; {@code location} is an empty directory. */
	H build(Path src, Path location) throws Exception;

	/** A queryable store from what {@link #build} left at {@code location}; a store kept only in memory re-parses {@code src}. */
	H open(Path src, Path location) throws Exception;

	/** Convert a pattern's N-Triples spellings into the library's terms, outside every timed region. */
	Q prepare(Pattern p) throws Exception;

	long count(H store, Q query) throws Exception;

	long countOnly(H store, Q query) throws Exception;

	/** Add each quad, converting its spellings as it goes, and commit. */
	default void add(H store, List<Quad> quads) throws Exception {
		throw new UnsupportedOperationException(mutationUnsupported());
	}

	/** Delete each quad, converting its spellings as it goes, and commit. */
	default void delete(H store, List<Quad> quads) throws Exception {
		throw new UnsupportedOperationException(mutationUnsupported());
	}

	/** Close the store and release whatever the library holds for it, so the next open is a real one. */
	void dispose(H store) throws Exception;
}
