package org.vortexrdf.bench;

import java.util.Locale;

/** How an adapter's {@link Adapter#open} obtains its store, which the memory panel's tooltips name. */
public enum Obtained {
	/** Opened from a file that it memory-maps. */
	MAPPED,
	/** Read from a file into memory. */
	LOADED,
	/** Built from the triples. */
	BUILT,
	/** Opened from an on-disk database. */
	OPENED;

	/** The worker's JSON spelling: the lowercase name. */
	public String json() {
		return name().toLowerCase(Locale.ROOT);
	}
}
