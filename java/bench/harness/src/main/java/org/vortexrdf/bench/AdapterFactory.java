package org.vortexrdf.bench;

import java.util.List;

/**
 * A library module's adapters, found by {@link Worker} through {@link java.util.ServiceLoader}: each module lists
 * its factory in {@code META-INF/services/org.vortexrdf.bench.AdapterFactory}, and a worker's classpath holds one
 * module.
 */
public interface AdapterFactory {
	/** The slugs this module serves, as the dashboard names its rows. */
	List<String> slugs();

	Adapter<?, ?> create(String slug);
}
