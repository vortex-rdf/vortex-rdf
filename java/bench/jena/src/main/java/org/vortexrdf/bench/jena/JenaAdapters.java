package org.vortexrdf.bench.jena;

import java.util.List;
import org.vortexrdf.bench.Adapter;
import org.vortexrdf.bench.AdapterFactory;

/** Jena's in-memory dataset and TDB2. */
public final class JenaAdapters implements AdapterFactory {
	@Override
	public List<String> slugs() {
		return List.of("jena_tim", "jena_tdb2");
	}

	@Override
	public Adapter<?, ?> create(String slug) {
		return switch (slug) {
		case "jena_tim" -> new JenaTimAdapter();
		case "jena_tdb2" -> new JenaTdb2Adapter();
		default -> throw new IllegalArgumentException("not a Jena slug: " + slug);
		};
	}
}
