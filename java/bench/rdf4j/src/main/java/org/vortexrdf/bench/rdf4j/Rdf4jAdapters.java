package org.vortexrdf.bench.rdf4j;

import java.util.List;
import org.vortexrdf.bench.Adapter;
import org.vortexrdf.bench.AdapterFactory;

/** RDF4J's MemoryStore, NativeStore and LMDB store. */
public final class Rdf4jAdapters implements AdapterFactory {
	@Override
	public List<String> slugs() {
		return List.of("rdf4j_memory", "rdf4j_native", "rdf4j_lmdb");
	}

	@Override
	public Adapter<?, ?> create(String slug) {
		return switch (slug) {
		case "rdf4j_memory" -> new Rdf4jAdapter(slug, "RDF4J MemoryStore (memory)", Rdf4jAdapter.Store.MEMORY);
		case "rdf4j_native" -> new Rdf4jAdapter(slug, "RDF4J NativeStore (file)", Rdf4jAdapter.Store.NATIVE);
		case "rdf4j_lmdb" -> new Rdf4jAdapter(slug, "RDF4J LMDB (file)", Rdf4jAdapter.Store.LMDB);
		default -> throw new IllegalArgumentException("not an RDF4J slug: " + slug);
		};
	}
}
