package org.vortexrdf.bench.hdt;

import java.util.List;
import org.vortexrdf.bench.Adapter;
import org.vortexrdf.bench.AdapterFactory;

/** hdt-java. */
public final class HdtJavaAdapters implements AdapterFactory {
	@Override
	public List<String> slugs() {
		return List.of("hdt_java");
	}

	@Override
	public Adapter<?, ?> create(String slug) {
		if (!slug.equals("hdt_java")) {
			throw new IllegalArgumentException("not an hdt-java slug: " + slug);
		}
		return new HdtJavaAdapter();
	}
}
