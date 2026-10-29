package org.vortexrdf.bench.qendpoint;

import java.util.List;
import org.vortexrdf.bench.Adapter;
import org.vortexrdf.bench.AdapterFactory;

/** qEndpoint's HDT. */
public final class QendpointAdapters implements AdapterFactory {
	@Override
	public List<String> slugs() {
		return List.of("qendpoint_hdt");
	}

	@Override
	public Adapter<?, ?> create(String slug) {
		if (!slug.equals("qendpoint_hdt")) {
			throw new IllegalArgumentException("not a qEndpoint slug: " + slug);
		}
		return new QendpointAdapter();
	}
}
